package scheduler

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/goto/optimus/core/scheduler"
	"github.com/goto/optimus/core/tenant"
	"github.com/goto/optimus/internal/errors"
)

// AirflowSyncStateRepository backs the per-project window claim used by the
// manual-state-override reconciliation worker. See
// docs/docs/rfcs/20260727_manual_state_override_reconciliation.md for the design this
// implements: a claimed window row is the mutex (no advisory locks), fenced by
// worker_id + locked_until so a crashed worker's window becomes re-claimable rather than
// wedging the project's sync forever.
type AirflowSyncStateRepository struct {
	db *pgxpool.Pool
}

func NewAirflowSyncStateRepository(pool *pgxpool.Pool) *AirflowSyncStateRepository {
	return &AirflowSyncStateRepository{db: pool}
}

// GetWatermark returns the end_time of the most recently terminal (success or failed)
// window for the project, or nil if none exist yet. failed windows count towards the
// watermark deliberately: excluding them would let one permanently-failing window block
// all later windows for the project from ever being attempted.
func (a *AirflowSyncStateRepository) GetWatermark(ctx context.Context, projectName tenant.ProjectName) (*time.Time, error) {
	query := `SELECT max(end_time) FROM airflow_sync_state WHERE project_name = $1 AND status IN ($2, $3)`

	var watermark *time.Time
	err := a.db.QueryRow(ctx, query, projectName, scheduler.AirflowSyncSuccess, scheduler.AirflowSyncFailed).Scan(&watermark)
	if err != nil {
		return nil, errors.Wrap(scheduler.EntityAirflowSync, "error getting airflow sync watermark", err)
	}
	return watermark, nil
}

// ClaimWindow attempts to claim [startTime, endTime) for the project. The insert is the
// claim: a conflict means some worker (this replica in an earlier attempt, or another
// replica) already owns or has finished this window, so the caller should skip it rather
// than treat that as an error.
func (a *AirflowSyncStateRepository) ClaimWindow(ctx context.Context, projectName tenant.ProjectName, startTime, endTime time.Time, workerID uuid.UUID, lockDuration time.Duration) (id uuid.UUID, claimed bool, err error) {
	query := `
		INSERT INTO airflow_sync_state (project_name, start_time, end_time, status, attempt_count, worker_id, locked_until)
		VALUES ($1, $2, $3, $4, 1, $5, now() + $6)
		ON CONFLICT (project_name, start_time, end_time) DO NOTHING
		RETURNING id`

	err = a.db.QueryRow(ctx, query, projectName, startTime, endTime, scheduler.AirflowSyncInProgress, workerID, lockDuration).Scan(&id)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return uuid.Nil, false, nil
		}
		return uuid.Nil, false, errors.Wrap(scheduler.EntityAirflowSync, "error claiming airflow sync window", err)
	}
	return id, true, nil
}

// ReclaimStaleWindow re-claims the single oldest window left `in_progress` past its
// locked_until (a worker that died mid-window) for the project, provided it has not yet
// exhausted maxAttempts. It returns nil, nil if there is nothing to reclaim.
func (a *AirflowSyncStateRepository) ReclaimStaleWindow(ctx context.Context, projectName tenant.ProjectName, workerID uuid.UUID, lockDuration time.Duration, maxAttempts int) (*scheduler.AirflowSyncWindow, error) {
	query := `
		UPDATE airflow_sync_state
		SET worker_id = $1, locked_until = now() + $2, attempt_count = attempt_count + 1, updated_at = now()
		WHERE id = (
			SELECT id FROM airflow_sync_state
			WHERE project_name = $3 AND status = $4 AND locked_until < now() AND attempt_count < $5
			ORDER BY start_time ASC
			LIMIT 1
		)
		RETURNING id, project_name, start_time, end_time, status, attempt_count, worker_id, locked_until`

	var w airflowSyncStateRow
	err := a.db.QueryRow(ctx, query, workerID, lockDuration, projectName, scheduler.AirflowSyncInProgress, maxAttempts).Scan(
		&w.ID, &w.ProjectName, &w.StartTime, &w.EndTime, &w.Status, &w.AttemptCount, &w.WorkerID, &w.LockedUntil)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil //nolint:nilnil
		}
		return nil, errors.Wrap(scheduler.EntityAirflowSync, "error reclaiming stale airflow sync window", err)
	}
	return w.toSchedulerWindow()
}

// FailExhaustedWindows marks every window left `in_progress` past its locked_until that has
// already reached maxAttempts as failed, so it stops being retried and the watermark can
// advance past it. It returns how many were failed, for the caller to alert on.
func (a *AirflowSyncStateRepository) FailExhaustedWindows(ctx context.Context, projectName tenant.ProjectName, maxAttempts int, lastError string) (int64, error) {
	query := `
		UPDATE airflow_sync_state
		SET status = $1, last_error = $2, updated_at = now()
		WHERE project_name = $3 AND status = $4 AND locked_until < now() AND attempt_count >= $5`

	tag, err := a.db.Exec(ctx, query, scheduler.AirflowSyncFailed, lastError, projectName, scheduler.AirflowSyncInProgress, maxAttempts)
	if err != nil {
		return 0, errors.Wrap(scheduler.EntityAirflowSync, "error failing exhausted airflow sync windows", err)
	}
	return tag.RowsAffected(), nil
}

// CompleteWindow marks a window success, fenced on worker_id + an unexpired lock so a
// worker that lost its lease mid-window (reclaimed by another replica) cannot overwrite
// that replica's progress. 0 rows affected means exactly that happened; the caller should
// discard its progress rather than treat it as an error.
func (a *AirflowSyncStateRepository) CompleteWindow(ctx context.Context, id, workerID uuid.UUID, maxProcessedLogID *int64, eventsMatched, runsReconciled int) (bool, error) {
	query := `
		UPDATE airflow_sync_state
		SET status = $1, max_processed_log_id = $2, events_matched = $3, runs_reconciled = $4, updated_at = now()
		WHERE id = $5 AND worker_id = $6 AND locked_until > now()`

	tag, err := a.db.Exec(ctx, query, scheduler.AirflowSyncSuccess, maxProcessedLogID, eventsMatched, runsReconciled, id, workerID)
	if err != nil {
		return false, errors.Wrap(scheduler.EntityAirflowSync, "error completing airflow sync window", err)
	}
	return tag.RowsAffected() == 1, nil
}

// RecordAttemptError annotates a window with the error from its most recent attempt,
// without changing its status/lock -- ReclaimStaleWindow/FailExhaustedWindows own those
// transitions. Fenced the same way as CompleteWindow so a lease-losing worker cannot stamp
// a stale error over a newer owner's progress.
func (a *AirflowSyncStateRepository) RecordAttemptError(ctx context.Context, id, workerID uuid.UUID, lastError string) error {
	query := `UPDATE airflow_sync_state SET last_error = $1, updated_at = now() WHERE id = $2 AND worker_id = $3 AND locked_until > now()`

	_, err := a.db.Exec(ctx, query, lastError, id, workerID)
	return errors.WrapIfErr(scheduler.EntityAirflowSync, "error recording airflow sync attempt error", err)
}

type airflowSyncStateRow struct {
	ID           uuid.UUID
	ProjectName  string
	StartTime    time.Time
	EndTime      time.Time
	Status       string
	AttemptCount int
	LastError    *string
	WorkerID     uuid.UUID
	LockedUntil  time.Time
}

func (w airflowSyncStateRow) toSchedulerWindow() (*scheduler.AirflowSyncWindow, error) {
	projectName, err := tenant.ProjectNameFrom(w.ProjectName)
	if err != nil {
		return nil, err
	}
	lastError := ""
	if w.LastError != nil {
		lastError = *w.LastError
	}
	return &scheduler.AirflowSyncWindow{
		ID:           w.ID,
		ProjectName:  projectName,
		StartTime:    w.StartTime,
		EndTime:      w.EndTime,
		Status:       scheduler.AirflowSyncStatus(w.Status),
		AttemptCount: w.AttemptCount,
		LastError:    lastError,
		WorkerID:     w.WorkerID,
		LockedUntil:  w.LockedUntil,
	}, nil
}
