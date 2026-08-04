//go:build !unit_test

package scheduler_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/goto/optimus/core/scheduler"
	"github.com/goto/optimus/core/tenant"
	"github.com/goto/optimus/internal/errors"
	"github.com/goto/optimus/internal/lib/interval"
	postgres "github.com/goto/optimus/internal/store/postgres/scheduler"
)

func TestPostgresOperatorRunTriggerSource(t *testing.T) {
	ctx := context.Background()
	tnnt, _ := tenant.NewTenant("test-proj", "test-ns")
	currentTime := time.Now().UTC()
	scheduledAt := currentTime.Add(-time.Hour)
	slaDefinitionInSec := int64(3600)
	start := currentTime.Truncate(time.Hour * 24)
	intr := interval.NewInterval(start, start.Add(time.Hour*24))

	// newTaskRun creates a job run plus one task run and returns both ids, since a trigger source
	// row is only meaningful against a real operator run.
	newTaskRun := func(t *testing.T, db *pgxpool.Pool) (jobRunID, taskRunID uuid.UUID) {
		t.Helper()
		jobRunRepo := postgres.NewJobRunRepository(db, nil)
		require.NoError(t, jobRunRepo.Create(ctx, tnnt, jobAName, scheduledAt, intr, slaDefinitionInSec))

		jobRun, err := jobRunRepo.GetByScheduledAt(ctx, tnnt, jobAName, scheduledAt)
		require.NoError(t, err)

		operatorRunRepo := postgres.NewOperatorRunRepository(db)
		taskRunID, err = operatorRunRepo.CreateOperatorRun(ctx, "a-task", scheduler.OperatorTask, jobRun.ID,
			currentTime, scheduler.ScheduledAttribution(), 1)
		require.NoError(t, err)
		return jobRun.ID, taskRunID
	}

	t.Run("InsertTriggerSource", func(t *testing.T) {
		t.Run("records a manual run awaiting resolution and reads it back", func(t *testing.T) {
			db := dbSetup()
			_ = addJobs(ctx, t, db)
			jobRunID, taskRunID := newTaskRun(t, db)

			repo := postgres.NewOperatorRunRepository(db)
			id, err := repo.InsertTriggerSource(ctx, &scheduler.TriggerSource{
				OperatorRunID:  taskRunID,
				OperatorType:   scheduler.OperatorTask,
				JobRunID:       jobRunID,
				SchedulerRunID: "manual__2026-07-20T13:00:00+00:00",
				Attribution: scheduler.RunAttribution{
					RunType:     scheduler.RunTypeManual,
					TriggeredBy: scheduler.TriggeredByUnidentified,
					SourceType:  scheduler.SourceTypeManual,
					Attribution: scheduler.AttributionPending,
				},
			})
			require.NoError(t, err)
			assert.NotEqual(t, uuid.Nil, id)

			stored, err := repo.GetTriggerSourceByOperatorRunID(ctx, taskRunID)
			require.NoError(t, err)
			assert.Equal(t, taskRunID, stored.OperatorRunID)
			assert.Equal(t, scheduler.OperatorTask, stored.OperatorType)
			assert.Equal(t, jobRunID, stored.JobRunID)
			assert.Equal(t, "manual__2026-07-20T13:00:00+00:00", stored.SchedulerRunID)
			assert.Equal(t, scheduler.AttributionPending, stored.Attribution.Attribution)
			assert.Nil(t, stored.Attribution.ReplayID)
			assert.Nil(t, stored.Attribution.BackfillID)
			// Absent audit detail must round trip as empty, not as an empty JSON string.
			assert.Empty(t, stored.Attribution.AuditEvent)
			assert.Nil(t, stored.Attribution.AuditEventID)
		})

		t.Run("is idempotent for the same operator run", func(t *testing.T) {
			// A duplicated start event must update the existing row rather than add a second,
			// which is what the unique index on operator_run_id enforces.
			db := dbSetup()
			_ = addJobs(ctx, t, db)
			jobRunID, taskRunID := newTaskRun(t, db)

			repo := postgres.NewOperatorRunRepository(db)
			src := &scheduler.TriggerSource{
				OperatorRunID: taskRunID,
				OperatorType:  scheduler.OperatorTask,
				JobRunID:      jobRunID,
				Attribution: scheduler.RunAttribution{
					RunType:     scheduler.RunTypeManual,
					TriggeredBy: scheduler.TriggeredByUnidentified,
					SourceType:  scheduler.SourceTypeManual,
					Attribution: scheduler.AttributionPending,
				},
			}
			first, err := repo.InsertTriggerSource(ctx, src)
			require.NoError(t, err)

			src.Attribution.TriggeredBy = "dave"
			src.Attribution.Attribution = scheduler.AttributionAuditRunID
			second, err := repo.InsertTriggerSource(ctx, src)
			require.NoError(t, err)

			assert.Equal(t, first, second, "the same operator run must keep one trigger source row")
			stored, err := repo.GetTriggerSourceByOperatorRunID(ctx, taskRunID)
			require.NoError(t, err)
			assert.Equal(t, "dave", stored.Attribution.TriggeredBy)
		})

		t.Run("links a replay request", func(t *testing.T) {
			db := dbSetup()
			_ = addJobs(ctx, t, db)
			jobRunID, taskRunID := newTaskRun(t, db)

			replayRepo := postgres.NewReplayRepository(db)
			replayConfig := scheduler.NewReplayConfig(scheduledAt.Add(-time.Hour), scheduledAt.Add(time.Hour),
				true, map[string]string{}, "attribution test", "", "approval_id", "user_id")
			replayID, err := replayRepo.RegisterReplay(ctx,
				scheduler.NewReplayRequest(jobAName, tnnt, replayConfig, scheduler.ReplayStateCreated),
				[]*scheduler.JobRunStatus{{ScheduledAt: scheduledAt, State: scheduler.StatePending}})
			require.NoError(t, err)

			repo := postgres.NewOperatorRunRepository(db)
			_, err = repo.InsertTriggerSource(ctx, &scheduler.TriggerSource{
				OperatorRunID: taskRunID,
				OperatorType:  scheduler.OperatorTask,
				JobRunID:      jobRunID,
				Attribution: scheduler.RunAttribution{
					RunType:     scheduler.RunTypeReplay,
					TriggeredBy: "user_id",
					SourceType:  scheduler.SourceTypeReplay,
					Attribution: scheduler.AttributionOptimusReplay,
					ReplayID:    &replayID,
				},
			})
			require.NoError(t, err)

			stored, err := repo.GetTriggerSourceByOperatorRunID(ctx, taskRunID)
			require.NoError(t, err)
			require.NotNil(t, stored.Attribution.ReplayID)
			assert.Equal(t, replayID, *stored.Attribution.ReplayID)
		})
	})

	t.Run("UpdateTriggerSourceResolution", func(t *testing.T) {
		t.Run("replaces a pending row with the resolved actor", func(t *testing.T) {
			db := dbSetup()
			_ = addJobs(ctx, t, db)
			jobRunID, taskRunID := newTaskRun(t, db)

			repo := postgres.NewOperatorRunRepository(db)
			id, err := repo.InsertTriggerSource(ctx, &scheduler.TriggerSource{
				OperatorRunID: taskRunID,
				OperatorType:  scheduler.OperatorTask,
				JobRunID:      jobRunID,
				Attribution: scheduler.RunAttribution{
					RunType:     scheduler.RunTypeManual,
					TriggeredBy: scheduler.TriggeredByUnidentified,
					SourceType:  scheduler.SourceTypeManual,
					Attribution: scheduler.AttributionPending,
				},
			})
			require.NoError(t, err)

			eventLogID := int64(42)
			require.NoError(t, repo.UpdateTriggerSourceResolution(ctx, id, scheduler.OperatorTask, taskRunID,
				scheduler.RunAttribution{
					RunType:      scheduler.RunTypeManual,
					TriggeredBy:  "dave",
					SourceType:   scheduler.SourceTypeManual,
					Attribution:  scheduler.AttributionAuditRunID,
					AuditEvent:   "dagrun_clear",
					AuditEventID: &eventLogID,
					AuditExtra:   `{"rowid": "1234"}`,
				}, 2))

			stored, err := repo.GetTriggerSourceByOperatorRunID(ctx, taskRunID)
			require.NoError(t, err)
			assert.Equal(t, "dave", stored.Attribution.TriggeredBy)
			assert.Equal(t, scheduler.AttributionAuditRunID, stored.Attribution.Attribution)
			assert.Equal(t, "dagrun_clear", stored.Attribution.AuditEvent)
			require.NotNil(t, stored.Attribution.AuditEventID)
			assert.Equal(t, eventLogID, *stored.Attribution.AuditEventID)
			assert.JSONEq(t, `{"rowid": "1234"}`, stored.Attribution.AuditExtra)
			assert.Equal(t, 2, stored.ResolveAttempts)

			// The whole point of merging this into the operator repository: task_run must carry the
			// same verdict, written in the same transaction, since that is what consumers read.
			operatorRun, err := repo.GetOperatorRun(ctx, "a-task", scheduler.OperatorTask, jobRunID)
			require.NoError(t, err)
			assert.Equal(t, scheduler.RunTypeManual, operatorRun.RunType)
			assert.Equal(t, "dave", operatorRun.TriggeredBy)
		})

		t.Run("leaves both tables untouched when the operator type is invalid", func(t *testing.T) {
			// Proves the two writes are atomic rather than sequential: a failure must not leave the
			// trigger source naming an actor while task_run still says unidentified.
			db := dbSetup()
			_ = addJobs(ctx, t, db)
			jobRunID, taskRunID := newTaskRun(t, db)

			repo := postgres.NewOperatorRunRepository(db)
			id, err := repo.InsertTriggerSource(ctx, &scheduler.TriggerSource{
				OperatorRunID: taskRunID,
				OperatorType:  scheduler.OperatorTask,
				JobRunID:      jobRunID,
				Attribution: scheduler.RunAttribution{
					RunType:     scheduler.RunTypeManual,
					TriggeredBy: scheduler.TriggeredByUnidentified,
					SourceType:  scheduler.SourceTypeManual,
					Attribution: scheduler.AttributionPending,
				},
			})
			require.NoError(t, err)

			err = repo.UpdateTriggerSourceResolution(ctx, id, scheduler.OperatorType("bogus"), taskRunID,
				scheduler.RunAttribution{
					RunType:     scheduler.RunTypeManual,
					TriggeredBy: "dave",
					SourceType:  scheduler.SourceTypeManual,
					Attribution: scheduler.AttributionAuditRunID,
				}, 1)
			assert.Error(t, err)

			stored, err := repo.GetTriggerSourceByOperatorRunID(ctx, taskRunID)
			require.NoError(t, err)
			assert.Equal(t, scheduler.TriggeredByUnidentified, stored.Attribution.TriggeredBy)
			assert.Equal(t, scheduler.AttributionPending, stored.Attribution.Attribution)
		})
	})

	t.Run("GetTriggerSourceByOperatorRunID", func(t *testing.T) {
		t.Run("reports not found for a scheduled run", func(t *testing.T) {
			// Scheduled runs deliberately have no trigger source row.
			db := dbSetup()
			_ = addJobs(ctx, t, db)
			_, taskRunID := newTaskRun(t, db)

			repo := postgres.NewOperatorRunRepository(db)
			_, err := repo.GetTriggerSourceByOperatorRunID(ctx, taskRunID)
			assert.True(t, errors.IsErrorType(err, errors.ErrNotFound))
		})
	})

	t.Run("CountPendingTriggerSourcesSince", func(t *testing.T) {
		t.Run("counts only rows that never resolved", func(t *testing.T) {
			db := dbSetup()
			_ = addJobs(ctx, t, db)
			jobRunID, taskRunID := newTaskRun(t, db)

			repo := postgres.NewOperatorRunRepository(db)
			id, err := repo.InsertTriggerSource(ctx, &scheduler.TriggerSource{
				OperatorRunID: taskRunID,
				OperatorType:  scheduler.OperatorTask,
				JobRunID:      jobRunID,
				Attribution: scheduler.RunAttribution{
					RunType:     scheduler.RunTypeManual,
					TriggeredBy: scheduler.TriggeredByUnidentified,
					SourceType:  scheduler.SourceTypeManual,
					Attribution: scheduler.AttributionPending,
				},
			})
			require.NoError(t, err)

			since := currentTime.Add(-time.Hour)
			count, err := repo.CountPendingTriggerSourcesSince(ctx, since)
			require.NoError(t, err)
			assert.Equal(t, 1, count)

			require.NoError(t, repo.UpdateTriggerSourceResolution(ctx, id, scheduler.OperatorTask, taskRunID,
				scheduler.RunAttribution{
					RunType:     scheduler.RunTypeManual,
					TriggeredBy: "dave",
					SourceType:  scheduler.SourceTypeManual,
					Attribution: scheduler.AttributionAuditRunID,
				}, 1))

			count, err = repo.CountPendingTriggerSourcesSince(ctx, since)
			require.NoError(t, err)
			assert.Zero(t, count)
		})
	})

	t.Run("GetReplayAttributionByScheduledAt", func(t *testing.T) {
		registerReplay := func(t *testing.T, db *pgxpool.Pool, state scheduler.ReplayState, userID string) uuid.UUID {
			t.Helper()
			replayRepo := postgres.NewReplayRepository(db)
			replayConfig := scheduler.NewReplayConfig(scheduledAt.Add(-time.Hour), scheduledAt.Add(time.Hour),
				true, map[string]string{}, "attribution test", "", "approval_"+userID, userID)
			replayID, err := replayRepo.RegisterReplay(ctx,
				scheduler.NewReplayRequest(jobAName, tnnt, replayConfig, state),
				[]*scheduler.JobRunStatus{{ScheduledAt: scheduledAt, State: scheduler.StatePending}})
			require.NoError(t, err)
			return replayID
		}

		t.Run("finds an in-flight replay covering the scheduled time", func(t *testing.T) {
			db := dbSetup()
			_ = addJobs(ctx, t, db)
			replayID := registerReplay(t, db, scheduler.ReplayStateInProgress, "alice")

			replayRepo := postgres.NewReplayRepository(db)
			gotID, gotUser, err := replayRepo.GetReplayAttributionByScheduledAt(ctx, tnnt, jobAName, scheduledAt)
			require.NoError(t, err)
			assert.Equal(t, replayID, gotID)
			assert.Equal(t, "alice", gotUser)
		})

		t.Run("finds a replay that has been created but not yet started", func(t *testing.T) {
			// ReplayWorker only moves the request to 'in progress' on its first loop iteration,
			// so a run must still be attributable while the request is merely 'created'.
			db := dbSetup()
			_ = addJobs(ctx, t, db)
			replayID := registerReplay(t, db, scheduler.ReplayStateCreated, "bob")

			replayRepo := postgres.NewReplayRepository(db)
			gotID, gotUser, err := replayRepo.GetReplayAttributionByScheduledAt(ctx, tnnt, jobAName, scheduledAt)
			require.NoError(t, err)
			assert.Equal(t, replayID, gotID)
			assert.Equal(t, "bob", gotUser)
		})

		t.Run("ignores a finished replay so a later scheduled run is not blamed on it", func(t *testing.T) {
			db := dbSetup()
			_ = addJobs(ctx, t, db)
			replayID := registerReplay(t, db, scheduler.ReplayStateCreated, "carol")

			replayRepo := postgres.NewReplayRepository(db)
			require.NoError(t, replayRepo.UpdateReplayStatus(ctx, replayID, scheduler.ReplayStateSuccess, "done"))

			_, _, err := replayRepo.GetReplayAttributionByScheduledAt(ctx, tnnt, jobAName, scheduledAt)
			assert.True(t, errors.IsErrorType(err, errors.ErrNotFound))
		})

		t.Run("ignores a replay whose window excludes the scheduled time", func(t *testing.T) {
			db := dbSetup()
			_ = addJobs(ctx, t, db)
			_ = registerReplay(t, db, scheduler.ReplayStateInProgress, "dave")

			replayRepo := postgres.NewReplayRepository(db)
			outside := scheduledAt.Add(48 * time.Hour)
			_, _, err := replayRepo.GetReplayAttributionByScheduledAt(ctx, tnnt, jobAName, outside)
			assert.True(t, errors.IsErrorType(err, errors.ErrNotFound))
		})
	})
}
