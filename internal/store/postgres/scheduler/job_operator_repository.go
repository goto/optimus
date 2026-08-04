package scheduler

import (
	"context"
	"database/sql"
	"encoding/json"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/goto/optimus/core/scheduler"
	"github.com/goto/optimus/internal/errors"
)

const (
	sensorRunTableName = "sensor_run"
	taskRunTableName   = "task_run"
	hookRunTableName   = "hook_run"

	jobOperatorColumnsToStore = `name, job_run_id, status, start_time, end_time, run_type, triggered_by, attempt`
	jobOperatorColumns        = `id, ` + jobOperatorColumnsToStore

	triggerSourceColumnsToStore = `operator_run_id, operator_type, job_run_id, scheduler_run_id, source_type, ` +
		`replay_id, backfill_id, triggered_by, attribution, resolve_attempts, audit_event, audit_event_id, audit_extra`
	triggerSourceColumns = `id, ` + triggerSourceColumnsToStore
)

type OperatorRunRepository struct {
	db *pgxpool.Pool
}

type operatorRun struct {
	ID       uuid.UUID
	JobRunID uuid.UUID

	Name         string
	OperatorType string
	Status       string

	StartTime time.Time
	EndTime   *time.Time

	RunType     string
	TriggeredBy string
	Attempt     int

	CreatedAt time.Time
	UpdatedAt time.Time
	// TODO:  add a remarks colum to capture failure reason
	DeletedAt sql.NullTime
}

func operatorTypeToTableName(operatorType scheduler.OperatorType) (string, error) {
	switch operatorType {
	case scheduler.OperatorSensor:
		return sensorRunTableName, nil
	case scheduler.OperatorHook:
		return hookRunTableName, nil
	case scheduler.OperatorTask:
		return taskRunTableName, nil
	default:
		return "", errors.InvalidArgument(scheduler.EntityJobRun, "invalid operator Type:"+operatorType.String())
	}
}

func (o *operatorRun) toOperatorRun() (*scheduler.OperatorRun, error) {
	status, err := scheduler.StateFromString(o.Status)
	if err != nil {
		return nil, errors.NewError(scheduler.EntityJobRun, "invalid operator run state in database", err.Error())
	}
	return &scheduler.OperatorRun{
		ID:           o.ID,
		JobRunID:     o.JobRunID,
		Name:         o.Name,
		OperatorType: scheduler.OperatorType(o.OperatorType),
		Status:       status,
		StartTime:    o.StartTime,
		EndTime:      o.EndTime,
		RunType:      scheduler.RunType(o.RunType),
		TriggeredBy:  o.TriggeredBy,
		Attempt:      o.Attempt,
	}, nil
}

func (o *OperatorRunRepository) GetOperatorRun(ctx context.Context, name string, operatorType scheduler.OperatorType, jobRunID uuid.UUID) (*scheduler.OperatorRun, error) {
	var opRun operatorRun
	operatorTableName, err := operatorTypeToTableName(operatorType)
	if err != nil {
		return nil, err
	}
	getJobRunByID := "SELECT " + jobOperatorColumns + " FROM " + operatorTableName + " j where job_run_id = $1 and name = $2 order by created_at desc limit 1"
	err = o.db.QueryRow(ctx, getJobRunByID, jobRunID, name).
		Scan(&opRun.ID, &opRun.Name, &opRun.JobRunID, &opRun.Status, &opRun.StartTime, &opRun.EndTime,
			&opRun.RunType, &opRun.TriggeredBy, &opRun.Attempt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, errors.NotFound(scheduler.EntityJobRun, "no record for "+operatorType.String()+"/"+name+" for job_run ID: "+jobRunID.String())
		}
		return nil, errors.Wrap(scheduler.EntityJobRun, "error while getting operator run", err)
	}
	return opRun.toOperatorRun()
}

// CreateOperatorRun inserts a new attempt row and returns its id, which the caller needs in
// order to link the run back to whatever caused it.
func (o *OperatorRunRepository) CreateOperatorRun(ctx context.Context, name string, operatorType scheduler.OperatorType, jobRunID uuid.UUID, startTime time.Time, attribution scheduler.RunAttribution, attempt int) (uuid.UUID, error) {
	operatorTableName, err := operatorTypeToTableName(operatorType)
	if err != nil {
		return uuid.Nil, err
	}
	if attribution.RunType == "" {
		attribution = scheduler.ScheduledAttribution()
	}
	if attempt < 1 {
		// Airflow's try_number is 1-based; treat a missing value as the first attempt rather
		// than storing a 0 that no Airflow payload would produce.
		attempt = 1
	}
	insertOperatorRun := "INSERT INTO " + operatorTableName + " ( " + jobOperatorColumnsToStore +
		", created_at, updated_at) values ( $1, $2, $3, $4, null, $5, $6, $7, NOW(), NOW()) RETURNING id"
	var operatorRunID uuid.UUID
	err = o.db.QueryRow(ctx, insertOperatorRun, name, jobRunID, scheduler.StateRunning, startTime,
		attribution.RunType.String(), attribution.TriggeredBy, attempt).Scan(&operatorRunID)
	if err != nil {
		return uuid.Nil, errors.Wrap(scheduler.EntityJobRun, "error while inserting the run", err)
	}
	return operatorRunID, nil
}

func (o *OperatorRunRepository) UpdateOperatorRun(ctx context.Context, operatorType scheduler.OperatorType, operatorRunID uuid.UUID, eventTime time.Time, state scheduler.State) error {
	operatorTableName, err := operatorTypeToTableName(operatorType)
	if err != nil {
		return err
	}
	updateJobRun := "UPDATE " + operatorTableName + " SET status = $1, end_time = $2, updated_at = NOW() where id = $3"
	_, err = o.db.Exec(ctx, updateJobRun, state, eventTime, operatorRunID)
	return errors.WrapIfErr(scheduler.EntityJobRun, "error while updating the run", err)
}

// -- operator_run_trigger_source -------------------------------------------------------------
//
// A trigger source records why an operator run happened: an Optimus replay_request, an Optimus
// backfill, or a manual action taken directly in Airflow. It lives on this repository rather
// than its own because it is 1:1 with an operator run, and because resolving one has to update
// both tables together (see UpdateTriggerSourceResolution).

type triggerSource struct {
	ID uuid.UUID

	OperatorRunID  uuid.UUID
	OperatorType   string
	JobRunID       uuid.UUID
	SchedulerRunID *string

	SourceType  string
	ReplayID    *uuid.UUID
	BackfillID  *uuid.UUID
	TriggeredBy string

	Attribution     string
	ResolveAttempts int

	AuditEvent   *string
	AuditEventID *int64
	// AuditExtra is a jsonb column, so it has to be scanned as raw bytes rather than a string.
	AuditExtra []byte
}

func (t *triggerSource) toTriggerSource() *scheduler.TriggerSource {
	src := &scheduler.TriggerSource{
		ID:              t.ID,
		OperatorRunID:   t.OperatorRunID,
		OperatorType:    scheduler.OperatorType(t.OperatorType),
		JobRunID:        t.JobRunID,
		ResolveAttempts: t.ResolveAttempts,
		Attribution: scheduler.RunAttribution{
			SourceType:   t.SourceType,
			ReplayID:     t.ReplayID,
			BackfillID:   t.BackfillID,
			TriggeredBy:  t.TriggeredBy,
			Attribution:  t.Attribution,
			AuditEventID: t.AuditEventID,
		},
	}
	if t.SchedulerRunID != nil {
		src.SchedulerRunID = *t.SchedulerRunID
	}
	if t.AuditEvent != nil {
		src.Attribution.AuditEvent = *t.AuditEvent
	}
	if len(t.AuditExtra) > 0 {
		src.Attribution.AuditExtra = string(t.AuditExtra)
	}
	return src
}

// nullIfEmpty keeps genuinely absent values out of the table as NULL rather than as empty
// strings, so that "we have no audit event" is distinguishable from "the event name was blank".
func nullIfEmpty(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

// toJSONB prepares a value for the audit_extra jsonb column.
//
// Airflow's own `extra` field is only conventionally JSON: it is whatever json.dumps produced
// for that request, and nothing guarantees it parses. Rather than let a malformed value fail the
// insert and lose the whole trigger source row, anything unparseable is wrapped so it is still
// preserved and still queryable.
func toJSONB(s string) []byte {
	if s == "" {
		return nil
	}
	if json.Valid([]byte(s)) {
		return []byte(s)
	}
	wrapped, err := json.Marshal(map[string]string{"raw": s})
	if err != nil {
		return nil
	}
	return wrapped
}

// InsertTriggerSource records the cause of an operator run. Scheduled runs are not written here;
// only replay, backfill and manual runs get a row.
//
// The unique index on operator_run_id makes this idempotent: a duplicated start event that
// somehow reaches the same operator run updates the existing row instead of adding a second.
func (o *OperatorRunRepository) InsertTriggerSource(ctx context.Context, src *scheduler.TriggerSource) (uuid.UUID, error) {
	insert := `INSERT INTO operator_run_trigger_source (` + triggerSourceColumnsToStore + `, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, NOW(), NOW())
		ON CONFLICT (operator_run_id) DO UPDATE SET
			source_type = EXCLUDED.source_type,
			replay_id = EXCLUDED.replay_id,
			backfill_id = EXCLUDED.backfill_id,
			triggered_by = EXCLUDED.triggered_by,
			attribution = EXCLUDED.attribution,
			audit_event = EXCLUDED.audit_event,
			audit_event_id = EXCLUDED.audit_event_id,
			audit_extra = EXCLUDED.audit_extra,
			updated_at = NOW()
		RETURNING id`

	a := src.Attribution
	var id uuid.UUID
	err := o.db.QueryRow(ctx, insert,
		src.OperatorRunID, src.OperatorType.String(), src.JobRunID, nullIfEmpty(src.SchedulerRunID),
		a.SourceType, a.ReplayID, a.BackfillID, a.TriggeredBy, a.Attribution, src.ResolveAttempts,
		nullIfEmpty(a.AuditEvent), a.AuditEventID, toJSONB(a.AuditExtra),
	).Scan(&id)
	if err != nil {
		return uuid.Nil, errors.Wrap(scheduler.EntityRunAttribution, "error while inserting run trigger source", err)
	}
	return id, nil
}

// UpdateTriggerSourceResolution writes the outcome of an audit lookup to both the trigger source
// row and the operator run it belongs to, in one transaction.
//
// The two writes must be atomic: task_run and hook_run are what most consumers read, so a
// partial write would leave them claiming the actor is unidentified while the trigger source
// names somebody. Nothing reconciles the two afterwards, so it has to be all or nothing.
func (o *OperatorRunRepository) UpdateTriggerSourceResolution(ctx context.Context, triggerSourceID uuid.UUID,
	operatorType scheduler.OperatorType, operatorRunID uuid.UUID, a scheduler.RunAttribution, resolveAttempts int,
) error {
	operatorTableName, err := operatorTypeToTableName(operatorType)
	if err != nil {
		return err
	}

	tx, err := o.db.Begin(ctx)
	if err != nil {
		return errors.Wrap(scheduler.EntityRunAttribution, "error while starting resolution transaction", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // no-op once Commit has succeeded

	updateTriggerSource := `UPDATE operator_run_trigger_source SET
			source_type = $1, triggered_by = $2, attribution = $3, resolve_attempts = $4,
			audit_event = $5, audit_event_id = $6, audit_extra = $7, updated_at = NOW()
		WHERE id = $8`
	if _, err := tx.Exec(ctx, updateTriggerSource,
		a.SourceType, a.TriggeredBy, a.Attribution, resolveAttempts,
		nullIfEmpty(a.AuditEvent), a.AuditEventID, toJSONB(a.AuditExtra), triggerSourceID); err != nil {
		return errors.Wrap(scheduler.EntityRunAttribution, "error while updating run trigger source", err)
	}

	updateOperatorRun := "UPDATE " + operatorTableName + " SET run_type = $1, triggered_by = $2, updated_at = NOW() where id = $3"
	if _, err := tx.Exec(ctx, updateOperatorRun, a.RunType.String(), a.TriggeredBy, operatorRunID); err != nil {
		return errors.Wrap(scheduler.EntityRunAttribution, "error while updating run attribution", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return errors.Wrap(scheduler.EntityRunAttribution, "error while committing resolution", err)
	}
	return nil
}

// GetTriggerSourceByOperatorRunID returns the recorded cause of a single operator run.
func (o *OperatorRunRepository) GetTriggerSourceByOperatorRunID(ctx context.Context, operatorRunID uuid.UUID) (*scheduler.TriggerSource, error) {
	query := `SELECT ` + triggerSourceColumns + ` FROM operator_run_trigger_source WHERE operator_run_id = $1`
	var ts triggerSource
	err := o.db.QueryRow(ctx, query, operatorRunID).Scan(
		&ts.ID, &ts.OperatorRunID, &ts.OperatorType, &ts.JobRunID, &ts.SchedulerRunID,
		&ts.SourceType, &ts.ReplayID, &ts.BackfillID, &ts.TriggeredBy, &ts.Attribution,
		&ts.ResolveAttempts, &ts.AuditEvent, &ts.AuditEventID, &ts.AuditExtra)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, errors.NotFound(scheduler.EntityRunAttribution, "no trigger source for operator run "+operatorRunID.String())
		}
		return nil, errors.Wrap(scheduler.EntityRunAttribution, "error while getting run trigger source", err)
	}
	return ts.toTriggerSource(), nil
}

// CountPendingTriggerSourcesSince reports how many rows never reached a resolved attribution.
// Exposed for monitoring: because nothing rescans pending rows, a rising count is the signal
// that resolution is failing or being shed under load.
func (o *OperatorRunRepository) CountPendingTriggerSourcesSince(ctx context.Context, since time.Time) (int, error) {
	var count int
	query := `SELECT COUNT(1) FROM operator_run_trigger_source WHERE attribution = $1 AND created_at > $2`
	if err := o.db.QueryRow(ctx, query, scheduler.AttributionPending, since).Scan(&count); err != nil {
		return 0, errors.Wrap(scheduler.EntityRunAttribution, "error while counting pending attributions", err)
	}
	return count, nil
}

func NewOperatorRunRepository(pool *pgxpool.Pool) *OperatorRunRepository {
	return &OperatorRunRepository{
		db: pool,
	}
}
