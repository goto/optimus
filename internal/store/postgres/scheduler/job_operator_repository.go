package scheduler

import (
	"context"
	"database/sql"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/goto/optimus/core/scheduler"
	"github.com/goto/optimus/core/scheduler/rootcause"
	"github.com/goto/optimus/core/tenant"
	"github.com/goto/optimus/internal/errors"
)

const (
	sensorRunTableName = "sensor_run"
	taskRunTableName   = "task_run"
	hookRunTableName   = "hook_run"

	jobOperatorColumnsToStore = `name, job_run_id, status, start_time, end_time`
	jobOperatorColumns        = `id, ` + jobOperatorColumnsToStore
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
		UpdatedAt:    o.UpdatedAt,
	}, nil
}

func (o *OperatorRunRepository) GetOperatorRun(ctx context.Context, name string, operatorType scheduler.OperatorType, jobRunID uuid.UUID) (*scheduler.OperatorRun, error) {
	var opRun operatorRun
	operatorTableName, err := operatorTypeToTableName(operatorType)
	if err != nil {
		return nil, err
	}
	getJobRunByID := "SELECT " + jobOperatorColumns + ", updated_at FROM " + operatorTableName + " j where job_run_id = $1 and name = $2 order by created_at desc limit 1"
	err = o.db.QueryRow(ctx, getJobRunByID, jobRunID, name).
		Scan(&opRun.ID, &opRun.Name, &opRun.JobRunID, &opRun.Status, &opRun.StartTime, &opRun.EndTime, &opRun.UpdatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, errors.NotFound(scheduler.EntityJobRun, "no record for "+operatorType.String()+"/"+name+" for job_run ID: "+jobRunID.String())
		}
		return nil, errors.Wrap(scheduler.EntityJobRun, "error while getting operator run", err)
	}
	return opRun.toOperatorRun()
}

// ListLatestOperatorRunsByJobRunID returns the newest row per distinct operator name for the
// given job_run_id and operator type. Used by the airflow-sync reconciler to mirror
// Airflow's own dagrun-level cascade (only non-terminal children get overwritten), which
// needs to see every child's *current* status first -- unlike GetOperatorRun, which only
// looks up one already-known name.
func (o *OperatorRunRepository) ListLatestOperatorRunsByJobRunID(ctx context.Context, operatorType scheduler.OperatorType, jobRunID uuid.UUID) ([]*scheduler.OperatorRun, error) {
	operatorTableName, err := operatorTypeToTableName(operatorType)
	if err != nil {
		return nil, err
	}
	query := "SELECT DISTINCT ON (name) " + jobOperatorColumns + ", updated_at FROM " + operatorTableName + " WHERE job_run_id = $1 ORDER BY name, created_at DESC"
	rows, err := o.db.Query(ctx, query, jobRunID)
	if err != nil {
		return nil, errors.Wrap(scheduler.EntityJobRun, "error while listing operator runs", err)
	}
	defer rows.Close()

	var operatorRuns []*scheduler.OperatorRun
	for rows.Next() {
		var opRun operatorRun
		if err := rows.Scan(&opRun.ID, &opRun.Name, &opRun.JobRunID, &opRun.Status, &opRun.StartTime, &opRun.EndTime, &opRun.UpdatedAt); err != nil {
			return nil, errors.Wrap(scheduler.EntityJobRun, "error scanning operator run", err)
		}
		opRun.OperatorType = operatorType.String()
		schedulerOpRun, err := opRun.toOperatorRun()
		if err != nil {
			return nil, err
		}
		operatorRuns = append(operatorRuns, schedulerOpRun)
	}
	return operatorRuns, nil
}

func (o *OperatorRunRepository) CreateOperatorRun(ctx context.Context, name string, operatorType scheduler.OperatorType, jobRunID uuid.UUID, startTime time.Time) error {
	operatorTableName, err := operatorTypeToTableName(operatorType)
	if err != nil {
		return err
	}
	insertOperatorRun := "INSERT INTO " + operatorTableName + " ( " + jobOperatorColumnsToStore + ", created_at, updated_at) values ( $1, $2, $3, $4, null, NOW(), NOW())"
	_, err = o.db.Exec(ctx, insertOperatorRun, name, jobRunID, scheduler.StateRunning, startTime)
	if err != nil {
		return errors.WrapIfErr(scheduler.EntityJobRun, "error while inserting the run", err)
	}
	return nil
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

// GetPendingSensors returns, per requested run, the sensor task names that have not
// finished. Retries insert a fresh sensor_run row rather than updating, so only the
// newest row per name decides whether that sensor is still waiting.
func (o *OperatorRunRepository) GetPendingSensors(ctx context.Context, keys []rootcause.JobRunKey) (map[string][]string, error) {
	if len(keys) == 0 {
		return map[string][]string{}, nil
	}

	projects := make([]string, len(keys))
	jobNames := make([]string, len(keys))
	scheduledAts := make([]time.Time, len(keys))
	for idx, key := range keys {
		projects[idx] = key.Project.String()
		jobNames[idx] = key.JobName.String()
		scheduledAts[idx] = key.ScheduledAt.UTC()
	}

	query := `
WITH targets AS (
	SELECT * FROM unnest($1::text[], $2::text[], $3::timestamptz[]) AS t(project_name, job_name, scheduled_at)
), runs AS (
	SELECT jr.id, jr.project_name, jr.job_name, jr.scheduled_at
	FROM job_run jr
	JOIN targets t ON t.project_name = jr.project_name
		AND t.job_name = jr.job_name
		AND t.scheduled_at = jr.scheduled_at
), latest_sensor AS (
	SELECT DISTINCT ON (sr.job_run_id, sr.name) sr.job_run_id, sr.name, sr.end_time
	FROM sensor_run sr
	WHERE sr.job_run_id IN (SELECT id FROM runs)
	ORDER BY sr.job_run_id, sr.name, sr.created_at DESC
)
SELECT r.project_name, r.job_name, r.scheduled_at, ls.name
FROM runs r
JOIN latest_sensor ls ON ls.job_run_id = r.id
WHERE ls.end_time IS NULL
ORDER BY r.project_name, r.job_name, r.scheduled_at, ls.name`

	rows, err := o.db.Query(ctx, query, projects, jobNames, scheduledAts)
	if err != nil {
		return nil, errors.Wrap(scheduler.EntityJobRun, "error while getting pending sensors", err)
	}
	defer rows.Close()

	pending := map[string][]string{}
	for rows.Next() {
		var projectName, jobName, sensorName string
		var scheduledAt time.Time
		if err := rows.Scan(&projectName, &jobName, &scheduledAt, &sensorName); err != nil {
			return nil, errors.Wrap(scheduler.EntityJobRun, "error scanning pending sensor", err)
		}
		key := rootcause.JobRunKey{
			Project:     tenant.ProjectName(projectName),
			JobName:     scheduler.JobName(jobName),
			ScheduledAt: scheduledAt,
		}
		pending[key.String()] = append(pending[key.String()], sensorName)
	}
	if err := rows.Err(); err != nil {
		return nil, errors.Wrap(scheduler.EntityJobRun, "error iterating pending sensors", err)
	}
	return pending, nil
}

// listOperatorRunsByJobRun resolves the job run by (project, job, scheduled_at)
// and unions its sensor, task and hook runs in a single round trip. A sentinel
// row (is_job_run = true) carries the job run's existence so the caller can
// still tell "run not found" apart from "run has no operator runs yet". Every
// stored row is returned, so retry attempts appear as separate entries; the
// ordering is newest start_time first, with id as a tie-breaker so the order is
// total and stable.
const listOperatorRunsByJobRun = `
WITH jr AS (
	SELECT id FROM job_run
	WHERE project_name = $1 AND job_name = $2 AND scheduled_at = $3
	ORDER BY created_at DESC
	LIMIT 1
)
SELECT is_job_run, operator_type, id, name, status, start_time, end_time
FROM (
	SELECT true AS is_job_run, 'job_run' AS operator_type, id, ''::text AS name, ''::text AS status,
		NULL::timestamptz AS start_time, NULL::timestamptz AS end_time
	FROM jr
	UNION ALL
	SELECT false, 'sensor', s.id, s.name, s.status, s.start_time, s.end_time FROM sensor_run s
	WHERE s.job_run_id IN (SELECT id FROM jr)
	UNION ALL
	SELECT false, 'task', t.id, t.name, t.status, t.start_time, t.end_time FROM task_run t
	WHERE t.job_run_id IN (SELECT id FROM jr)
	UNION ALL
	SELECT false, 'hook', h.id, h.name, h.status, h.start_time, h.end_time FROM hook_run h
	WHERE h.job_run_id IN (SELECT id FROM jr)
) all_rows
ORDER BY is_job_run DESC, start_time DESC NULLS LAST, id DESC NULLS LAST`

func (o *OperatorRunRepository) ListOperatorRunsByJobRun(ctx context.Context, projectName tenant.ProjectName, jobName scheduler.JobName, scheduledAt time.Time) ([]*scheduler.OperatorRun, error) {
	rows, err := o.db.Query(ctx, listOperatorRunsByJobRun, projectName, jobName, scheduledAt)
	if err != nil {
		return nil, errors.Wrap(scheduler.EntityJobRun, "error while listing operator runs", err)
	}
	defer rows.Close()

	var (
		jobRunID     uuid.UUID
		jobRunFound  bool
		operatorRuns []*scheduler.OperatorRun
	)
	for rows.Next() {
		var (
			isJobRun  bool
			startTime sql.NullTime
			endTime   sql.NullTime
			opRun     operatorRun
		)
		if err := rows.Scan(&isJobRun, &opRun.OperatorType, &opRun.ID, &opRun.Name, &opRun.Status, &startTime, &endTime); err != nil {
			return nil, errors.Wrap(scheduler.EntityJobRun, "error scanning operator run", err)
		}
		if isJobRun {
			jobRunFound = true
			jobRunID = opRun.ID
			continue
		}
		opRun.JobRunID = jobRunID
		opRun.StartTime = startTime.Time
		if endTime.Valid {
			opRun.EndTime = &endTime.Time
		}
		schedulerOpRun, err := opRun.toOperatorRun()
		if err != nil {
			return nil, err
		}
		operatorRuns = append(operatorRuns, schedulerOpRun)
	}
	if err := rows.Err(); err != nil {
		return nil, errors.Wrap(scheduler.EntityJobRun, "error iterating operator runs", err)
	}
	if !jobRunFound {
		return nil, errors.NotFound(scheduler.EntityJobRun, "no job run for "+jobName.String()+" scheduled at "+scheduledAt.String()+" in project "+projectName.String())
	}
	return operatorRuns, nil
}

func NewOperatorRunRepository(pool *pgxpool.Pool) *OperatorRunRepository {
	return &OperatorRunRepository{
		db: pool,
	}
}
