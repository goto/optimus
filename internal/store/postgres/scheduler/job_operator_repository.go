package scheduler

import (
	"context"
	"database/sql"
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

func NewOperatorRunRepository(pool *pgxpool.Pool) *OperatorRunRepository {
	return &OperatorRunRepository{
		db: pool,
	}
}
