package scheduler

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/goto/optimus/core/scheduler"
	"github.com/goto/optimus/core/tenant"
	"github.com/goto/optimus/internal/errors"
)

type OperatorsSLA struct {
	ID           uuid.UUID
	JobName      string
	ProjectName  string
	OperatorName string
	RunID        string
	OperatorType string
	SLATime      time.Time
	AlertTag     string

	WorkerSignature string
	WorkerLockUntil time.Time

	CreatedAt time.Time
	UpdatedAt time.Time
}

func (o OperatorsSLA) toSchedulerSLAObject() (*scheduler.OperatorsSLA, error) {
	projectName, err := tenant.ProjectNameFrom(o.ProjectName)
	if err != nil {
		return nil, err
	}
	jobName, err := scheduler.JobNameFrom(o.JobName)
	if err != nil {
		return nil, err
	}

	operatorType, err := scheduler.NewOperatorType(o.OperatorType) // to validate operator type
	if err != nil {
		return nil, err
	}

	return &scheduler.OperatorsSLA{
		ID:              o.ID,
		JobName:         jobName,
		ProjectName:     projectName,
		OperatorName:    o.OperatorName,
		RunID:           o.RunID,
		OperatorType:    operatorType,
		SLATime:         o.SLATime,
		AlertTag:        o.AlertTag,
		WorkerSignature: o.WorkerSignature,
		WorkerLockUntil: o.WorkerLockUntil,
	}, nil
}

var operatorSLAColumnsToUpdate = "project_name, job_name, operator_name, operator_type, run_id, sla_time, alert_tag"
var operatorSLAColumns = operatorSLAColumnsToUpdate + ", worker_signature , worker_lock_until,  created_at, updated_at"

type SLARepository struct {
	db *pgxpool.Pool
}

func NewSLARepository(pool *pgxpool.Pool) *SLARepository {
	return &SLARepository{
		db: pool,
	}
}

<<<<<<< Updated upstream
func (s *SLARepository) RegisterSLA(ctx context.Context, projectName tenant.ProjectName, jobName, operatorName, operatorType, runID string, slaTime time.Time, alertTag string, scheduledAt, operatorStartTime time.Time) error {
	slaQuery := "INSERT INTO operator_sla ( project_name, job_name, operator_name, operator_type, run_id, sla_time, alert_tag, scheduled_at, operator_start_time) values ( $1, $2, $3, $4, $5, $6, $7, $8, $9)"
=======
func (s *SLARepository) RegisterSLA(ctx context.Context, projectName tenant.ProjectName, jobName, operatorName, operatorType, runID string, slaTime time.Time, alertTag string) error {
	slaQuery := "INSERT INTO operator_sla ( " + operatorSLAColumnsToUpdate + ") values ( $1, $2, $3, $4, $5, $6, $7)"
<<<<<<< Updated upstream
>>>>>>> Stashed changes
=======
>>>>>>> Stashed changes

	tag, err := s.db.Exec(ctx, slaQuery, projectName, jobName, operatorName, operatorType, runID, slaTime, alertTag, scheduledAt, operatorStartTime)
	if err != nil {
		errMsg := fmt.Sprintf("error executing sla insert, params: %s, %s, %s, %s, %s, %s", jobName, operatorName, operatorType, runID, slaTime, alertTag)
		return errors.Wrap(scheduler.EntityEvent, errMsg, err)
	}
	if tag.RowsAffected() == 0 {
		errMsg := fmt.Sprintf("error executing sla insert, params: %s, %s, %s, %s, %s, %s, err: now new rows created", jobName, operatorName, operatorType, runID, slaTime, alertTag)
		return errors.NewError(errors.ErrInternalError, scheduler.EntityEvent, errMsg)
	}
	return nil
}

func (s *SLARepository) UpdateSLA(ctx context.Context, projectName tenant.ProjectName, jobName, operatorName, operatorType, runID string, slaTime, operatorStartTime time.Time) error {
	slaQuery := "update operator_sla set sla_time = $1, operator_start_time = $2 where project_name = $3 and job_name = $4 and  operator_name = $5 and operator_type = $6 and  run_id = $7 "

	tag, err := s.db.Exec(ctx, slaQuery, slaTime, operatorStartTime, projectName, jobName, operatorName, operatorType, runID)
	if err != nil {
		errMsg := fmt.Sprintf("error executing sla update, params: %s:%s, %s, %s, %s", jobName, operatorName, operatorType, runID, slaTime)
		return errors.Wrap(scheduler.EntityEvent, errMsg, err)
	}
	if tag.RowsAffected() == 0 {
		errMsg := fmt.Sprintf("error executing sla update, params: %s:%s, %s, %s, %s, err: now new rows created", jobName, operatorName, operatorType, runID, slaTime)
		return errors.NewError(errors.ErrInternalError, scheduler.EntityEvent, errMsg)
	}
	return nil
}

func (s *SLARepository) FinishSLA(ctx context.Context, projectName tenant.ProjectName, jobName, operatorName, operatorType, runID string, operatorEndTime time.Time) error {
	slaQuery := "delete from operator_sla where project_name = $1 and job_name = $2 and  operator_name = $3 and operator_type = $4 and run_id = $5  and SLA_time > $6 "

	_, err := s.db.Exec(ctx, slaQuery, projectName, jobName, operatorName, operatorType, runID, operatorEndTime)
	if err != nil {
		errMsg := fmt.Sprintf("error finishing SLA, params: %s:%s operatorType:%s, runID:%s, operatorEndTime:%s", jobName, operatorName, operatorType, runID, operatorEndTime)
		return errors.Wrap(scheduler.EntityEvent, errMsg, err)
	}

	return nil
}

func (s *SLARepository) GetExpiredSLAsForProcessing(ctx context.Context, signature string, processingDuration time.Duration) ([]*scheduler.OperatorsSLA, error) {

	slaQuery := "update operator_sla set worker_signature = $1 , worker_lock_untill = $2 , updated_at = now() where  worker_lock_untill < now() and sla_time < now()"

	tag, err := s.db.Exec(ctx, slaQuery, signature, processingDuration)
	if err != nil {
		errMsg := fmt.Sprintf("error acquiring sla breaches")
		return nil, errors.Wrap(scheduler.EntityEvent, errMsg, err)
	}
	if tag.RowsAffected() == 0 {
		return nil, nil //nolint:nilnil
	}

	var breachedSLAs []*scheduler.OperatorsSLA
	multiError := errors.NewMultiError("GetExpiredSLAsForProcessing")
	getAcquiredSlaRecords := "select id, " + operatorSLAColumns + " from operator_sla where worker_signature = $1 "
	rows, err := s.db.Query(ctx, getAcquiredSlaRecords, signature)
	for rows.Next() {
		slaRecord, err := SLAFromRow(rows)
		if err != nil {
			multiError.Append(errors.Wrap(scheduler.EntityJobRun, "error parsing ", err))
			continue
		}
		schedulerSLAObject, err := slaRecord.toSchedulerSLAObject()
		if err != nil {
			multiError.Append(errors.Wrap(scheduler.EntityJobRun, "error converting to scheduler sla object", err))
			continue
		}

		breachedSLAs = append(breachedSLAs, schedulerSLAObject)
	}

	if int64(len(breachedSLAs)) != tag.RowsAffected() {
		multiError.Append(errors.NewError(errors.ErrInternalError, scheduler.EntityEvent, "sla acquired count not equal Records read for processing"))
	}
	return breachedSLAs, nil
}

func (s *SLARepository) RemoveProcessedSLA(ctx context.Context, slaID uuid.UUID) error {
	deleteSlaRecords := "delete from operator_sla where id = $1"
	_, err := s.db.Exec(ctx, deleteSlaRecords, slaID)
	if err != nil {
		errMsg := fmt.Sprintf("error removing SLA record, with SLA ID: %s", slaID.String())
		return errors.Wrap(scheduler.EntityEvent, errMsg, err)
	}
	return nil
}

func SLAFromRow(row pgx.Row) (*OperatorsSLA, error) {
	var sla OperatorsSLA
	err := row.Scan(&sla.ID, &sla.ProjectName, &sla.JobName, &sla.OperatorName, &sla.OperatorType, &sla.RunID,
		&sla.SLATime, &sla.AlertTag, &sla.WorkerSignature, &sla.WorkerLockUntil, &sla.CreatedAt, &sla.UpdatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, errors.NotFound(scheduler.EntityEvent, "sla record not found")
		}
		return nil, errors.Wrap(scheduler.EntityEvent, "error in reading row for operator sla", err)
	}
	return &sla, nil
}
