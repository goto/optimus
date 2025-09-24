package scheduler

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/goto/optimus/core/scheduler"
	"github.com/goto/optimus/internal/errors"
)

type OperatorsSLA struct {
	ID           uuid.UUID
	JobName      string
	OperatorName string
	RunID        string
	OperatorType string
	SLATime      time.Time
	Description  string

	WorkerSignature string
	WorkerLockUntil time.Time

	CreatedAt time.Time
	UpdatedAt time.Time
}

type SLARepository struct {
	db *pgxpool.Pool
}

func NewSLARepository(pool *pgxpool.Pool) *SLARepository {
	return &SLARepository{
		db: pool,
	}
}

func (s *SLARepository) RegisterSLA(ctx context.Context, jobName, operatorName, operatorType, runID string, slaTime time.Time, description string) error {
	slaQuery := "INSERT INTO operator_sla ( job_name, operator_name, operator_type, run_id, sla_time, description) values ( $1, $2, $3, $4, $5, $6)"

	tag, err := s.db.Exec(ctx, slaQuery, jobName, operatorName, operatorType, runID, slaTime, description)
	if err != nil {
		errMsg := fmt.Sprintf("error executing sla insert, params: %s, %s, %s, %s, %s, %s", jobName, operatorName, operatorType, runID, slaTime, description)
		return errors.Wrap(scheduler.EntityEvent, errMsg, err)
	}
	if tag.RowsAffected() == 0 {
		errMsg := fmt.Sprintf("error executing sla insert, params: %s, %s, %s, %s, %s, %s, err: now new rows created", jobName, operatorName, operatorType, runID, slaTime, description)
		return errors.NewError(errors.ErrInternalError, scheduler.EntityEvent, errMsg)
	}
	return nil
}

func (s *SLARepository) UpdateSLA(ctx context.Context, jobName, operatorName, operatorType, runID string, slaTime time.Time) error {
	slaQuery := "update operator_sla set sla_time = $1 where job_name = $2 and  operator_name = $3 and operator_type = $4 and run_id = $5 "

	tag, err := s.db.Exec(ctx, slaQuery, slaTime, jobName, operatorName, operatorType, runID)
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

func (s *SLARepository) FinishSLA(ctx context.Context, jobName, operatorName, operatorType, runID string, operatorEndTime time.Time) error {
	slaQuery := "delete from operator_sla where job_name = $1 and  operator_name = $2 and operator_type = $3 and run_id = $4 and SLA_time > $5 "

	_, err := s.db.Exec(ctx, slaQuery, jobName, operatorName, operatorType, runID, operatorEndTime)
	if err != nil {
		errMsg := fmt.Sprintf("error finishing SLA, params: %s:%s operatorType:%s, runID:%s, operatorEndTime:%s", jobName, operatorName, operatorType, runID, operatorEndTime)
		return errors.Wrap(scheduler.EntityEvent, errMsg, err)
	}

	return nil
}
