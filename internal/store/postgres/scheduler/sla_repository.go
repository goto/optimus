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

type SLA struct {
	ID           uuid.UUID
	OperatorType string
	RunID        uuid.UUID
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

func (s *SLARepository) RegisterSLA(ctx context.Context, operatorType string, runID uuid.UUID, slaTime time.Time, description string) error {
	slaQuery := "INSERT INTO sla ( operator_type, run_id, sla_time, description) values ( $1, $2, $3, $4)"

	tag, err := s.db.Exec(ctx, slaQuery, operatorType, runID, slaTime, description)
	if err != nil {
		errMsg := fmt.Sprintf("error executing sla insert, params: %s, %s, %s, %s", operatorType, runID, slaTime, description)
		return errors.Wrap(scheduler.EntityEvent, errMsg, err)
	}
	if tag.RowsAffected() == 0 {
		errMsg := fmt.Sprintf("error executing sla insert, params: %s, %s, %s, %s, err: now new rows created", operatorType, runID, slaTime, description)
		return errors.NewError(errors.ErrInternalError, scheduler.EntityEvent, errMsg)
	}
	return nil
}

func (s *SLARepository) UpdateSLA(ctx context.Context, operatorType string, runID uuid.UUID, slaTime time.Time) error {
	slaQuery := "update sla set sla_time = $1 where operator_type = $2 and run_id = $3 "

	tag, err := s.db.Exec(ctx, slaQuery, slaTime, operatorType, runID)
	if err != nil {
		errMsg := fmt.Sprintf("error executing sla update, params: %s, %s, %s", operatorType, runID, slaTime)
		return errors.Wrap(scheduler.EntityEvent, errMsg, err)
	}
	if tag.RowsAffected() == 0 {
		errMsg := fmt.Sprintf("error executing sla update, params: %s, %s, %s, err: now new rows created", operatorType, runID, slaTime)
		return errors.NewError(errors.ErrInternalError, scheduler.EntityEvent, errMsg)
	}
	return nil
}

func (s *SLARepository) FinishSLA(ctx context.Context, operatorType string, operatorEndTime time.Time, runID uuid.UUID) error {
	slaQuery := "delete from sla where operator_type = $1 and run_id = $2 and SLA_time > $3 "

	_, err := s.db.Exec(ctx, slaQuery, operatorType, runID, operatorEndTime)
	if err != nil {
		errMsg := fmt.Sprintf("error finishing SLA, params: operatorType:%s, runID:%s, operatorEndTime:%s", operatorType, runID, operatorEndTime)
		return errors.Wrap(scheduler.EntityEvent, errMsg, err)
	}

	return nil
}
