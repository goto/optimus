package service

import (
	"fmt"
	"time"

	"golang.org/x/net/context"

	"github.com/goto/optimus/core/scheduler"
	"github.com/goto/optimus/core/tenant"
	"github.com/goto/optimus/internal/errors"
	"github.com/goto/optimus/internal/lib/cron"
	"github.com/goto/optimus/internal/utils/filter"
)

type Validator struct {
	replayRepository   ReplayRepository
	backfillRepository BackfillRepository
	scheduler          ReplayScheduler
	jobRepo            JobRepository
}

type ReplayRequest interface {
	JobName() scheduler.JobName
	Tenant() tenant.Tenant
	GetJobConfig() map[string]string
	GetStartTime() time.Time
	GetEndTime() time.Time
}

func NewValidator(replayRepository ReplayRepository, scheduler ReplayScheduler, jobRepo JobRepository, backfillRepo BackfillRepository) *Validator {
	return &Validator{replayRepository: replayRepository, scheduler: scheduler, jobRepo: jobRepo, backfillRepository: backfillRepo}
}

func (v Validator) ValidateBackfill(ctx context.Context, replayRequest ReplayRequest, jobCron *cron.ScheduleSpec) error {
	currentTime := time.Now().UTC()
	if replayRequest.GetEndTime().After(currentTime) {
		return errors.NewError(errors.ErrFailedPrecond, scheduler.EntityReplay, fmt.Sprintf("replay end date (%s) is not allowed to be set to a future date, current time: (%s)", replayRequest.GetEndTime().String(), currentTime.String()))
	}

	if err := v.validateConflictedReplay(ctx, replayRequest); err != nil {
		return err
	}
	// validate conflicted backfills
	err := v.validateConflictedBackfills(ctx, replayRequest)
	if err != nil {
		return err
	}

	return v.validateConflictedRun(ctx, replayRequest, jobCron)
}

func (v Validator) Validate(ctx context.Context, replayRequest ReplayRequest, jobCron *cron.ScheduleSpec) error {
	if err := v.validateDateRange(ctx, replayRequest, jobCron); err != nil {
		return err
	}

	if err := v.validateConflictedReplay(ctx, replayRequest); err != nil {
		return err
	}

	// validate conflicted backfills
	err := v.validateConflictedBackfills(ctx, replayRequest)
	if err != nil {
		return err
	}

	return v.validateConflictedRun(ctx, replayRequest, jobCron)
}

func (v Validator) validateDateRange(ctx context.Context, replayRequest ReplayRequest, jobCron *cron.ScheduleSpec) error {
	jobSpec, err := v.jobRepo.GetJobDetails(ctx, replayRequest.Tenant().ProjectName(), replayRequest.JobName())
	if err != nil {
		return err
	}
	replayStartDate := replayRequest.GetStartTime().UTC()
	replayEndDate := replayRequest.GetEndTime().UTC()
	jobLogicalStartDate := jobSpec.Schedule.StartDate.UTC()
	jobScheduleStartDate := jobCron.Next(jobLogicalStartDate)

	if jobSpec.Schedule.EndDate != nil {
		jobLogicalEndDate := jobSpec.Schedule.EndDate.UTC()
		jobScheduleEndDate := jobCron.Next(jobLogicalEndDate)

		// time bound for end date
		if replayEndDate.After(jobScheduleEndDate.UTC()) {
			return errors.NewError(errors.ErrFailedPrecond, scheduler.EntityReplay, fmt.Sprintf("replay end date (%s) is not allowed to be set after the job scheduled end date (%s)", replayEndDate.String(), jobScheduleEndDate.UTC().String()))
		}
	}

	currentTime := time.Now().UTC()
	if replayEndDate.After(currentTime) {
		return errors.NewError(errors.ErrFailedPrecond, scheduler.EntityReplay, fmt.Sprintf("replay end date (%s) is not allowed to be set to a future date, current time: (%s)", replayEndDate.String(), currentTime.String()))
	}

	if replayStartDate.Before(jobScheduleStartDate) {
		return errors.NewError(errors.ErrFailedPrecond, scheduler.EntityReplay, fmt.Sprintf("replay start date (%s) is not allowed to be set before job scheduling start date (%s)", replayStartDate.String(), jobScheduleStartDate.String()))
	}

	return nil
}

func (v Validator) validateConflictedBackfills(ctx context.Context, replayRequest ReplayRequest) error {
	backfills, err := v.backfillRepository.GetBackfillsByFilter(ctx, replayRequest.Tenant().ProjectName(),
		filter.WithString(filter.BackfillStatus, scheduler.BackfillStateInProgress.String()),
		filter.WithStringArray(filter.JobNames, []string{replayRequest.JobName().String()}),
	)
	if err != nil {
		return err
	}
	// check if any intersection of date range
	for _, backfill := range backfills {
		if (backfill.Config().Dstart.Equal(replayRequest.GetEndTime()) || backfill.Config().Dstart.Before(replayRequest.GetEndTime())) &&
			(backfill.Config().Dend.Equal(replayRequest.GetStartTime()) || backfill.Config().Dend.After(replayRequest.GetStartTime())) {
			return errors.NewError(errors.ErrFailedPrecond, scheduler.EntityJobRun, fmt.Sprintf("request is conflicted with an on going backfill with ID %s", backfill.ID().String()))
		}
	}
	return nil
}

func (v Validator) validateConflictedReplay(ctx context.Context, replayRequest ReplayRequest) error {
	onGoingReplays, err := v.replayRepository.GetReplayRequestsByStatus(ctx, scheduler.ReplayNonTerminalStates)
	if err != nil {
		return err
	}
	for _, onGoingReplay := range onGoingReplays {
		if onGoingReplay.Tenant() != replayRequest.Tenant() || onGoingReplay.JobName() != replayRequest.JobName() {
			continue
		}

		// Check any intersection of date range
		if (onGoingReplay.GetStartTime().Equal(replayRequest.GetEndTime()) || onGoingReplay.GetStartTime().Before(replayRequest.GetEndTime())) &&
			(onGoingReplay.GetEndTime().Equal(replayRequest.GetStartTime()) || onGoingReplay.GetEndTime().After(replayRequest.GetStartTime())) {
			return errors.NewError(errors.ErrFailedPrecond, scheduler.EntityJobRun, fmt.Sprintf("request is conflicted with an on going replay with ID %s", onGoingReplay.ID().String()))
		}
	}
	return nil
}

func (v Validator) validateConflictedRun(ctx context.Context, replayRequest ReplayRequest, jobCron *cron.ScheduleSpec) error {
	jobRunCriteria := &scheduler.JobRunsCriteria{
		Name:      replayRequest.JobName().String(),
		StartDate: replayRequest.GetStartTime(),
		EndDate:   replayRequest.GetEndTime(),
	}
	runs, err := v.scheduler.GetJobRuns(ctx, replayRequest.Tenant(), jobRunCriteria, jobCron)
	if err != nil {
		return err
	}
	for _, run := range runs {
		if run.State == scheduler.StateQueued || run.State == scheduler.StateRunning {
			return errors.NewError(errors.ErrFailedPrecond, scheduler.EntityJobRun, fmt.Sprintf("conflicted job run found. job run %s is already in %s state", run.ScheduledAt.Format(time.DateTime), run.State.String()))
		}
	}
	return nil
}
