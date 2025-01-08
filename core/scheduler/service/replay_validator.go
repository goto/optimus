package service

import (
	"fmt"
	"time"

	"golang.org/x/net/context"

	"github.com/goto/optimus/core/scheduler"
	"github.com/goto/optimus/internal/errors"
	"github.com/goto/optimus/internal/lib/cron"
)

type Validator struct {
	replayRepository ReplayRepository
	scheduler        ReplayScheduler
	jobRepo          JobRepository
}

func NewValidator(replayRepository ReplayRepository, scheduler ReplayScheduler, jobRepo JobRepository) *Validator {
	return &Validator{replayRepository: replayRepository, scheduler: scheduler, jobRepo: jobRepo}
}

func (v Validator) Validate(ctx context.Context, replayRequest *scheduler.Replay, jobCron *cron.ScheduleSpec) error {
	if err := v.validateDateRange(ctx, replayRequest, jobCron); err != nil {
		return err
	}

	if err := v.validateConflictedReplay(ctx, replayRequest); err != nil {
		return err
	}

	return v.validateConflictedRun(ctx, replayRequest, jobCron)
}

func (v Validator) validateDateRange(ctx context.Context, replayRequest *scheduler.Replay, jobCron *cron.ScheduleSpec) error {
	jobSpec, err := v.jobRepo.GetJobDetails(ctx, replayRequest.Tenant().ProjectName(), replayRequest.JobName())
	if err != nil {
		return err
	}
	replayStartDate := replayRequest.Config().StartTime.UTC()
	replayEndDate := replayRequest.Config().EndTime.UTC()
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

func (v Validator) validateConflictedReplay(ctx context.Context, replayRequest *scheduler.Replay) error {
	onGoingReplays, err := v.replayRepository.GetReplayRequestsByStatus(ctx, scheduler.ReplayNonTerminalStates)
	if err != nil {
		return err
	}
	for _, onGoingReplay := range onGoingReplays {
		if onGoingReplay.Tenant() != replayRequest.Tenant() || onGoingReplay.JobName() != replayRequest.JobName() {
			continue
		}

		// Check any intersection of date range
		if (onGoingReplay.Config().StartTime.Equal(replayRequest.Config().EndTime) || onGoingReplay.Config().StartTime.Before(replayRequest.Config().EndTime)) &&
			(onGoingReplay.Config().EndTime.Equal(replayRequest.Config().StartTime) || onGoingReplay.Config().EndTime.After(replayRequest.Config().StartTime)) {
			return errors.NewError(errors.ErrFailedPrecond, scheduler.EntityJobRun, fmt.Sprintf("request is conflicted with an on going replay with ID %s", onGoingReplay.ID().String()))
		}
	}
	return nil
}

func (v Validator) validateConflictedRun(ctx context.Context, replayRequest *scheduler.Replay, jobCron *cron.ScheduleSpec) error {
	jobRunCriteria := &scheduler.JobRunsCriteria{
		Name:      replayRequest.JobName().String(),
		StartDate: replayRequest.Config().StartTime,
		EndDate:   replayRequest.Config().EndTime,
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
