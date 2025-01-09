package service_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/goto/optimus/core/scheduler"
	"github.com/goto/optimus/core/scheduler/service"
	"github.com/goto/optimus/core/tenant"
	"github.com/goto/optimus/internal/lib/cron"
)

func TestReplayValidator(t *testing.T) {
	ctx := context.Background()
	tnnt, _ := tenant.NewTenant("sample-project", "sample-namespace")
	jobName := scheduler.JobName("sample_select")
	jobStartTimeStr := "2023-01-02T15:00:00Z"
	jobStartTime, _ := time.Parse(scheduler.ISODateFormat, jobStartTimeStr)
	jobEndTime := jobStartTime.Add(48 * time.Hour)
	parallel := true
	description := "sample backfill"
	replayJobConfig := map[string]string{"EXECUTION_PROJECT": "example_project"}
	jobCronStr := "0 12 * * *"
	jobCron, _ := cron.ParseCronSchedule(jobCronStr)

	replayStartTime := jobCron.Next(jobStartTime)
	replayEndTime := jobEndTime

	replayConfig := scheduler.NewReplayConfig(replayStartTime, replayEndTime, parallel, replayJobConfig, description)
	replayRunsCriteriaJobA := &scheduler.JobRunsCriteria{
		Name:      jobName.String(),
		StartDate: replayStartTime,
		EndDate:   replayEndTime,
	}

	scheduledTimeStr1 := "2023-01-02T12:00:00Z"
	scheduledTime1, _ := time.Parse(scheduler.ISODateFormat, scheduledTimeStr1)
	replayStatusToValidate := scheduler.ReplayNonTerminalStates
	replayReq := scheduler.NewReplayRequest(jobName, tnnt, replayConfig, scheduler.ReplayStateCreated)

	t.Run("Validate", func(t *testing.T) {
		t.Run("should return nil if validation valid", func(t *testing.T) {
			replayRepository := new(ReplayRepository)
			defer replayRepository.AssertExpectations(t)

			sch := new(mockReplayScheduler)
			defer sch.AssertExpectations(t)

			jobRepository := new(JobRepository)
			defer jobRepository.AssertExpectations(t)

			onGoingReplayConfig := scheduler.NewReplayConfig(time.Now(), time.Now(), parallel, replayJobConfig, description)
			onGoingReplay := []*scheduler.Replay{
				scheduler.NewReplayRequest(jobName, tnnt, onGoingReplayConfig, scheduler.ReplayStateCreated),
				scheduler.NewReplayRequest("other-job", tnnt, onGoingReplayConfig, scheduler.ReplayStateCreated),
			}
			currentRuns := []*scheduler.JobRunStatus{
				{
					ScheduledAt: scheduledTime1,
					State:       scheduler.StateSuccess,
				},
			}

			jobRepository.On("GetJobDetails", ctx, replayReq.Tenant().ProjectName(), replayReq.JobName()).Return(&scheduler.JobWithDetails{
				Schedule: &scheduler.Schedule{
					StartDate: jobStartTime,
					EndDate:   &jobEndTime,
				},
			}, nil)
			replayRepository.On("GetReplayRequestsByStatus", ctx, replayStatusToValidate).Return(onGoingReplay, nil)
			sch.On("GetJobRuns", ctx, tnnt, replayRunsCriteriaJobA, jobCron).Return(currentRuns, nil)

			validator := service.NewValidator(replayRepository, sch, jobRepository)
			err := validator.Validate(ctx, replayReq, jobCron)
			assert.NoError(t, err)
		})
		t.Run("should return error if start date of replay is before the start date of the job", func(t *testing.T) {
			replayRepository := new(ReplayRepository)
			defer replayRepository.AssertExpectations(t)

			sch := new(mockReplayScheduler)
			defer sch.AssertExpectations(t)

			jobRepository := new(JobRepository)
			defer jobRepository.AssertExpectations(t)

			jobRepository.On("GetJobDetails", ctx, replayReq.Tenant().ProjectName(), replayReq.JobName()).Return(&scheduler.JobWithDetails{
				Schedule: &scheduler.Schedule{
					StartDate: jobStartTime.Add(24 * time.Hour), // logical start date 24 hours ahead
					EndDate:   &jobEndTime,
				},
			}, nil)

			validator := service.NewValidator(replayRepository, sch, jobRepository)
			err := validator.Validate(ctx, replayReq, jobCron)
			assert.ErrorContains(t, err, "replay start date")
		})
		t.Run("should return error if end date of replay is on the future", func(t *testing.T) {
			replayRepository := new(ReplayRepository)
			defer replayRepository.AssertExpectations(t)

			sch := new(mockReplayScheduler)
			defer sch.AssertExpectations(t)

			jobRepository := new(JobRepository)
			defer jobRepository.AssertExpectations(t)

			schEndTime := jobEndTime.Add(-25 * time.Hour)
			jobRepository.On("GetJobDetails", ctx, replayReq.Tenant().ProjectName(), replayReq.JobName()).Return(&scheduler.JobWithDetails{
				Schedule: &scheduler.Schedule{
					StartDate: jobStartTime,
					EndDate:   &schEndTime, // end date 1 second prior of replay
				},
			}, nil)

			validator := service.NewValidator(replayRepository, sch, jobRepository)
			err := validator.Validate(ctx, replayReq, jobCron)
			assert.ErrorContains(t, err, "replay end date")
		})
		t.Run("should not return error if replay end date equals to job's end_date scheduled date", func(t *testing.T) {
			replayRepository := new(ReplayRepository)
			defer replayRepository.AssertExpectations(t)

			sch := new(mockReplayScheduler)
			defer sch.AssertExpectations(t)

			jobRepository := new(JobRepository)
			defer jobRepository.AssertExpectations(t)

			jobAStartTimeStr := "2023-01-02T12:00:00Z"
			jobAStartTime, _ := time.Parse(scheduler.ISODateFormat, jobAStartTimeStr)
			jobAEndTime := jobAStartTime.Add(48 * time.Hour)
			jobAEndScheduledTime := jobAEndTime.Add(24 * time.Hour)

			replayJobAStartTime := jobCron.Next(jobStartTime)
			replayJobAEndTime := jobAEndScheduledTime

			replayJobAConfig := scheduler.NewReplayConfig(replayJobAStartTime, replayJobAEndTime, parallel, replayJobConfig, description)
			replayJobAReq := scheduler.NewReplayRequest(jobName, tnnt, replayJobAConfig, scheduler.ReplayStateCreated)

			replayJobARunsCriteria := &scheduler.JobRunsCriteria{
				Name:      jobName.String(),
				StartDate: replayJobAStartTime,
				EndDate:   replayJobAEndTime,
			}

			jobRepository.On("GetJobDetails", ctx, replayReq.Tenant().ProjectName(), replayReq.JobName()).Return(&scheduler.JobWithDetails{
				Schedule: &scheduler.Schedule{
					StartDate: jobStartTime,
					EndDate:   &jobAEndTime,
				},
			}, nil)
			replayRepository.On("GetReplayRequestsByStatus", ctx, replayStatusToValidate).Return(nil, nil)
			sch.On("GetJobRuns", ctx, tnnt, replayJobARunsCriteria, jobCron).Return(nil, nil)

			validator := service.NewValidator(replayRepository, sch, jobRepository)
			err := validator.Validate(ctx, replayJobAReq, jobCron)
			assert.NoError(t, err)
		})
		t.Run("should return error if conflict replay found", func(t *testing.T) {
			replayRepository := new(ReplayRepository)
			defer replayRepository.AssertExpectations(t)

			sch := new(mockReplayScheduler)
			defer sch.AssertExpectations(t)

			jobRepository := new(JobRepository)
			defer jobRepository.AssertExpectations(t)

			onGoingReplay := []*scheduler.Replay{
				scheduler.NewReplayRequest(jobName, tnnt, replayConfig, scheduler.ReplayStateInProgress),
			}

			jobRepository.On("GetJobDetails", ctx, replayReq.Tenant().ProjectName(), replayReq.JobName()).Return(&scheduler.JobWithDetails{
				Schedule: &scheduler.Schedule{
					StartDate: jobStartTime,
					EndDate:   &jobEndTime,
				},
			}, nil)

			replayRepository.On("GetReplayRequestsByStatus", ctx, replayStatusToValidate).Return(onGoingReplay, nil)

			validator := service.NewValidator(replayRepository, sch, jobRepository)
			err := validator.Validate(ctx, replayReq, jobCron)
			assert.ErrorContains(t, err, "request is conflicted")
		})
		t.Run("should return error if conflict run found", func(t *testing.T) {
			replayRepository := new(ReplayRepository)
			defer replayRepository.AssertExpectations(t)

			sch := new(mockReplayScheduler)
			defer sch.AssertExpectations(t)

			jobRepository := new(JobRepository)
			defer jobRepository.AssertExpectations(t)

			onGoingReplayConfig := scheduler.NewReplayConfig(time.Now(), time.Now(), parallel, replayJobConfig, description)
			onGoingReplay := []*scheduler.Replay{
				scheduler.NewReplayRequest(jobName, tnnt, onGoingReplayConfig, scheduler.ReplayStateCreated),
			}
			currentRuns := []*scheduler.JobRunStatus{
				{
					ScheduledAt: scheduledTime1,
					State:       scheduler.StateRunning,
				},
			}

			jobRepository.On("GetJobDetails", ctx, replayReq.Tenant().ProjectName(), replayReq.JobName()).Return(&scheduler.JobWithDetails{
				Schedule: &scheduler.Schedule{
					StartDate: jobStartTime,
					EndDate:   &jobEndTime,
				},
			}, nil)
			replayRepository.On("GetReplayRequestsByStatus", ctx, replayStatusToValidate).Return(onGoingReplay, nil)
			sch.On("GetJobRuns", ctx, tnnt, replayRunsCriteriaJobA, jobCron).Return(currentRuns, nil)

			validator := service.NewValidator(replayRepository, sch, jobRepository)
			err := validator.Validate(ctx, replayReq, jobCron)
			assert.ErrorContains(t, err, "conflicted job run found")
		})
		t.Run("should return error if unable to GetReplayRequestsByStatus", func(t *testing.T) {
			replayRepository := new(ReplayRepository)
			defer replayRepository.AssertExpectations(t)

			sch := new(mockReplayScheduler)
			defer sch.AssertExpectations(t)

			jobRepository := new(JobRepository)
			defer jobRepository.AssertExpectations(t)

			internalErr := errors.New("internal error")
			jobRepository.On("GetJobDetails", ctx, replayReq.Tenant().ProjectName(), replayReq.JobName()).Return(&scheduler.JobWithDetails{
				Schedule: &scheduler.Schedule{
					StartDate: jobStartTime,
					EndDate:   &jobEndTime,
				},
			}, nil)
			replayRepository.On("GetReplayRequestsByStatus", ctx, replayStatusToValidate).Return(nil, internalErr)

			validator := service.NewValidator(replayRepository, sch, jobRepository)
			err := validator.Validate(ctx, replayReq, jobCron)
			assert.ErrorIs(t, err, internalErr)
		})
		t.Run("should return error if unable to get job runs", func(t *testing.T) {
			replayRepository := new(ReplayRepository)
			defer replayRepository.AssertExpectations(t)

			sch := new(mockReplayScheduler)
			defer sch.AssertExpectations(t)

			jobRepository := new(JobRepository)
			defer jobRepository.AssertExpectations(t)

			onGoingReplayConfig := scheduler.NewReplayConfig(time.Now(), time.Now(), parallel, map[string]string{}, description)
			onGoingReplay := []*scheduler.Replay{
				scheduler.NewReplayRequest(jobName, tnnt, onGoingReplayConfig, scheduler.ReplayStateCreated),
			}

			replayRepository.On("GetReplayRequestsByStatus", ctx, replayStatusToValidate).Return(onGoingReplay, nil)

			internalErr := errors.New("internal error")
			jobRepository.On("GetJobDetails", ctx, replayReq.Tenant().ProjectName(), replayReq.JobName()).Return(&scheduler.JobWithDetails{
				Schedule: &scheduler.Schedule{
					StartDate: jobStartTime,
					EndDate:   &jobEndTime,
				},
			}, nil)
			sch.On("GetJobRuns", ctx, tnnt, replayRunsCriteriaJobA, jobCron).Return(nil, internalErr)

			validator := service.NewValidator(replayRepository, sch, jobRepository)
			err := validator.Validate(ctx, replayReq, jobCron)
			assert.ErrorIs(t, err, internalErr)
		})
	})
}
