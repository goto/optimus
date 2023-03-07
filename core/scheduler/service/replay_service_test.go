package service_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/odpf/optimus/core/scheduler"
	"github.com/odpf/optimus/core/scheduler/service"
	"github.com/odpf/optimus/core/tenant"
	"github.com/odpf/optimus/internal/lib/cron"
)

func TestReplayService(t *testing.T) {
	ctx := context.Background()
	projName := tenant.ProjectName("proj")
	namespaceName := tenant.ProjectName("ns1")
	jobName := scheduler.JobName("sample_select")
	startTimeStr := "2023-01-02T15:00:00Z"
	startTime, _ := time.Parse(scheduler.ISODateFormat, startTimeStr)
	endTime := startTime.Add(48 * time.Hour)
	tnnt, _ := tenant.NewTenant(projName.String(), namespaceName.String())
	parallel := true
	description := "sample backfill"
	replayJobConfig := map[string]string{"EXECUTION_PROJECT": "example_project"}
	replayConfig := scheduler.NewReplayConfig(startTime, endTime, parallel, replayJobConfig, description)
	replayID := uuid.New()
	job := scheduler.Job{
		Name:   jobName,
		Tenant: tnnt,
	}
	jobWithDetails := &scheduler.JobWithDetails{
		Job: &job,
		JobMetadata: &scheduler.JobMetadata{
			Version: 1,
		},
		Schedule: &scheduler.Schedule{
			StartDate: startTime.Add(-time.Hour * 24),
			Interval:  "0 12 * * *",
		},
	}
	jobCronStr := "0 12 * * *"
	jobCron, _ := cron.ParseCronSchedule(jobCronStr)

	t.Run("CreateReplay", func(t *testing.T) {
		t.Run("should return replay ID if replay created successfully", func(t *testing.T) {
			replayRepository := new(ReplayRepository)
			defer replayRepository.AssertExpectations(t)

			jobRepository := new(JobRepository)
			defer jobRepository.AssertExpectations(t)

			replayValidator := new(ReplayValidator)
			defer replayValidator.AssertExpectations(t)

			scheduledTime1Str := "2023-01-03T12:00:00Z"
			scheduledTime1, _ := time.Parse(scheduler.ISODateFormat, scheduledTime1Str)
			scheduledTime2 := scheduledTime1.Add(24 * time.Hour)
			replayRuns := []*scheduler.JobRunStatus{
				{ScheduledAt: scheduledTime1, State: scheduler.StatePending},
				{ScheduledAt: scheduledTime2, State: scheduler.StatePending},
			}
			replayReq := scheduler.NewReplayRequest(jobName, tnnt, replayConfig, scheduler.ReplayStateCreated)

			jobRepository.On("GetJobDetails", ctx, projName, jobName).Return(jobWithDetails, nil)
			replayValidator.On("Validate", ctx, replayReq, jobCron).Return(nil)
			replayRepository.On("RegisterReplay", ctx, replayReq, replayRuns).Return(replayID, nil)

			replayService := service.NewReplayService(replayRepository, jobRepository, replayValidator)
			result, err := replayService.CreateReplay(ctx, tnnt, jobName, replayConfig)
			assert.NoError(t, err)
			assert.Equal(t, replayID, result)
		})

		t.Run("should return error if not pass validation", func(t *testing.T) {
			replayRepository := new(ReplayRepository)
			defer replayRepository.AssertExpectations(t)

			jobRepository := new(JobRepository)
			defer jobRepository.AssertExpectations(t)

			replayValidator := new(ReplayValidator)
			defer replayValidator.AssertExpectations(t)

			replayReq := scheduler.NewReplayRequest(jobName, tnnt, replayConfig, scheduler.ReplayStateCreated)

			jobRepository.On("GetJobDetails", ctx, projName, jobName).Return(jobWithDetails, nil)
			replayValidator.On("Validate", ctx, replayReq, jobCron).Return(errors.New("not passed validation"))

			replayService := service.NewReplayService(replayRepository, jobRepository, replayValidator)
			result, err := replayService.CreateReplay(ctx, tnnt, jobName, replayConfig)
			assert.ErrorContains(t, err, "not passed validation")
			assert.Equal(t, uuid.Nil, result)
		})
	})
}

// ReplayRepository is an autogenerated mock type for the ReplayRepository type
type ReplayRepository struct {
	mock.Mock
}

// GetReplayRequestsByStatus provides a mock function with given fields: ctx, statusList
func (_m *ReplayRepository) GetReplayRequestsByStatus(ctx context.Context, statusList []scheduler.ReplayState) ([]*scheduler.Replay, error) {
	ret := _m.Called(ctx, statusList)

	var r0 []*scheduler.Replay
	if rf, ok := ret.Get(0).(func(context.Context, []scheduler.ReplayState) []*scheduler.Replay); ok {
		r0 = rf(ctx, statusList)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]*scheduler.Replay)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, []scheduler.ReplayState) error); ok {
		r1 = rf(ctx, statusList)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// GetReplayToExecute provides a mock function with given fields: _a0
func (_m *ReplayRepository) GetReplayToExecute(_a0 context.Context) (*scheduler.ReplayWithRun, error) {
	ret := _m.Called(_a0)

	var r0 *scheduler.ReplayWithRun
	if rf, ok := ret.Get(0).(func(context.Context) *scheduler.ReplayWithRun); ok {
		r0 = rf(_a0)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*scheduler.ReplayWithRun)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context) error); ok {
		r1 = rf(_a0)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// RegisterReplay provides a mock function with given fields: ctx, replay, runs
func (_m *ReplayRepository) RegisterReplay(ctx context.Context, replay *scheduler.Replay, runs []*scheduler.JobRunStatus) (uuid.UUID, error) {
	ret := _m.Called(ctx, replay, runs)

	var r0 uuid.UUID
	if rf, ok := ret.Get(0).(func(context.Context, *scheduler.Replay, []*scheduler.JobRunStatus) uuid.UUID); ok {
		r0 = rf(ctx, replay, runs)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(uuid.UUID)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, *scheduler.Replay, []*scheduler.JobRunStatus) error); ok {
		r1 = rf(ctx, replay, runs)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// UpdateReplay provides a mock function with given fields: ctx, replayID, state, runs, message
func (_m *ReplayRepository) UpdateReplay(ctx context.Context, replayID uuid.UUID, state scheduler.ReplayState, runs []*scheduler.JobRunStatus, message string) error {
	ret := _m.Called(ctx, replayID, state, runs, message)

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, uuid.UUID, scheduler.ReplayState, []*scheduler.JobRunStatus, string) error); ok {
		r0 = rf(ctx, replayID, state, runs, message)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// UpdateReplayStatus provides a mock function with given fields: ctx, replayID, state, message
func (_m *ReplayRepository) UpdateReplayStatus(ctx context.Context, replayID uuid.UUID, state scheduler.ReplayState, message string) error {
	ret := _m.Called(ctx, replayID, state, message)

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, uuid.UUID, scheduler.ReplayState, string) error); ok {
		r0 = rf(ctx, replayID, state, message)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// ReplayValidator is an autogenerated mock type for the ReplayValidator type
type ReplayValidator struct {
	mock.Mock
}

// Validate provides a mock function with given fields: ctx, replayRequest, jobCron
func (_m *ReplayValidator) Validate(ctx context.Context, replayRequest *scheduler.Replay, jobCron *cron.ScheduleSpec) error {
	ret := _m.Called(ctx, replayRequest, jobCron)
	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, *scheduler.Replay, *cron.ScheduleSpec) error); ok {
		r0 = rf(ctx, replayRequest, jobCron)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}
