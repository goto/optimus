package service_test

import (
	"context"
	"testing"
	"time"

	"github.com/goto/salt/log"
	"github.com/stretchr/testify/assert"

	"github.com/goto/optimus/core/scheduler"
	"github.com/goto/optimus/core/scheduler/service"
	"github.com/goto/optimus/core/tenant"
	"github.com/goto/optimus/internal/lib/cron"
	"github.com/goto/optimus/internal/lib/window"
)

type windowConfig struct {
	Size, ShiftBy, TruncateTo, Location string
}

type testContext struct {
	BaseJobStartDate        string
	BaseJobScheduleInterval string
	BaseJobWindowConfig     windowConfig

	UpstreamJobScheduleInterval string
	UpstreamJobStartDate        string
	RecentUpstreamRuns          []*scheduler.JobRunStatus
}

type testExpectation struct {
	IntervalStart string
	IntervalEnd   string
}

type testCase struct {
	Description            string
	BaseJobRunScheduleTime string
	testContext            testContext
	Expectations           testExpectation
}

func TestGetUpstreamJobRuns(t *testing.T) {
	logger := log.NewNoop()
	ctx := context.Background()

	// Tests Setup
	subjectJob, _ := scheduler.JobNameFrom("Job")
	subjectProjectName, _ := tenant.ProjectNameFrom("Proj")
	subjectProject, err := tenant.NewProject(subjectProjectName.String(), map[string]string{
		tenant.ProjectStoragePathKey:   "",
		tenant.ProjectSchedulerHost:    "",
		tenant.ProjectSchedulerVersion: "",
	})
	assert.Nil(t, err)
	if err != nil {
		return
	}
	upstreamJob, _ := scheduler.JobNameFrom("UpstreamJob")
	upstreamTenant, _ := tenant.NewTenant("upstream-proj", "UpstreamTenant-ns")

	// Test Cases
	tests := []testCase{
		{
			Description:            "Get all job runs for a given schedule time",
			BaseJobRunScheduleTime: "2016-01-04T01:00:00Z",
			testContext: testContext{
				BaseJobStartDate:        "2006-01-02T15:04:05Z",
				BaseJobWindowConfig:     windowConfig{"24h", "0d", "d", ""},
				BaseJobScheduleInterval: "0 1 * * *",

				UpstreamJobStartDate:        "2006-01-02T15:04:05Z",
				UpstreamJobScheduleInterval: "0 1 * * *",
			},
			Expectations: testExpectation{
				IntervalStart: "2016-01-03T01:00:00Z",
				IntervalEnd:   "2016-01-04T01:00:00Z",
			},
		},
	}

	// Test Execution
	for _, test := range tests {
		t.Run(test.Description, func(t *testing.T) {
			jobRepo := new(JobRepository)
			upstreamJobStartDate, err := time.Parse(time.RFC3339, test.testContext.UpstreamJobStartDate)
			assert.Nil(t, err)
			if err != nil {
				return
			}
			upstreamJobWithDetails := &scheduler.JobWithDetails{
				Name: upstreamJob,
				Job: &scheduler.Job{
					Name:   upstreamJob,
					Tenant: upstreamTenant,
				},
				Schedule: &scheduler.Schedule{
					StartDate: upstreamJobStartDate,
					Interval:  test.testContext.UpstreamJobScheduleInterval,
				},
			}
			subjectJobWindow, err := getWindowConfig(test.testContext.BaseJobWindowConfig)
			assert.Nil(t, err)
			if err != nil {
				return
			}

			baseJobStartDate, err := time.Parse(time.RFC3339, test.testContext.BaseJobStartDate)
			assert.Nil(t, err)
			if err != nil {
				return
			}
			subjectJobWithDetails := &scheduler.JobWithDetails{
				Name: subjectJob,
				Job: &scheduler.Job{
					Name:         subjectJob,
					WindowConfig: subjectJobWindow,
				},
				Schedule: &scheduler.Schedule{
					StartDate: baseJobStartDate,
					Interval:  test.testContext.BaseJobScheduleInterval,
				},
			}
			jobRepo.On("GetJobDetails", ctx, upstreamTenant.ProjectName(), upstreamJob).Return(upstreamJobWithDetails, nil)
			jobRepo.On("GetJobDetails", ctx, subjectProjectName, subjectJob).Return(subjectJobWithDetails, nil)
			defer jobRepo.AssertExpectations(t)

			schedulerService := new(mockScheduler)
			expectedIntervalStart, err := time.Parse(time.RFC3339, test.Expectations.IntervalStart)
			assert.Nil(t, err)
			if err != nil {
				return
			}
			expectedIntervalEnd, err := time.Parse(time.RFC3339, test.Expectations.IntervalEnd)
			assert.Nil(t, err)
			if err != nil {
				return
			}
			expectedCriteria := &scheduler.JobRunsCriteria{
				Name:      upstreamJob.String(),
				StartDate: expectedIntervalStart,
				EndDate:   expectedIntervalEnd,
				Filter:    []string{},
			}
			expectedCronSchedule, err := cron.ParseCronSchedule(test.testContext.UpstreamJobScheduleInterval)
			assert.Nil(t, err)
			schedulerService.On("GetJobRuns", ctx, upstreamTenant, expectedCriteria, expectedCronSchedule).Return([]*scheduler.JobRunStatus{}, nil)
			defer schedulerService.AssertExpectations(t)

			projectGetter := new(mockProjectGetter)
			projectGetter.On("Get", ctx, subjectProjectName).Return(subjectProject, nil)
			defer projectGetter.AssertExpectations(t)

			sensorService := service.NewJobRunService(logger,
				jobRepo, nil, nil, nil, schedulerService, nil, nil, nil, projectGetter, nil)

			baseJobScheduleTime, err := time.Parse(time.RFC3339, test.BaseJobRunScheduleTime)
			assert.Nil(t, err)
			if err != nil {
				return
			}
			sensorParams := scheduler.JobSensorParameters{
				SubjectJobName:     subjectJob,
				SubjectProjectName: subjectProjectName,
				ScheduledTime:      baseJobScheduleTime,
				UpstreamJobName:    upstreamJob,
				UpstreamTenant:     upstreamTenant,
			}

			_, err = sensorService.GetUpstreamJobRuns(ctx, "", sensorParams, []string{})
			assert.Nil(t, err)
		})
	}
}

func getWindowConfig(w windowConfig) (window.Config, error) {
	return window.NewConfig(w.Size, w.ShiftBy, w.Location, w.TruncateTo)
}
