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
	BaseJobScheduleInterval string
	BaseJobWindowConfig     windowConfig

	UpstreamJobScheduleInterval string
	RecentUpstreamRuns          []*scheduler.JobRunStatus
}

type testExpectation struct {
	IntervalStart string
	IntervalEnd   string
}

type testCase struct {
	Description  string
	testContext  testContext
	Expectations testExpectation
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
	jobStartDate := "2006-01-02T15:04:05Z"

	if err != nil {
		return
	}
	upstreamJob, _ := scheduler.JobNameFrom("UpstreamJob")
	upstreamTenant, _ := tenant.NewTenant("upstream-proj", "UpstreamTenant-ns")

	// Test Cases
	baseJobRunScheduleTime := "2016-01-04T01:00:00Z"
	tests := []testCase{
		{
			Description: "Get today data until tomorrow",
			testContext: testContext{
				BaseJobWindowConfig:         windowConfig{"1d", "1d", "d", ""},
				BaseJobScheduleInterval:     "0 1 * * *",
				UpstreamJobScheduleInterval: "0 1 * * *",
			},
			Expectations: testExpectation{
				IntervalStart: "2016-01-03T01:00:00Z",
				IntervalEnd:   "2016-01-04T01:00:00Z",
			},
		},
		{
			Description: "Get yesterday data",
			testContext: testContext{
				BaseJobWindowConfig:         windowConfig{"1d", "0d", "d", ""},
				BaseJobScheduleInterval:     "0 1 * * *",
				UpstreamJobScheduleInterval: "0 1 * * *",
			},
			Expectations: testExpectation{
				IntervalStart: "2016-01-03T01:00:00Z",
				IntervalEnd:   "2016-01-04T01:00:00Z",
			},
		},
		{
			Description: "Get last 2 days data until today",
			testContext: testContext{
				BaseJobWindowConfig:         windowConfig{"2d", "0d", "d", ""},
				BaseJobScheduleInterval:     "0 1 * * *",
				UpstreamJobScheduleInterval: "0 1 * * *",
			},
			Expectations: testExpectation{
				IntervalStart: "2016-01-02T01:00:00Z",
				IntervalEnd:   "2016-01-04T01:00:00Z",
			},
		},
		{
			Description: "Get yesterday data until tomorrow",
			testContext: testContext{
				BaseJobWindowConfig:         windowConfig{"2d", "1d", "d", ""},
				BaseJobScheduleInterval:     "0 1 * * *",
				UpstreamJobScheduleInterval: "0 1 * * *",
			},
			Expectations: testExpectation{
				IntervalStart: "2016-01-02T01:00:00Z",
				IntervalEnd:   "2016-01-04T01:00:00Z",
			},
		},
		{
			Description: "Get last 2 days data until tomorrow",
			testContext: testContext{
				BaseJobWindowConfig:         windowConfig{"3d", "1d", "d", ""},
				BaseJobScheduleInterval:     "0 1 * * *",
				UpstreamJobScheduleInterval: "0 1 * * *",
			},
			Expectations: testExpectation{
				IntervalStart: "2016-01-01T01:00:00Z",
				IntervalEnd:   "2016-01-04T01:00:00Z",
			},
		},
		{
			Description: "Get last 4 days data until tomorrow",
			testContext: testContext{
				BaseJobWindowConfig:         windowConfig{"5d", "1d", "d", ""},
				BaseJobScheduleInterval:     "0 1 * * *",
				UpstreamJobScheduleInterval: "0 1 * * *",
			},
			Expectations: testExpectation{
				IntervalStart: "2015-12-30T01:00:00Z",
				IntervalEnd:   "2016-01-04T01:00:00Z",
			},
		},
		{
			Description: "Get last 7 days data until yesterday",
			testContext: testContext{
				BaseJobWindowConfig:         windowConfig{"6d", "-1d", "d", ""},
				BaseJobScheduleInterval:     "0 1 * * *",
				UpstreamJobScheduleInterval: "0 1 * * *",
			},
			Expectations: testExpectation{
				IntervalStart: "2015-12-29T01:00:00Z",
				IntervalEnd:   "2016-01-04T01:00:00Z",
			},
		},
		{
			Description: "Get last 6 days data until tomorrow",
			testContext: testContext{
				BaseJobWindowConfig:         windowConfig{"7d", "1d", "d", ""},
				BaseJobScheduleInterval:     "0 1 * * *",
				UpstreamJobScheduleInterval: "0 1 * * *",
			},
			Expectations: testExpectation{
				IntervalStart: "2015-12-28T01:00:00Z",
				IntervalEnd:   "2016-01-04T01:00:00Z",
			},
		},
		{
			Description: "Get 14 days data until tomorrow",
			testContext: testContext{
				BaseJobWindowConfig:         windowConfig{"15d", "1d", "d", ""},
				BaseJobScheduleInterval:     "0 1 * * *",
				UpstreamJobScheduleInterval: "0 1 * * *",
			},
			Expectations: testExpectation{
				IntervalStart: "2015-12-20T01:00:00Z",
				IntervalEnd:   "2016-01-04T01:00:00Z",
			},
		},
		{
			Description: "Get previous monday of the week data until sunday",
			testContext: testContext{
				BaseJobWindowConfig:         windowConfig{"6d", "-1d", "w", ""},
				BaseJobScheduleInterval:     "0 1 * * *",
				UpstreamJobScheduleInterval: "0 1 * * *",
			},
			Expectations: testExpectation{
				IntervalStart: "2015-12-29T01:00:00Z",
				IntervalEnd:   "2016-01-04T01:00:00Z",
			},
		},
		{
			Description: "Get previous week data",
			testContext: testContext{
				BaseJobWindowConfig:         windowConfig{"1w", "0w", "w", ""},
				BaseJobScheduleInterval:     "0 1 * * *",
				UpstreamJobScheduleInterval: "0 1 * * *",
			},
			Expectations: testExpectation{
				IntervalStart: "2015-12-28T01:00:00Z",
				IntervalEnd:   "2016-01-04T01:00:00Z",
			},
		},
		{
			Description: "Get this week data",
			testContext: testContext{
				BaseJobWindowConfig:         windowConfig{"7d", "6d", "w", ""},
				BaseJobScheduleInterval:     "0 1 * * *",
				UpstreamJobScheduleInterval: "0 1 * * *",
			},
			Expectations: testExpectation{
				IntervalStart: "2015-12-28T01:00:00Z",
				IntervalEnd:   "2016-01-04T01:00:00Z",
			},
		},
		{
			Description: "Get this month data until the next first day of the month data",
			testContext: testContext{
				BaseJobWindowConfig:         windowConfig{"1M", "1M", "M", ""},
				BaseJobScheduleInterval:     "0 1 * * *",
				UpstreamJobScheduleInterval: "0 1 * * *",
			},
			Expectations: testExpectation{
				IntervalStart: "2015-12-04T01:00:00Z",
				IntervalEnd:   "2016-01-04T01:00:00Z",
			},
		},
		{
			Description: "Get previous month data",
			testContext: testContext{
				BaseJobWindowConfig:         windowConfig{"1M", "0M", "M", ""},
				BaseJobScheduleInterval:     "0 1 * * *",
				UpstreamJobScheduleInterval: "0 1 * * *",
			},
			Expectations: testExpectation{
				IntervalStart: "2015-12-04T01:00:00Z",
				IntervalEnd:   "2016-01-04T01:00:00Z",
			},
		},
		{
			Description: "Get last 2 months data until previous month data",
			testContext: testContext{
				BaseJobWindowConfig:         windowConfig{"1M", "-1M", "M", ""},
				BaseJobScheduleInterval:     "0 1 * * *",
				UpstreamJobScheduleInterval: "0 1 * * *",
			},
			Expectations: testExpectation{
				IntervalStart: "2015-12-04T01:00:00Z",
				IntervalEnd:   "2016-01-04T01:00:00Z",
			},
		},
		{
			Description: "Get last 1 hour data",
			testContext: testContext{
				BaseJobWindowConfig:         windowConfig{"1h", "0h", "h", ""},
				BaseJobScheduleInterval:     "0 1 * * *",
				UpstreamJobScheduleInterval: "0 1 * * *",
			},
			Expectations: testExpectation{
				IntervalStart: "2016-01-04T00:00:00Z",
				IntervalEnd:   "2016-01-04T01:00:00Z",
			},
		},
		{
			Description: "Get last 24 hour data",
			testContext: testContext{
				BaseJobWindowConfig:         windowConfig{"24h", "0h", "h", ""},
				BaseJobScheduleInterval:     "0 1 * * *",
				UpstreamJobScheduleInterval: "0 1 * * *",
			},
			Expectations: testExpectation{
				IntervalStart: "2016-01-03T01:00:00Z",
				IntervalEnd:   "2016-01-04T01:00:00Z",
			},
		},
		{
			Description: "Get current data until next 24 hours data",
			testContext: testContext{
				BaseJobWindowConfig:         windowConfig{"24h", "24h", "h", ""},
				BaseJobScheduleInterval:     "0 1 * * *",
				UpstreamJobScheduleInterval: "0 1 * * *",
			},
			Expectations: testExpectation{
				IntervalStart: "2016-01-03T01:00:00Z",
				IntervalEnd:   "2016-01-04T01:00:00Z",
			},
		},
		{
			Description: "Get last 48 hours data",
			testContext: testContext{
				BaseJobWindowConfig:         windowConfig{"48h", "0h", "h", ""},
				BaseJobScheduleInterval:     "0 1 * * *",
				UpstreamJobScheduleInterval: "0 1 * * *",
			},
			Expectations: testExpectation{
				IntervalStart: "2016-01-02T01:00:00Z",
				IntervalEnd:   "2016-01-04T01:00:00Z",
			},
		},
		{
			Description: "Get next 7 hours data",
			testContext: testContext{
				BaseJobWindowConfig:         windowConfig{"0h", "7h", "h", ""},
				BaseJobScheduleInterval:     "0 1 * * *",
				UpstreamJobScheduleInterval: "0 1 * * *",
			},
			Expectations: testExpectation{
				IntervalStart: "2016-01-04T01:00:00Z",
				IntervalEnd:   "2016-01-04T01:00:00Z",
			},
		},
	}

	// Test Execution
	for _, test := range tests {
		t.Run(test.Description, func(t *testing.T) {
			jobRepo := new(JobRepository)
			jobStartDateTime, err := time.Parse(time.RFC3339, jobStartDate)
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
					StartDate: jobStartDateTime,
					Interval:  test.testContext.UpstreamJobScheduleInterval,
				},
			}
			subjectJobWindow, err := getWindowConfig(test.testContext.BaseJobWindowConfig)
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
					StartDate: jobStartDateTime,
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

			baseJobScheduleTime, err := time.Parse(time.RFC3339, baseJobRunScheduleTime)
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
