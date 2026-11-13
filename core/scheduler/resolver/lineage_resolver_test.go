package resolver_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/goto/salt/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/goto/optimus/core/scheduler"
	"github.com/goto/optimus/core/scheduler/resolver"
	"github.com/goto/optimus/core/tenant"
	"github.com/goto/optimus/internal/lib/window"
)

func matchValues[T any](expected []T, getIdentifier func(T) string) any {
	return mock.MatchedBy(func(actual []T) bool {
		if len(expected) != len(actual) {
			return false
		}

		expectedMap := make(map[string]bool)
		for _, item := range expected {
			expectedMap[getIdentifier(item)] = true
		}

		for _, item := range actual {
			if !expectedMap[getIdentifier(item)] {
				return false
			}
		}

		return true
	})
}

func timePtr(t time.Time) *time.Time {
	return &t
}

func TestLineageResolver_BuildLineage(t *testing.T) {
	logger := log.NewNoop()
	ctx := context.Background()

	yesterdayPreset, _ := tenant.NewPreset("YESTERDAY", "preset for test", "1d", "0d", "", "d")
	multiDayPreset, _ := tenant.NewPreset("DAILY_LAST_6_DAYS", "preset for test", "6d", "0d", "", "d")
	presets := map[string]tenant.Preset{
		"yesterday":         yesterdayPreset,
		"daily_last_6_days": multiDayPreset,
	}
	yestWindowCfg, _ := window.NewPresetConfig("yesterday")
	multidayWindowCfg, _ := window.NewPresetConfig("daily_last_6_days")

	project, _ := tenant.NewProject("test-proj",
		map[string]string{
			"bucket":                     "gs://some_folder-2",
			tenant.ProjectSchedulerHost:  "host",
			tenant.ProjectStoragePathKey: "gs://location",
		}, map[string]string{})
	project.SetPresets(presets)
	namespace, _ := tenant.NewNamespace("test-ns", project.Name(), map[string]string{}, map[string]string{})
	jobTenant, _ := tenant.NewTenant(project.Name().String(), namespace.Name().String())

	// define jobs: job A depends on job B, job B depends on job C
	jobNameA := scheduler.JobName("job-a")
	jobNameB := scheduler.JobName("job-b")
	jobNameC := scheduler.JobName("job-c")

	jobAWithDetails := &scheduler.JobSummary{
		JobName:          jobNameA,
		IsEnabled:        true,
		Tenant:           jobTenant,
		Window:           yestWindowCfg,
		SLA:              scheduler.SLAConfig{},
		ScheduleInterval: "0 19 * * *",
	}

	jobBWithDetails := &scheduler.JobSummary{
		JobName:          jobNameB,
		IsEnabled:        true,
		Tenant:           jobTenant,
		Window:           multidayWindowCfg,
		SLA:              scheduler.SLAConfig{},
		ScheduleInterval: "0 13 * * *",
	}

	jobCWithDetails := &scheduler.JobSummary{
		JobName:          jobNameC,
		IsEnabled:        true,
		Tenant:           jobTenant,
		Window:           yestWindowCfg,
		SLA:              scheduler.SLAConfig{},
		ScheduleInterval: "0 7 * * *",
	}

	jobUpstreams := map[scheduler.JobName][]scheduler.JobName{
		jobNameA: {jobNameB},
		jobNameB: {jobNameC},
	}

	scheduledTime := time.Date(2023, 1, 1, 19, 0, 0, 0, time.UTC)

	t.Run("successfully builds lineage with upstreams", func(t *testing.T) {
		upstreamRepo := new(MockJobUpstreamRepository)
		jobRepo := new(MockJobRepository)
		jobRunService := new(MockJobRunService)
		projectGetter := new(MockProjectGetter)
		sampleSensorName := "sample-sensor"
		sampleTaskName := "sample-task"
		sampleHookName := "sample-hook"

		jobSchedules := []*scheduler.JobSchedule{
			{JobName: jobNameA, ScheduledAt: scheduledTime},
		}
		jobMap := map[scheduler.JobName]*scheduler.JobSummary{
			jobNameA: jobAWithDetails,
			jobNameB: jobBWithDetails,
			jobNameC: jobCWithDetails,
		}

		// expected job runs to be fetched
		// from jobA to jobB, expected run is 2023-01-01 13:00 UTC (yesterday of jobA's scheduled time)
		jobBExpectedSchedules := []time.Time{
			time.Date(2023, 1, 1, 13, 0, 0, 0, time.UTC),
		}
		// from jobB to jobC, expected runs are 2023-01-01 07:00 UTC, 2022-12-31 07:00 UTC, 2022-12-30 07:00 UTC (last 3 days of jobB's scheduled time)
		// and also 2023-12-29 07:00 UTC, 2023-12-28 07:00 UTC, 2023-12-27 07:00 UTC (last 3 days of jobB's scheduled time in previous year)
		jobCExpectedSchedules := []time.Time{
			time.Date(2023, 12, 27, 7, 0, 0, 0, time.UTC),
			time.Date(2023, 12, 28, 7, 0, 0, 0, time.UTC),
			time.Date(2023, 12, 29, 7, 0, 0, 0, time.UTC),
			time.Date(2022, 12, 30, 7, 0, 0, 0, time.UTC),
			time.Date(2022, 12, 31, 7, 0, 0, 0, time.UTC),
			time.Date(2023, 1, 1, 7, 0, 0, 0, time.UTC),
		}
		// total 8 job runs to be fetched
		selectedCSchedule := jobCExpectedSchedules[len(jobCExpectedSchedules)-1]

		// although there are multiple expected schedules for jobC, only fetch the latest one
		jobRunsToFetch := []scheduler.JobRunIdentifier{
			{JobName: jobNameA, ScheduledAt: time.Date(2023, 1, 1, 19, 0, 0, 0, time.UTC)},
			{JobName: jobNameB, ScheduledAt: time.Date(2023, 1, 1, 13, 0, 0, 0, time.UTC)},
			{JobName: jobNameC, ScheduledAt: time.Date(2023, 1, 1, 7, 0, 0, 0, time.UTC)},
		}

		fetchedJobRunSummaries := []*scheduler.JobRunSummary{
			{
				JobName:       jobNameA,
				ScheduledAt:   time.Date(2023, 1, 1, 19, 0, 0, 0, time.UTC),
				JobStartTime:  timePtr(time.Date(2023, 1, 1, 19, 0, 0, 0, time.UTC)),
				JobEndTime:    timePtr(time.Date(2023, 1, 1, 19, 30, 0, 0, time.UTC)),
				WaitStartTime: nil,
				WaitEndTime:   nil,
				TaskStartTime: timePtr(time.Date(2023, 1, 1, 19, 0, 0, 0, time.UTC)),
				TaskEndTime:   timePtr(time.Date(2023, 1, 1, 19, 15, 0, 0, time.UTC)),
				HookStartTime: timePtr(time.Date(2023, 1, 1, 19, 15, 0, 0, time.UTC)),
				HookEndTime:   timePtr(time.Date(2023, 1, 1, 19, 30, 0, 0, time.UTC)),
				SensorName:    nil,
				TaskName:      &sampleTaskName,
				HookName:      &sampleHookName,
				TaskAttempts:  1,
				HookAttempts:  1,
			},
			{
				JobName:       jobNameB,
				ScheduledAt:   time.Date(2023, 1, 1, 13, 0, 0, 0, time.UTC),
				JobStartTime:  timePtr(time.Date(2023, 1, 1, 13, 0, 0, 0, time.UTC)),
				JobEndTime:    timePtr(time.Date(2023, 1, 1, 13, 20, 0, 0, time.UTC)),
				WaitStartTime: timePtr(time.Date(2023, 1, 1, 13, 0, 0, 0, time.UTC)),
				WaitEndTime:   timePtr(time.Date(2023, 1, 1, 13, 8, 0, 0, time.UTC)),
				TaskStartTime: timePtr(time.Date(2023, 1, 1, 13, 8, 0, 0, time.UTC)),
				TaskEndTime:   timePtr(time.Date(2023, 1, 1, 13, 10, 0, 0, time.UTC)),
				HookStartTime: timePtr(time.Date(2023, 1, 1, 13, 10, 0, 0, time.UTC)),
				HookEndTime:   timePtr(time.Date(2023, 1, 1, 13, 20, 0, 0, time.UTC)),
				SensorName:    &sampleSensorName,
				TaskName:      &sampleTaskName,
				HookName:      &sampleHookName,
				TaskAttempts:  2,
				HookAttempts:  1,
			},
			{
				JobName:       jobNameC,
				ScheduledAt:   time.Date(2023, 1, 1, 7, 0, 0, 0, time.UTC),
				JobStartTime:  timePtr(time.Date(2023, 1, 1, 7, 0, 0, 0, time.UTC)),
				JobEndTime:    timePtr(time.Date(2023, 1, 1, 7, 10, 0, 0, time.UTC)),
				WaitStartTime: nil,
				WaitEndTime:   nil,
				TaskStartTime: timePtr(time.Date(2023, 1, 1, 7, 0, 0, 0, time.UTC)),
				TaskEndTime:   timePtr(time.Date(2023, 1, 1, 7, 5, 0, 0, time.UTC)),
				HookStartTime: timePtr(time.Date(2023, 1, 1, 7, 5, 0, 0, time.UTC)),
				HookEndTime:   timePtr(time.Date(2023, 1, 1, 7, 10, 0, 0, time.UTC)),
				SensorName:    &sampleSensorName,
				TaskName:      &sampleTaskName,
				HookName:      &sampleHookName,
				TaskAttempts:  1,
				HookAttempts:  3,
			},
		}

		upstreamRepo.On("GetAllResolvedUpstreams", ctx).Return(jobUpstreams, nil)
		jobRepo.On("GetSummaryByNames",
			ctx,
			matchValues([]scheduler.JobName{jobNameA, jobNameB, jobNameC},
				func(name scheduler.JobName) string {
					return name.String()
				}),
		).Return(jobMap, nil)
		projectGetter.On("Get", ctx, project.Name()).Return(project, nil)

		jobRunService.On("GetExpectedRunSchedules",
			ctx, project, jobAWithDetails.ScheduleInterval, jobAWithDetails.Window, jobBWithDetails.ScheduleInterval, scheduledTime).Return(jobBExpectedSchedules, nil)
		jobRunService.On("GetExpectedRunSchedules",
			ctx, project, jobBWithDetails.ScheduleInterval, jobBWithDetails.Window, jobCWithDetails.ScheduleInterval, jobBExpectedSchedules[0]).Return(jobCExpectedSchedules, nil)

		jobRunService.On("GetJobRunsByIdentifiers", ctx, matchValues(jobRunsToFetch, func(id scheduler.JobRunIdentifier) string {
			return id.JobName.String() + id.ScheduledAt.String()
		})).Return(fetchedJobRunSummaries, nil)

		resolver := resolver.NewLineageResolver(upstreamRepo, jobRepo, jobRunService, projectGetter, logger)

		resultMap, err := resolver.BuildLineage(ctx, jobSchedules, 24)

		assert.NoError(t, err)
		assert.Len(t, resultMap, 1)
		result := resultMap[jobSchedules[0]]

		// assert job A results
		assert.Equal(t, jobNameA, result.JobName)
		assert.EqualValues(t, result.ScheduleInterval, jobAWithDetails.ScheduleInterval)
		assert.EqualValues(t, result.Window, &yestWindowCfg)
		rootRun := result.GetTargetRun()
		if assert.NotNil(t, rootRun) {
			assert.Equal(t, scheduledTime, rootRun.ScheduledAt)
			assert.Equal(t, jobNameA, rootRun.JobName)
			assert.Equal(t, 1, rootRun.TaskAttempts)
			assert.Equal(t, 1, rootRun.HookAttempts)
		}

		// assert job B results
		assert.Len(t, result.Upstreams, 1)
		upstreamB := result.Upstreams[0]
		assert.Equal(t, jobNameB, upstreamB.JobName)
		assert.EqualValues(t, upstreamB.ScheduleInterval, jobBWithDetails.ScheduleInterval)
		assert.EqualValues(t, upstreamB.Window, &multidayWindowCfg)
		assert.Equal(t, 1, upstreamB.DownstreamRunCount())
		expectedBSchedule := jobBExpectedSchedules[0]
		runB := upstreamB.GetRunForDownstream(scheduler.NewJobRunKey(jobNameA, scheduledTime))
		if assert.NotNil(t, runB) {
			assert.Equal(t, expectedBSchedule, runB.ScheduledAt)
			assert.Equal(t, jobNameB, runB.JobName)
			assert.Equal(t, 2, runB.TaskAttempts)
			assert.Equal(t, 1, runB.HookAttempts)
		}

		// assert job C results
		assert.Len(t, upstreamB.Upstreams, 1)
		upstreamC := upstreamB.Upstreams[0]
		assert.Equal(t, jobNameC, upstreamC.JobName)
		assert.EqualValues(t, upstreamC.ScheduleInterval, jobCWithDetails.ScheduleInterval)
		assert.EqualValues(t, upstreamC.Window, &yestWindowCfg)
		assert.Equal(t, 1, upstreamC.DownstreamRunCount())
		expectedCSchedule := selectedCSchedule
		// job-c's immediate downstream is job-b, so its run is keyed by job-b, not the tree root
		runC := upstreamC.GetRunForDownstream(scheduler.NewJobRunKey(jobNameB, expectedBSchedule))
		if assert.NotNil(t, runC) {
			assert.Equal(t, expectedCSchedule, runC.ScheduledAt)
			assert.Equal(t, jobNameC, runC.JobName)
			assert.Equal(t, 1, runC.TaskAttempts)
			assert.Equal(t, 3, runC.HookAttempts)
		}

		assert.Empty(t, upstreamC.Upstreams)

		upstreamRepo.AssertExpectations(t)
		jobRepo.AssertExpectations(t)
		jobRunService.AssertExpectations(t)
		projectGetter.AssertExpectations(t)
	})

	t.Run("returns error when getting upstreams fails", func(t *testing.T) {
		upstreamRepo := new(MockJobUpstreamRepository)
		jobRepo := new(MockJobRepository)
		jobRunService := new(MockJobRunService)
		projectGetter := new(MockProjectGetter)

		jobSchedules := []*scheduler.JobSchedule{
			{JobName: jobNameA, ScheduledAt: scheduledTime},
		}

		expectedErr := errors.New("upstream repository error")
		upstreamRepo.On("GetAllResolvedUpstreams", ctx).Return(nil, expectedErr)

		resolver := resolver.NewLineageResolver(upstreamRepo, jobRepo, jobRunService, projectGetter, logger)

		result, err := resolver.BuildLineage(ctx, jobSchedules, 24)

		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Equal(t, expectedErr, err)

		upstreamRepo.AssertExpectations(t)
	})

	t.Run("returns error when finding jobs by names fails", func(t *testing.T) {
		upstreamRepo := new(MockJobUpstreamRepository)
		jobRepo := new(MockJobRepository)
		jobRunService := new(MockJobRunService)
		projectGetter := new(MockProjectGetter)

		jobSchedules := []*scheduler.JobSchedule{
			{JobName: jobNameA, ScheduledAt: scheduledTime},
		}

		upstreamsMap := map[scheduler.JobName][]scheduler.JobName{
			jobNameA: {jobNameB},
		}

		expectedErr := errors.New("job repository error")

		upstreamRepo.On("GetAllResolvedUpstreams", ctx).Return(upstreamsMap, nil)
		jobRepo.On("GetSummaryByNames", ctx, matchValues([]scheduler.JobName{jobNameA, jobNameB}, func(jobName scheduler.JobName) string {
			return string(jobName)
		})).Return(nil, expectedErr)

		resolver := resolver.NewLineageResolver(upstreamRepo, jobRepo, jobRunService, projectGetter, logger)

		result, err := resolver.BuildLineage(ctx, jobSchedules, 24)

		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Equal(t, expectedErr, err)

		upstreamRepo.AssertExpectations(t)
		jobRepo.AssertExpectations(t)
	})

	t.Run("returns error when getting project fails", func(t *testing.T) {
		upstreamRepo := new(MockJobUpstreamRepository)
		jobRepo := new(MockJobRepository)
		jobRunService := new(MockJobRunService)
		projectGetter := new(MockProjectGetter)

		jobSchedules := []*scheduler.JobSchedule{
			{JobName: jobNameA, ScheduledAt: scheduledTime},
		}

		upstreamsMap := map[scheduler.JobName][]scheduler.JobName{
			jobNameA: {jobNameB},
		}
		jobsByName := map[scheduler.JobName]*scheduler.JobSummary{
			jobNameB: jobBWithDetails,
		}
		expectedErr := errors.New("project getter error")

		upstreamRepo.On("GetAllResolvedUpstreams", ctx).Return(upstreamsMap, nil)
		jobRepo.On("GetSummaryByNames", ctx, matchValues([]scheduler.JobName{jobNameA, jobNameB}, func(jobName scheduler.JobName) string {
			return string(jobName)
		})).Return(jobsByName, nil)
		projectGetter.On("Get", ctx, project.Name()).Return(nil, expectedErr)

		resolver := resolver.NewLineageResolver(upstreamRepo, jobRepo, jobRunService, projectGetter, logger)

		result, err := resolver.BuildLineage(ctx, jobSchedules, 24)

		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Equal(t, expectedErr, err)

		upstreamRepo.AssertExpectations(t)
		jobRepo.AssertExpectations(t)
		projectGetter.AssertExpectations(t)
	})

	t.Run("returns error when getting expected run schedules fails", func(t *testing.T) {
		upstreamRepo := new(MockJobUpstreamRepository)
		jobRepo := new(MockJobRepository)
		jobRunService := new(MockJobRunService)
		projectGetter := new(MockProjectGetter)

		jobSchedules := []*scheduler.JobSchedule{
			{JobName: jobNameA, ScheduledAt: scheduledTime},
		}

		upstreamsMap := map[scheduler.JobName][]scheduler.JobName{
			jobNameA: {jobNameB},
		}

		jobsByName := map[scheduler.JobName]*scheduler.JobSummary{
			jobNameB: jobBWithDetails,
			jobNameA: jobAWithDetails,
		}

		expectedErr := errors.New("job run service error")

		upstreamRepo.On("GetAllResolvedUpstreams", ctx).Return(upstreamsMap, nil)
		jobRepo.On("GetSummaryByNames", ctx, matchValues([]scheduler.JobName{jobNameA, jobNameB}, func(jobName scheduler.JobName) string {
			return string(jobName)
		})).Return(jobsByName, nil)
		projectGetter.On("Get", ctx, project.Name()).Return(project, nil)
		jobRunService.On("GetExpectedRunSchedules", ctx, project, jobAWithDetails.ScheduleInterval, jobAWithDetails.Window, jobBWithDetails.ScheduleInterval, scheduledTime).Return(nil, expectedErr)

		resolver := resolver.NewLineageResolver(upstreamRepo, jobRepo, jobRunService, projectGetter, nil)

		result, err := resolver.BuildLineage(ctx, jobSchedules, 24)

		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Equal(t, expectedErr, err)

		upstreamRepo.AssertExpectations(t)
		jobRepo.AssertExpectations(t)
		projectGetter.AssertExpectations(t)
		jobRunService.AssertExpectations(t)
	})

	t.Run("returns error when getting job runs by identifiers fails", func(t *testing.T) {
		upstreamRepo := new(MockJobUpstreamRepository)
		jobRepo := new(MockJobRepository)
		jobRunService := new(MockJobRunService)
		projectGetter := new(MockProjectGetter)

		jobSchedules := []*scheduler.JobSchedule{
			{JobName: jobNameA, ScheduledAt: scheduledTime},
		}

		upstreamsMap := map[scheduler.JobName][]scheduler.JobName{
			jobNameA: {jobNameB},
		}

		jobsByName := map[scheduler.JobName]*scheduler.JobSummary{
			jobNameB: jobBWithDetails,
			jobNameA: jobAWithDetails,
		}

		expectedSchedules := []time.Time{scheduledTime.Add(-time.Hour)}
		expectedErr := errors.New("job run service error")

		upstreamRepo.On("GetAllResolvedUpstreams", ctx).Return(upstreamsMap, nil)
		jobRepo.On("GetSummaryByNames", ctx, matchValues([]scheduler.JobName{jobNameA, jobNameB}, func(jobName scheduler.JobName) string {
			return string(jobName)
		})).Return(jobsByName, nil)
		projectGetter.On("Get", ctx, project.Name()).Return(project, nil)
		jobRunService.On("GetExpectedRunSchedules", ctx, project, jobAWithDetails.ScheduleInterval, jobAWithDetails.Window, jobBWithDetails.ScheduleInterval, scheduledTime).Return(expectedSchedules, nil)
		jobRunService.On("GetJobRunsByIdentifiers", ctx, mock.AnythingOfType("[]scheduler.JobRunIdentifier")).Return(nil, expectedErr)

		resolver := resolver.NewLineageResolver(upstreamRepo, jobRepo, jobRunService, projectGetter, logger)

		result, err := resolver.BuildLineage(ctx, jobSchedules, 24)

		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Equal(t, expectedErr, err)

		upstreamRepo.AssertExpectations(t)
		jobRepo.AssertExpectations(t)
		projectGetter.AssertExpectations(t)
		jobRunService.AssertExpectations(t)
	})

	t.Run("handles empty job schedules", func(t *testing.T) {
		upstreamRepo := new(MockJobUpstreamRepository)
		jobRepo := new(MockJobRepository)
		jobRunService := new(MockJobRunService)
		projectGetter := new(MockProjectGetter)

		upstreamsMap := map[scheduler.JobName][]scheduler.JobName{}

		upstreamRepo.On("GetAllResolvedUpstreams", ctx).Return(upstreamsMap, nil)
		jobRepo.On("GetSummaryByNames", ctx, []scheduler.JobName{}).Return(map[scheduler.JobName]*scheduler.JobSummary{}, nil)

		resolver := resolver.NewLineageResolver(upstreamRepo, jobRepo, jobRunService, projectGetter, logger)

		result, err := resolver.BuildLineage(ctx, []*scheduler.JobSchedule{}, 24)

		assert.NoError(t, err)
		assert.Empty(t, result)

		upstreamRepo.AssertExpectations(t)
		jobRepo.AssertExpectations(t)
	})

	t.Run("handles jobs with no upstreams", func(t *testing.T) {
		upstreamRepo := new(MockJobUpstreamRepository)
		jobRepo := new(MockJobRepository)
		jobRunService := new(MockJobRunService)
		projectGetter := new(MockProjectGetter)

		jobSchedules := []*scheduler.JobSchedule{
			{JobName: jobNameA, ScheduledAt: scheduledTime},
		}

		upstreamsMap := map[scheduler.JobName][]scheduler.JobName{
			jobNameA: {},
		}

		expectedJobRunSummaries := []*scheduler.JobRunSummary{
			{
				JobName:     jobNameA,
				ScheduledAt: scheduledTime,
			},
		}

		upstreamRepo.On("GetAllResolvedUpstreams", ctx).Return(upstreamsMap, nil)
		jobRepo.On("GetSummaryByNames", ctx, []scheduler.JobName{jobNameA}).Return(map[scheduler.JobName]*scheduler.JobSummary{}, nil)
		projectGetter.On("Get", ctx, project.Name()).Return(project, nil)
		jobRunService.On("GetJobRunsByIdentifiers", ctx, []scheduler.JobRunIdentifier{
			{JobName: jobNameA, ScheduledAt: scheduledTime},
		}).Return(expectedJobRunSummaries, nil)

		resolver := resolver.NewLineageResolver(upstreamRepo, jobRepo, jobRunService, projectGetter, logger)

		resultMap, err := resolver.BuildLineage(ctx, jobSchedules, 24)

		assert.NoError(t, err)
		assert.Len(t, resultMap, 1)
		result := resultMap[jobSchedules[0]]

		assert.Equal(t, jobNameA, result.JobName)
		assert.Empty(t, result.Upstreams)

		upstreamRepo.AssertExpectations(t)
		jobRepo.AssertExpectations(t)
	})

	// this is an important assertion built around the functionality of lineage resolver
	// containing 2 implicit expectations
	// 1. jobs which is not running yet will still have job runs with empty start & end times,
	//    as long as the schedule time is within the valid lineage interval hours
	// 2. upstream jobs containing runs which are outside of the interval hours won't have the job run populated
	t.Run("successfully builds lineage for runs outside valid interval hours", func(t *testing.T) {
		// topology: A -> B -> C
		// A is not running yet, B already runs, C already runs but outside of the valid hours (10 hours)
		validLineageIntervalInHours := 10
		upstreamRepo := new(MockJobUpstreamRepository)
		jobRepo := new(MockJobRepository)
		jobRunService := new(MockJobRunService)
		projectGetter := new(MockProjectGetter)
		sampleSensorName := "sample-sensor"
		sampleTaskName := "sample-task"
		sampleHookName := "sample-hook"

		jobSchedules := []*scheduler.JobSchedule{
			{JobName: jobNameA, ScheduledAt: scheduledTime},
		}
		jobMap := map[scheduler.JobName]*scheduler.JobSummary{
			jobNameA: jobAWithDetails,
			jobNameB: jobBWithDetails,
			jobNameC: jobCWithDetails,
		}

		// expected job runs to be fetched
		// from jobA to jobB, expected run is 2023-01-01 13:00 UTC (yesterday of jobA's scheduled time)
		jobBExpectedSchedules := []time.Time{
			time.Date(2023, 1, 1, 13, 0, 0, 0, time.UTC),
		}
		// from jobB to jobC, expected runs are 2023-01-01 07:00 UTC, 2022-12-31 07:00 UTC, 2022-12-30 07:00 UTC (last 3 days of jobB's scheduled time)
		// and also 2023-12-29 07:00 UTC, 2023-12-28 07:00 UTC, 2023-12-27 07:00 UTC (last 3 days of jobB's scheduled time in previous year)
		// but since this is outside of the 10 hours range, it wont be considered
		jobCExpectedSchedules := []time.Time{
			time.Date(2022, 12, 27, 7, 0, 0, 0, time.UTC),
			time.Date(2022, 12, 28, 7, 0, 0, 0, time.UTC),
			time.Date(2022, 12, 29, 7, 0, 0, 0, time.UTC),
			time.Date(2022, 12, 30, 7, 0, 0, 0, time.UTC),
			time.Date(2022, 12, 31, 7, 0, 0, 0, time.UTC),
			time.Date(2023, 1, 1, 7, 0, 0, 0, time.UTC),
		}

		// although there are multiple expected schedules for jobC, only fetch the latest one
		jobRunsToFetch := []scheduler.JobRunIdentifier{
			{JobName: jobNameA, ScheduledAt: time.Date(2023, 1, 1, 19, 0, 0, 0, time.UTC)},
			{JobName: jobNameB, ScheduledAt: time.Date(2023, 1, 1, 13, 0, 0, 0, time.UTC)},
		}

		fetchedJobRunSummaries := []*scheduler.JobRunSummary{
			{
				JobName:       jobNameB,
				ScheduledAt:   time.Date(2023, 1, 1, 13, 0, 0, 0, time.UTC),
				JobStartTime:  timePtr(time.Date(2023, 1, 1, 13, 0, 0, 0, time.UTC)),
				JobEndTime:    timePtr(time.Date(2023, 1, 1, 13, 20, 0, 0, time.UTC)),
				WaitStartTime: timePtr(time.Date(2023, 1, 1, 13, 0, 0, 0, time.UTC)),
				WaitEndTime:   timePtr(time.Date(2023, 1, 1, 13, 8, 0, 0, time.UTC)),
				TaskStartTime: timePtr(time.Date(2023, 1, 1, 13, 8, 0, 0, time.UTC)),
				TaskEndTime:   timePtr(time.Date(2023, 1, 1, 13, 10, 0, 0, time.UTC)),
				HookStartTime: timePtr(time.Date(2023, 1, 1, 13, 10, 0, 0, time.UTC)),
				HookEndTime:   timePtr(time.Date(2023, 1, 1, 13, 20, 0, 0, time.UTC)),
				SensorName:    &sampleSensorName,
				TaskName:      &sampleTaskName,
				HookName:      &sampleHookName,
			},
		}

		upstreamRepo.On("GetAllResolvedUpstreams", ctx).Return(jobUpstreams, nil)
		jobRepo.On("GetSummaryByNames",
			ctx,
			matchValues([]scheduler.JobName{jobNameA, jobNameB, jobNameC},
				func(name scheduler.JobName) string {
					return name.String()
				}),
		).Return(jobMap, nil)
		projectGetter.On("Get", ctx, project.Name()).Return(project, nil)

		jobRunService.On("GetExpectedRunSchedules",
			ctx, project, jobAWithDetails.ScheduleInterval, jobAWithDetails.Window, jobBWithDetails.ScheduleInterval, scheduledTime).Return(jobBExpectedSchedules, nil)
		jobRunService.On("GetExpectedRunSchedules",
			ctx, project, jobBWithDetails.ScheduleInterval, jobBWithDetails.Window, jobCWithDetails.ScheduleInterval, jobBExpectedSchedules[0]).Return(jobCExpectedSchedules, nil)

		jobRunService.On("GetJobRunsByIdentifiers", ctx, matchValues(jobRunsToFetch, func(id scheduler.JobRunIdentifier) string {
			return id.JobName.String() + id.ScheduledAt.String()
		})).Return(fetchedJobRunSummaries, nil)

		resolver := resolver.NewLineageResolver(upstreamRepo, jobRepo, jobRunService, projectGetter, logger)

		resultMap, err := resolver.BuildLineage(ctx, jobSchedules, validLineageIntervalInHours)

		assert.NoError(t, err)
		assert.Len(t, resultMap, 1)
		result := resultMap[jobSchedules[0]]

		// assert job A results
		assert.Equal(t, jobNameA, result.JobName)
		assert.EqualValues(t, result.ScheduleInterval, jobAWithDetails.ScheduleInterval)
		assert.EqualValues(t, result.Window, &yestWindowCfg)
		rootRun := result.GetTargetRun()
		if assert.NotNil(t, rootRun) {
			assert.Equal(t, scheduledTime, rootRun.ScheduledAt)
			assert.Nil(t, rootRun.JobStartTime)
			assert.Nil(t, rootRun.JobEndTime)
			assert.Nil(t, rootRun.TaskStartTime)
			assert.Nil(t, rootRun.TaskEndTime)
		}

		// assert job B results
		assert.Len(t, result.Upstreams, 1)
		upstreamB := result.Upstreams[0]
		assert.Equal(t, jobNameB, upstreamB.JobName)
		assert.EqualValues(t, upstreamB.ScheduleInterval, jobBWithDetails.ScheduleInterval)
		assert.EqualValues(t, upstreamB.Window, &multidayWindowCfg)
		assert.Equal(t, 1, upstreamB.DownstreamRunCount())
		expectedBSchedule := jobBExpectedSchedules[0]
		runB := upstreamB.GetRunForDownstream(scheduler.NewJobRunKey(jobNameA, scheduledTime))
		if assert.NotNil(t, runB) {
			assert.Equal(t, expectedBSchedule, runB.ScheduledAt)
			assert.Equal(t, jobNameB, runB.JobName)
		}

		// assert job C results: should be empty
		assert.Len(t, upstreamB.Upstreams, 1)
		upstreamC := upstreamB.Upstreams[0]
		assert.Equal(t, jobNameC, upstreamC.JobName)
		assert.EqualValues(t, upstreamC.ScheduleInterval, jobCWithDetails.ScheduleInterval)
		assert.EqualValues(t, upstreamC.Window, &yestWindowCfg)
		assert.False(t, upstreamC.HasRuns())
		assert.Empty(t, upstreamC.Upstreams)

		upstreamRepo.AssertExpectations(t)
		jobRepo.AssertExpectations(t)
		jobRunService.AssertExpectations(t)
		projectGetter.AssertExpectations(t)
	})

	t.Run("diamond upstream keeps a distinct run per downstream path", func(t *testing.T) {
		// topology: A -> {B, C}, both B and C -> D. D is shared, but B and C imply different
		// actual schedules for D (different window/interval), so D must carry two distinct
		// runs - one attributed to B, one to C - instead of one clobbering the other.
		upstreamRepo := new(MockJobUpstreamRepository)
		jobRepo := new(MockJobRepository)
		jobRunService := new(MockJobRunService)
		projectGetter := new(MockProjectGetter)

		jobNameD := scheduler.JobName("job-d")

		jobDWithDetails := &scheduler.JobSummary{
			JobName:          jobNameD,
			IsEnabled:        true,
			Tenant:           jobTenant,
			Window:           yestWindowCfg,
			SLA:              scheduler.SLAConfig{},
			ScheduleInterval: "0 5 * * *",
		}

		jobSchedules := []*scheduler.JobSchedule{
			{JobName: jobNameA, ScheduledAt: scheduledTime},
		}

		upstreamsMap := map[scheduler.JobName][]scheduler.JobName{
			jobNameA: {jobNameB, jobNameC},
			jobNameB: {jobNameD},
			jobNameC: {jobNameD},
		}

		jobsByName := map[scheduler.JobName]*scheduler.JobSummary{
			jobNameA: jobAWithDetails,
			jobNameB: jobBWithDetails,
			jobNameC: jobCWithDetails,
			jobNameD: jobDWithDetails,
		}

		scheduleB := time.Date(2023, 1, 1, 13, 0, 0, 0, time.UTC)
		scheduleC := time.Date(2023, 1, 1, 7, 0, 0, 0, time.UTC)
		scheduleDViaB := time.Date(2023, 1, 1, 5, 0, 0, 0, time.UTC)
		scheduleDViaC := time.Date(2022, 12, 31, 5, 0, 0, 0, time.UTC)

		upstreamRepo.On("GetAllResolvedUpstreams", ctx).Return(upstreamsMap, nil)
		jobRepo.On("GetSummaryByNames",
			ctx,
			matchValues([]scheduler.JobName{jobNameA, jobNameB, jobNameC, jobNameD},
				func(name scheduler.JobName) string { return name.String() }),
		).Return(jobsByName, nil)
		projectGetter.On("Get", ctx, project.Name()).Return(project, nil)

		jobRunService.On("GetExpectedRunSchedules",
			ctx, project, jobAWithDetails.ScheduleInterval, jobAWithDetails.Window, jobBWithDetails.ScheduleInterval, scheduledTime,
		).Return([]time.Time{scheduleB}, nil)
		jobRunService.On("GetExpectedRunSchedules",
			ctx, project, jobAWithDetails.ScheduleInterval, jobAWithDetails.Window, jobCWithDetails.ScheduleInterval, scheduledTime,
		).Return([]time.Time{scheduleC}, nil)
		jobRunService.On("GetExpectedRunSchedules",
			ctx, project, jobBWithDetails.ScheduleInterval, jobBWithDetails.Window, jobDWithDetails.ScheduleInterval, scheduleB,
		).Return([]time.Time{scheduleDViaB}, nil)
		jobRunService.On("GetExpectedRunSchedules",
			ctx, project, jobCWithDetails.ScheduleInterval, jobCWithDetails.Window, jobDWithDetails.ScheduleInterval, scheduleC,
		).Return([]time.Time{scheduleDViaC}, nil)

		jobRunsToFetch := []scheduler.JobRunIdentifier{
			{JobName: jobNameA, ScheduledAt: scheduledTime},
			{JobName: jobNameB, ScheduledAt: scheduleB},
			{JobName: jobNameC, ScheduledAt: scheduleC},
			{JobName: jobNameD, ScheduledAt: scheduleDViaB},
			{JobName: jobNameD, ScheduledAt: scheduleDViaC},
		}
		jobRunService.On("GetJobRunsByIdentifiers", ctx, matchValues(jobRunsToFetch, func(id scheduler.JobRunIdentifier) string {
			return id.JobName.String() + id.ScheduledAt.String()
		})).Return([]*scheduler.JobRunSummary{}, nil)

		resolver := resolver.NewLineageResolver(upstreamRepo, jobRepo, jobRunService, projectGetter, logger)

		resultMap, err := resolver.BuildLineage(ctx, jobSchedules, 24*365)

		assert.NoError(t, err)
		result := resultMap[jobSchedules[0]]

		assert.Len(t, result.Upstreams, 2)
		upstreamB := result.Upstreams[0]
		upstreamC := result.Upstreams[1]
		assert.Equal(t, jobNameB, upstreamB.JobName)
		assert.Equal(t, jobNameC, upstreamC.JobName)

		// D is the same shared node reached from both B and C
		upstreamDViaB := upstreamB.Upstreams[0]
		upstreamDViaC := upstreamC.Upstreams[0]
		assert.Same(t, upstreamDViaB, upstreamDViaC)

		// D must carry both runs, keyed by its immediate downstream, not just the last one processed
		assert.Equal(t, 2, upstreamDViaB.DownstreamRunCount())
		runDViaB := upstreamDViaB.GetRunForDownstream(scheduler.NewJobRunKey(jobNameB, scheduleB))
		runDViaC := upstreamDViaB.GetRunForDownstream(scheduler.NewJobRunKey(jobNameC, scheduleC))
		if assert.NotNil(t, runDViaB) && assert.NotNil(t, runDViaC) {
			assert.Equal(t, scheduleDViaB, runDViaB.ScheduledAt)
			assert.Equal(t, scheduleDViaC, runDViaC.ScheduledAt)
			assert.Equal(t, 0, runDViaB.TaskAttempts)
			assert.Equal(t, 0, runDViaB.HookAttempts)
		}

		upstreamRepo.AssertExpectations(t)
		jobRepo.AssertExpectations(t)
		jobRunService.AssertExpectations(t)
		projectGetter.AssertExpectations(t)
	})

	t.Run("preserves distinct per-run edges across a 2-hop diamond sharing an intermediate job at different schedules", func(t *testing.T) {
		upstreamRepo := new(MockJobUpstreamRepository)
		jobRepo := new(MockJobRepository)
		jobRunService := new(MockJobRunService)
		projectGetter := new(MockProjectGetter)

		jobNameRoot := scheduler.JobName("job-root")
		jobNameD := scheduler.JobName("job-d")

		jobRootDetails := &scheduler.JobSummary{
			JobName:          jobNameRoot,
			IsEnabled:        true,
			Tenant:           jobTenant,
			Window:           yestWindowCfg,
			ScheduleInterval: "0 9 * * *",
		}
		jobDWithDetails := &scheduler.JobSummary{
			JobName:          jobNameD,
			IsEnabled:        true,
			Tenant:           jobTenant,
			Window:           yestWindowCfg,
			ScheduleInterval: "0 * * * *",
		}

		// Use non-UTC timezone on root to also verify UTC normalization in JobRunKey lookups.
		wib := time.FixedZone("WIB", 7*3600)
		rootScheduleWIB := time.Date(2023, 1, 1, 16, 0, 0, 0, wib) // 09:00 UTC
		rootScheduleUTC := rootScheduleWIB.UTC()
		scheduleA := time.Date(2023, 1, 1, 8, 0, 0, 0, time.UTC)
		scheduleC := time.Date(2023, 1, 1, 8, 0, 0, 0, time.UTC)
		scheduleB7am := time.Date(2023, 1, 1, 7, 0, 0, 0, time.UTC)
		scheduleB3am := time.Date(2023, 1, 1, 3, 0, 0, 0, time.UTC)
		scheduleD6am := time.Date(2023, 1, 1, 6, 0, 0, 0, time.UTC)
		scheduleD2am := time.Date(2023, 1, 1, 2, 0, 0, 0, time.UTC)

		jobSchedules := []*scheduler.JobSchedule{
			{JobName: jobNameRoot, ScheduledAt: rootScheduleWIB},
		}

		// Root -> {A, C} -> B -> D where A->B@7am->D@6am and C->B@3am->D@2am.
		upstreamsMap := map[scheduler.JobName][]scheduler.JobName{
			jobNameRoot: {jobNameA, jobNameC},
			jobNameA:    {jobNameB},
			jobNameC:    {jobNameB},
			jobNameB:    {jobNameD},
		}
		jobsByName := map[scheduler.JobName]*scheduler.JobSummary{
			jobNameRoot: jobRootDetails,
			jobNameA:    jobAWithDetails,
			jobNameB:    jobBWithDetails,
			jobNameC:    jobCWithDetails,
			jobNameD:    jobDWithDetails,
		}

		upstreamRepo.On("GetAllResolvedUpstreams", ctx).Return(upstreamsMap, nil)
		jobRepo.On("GetSummaryByNames",
			ctx,
			matchValues([]scheduler.JobName{jobNameRoot, jobNameA, jobNameB, jobNameC, jobNameD},
				func(name scheduler.JobName) string { return name.String() }),
		).Return(jobsByName, nil)
		projectGetter.On("Get", ctx, project.Name()).Return(project, nil)

		jobRunService.On("GetExpectedRunSchedules",
			ctx, project, jobRootDetails.ScheduleInterval, jobRootDetails.Window, jobAWithDetails.ScheduleInterval, rootScheduleWIB,
		).Return([]time.Time{scheduleA}, nil)
		jobRunService.On("GetExpectedRunSchedules",
			ctx, project, jobRootDetails.ScheduleInterval, jobRootDetails.Window, jobCWithDetails.ScheduleInterval, rootScheduleWIB,
		).Return([]time.Time{scheduleC}, nil)
		jobRunService.On("GetExpectedRunSchedules",
			ctx, project, jobAWithDetails.ScheduleInterval, jobAWithDetails.Window, jobBWithDetails.ScheduleInterval, scheduleA,
		).Return([]time.Time{scheduleB7am}, nil)
		jobRunService.On("GetExpectedRunSchedules",
			ctx, project, jobCWithDetails.ScheduleInterval, jobCWithDetails.Window, jobBWithDetails.ScheduleInterval, scheduleC,
		).Return([]time.Time{scheduleB3am}, nil)
		jobRunService.On("GetExpectedRunSchedules",
			ctx, project, jobBWithDetails.ScheduleInterval, jobBWithDetails.Window, jobDWithDetails.ScheduleInterval, scheduleB7am,
		).Return([]time.Time{scheduleD6am}, nil)
		jobRunService.On("GetExpectedRunSchedules",
			ctx, project, jobBWithDetails.ScheduleInterval, jobBWithDetails.Window, jobDWithDetails.ScheduleInterval, scheduleB3am,
		).Return([]time.Time{scheduleD2am}, nil)

		jobRunsToFetch := []scheduler.JobRunIdentifier{
			{JobName: jobNameRoot, ScheduledAt: rootScheduleWIB},
			{JobName: jobNameA, ScheduledAt: scheduleA},
			{JobName: jobNameC, ScheduledAt: scheduleC},
			{JobName: jobNameB, ScheduledAt: scheduleB7am},
			{JobName: jobNameB, ScheduledAt: scheduleB3am},
			{JobName: jobNameD, ScheduledAt: scheduleD6am},
			{JobName: jobNameD, ScheduledAt: scheduleD2am},
		}
		jobRunService.On("GetJobRunsByIdentifiers", ctx, matchValues(jobRunsToFetch, func(id scheduler.JobRunIdentifier) string {
			return id.JobName.String() + id.ScheduledAt.UTC().String()
		})).Return([]*scheduler.JobRunSummary{
			{JobName: jobNameRoot, ScheduledAt: rootScheduleUTC, TaskAttempts: 1},
			{JobName: jobNameB, ScheduledAt: scheduleB7am, TaskAttempts: 7},
			{JobName: jobNameB, ScheduledAt: scheduleB3am, TaskAttempts: 3},
			{JobName: jobNameD, ScheduledAt: scheduleD6am, TaskAttempts: 6},
			{JobName: jobNameD, ScheduledAt: scheduleD2am, TaskAttempts: 2},
		}, nil)

		r := resolver.NewLineageResolver(upstreamRepo, jobRepo, jobRunService, projectGetter, logger)
		resultMap, err := r.BuildLineage(ctx, jobSchedules, 24*365)

		assert.NoError(t, err)
		root := resultMap[jobSchedules[0]]
		assert.NotNil(t, root.GetTargetRun())
		assert.Equal(t, 1, root.GetTargetRun().TaskAttempts)

		nodeA := root.Upstreams[0]
		nodeC := root.Upstreams[1]
		assert.Same(t, nodeA.Upstreams[0], nodeC.Upstreams[0])
		nodeB := nodeA.Upstreams[0]
		assert.Equal(t, 2, nodeB.DownstreamRunCount())
		assert.Equal(t, scheduleB7am, nodeB.GetRunForDownstream(scheduler.NewJobRunKey(jobNameA, scheduleA)).ScheduledAt)
		assert.Equal(t, 7, nodeB.GetRunForDownstream(scheduler.NewJobRunKey(jobNameA, scheduleA)).TaskAttempts)
		assert.Equal(t, scheduleB3am, nodeB.GetRunForDownstream(scheduler.NewJobRunKey(jobNameC, scheduleC)).ScheduledAt)
		assert.Equal(t, 3, nodeB.GetRunForDownstream(scheduler.NewJobRunKey(jobNameC, scheduleC)).TaskAttempts)

		nodeD := nodeB.Upstreams[0]
		assert.Equal(t, 2, nodeD.DownstreamRunCount())
		runD6 := nodeD.GetRunForDownstream(scheduler.NewJobRunKey(jobNameB, scheduleB7am))
		runD2 := nodeD.GetRunForDownstream(scheduler.NewJobRunKey(jobNameB, scheduleB3am))
		assert.NotNil(t, runD6)
		assert.NotNil(t, runD2)
		assert.Equal(t, scheduleD6am, runD6.ScheduledAt)
		assert.Equal(t, 6, runD6.TaskAttempts)
		assert.Equal(t, scheduleD2am, runD2.ScheduledAt)
		assert.Equal(t, 2, runD2.TaskAttempts)
		assert.Nil(t, nodeD.GetRunForDownstream(scheduler.NewJobRunKey(jobNameB, time.Date(2023, 1, 1, 12, 0, 0, 0, time.UTC))))

		upstreamRepo.AssertExpectations(t)
		jobRepo.AssertExpectations(t)
		jobRunService.AssertExpectations(t)
		projectGetter.AssertExpectations(t)
	})

	t.Run("deduplicates shared upstream work when two branches reach the same (job, schedule) with different timezones", func(t *testing.T) {
		upstreamRepo := new(MockJobUpstreamRepository)
		jobRepo := new(MockJobRepository)
		jobRunService := new(MockJobRunService)
		projectGetter := new(MockProjectGetter)

		jobNameRoot := scheduler.JobName("job-root")
		jobRootDetails := &scheduler.JobSummary{
			JobName:          jobNameRoot,
			IsEnabled:        true,
			Tenant:           jobTenant,
			Window:           yestWindowCfg,
			ScheduleInterval: "0 9 * * *",
		}

		wib := time.FixedZone("WIB", 7*3600)
		rootSched := time.Date(2023, 1, 1, 9, 0, 0, 0, time.UTC)
		scheduleA := time.Date(2023, 1, 1, 8, 0, 0, 0, time.UTC)
		scheduleC := time.Date(2023, 1, 1, 7, 0, 0, 0, time.UTC)
		sharedBUTC := time.Date(2023, 1, 1, 5, 0, 0, 0, time.UTC)
		sharedBWIB := sharedBUTC.In(wib)
		scheduleD := time.Date(2023, 1, 1, 4, 0, 0, 0, time.UTC)

		jobNameD := scheduler.JobName("job-d")
		jobDDetails := &scheduler.JobSummary{
			JobName:          jobNameD,
			IsEnabled:        true,
			Tenant:           jobTenant,
			Window:           yestWindowCfg,
			ScheduleInterval: "0 4 * * *",
		}

		jobSchedules := []*scheduler.JobSchedule{{JobName: jobNameRoot, ScheduledAt: rootSched}}
		upstreamsMap := map[scheduler.JobName][]scheduler.JobName{
			jobNameRoot: {jobNameA, jobNameC},
			jobNameA:    {jobNameB},
			jobNameC:    {jobNameB},
			jobNameB:    {jobNameD},
		}
		jobsByName := map[scheduler.JobName]*scheduler.JobSummary{
			jobNameRoot: jobRootDetails,
			jobNameA:    jobAWithDetails,
			jobNameB:    jobBWithDetails,
			jobNameC:    jobCWithDetails,
			jobNameD:    jobDDetails,
		}

		upstreamRepo.On("GetAllResolvedUpstreams", ctx).Return(upstreamsMap, nil)
		jobRepo.On("GetSummaryByNames",
			ctx,
			matchValues([]scheduler.JobName{jobNameRoot, jobNameA, jobNameB, jobNameC, jobNameD},
				func(name scheduler.JobName) string { return name.String() }),
		).Return(jobsByName, nil)
		projectGetter.On("Get", ctx, project.Name()).Return(project, nil)

		jobRunService.On("GetExpectedRunSchedules", ctx, project, jobRootDetails.ScheduleInterval, jobRootDetails.Window, jobAWithDetails.ScheduleInterval, rootSched).
			Return([]time.Time{scheduleA}, nil)
		jobRunService.On("GetExpectedRunSchedules", ctx, project, jobRootDetails.ScheduleInterval, jobRootDetails.Window, jobCWithDetails.ScheduleInterval, rootSched).
			Return([]time.Time{scheduleC}, nil)
		jobRunService.On("GetExpectedRunSchedules", ctx, project, jobAWithDetails.ScheduleInterval, jobAWithDetails.Window, jobBWithDetails.ScheduleInterval, scheduleA).
			Return([]time.Time{sharedBUTC}, nil)
		jobRunService.On("GetExpectedRunSchedules", ctx, project, jobCWithDetails.ScheduleInterval, jobCWithDetails.Window, jobBWithDetails.ScheduleInterval, scheduleC).
			Return([]time.Time{sharedBWIB}, nil)
		// Assert B -> D schedule resolution is invoked only once despite UTC vs WIB representations of 05:00 UTC.
		jobRunService.On("GetExpectedRunSchedules", ctx, project, jobBWithDetails.ScheduleInterval, jobBWithDetails.Window, jobDDetails.ScheduleInterval, sharedBUTC).
			Return([]time.Time{scheduleD}, nil).Once()

		jobRunService.On("GetJobRunsByIdentifiers", ctx, matchValues([]scheduler.JobRunIdentifier{
			{JobName: jobNameRoot, ScheduledAt: rootSched},
			{JobName: jobNameA, ScheduledAt: scheduleA},
			{JobName: jobNameC, ScheduledAt: scheduleC},
			{JobName: jobNameB, ScheduledAt: sharedBUTC},
			{JobName: jobNameD, ScheduledAt: scheduleD},
		}, func(id scheduler.JobRunIdentifier) string {
			return id.JobName.String() + id.ScheduledAt.UTC().String()
		})).Return([]*scheduler.JobRunSummary{}, nil)

		r := resolver.NewLineageResolver(upstreamRepo, jobRepo, jobRunService, projectGetter, logger)
		resultMap, err := r.BuildLineage(ctx, jobSchedules, 24*365)

		assert.NoError(t, err)
		root := resultMap[jobSchedules[0]]
		nodeB := root.Upstreams[0].Upstreams[0]
		runBFromA := nodeB.GetRunForDownstream(scheduler.NewJobRunKey(jobNameA, scheduleA))
		runBFromC := nodeB.GetRunForDownstream(scheduler.NewJobRunKey(jobNameC, scheduleC))
		assert.Equal(t, sharedBUTC, runBFromA.ScheduledAt)
		assert.True(t, sharedBWIB.Equal(runBFromC.ScheduledAt))
		assert.Same(t, runBFromA, runBFromC)
		assert.Equal(t, scheduleD, nodeB.Upstreams[0].GetRunForDownstream(scheduler.NewJobRunKey(jobNameB, sharedBUTC)).ScheduledAt)

		upstreamRepo.AssertExpectations(t)
		jobRepo.AssertExpectations(t)
		jobRunService.AssertExpectations(t)
		projectGetter.AssertExpectations(t)
	})
}

type MockJobUpstreamRepository struct {
	mock.Mock
}

func (m *MockJobUpstreamRepository) GetAllResolvedUpstreams(ctx context.Context) (map[scheduler.JobName][]scheduler.JobName, error) {
	args := m.Called(ctx)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(map[scheduler.JobName][]scheduler.JobName), args.Error(1)
}

type MockJobRepository struct {
	mock.Mock
}

func (m *MockJobRepository) GetSummaryByNames(ctx context.Context, jobNames []scheduler.JobName) (map[scheduler.JobName]*scheduler.JobSummary, error) {
	args := m.Called(ctx, jobNames)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(map[scheduler.JobName]*scheduler.JobSummary), args.Error(1)
}

type MockJobRunService struct {
	mock.Mock
}

func (m *MockJobRunService) GetExpectedRunSchedules(ctx context.Context, sourceProject *tenant.Project, sourceSchedule string, sourceWindow window.Config, upstreamSchedule string, referenceTime time.Time) ([]time.Time, error) {
	args := m.Called(ctx, sourceProject, sourceSchedule, sourceWindow, upstreamSchedule, referenceTime)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]time.Time), args.Error(1)
}

func (m *MockJobRunService) GetJobRunsByIdentifiers(ctx context.Context, jobRuns []scheduler.JobRunIdentifier) ([]*scheduler.JobRunSummary, error) {
	args := m.Called(ctx, jobRuns)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]*scheduler.JobRunSummary), args.Error(1)
}

type MockProjectGetter struct {
	mock.Mock
}

func (m *MockProjectGetter) Get(ctx context.Context, projectName tenant.ProjectName) (*tenant.Project, error) {
	args := m.Called(ctx, projectName)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*tenant.Project), args.Error(1)
}
