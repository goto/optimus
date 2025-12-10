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

		resultMap, err := resolver.BuildLineage(ctx, jobSchedules, 0, 24)

		assert.NoError(t, err)
		assert.Len(t, resultMap, 1)
		result := resultMap[jobSchedules[0]]

		// assert job A results
		assert.Equal(t, jobNameA, result.JobName)
		assert.EqualValues(t, result.ScheduleInterval, jobAWithDetails.ScheduleInterval)
		assert.EqualValues(t, result.Window, &yestWindowCfg)
		assert.Equal(t, len(result.JobRuns), 1)
		assert.Equal(t, scheduledTime, result.JobRuns[jobNameA].ScheduledAt)
		assert.Equal(t, jobNameA, result.JobRuns[jobNameA].JobName)

		// assert job B results
		assert.Len(t, result.Upstreams, 1)
		upstreamB := result.Upstreams[0]
		assert.Equal(t, jobNameB, upstreamB.JobName)
		assert.EqualValues(t, upstreamB.ScheduleInterval, jobBWithDetails.ScheduleInterval)
		assert.EqualValues(t, upstreamB.Window, &multidayWindowCfg)
		assert.Len(t, upstreamB.JobRuns, 1)
		expectedBSchedule := jobBExpectedSchedules[0]
		assert.Equal(t, expectedBSchedule, upstreamB.JobRuns[jobNameA].ScheduledAt)
		assert.Equal(t, jobNameB, upstreamB.JobRuns[jobNameA].JobName)

		// assert job C results
		assert.Len(t, upstreamB.Upstreams, 1)
		upstreamC := upstreamB.Upstreams[0]
		assert.Equal(t, jobNameC, upstreamC.JobName)
		assert.EqualValues(t, upstreamC.ScheduleInterval, jobCWithDetails.ScheduleInterval)
		assert.EqualValues(t, upstreamC.Window, &yestWindowCfg)
		assert.Len(t, upstreamC.JobRuns, 1)
		expectedCSchedule := selectedCSchedule
		assert.Equal(t, expectedCSchedule, upstreamC.JobRuns[jobNameA].ScheduledAt)
		assert.Equal(t, jobNameC, upstreamC.JobRuns[jobNameA].JobName)

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

		result, err := resolver.BuildLineage(ctx, jobSchedules, 0, 24)

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

		result, err := resolver.BuildLineage(ctx, jobSchedules, 0, 24)

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

		result, err := resolver.BuildLineage(ctx, jobSchedules, 0, 24)

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

		result, err := resolver.BuildLineage(ctx, jobSchedules, 0, 24)

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

		result, err := resolver.BuildLineage(ctx, jobSchedules, 0, 24)

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

		result, err := resolver.BuildLineage(ctx, []*scheduler.JobSchedule{}, 0, 24)

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

		resultMap, err := resolver.BuildLineage(ctx, jobSchedules, 0, 24)

		assert.NoError(t, err)
		assert.Len(t, resultMap, 1)
		result := resultMap[jobSchedules[0]]

		assert.Equal(t, jobNameA, result.JobName)
		assert.Empty(t, result.Upstreams)

		upstreamRepo.AssertExpectations(t)
		jobRepo.AssertExpectations(t)
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
