package resolver_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/goto/optimus/core/scheduler"
	"github.com/goto/optimus/core/scheduler/resolver"
	"github.com/goto/optimus/core/tenant"
	"github.com/goto/optimus/internal/lib/window"
)

func TestLineageResolver_BuildLineage(t *testing.T) {
	ctx := context.Background()

	yesterdayPreset, _ := tenant.NewPreset("YESTERDAY", "preset for test", "1d", "0d", "", "d")
	multiDayPreset, _ := tenant.NewPreset("DAILY_LAST_6_DAYS", "preset for test", "6d", "0d", "", "d")
	presets := map[string]tenant.Preset{
		"yesterday":         yesterdayPreset,
		"daily_last_6_days": multiDayPreset,
	}
	yestWindowCfg, _ := window.NewPresetConfig("yesterday")
	multidayWindowCfg, _ := window.NewPresetConfig("daily_last_6_days")

	project, _ := tenant.NewProject("test-proj", map[string]string{}, map[string]string{})
	project.SetPresets(presets)
	namespace, _ := tenant.NewNamespace("test-ns", project.Name(), map[string]string{}, map[string]string{})
	jobTenant, _ := tenant.NewTenant(project.Name().String(), namespace.Name().String())

	// define jobs: job A depends on job B, job B depends on job C
	jobNameA := scheduler.JobName("job-a")
	jobNameB := scheduler.JobName("job-b")
	jobNameC := scheduler.JobName("job-c")

	jobA := &scheduler.Job{
		Name:         jobNameA,
		Tenant:       jobTenant,
		WindowConfig: yestWindowCfg,
	}
	jobB := &scheduler.Job{
		Name:         jobNameB,
		Tenant:       jobTenant,
		WindowConfig: multidayWindowCfg,
	}
	jobC := &scheduler.Job{
		Name:         jobNameC,
		Tenant:       jobTenant,
		WindowConfig: yestWindowCfg,
	}

	jobAWithDetails := &scheduler.JobWithDetails{
		Name: jobNameA,
		Job:  jobA,
		Schedule: &scheduler.Schedule{
			Interval: "0 19 * * *",
		},
	}

	jobBWithDetails := &scheduler.JobWithDetails{
		Name: jobNameB,
		Job:  jobB,
		Schedule: &scheduler.Schedule{
			Interval: "0 13 * * *",
		},
	}

	jobCWithDetails := &scheduler.JobWithDetails{
		Name: jobNameC,
		Job:  jobC,
		Schedule: &scheduler.Schedule{
			Interval: "0 7 * * *",
		},
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
		jobMap := map[scheduler.JobName]*scheduler.JobWithDetails{
			jobNameA: jobAWithDetails,
			jobNameB: jobBWithDetails,
			jobNameC: jobCWithDetails,
		}

		// expected job runs to be fetched

		jobRunsToFetch := []scheduler.JobRunIdentifier{
			{JobName: jobNameA, ScheduledAt: time.Date(2023, 1, 1, 19, 0, 0, 0, time.UTC)},
			{JobName: jobNameB, ScheduledAt: time.Date(2023, 1, 1, 13, 0, 0, 0, time.UTC)},
			{JobName: jobNameC, ScheduledAt: time.Date(2023, 1, 1, 7, 0, 0, 0, time.UTC)},
			{JobName: jobNameC, ScheduledAt: time.Date(2022, 12, 31, 7, 0, 0, 0, time.UTC)},
			{JobName: jobNameC, ScheduledAt: time.Date(2022, 12, 30, 7, 0, 0, 0, time.UTC)},
			{JobName: jobNameC, ScheduledAt: time.Date(2023, 12, 29, 7, 0, 0, 0, time.UTC)},
			{JobName: jobNameC, ScheduledAt: time.Date(2023, 12, 28, 7, 0, 0, 0, time.UTC)},
			{JobName: jobNameC, ScheduledAt: time.Date(2023, 12, 27, 7, 0, 0, 0, time.UTC)},
		}

		fetchedJobRunSummaries := []*scheduler.JobRunSummary{
			{
				JobName:     jobNameA,
				ScheduledAt: time.Date(2023, 1, 1, 19, 0, 0, 0, time.UTC),
			},
			{
				JobName:     jobNameB,
				ScheduledAt: time.Date(2023, 1, 1, 13, 0, 0, 0, time.UTC),
			},
			{
				JobName:     jobNameC,
				ScheduledAt: time.Date(2023, 1, 1, 7, 0, 0, 0, time.UTC),
			},
			{
				JobName:     jobNameC,
				ScheduledAt: time.Date(2022, 12, 31, 7, 0, 0, 0, time.UTC),
			},
			{
				JobName:     jobNameC,
				ScheduledAt: time.Date(2022, 12, 30, 7, 0, 0, 0, time.UTC),
			},
			{
				JobName:     jobNameC,
				ScheduledAt: time.Date(2023, 12, 29, 7, 0, 0, 0, time.UTC),
			},
			{
				JobName:     jobNameC,
				ScheduledAt: time.Date(2023, 12, 28, 7, 0, 0, 0, time.UTC),
			},
			{
				JobName:     jobNameC,
				ScheduledAt: time.Date(2023, 12, 27, 7, 0, 0, 0, time.UTC),
			},
		}

		upstreamRepo.On("GetAllResolvedUpstreams", ctx).Return(jobUpstreams, nil)
		jobRepo.On("FindByNames", ctx, []scheduler.JobName{jobNameA, jobNameB, jobNameC}).Return(jobMap, nil)
		projectGetter.On("Get", ctx, project.Name()).Return(project, nil)

		jobRunService.On("GetExpectedRunSchedules", ctx, project, jobRunsToFetch).Return(fetchedJobRunSummaries, nil)

		jobRunService.On("GetJobRunsByIdentifiers", ctx, jobRunsToFetch).Return(fetchedJobRunSummaries, nil)

		resolver := resolver.NewLineageResolver(upstreamRepo, jobRepo, jobRunService, projectGetter)

		result, err := resolver.BuildLineage(ctx, jobSchedules)

		assert.NoError(t, err)
		assert.Len(t, result, 1)
		assert.Equal(t, jobNameA, result[0].JobName)
		assert.Len(t, result[0].Upstreams, 1)
		assert.Equal(t, jobNameB, result[0].Upstreams[0].JobName)
		assert.Len(t, result[0].Upstreams[0].Upstreams, 1)
		assert.Equal(t, jobNameC, result[0].Upstreams[0].Upstreams[0].JobName)

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

		resolver := resolver.NewLineageResolver(upstreamRepo, jobRepo, jobRunService, projectGetter)

		result, err := resolver.BuildLineage(ctx, jobSchedules)

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
		jobRepo.On("FindByNames", ctx, []scheduler.JobName{jobNameB}).Return(nil, expectedErr)

		resolver := resolver.NewLineageResolver(upstreamRepo, jobRepo, jobRunService, projectGetter)

		result, err := resolver.BuildLineage(ctx, jobSchedules)

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
		jobsByName := map[scheduler.JobName]*scheduler.JobWithDetails{
			jobNameB: jobBWithDetails,
		}
		expectedErr := errors.New("project getter error")

		upstreamRepo.On("GetAllResolvedUpstreams", ctx).Return(upstreamsMap, nil)
		jobRepo.On("FindByNames", ctx, []scheduler.JobName{jobNameB}).Return(jobsByName, nil)
		projectGetter.On("Get", ctx, project.Name()).Return(nil, expectedErr)

		resolver := resolver.NewLineageResolver(upstreamRepo, jobRepo, jobRunService, projectGetter)

		result, err := resolver.BuildLineage(ctx, jobSchedules)

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

		jobsByName := map[scheduler.JobName]*scheduler.JobWithDetails{
			jobNameB: jobBWithDetails,
		}

		expectedErr := errors.New("job run service error")

		upstreamRepo.On("GetAllResolvedUpstreams", ctx).Return(upstreamsMap, nil)
		jobRepo.On("FindByNames", ctx, []scheduler.JobName{jobNameB}).Return(jobsByName, nil)
		projectGetter.On("Get", ctx, project.Name()).Return(project, nil)
		jobRunService.On("GetExpectedRunSchedules", ctx, project, mock.AnythingOfType("*scheduler.JobWithDetails"), jobBWithDetails, scheduledTime).Return(nil, expectedErr)

		resolver := resolver.NewLineageResolver(upstreamRepo, jobRepo, jobRunService, projectGetter)

		result, err := resolver.BuildLineage(ctx, jobSchedules)

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

		jobsByName := map[scheduler.JobName]*scheduler.JobWithDetails{
			jobNameB: jobBWithDetails,
		}

		expectedSchedules := []time.Time{scheduledTime.Add(-time.Hour)}
		expectedErr := errors.New("job run service error")

		upstreamRepo.On("GetAllResolvedUpstreams", ctx).Return(upstreamsMap, nil)
		jobRepo.On("FindByNames", ctx, []scheduler.JobName{jobNameB}).Return(jobsByName, nil)
		projectGetter.On("Get", ctx, project.Name()).Return(project, nil)
		jobRunService.On("GetExpectedRunSchedules", ctx, project, mock.AnythingOfType("*scheduler.JobWithDetails"), jobBWithDetails, scheduledTime).Return(expectedSchedules, nil)
		jobRunService.On("GetJobRunsByIdentifiers", ctx, mock.AnythingOfType("[]scheduler.JobRunIdentifier")).Return(nil, expectedErr)

		resolver := resolver.NewLineageResolver(upstreamRepo, jobRepo, jobRunService, projectGetter)

		result, err := resolver.BuildLineage(ctx, jobSchedules)

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
		jobRepo.On("FindByNames", ctx, []scheduler.JobName{}).Return(map[scheduler.JobName]*scheduler.JobWithDetails{}, nil)

		resolver := resolver.NewLineageResolver(upstreamRepo, jobRepo, jobRunService, projectGetter)

		result, err := resolver.BuildLineage(ctx, []*scheduler.JobSchedule{})

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

		upstreamRepo.On("GetAllResolvedUpstreams", ctx).Return(upstreamsMap, nil)
		jobRepo.On("FindByNames", ctx, []scheduler.JobName{}).Return(map[scheduler.JobName]*scheduler.JobWithDetails{}, nil)

		resolver := resolver.NewLineageResolver(upstreamRepo, jobRepo, jobRunService, projectGetter)

		result, err := resolver.BuildLineage(ctx, jobSchedules)

		assert.NoError(t, err)
		assert.Len(t, result, 1)
		assert.Equal(t, jobNameA, result[0].JobName)
		assert.Empty(t, result[0].Upstreams)

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

func (m *MockJobRepository) FindByNames(ctx context.Context, jobNames []scheduler.JobName) (map[scheduler.JobName]*scheduler.JobWithDetails, error) {
	args := m.Called(ctx, jobNames)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(map[scheduler.JobName]*scheduler.JobWithDetails), args.Error(1)
}

type MockJobRunService struct {
	mock.Mock
}

func (m *MockJobRunService) GetExpectedRunSchedules(ctx context.Context, sourceProject *tenant.Project, sourceJob *scheduler.JobWithDetails, upstreamJob *scheduler.JobWithDetails, referenceTime time.Time) ([]time.Time, error) {
	args := m.Called(ctx, sourceProject, sourceJob, upstreamJob, referenceTime)
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
