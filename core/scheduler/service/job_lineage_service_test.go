package service_test

import (
	"context"
	"testing"
	"time"

	"github.com/goto/salt/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/goto/optimus/core/scheduler"
	"github.com/goto/optimus/core/scheduler/service"
)

func ptr[T any](v T) *T {
	return &v
}

func TestJobLineageService_GetJobExecutionSummary(t *testing.T) {
	ctx := context.Background()
	l := log.NewNoop()
	defaultHistoricalDurationLastNRuns := 10
	defaultHistoricalDurationPercentile := 50
	defaultMaxLineageDepth := 5

	t.Run("when lineage builder returns error, propagate error", func(t *testing.T) {
		lineageBuilder := NewMockLineageBuilder(t)
		durationEstimator := NewMockDurationEstimatorRepo(t)
		svc := service.NewJobLineageService(l, lineageBuilder, durationEstimator, defaultHistoricalDurationLastNRuns, defaultHistoricalDurationPercentile, defaultMaxLineageDepth)

		jobSchedule := &scheduler.JobSchedule{JobName: "job-A", ScheduledAt: time.Now().UTC()}
		lineageBuilder.On("BuildLineage", ctx, []*scheduler.JobSchedule{jobSchedule}, 24).Return(nil, assert.AnError).Once()

		result, err := svc.GetJobExecutionSummary(ctx, []*scheduler.JobSchedule{jobSchedule}, 1)

		assert.Error(t, err)
		assert.Nil(t, result)
	})

	t.Run("when lineage builder returns empty map, return nil result", func(t *testing.T) {
		lineageBuilder := NewMockLineageBuilder(t)
		defer lineageBuilder.AssertExpectations(t)
		durationEstimator := NewMockDurationEstimatorRepo(t)
		defer durationEstimator.AssertExpectations(t)
		svc := service.NewJobLineageService(l, lineageBuilder, durationEstimator, defaultHistoricalDurationLastNRuns, defaultHistoricalDurationPercentile, defaultMaxLineageDepth)

		lineageBuilder.On("BuildLineage", ctx, []*scheduler.JobSchedule{}, 24).Return(map[*scheduler.JobSchedule]*scheduler.JobLineageSummary{}, nil).Once()

		result, err := svc.GetJobExecutionSummary(ctx, []*scheduler.JobSchedule{}, 1)

		assert.NoError(t, err)
		assert.Nil(t, result)
	})

	t.Run("when lineage and duration estimation succeed, return enriched job run lineages", func(t *testing.T) {
		lineageBuilder := NewMockLineageBuilder(t)
		defer lineageBuilder.AssertExpectations(t)
		durationEstimator := NewMockDurationEstimatorRepo(t)
		defer durationEstimator.AssertExpectations(t)
		svc := service.NewJobLineageService(l, lineageBuilder, durationEstimator, defaultHistoricalDurationLastNRuns, defaultHistoricalDurationPercentile, defaultMaxLineageDepth)

		scheduledAt := time.Now().UTC().Truncate(time.Second)
		jobSchedule := &scheduler.JobSchedule{JobName: "job-A", ScheduledAt: scheduledAt}
		lineageSummary := &scheduler.JobLineageSummary{
			JobName:   "job-A",
			IsEnabled: true,
			JobRuns: map[scheduler.JobName]*scheduler.JobRunSummary{
				"job-A": {JobName: "job-A", ScheduledAt: scheduledAt, HookName: nil},
			},
			Upstreams: []*scheduler.JobLineageSummary{},
		}

		lineageBuilder.On("BuildLineage", ctx, []*scheduler.JobSchedule{jobSchedule}, 24).Return(
			map[*scheduler.JobSchedule]*scheduler.JobLineageSummary{jobSchedule: lineageSummary}, nil,
		).Once()
		durationEstimator.On("GetPercentileDurationByJobNames",
			ctx,
			[]scheduler.JobName{"job-A"},
			map[string][]string{"task": {}},
			scheduledAt, defaultHistoricalDurationLastNRuns, defaultHistoricalDurationPercentile,
		).Return(map[scheduler.JobName]*time.Duration{
			scheduler.JobName("job-A"): ptr(30 * time.Second),
		}, nil).Once()

		result, err := svc.GetJobExecutionSummary(ctx, []*scheduler.JobSchedule{jobSchedule}, 1)

		assert.NoError(t, err)
		assert.Len(t, result, 1)
		assert.Equal(t, scheduler.JobName("job-A"), result[0].JobName)
		assert.Equal(t, 30*time.Second, result[0].JobRuns[0].HistoricalSummary.TaskDuration)
	})

	t.Run("when duration estimator fails, skip lineage and return no error", func(t *testing.T) {
		lineageBuilder := NewMockLineageBuilder(t)
		defer lineageBuilder.AssertExpectations(t)
		durationEstimator := NewMockDurationEstimatorRepo(t)
		defer durationEstimator.AssertExpectations(t)
		svc := service.NewJobLineageService(l, lineageBuilder, durationEstimator, defaultHistoricalDurationLastNRuns, defaultHistoricalDurationPercentile, defaultMaxLineageDepth)

		scheduledAt := time.Now().UTC().Truncate(time.Second)
		jobSchedule := &scheduler.JobSchedule{JobName: "job-A", ScheduledAt: scheduledAt}
		lineageSummary := &scheduler.JobLineageSummary{
			JobName:   "job-A",
			IsEnabled: true,
			JobRuns: map[scheduler.JobName]*scheduler.JobRunSummary{
				"job-A": {JobName: "job-A", ScheduledAt: scheduledAt},
			},
			Upstreams: []*scheduler.JobLineageSummary{},
		}

		lineageBuilder.On("BuildLineage", ctx, []*scheduler.JobSchedule{jobSchedule}, 24).Return(
			map[*scheduler.JobSchedule]*scheduler.JobLineageSummary{jobSchedule: lineageSummary}, nil,
		).Once()
		durationEstimator.On("GetPercentileDurationByJobNames",
			ctx, mock.Anything, map[string][]string{"task": {}}, scheduledAt, defaultHistoricalDurationLastNRuns, defaultHistoricalDurationPercentile,
		).Return(nil, assert.AnError).Once()

		result, err := svc.GetJobExecutionSummary(ctx, []*scheduler.JobSchedule{jobSchedule}, 5)

		assert.NoError(t, err)
		assert.Len(t, result, 1)
		assert.Equal(t, scheduler.JobName("job-A"), result[0].JobName)
		assert.Equal(t, time.Duration(0), result[0].JobRuns[0].HistoricalSummary.TaskDuration)
	})

	t.Run("when job run has a hook, enrich hook durations for it", func(t *testing.T) {
		lineageBuilder := NewMockLineageBuilder(t)
		defer lineageBuilder.AssertExpectations(t)
		durationEstimator := NewMockDurationEstimatorRepo(t)
		defer durationEstimator.AssertExpectations(t)
		svc := service.NewJobLineageService(l, lineageBuilder, durationEstimator, defaultHistoricalDurationLastNRuns, defaultHistoricalDurationPercentile, defaultMaxLineageDepth)

		scheduledAt := time.Now().UTC().Truncate(time.Second)
		hookName := "my-hook"
		jobSchedule := &scheduler.JobSchedule{JobName: "job-A", ScheduledAt: scheduledAt}
		lineageSummary := &scheduler.JobLineageSummary{
			JobName:   "job-A",
			IsEnabled: true,
			JobRuns: map[scheduler.JobName]*scheduler.JobRunSummary{
				"job-A": {JobName: "job-A", ScheduledAt: scheduledAt, HookName: &hookName},
			},
			Upstreams: []*scheduler.JobLineageSummary{},
		}

		lineageBuilder.On("BuildLineage", ctx, []*scheduler.JobSchedule{jobSchedule}, 24).Return(
			map[*scheduler.JobSchedule]*scheduler.JobLineageSummary{jobSchedule: lineageSummary}, nil,
		).Once()
		durationEstimator.On("GetPercentileDurationByJobNames",
			ctx,
			mock.MatchedBy(func(names []scheduler.JobName) bool {
				return len(names) == 1 && names[0] == "job-A"
			}),
			map[string][]string{"task": {}},
			scheduledAt, defaultHistoricalDurationLastNRuns, defaultHistoricalDurationPercentile,
		).Return(map[scheduler.JobName]*time.Duration{
			scheduler.JobName("job-A"): ptr(30 * time.Second),
		}, nil).Once()
		durationEstimator.On("GetPercentileDurationByJobNames",
			ctx,
			mock.MatchedBy(func(names []scheduler.JobName) bool {
				return len(names) == 1 && names[0] == "job-A"
			}),
			map[string][]string{"hook": {hookName}},
			scheduledAt, defaultHistoricalDurationLastNRuns, defaultHistoricalDurationPercentile,
		).Return(map[scheduler.JobName]*time.Duration{
			scheduler.JobName("job-A"): ptr(10 * time.Second),
		}, nil).Once()

		result, err := svc.GetJobExecutionSummary(ctx, []*scheduler.JobSchedule{jobSchedule}, 5)

		assert.NoError(t, err)
		assert.Len(t, result, 1)
		assert.Equal(t, scheduler.JobName("job-A"), result[0].JobName)
		assert.Equal(t, 30*time.Second, result[0].JobRuns[0].HistoricalSummary.TaskDuration)
		assert.Equal(t, 10*time.Second, result[0].JobRuns[0].HistoricalSummary.HookDuration)
	})
}

// MockLineageBuilder is a mock for the LineageBuilder interface.
type MockLineageBuilder struct {
	mock.Mock
}

func (m *MockLineageBuilder) BuildLineage(ctx context.Context, jobSchedules []*scheduler.JobSchedule, depth int) (map[*scheduler.JobSchedule]*scheduler.JobLineageSummary, error) {
	ret := m.Called(ctx, jobSchedules, depth)

	if len(ret) == 0 {
		panic("no return value specified for BuildLineage")
	}

	var r0 map[*scheduler.JobSchedule]*scheduler.JobLineageSummary
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context, []*scheduler.JobSchedule, int) (map[*scheduler.JobSchedule]*scheduler.JobLineageSummary, error)); ok {
		return rf(ctx, jobSchedules, depth)
	}
	if rf, ok := ret.Get(0).(func(context.Context, []*scheduler.JobSchedule, int) map[*scheduler.JobSchedule]*scheduler.JobLineageSummary); ok {
		r0 = rf(ctx, jobSchedules, depth)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(map[*scheduler.JobSchedule]*scheduler.JobLineageSummary)
		}
	}

	if rf, ok := ret.Get(1).(func(context.Context, []*scheduler.JobSchedule, int) error); ok {
		r1 = rf(ctx, jobSchedules, depth)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

func NewMockLineageBuilder(t interface {
	mock.TestingT
	Cleanup(func())
}) *MockLineageBuilder {
	m := &MockLineageBuilder{}
	m.Test(t)
	t.Cleanup(func() { m.AssertExpectations(t) })
	return m
}

// MockDurationEstimatorRepo is a mock for the DurationEstimatorRepo interface.
type MockDurationEstimatorRepo struct {
	mock.Mock
}

func (m *MockDurationEstimatorRepo) GetPercentileDurationByJobNames(ctx context.Context, jobNames []scheduler.JobName, operators map[string][]string, referenceTime time.Time, lastNRuns, percentile int) (map[scheduler.JobName]*time.Duration, error) {
	ret := m.Called(ctx, jobNames, operators, referenceTime, lastNRuns, percentile)

	if len(ret) == 0 {
		panic("no return value specified for GetPercentileDurationByJobNames")
	}

	var r0 map[scheduler.JobName]*time.Duration
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context, []scheduler.JobName, map[string][]string, time.Time, int, int) (map[scheduler.JobName]*time.Duration, error)); ok {
		return rf(ctx, jobNames, operators, referenceTime, lastNRuns, percentile)
	}
	if rf, ok := ret.Get(0).(func(context.Context, []scheduler.JobName, map[string][]string, time.Time, int, int) map[scheduler.JobName]*time.Duration); ok {
		r0 = rf(ctx, jobNames, operators, referenceTime, lastNRuns, percentile)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(map[scheduler.JobName]*time.Duration)
		}
	}

	if rf, ok := ret.Get(1).(func(context.Context, []scheduler.JobName, map[string][]string, time.Time, int, int) error); ok {
		r1 = rf(ctx, jobNames, operators, referenceTime, lastNRuns, percentile)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

func NewMockDurationEstimatorRepo(t interface {
	mock.TestingT
	Cleanup(func())
}) *MockDurationEstimatorRepo {
	m := &MockDurationEstimatorRepo{}
	m.Test(t)
	t.Cleanup(func() { m.AssertExpectations(t) })
	return m
}
