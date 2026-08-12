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
	defaultLineageWindowHours := 24

	t.Run("when lineage builder returns error, propagate error", func(t *testing.T) {
		lineageBuilder := NewMockLineageBuilder(t)
		durationEstimator := NewMockDurationEstimatorRepo(t)
		svc := service.NewJobLineageService(l, lineageBuilder, durationEstimator, defaultHistoricalDurationLastNRuns, defaultHistoricalDurationPercentile, defaultMaxLineageDepth, defaultLineageWindowHours)

		jobSchedule := &scheduler.JobSchedule{JobName: "job-A", ScheduledAt: time.Now().UTC()}
		lineageBuilder.On("BuildLineage", ctx, []*scheduler.JobSchedule{jobSchedule}, 24).Return(nil, assert.AnError).Once()

		result, err := svc.GetJobExecutionSummary(ctx, []*scheduler.JobSchedule{jobSchedule}, scheduler.LineageSummaryOptions{MaxNodes: 1, WindowHours: 0})

		assert.Error(t, err)
		assert.Nil(t, result)
	})

	t.Run("when lineage builder returns empty map, return nil result", func(t *testing.T) {
		lineageBuilder := NewMockLineageBuilder(t)
		defer lineageBuilder.AssertExpectations(t)
		durationEstimator := NewMockDurationEstimatorRepo(t)
		defer durationEstimator.AssertExpectations(t)
		svc := service.NewJobLineageService(l, lineageBuilder, durationEstimator, defaultHistoricalDurationLastNRuns, defaultHistoricalDurationPercentile, defaultMaxLineageDepth, defaultLineageWindowHours)

		lineageBuilder.On("BuildLineage", ctx, []*scheduler.JobSchedule{}, 24).Return(map[*scheduler.JobSchedule]*scheduler.JobLineageSummary{}, nil).Once()

		result, err := svc.GetJobExecutionSummary(ctx, []*scheduler.JobSchedule{}, scheduler.LineageSummaryOptions{MaxNodes: 1, WindowHours: 0})

		assert.NoError(t, err)
		assert.Nil(t, result)
	})

	t.Run("when a lineage window is configured, use it instead of the previously hard-coded 24 hours", func(t *testing.T) {
		lineageBuilder := NewMockLineageBuilder(t)
		defer lineageBuilder.AssertExpectations(t)
		durationEstimator := NewMockDurationEstimatorRepo(t)
		defer durationEstimator.AssertExpectations(t)
		svc := service.NewJobLineageService(l, lineageBuilder, durationEstimator, defaultHistoricalDurationLastNRuns, defaultHistoricalDurationPercentile, defaultMaxLineageDepth, 10)

		lineageBuilder.On("BuildLineage", ctx, []*scheduler.JobSchedule{}, 10).Return(map[*scheduler.JobSchedule]*scheduler.JobLineageSummary{}, nil).Once()

		_, err := svc.GetJobExecutionSummary(ctx, []*scheduler.JobSchedule{}, scheduler.LineageSummaryOptions{MaxNodes: 0, WindowHours: 0})

		assert.NoError(t, err)
	})

	t.Run("when the request carries a lineage window, it overrides the configured one", func(t *testing.T) {
		lineageBuilder := NewMockLineageBuilder(t)
		defer lineageBuilder.AssertExpectations(t)
		durationEstimator := NewMockDurationEstimatorRepo(t)
		defer durationEstimator.AssertExpectations(t)
		svc := service.NewJobLineageService(l, lineageBuilder, durationEstimator, defaultHistoricalDurationLastNRuns, defaultHistoricalDurationPercentile, defaultMaxLineageDepth, 24)

		lineageBuilder.On("BuildLineage", ctx, []*scheduler.JobSchedule{}, 6).Return(map[*scheduler.JobSchedule]*scheduler.JobLineageSummary{}, nil).Once()

		_, err := svc.GetJobExecutionSummary(ctx, []*scheduler.JobSchedule{}, scheduler.LineageSummaryOptions{MaxNodes: 0, WindowHours: 6})

		assert.NoError(t, err)
	})

	t.Run("when no lineage window is configured, fall back to the default", func(t *testing.T) {
		lineageBuilder := NewMockLineageBuilder(t)
		defer lineageBuilder.AssertExpectations(t)
		durationEstimator := NewMockDurationEstimatorRepo(t)
		defer durationEstimator.AssertExpectations(t)
		svc := service.NewJobLineageService(l, lineageBuilder, durationEstimator, defaultHistoricalDurationLastNRuns, defaultHistoricalDurationPercentile, defaultMaxLineageDepth, 0)

		lineageBuilder.On("BuildLineage", ctx, []*scheduler.JobSchedule{}, service.DefaultLineageWindowHours).
			Return(map[*scheduler.JobSchedule]*scheduler.JobLineageSummary{}, nil).Once()

		_, err := svc.GetJobExecutionSummary(ctx, []*scheduler.JobSchedule{}, scheduler.LineageSummaryOptions{MaxNodes: 0, WindowHours: 0})

		assert.NoError(t, err)
	})

	t.Run("when lineage and duration estimation succeed, return enriched job run lineages", func(t *testing.T) {
		lineageBuilder := NewMockLineageBuilder(t)
		defer lineageBuilder.AssertExpectations(t)
		durationEstimator := NewMockDurationEstimatorRepo(t)
		defer durationEstimator.AssertExpectations(t)
		svc := service.NewJobLineageService(l, lineageBuilder, durationEstimator, defaultHistoricalDurationLastNRuns, defaultHistoricalDurationPercentile, defaultMaxLineageDepth, defaultLineageWindowHours)

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

		result, err := svc.GetJobExecutionSummary(ctx, []*scheduler.JobSchedule{jobSchedule}, scheduler.LineageSummaryOptions{MaxNodes: 1, WindowHours: 0})

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
		svc := service.NewJobLineageService(l, lineageBuilder, durationEstimator, defaultHistoricalDurationLastNRuns, defaultHistoricalDurationPercentile, defaultMaxLineageDepth, defaultLineageWindowHours)

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

		result, err := svc.GetJobExecutionSummary(ctx, []*scheduler.JobSchedule{jobSchedule}, scheduler.LineageSummaryOptions{MaxNodes: 5, WindowHours: 0})

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
		svc := service.NewJobLineageService(l, lineageBuilder, durationEstimator, defaultHistoricalDurationLastNRuns, defaultHistoricalDurationPercentile, defaultMaxLineageDepth, defaultLineageWindowHours)

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

		result, err := svc.GetJobExecutionSummary(ctx, []*scheduler.JobSchedule{jobSchedule}, scheduler.LineageSummaryOptions{MaxNodes: 5, WindowHours: 0})

		assert.NoError(t, err)
		assert.Len(t, result, 1)
		assert.Equal(t, scheduler.JobName("job-A"), result[0].JobName)
		assert.Equal(t, 30*time.Second, result[0].JobRuns[0].HistoricalSummary.TaskDuration)
		assert.Equal(t, 10*time.Second, result[0].JobRuns[0].HistoricalSummary.HookDuration)
	})

	t.Run("should fetch durations once for a job shared by several targets", func(t *testing.T) {
		lineageBuilder := NewMockLineageBuilder(t)
		defer lineageBuilder.AssertExpectations(t)
		durationEstimator := NewMockDurationEstimatorRepo(t)
		defer durationEstimator.AssertExpectations(t)
		svc := service.NewJobLineageService(l, lineageBuilder, durationEstimator, defaultHistoricalDurationLastNRuns, defaultHistoricalDurationPercentile, defaultMaxLineageDepth, defaultLineageWindowHours)

		scheduledAt := time.Now().UTC().Truncate(time.Second)
		scheduleA := &scheduler.JobSchedule{JobName: "job-A", ScheduledAt: scheduledAt}
		scheduleB := &scheduler.JobSchedule{JobName: "job-B", ScheduledAt: scheduledAt}

		// both targets depend on the same upstream, at the same schedule
		sharedRun := func(downstream scheduler.JobName) *scheduler.JobLineageSummary {
			return &scheduler.JobLineageSummary{
				JobName:   "shared-upstream",
				IsEnabled: true,
				JobRuns: map[scheduler.JobName]*scheduler.JobRunSummary{
					downstream: {JobName: "shared-upstream", ScheduledAt: scheduledAt},
				},
			}
		}
		lineageA := &scheduler.JobLineageSummary{
			JobName: "job-A", IsEnabled: true,
			JobRuns:   map[scheduler.JobName]*scheduler.JobRunSummary{"job-A": {JobName: "job-A", ScheduledAt: scheduledAt}},
			Upstreams: []*scheduler.JobLineageSummary{sharedRun("job-A")},
		}
		lineageB := &scheduler.JobLineageSummary{
			JobName: "job-B", IsEnabled: true,
			JobRuns:   map[scheduler.JobName]*scheduler.JobRunSummary{"job-B": {JobName: "job-B", ScheduledAt: scheduledAt}},
			Upstreams: []*scheduler.JobLineageSummary{sharedRun("job-B")},
		}

		schedules := []*scheduler.JobSchedule{scheduleA, scheduleB}
		lineageBuilder.On("BuildLineage", ctx, schedules, 24).Return(
			map[*scheduler.JobSchedule]*scheduler.JobLineageSummary{scheduleA: lineageA, scheduleB: lineageB}, nil,
		).Once()

		// a single task batch covering both targets and the upstream they share, deduplicated
		// and sorted - not one call per target
		durationEstimator.On("GetPercentileDurationByJobNames",
			ctx,
			[]scheduler.JobName{"job-A", "job-B", "shared-upstream"},
			map[string][]string{"task": {}},
			scheduledAt, defaultHistoricalDurationLastNRuns, defaultHistoricalDurationPercentile,
		).Return(map[scheduler.JobName]*time.Duration{
			scheduler.JobName("shared-upstream"): ptr(45 * time.Second),
		}, nil).Once()

		result, err := svc.GetJobExecutionSummary(ctx, schedules, scheduler.LineageSummaryOptions{})

		assert.NoError(t, err)
		assert.Len(t, result, 2)
		for _, lineage := range result {
			for _, run := range lineage.JobRuns {
				if run.JobName == "shared-upstream" {
					assert.Equal(t, 45*time.Second, run.HistoricalSummary.TaskDuration)
				}
			}
		}
	})

	t.Run("should keep applying the batches that succeeded when one fails", func(t *testing.T) {
		lineageBuilder := NewMockLineageBuilder(t)
		defer lineageBuilder.AssertExpectations(t)
		durationEstimator := NewMockDurationEstimatorRepo(t)
		defer durationEstimator.AssertExpectations(t)
		svc := service.NewJobLineageService(l, lineageBuilder, durationEstimator, defaultHistoricalDurationLastNRuns, defaultHistoricalDurationPercentile, defaultMaxLineageDepth, defaultLineageWindowHours)

		scheduledAt := time.Now().UTC().Truncate(time.Second)
		hookName := "my-hook"
		jobSchedule := &scheduler.JobSchedule{JobName: "job-A", ScheduledAt: scheduledAt}
		lineageSummary := &scheduler.JobLineageSummary{
			JobName: "job-A", IsEnabled: true,
			JobRuns: map[scheduler.JobName]*scheduler.JobRunSummary{
				"job-A": {JobName: "job-A", ScheduledAt: scheduledAt, HookName: &hookName},
			},
		}

		lineageBuilder.On("BuildLineage", ctx, []*scheduler.JobSchedule{jobSchedule}, 24).Return(
			map[*scheduler.JobSchedule]*scheduler.JobLineageSummary{jobSchedule: lineageSummary}, nil,
		).Once()
		durationEstimator.On("GetPercentileDurationByJobNames",
			ctx, mock.Anything, map[string][]string{"task": {}}, scheduledAt,
			defaultHistoricalDurationLastNRuns, defaultHistoricalDurationPercentile,
		).Return(map[scheduler.JobName]*time.Duration{scheduler.JobName("job-A"): ptr(30 * time.Second)}, nil).Once()
		durationEstimator.On("GetPercentileDurationByJobNames",
			ctx, mock.Anything, map[string][]string{"hook": {hookName}}, scheduledAt,
			defaultHistoricalDurationLastNRuns, defaultHistoricalDurationPercentile,
		).Return(nil, assert.AnError).Once()

		result, err := svc.GetJobExecutionSummary(ctx, []*scheduler.JobSchedule{jobSchedule}, scheduler.LineageSummaryOptions{})

		assert.NoError(t, err) // enrichment is best effort
		assert.Len(t, result, 1)
		assert.Equal(t, 30*time.Second, result[0].JobRuns[0].HistoricalSummary.TaskDuration,
			"the task batch still lands even though the hook batch failed")
		assert.Equal(t, time.Duration(0), result[0].JobRuns[0].HistoricalSummary.HookDuration)
	})

	t.Run("pagination", func(t *testing.T) {
		scheduledAt := time.Now().UTC().Truncate(time.Second)
		jobSchedule := &scheduler.JobSchedule{JobName: "job-A", ScheduledAt: scheduledAt}

		makeUpstream := func(name string, endOffset time.Duration) *scheduler.JobLineageSummary {
			end := scheduledAt.Add(endOffset)
			start := end.Add(-5 * time.Minute)
			return &scheduler.JobLineageSummary{
				JobName: scheduler.JobName(name),
				JobRuns: map[scheduler.JobName]*scheduler.JobRunSummary{
					"job-A": {JobName: scheduler.JobName(name), ScheduledAt: scheduledAt, TaskStartTime: &start, TaskEndTime: &end},
				},
			}
		}
		// target job-A plus 3 finished upstreams: 4 nodes total, enough to span two pages of 2
		lineageSummary := func() *scheduler.JobLineageSummary {
			return &scheduler.JobLineageSummary{
				JobName: "job-A", IsEnabled: true,
				JobRuns: map[scheduler.JobName]*scheduler.JobRunSummary{"job-A": {JobName: "job-A", ScheduledAt: scheduledAt}},
				Upstreams: []*scheduler.JobLineageSummary{
					makeUpstream("job-B", 12*time.Minute),
					makeUpstream("job-C", 8*time.Minute),
					makeUpstream("job-D", 6*time.Minute),
				},
			}
		}
		allJobNames := []scheduler.JobName{"job-A", "job-B", "job-C", "job-D"}

		newSvcWithNoDurations := func(lineageBuilder *MockLineageBuilder, durationEstimator *MockDurationEstimatorRepo, calls int) *service.JobLineageService {
			for i := 0; i < calls; i++ {
				lineageBuilder.On("BuildLineage", ctx, []*scheduler.JobSchedule{jobSchedule}, 24).Return(
					map[*scheduler.JobSchedule]*scheduler.JobLineageSummary{jobSchedule: lineageSummary()}, nil,
				).Once()
				durationEstimator.On("GetPercentileDurationByJobNames",
					ctx, allJobNames, map[string][]string{"task": {}}, scheduledAt,
					defaultHistoricalDurationLastNRuns, defaultHistoricalDurationPercentile,
				).Return(map[scheduler.JobName]*time.Duration{}, nil).Once()
			}
			return service.NewJobLineageService(l, lineageBuilder, durationEstimator, defaultHistoricalDurationLastNRuns, defaultHistoricalDurationPercentile, defaultMaxLineageDepth, defaultLineageWindowHours)
		}

		t.Run("should return a page and a cursor for the rest when the lineage is larger than page_size", func(t *testing.T) {
			lineageBuilder := NewMockLineageBuilder(t)
			defer lineageBuilder.AssertExpectations(t)
			durationEstimator := NewMockDurationEstimatorRepo(t)
			defer durationEstimator.AssertExpectations(t)
			svc := newSvcWithNoDurations(lineageBuilder, durationEstimator, 1)

			result, err := svc.GetJobExecutionSummary(ctx, []*scheduler.JobSchedule{jobSchedule}, scheduler.LineageSummaryOptions{PageSize: 2})

			assert.NoError(t, err)
			assert.Len(t, result, 1)
			assert.Len(t, result[0].JobRuns, 2)
			if assert.NotNil(t, result[0].NextPageCursor) {
				assert.Equal(t, 2, result[0].NextPageCursor.NodeOffset)
			}
		})

		t.Run("should follow the cursor across two calls and reach every node with no gaps or duplicates", func(t *testing.T) {
			lineageBuilder := NewMockLineageBuilder(t)
			defer lineageBuilder.AssertExpectations(t)
			durationEstimator := NewMockDurationEstimatorRepo(t)
			defer durationEstimator.AssertExpectations(t)
			// approach A re-walks the full lineage on every page, so the fixture must be primed
			// for two identical calls to BuildLineage and to the duration estimator
			svc := newSvcWithNoDurations(lineageBuilder, durationEstimator, 2)

			firstPage, err := svc.GetJobExecutionSummary(ctx, []*scheduler.JobSchedule{jobSchedule}, scheduler.LineageSummaryOptions{PageSize: 2})
			assert.NoError(t, err)
			cursor := firstPage[0].NextPageCursor
			if !assert.NotNil(t, cursor, "the fixture has 4 nodes, so the first page of 2 must not be the last") {
				return
			}

			secondPage, err := svc.GetJobExecutionSummary(ctx, []*scheduler.JobSchedule{jobSchedule},
				scheduler.LineageSummaryOptions{PageSize: 2, PageCursor: cursor})
			assert.NoError(t, err)

			assert.Nil(t, secondPage[0].NextPageCursor, "the fixture has exactly 4 nodes, so the second page must be the last")
			var seen []scheduler.JobName
			for _, run := range append(firstPage[0].JobRuns, secondPage[0].JobRuns...) {
				seen = append(seen, run.JobName)
			}
			assert.ElementsMatch(t, allJobNames, seen)
		})

		t.Run("should reject page_size when more than one target job is requested", func(t *testing.T) {
			lineageBuilder := NewMockLineageBuilder(t)
			defer lineageBuilder.AssertExpectations(t)
			durationEstimator := NewMockDurationEstimatorRepo(t)
			defer durationEstimator.AssertExpectations(t)
			svc := service.NewJobLineageService(l, lineageBuilder, durationEstimator, defaultHistoricalDurationLastNRuns, defaultHistoricalDurationPercentile, defaultMaxLineageDepth, defaultLineageWindowHours)

			other := &scheduler.JobSchedule{JobName: "job-B", ScheduledAt: scheduledAt}
			result, err := svc.GetJobExecutionSummary(ctx, []*scheduler.JobSchedule{jobSchedule, other}, scheduler.LineageSummaryOptions{PageSize: 2})

			assert.ErrorContains(t, err, "single target")
			assert.Nil(t, result)
			// the request is rejected before the lineage is ever fetched
			lineageBuilder.AssertNotCalled(t, "BuildLineage")
		})

		t.Run("should reject a page cursor given without a page size", func(t *testing.T) {
			lineageBuilder := NewMockLineageBuilder(t)
			defer lineageBuilder.AssertExpectations(t)
			durationEstimator := NewMockDurationEstimatorRepo(t)
			defer durationEstimator.AssertExpectations(t)
			svc := service.NewJobLineageService(l, lineageBuilder, durationEstimator, defaultHistoricalDurationLastNRuns, defaultHistoricalDurationPercentile, defaultMaxLineageDepth, defaultLineageWindowHours)

			cursor := &scheduler.LineagePageCursor{NodeOffset: 2, Fingerprint: "whatever"}
			result, err := svc.GetJobExecutionSummary(ctx, []*scheduler.JobSchedule{jobSchedule}, scheduler.LineageSummaryOptions{PageCursor: cursor})

			assert.ErrorContains(t, err, "page_size")
			assert.Nil(t, result)
			lineageBuilder.AssertNotCalled(t, "BuildLineage")
		})

		t.Run("should reject a cursor produced under different lineage options", func(t *testing.T) {
			lineageBuilder := NewMockLineageBuilder(t)
			defer lineageBuilder.AssertExpectations(t)
			durationEstimator := NewMockDurationEstimatorRepo(t)
			defer durationEstimator.AssertExpectations(t)
			// the mismatch is only discovered after the lineage is re-walked and re-enriched -
			// there is no cache to short-circuit it earlier
			svc := newSvcWithNoDurations(lineageBuilder, durationEstimator, 1)

			staleCursor := &scheduler.LineagePageCursor{NodeOffset: 2, Fingerprint: "a-cursor-from-a-different-query"}
			result, err := svc.GetJobExecutionSummary(ctx, []*scheduler.JobSchedule{jobSchedule},
				scheduler.LineageSummaryOptions{PageSize: 2, PageCursor: staleCursor})

			assert.ErrorContains(t, err, "does not match")
			assert.Nil(t, result)
		})
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
},
) *MockLineageBuilder {
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
},
) *MockDurationEstimatorRepo {
	m := &MockDurationEstimatorRepo{}
	m.Test(t)
	t.Cleanup(func() { m.AssertExpectations(t) })
	return m
}
