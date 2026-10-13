package service // nolint: testpackage

import (
	"context"
	"testing"
	"time"

	"github.com/goto/salt/log"
	"github.com/stretchr/testify/assert"

	"github.com/goto/optimus/config"
	"github.com/goto/optimus/core/scheduler"
)

type stubStartDelayEstimator struct {
	delays map[scheduler.JobName]*time.Duration
	err    error
}

func (s stubStartDelayEstimator) GetPercentileStartDelayByJobNames(_ context.Context, jobNames []scheduler.JobName, _ time.Time, _, _ int) (map[scheduler.JobName]*time.Duration, error) {
	if s.err != nil {
		return nil, s.err
	}
	out := map[scheduler.JobName]*time.Duration{}
	for _, jobName := range jobNames {
		if d, ok := s.delays[jobName]; ok {
			out[jobName] = d
		}
	}
	return out, nil
}

type stubDownstreamCounter struct {
	counts map[scheduler.JobName]map[string]int
	err    error
}

func (s stubDownstreamCounter) GetDownstreamJobCategoryCounts(_ context.Context, jobName scheduler.JobName, _ string) (map[string]int, error) {
	if s.err != nil {
		return nil, s.err
	}
	return s.counts[jobName], nil
}

func TestWantsStartTimeBasedDelay(t *testing.T) {
	cases := []struct {
		name   string
		alert  *scheduler.PotentialSLABreachAlert
		expect bool
	}{
		{"running long", &scheduler.PotentialSLABreachAlert{Reason: scheduler.ReasonRunningLong}, false},
		{"started late", &scheduler.PotentialSLABreachAlert{Reason: scheduler.ReasonStartedLate}, false},
		{"unclassified and not started", &scheduler.PotentialSLABreachAlert{Reason: scheduler.ReasonUnknown, Status: scheduler.SLABreachCauseNotStarted}, true},
		{"unclassified but running late", &scheduler.PotentialSLABreachAlert{Reason: scheduler.ReasonUnknown, Status: scheduler.SLABreachCauseRunningLate}, false},
		{"third party delay", &scheduler.PotentialSLABreachAlert{Reason: scheduler.ReasonThirdPartyDelay}, false},
		{"upstream failed", &scheduler.PotentialSLABreachAlert{Reason: scheduler.ReasonUpstreamFailed}, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			assert.Equal(t, c.expect, wantsStartTimeBasedDelay(c.alert))
		})
	}
}

func TestCopyStartTimeEvidence(t *testing.T) {
	t.Run("mirrors a running-long cause's safe-start evidence onto the display fields", func(t *testing.T) {
		safeStart := time.Date(2026, 10, 13, 7, 50, 0, 0, time.UTC)
		startedAt := time.Date(2026, 10, 13, 8, 10, 0, 0, time.UTC)
		induced := 25 * time.Minute
		alert := &scheduler.PotentialSLABreachAlert{
			Reason: scheduler.ReasonRunningLong,
			Evidence: scheduler.RootCauseEvidence{
				LatestSafeStartAt: &safeStart,
				StartedAt:         &startedAt,
				InducedDelay:      induced,
			},
		}

		ok := copyStartTimeEvidence(alert)

		assert.True(t, ok)
		assert.Equal(t, &safeStart, alert.HistoricalP50StartTime)
		assert.Equal(t, &startedAt, alert.CurrentStartTime)
		assert.Equal(t, &induced, alert.TotalInducedDelay)
	})

	t.Run("mirrors a started-late cause the same way", func(t *testing.T) {
		safeStart := time.Date(2026, 10, 13, 7, 50, 0, 0, time.UTC)
		startedAt := time.Date(2026, 10, 13, 8, 10, 0, 0, time.UTC)
		induced := 20 * time.Minute
		alert := &scheduler.PotentialSLABreachAlert{
			Reason: scheduler.ReasonStartedLate,
			Evidence: scheduler.RootCauseEvidence{
				LatestSafeStartAt: &safeStart,
				StartedAt:         &startedAt,
				InducedDelay:      induced,
			},
		}

		ok := copyStartTimeEvidence(alert)

		assert.True(t, ok)
		assert.Equal(t, &safeStart, alert.HistoricalP50StartTime)
		assert.Equal(t, &startedAt, alert.CurrentStartTime)
		assert.Equal(t, &induced, alert.TotalInducedDelay)
	})

	t.Run("is a no-op for every other reason", func(t *testing.T) {
		for _, reason := range []scheduler.RootCauseReason{
			scheduler.ReasonThirdPartyDelay, scheduler.ReasonUpstreamFailed, scheduler.ReasonUnknown,
		} {
			alert := &scheduler.PotentialSLABreachAlert{Reason: reason}

			ok := copyStartTimeEvidence(alert)

			assert.False(t, ok)
			assert.Nil(t, alert.HistoricalP50StartTime)
			assert.Nil(t, alert.CurrentStartTime)
			assert.Nil(t, alert.TotalInducedDelay)
		}
	})
}

func TestEnrichBreachAlerts(t *testing.T) {
	ctx := context.Background()
	referenceTime := time.Date(2026, 10, 13, 10, 0, 0, 0, time.UTC)
	scheduledAt := referenceTime.Add(-2 * time.Hour)
	reqConfig := JobSLAPredictorRequestConfig{ReferenceTime: referenceTime}

	t.Run("a running-long cause copies its already-classified safe-start evidence rather than re-deriving it", func(t *testing.T) {
		safeStart := scheduledAt.Add(10 * time.Minute)
		startedAt := scheduledAt.Add(45 * time.Minute)
		induced := 90 * time.Minute
		svc := &JobSLAPredictorService{
			l:                 log.NewNoop(),
			config:            config.PotentialSLABreachConfig{DownstreamCategoryLabelKey: "category_table_tag"},
			downstreamCounter: stubDownstreamCounter{counts: map[scheduler.JobName]map[string]int{"job-C": {"business-critical": 3, "ssot": 1}}},
		}
		alert := &scheduler.PotentialSLABreachAlert{
			RootCauseJob:         "job-C",
			Reason:               scheduler.ReasonRunningLong,
			RootCauseScheduledAt: &scheduledAt,
			Evidence: scheduler.RootCauseEvidence{
				LatestSafeStartAt: &safeStart,
				StartedAt:         &startedAt,
				InducedDelay:      induced,
			},
		}

		svc.enrichBreachAlerts(ctx, []*scheduler.PotentialSLABreachAlert{alert}, reqConfig)

		assert.Equal(t, &safeStart, alert.HistoricalP50StartTime)
		assert.Equal(t, &startedAt, alert.CurrentStartTime)
		assert.Equal(t, &induced, alert.TotalInducedDelay)
		assert.Equal(t, map[string]int{"business-critical": 3, "ssot": 1}, alert.DownstreamCounts)
	})

	t.Run("a started-late cause is handled the same way", func(t *testing.T) {
		safeStart := scheduledAt.Add(10 * time.Minute)
		startedAt := scheduledAt.Add(50 * time.Minute)
		induced := 40 * time.Minute
		svc := &JobSLAPredictorService{
			l:                 log.NewNoop(),
			config:            config.PotentialSLABreachConfig{},
			downstreamCounter: stubDownstreamCounter{counts: map[scheduler.JobName]map[string]int{}},
		}
		alert := &scheduler.PotentialSLABreachAlert{
			RootCauseJob:         "job-C",
			Reason:               scheduler.ReasonStartedLate,
			RootCauseScheduledAt: &scheduledAt,
			Evidence: scheduler.RootCauseEvidence{
				LatestSafeStartAt: &safeStart,
				StartedAt:         &startedAt,
				InducedDelay:      induced,
			},
		}

		svc.enrichBreachAlerts(ctx, []*scheduler.PotentialSLABreachAlert{alert}, reqConfig)

		assert.Equal(t, &induced, alert.TotalInducedDelay)
		assert.Equal(t, &startedAt, alert.CurrentStartTime)
	})

	t.Run("an unclassified not-started cause falls back to a fresh historical-start lookup, since Identify never ran a detector for it", func(t *testing.T) {
		offset := 10 * time.Minute
		svc := &JobSLAPredictorService{
			l:                   log.NewNoop(),
			config:              config.PotentialSLABreachConfig{},
			startDelayEstimator: stubStartDelayEstimator{delays: map[scheduler.JobName]*time.Duration{"job-C": &offset}},
			downstreamCounter:   stubDownstreamCounter{counts: map[scheduler.JobName]map[string]int{}},
		}
		alert := &scheduler.PotentialSLABreachAlert{
			RootCauseJob:         "job-C",
			Reason:               scheduler.ReasonUnknown,
			Status:               scheduler.SLABreachCauseNotStarted,
			RootCauseScheduledAt: &scheduledAt,
		}

		svc.enrichBreachAlerts(ctx, []*scheduler.PotentialSLABreachAlert{alert}, reqConfig)

		wantP50Start := scheduledAt.Add(offset)
		assert.Equal(t, &wantP50Start, alert.HistoricalP50StartTime)
		assert.Equal(t, &referenceTime, alert.CurrentStartTime)
		wantDelay := referenceTime.Sub(wantP50Start)
		assert.Equal(t, &wantDelay, alert.TotalInducedDelay)
	})

	t.Run("skips both lookups for third-party delay, since the root cause job name is a sensor identity", func(t *testing.T) {
		svc := &JobSLAPredictorService{
			l:                   log.NewNoop(),
			config:              config.PotentialSLABreachConfig{},
			startDelayEstimator: stubStartDelayEstimator{err: assert.AnError},
			downstreamCounter:   stubDownstreamCounter{err: assert.AnError},
		}
		alert := &scheduler.PotentialSLABreachAlert{
			RootCauseJob: "wait_dex_orders",
			Reason:       scheduler.ReasonThirdPartyDelay,
		}

		svc.enrichBreachAlerts(ctx, []*scheduler.PotentialSLABreachAlert{alert}, reqConfig)

		assert.Nil(t, alert.TotalInducedDelay)
		assert.Nil(t, alert.HistoricalP50StartTime)
		assert.Nil(t, alert.CurrentStartTime)
		assert.Nil(t, alert.DownstreamCounts)
	})

	t.Run("leaves induced delay unset when the historical start estimate is unavailable", func(t *testing.T) {
		svc := &JobSLAPredictorService{
			l:                   log.NewNoop(),
			config:              config.PotentialSLABreachConfig{},
			startDelayEstimator: stubStartDelayEstimator{delays: map[scheduler.JobName]*time.Duration{}},
			downstreamCounter:   stubDownstreamCounter{counts: map[scheduler.JobName]map[string]int{}},
		}
		alert := &scheduler.PotentialSLABreachAlert{
			RootCauseJob:         "job-C",
			Reason:               scheduler.ReasonUnknown,
			Status:               scheduler.SLABreachCauseNotStarted,
			RootCauseScheduledAt: &scheduledAt,
		}

		svc.enrichBreachAlerts(ctx, []*scheduler.PotentialSLABreachAlert{alert}, reqConfig)

		assert.Nil(t, alert.TotalInducedDelay)
		assert.Nil(t, alert.HistoricalP50StartTime)
		assert.Nil(t, alert.CurrentStartTime)
	})

	t.Run("a downstream-counter failure logs and leaves the alert without counts rather than failing the send", func(t *testing.T) {
		svc := &JobSLAPredictorService{
			l:                   log.NewNoop(),
			config:              config.PotentialSLABreachConfig{},
			startDelayEstimator: stubStartDelayEstimator{delays: map[scheduler.JobName]*time.Duration{}},
			downstreamCounter:   stubDownstreamCounter{err: assert.AnError},
		}
		alert := &scheduler.PotentialSLABreachAlert{
			RootCauseJob: "job-C",
			Reason:       scheduler.ReasonUpstreamFailed,
		}

		svc.enrichBreachAlerts(ctx, []*scheduler.PotentialSLABreachAlert{alert}, reqConfig)

		assert.Nil(t, alert.DownstreamCounts)
	})
}
