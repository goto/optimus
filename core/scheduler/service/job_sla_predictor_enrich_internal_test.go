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
		{"running long", &scheduler.PotentialSLABreachAlert{Reason: scheduler.ReasonRunningLong}, true},
		{"started late", &scheduler.PotentialSLABreachAlert{Reason: scheduler.ReasonStartedLate}, true},
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

func TestEnrichBreachAlerts(t *testing.T) {
	ctx := context.Background()
	referenceTime := time.Date(2026, 10, 13, 10, 0, 0, 0, time.UTC)
	scheduledAt := referenceTime.Add(-2 * time.Hour)
	reqConfig := JobSLAPredictorRequestConfig{ReferenceTime: referenceTime}

	t.Run("a running-long cause that has not finished uses reference time as the current start", func(t *testing.T) {
		offset := 10 * time.Minute
		svc := &JobSLAPredictorService{
			l:                   log.NewNoop(),
			config:              config.PotentialSLABreachConfig{DownstreamCategoryLabelKey: "category_table_tag"},
			startDelayEstimator: stubStartDelayEstimator{delays: map[scheduler.JobName]*time.Duration{"job-C": &offset}},
			downstreamCounter:   stubDownstreamCounter{counts: map[scheduler.JobName]map[string]int{"job-C": {"business-critical": 3, "ssot": 1}}},
		}
		alert := &scheduler.PotentialSLABreachAlert{
			RootCauseJob:         "job-C",
			Reason:               scheduler.ReasonRunningLong,
			RootCauseScheduledAt: &scheduledAt,
		}

		svc.enrichBreachAlerts(ctx, []*scheduler.PotentialSLABreachAlert{alert}, reqConfig)

		wantP50Start := scheduledAt.Add(offset)
		assert.Equal(t, &wantP50Start, alert.HistoricalP50StartTime)
		assert.Equal(t, &referenceTime, alert.CurrentStartTime)
		wantDelay := referenceTime.Sub(wantP50Start)
		assert.Equal(t, &wantDelay, alert.TotalInducedDelay)
		assert.Equal(t, map[string]int{"business-critical": 3, "ssot": 1}, alert.DownstreamCounts)
	})

	t.Run("uses the actual start time once the root cause has started", func(t *testing.T) {
		offset := 10 * time.Minute
		startedAt := scheduledAt.Add(30 * time.Minute)
		svc := &JobSLAPredictorService{
			l:                   log.NewNoop(),
			config:              config.PotentialSLABreachConfig{},
			startDelayEstimator: stubStartDelayEstimator{delays: map[scheduler.JobName]*time.Duration{"job-C": &offset}},
			downstreamCounter:   stubDownstreamCounter{counts: map[scheduler.JobName]map[string]int{}},
		}
		alert := &scheduler.PotentialSLABreachAlert{
			RootCauseJob:         "job-C",
			Reason:               scheduler.ReasonStartedLate,
			RootCauseScheduledAt: &scheduledAt,
			Evidence:             scheduler.RootCauseEvidence{StartedAt: &startedAt},
		}

		svc.enrichBreachAlerts(ctx, []*scheduler.PotentialSLABreachAlert{alert}, reqConfig)

		wantP50Start := scheduledAt.Add(offset)
		wantDelay := startedAt.Sub(wantP50Start)
		assert.Equal(t, &wantDelay, alert.TotalInducedDelay)
		assert.Equal(t, &startedAt, alert.CurrentStartTime)
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
			Reason:               scheduler.ReasonRunningLong,
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
