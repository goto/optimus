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

// wantsStartTimeBasedDelay now covers only the not-yet-started/UNKNOWN gap: RUNNING_LONG and
// STARTED_LATE are handled by copyStartTimeEvidence instead, since rootcause.Identify already
// computes their safe-start evidence using the same historical-start data (see
// effectiveSafeStart in core/scheduler/rootcause/delay.go) as the real basis for classification
// and root-cause selection, not just display.
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
			l:      log.NewNoop(),
			config: config.PotentialSLABreachConfig{},
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
	})

	t.Run("a started-late cause is handled the same way", func(t *testing.T) {
		safeStart := scheduledAt.Add(10 * time.Minute)
		startedAt := scheduledAt.Add(50 * time.Minute)
		induced := 40 * time.Minute
		svc := &JobSLAPredictorService{
			l:      log.NewNoop(),
			config: config.PotentialSLABreachConfig{},
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

	t.Run("skips the start-time lookup for third-party delay, since the root cause job name is a sensor identity", func(t *testing.T) {
		svc := &JobSLAPredictorService{
			l:                   log.NewNoop(),
			config:              config.PotentialSLABreachConfig{},
			startDelayEstimator: stubStartDelayEstimator{err: assert.AnError},
		}
		alert := &scheduler.PotentialSLABreachAlert{
			RootCauseJob: "wait_dex_orders",
			Reason:       scheduler.ReasonThirdPartyDelay,
		}

		svc.enrichBreachAlerts(ctx, []*scheduler.PotentialSLABreachAlert{alert}, reqConfig)

		assert.Nil(t, alert.TotalInducedDelay)
		assert.Nil(t, alert.HistoricalP50StartTime)
		assert.Nil(t, alert.CurrentStartTime)
	})

	t.Run("leaves induced delay unset when the historical start estimate is unavailable", func(t *testing.T) {
		svc := &JobSLAPredictorService{
			l:                   log.NewNoop(),
			config:              config.PotentialSLABreachConfig{},
			startDelayEstimator: stubStartDelayEstimator{delays: map[scheduler.JobName]*time.Duration{}},
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
}
