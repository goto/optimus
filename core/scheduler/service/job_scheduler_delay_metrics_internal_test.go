package service

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"

	"github.com/goto/salt/log"

	"github.com/goto/optimus/config"
	"github.com/goto/optimus/core/scheduler"
	"github.com/goto/optimus/core/tenant"
)

type stubJobRunRepository struct {
	JobRunRepository

	summaries       []*scheduler.JobRunSummary
	err             error
	callIdentifiers []scheduler.JobRunIdentifier
}

func (s *stubJobRunRepository) GetRunSummaryByIdentifiers(_ context.Context, identifiers []scheduler.JobRunIdentifier) ([]*scheduler.JobRunSummary, error) {
	s.callIdentifiers = identifiers
	return s.summaries, s.err
}

func skippedDelayCount(t *testing.T, reason string) float64 {
	t.Helper()
	metric := &dto.Metric{}
	if err := jobSchedulerDelaySkippedTotal.WithLabelValues(reason).Write(metric); err != nil {
		return 0
	}
	return metric.Counter.GetValue()
}

func observedDelayCount(t *testing.T, project, namespace, phase, gate string) uint64 {
	t.Helper()
	families, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		return 0
	}
	for _, family := range families {
		if family.GetName() != MetricSchedulerDelaySeconds {
			continue
		}
		for _, metric := range family.GetMetric() {
			labels := map[string]string{}
			for _, lp := range metric.GetLabel() {
				labels[lp.GetName()] = lp.GetValue()
			}
			if labels["project"] == project && labels["namespace"] == namespace &&
				labels["phase"] == phase && labels["gate"] == gate {
				return metric.GetHistogram().GetSampleCount()
			}
		}
	}
	return 0
}

func TestRecordDagCloseDelay(t *testing.T) {
	ctx := context.Background()
	logger := log.NewNoop()

	tnnt, err := tenant.NewTenant("proj", "ns1")
	assert.NoError(t, err)
	jobName := scheduler.JobName("job_a")

	scheduledAt := time.Date(2026, 11, 17, 5, 0, 0, 0, time.UTC)
	eventTime := scheduledAt.Add(2 * time.Hour)
	operatorEnd := eventTime.Add(-90 * time.Second) // dag callback arrived 90s after the last operator

	newEvent := func(eventType scheduler.JobEventType, runID string) *scheduler.Event {
		event := &scheduler.Event{
			JobName:        jobName,
			Tenant:         tnnt,
			Type:           eventType,
			EventTime:      eventTime,
			JobScheduledAt: scheduledAt,
			Status:         scheduler.StateSuccess,
		}
		if runID != "" {
			event.EventContext = &scheduler.EventContext{
				DagRun: scheduler.DagRun{RunID: runID},
			}
		}
		return event
	}

	newService := func(repo *stubJobRunRepository) *JobRunService {
		return NewJobRunService(logger, nil, repo, nil, nil, nil, nil, nil, nil, nil, nil, config.FeaturesConfig{}, nil, nil)
	}

	const runIDScheduled = "scheduled__2026-11-17T05:00:00+00:00"

	t.Run("ignores non job_success events", func(t *testing.T) {
		service := newService(&stubJobRunRepository{})

		observed := observedDelayCount(t, "proj", "ns1", SchedulerDelayPhaseDagClose, SchedulerDelayGateNone)
		service.recordDagCloseDelay(ctx, newEvent(scheduler.TaskStartEvent, runIDScheduled))

		assert.Equal(t, observed, observedDelayCount(t, "proj", "ns1", SchedulerDelayPhaseDagClose, SchedulerDelayGateNone))
	})

	t.Run("skips when run id is missing", func(t *testing.T) {
		service := newService(&stubJobRunRepository{})

		before := skippedDelayCount(t, "missing_run_id")
		service.recordDagCloseDelay(ctx, newEvent(scheduler.JobSuccessEvent, ""))

		assert.Equal(t, before+1, skippedDelayCount(t, "missing_run_id"))
	})

	t.Run("skips non regular runs without querying the repository", func(t *testing.T) {
		repo := &stubJobRunRepository{}
		service := newService(repo)

		for _, runID := range []string{
			"backfill_2026-11-17T05:00:00+00:00",
			"manual__2026-11-17T05:00:00+00:00",
			"replayed__2026-11-17T05:00:00+00:00",
		} {
			before := skippedDelayCount(t, "non_regular_run")
			service.recordDagCloseDelay(ctx, newEvent(scheduler.JobSuccessEvent, runID))
			assert.Equal(t, before+1, skippedDelayCount(t, "non_regular_run"))
		}
		assert.Nil(t, repo.summaries)
	})

	t.Run("skips when the run summary cannot be read", func(t *testing.T) {
		service := newService(&stubJobRunRepository{err: fmt.Errorf("db down")})

		before := skippedDelayCount(t, "summary_lookup_failed")
		service.recordDagCloseDelay(ctx, newEvent(scheduler.JobSuccessEvent, runIDScheduled))

		assert.Equal(t, before+1, skippedDelayCount(t, "summary_lookup_failed"))
	})

	t.Run("skips when no run summary is found", func(t *testing.T) {
		service := newService(&stubJobRunRepository{})

		before := skippedDelayCount(t, "no_run_summary")
		service.recordDagCloseDelay(ctx, newEvent(scheduler.JobSuccessEvent, runIDScheduled))

		assert.Equal(t, before+1, skippedDelayCount(t, "no_run_summary"))
	})

	t.Run("skips when no operator has finished", func(t *testing.T) {
		service := newService(&stubJobRunRepository{
			summaries: []*scheduler.JobRunSummary{{JobName: jobName}},
		})

		before := skippedDelayCount(t, "no_operator_end")
		service.recordDagCloseDelay(ctx, newEvent(scheduler.JobSuccessEvent, runIDScheduled))

		assert.Equal(t, before+1, skippedDelayCount(t, "no_operator_end"))
	})

	t.Run("skips negative delays", func(t *testing.T) {
		laterOperatorEnd := eventTime.Add(time.Second)
		service := newService(&stubJobRunRepository{
			summaries: []*scheduler.JobRunSummary{{
				JobName:     jobName,
				TaskEndTime: &laterOperatorEnd, // operator "ends" after the callback arrived
			}},
		})

		before := skippedDelayCount(t, "negative_delay")
		service.recordDagCloseDelay(ctx, newEvent(scheduler.JobSuccessEvent, runIDScheduled))

		assert.Equal(t, before+1, skippedDelayCount(t, "negative_delay"))
	})

	t.Run("records the dag close lag for regular runs", func(t *testing.T) {
		repo := &stubJobRunRepository{
			summaries: []*scheduler.JobRunSummary{{
				JobName:     jobName,
				TaskEndTime: &operatorEnd,
			}},
		}
		service := newService(repo)

		before := observedDelayCount(t, "proj", "ns1", SchedulerDelayPhaseDagClose, SchedulerDelayGateNone)
		service.recordDagCloseDelay(ctx, newEvent(scheduler.JobSuccessEvent, runIDScheduled))

		assert.Equal(t, before+1, observedDelayCount(t, "proj", "ns1", SchedulerDelayPhaseDagClose, SchedulerDelayGateNone))
		assert.Equal(t, []scheduler.JobRunIdentifier{{JobName: jobName, ScheduledAt: scheduledAt}}, repo.callIdentifiers)
	})

	t.Run("uses hook end when a hook finished later than the task", func(t *testing.T) {
		hookEnd := operatorEnd.Add(10 * time.Second)
		repo := &stubJobRunRepository{
			summaries: []*scheduler.JobRunSummary{{
				JobName:     jobName,
				TaskEndTime: &operatorEnd,
				HookEndTime: &hookEnd,
			}},
		}
		service := newService(repo)

		// hook end is 80s before the callback, so a sample must be recorded,
		// not skipped as negative
		delayObservedBefore := observedDelayCount(t, "proj", "ns1", SchedulerDelayPhaseDagClose, SchedulerDelayGateNone)
		skipBefore := skippedDelayCount(t, "negative_delay")
		service.recordDagCloseDelay(ctx, newEvent(scheduler.JobSuccessEvent, runIDScheduled))

		assert.Equal(t, delayObservedBefore+1, observedDelayCount(t, "proj", "ns1", SchedulerDelayPhaseDagClose, SchedulerDelayGateNone))
		assert.Equal(t, skipBefore, skippedDelayCount(t, "negative_delay"))
	})
}
