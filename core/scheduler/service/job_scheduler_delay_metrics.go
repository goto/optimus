package service

import (
	"context"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/goto/optimus/core/scheduler"
)

// Phases of the scheduler-delay decomposition. Each phase measures one stretch
// of the wait between a job's scheduled time and its actual start; more phases
// are added by follow-up changes.
const (
	// SchedulerDelayPhaseDagClose measures how long the Airflow DAG-level
	// success callback arrives after the last operator (task or hook) of the
	// same run finished. A large value points at DAG-processor or callback
	// queueing, not at Optimus scheduling.
	SchedulerDelayPhaseDagClose = "dag_close"
)

// Gates describe what the job was waiting on; not every phase has one.
const (
	SchedulerDelayGateNone = "none"
)

const (
	MetricSchedulerDelaySeconds     = "job_scheduler_delay_seconds"
	MetricSchedulerDelaySkippedName = "job_scheduler_delay_skipped_total"
)

var jobSchedulerDelaySeconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Name: MetricSchedulerDelaySeconds,
	Help: "Time spent in each scheduler delay phase, in seconds",
	Buckets: []float64{
		5, 15, 30, 60, 90, 120, 180, 240, 300, 600, 900, 1800, 3600,
	},
}, []string{"project", "namespace", "phase", "gate"})

var jobSchedulerDelaySkippedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: MetricSchedulerDelaySkippedName,
	Help: "Samples that could not be recorded in job_scheduler_delay_seconds, by reason",
}, []string{"reason"})

// observeSchedulerDelay records one phase sample. Delays are second-precision
// because the Airflow callback truncates event_time to whole seconds.
func observeSchedulerDelay(project, namespace, phase, gate string, delay time.Duration) {
	jobSchedulerDelaySeconds.WithLabelValues(project, namespace, phase, gate).Observe(delay.Seconds())
}

// skipSchedulerDelay counts a sample we saw but could not record, so silent
// data loss is visible in the metrics.
func skipSchedulerDelay(reason string) {
	jobSchedulerDelaySkippedTotal.WithLabelValues(reason).Inc()
}

// recordDagCloseDelay measures phase dag_close for the DAG-level success
// callback: how long after the last operator finished did the callback arrive.
// It never fails the event handling -- a measurement problem must not turn
// into a missed state update -- so every failure path only counts a skip.
func (s *JobRunService) recordDagCloseDelay(ctx context.Context, event *scheduler.Event) {
	if event.Type != scheduler.JobSuccessEvent {
		return
	}

	runID := event.DagRunID()
	switch {
	case runID == "":
		skipSchedulerDelay("missing_run_id")
		return
	case !strings.HasPrefix(runID, scheduler.RunIDScheduledPrefix):
		skipSchedulerDelay("non_regular_run")
		return
	}

	summaries, err := s.repo.GetRunSummaryByIdentifiers(ctx, []scheduler.JobRunIdentifier{{
		JobName:     event.JobName,
		ScheduledAt: event.JobScheduledAt,
	}})
	if err != nil {
		s.l.Error("error getting run summary for dag close delay, job [%s]: %s", event.JobName, err)
		skipSchedulerDelay("summary_lookup_failed")
		return
	}
	if len(summaries) == 0 {
		skipSchedulerDelay("no_run_summary")
		return
	}

	operatorEnd := summaries[0].GetActualEndTime()
	if operatorEnd == nil {
		skipSchedulerDelay("no_operator_end")
		return
	}

	delay := event.EventTime.Sub(*operatorEnd)
	if delay < 0 {
		// Callbacks can arrive out of order or workers' clocks can drift; a
		// negative gap is noise, not a real measurement.
		skipSchedulerDelay("negative_delay")
		return
	}

	observeSchedulerDelay(
		event.Tenant.ProjectName().String(),
		event.Tenant.NamespaceName().String(),
		SchedulerDelayPhaseDagClose,
		SchedulerDelayGateNone,
		delay,
	)
}
