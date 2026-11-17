package v1beta1

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Stage names for the job_event_handling_seconds histogram. They cover the
// synchronous steps of RegisterJobEvent, which Airflow task callbacks call
// inline before the operator's execute() runs, so latency here directly delays task start on the worker.
const (
	jobEventStageTotal          = "total"
	jobEventStageUpdateJobState = "update_job_state"
	jobEventStageRelay          = "relay"
	jobEventStageWebhook        = "webhook"
	jobEventStagePush           = "push"

	jobEventUnknownType = "unknown"
)

var (
	// jobEventHandlingSeconds measures how long each stage of RegisterJobEvent
	// takes, per event type. Stages run sequentially, so total >= sum of the
	// other stages; the difference is request parsing and gRPC overhead.
	jobEventHandlingSeconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name: "job_event_handling_seconds",
		Help: "Time spent handling a job event, split by processing stage (total, update_job_state, relay, webhook, push)",
		Buckets: []float64{
			0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60,
		},
	}, []string{"event_type", "stage"})

	// jobEventStageErrorsTotal counts failed stages. RegisterJobEvent collects
	// stage errors into a multi-error and only then returns, so the per-stage
	// counter is what shows which step actually failed.
	jobEventStageErrorsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "job_event_stage_errors_total",
		Help: "Number of failed job event processing stages",
	}, []string{"event_type", "stage"})

	// jobRunInputHandlingSeconds measures the JobRunInput RPC. The init
	// container calls it with a short timeout before every task run, so its
	// latency sits directly on the task start path in Airflow.
	jobRunInputHandlingSeconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name: "job_run_input_handling_seconds",
		Help: "Time spent serving the job run input RPC used by the task init container",
		Buckets: []float64{
			0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60,
		},
	}, []string{"status"})
)

// observeJobEventStage records one RegisterJobEvent stage: how long it took and
// whether it failed. eventTypeName comes from the request and is a small,
// fixed set of values (task_start, job_success, ...), so the label cardinality
// stays bounded.
func observeJobEventStage(eventTypeName, stage string, started time.Time, err error) {
	jobEventHandlingSeconds.WithLabelValues(eventTypeName, stage).Observe(time.Since(started).Seconds())
	if err != nil {
		jobEventStageErrorsTotal.WithLabelValues(eventTypeName, stage).Inc()
	}
}
