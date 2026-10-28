package service

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const (
	MetricRootCauseEvaluations = "rootcause_evaluations_total"
	MetricRootCauseDetections  = "rootcause_detections_total"
)

var (
	rootCauseEvaluationsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: MetricRootCauseEvaluations,
	}, []string{"result"})

	rootCauseDetectionsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: MetricRootCauseDetections,
	}, []string{"reason", "project"})
)

// recordEvaluation counts one publisher tick: errored when the batch failed, empty when it
// found nothing, successful otherwise.
func (s *JobRootCauseService) recordEvaluation(errored bool, incidentCount int) {
	switch {
	case errored:
		rootCauseEvaluationsTotal.WithLabelValues("errored").Inc()
	case incidentCount == 0:
		rootCauseEvaluationsTotal.WithLabelValues("empty").Inc()
	default:
		rootCauseEvaluationsTotal.WithLabelValues("success").Inc()
	}
}
