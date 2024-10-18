package telemetry

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var panicMetric = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "panics_recovered",
}, []string{"entity", "msg"})

func LogPanic(entity, message string) {
	panicMetric.WithLabelValues(entity, message).Inc()
}
