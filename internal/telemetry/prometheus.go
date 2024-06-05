package telemetry

import (
	"sort"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/push"
	"github.com/prometheus/common/expfmt"
)

var (
	counterMetricMap   = map[string]prometheus.Counter{}
	counterMetricMutex = sync.Mutex{}

	gaugeMetricMap   = map[string]prometheus.Gauge{}
	gaugeMetricMutex = sync.Mutex{}

	MetricServer string

	panicMetric = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "panics_recovered",
	}, []string{"entity", "msg"})
)

const metricsPushJob = "optimus_push"

func LogPanic(entity string, message string) {
	panicMetric.WithLabelValues(entity, message).Inc()
}

func getKey(metric string, labels map[string]string) string {
	eventMetricKey := metric
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, key := range keys {
		eventMetricKey += "/" + key + ":" + labels[key]
	}
	return eventMetricKey
}

func NewCounter(metric string, labels map[string]string) prometheus.Counter {
	metricKey := getKey(metric, labels)

	counterMetricMutex.Lock()
	defer counterMetricMutex.Unlock()

	if existingMetric, ok := counterMetricMap[metricKey]; ok {
		return existingMetric
	}
	newMetric := promauto.NewCounter(prometheus.CounterOpts{Name: metric, ConstLabels: labels})
	counterMetricMap[metricKey] = newMetric
	return newMetric
}

func SetGaugeViaPush(name string, labels map[string]string, val float64) error {
	if MetricServer == "" {
		return nil
	}
	metric := prometheus.NewGauge(prometheus.GaugeOpts{
		Name:        name,
		ConstLabels: labels,
	})
	metric.Set(val)

	return push.New(MetricServer, metricsPushJob).
		Format(expfmt.FmtText).
		Collector(metric).
		Push()
}

func NewGauge(metric string, labels map[string]string) prometheus.Gauge {
	metricKey := getKey(metric, labels)

	gaugeMetricMutex.Lock()
	defer gaugeMetricMutex.Unlock()

	if existingMetric, ok := gaugeMetricMap[metricKey]; ok {
		return existingMetric
	}
	newMetric := promauto.NewGauge(prometheus.GaugeOpts{Name: metric, ConstLabels: labels})
	gaugeMetricMap[metricKey] = newMetric
	return newMetric
}
