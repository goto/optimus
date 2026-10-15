package service // nolint: testpackage

import (
	"context"
	"testing"

	"github.com/goto/salt/log"
	"github.com/stretchr/testify/assert"

	"github.com/goto/optimus/config"
	"github.com/goto/optimus/core/scheduler"
)

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

func TestEnrichBreachAlertsDownstream(t *testing.T) {
	ctx := context.Background()
	reqConfig := JobSLAPredictorRequestConfig{}

	t.Run("attaches category counts to the alert", func(t *testing.T) {
		svc := &JobSLAPredictorService{
			l:                 log.NewNoop(),
			config:            config.PotentialSLABreachConfig{DownstreamCategoryLabelKey: "category_table_tag"},
			downstreamCounter: stubDownstreamCounter{counts: map[scheduler.JobName]map[string]int{"job-C": {"business-critical": 3, "ssot": 1}}},
		}
		alert := &scheduler.PotentialSLABreachAlert{RootCauseJob: "job-C", Reason: scheduler.ReasonRunningLong}

		svc.enrichBreachAlerts(ctx, []*scheduler.PotentialSLABreachAlert{alert}, reqConfig)

		assert.Equal(t, map[string]int{"business-critical": 3, "ssot": 1}, alert.DownstreamCounts)
	})

	t.Run("skips third-party delay, since the root cause job name is a sensor identity, not a real row in job_upstream", func(t *testing.T) {
		svc := &JobSLAPredictorService{
			l:                 log.NewNoop(),
			config:            config.PotentialSLABreachConfig{},
			downstreamCounter: stubDownstreamCounter{err: assert.AnError},
		}
		alert := &scheduler.PotentialSLABreachAlert{RootCauseJob: "wait_dex_orders", Reason: scheduler.ReasonThirdPartyDelay}

		svc.enrichBreachAlerts(ctx, []*scheduler.PotentialSLABreachAlert{alert}, reqConfig)

		assert.Nil(t, alert.DownstreamCounts)
	})

	t.Run("a counter failure logs and leaves the alert without counts rather than failing the send", func(t *testing.T) {
		svc := &JobSLAPredictorService{
			l:                 log.NewNoop(),
			config:            config.PotentialSLABreachConfig{},
			downstreamCounter: stubDownstreamCounter{err: assert.AnError},
		}
		alert := &scheduler.PotentialSLABreachAlert{RootCauseJob: "job-C", Reason: scheduler.ReasonUpstreamFailed}

		svc.enrichBreachAlerts(ctx, []*scheduler.PotentialSLABreachAlert{alert}, reqConfig)

		assert.Nil(t, alert.DownstreamCounts)
	})

	t.Run("a nil counter leaves alerts untouched", func(t *testing.T) {
		svc := &JobSLAPredictorService{
			l:      log.NewNoop(),
			config: config.PotentialSLABreachConfig{},
		}
		alert := &scheduler.PotentialSLABreachAlert{RootCauseJob: "job-C", Reason: scheduler.ReasonRunningLong}

		svc.enrichBreachAlerts(ctx, []*scheduler.PotentialSLABreachAlert{alert}, reqConfig)

		assert.Nil(t, alert.DownstreamCounts)
	})
}
