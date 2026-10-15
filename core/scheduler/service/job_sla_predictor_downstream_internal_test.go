package service // nolint: testpackage

import (
	"context"
	"testing"

	"github.com/goto/salt/log"
	"github.com/stretchr/testify/assert"

	"github.com/goto/optimus/config"
	"github.com/goto/optimus/core/scheduler"
	"github.com/goto/optimus/core/tenant"
)

type stubDownstreamCounter struct {
	counts map[tenant.ProjectName]map[scheduler.JobName]map[string]int
	err    error
}

func (s stubDownstreamCounter) GetDownstreamJobCategoryCounts(_ context.Context, projectName tenant.ProjectName, jobName scheduler.JobName, _ string) (map[string]int, error) {
	if s.err != nil {
		return nil, s.err
	}
	return s.counts[projectName][jobName], nil
}

func TestEnrichBreachAlertsDownstream(t *testing.T) {
	ctx := context.Background()

	t.Run("attaches category counts to the alert", func(t *testing.T) {
		svc := &JobSLAPredictorService{
			l:                 log.NewNoop(),
			config:            config.PotentialSLABreachConfig{DownstreamCategoryLabelKey: "labelA"},
			downstreamCounter: stubDownstreamCounter{counts: map[tenant.ProjectName]map[scheduler.JobName]map[string]int{"proj-1": {"job-C": {"categoryA": 3, "categoryB": 1}}}},
		}
		alert := &scheduler.PotentialSLABreachAlert{RootCauseProject: "proj-1", RootCauseJob: "job-C", Reason: scheduler.ReasonRunningLong}

		svc.enrichBreachAlerts(ctx, []*scheduler.PotentialSLABreachAlert{alert})

		assert.Equal(t, map[string]int{"categoryA": 3, "categoryB": 1}, alert.DownstreamCounts)
	})

	t.Run("skips third-party delay, since the root cause job name is a sensor identity, not a real row in job_upstream", func(t *testing.T) {
		svc := &JobSLAPredictorService{
			l:                 log.NewNoop(),
			config:            config.PotentialSLABreachConfig{},
			downstreamCounter: stubDownstreamCounter{err: assert.AnError},
		}
		alert := &scheduler.PotentialSLABreachAlert{RootCauseJob: "wait_dex_orders", Reason: scheduler.ReasonThirdPartyDelay}

		svc.enrichBreachAlerts(ctx, []*scheduler.PotentialSLABreachAlert{alert})

		assert.Nil(t, alert.DownstreamCounts)
	})

	t.Run("a counter failure logs and leaves the alert without counts rather than failing the send", func(t *testing.T) {
		svc := &JobSLAPredictorService{
			l:                 log.NewNoop(),
			config:            config.PotentialSLABreachConfig{},
			downstreamCounter: stubDownstreamCounter{err: assert.AnError},
		}
		alert := &scheduler.PotentialSLABreachAlert{RootCauseJob: "job-C", Reason: scheduler.ReasonUpstreamFailed}

		svc.enrichBreachAlerts(ctx, []*scheduler.PotentialSLABreachAlert{alert})

		assert.Nil(t, alert.DownstreamCounts)
	})

	t.Run("a nil counter leaves alerts untouched", func(t *testing.T) {
		svc := &JobSLAPredictorService{
			l:      log.NewNoop(),
			config: config.PotentialSLABreachConfig{},
		}
		alert := &scheduler.PotentialSLABreachAlert{RootCauseJob: "job-C", Reason: scheduler.ReasonRunningLong}

		svc.enrichBreachAlerts(ctx, []*scheduler.PotentialSLABreachAlert{alert})

		assert.Nil(t, alert.DownstreamCounts)
	})
}
