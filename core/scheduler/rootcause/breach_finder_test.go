//go:build unit_test

package rootcause_test

import (
	"context"
	"testing"
	"time"

	"github.com/goto/salt/log"
	"github.com/stretchr/testify/assert"

	"github.com/goto/optimus/core/scheduler"
	"github.com/goto/optimus/core/scheduler/rootcause"
	"github.com/goto/optimus/core/tenant"
)

// TestIdentifier_FindInferredBreaches covers the shared inferred-SLA breach analysis:
// origins, determinism and the early exits. It is the golden surface the root-cause
// engine builds on.
func TestIdentifier_FindInferredBreaches(t *testing.T) {
	tnnt, err := tenant.NewTenant("project-a", "team-a")
	assert.NoError(t, err)

	scheduledAt := time.Date(2026, 9, 10, 10, 0, 0, 0, time.UTC)
	dur := func(d time.Duration) *time.Duration { return &d }

	damper := scheduler.DamperFactor{Method: scheduler.DamperFactorMethodConstant, BaseValue: 1.0}
	identifier := rootcause.NewIdentifier(log.NewNoop(), fakeScheduledChangeGetter{}, nil, []string{"dex"}, rootcause.IdentifierConfig{})

	node := func(jobName string, enabled bool, runs map[scheduler.JobName]*scheduler.JobRunSummary, upstreams ...*scheduler.JobLineageSummary) *scheduler.JobLineageSummary {
		return &scheduler.JobLineageSummary{
			JobName:   scheduler.JobName(jobName),
			IsEnabled: enabled,
			Tenant:    tnnt,
			Upstreams: upstreams,
			JobRuns:   runs,
		}
	}
	run := func(jobName string) *scheduler.JobRunSummary {
		return &scheduler.JobRunSummary{
			JobName:     scheduler.JobName(jobName),
			ScheduledAt: scheduledAt,
		}
	}
	durations := map[scheduler.JobName]*time.Duration{
		"job-A": dur(10 * time.Minute),
		"job-B": dur(10 * time.Minute),
		"job-C": dur(10 * time.Minute),
		"job-D": dur(10 * time.Minute),
	}
	sla := scheduledAt.Add(30 * time.Minute)
	skipNone := map[scheduler.JobName]bool{}

	t.Run("returns the single breach origin of a linear chain", func(t *testing.T) {
		// A -> B -> C; reference 25m after schedule breaches C's inferred SLA (+10m) and
		// B's (+20m); A's own SLA (+30m) is not yet breached, so C is the only breach origin.
		nodeC := node("job-C", true, map[scheduler.JobName]*scheduler.JobRunSummary{"job-C": run("job-C")})
		nodeB := node("job-B", true, map[scheduler.JobName]*scheduler.JobRunSummary{"job-B": run("job-B")}, nodeC)
		target := node("job-A", true, map[scheduler.JobName]*scheduler.JobRunSummary{"job-A": run("job-A")}, nodeB)

		finding := identifier.FindInferredBreaches(context.Background(), target, durations, nil, &sla, skipNone, damper, scheduledAt.Add(25*time.Minute))

		assert.Len(t, finding.Origins, 1)
		assert.Equal(t, scheduler.JobName("job-C"), finding.Origins[0].JobName)
		// condition 2 (not started past the safe start) takes precedence over condition 1
		// when both hold, matching the original single-run check order
		assert.Equal(t, scheduler.SLABreachCauseNotStarted, finding.Origins[0].Status)
		if assert.Len(t, finding.OriginPaths, 1) && assert.Len(t, finding.OriginPaths[0], 3) {
			assert.Equal(t, scheduler.JobName("job-A"), finding.OriginPaths[0][0].JobName)
			assert.Equal(t, scheduler.JobName("job-B"), finding.OriginPaths[0][1].JobName)
			assert.Equal(t, scheduler.JobName("job-C"), finding.OriginPaths[0][2].JobName)
		}
		assert.Equal(t, scheduler.JobName("job-B"), finding.Bottleneck.Pred[scheduler.JobName("job-C")])
		assert.NotNil(t, finding.SLAStates[scheduler.JobName("job-B")].InferredSLA)
	})

	t.Run("collapses a diamond to the shared breach origin", func(t *testing.T) {
		// A <- {B, C} <- D; only D breaches its inferred SLA.
		shared := node("job-D", true, map[scheduler.JobName]*scheduler.JobRunSummary{"job-D": run("job-D")})
		nodeB := node("job-B", true, map[scheduler.JobName]*scheduler.JobRunSummary{"job-B": run("job-B")}, shared)
		nodeC := node("job-C", true, map[scheduler.JobName]*scheduler.JobRunSummary{"job-C": run("job-C")}, shared)
		target := node("job-A", true, map[scheduler.JobName]*scheduler.JobRunSummary{"job-A": run("job-A")}, nodeB, nodeC)

		finding := identifier.FindInferredBreaches(context.Background(), target, durations, nil, &sla, skipNone, damper, scheduledAt.Add(25*time.Minute))

		assert.Len(t, finding.Origins, 1)
		assert.Equal(t, scheduler.JobName("job-D"), finding.Origins[0].JobName)
	})

	t.Run("returns every independent breach origin in sorted order across repeated runs", func(t *testing.T) {
		// A <- {B, C}; both breach their inferred SLA (+20m) at reference +25m.
		nodeB := node("job-B", true, map[scheduler.JobName]*scheduler.JobRunSummary{"job-B": run("job-B")})
		nodeC := node("job-C", true, map[scheduler.JobName]*scheduler.JobRunSummary{"job-C": run("job-C")})
		target := node("job-A", true, map[scheduler.JobName]*scheduler.JobRunSummary{"job-A": run("job-A")}, nodeC, nodeB)

		var firstOrder []scheduler.JobName
		for range 20 {
			finding := identifier.FindInferredBreaches(context.Background(), target, durations, nil, &sla, skipNone, damper, scheduledAt.Add(25*time.Minute))
			order := make([]scheduler.JobName, 0, len(finding.Origins))
			for _, origin := range finding.Origins {
				order = append(order, origin.JobName)
			}
			if firstOrder == nil {
				firstOrder = order
				assert.Equal(t, []scheduler.JobName{"job-B", "job-C"}, order)
			}
			assert.Equal(t, firstOrder, order, "breach origin order must be deterministic across runs")
		}
	})

	t.Run("returns an empty finding when the target finished before its SLA", func(t *testing.T) {
		finished := run("job-A")
		end := scheduledAt.Add(20 * time.Minute)
		// GetActualEndTime reads HookEndTime then TaskEndTime (JobEndTime alone is not enough)
		finished.TaskEndTime = &end
		finished.JobEndTime = &end
		target := node("job-A", true, map[scheduler.JobName]*scheduler.JobRunSummary{"job-A": finished})

		finding := identifier.FindInferredBreaches(context.Background(), target, durations, nil, &sla, skipNone, damper, scheduledAt.Add(25*time.Minute))

		assert.Empty(t, finding.Origins)
		assert.Empty(t, finding.BreachStates)
	})

	t.Run("skips disabled and skipped jobs", func(t *testing.T) {
		nodeB := node("job-B", true, map[scheduler.JobName]*scheduler.JobRunSummary{"job-B": run("job-B")})
		disabled := node("job-C", false, map[scheduler.JobName]*scheduler.JobRunSummary{"job-C": run("job-C")})
		target := node("job-A", true, map[scheduler.JobName]*scheduler.JobRunSummary{"job-A": run("job-A")}, nodeB, disabled)

		finding := identifier.FindInferredBreaches(context.Background(), target, durations, nil, &sla, skipNone, damper, scheduledAt.Add(25*time.Minute))
		assert.Len(t, finding.Origins, 1)
		assert.Equal(t, scheduler.JobName("job-B"), finding.Origins[0].JobName)

		skipB := map[scheduler.JobName]bool{"job-B": true}
		finding = identifier.FindInferredBreaches(context.Background(), target, durations, nil, &sla, skipB, damper, scheduledAt.Add(25*time.Minute))
		// with B skipped and C disabled, the target itself is the only breaching job left:
		// it has not started past its safe start (inferred SLA -30m -10m ... i.e. +20m)
		if assert.Len(t, finding.Origins, 1) {
			assert.Equal(t, scheduler.JobName("job-A"), finding.Origins[0].JobName)
			assert.Equal(t, scheduler.SLABreachCauseNotStarted, finding.Origins[0].Status)
		}
	})

	t.Run("returns an empty finding without a targeted SLA", func(t *testing.T) {
		nodeB := node("job-B", true, map[scheduler.JobName]*scheduler.JobRunSummary{"job-B": run("job-B")})
		target := node("job-A", true, map[scheduler.JobName]*scheduler.JobRunSummary{"job-A": run("job-A")}, nodeB)

		finding := identifier.FindInferredBreaches(context.Background(), target, durations, nil, nil, skipNone, damper, scheduledAt.Add(25*time.Minute))
		assert.Empty(t, finding.Origins)
	})
}
