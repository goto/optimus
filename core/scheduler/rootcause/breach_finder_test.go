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

	node := func(jobName string, enabled bool, runSummary *scheduler.JobRunSummary, upstreams ...*scheduler.JobLineageSummary) *scheduler.JobLineageSummary {
		n := &scheduler.JobLineageSummary{
			JobName:   scheduler.JobName(jobName),
			IsEnabled: enabled,
			Tenant:    tnnt,
			Upstreams: upstreams,
		}
		n.RecordTargetRun(runSummary)
		var sched time.Time
		if runSummary != nil {
			sched = runSummary.ScheduledAt
		}
		downstreamKey := scheduler.NewJobRunKey(scheduler.JobName(jobName), sched)
		for _, u := range upstreams {
			u.RecordUpstreamRun(downstreamKey, u.GetTargetRun())
		}
		return n
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
		nodeC := node("job-C", true, run("job-C"))
		nodeB := node("job-B", true, run("job-B"), nodeC)
		target := node("job-A", true, run("job-A"), nodeB)

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
		shared := node("job-D", true, run("job-D"))
		nodeB := node("job-B", true, run("job-B"), shared)
		nodeC := node("job-C", true, run("job-C"), shared)
		target := node("job-A", true, run("job-A"), nodeB, nodeC)

		finding := identifier.FindInferredBreaches(context.Background(), target, durations, nil, &sla, skipNone, damper, scheduledAt.Add(25*time.Minute))

		assert.Len(t, finding.Origins, 1)
		assert.Equal(t, scheduler.JobName("job-D"), finding.Origins[0].JobName)
	})

	t.Run("returns every independent breach origin in sorted order across repeated runs", func(t *testing.T) {
		// A <- {B, C}; both breach their inferred SLA (+20m) at reference +25m.
		nodeB := node("job-B", true, run("job-B"))
		nodeC := node("job-C", true, run("job-C"))
		target := node("job-A", true, run("job-A"), nodeC, nodeB)

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
		target := node("job-A", true, finished)

		finding := identifier.FindInferredBreaches(context.Background(), target, durations, nil, &sla, skipNone, damper, scheduledAt.Add(25*time.Minute))

		assert.Empty(t, finding.Origins)
		assert.Empty(t, finding.BreachStates)
	})

	t.Run("skips disabled and skipped jobs", func(t *testing.T) {
		nodeB := node("job-B", true, run("job-B"))
		disabled := node("job-C", false, run("job-C"))
		target := node("job-A", true, run("job-A"), nodeB, disabled)

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
		nodeB := node("job-B", true, run("job-B"))
		target := node("job-A", true, run("job-A"), nodeB)

		finding := identifier.FindInferredBreaches(context.Background(), target, durations, nil, nil, skipNone, damper, scheduledAt.Add(25*time.Minute))
		assert.Empty(t, finding.Origins)
	})

	t.Run("computes independent per-run inferred SLAs across diverging branches and avoids false-positive breaches", func(t *testing.T) {
		// Root@09:00 (SLA 10:00, dur 0m):
		//   Branch A (dur 10m, inferred 10:00) -> B@7am (dur 10m, inferred 09:50, safeStart 09:40) -> D@6am (dur 10m, inferred 09:40)
		//   Branch C (dur 30m, inferred 10:00) -> B@3am (dur 10m, inferred 09:30, safeStart 09:20) -> D@2am (dur 10m, inferred 09:20)
		// At 09:35: C, B@3am, D@2am have finished; B@7am and D@6am are not yet started (safeStarts 09:40 and 09:30,
		// wait: set refTime = 09:25 so D@6am safeStart 09:30 hasn't passed, whereas C's branch safeStart 09:20 HAS passed!).
		// Because B@7am's per-run safe start is 09:40 (not C's 09:20), no breach is reported.
		t09 := time.Date(2026, 9, 10, 9, 0, 0, 0, time.UTC)
		t08 := time.Date(2026, 9, 10, 8, 0, 0, 0, time.UTC)
		t07 := time.Date(2026, 9, 10, 7, 0, 0, 0, time.UTC)
		t06 := time.Date(2026, 9, 10, 6, 0, 0, 0, time.UTC)
		t03 := time.Date(2026, 9, 10, 3, 0, 0, 0, time.UTC)
		t02 := time.Date(2026, 9, 10, 2, 0, 0, 0, time.UTC)
		timePtr := func(tm time.Time) *time.Time { return &tm }

		branchDurations := map[scheduler.JobName]*time.Duration{
			"job-root": dur(0),
			"job-A":    dur(10 * time.Minute),
			"job-C":    dur(30 * time.Minute),
			"job-B":    dur(10 * time.Minute),
			"job-D":    dur(10 * time.Minute),
		}
		targetSLA := t09.Add(1 * time.Hour) // 10:00

		nodeD := &scheduler.JobLineageSummary{JobName: "job-D", IsEnabled: true, Tenant: tnnt}
		nodeD.RecordUpstreamRun(scheduler.NewJobRunKey("job-B", t07), &scheduler.JobRunSummary{
			JobName: "job-D", ScheduledAt: t06, TaskStartTime: timePtr(t06), TaskEndTime: timePtr(t06.Add(5 * time.Minute)), JobEndTime: timePtr(t06.Add(5 * time.Minute)),
		})
		nodeD.RecordUpstreamRun(scheduler.NewJobRunKey("job-B", t03), &scheduler.JobRunSummary{
			JobName: "job-D", ScheduledAt: t02, TaskStartTime: timePtr(t02), TaskEndTime: timePtr(t02.Add(5 * time.Minute)), JobEndTime: timePtr(t02.Add(5 * time.Minute)),
		})

		nodeB := &scheduler.JobLineageSummary{JobName: "job-B", IsEnabled: true, Tenant: tnnt, Upstreams: []*scheduler.JobLineageSummary{nodeD}}
		nodeB.RecordUpstreamRun(scheduler.NewJobRunKey("job-A", t08), &scheduler.JobRunSummary{
			JobName: "job-B", ScheduledAt: t07,
		})
		nodeB.RecordUpstreamRun(scheduler.NewJobRunKey("job-C", t08), &scheduler.JobRunSummary{
			JobName: "job-B", ScheduledAt: t03, TaskStartTime: timePtr(t03), TaskEndTime: timePtr(t03.Add(5 * time.Minute)), JobEndTime: timePtr(t03.Add(5 * time.Minute)),
		})

		nodeA := &scheduler.JobLineageSummary{JobName: "job-A", IsEnabled: true, Tenant: tnnt, Upstreams: []*scheduler.JobLineageSummary{nodeB}}
		nodeA.RecordUpstreamRun(scheduler.NewJobRunKey("job-root", t09), &scheduler.JobRunSummary{JobName: "job-A", ScheduledAt: t08})

		nodeC := &scheduler.JobLineageSummary{JobName: "job-C", IsEnabled: true, Tenant: tnnt, Upstreams: []*scheduler.JobLineageSummary{nodeB}}
		nodeC.RecordUpstreamRun(scheduler.NewJobRunKey("job-root", t09), &scheduler.JobRunSummary{
			JobName: "job-C", ScheduledAt: t08, TaskStartTime: timePtr(t08), TaskEndTime: timePtr(t08.Add(20 * time.Minute)), JobEndTime: timePtr(t08.Add(20 * time.Minute)),
		})

		root := &scheduler.JobLineageSummary{
			JobName:   "job-root",
			IsEnabled: true,
			Tenant:    tnnt,
			Upstreams: []*scheduler.JobLineageSummary{nodeA, nodeC},
		}
		root.RecordTargetRun(&scheduler.JobRunSummary{JobName: "job-root", ScheduledAt: t09})

		finding := identifier.FindInferredBreaches(context.Background(), root, branchDurations, nil, &targetSLA, skipNone, damper, t09.Add(25*time.Minute))

		assert.Empty(t, finding.Origins)
		assert.Equal(t, t09.Add(50*time.Minute), *finding.RunInferredSLAs[scheduler.NewJobRunKey("job-B", t07)])
		assert.Equal(t, t09.Add(30*time.Minute), *finding.RunInferredSLAs[scheduler.NewJobRunKey("job-B", t03)])
		assert.Equal(t, t09.Add(40*time.Minute), *finding.RunInferredSLAs[scheduler.NewJobRunKey("job-D", t06)])
		assert.Equal(t, t09.Add(20*time.Minute), *finding.RunInferredSLAs[scheduler.NewJobRunKey("job-D", t02)])
	})

	t.Run("identifies distinct run-specific breach origins across shared intermediate job runs", func(t *testing.T) {
		// Root@09:00 (SLA 10:00, dur 0m) evaluated at 09:45:
		//   Branch A -> B@7am (not started, safe start 09:40 -> BREACHED) -> D@6am (started & finished -> healthy)
		//   Branch C -> B@3am (not started, safe start 09:20) -> D@2am (not started, safe start 09:10 -> BREACHED)
		// Origins must be B@7am (since D@6am finished) AND D@2am (blocking B@3am and C), each with its exact path.
		t09 := time.Date(2026, 9, 10, 9, 0, 0, 0, time.UTC)
		t08 := time.Date(2026, 9, 10, 8, 0, 0, 0, time.UTC)
		t07 := time.Date(2026, 9, 10, 7, 0, 0, 0, time.UTC)
		t06 := time.Date(2026, 9, 10, 6, 0, 0, 0, time.UTC)
		t03 := time.Date(2026, 9, 10, 3, 0, 0, 0, time.UTC)
		t02 := time.Date(2026, 9, 10, 2, 0, 0, 0, time.UTC)
		timePtr := func(tm time.Time) *time.Time { return &tm }

		branchDurations := map[scheduler.JobName]*time.Duration{
			"job-root": dur(0),
			"job-A":    dur(10 * time.Minute),
			"job-C":    dur(30 * time.Minute),
			"job-B":    dur(10 * time.Minute),
			"job-D":    dur(10 * time.Minute),
		}
		targetSLA := t09.Add(1 * time.Hour)

		nodeD := &scheduler.JobLineageSummary{JobName: "job-D", IsEnabled: true, Tenant: tnnt}
		nodeD.RecordUpstreamRun(scheduler.NewJobRunKey("job-B", t07), &scheduler.JobRunSummary{
			JobName: "job-D", ScheduledAt: t06, TaskStartTime: timePtr(t06), TaskEndTime: timePtr(t06.Add(5 * time.Minute)), JobEndTime: timePtr(t06.Add(5 * time.Minute)),
		})
		nodeD.RecordUpstreamRun(scheduler.NewJobRunKey("job-B", t03), &scheduler.JobRunSummary{
			JobName: "job-D", ScheduledAt: t02,
		})

		nodeB := &scheduler.JobLineageSummary{JobName: "job-B", IsEnabled: true, Tenant: tnnt, Upstreams: []*scheduler.JobLineageSummary{nodeD}}
		nodeB.RecordUpstreamRun(scheduler.NewJobRunKey("job-A", t08), &scheduler.JobRunSummary{JobName: "job-B", ScheduledAt: t07})
		nodeB.RecordUpstreamRun(scheduler.NewJobRunKey("job-C", t08), &scheduler.JobRunSummary{JobName: "job-B", ScheduledAt: t03})

		nodeA := &scheduler.JobLineageSummary{JobName: "job-A", IsEnabled: true, Tenant: tnnt, Upstreams: []*scheduler.JobLineageSummary{nodeB}}
		nodeA.RecordUpstreamRun(scheduler.NewJobRunKey("job-root", t09), &scheduler.JobRunSummary{JobName: "job-A", ScheduledAt: t08})

		nodeC := &scheduler.JobLineageSummary{JobName: "job-C", IsEnabled: true, Tenant: tnnt, Upstreams: []*scheduler.JobLineageSummary{nodeB}}
		nodeC.RecordUpstreamRun(scheduler.NewJobRunKey("job-root", t09), &scheduler.JobRunSummary{JobName: "job-C", ScheduledAt: t08})

		root := &scheduler.JobLineageSummary{
			JobName:   "job-root",
			IsEnabled: true,
			Tenant:    tnnt,
			Upstreams: []*scheduler.JobLineageSummary{nodeA, nodeC},
		}
		root.RecordTargetRun(&scheduler.JobRunSummary{JobName: "job-root", ScheduledAt: t09})

		finding := identifier.FindInferredBreaches(context.Background(), root, branchDurations, nil, &targetSLA, skipNone, damper, t09.Add(45*time.Minute))

		if assert.Len(t, finding.Origins, 2) {
			originsByKey := map[scheduler.JobRunKey]*scheduler.JobState{}
			for _, o := range finding.Origins {
				originsByKey[scheduler.NewJobRunKey(o.JobName, o.JobRun.ScheduledAt)] = o
			}
			assert.Contains(t, originsByKey, scheduler.NewJobRunKey("job-B", t07))
			assert.Contains(t, originsByKey, scheduler.NewJobRunKey("job-D", t02))
		}
		assert.Nil(t, finding.RunBreachStates[scheduler.NewJobRunKey("job-D", t06)])
		assert.NotNil(t, finding.RunBreachStates[scheduler.NewJobRunKey("job-D", t02)])

		if assert.Len(t, finding.OriginPaths, 2) {
			pathByLeaf := map[scheduler.JobRunKey][]scheduler.JobRunKey{}
			for _, p := range finding.OriginPaths {
				var keys []scheduler.JobRunKey
				for _, step := range p {
					keys = append(keys, scheduler.NewJobRunKey(step.JobName, step.JobRun.ScheduledAt))
				}
				pathByLeaf[keys[len(keys)-1]] = keys
			}
			assert.Equal(t, []scheduler.JobRunKey{
				scheduler.NewJobRunKey("job-root", t09),
				scheduler.NewJobRunKey("job-A", t08),
				scheduler.NewJobRunKey("job-B", t07),
			}, pathByLeaf[scheduler.NewJobRunKey("job-B", t07)])
			assert.Equal(t, []scheduler.JobRunKey{
				scheduler.NewJobRunKey("job-root", t09),
				scheduler.NewJobRunKey("job-C", t08),
				scheduler.NewJobRunKey("job-B", t03),
				scheduler.NewJobRunKey("job-D", t02),
			}, pathByLeaf[scheduler.NewJobRunKey("job-D", t02)])
		}
	})
}
