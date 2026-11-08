package rootcause_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/goto/optimus/core/scheduler"
	"github.com/goto/optimus/core/scheduler/rootcause"
	"github.com/goto/optimus/core/tenant"
)

func mustTenant(project, namespace string) tenant.Tenant {
	tnnt, err := tenant.NewTenant(project, namespace)
	if err != nil {
		panic(err)
	}
	return tnnt
}

var (
	testTenant   = mustTenant("p_root", "ns1")
	tenantProjA  = mustTenant("p_a", "ns1")
	tenantProjB  = mustTenant("p_b", "ns1")
	testBaseTime = time.Date(2026, 10, 26, 10, 0, 0, 0, time.UTC)
	targetSLA    = 2 * time.Hour
)

func ptr(t time.Time) *time.Time { return &t }

func strPtr(s string) *string { return &s }

// makeRun builds a JobRunSummary the way the resolver does for a resolved run: SLATime is
// always set, status is derived from the given timestamps plus an explicit job status.
func makeRun(jobName scheduler.JobName, scheduledAt time.Time, taskStart, taskEnd *time.Time, status string) *scheduler.JobRunSummary {
	slaTime := scheduledAt.Add(targetSLA)
	run := &scheduler.JobRunSummary{
		JobName:     jobName,
		ScheduledAt: scheduledAt,
		SLATime:     &slaTime,
		JobStatus:   status,
	}
	run.TaskStartTime = taskStart
	run.TaskEndTime = taskEnd
	return run
}

// makeLineage assembles a node with its upstreams the way the resolver links them: each
// upstream carries, under the downstream's name, the run the downstream depends on.
func makeLineage(jobName scheduler.JobName, tnnt tenant.Tenant, run *scheduler.JobRunSummary, upstreams ...*scheduler.JobLineageSummary) *scheduler.JobLineageSummary {
	lineage := &scheduler.JobLineageSummary{
		JobName:   jobName,
		Tenant:    tnnt,
		IsEnabled: true,
		JobRuns:   map[scheduler.JobName]*scheduler.JobRunSummary{jobName: run},
	}
	for _, upstream := range upstreams {
		upstream.JobRuns[jobName] = upstream.JobRuns[upstream.JobName]
	}
	if len(upstreams) > 0 {
		lineage.Upstreams = upstreams
	}
	return lineage
}

func testPublisher() *rootcause.Publisher {
	return rootcause.NewPublisher(rootcause.PublisherConfig{
		RuntimeDelayThreshold:       30 * time.Minute,
		SchedulerDelayPerDepth:      150 * time.Second,
		ThirdPartyDelayThreshold:    10 * time.Minute,
		ThirdPartyDelayStartHourUTC: 18, // 01:00 JKT
		ThirdPartyTypes:             []string{"dex"},
	})
}

func detectionsByReason(detections []scheduler.RootCauseDetection, reason scheduler.SLABreachTier) []scheduler.RootCauseDetection {
	var matched []scheduler.RootCauseDetection
	for _, detection := range detections {
		if detection.Reason.Reason == reason {
			matched = append(matched, detection)
		}
	}
	return matched
}

// permissiveFinding builds a gate-passing BreachFinding whose only leaf is the named job.
// The reason detectors operate on the walk, not on the finding, so tier tests only need
// the gate satisfied; gate-specific tests build their own findings.
func permissiveFinding(target *scheduler.JobLineageSummary) *rootcause.BreachFinding {
	return &rootcause.BreachFinding{
		Leaves: []*scheduler.JobState{{
			JobName: target.JobName,
			JobRun:  scheduler.JobRunSummary{JobName: target.JobName},
		}},
	}
}

func eval(target *scheduler.JobLineageSummary, historical map[scheduler.JobName]scheduler.JobHistoricalDuration, referenceTime time.Time) []scheduler.RootCauseDetection {
	return testPublisher().Evaluate(rootcause.TargetEvaluation{
		Target:          target,
		ReferenceTime:   referenceTime,
		HistoricalByJob: historical,
		Finding:         permissiveFinding(target),
	})
}

func TestPublisher_Evaluate(t *testing.T) {
	// the reference time is past the target's SLA deadline (10:00 + 2h), so a still-running
	// target counts as breaching
	referenceTime := testBaseTime.Add(3 * time.Hour)

	// a target whose own runtime matches its historical baseline so it induces nothing
	targetBaseline := scheduler.JobHistoricalDuration{TaskDuration: 2 * time.Hour, HookDuration: time.Hour}

	t.Run("returns nothing when the target did not breach its SLA", func(t *testing.T) {
		// target finished at 11:00, within its 12:00 deadline
		targetRun := makeRun("job_target", testBaseTime,
			ptr(testBaseTime.Add(time.Hour)), ptr(testBaseTime.Add(time.Hour+10*time.Minute)), "success")
		target := makeLineage("job_target", testTenant, targetRun)

		assert.Empty(t, eval(target, nil, testBaseTime.Add(2*time.Hour)))
	})

	t.Run("tier 1 condition A: individually delayed job becomes its own incident", func(t *testing.T) {
		// upstream ran 45m against a 10m baseline: 35m induced, over the 30m threshold
		upstreamRun := makeRun("job_upstream", testBaseTime.Add(-time.Hour),
			ptr(testBaseTime.Add(-time.Hour)), ptr(testBaseTime.Add(-15*time.Minute)), "success")
		targetRun := makeRun("job_target", testBaseTime,
			ptr(testBaseTime.Add(time.Hour)), nil, "running")
		target := makeLineage("job_target", testTenant, targetRun,
			makeLineage("job_upstream", testTenant, upstreamRun))

		historical := map[scheduler.JobName]scheduler.JobHistoricalDuration{
			"job_upstream": {TaskDuration: 10 * time.Minute},
			"job_target":   targetBaseline,
		}

		detections := eval(target, historical, referenceTime)

		matched := detectionsByReason(detections, scheduler.TierRuntimeDelay)
		assert.Len(t, matched, 1)
		assert.Equal(t, "job_upstream", matched[0].IncidentKey.JobName.String())
		assert.Equal(t, int(35*time.Minute/time.Second), matched[0].Reason.InducedDelaySeconds)
		assert.True(t, matched[0].Reason.IsJobFinished, "the offending run has finished")
		assert.Equal(t, scheduler.ReasonStatusActive, matched[0].Reason.Status)
		assert.Equal(t, "job_target", matched[0].ImpactedJobs[0].JobName.String())
	})

	t.Run("tier 1 conditions B and C: project flat sum fires for the max-delayed project only", func(t *testing.T) {
		// chain target <- a1(p_a) <- a2(p_a) <- b1(p_b); no job except b1 crosses 30m alone,
		// p_a flat-sums to 45m and p_b's b1 fires individually — so p_a is the only
		// project-level detection, keyed on its max-delay job a2 with the project total
		b1Run := makeRun("job_b1", testBaseTime.Add(-3*time.Hour),
			ptr(testBaseTime.Add(-3*time.Hour)), ptr(testBaseTime.Add(-2*time.Hour-15*time.Minute)), "success")
		a2Run := makeRun("job_a2", testBaseTime.Add(-2*time.Hour),
			ptr(testBaseTime.Add(-2*time.Hour)), ptr(testBaseTime.Add(-1*time.Hour-35*time.Minute)), "success")
		a1Run := makeRun("job_a1", testBaseTime.Add(-90*time.Minute),
			ptr(testBaseTime.Add(-90*time.Minute)), ptr(testBaseTime.Add(-70*time.Minute)), "success")
		targetRun := makeRun("job_target", testBaseTime,
			ptr(testBaseTime.Add(time.Hour)), nil, "running")
		target := makeLineage("job_target", testTenant, targetRun,
			makeLineage("job_a1", tenantProjA, a1Run,
				makeLineage("job_a2", tenantProjA, a2Run,
					makeLineage("job_b1", tenantProjB, b1Run))))

		historical := map[scheduler.JobName]scheduler.JobHistoricalDuration{
			"job_target": targetBaseline,
		}

		detections := eval(target, historical, referenceTime)

		matched := detectionsByReason(detections, scheduler.TierRuntimeDelay)
		assert.Len(t, matched, 2)

		byJob := map[string]scheduler.RootCauseDetection{}
		for _, detection := range matched {
			byJob[detection.IncidentKey.JobName.String()] = detection
		}
		// condition 1: b1 is individually over the threshold
		assert.Equal(t, 2700, byJob["job_b1"].Reason.InducedDelaySeconds) // 45m
		// condition 2/3: p_a's 45m flat sum, keyed on its max-delay job a2
		projectDetection := byJob["job_a2"]
		assert.Equal(t, "p_a", projectDetection.IncidentKey.ProjectName.String())
		assert.Equal(t, 2700, projectDetection.Reason.InducedDelaySeconds) // 45m project total
	})

	t.Run("tier 2: scheduler-attributable wait beyond the per-level budget", func(t *testing.T) {
		// a2 finished at 09:15; b1 was scheduled 09:00 but only started 10:40 — a wait the
		// scheduler owns (85m), far above the 2.5m level-1 budget
		a2Run := makeRun("job_a2", testBaseTime.Add(-2*time.Hour),
			ptr(testBaseTime.Add(-2*time.Hour)), ptr(testBaseTime.Add(-45*time.Minute)), "success")
		b1Run := makeRun("job_b1", testBaseTime.Add(-time.Hour),
			ptr(testBaseTime.Add(40*time.Minute)), ptr(testBaseTime.Add(45*time.Minute)), "success")
		targetRun := makeRun("job_target", testBaseTime,
			ptr(testBaseTime.Add(time.Hour)), nil, "running")
		target := makeLineage("job_target", testTenant, targetRun,
			makeLineage("job_b1", tenantProjB, b1Run,
				makeLineage("job_a2", tenantProjA, a2Run)))

		historical := map[scheduler.JobName]scheduler.JobHistoricalDuration{
			"job_target": {TaskDuration: 90 * time.Minute},
			"job_b1":     {TaskDuration: 5 * time.Minute},
			"job_a2":     {TaskDuration: 75 * time.Minute},
		}

		detections := eval(target, historical, referenceTime)

		matched := detectionsByReason(detections, scheduler.TierSchedulerDelay)
		assert.Len(t, matched, 1)
		assert.Equal(t, "job_b1", matched[0].IncidentKey.JobName.String())
		// 10:40 - max(09:00, 09:15) = 85 minutes
		assert.Equal(t, int(85*time.Minute/time.Second), matched[0].Reason.InducedDelaySeconds)
	})

	t.Run("tier 2 skipped when the gating upstream failed; tier 4 fires instead", func(t *testing.T) {
		// a2 failed at 08:10; b1 started anyway 20 minutes later — its wait is not charged
		// to the scheduler, and the failed upstream is the reported cause
		a2Run := makeRun("job_a2", testBaseTime.Add(-2*time.Hour),
			ptr(testBaseTime.Add(-2*time.Hour)), ptr(testBaseTime.Add(-110*time.Minute)), "failed")
		b1Run := makeRun("job_b1", testBaseTime.Add(-time.Hour),
			ptr(testBaseTime.Add(-90*time.Minute)), ptr(testBaseTime.Add(-80*time.Minute)), "success")
		targetRun := makeRun("job_target", testBaseTime,
			ptr(testBaseTime.Add(time.Hour)), nil, "running")
		target := makeLineage("job_target", testTenant, targetRun,
			makeLineage("job_b1", tenantProjB, b1Run,
				makeLineage("job_a2", tenantProjA, a2Run)))

		historical := map[scheduler.JobName]scheduler.JobHistoricalDuration{
			"job_target": {TaskDuration: 90 * time.Minute},
			"job_b1":     {TaskDuration: 10 * time.Minute},
		}

		detections := eval(target, historical, referenceTime)

		assert.Empty(t, detectionsByReason(detections, scheduler.TierSchedulerDelay))
		failed := detectionsByReason(detections, scheduler.TierFailedUpstream)
		assert.Len(t, failed, 1)
		assert.Equal(t, "job_a2", failed[0].IncidentKey.JobName.String())
		assert.Equal(t, "p_a", failed[0].IncidentKey.ProjectName.String())
	})

	t.Run("tier 3: pending third-party sensor wait keys on the daily 01:00 JKT boundary", func(t *testing.T) {
		// target scheduled 20:00 UTC the previous day (SLA deadline 22:00 UTC), waiting on its
		// own wait_dex sensor since 20:00 UTC; the reference time is 03:00 UTC the next day
		// (10:00 JKT)
		refTime := time.Date(2026, 10, 26, 3, 0, 0, 0, time.UTC)
		targetRun := makeRun("job_target", time.Date(2026, 10, 25, 20, 0, 0, 0, time.UTC), nil, nil, "running")
		targetRun.SensorName = strPtr("wait_dex_event_delivery")
		targetRun.WaitStartTime = ptr(time.Date(2026, 10, 25, 20, 0, 0, 0, time.UTC))
		target := makeLineage("job_target", testTenant, targetRun)

		detections := eval(target, nil, refTime)

		assert.Len(t, detections, 1)
		assert.Equal(t, scheduler.TierThirdPartyDelay, detections[0].Reason.Reason)
		// 01:00 JKT on the reference date = 18:00 UTC the previous day
		assert.Equal(t, time.Date(2026, 10, 25, 18, 0, 0, 0, time.UTC), detections[0].IncidentKey.ScheduledAt)
		assert.Equal(t, []string{"wait_dex_event_delivery"}, detections[0].Reason.BlockedOnSensors)
		assert.Equal(t, "dex", detections[0].Reason.SourceType)
		// wait counted from the sensor's own start (20:00 UTC, after the boundary floor):
		// 03:00 - 20:00 = 7 hours
		assert.Equal(t, 7*3600, detections[0].Reason.InducedDelaySeconds)
		// the sensor wait is Tier 3's finding; Tier 5 must not double-report the stuck target
		assert.Empty(t, detectionsByReason(detections, scheduler.TierWaitingUpstream))
	})

	t.Run("tier 5: stuck node with successful upstreams becomes the incident", func(t *testing.T) {
		// a2 succeeded; b1 has no run row at all (missing run) but is past its own SLA
		a2Run := makeRun("job_a2", testBaseTime.Add(-2*time.Hour),
			ptr(testBaseTime.Add(-2*time.Hour)), ptr(testBaseTime.Add(-90*time.Minute)), "success")
		b1Run := makeRun("job_b1", testBaseTime.Add(-time.Hour), nil, nil, "")
		targetRun := makeRun("job_target", testBaseTime,
			ptr(testBaseTime.Add(time.Hour)), nil, "running")
		target := makeLineage("job_target", testTenant, targetRun,
			makeLineage("job_b1", tenantProjB, b1Run,
				makeLineage("job_a2", tenantProjA, a2Run)))

		historical := map[scheduler.JobName]scheduler.JobHistoricalDuration{
			"job_target": {TaskDuration: 90 * time.Minute},
		}

		detections := eval(target, historical, referenceTime)

		matched := detectionsByReason(detections, scheduler.TierWaitingUpstream)
		assert.Len(t, matched, 1)
		assert.Equal(t, "job_b1", matched[0].IncidentKey.JobName.String())
	})

	t.Run("tier 5 skipped while the upstream is still running; tier 1 reports the slow run", func(t *testing.T) {
		// a2 has been running 4 hours against a 30m baseline: the bottleneck is its runtime,
		// not a structural stall, so b1 waiting on it is normal
		a2Run := makeRun("job_a2", testBaseTime.Add(-2*time.Hour),
			ptr(testBaseTime.Add(-90*time.Minute)), nil, "running")
		b1Run := makeRun("job_b1", testBaseTime.Add(-time.Hour), nil, nil, "")
		targetRun := makeRun("job_target", testBaseTime,
			ptr(testBaseTime.Add(time.Hour)), nil, "running")
		target := makeLineage("job_target", testTenant, targetRun,
			makeLineage("job_b1", tenantProjB, b1Run,
				makeLineage("job_a2", tenantProjA, a2Run)))

		historical := map[scheduler.JobName]scheduler.JobHistoricalDuration{
			"job_target": {TaskDuration: 90 * time.Minute},
			"job_a2":     {TaskDuration: 30 * time.Minute},
		}

		detections := eval(target, historical, referenceTime)

		assert.Empty(t, detectionsByReason(detections, scheduler.TierWaitingUpstream))
		matched := detectionsByReason(detections, scheduler.TierRuntimeDelay)
		assert.Len(t, matched, 1)
		assert.Equal(t, "job_a2", matched[0].IncidentKey.JobName.String())
		assert.False(t, matched[0].Reason.IsJobFinished, "the offending run is still running")
		// reference - task start - baseline = 4h30m - 30m
		assert.Equal(t, int((240*time.Minute)/time.Second), matched[0].Reason.InducedDelaySeconds)
	})

	t.Run("tier 6: breached with no upstream explanation is an aggressive SLA", func(t *testing.T) {
		// target started on time and runs within its baseline, yet past its SLA deadline
		targetRun := makeRun("job_target", testBaseTime,
			ptr(testBaseTime), nil, "running")
		target := makeLineage("job_target", testTenant, targetRun)

		historical := map[scheduler.JobName]scheduler.JobHistoricalDuration{
			"job_target": {TaskDuration: 4 * time.Hour},
		}

		detections := eval(target, historical, referenceTime)

		assert.Len(t, detections, 1)
		assert.Equal(t, scheduler.TierAggressiveSLA, detections[0].Reason.Reason)
		assert.Equal(t, "job_target", detections[0].IncidentKey.JobName.String())
		assert.Equal(t, "p_root", detections[0].IncidentKey.ProjectName.String())
	})

	t.Run("an in-flight lineage keeps the finished history behind the blocking frontier", func(t *testing.T) {
		// target waits on b1 (stuck on its own sensor), whose upstream a2 already ran 45m over
		// its 30m baseline — both belong on the wait chain even though the gating path stops at
		// the target
		a2Run := makeRun("job_a2", testBaseTime.Add(-2*time.Hour),
			ptr(testBaseTime.Add(-2*time.Hour)), ptr(testBaseTime.Add(-45*time.Minute)), "success")
		b1Run := makeRun("job_b1", testBaseTime.Add(-time.Hour), nil, nil, "running")
		b1Run.SensorName = strPtr("wait_dex_events")
		b1Run.WaitStartTime = ptr(testBaseTime.Add(-30 * time.Minute))
		targetRun := makeRun("job_target", testBaseTime,
			ptr(testBaseTime.Add(time.Hour)), nil, "running")
		target := makeLineage("job_target", testTenant, targetRun,
			makeLineage("job_b1", tenantProjB, b1Run,
				makeLineage("job_a2", tenantProjA, a2Run)))

		historical := map[scheduler.JobName]scheduler.JobHistoricalDuration{
			"job_target": {TaskDuration: 90 * time.Minute},
			"job_a2":     {TaskDuration: 30 * time.Minute},
		}

		detections := eval(target, historical, referenceTime)

		assert.NotEmpty(t, detectionsByReason(detections, scheduler.TierThirdPartyDelay))
		runtime := detectionsByReason(detections, scheduler.TierRuntimeDelay)
		assert.Len(t, runtime, 1)
		assert.Equal(t, "job_a2", runtime[0].IncidentKey.JobName.String())
	})
}

// TestPublisher_InferredBreachGate covers the potential-first gate: without a leaf cause
// from the inferred-SLA analysis nothing is detected, and breach status follows the
// target's own deadline.
func TestPublisher_InferredBreachGate(t *testing.T) {
	publisher := testPublisher()
	// reference is past the target's SLA (10:00 + 2h)
	referenceTime := testBaseTime.Add(3 * time.Hour)
	targetRun := makeRun("job_target", testBaseTime,
		ptr(testBaseTime.Add(time.Hour)), nil, "running")
	target := makeLineage("job_target", testTenant, targetRun)
	historical := map[scheduler.JobName]scheduler.JobHistoricalDuration{
		"job_target": {TaskDuration: 4 * time.Hour},
	}

	t.Run("no finding means no detection", func(t *testing.T) {
		detections := publisher.Evaluate(rootcause.TargetEvaluation{
			Target: target, HistoricalByJob: historical, ReferenceTime: referenceTime,
		})
		assert.Empty(t, detections)
	})

	t.Run("a finding without leaves means no detection", func(t *testing.T) {
		detections := publisher.Evaluate(rootcause.TargetEvaluation{
			Target: target, HistoricalByJob: historical, ReferenceTime: referenceTime,
			Finding: &rootcause.BreachFinding{},
		})
		assert.Empty(t, detections)
	})

	t.Run("a target past its SLA is an actual breach", func(t *testing.T) {
		detections := eval(target, historical, referenceTime)
		if assert.Len(t, detections, 1) {
			assert.Equal(t, scheduler.TierAggressiveSLA, detections[0].Reason.Reason)
			assert.Equal(t, scheduler.BreachStatusActual, detections[0].BreachStatus)
		}
	})

	t.Run("a target within its SLA with an upstream breach is potential", func(t *testing.T) {
		// the target is still within its SLA deadline (reference before 12:00)
		withinSLA := testBaseTime.Add(time.Hour)
		detections := eval(target, historical, withinSLA)
		// no reason applies here (the target started on time and is within baseline), so
		// a potential breach with no cause stays unrecorded
		assert.Empty(t, detections)

		// with an upstream slow enough to detect, the same finding is a potential breach
		upstreamRun := makeRun("job_upstream", testBaseTime.Add(-time.Hour),
			ptr(testBaseTime.Add(-time.Hour)), ptr(testBaseTime.Add(-15*time.Minute)), "success")
		upstream := makeLineage("job_upstream", testTenant, upstreamRun)
		targetWithUpstream := makeLineage("job_target", testTenant, targetRun, upstream)
		historical["job_upstream"] = scheduler.JobHistoricalDuration{TaskDuration: 10 * time.Minute}
		detections = eval(targetWithUpstream, historical, withinSLA)
		if assert.Len(t, detections, 1) {
			assert.Equal(t, scheduler.TierRuntimeDelay, detections[0].Reason.Reason)
			assert.Equal(t, scheduler.BreachStatusPotential, detections[0].BreachStatus)
		}
	})

	t.Run("a leaf other than the target never becomes AGGRESSIVE_SLA", func(t *testing.T) {
		finding := &rootcause.BreachFinding{
			Leaves: []*scheduler.JobState{{
				JobName: "job_upstream",
				JobRun:  scheduler.JobRunSummary{JobName: "job_upstream"},
			}},
		}
		detections := publisher.Evaluate(rootcause.TargetEvaluation{
			Target: target, HistoricalByJob: historical, ReferenceTime: referenceTime,
			Finding: finding,
		})
		assert.Empty(t, detections)
	})
}

// TestPublisher_CauseChain anchors RUNTIME_DELAY and THIRD_PARTY_DELAY attribution on the
// inferred-breach leaves instead of the target's whole wait chain.
func TestPublisher_CauseChain(t *testing.T) {
	publisher := testPublisher()
	referenceTime := testBaseTime.Add(3 * time.Hour) // past the target's SLA

	// chain: target <- stuck leaf-side frontier (b1 waiting on its sensor) <- a2 finished
	// far over baseline <- healthy c1 that the gating path would have included
	c1Run := makeRun("job_c1", testBaseTime.Add(-4*time.Hour),
		ptr(testBaseTime.Add(-4*time.Hour)), ptr(testBaseTime.Add(-2*time.Hour)), "success")
	a2Run := makeRun("job_a2", testBaseTime.Add(-2*time.Hour),
		ptr(testBaseTime.Add(-2*time.Hour)), ptr(testBaseTime.Add(-45*time.Minute)), "success")
	b1Run := makeRun("job_b1", testBaseTime.Add(-time.Hour), nil, nil, "running")
	b1Run.SensorName = strPtr("wait_dex_events")
	b1Run.WaitStartTime = ptr(testBaseTime.Add(-30 * time.Minute))
	targetRun := makeRun("job_target", testBaseTime,
		ptr(testBaseTime.Add(time.Hour)), nil, "running")
	target := makeLineage("job_target", testTenant, targetRun,
		makeLineage("job_b1", tenantProjB, b1Run,
			makeLineage("job_a2", tenantProjA, a2Run,
				makeLineage("job_c1", tenantProjB, c1Run))))

	historical := map[scheduler.JobName]scheduler.JobHistoricalDuration{
		"job_target": {TaskDuration: 90 * time.Minute},
		"job_a2":     {TaskDuration: 30 * time.Minute},
		"job_c1":     {TaskDuration: 2 * time.Hour},
	}
	// the leaf cause is a2, the finished job whose runtime blew past its baseline
	finding := &rootcause.BreachFinding{
		Leaves: []*scheduler.JobState{{
			JobName: "job_a2",
			JobRun:  scheduler.JobRunSummary{JobName: "job_a2", ScheduledAt: testBaseTime.Add(-2 * time.Hour)},
		}},
	}

	t.Run("attribution follows the leaf's chain, not the target's whole wait chain", func(t *testing.T) {
		detections := publisher.Evaluate(rootcause.TargetEvaluation{
			Target: target, HistoricalByJob: historical, ReferenceTime: referenceTime,
			Finding: finding,
		})

		runtime := detectionsByReason(detections, scheduler.TierRuntimeDelay)
		assert.Len(t, runtime, 1, "only the leaf's induced delay is attributed")
		assert.Equal(t, "job_a2", runtime[0].IncidentKey.JobName.String())
		// a2's overrun: 1h15m runtime minus its 30m baseline = 45m
		assert.Equal(t, int((45 * time.Minute / time.Second)), runtime[0].Reason.InducedDelaySeconds)
		// b1's sensor wait sits between the leaf and the target, so it is impact
		// propagation, not a cause; a stuck sensor would itself be the leaf
		assert.Empty(t, detectionsByReason(detections, scheduler.TierThirdPartyDelay))
	})

	t.Run("a sensor-stuck leaf carries its own wait and the history behind it", func(t *testing.T) {
		// b1 waiting on its own third-party sensor is the leaf; its finished history
		// (a2, over baseline) still belongs to the chain
		b1Finding := &rootcause.BreachFinding{
			Leaves: []*scheduler.JobState{{
				JobName: "job_b1",
				JobRun:  scheduler.JobRunSummary{JobName: "job_b1", ScheduledAt: testBaseTime.Add(-time.Hour)},
			}},
		}
		detections := publisher.Evaluate(rootcause.TargetEvaluation{
			Target: target, HistoricalByJob: historical, ReferenceTime: referenceTime,
			Finding: b1Finding,
		})

		thirdParty := detectionsByReason(detections, scheduler.TierThirdPartyDelay)
		assert.Len(t, thirdParty, 1)
		assert.Equal(t, "job_b1", thirdParty[0].IncidentKey.JobName.String())
		runtime := detectionsByReason(detections, scheduler.TierRuntimeDelay)
		assert.Len(t, runtime, 1)
		assert.Equal(t, "job_a2", runtime[0].IncidentKey.JobName.String())
	})

	t.Run("an unknown leaf falls back to the target's wait chain", func(t *testing.T) {
		finding := &rootcause.BreachFinding{
			Leaves: []*scheduler.JobState{{
				JobName: "job_ghost",
				JobRun:  scheduler.JobRunSummary{JobName: "job_ghost"},
			}},
		}
		detections := publisher.Evaluate(rootcause.TargetEvaluation{
			Target: target, HistoricalByJob: historical, ReferenceTime: referenceTime,
			Finding: finding,
		})

		// the full wait chain includes c1, whose runtime is at its baseline, and a2
		runtime := detectionsByReason(detections, scheduler.TierRuntimeDelay)
		assert.Len(t, runtime, 1)
		assert.Equal(t, "job_a2", runtime[0].IncidentKey.JobName.String())
		assert.NotEmpty(t, detectionsByReason(detections, scheduler.TierThirdPartyDelay))
	})
}
