package rootcause_test

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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

func durPtr(d time.Duration) *time.Duration { return &d }

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
		RunsByDownstream: map[scheduler.JobRunKey]*scheduler.JobRunSummary{
			scheduler.NewJobRunKey(jobName, run.ScheduledAt): run,
		},
	}
	for _, upstream := range upstreams {
		upstreamRun := upstream.TargetRun()
		upstream.AddRun(scheduler.NewJobRunKey(jobName, upstreamRun.ScheduledAt), upstreamRun)
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

// permissiveFinding builds a gate-passing BreachFinding whose only breach origin is the named job.
// The reason detectors operate on the walk, not on the finding, so tier tests only need
// the gate satisfied; gate-specific tests build their own findings.
func permissiveFinding(target *scheduler.JobLineageSummary) *rootcause.BreachFinding {
	return &rootcause.BreachFinding{
		Origins: []*scheduler.JobState{{
			JobName: target.JobName,
			JobRun:  scheduler.JobRunSummary{JobName: target.JobName},
		}},
	}
}

func eval(target *scheduler.JobLineageSummary, historical map[scheduler.JobName]time.Duration, referenceTime time.Time) []scheduler.RootCauseDetection {
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
	targetBaseline := 2*time.Hour + time.Hour

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

		historical := map[scheduler.JobName]time.Duration{
			"job_upstream": 10 * time.Minute,
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

		// the evidence shows exactly how the 35m was calculated:
		// 45m observed (start 09:00, actual end 09:45) minus the 10m baseline
		evidence := matched[0].Reason.Evidence
		assert.NotNil(t, evidence)
		assert.Equal(t, scheduler.EvidenceRuleRuntimeJobOverThreshold, evidence.Rule)
		assert.Equal(t, referenceTime.UTC(), evidence.ReferenceTime)
		assert.Equal(t, int((30 * time.Minute).Seconds()), evidence.ThresholdSeconds)
		runtime := evidence.Runtime
		assert.NotNil(t, runtime)
		assert.Len(t, runtime.CalculationComponents, 1)
		component := runtime.CalculationComponents[0]
		assert.Equal(t, "p_root", component.Project)
		assert.Equal(t, "job_upstream", component.Job)
		assert.Equal(t, int(35*time.Minute/time.Second), component.InducedSeconds)
		assert.Equal(t, testBaseTime.Add(-time.Hour).UTC(), *component.TaskStart)
		assert.Equal(t, testBaseTime.Add(-15*time.Minute).UTC(), *component.End)
		assert.Equal(t, scheduler.EvidenceSourceActualEnd, component.EndSource)
		assert.Equal(t, int(45*time.Minute/time.Second), *component.ObservedSeconds)
		assert.Equal(t, int(10*time.Minute/time.Second), *component.BaselineSeconds)
		assert.Nil(t, evidence.Scheduler, "a job-scoped reason fills only its own section")
	})

	t.Run("a job without a baseline is skipped instead of counting its whole runtime", func(t *testing.T) {
		// the upstream ran 45m but has no baseline entry: with nothing to compare
		// against, its whole runtime must not read as induced delay
		upstreamRun := makeRun("job_upstream", testBaseTime.Add(-time.Hour),
			ptr(testBaseTime.Add(-time.Hour)), ptr(testBaseTime.Add(-15*time.Minute)), "success")
		targetRun := makeRun("job_target", testBaseTime,
			ptr(testBaseTime.Add(time.Hour)), nil, "running")
		target := makeLineage("job_target", testTenant, targetRun,
			makeLineage("job_upstream", testTenant, upstreamRun))

		historical := map[scheduler.JobName]time.Duration{"job_target": targetBaseline}

		detections := eval(target, historical, referenceTime)
		assert.Empty(t, detectionsByReason(detections, scheduler.TierRuntimeDelay))

		// a zero baseline is unusable data and skips the job the same way
		historical["job_upstream"] = 0
		detections = eval(target, historical, referenceTime)
		assert.Empty(t, detectionsByReason(detections, scheduler.TierRuntimeDelay))

		// the same run with a real baseline is measured again
		historical["job_upstream"] = 10 * time.Minute
		detections = eval(target, historical, referenceTime)
		matched := detectionsByReason(detections, scheduler.TierRuntimeDelay)
		assert.Len(t, matched, 1)
		assert.Equal(t, "job_upstream", matched[0].IncidentKey.JobName.String())
		assert.Equal(t, int(35*time.Minute/time.Second), matched[0].Reason.InducedDelaySeconds)
	})

	t.Run("a job without a baseline is skipped instead of counting its whole runtime", func(t *testing.T) {
		// the upstream ran 45m but has no baseline entry: with nothing to compare
		// against, its whole runtime must not read as induced delay
		upstreamRun := makeRun("job_upstream", testBaseTime.Add(-time.Hour),
			ptr(testBaseTime.Add(-time.Hour)), ptr(testBaseTime.Add(-15*time.Minute)), "success")
		targetRun := makeRun("job_target", testBaseTime,
			ptr(testBaseTime.Add(time.Hour)), nil, "running")
		target := makeLineage("job_target", testTenant, targetRun,
			makeLineage("job_upstream", testTenant, upstreamRun))

		historical := map[scheduler.JobName]time.Duration{"job_target": targetBaseline}

		detections := eval(target, historical, referenceTime)
		assert.Empty(t, detectionsByReason(detections, scheduler.TierRuntimeDelay))

		// a zero baseline is unusable data and skips the job the same way
		historical["job_upstream"] = 0
		detections = eval(target, historical, referenceTime)
		assert.Empty(t, detectionsByReason(detections, scheduler.TierRuntimeDelay))

		// the same run with a real baseline is measured again
		historical["job_upstream"] = 10 * time.Minute
		detections = eval(target, historical, referenceTime)
		matched := detectionsByReason(detections, scheduler.TierRuntimeDelay)
		assert.Len(t, matched, 1)
		assert.Equal(t, "job_upstream", matched[0].IncidentKey.JobName.String())
		assert.Equal(t, int(35*time.Minute/time.Second), matched[0].Reason.InducedDelaySeconds)
	})

	t.Run("a job without a baseline is skipped instead of counting its whole runtime", func(t *testing.T) {
		// the upstream ran 45m but has no baseline entry: with nothing to compare
		// against, its whole runtime must not read as induced delay
		upstreamRun := makeRun("job_upstream", testBaseTime.Add(-time.Hour),
			ptr(testBaseTime.Add(-time.Hour)), ptr(testBaseTime.Add(-15*time.Minute)), "success")
		targetRun := makeRun("job_target", testBaseTime,
			ptr(testBaseTime.Add(time.Hour)), nil, "running")
		target := makeLineage("job_target", testTenant, targetRun,
			makeLineage("job_upstream", testTenant, upstreamRun))

		historical := map[scheduler.JobName]time.Duration{"job_target": targetBaseline}

		detections := eval(target, historical, referenceTime)
		assert.Empty(t, detectionsByReason(detections, scheduler.TierRuntimeDelay))

		// a zero baseline is unusable data and skips the job the same way
		historical["job_upstream"] = 0
		detections = eval(target, historical, referenceTime)
		assert.Empty(t, detectionsByReason(detections, scheduler.TierRuntimeDelay))

		// the same run with a real baseline is measured again
		historical["job_upstream"] = 10 * time.Minute
		detections = eval(target, historical, referenceTime)
		matched := detectionsByReason(detections, scheduler.TierRuntimeDelay)
		assert.Len(t, matched, 1)
		assert.Equal(t, "job_upstream", matched[0].IncidentKey.JobName.String())
		assert.Equal(t, int(35*time.Minute/time.Second), matched[0].Reason.InducedDelaySeconds)
	})

	t.Run("tier 1 conditions B and C: project flat sum fires for the max-delayed project only", func(t *testing.T) {
		// chain target <- a1(p_a) <- a2(p_a) <- b1(p_b); no job except b1 crosses 30m alone,
		// p_a flat-sums past the threshold and p_b's b1 fires individually — so p_a is the
		// only project-level detection, keyed on its max-delay job a2 with the project total.
		// Baselines of 1m each: b1 induces 44m, a2 24m, a1 19m (p_a sums to 43m).
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

		historical := map[scheduler.JobName]time.Duration{
			"job_target": targetBaseline,
			"job_b1":     time.Minute,
			"job_a2":     time.Minute,
			"job_a1":     time.Minute,
		}

		detections := eval(target, historical, referenceTime)

		matched := detectionsByReason(detections, scheduler.TierRuntimeDelay)
		assert.Len(t, matched, 2)

		byJob := map[string]scheduler.RootCauseDetection{}
		for _, detection := range matched {
			byJob[detection.IncidentKey.JobName.String()] = detection
		}
		// condition 1: b1 is individually over the threshold
		assert.Equal(t, int(44*time.Minute/time.Second), byJob["job_b1"].Reason.InducedDelaySeconds)
		assert.Equal(t, scheduler.DelayScopeJob, byJob["job_b1"].Reason.DelayScope)
		// condition 2/3: p_a's 43m flat sum, keyed on its max-delay job a2
		projectDetection := byJob["job_a2"]
		assert.Equal(t, "p_a", projectDetection.IncidentKey.ProjectName.String())
		assert.Equal(t, int(43*time.Minute/time.Second), projectDetection.Reason.InducedDelaySeconds) // project total
		// the alert-facing field carries the representative job's own delay (24m), while
		// thresholds and suppression keep reading the project total
		assert.Equal(t, scheduler.DelayScopeProject, projectDetection.Reason.DelayScope)
		assert.Equal(t, int(24*time.Minute/time.Second), projectDetection.Reason.MaxNodeInducedDelaySeconds)

		// the evidence decomposes the 43m project total into its two contributing jobs
		projectEvidence := projectDetection.Reason.Evidence
		assert.NotNil(t, projectEvidence)
		assert.Equal(t, scheduler.EvidenceRuleRuntimeProjectSumOverThreshold, projectEvidence.Rule)
		projectRuntime := projectEvidence.Runtime
		assert.NotNil(t, projectRuntime)
		assert.Equal(t, "p_a/job_a2", projectRuntime.MaxNode)
		assert.Equal(t, 2, projectRuntime.CalculationComponentsTotal)
		assert.Len(t, projectRuntime.CalculationComponents, 2)
		// sorted largest first
		assert.Equal(t, "job_a2", projectRuntime.CalculationComponents[0].Job)
		assert.Equal(t, int(24*time.Minute/time.Second), projectRuntime.CalculationComponents[0].InducedSeconds)
		assert.Equal(t, "job_a1", projectRuntime.CalculationComponents[1].Job)
		assert.Equal(t, int(19*time.Minute/time.Second), projectRuntime.CalculationComponents[1].InducedSeconds)
		assert.Empty(t, projectRuntime.SkippedNoBaseline)
	})

	t.Run("project evidence lists the jobs skipped for having no baseline", func(t *testing.T) {
		// chain target <- a1(p_a) <- a2(p_a, no baseline) <- a3(p_a); a1 and a3 induce
		// 20m each (sum 40m, over the threshold), while a2 ran 5h with no baseline and
		// must be skipped, not counted at full length
		a1Run := makeRun("job_a1", testBaseTime.Add(-time.Hour),
			ptr(testBaseTime.Add(-time.Hour)), ptr(testBaseTime.Add(-30*time.Minute)), "success")
		a2Run := makeRun("job_a2", testBaseTime.Add(-2*time.Hour),
			ptr(testBaseTime.Add(-2*time.Hour)), nil, "running")
		a3Run := makeRun("job_a3", testBaseTime.Add(-time.Hour),
			ptr(testBaseTime.Add(-time.Hour)), ptr(testBaseTime.Add(-30*time.Minute)), "success")
		targetRun := makeRun("job_target", testBaseTime,
			ptr(testBaseTime.Add(time.Hour)), nil, "running")
		target := makeLineage("job_target", testTenant, targetRun,
			makeLineage("job_a1", tenantProjA, a1Run,
				makeLineage("job_a2", tenantProjA, a2Run,
					makeLineage("job_a3", tenantProjA, a3Run))))

		historical := map[scheduler.JobName]time.Duration{
			"job_target": targetBaseline,
			"job_a1":     10 * time.Minute,
			"job_a3":     10 * time.Minute,
		}

		detections := eval(target, historical, referenceTime)

		matched := detectionsByReason(detections, scheduler.TierRuntimeDelay)
		assert.Len(t, matched, 1, "no job crosses the threshold alone; the project sum does")
		assert.Equal(t, int(40*time.Minute/time.Second), matched[0].Reason.InducedDelaySeconds) // project total
		evidence := matched[0].Reason.Evidence
		assert.NotNil(t, evidence)
		assert.Equal(t, 2, evidence.Runtime.CalculationComponentsTotal)
		assert.Len(t, evidence.Runtime.CalculationComponents, 2)
		assert.Equal(t, []string{"p_a/job_a2"}, evidence.Runtime.SkippedNoBaseline,
			"the no-baseline job is reported as skipped, not counted at full length")
	})

	t.Run("evidence carries the breach origins and the origin-to-target chain", func(t *testing.T) {
		// chain target <- a1(p_a) <- a2(p_a) <- b1(p_b), with a synthetic finding whose
		// sole inferred-SLA breach origin is b1 at level 3
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

		originState := &scheduler.JobState{
			JobName: "job_b1",
			Tenant:  tenantProjB,
			JobSLAState: scheduler.JobSLAState{
				InferredSLA:       ptr(testBaseTime.Add(30 * time.Minute)),
				EstimatedDuration: durPtr(time.Hour),
			},
			Status: scheduler.SLABreachCauseRunningLate,
			JobRun: scheduler.JobRunSummary{JobName: "job_b1", ScheduledAt: testBaseTime.Add(-3 * time.Hour)},
		}
		finding := &rootcause.BreachFinding{
			Origins: []*scheduler.JobState{originState},
			OriginPaths: [][]*scheduler.JobState{{
				{JobName: "job_target", Tenant: testTenant, JobSLAState: scheduler.JobSLAState{InferredSLA: ptr(testBaseTime.Add(2 * time.Hour))}},
				{JobName: "job_a1", Tenant: tenantProjA, JobSLAState: scheduler.JobSLAState{InferredSLA: ptr(testBaseTime.Add(90 * time.Minute))}},
				{JobName: "job_a2", Tenant: tenantProjA, JobSLAState: scheduler.JobSLAState{InferredSLA: ptr(testBaseTime.Add(time.Hour))}},
				originState,
			}},
			Bottleneck: rootcause.BottleneckPath{
				Pred:  map[scheduler.JobName]scheduler.JobName{"job_a1": "job_target", "job_a2": "job_a1", "job_b1": "job_a2"},
				Level: map[scheduler.JobName]int{"job_target": 0, "job_a1": 1, "job_a2": 2, "job_b1": 3},
			},
		}

		historical := map[scheduler.JobName]time.Duration{
			"job_target": targetBaseline,
			"job_b1":     time.Minute,
			"job_a2":     time.Minute,
			"job_a1":     time.Minute,
		}

		detections := testPublisher().Evaluate(rootcause.TargetEvaluation{
			Target: target, HistoricalByJob: historical, ReferenceTime: referenceTime,
			Finding: finding,
		})

		for _, detection := range detectionsByReason(detections, scheduler.TierRuntimeDelay) {
			origins := detection.Reason.Evidence.BreachSources
			assert.Len(t, origins, 1)
			origin := origins[0]
			assert.Equal(t, "p_b", origin.Project)
			assert.Equal(t, "job_b1", origin.Job)
			assert.Equal(t, 3, origin.Level, "three hops from the target")
			assert.Equal(t, scheduler.SLABreachCauseRunningLate, scheduler.SLABreachCause(origin.Status))
			assert.Equal(t, testBaseTime.Add(30*time.Minute).UTC(), *origin.InferredSLA,
				"the inferred deadline is when the origin was detected as breaching")
			assert.Equal(t, testBaseTime.Add(-3*time.Hour).UTC(), *origin.ScheduledAt)
			assert.Equal(t, int(time.Hour/time.Second), *origin.EstimatedDurationSeconds)

			// the chain traces origin → target with descending levels
			assert.Len(t, origin.Chain, 4)
			assert.Equal(t, "job_b1", origin.Chain[0].Job)
			assert.Equal(t, 3, origin.Chain[0].Level)
			assert.Equal(t, "job_a2", origin.Chain[1].Job)
			assert.Equal(t, 2, origin.Chain[1].Level)
			assert.Equal(t, testBaseTime.Add(time.Hour).UTC(), *origin.Chain[1].InferredSLA)
			assert.Equal(t, "job_a1", origin.Chain[2].Job)
			assert.Equal(t, 1, origin.Chain[2].Level)
			assert.Equal(t, "job_target", origin.Chain[3].Job)
			assert.Equal(t, 0, origin.Chain[3].Level)
			assert.Equal(t, testBaseTime.Add(2*time.Hour).UTC(), *origin.Chain[3].InferredSLA)

			// every calculation component carries its target-relative level and
			// whether its job is itself a breach source
			for _, component := range detection.Reason.Evidence.Runtime.CalculationComponents {
				assert.Equal(t, finding.Bottleneck.Level[scheduler.JobName(component.Job)], component.Level)
				assert.Equal(t, component.Job == "job_b1", component.IsBreachSource,
					"only the origin job is marked as a breach source")
			}
		}
	})

	t.Run("failed upstream evidence carries the origins without a rule", func(t *testing.T) {
		// a2 failed; the synthetic finding names it as the breach origin with no
		// reconstructable path
		a2Run := makeRun("job_a2", testBaseTime.Add(-2*time.Hour),
			ptr(testBaseTime.Add(-2*time.Hour)), ptr(testBaseTime.Add(-110*time.Minute)), "failed")
		b1Run := makeRun("job_b1", testBaseTime.Add(-time.Hour),
			ptr(testBaseTime.Add(-90*time.Minute)), ptr(testBaseTime.Add(-80*time.Minute)), "success")
		targetRun := makeRun("job_target", testBaseTime,
			ptr(testBaseTime), nil, "running")
		target := makeLineage("job_target", testTenant, targetRun,
			makeLineage("job_b1", tenantProjB, b1Run,
				makeLineage("job_a2", tenantProjA, a2Run)))

		finding := &rootcause.BreachFinding{
			Origins: []*scheduler.JobState{{
				JobName: "job_a2",
				Tenant:  tenantProjA,
				Status:  scheduler.SLABreachCauseRunningLate,
				JobRun:  scheduler.JobRunSummary{JobName: "job_a2", ScheduledAt: testBaseTime.Add(-2 * time.Hour)},
			}},
		}

		detections := testPublisher().Evaluate(rootcause.TargetEvaluation{
			Target:          target,
			HistoricalByJob: map[scheduler.JobName]time.Duration{"job_target": 90 * time.Minute},
			ReferenceTime:   referenceTime,
			Finding:         finding,
		})

		failed := detectionsByReason(detections, scheduler.TierFailedUpstream)
		assert.Len(t, failed, 1)
		evidence := failed[0].Reason.Evidence
		assert.NotNil(t, evidence)
		assert.Empty(t, evidence.Rule, "the failed-upstream evidence carries no rule")
		assert.Len(t, evidence.BreachSources, 1)
		assert.Equal(t, "job_a2", evidence.BreachSources[0].Job)
		assert.Empty(t, evidence.BreachSources[0].Chain, "no reconstructable path, no chain")

		// the JSON omits the rule key entirely
		raw, err := json.Marshal(evidence)
		require.NoError(t, err)
		assert.NotContains(t, string(raw), `"rule"`)
	})

	t.Run("the origin chain is capped keeping the origin side", func(t *testing.T) {
		// the finding's path holds 25 nodes; the stored chain keeps the 20 closest to
		// the origin (the deepest hops)
		upstreamRun := makeRun("job_upstream", testBaseTime.Add(-time.Hour),
			ptr(testBaseTime.Add(-time.Hour)), ptr(testBaseTime.Add(-15*time.Minute)), "success")
		targetRun := makeRun("job_target", testBaseTime,
			ptr(testBaseTime.Add(time.Hour)), nil, "running")
		target := makeLineage("job_target", testTenant, targetRun,
			makeLineage("job_upstream", testTenant, upstreamRun))

		path := []*scheduler.JobState{{
			JobName: "job_target", Tenant: testTenant,
			JobSLAState: scheduler.JobSLAState{InferredSLA: ptr(testBaseTime.Add(2 * time.Hour))},
		}}
		levels := map[scheduler.JobName]int{"job_target": 0}
		for i := 1; i <= 23; i++ {
			name := scheduler.JobName(fmt.Sprintf("job_d%02d", i))
			path = append(path, &scheduler.JobState{
				JobName: name, Tenant: tenantProjA,
				JobSLAState: scheduler.JobSLAState{InferredSLA: ptr(testBaseTime.Add(time.Duration(24-i) * time.Hour))},
			})
			levels[name] = i
		}
		path = append(path, &scheduler.JobState{
			JobName: "job_upstream", Tenant: testTenant,
			JobSLAState: scheduler.JobSLAState{InferredSLA: ptr(testBaseTime.Add(time.Hour))},
		})
		levels["job_upstream"] = 24

		finding := &rootcause.BreachFinding{
			Origins: []*scheduler.JobState{{
				JobName: "job_upstream", Tenant: testTenant,
				Status: scheduler.SLABreachCauseRunningLate,
				JobRun: scheduler.JobRunSummary{JobName: "job_upstream", ScheduledAt: testBaseTime.Add(-time.Hour)},
			}},
			OriginPaths: [][]*scheduler.JobState{path},
			Bottleneck:  rootcause.BottleneckPath{Level: levels},
		}

		detections := testPublisher().Evaluate(rootcause.TargetEvaluation{
			Target:          target,
			HistoricalByJob: map[scheduler.JobName]time.Duration{"job_target": targetBaseline, "job_upstream": 10 * time.Minute},
			ReferenceTime:   referenceTime,
			Finding:         finding,
		})

		matched := detectionsByReason(detections, scheduler.TierRuntimeDelay)
		assert.Len(t, matched, 1)
		chain := matched[0].Reason.Evidence.BreachSources[0].Chain
		assert.Len(t, chain, scheduler.MaxEvidenceComponents)
		assert.Equal(t, "job_upstream", chain[0].Job, "the origin side is kept")
		assert.Equal(t, 24, chain[0].Level)
		assert.Equal(t, "job_d05", chain[len(chain)-1].Job)
		assert.Equal(t, 5, chain[len(chain)-1].Level)
	})

	t.Run("project evidence caps its components at twenty", func(t *testing.T) {
		// 25 upstreams of one project each induce 5m: no individual detection, the 125m
		// flat sum fires once, and the evidence keeps the 20 biggest contributors
		targetRun := makeRun("job_target", testBaseTime,
			ptr(testBaseTime.Add(time.Hour)), nil, "running")
		node := makeLineage("job_target", testTenant, targetRun)
		historical := map[scheduler.JobName]time.Duration{"job_target": targetBaseline}
		for i := 1; i <= 25; i++ {
			upstreamName := scheduler.JobName(fmt.Sprintf("job_u%02d", i))
			upstreamRun := makeRun(upstreamName, testBaseTime.Add(-2*time.Hour),
				ptr(testBaseTime.Add(-2*time.Hour)), ptr(testBaseTime.Add(-2*time.Hour+35*time.Minute)), "success")
			node = makeLineage(upstreamName, tenantProjA, upstreamRun, node)
			historical[upstreamName] = 30 * time.Minute
		}

		detections := eval(node, historical, referenceTime)

		matched := detectionsByReason(detections, scheduler.TierRuntimeDelay)
		assert.Len(t, matched, 1)
		assert.Equal(t, scheduler.DelayScopeProject, matched[0].Reason.DelayScope)
		evidence := matched[0].Reason.Evidence
		assert.NotNil(t, evidence)
		assert.Equal(t, 25, evidence.Runtime.CalculationComponentsTotal)
		assert.Len(t, evidence.Runtime.CalculationComponents, scheduler.MaxEvidenceComponents)
		// deterministic order: largest first, ties by job name
		assert.Equal(t, "job_u01", evidence.Runtime.CalculationComponents[0].Job)
		assert.Equal(t, "job_u20", evidence.Runtime.CalculationComponents[len(evidence.Runtime.CalculationComponents)-1].Job)
		for _, component := range evidence.Runtime.CalculationComponents {
			assert.Equal(t, int(5*time.Minute/time.Second), component.InducedSeconds)
		}
	})

	t.Run("scheduler delay is the critical path's summed wait against the depth budget", func(t *testing.T) {
		// a2 finished at 09:15; b1 was scheduled 09:00 but only started 10:40 (85m the
		// scheduler owns); the target itself started 15m after b1's finish. The summed
		// wait is 100m against a 3-level budget of 7.5m, keyed on the target's run.
		a2Run := makeRun("job_a2", testBaseTime.Add(-2*time.Hour),
			ptr(testBaseTime.Add(-2*time.Hour)), ptr(testBaseTime.Add(-45*time.Minute)), "success")
		b1Run := makeRun("job_b1", testBaseTime.Add(-time.Hour),
			ptr(testBaseTime.Add(40*time.Minute)), ptr(testBaseTime.Add(45*time.Minute)), "success")
		targetRun := makeRun("job_target", testBaseTime,
			ptr(testBaseTime.Add(time.Hour)), nil, "running")
		target := makeLineage("job_target", testTenant, targetRun,
			makeLineage("job_b1", tenantProjB, b1Run,
				makeLineage("job_a2", tenantProjA, a2Run)))

		historical := map[scheduler.JobName]time.Duration{
			"job_target": 90 * time.Minute,
			"job_b1":     5 * time.Minute,
			"job_a2":     75 * time.Minute,
		}

		detections := eval(target, historical, referenceTime)

		matched := detectionsByReason(detections, scheduler.TierSchedulerDelay)
		assert.Len(t, matched, 1)
		assert.Equal(t, "job_target", matched[0].IncidentKey.JobName.String())
		// (11:00 - 10:45) + (10:40 - 09:15) = 15m + 85m = 100 minutes
		assert.Equal(t, int(100*time.Minute/time.Second), matched[0].Reason.InducedDelaySeconds)
		assert.False(t, matched[0].Reason.IsJobFinished, "the target is still running")

		// the evidence decomposes the 100m into per-node waits against the path allowance
		evidence := matched[0].Reason.Evidence
		assert.NotNil(t, evidence)
		assert.Equal(t, scheduler.EvidenceRuleSchedulerPathOverBudget, evidence.Rule)
		assert.Equal(t, 450, evidence.ThresholdSeconds, "2.5 minutes per depth, three measured nodes")
		schedulerSection := evidence.Scheduler
		assert.NotNil(t, schedulerSection)
		assert.Equal(t, 3, schedulerSection.CalculationComponentsTotal)
		// sorted by wait, largest first
		assert.Equal(t, "job_b1", schedulerSection.CalculationComponents[0].Job)
		assert.Equal(t, int(85*time.Minute/time.Second), schedulerSection.CalculationComponents[0].InducedSeconds)
		assert.Equal(t, scheduler.EvidenceSourceUpstreamEnd, schedulerSection.CalculationComponents[0].WaitFromSource)
		assert.Equal(t, "job_a2", schedulerSection.CalculationComponents[0].UpstreamJob)
		assert.Equal(t, testBaseTime.Add(-45*time.Minute).UTC(), *schedulerSection.CalculationComponents[0].UpstreamEnd)
		assert.Equal(t, testBaseTime.Add(40*time.Minute).UTC(), *schedulerSection.CalculationComponents[0].TaskStart)
		assert.Equal(t, "job_target", schedulerSection.CalculationComponents[1].Job)
		assert.Equal(t, int(15*time.Minute/time.Second), schedulerSection.CalculationComponents[1].InducedSeconds)
		assert.Equal(t, "job_b1", schedulerSection.CalculationComponents[1].UpstreamJob)
		// a2 has no finished upstream, so its wait is zero and carries no upstream fields
		assert.Equal(t, "job_a2", schedulerSection.CalculationComponents[2].Job)
		assert.Equal(t, 0, schedulerSection.CalculationComponents[2].InducedSeconds)
		assert.Empty(t, schedulerSection.CalculationComponents[2].UpstreamJob)
		assert.Nil(t, schedulerSection.CalculationComponents[2].UpstreamEnd)
	})

	t.Run("no scheduler charge behind a failed upstream; tier 4 fires instead", func(t *testing.T) {
		// a2 failed at 08:10; b1 started anyway at 08:30 — a wait the formula does not
		// charge to the scheduler (the upstream's end precedes the wait). The target
		// starts on its own schedule, so nothing on the path is scheduler-attributable;
		// the failed upstream is the reported cause.
		a2Run := makeRun("job_a2", testBaseTime.Add(-2*time.Hour),
			ptr(testBaseTime.Add(-2*time.Hour)), ptr(testBaseTime.Add(-110*time.Minute)), "failed")
		b1Run := makeRun("job_b1", testBaseTime.Add(-time.Hour),
			ptr(testBaseTime.Add(-90*time.Minute)), ptr(testBaseTime.Add(-80*time.Minute)), "success")
		targetRun := makeRun("job_target", testBaseTime,
			ptr(testBaseTime), nil, "running")
		target := makeLineage("job_target", testTenant, targetRun,
			makeLineage("job_b1", tenantProjB, b1Run,
				makeLineage("job_a2", tenantProjA, a2Run)))

		historical := map[scheduler.JobName]time.Duration{
			"job_target": 90 * time.Minute,
			"job_b1":     10 * time.Minute,
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

		// the evidence shows the boundary, the wait window and which end was used
		evidence := detections[0].Reason.Evidence
		assert.NotNil(t, evidence)
		assert.Equal(t, scheduler.EvidenceRuleThirdPartyWaitOverThreshold, evidence.Rule)
		thirdParty := evidence.ThirdParty
		assert.NotNil(t, thirdParty)
		assert.Equal(t, time.Date(2026, 10, 25, 18, 0, 0, 0, time.UTC), thirdParty.Boundary)
		assert.Equal(t, "wait_dex_event_delivery", thirdParty.Sensor)
		assert.Equal(t, time.Date(2026, 10, 25, 20, 0, 0, 0, time.UTC), thirdParty.WaitFrom)
		assert.Equal(t, scheduler.EvidenceSourceSensorWaitStart, thirdParty.WaitFromSource)
		assert.Equal(t, refTime, thirdParty.End)
		assert.Equal(t, scheduler.EvidenceSourceReferenceTime, thirdParty.EndSource)
	})

	t.Run("tier 5: stuck node with successful upstreams becomes the incident", func(t *testing.T) {
		// a2 succeeded; b1 has no run row at all (missing run) but is past its latest safe
		// start — the inferred-SLA NOT_STARTED condition
		a2Run := makeRun("job_a2", testBaseTime.Add(-2*time.Hour),
			ptr(testBaseTime.Add(-2*time.Hour)), ptr(testBaseTime.Add(-90*time.Minute)), "success")
		b1Run := makeRun("job_b1", testBaseTime.Add(-time.Hour), nil, nil, "")
		targetRun := makeRun("job_target", testBaseTime,
			ptr(testBaseTime.Add(time.Hour)), nil, "running")
		target := makeLineage("job_target", testTenant, targetRun,
			makeLineage("job_b1", tenantProjB, b1Run,
				makeLineage("job_a2", tenantProjA, a2Run)))

		historical := map[scheduler.JobName]time.Duration{
			"job_target": 90 * time.Minute,
		}

		finding := permissiveFinding(target)
		finding.BreachStates = map[scheduler.JobName]*scheduler.JobState{
			"job_b1": {
				JobName: "job_b1",
				JobSLAState: scheduler.JobSLAState{
					InferredSLA:       ptr(testBaseTime.Add(2 * time.Hour)),
					EstimatedDuration: durPtr(30 * time.Minute),
				},
				Status: scheduler.SLABreachCauseNotStarted,
				JobRun: scheduler.JobRunSummary{JobName: "job_b1", ScheduledAt: testBaseTime.Add(-time.Hour)},
			},
		}
		detections := testPublisher().Evaluate(rootcause.TargetEvaluation{
			Target: target, HistoricalByJob: historical, ReferenceTime: referenceTime,
			Finding: finding,
		})

		matched := detectionsByReason(detections, scheduler.TierWaitingUpstream)
		assert.Len(t, matched, 1)
		assert.Equal(t, "job_b1", matched[0].IncidentKey.JobName.String())
		// the overdue time doubles as the induced delay: safe start 11:30 (inferred SLA
		// 12:00 minus its 30m estimate), reference time 90m past it
		assert.Equal(t, int(90*time.Minute/time.Second), matched[0].Reason.InducedDelaySeconds)
		// the evidence records why the node counts as stuck: its safe start was 11:30
		// (inferred SLA 12:00 minus its 30m estimate) and the reference time is 90m past it
		waiting := matched[0].Reason.Evidence.WaitingUpstream
		assert.NotNil(t, waiting)
		assert.Equal(t, scheduler.EvidenceRuleWaitingUpstreamPastSafeStart, matched[0].Reason.Evidence.Rule)
		assert.Equal(t, string(scheduler.StateNotScheduled), waiting.NodeState)
		assert.Equal(t, testBaseTime.Add(2*time.Hour).UTC(), *waiting.InferredSLA)
		assert.Equal(t, int(30*time.Minute/time.Second), *waiting.EstimatedDurationSeconds)
		assert.Equal(t, testBaseTime.Add(90*time.Minute).UTC(), *waiting.LatestSafeStart)
		assert.Empty(t, waiting.MissingUpstreamRuns, "the upstream succeeded; the node waits on nothing missing")
	})

	t.Run("tier 5 names the upstream runs that were never created", func(t *testing.T) {
		// b1 is stuck waiting on its upstream, whose expected run was never created
		// (not_scheduled): the evidence names which upstream run does not exist
		a2Run := makeRun("job_a2", testBaseTime.Add(-2*time.Hour), nil, nil, "")
		b1Run := makeRun("job_b1", testBaseTime.Add(-time.Hour), nil, nil, "queued")
		b1Run.WaitStartTime = ptr(testBaseTime.Add(-time.Hour))
		targetRun := makeRun("job_target", testBaseTime,
			ptr(testBaseTime.Add(time.Hour)), nil, "running")
		target := makeLineage("job_target", testTenant, targetRun,
			makeLineage("job_b1", tenantProjB, b1Run,
				makeLineage("job_a2", tenantProjA, a2Run)))

		historical := map[scheduler.JobName]time.Duration{
			"job_target": 90 * time.Minute,
		}

		finding := permissiveFinding(target)
		finding.BreachStates = map[scheduler.JobName]*scheduler.JobState{
			"job_b1": {
				JobName: "job_b1",
				JobSLAState: scheduler.JobSLAState{
					InferredSLA:       ptr(testBaseTime.Add(2 * time.Hour)),
					EstimatedDuration: durPtr(30 * time.Minute),
				},
				Status: scheduler.SLABreachCauseNotStarted,
				JobRun: scheduler.JobRunSummary{JobName: "job_b1", ScheduledAt: testBaseTime.Add(-time.Hour)},
			},
		}
		detections := testPublisher().Evaluate(rootcause.TargetEvaluation{
			Target: target, HistoricalByJob: historical, ReferenceTime: referenceTime,
			Finding: finding,
		})

		matched := detectionsByReason(detections, scheduler.TierWaitingUpstream)
		assert.Len(t, matched, 1)
		waiting := matched[0].Reason.Evidence.WaitingUpstream
		assert.NotNil(t, waiting)
		assert.Equal(t, string(scheduler.StateWaitUpstream), waiting.NodeState)
		assert.Equal(t, []string{"p_a/job_a2"}, waiting.MissingUpstreamRuns,
			"the never-created upstream run is named so the reader knows what to create")
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

		historical := map[scheduler.JobName]time.Duration{
			"job_target": 90 * time.Minute,
			"job_a2":     30 * time.Minute,
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

		historical := map[scheduler.JobName]time.Duration{
			"job_target": 4 * time.Hour,
		}

		detections := eval(target, historical, referenceTime)

		assert.Len(t, detections, 1)
		assert.Equal(t, scheduler.TierAggressiveSLA, detections[0].Reason.Reason)
		assert.Equal(t, "job_target", detections[0].IncidentKey.JobName.String())
		assert.Equal(t, "p_root", detections[0].IncidentKey.ProjectName.String())

		// the evidence records the SLA, the end used (the reference time, the run is still
		// going) and the target's own baseline for comparison
		evidence := detections[0].Reason.Evidence
		assert.NotNil(t, evidence)
		assert.Equal(t, scheduler.EvidenceRuleAggressiveSLANoCause, evidence.Rule)
		aggressive := evidence.AggressiveSLA
		assert.Equal(t, testBaseTime.Add(2*time.Hour).UTC(), aggressive.TargetSLATime)
		assert.Equal(t, referenceTime.UTC(), *aggressive.End)
		assert.Equal(t, scheduler.EvidenceSourceReferenceTime, aggressive.EndSource)
		assert.Equal(t, int(time.Hour/time.Second), aggressive.LateBySeconds)
		assert.Equal(t, testBaseTime.UTC(), *aggressive.TaskStart)
		assert.Equal(t, int(4*time.Hour/time.Second), *aggressive.BaselineSeconds)
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

		historical := map[scheduler.JobName]time.Duration{
			"job_target": 90 * time.Minute,
			"job_a2":     30 * time.Minute,
		}

		detections := eval(target, historical, referenceTime)

		assert.NotEmpty(t, detectionsByReason(detections, scheduler.TierThirdPartyDelay))
		runtime := detectionsByReason(detections, scheduler.TierRuntimeDelay)
		assert.Len(t, runtime, 1)
		assert.Equal(t, "job_a2", runtime[0].IncidentKey.JobName.String())
	})
}

// TestPublisher_InferredBreachGate covers the potential-first contract: the caller gates
// on the Finding, and an unexplained breach is AGGRESSIVE_SLA only when the target is its
// own origin and past its SLA.
func TestPublisher_InferredBreachGate(t *testing.T) {
	publisher := testPublisher()
	// reference is past the target's SLA (10:00 + 2h); the target started on its own
	// schedule so its wait charges nothing to the scheduler
	referenceTime := testBaseTime.Add(3 * time.Hour)
	targetRun := makeRun("job_target", testBaseTime,
		ptr(testBaseTime), nil, "running")
	target := makeLineage("job_target", testTenant, targetRun)
	historical := map[scheduler.JobName]time.Duration{
		"job_target": 4 * time.Hour,
	}

	t.Run("a target past its SLA with itself as the only origin is AGGRESSIVE_SLA", func(t *testing.T) {
		detections := eval(target, historical, referenceTime)
		if assert.Len(t, detections, 1) {
			assert.Equal(t, scheduler.TierAggressiveSLA, detections[0].Reason.Reason)
		}
	})

	t.Run("a target within its SLA with an upstream breach still detects reasons", func(t *testing.T) {
		// the target is still within its SLA deadline (reference before 12:00)
		withinSLA := testBaseTime.Add(time.Hour)
		detections := eval(target, historical, withinSLA)
		// no reason applies here (the target started on time and is within baseline), so
		// a predicted miss with no cause stays unrecorded
		assert.Empty(t, detections)

		// with an upstream slow enough to detect, the reason fires while the target is
		// still within its SLA — the potential-first early warning
		upstreamRun := makeRun("job_upstream", testBaseTime.Add(-time.Hour),
			ptr(testBaseTime.Add(-time.Hour)), ptr(testBaseTime.Add(-15*time.Minute)), "success")
		upstream := makeLineage("job_upstream", testTenant, upstreamRun)
		targetWithUpstream := makeLineage("job_target", testTenant, targetRun, upstream)
		historical["job_upstream"] = 10 * time.Minute
		detections = eval(targetWithUpstream, historical, withinSLA)
		if assert.Len(t, detections, 1) {
			assert.Equal(t, scheduler.TierRuntimeDelay, detections[0].Reason.Reason)
		}
	})

	t.Run("a breach origin other than the target never becomes AGGRESSIVE_SLA", func(t *testing.T) {
		finding := &rootcause.BreachFinding{
			Origins: []*scheduler.JobState{{
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

// TestPublisher_WaitingUpstreamSafeStart pins the stuck-state condition to the
// inferred-SLA NOT_STARTED analysis: within the latest safe start a stuck-looking node is
// just early, past it the node is genuinely stuck.
func TestPublisher_WaitingUpstreamSafeStart(t *testing.T) {
	publisher := testPublisher()
	referenceTime := testBaseTime.Add(3 * time.Hour)

	buildTarget := func() (*scheduler.JobLineageSummary, *scheduler.JobRunSummary) {
		a2Run := makeRun("job_a2", testBaseTime.Add(-2*time.Hour),
			ptr(testBaseTime.Add(-2*time.Hour)), ptr(testBaseTime.Add(-90*time.Minute)), "success")
		b1Run := makeRun("job_b1", testBaseTime.Add(-time.Hour), nil, nil, "")
		targetRun := makeRun("job_target", testBaseTime,
			ptr(testBaseTime.Add(time.Hour)), nil, "running")
		target := makeLineage("job_target", testTenant, targetRun,
			makeLineage("job_b1", tenantProjB, b1Run,
				makeLineage("job_a2", tenantProjA, a2Run)))
		return target, b1Run
	}
	historical := map[scheduler.JobName]time.Duration{
		"job_target": 90 * time.Minute,
	}

	t.Run("a stuck node past its latest safe start is detected", func(t *testing.T) {
		target, b1Run := buildTarget()
		finding := permissiveFinding(target)
		finding.BreachStates = map[scheduler.JobName]*scheduler.JobState{
			"job_b1": {
				JobName: "job_b1",
				Status:  scheduler.SLABreachCauseNotStarted,
				JobRun:  *b1Run,
			},
		}
		detections := publisher.Evaluate(rootcause.TargetEvaluation{
			Target: target, HistoricalByJob: historical, ReferenceTime: referenceTime,
			Finding: finding,
		})
		assert.Len(t, detectionsByReason(detections, scheduler.TierWaitingUpstream), 1)
	})

	t.Run("a stuck node still within its safe start is not detected", func(t *testing.T) {
		// same geometry, but the inferred-SLA analysis has not flagged b1 yet
		target, _ := buildTarget()
		finding := permissiveFinding(target)
		detections := publisher.Evaluate(rootcause.TargetEvaluation{
			Target: target, HistoricalByJob: historical, ReferenceTime: referenceTime,
			Finding: finding,
		})
		assert.Empty(t, detectionsByReason(detections, scheduler.TierWaitingUpstream))
	})

	t.Run("a flagged run from another lineage path does not qualify the node", func(t *testing.T) {
		// the NOT_STARTED flag belongs to a different run of job_b1 (a diamond's other
		// branch), so this walk's run is not stuck
		target, _ := buildTarget()
		finding := permissiveFinding(target)
		finding.BreachStates = map[scheduler.JobName]*scheduler.JobState{
			"job_b1": {
				JobName: "job_b1",
				Status:  scheduler.SLABreachCauseNotStarted,
				JobRun:  scheduler.JobRunSummary{JobName: "job_b1", ScheduledAt: testBaseTime.Add(-3 * time.Hour)},
			},
		}
		detections := publisher.Evaluate(rootcause.TargetEvaluation{
			Target: target, HistoricalByJob: historical, ReferenceTime: referenceTime,
			Finding: finding,
		})
		assert.Empty(t, detectionsByReason(detections, scheduler.TierWaitingUpstream))
	})
}

// TestPublisher_CauseChain anchors RUNTIME_DELAY and THIRD_PARTY_DELAY attribution on the
// inferred-breach leaves instead of the target's whole wait chain.
func TestPublisher_CauseChain(t *testing.T) {
	publisher := testPublisher()
	referenceTime := testBaseTime.Add(3 * time.Hour) // past the target's SLA

	// chain: target <- stuck origin-side frontier (b1 waiting on its sensor) <- a2 finished
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

	historical := map[scheduler.JobName]time.Duration{
		"job_target": 90 * time.Minute,
		"job_a2":     30 * time.Minute,
		"job_c1":     2 * time.Hour,
	}
	// the breach origin is a2, the finished job whose runtime blew past its baseline
	finding := &rootcause.BreachFinding{
		Origins: []*scheduler.JobState{{
			JobName: "job_a2",
			JobRun:  scheduler.JobRunSummary{JobName: "job_a2", ScheduledAt: testBaseTime.Add(-2 * time.Hour)},
		}},
	}

	t.Run("attribution follows the breach origin's chain, not the target's whole wait chain", func(t *testing.T) {
		detections := publisher.Evaluate(rootcause.TargetEvaluation{
			Target: target, HistoricalByJob: historical, ReferenceTime: referenceTime,
			Finding: finding,
		})

		runtime := detectionsByReason(detections, scheduler.TierRuntimeDelay)
		assert.Len(t, runtime, 1, "only the breach origin's induced delay is attributed")
		assert.Equal(t, "job_a2", runtime[0].IncidentKey.JobName.String())
		// a2's overrun: 1h15m runtime minus its 30m baseline = 45m
		assert.Equal(t, int((45 * time.Minute / time.Second)), runtime[0].Reason.InducedDelaySeconds)
		// b1's sensor wait sits between the breach origin and the target, so it is impact
		// propagation, not a cause; a stuck sensor would itself be the breach origin
		assert.Empty(t, detectionsByReason(detections, scheduler.TierThirdPartyDelay))
	})

	t.Run("a sensor-stuck breach origin carries its own wait and the history behind it", func(t *testing.T) {
		// b1 waiting on its own third-party sensor is the breach origin; its finished history
		// (a2, over baseline) still belongs to the chain
		b1Finding := &rootcause.BreachFinding{
			Origins: []*scheduler.JobState{{
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

	t.Run("a started running head is summed into the scheduler total", func(t *testing.T) {
		// b1 scheduled 09:00 but only started 10:40 (85m past its latest-finished upstream's
		// 09:15 end), still running: it hangs off the gating path's end and its delay is
		// exact, so it is summed into the total keyed on the target's run. The target
		// itself contributes nothing (its only upstream has not finished).
		startedB1 := makeRun("job_b1", testBaseTime.Add(-time.Hour),
			ptr(testBaseTime.Add(40*time.Minute)), nil, "running")
		startedTarget := makeLineage("job_target", testTenant, targetRun,
			makeLineage("job_b1", tenantProjB, startedB1,
				makeLineage("job_a2", tenantProjA, a2Run)))
		b1Finding := &rootcause.BreachFinding{
			Origins: []*scheduler.JobState{{
				JobName: "job_b1",
				JobRun:  scheduler.JobRunSummary{JobName: "job_b1", ScheduledAt: testBaseTime.Add(-time.Hour)},
			}},
		}
		detections := publisher.Evaluate(rootcause.TargetEvaluation{
			Target: startedTarget, HistoricalByJob: historical, ReferenceTime: referenceTime,
			Finding: b1Finding,
		})

		schedulerDetections := detectionsByReason(detections, scheduler.TierSchedulerDelay)
		if assert.Len(t, schedulerDetections, 1) {
			assert.Equal(t, "job_target", schedulerDetections[0].IncidentKey.JobName.String())
			assert.Equal(t, int(85*time.Minute/time.Second), schedulerDetections[0].Reason.InducedDelaySeconds)
			assert.False(t, schedulerDetections[0].Reason.IsJobFinished)
		}
	})

	t.Run("an unknown breach origin falls back to the target's wait chain", func(t *testing.T) {
		finding := &rootcause.BreachFinding{
			Origins: []*scheduler.JobState{{
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
