package rootcause

import (
	"context"
	"time"

	"github.com/goto/optimus/core/scheduler"
)

// BreachFinding is the inferred-SLA breach analysis of one target, produced before any
// reason classification. It is the shared result behind the potential-SLA API and the
// root-cause engine, so both agree on which upstream jobs are breaching.
type BreachFinding struct {
	// InferredSLAs is the tightest inferred SLA per job across all paths.
	InferredSLAs map[scheduler.JobName]*time.Time
	// RunInferredSLAs is the tightest inferred SLA per job run (JobName, ScheduledAt UTC).
	RunInferredSLAs map[scheduler.JobRunKey]*time.Time
	// Bottleneck records the immediate downstream predecessor and depth on the
	// tightest-SLA path (see CalculateInferredSLAs).
	Bottleneck BottleneckPath
	// RunBottleneck records the immediate downstream predecessor and depth on the
	// tightest-SLA path per job run.
	RunBottleneck RunBottleneckPath
	// SLAStates carries the per-job inferred SLA, duration estimate and start offset.
	SLAStates map[scheduler.JobName]*scheduler.JobSLAState
	// RunSLAStates carries the per-run inferred SLA, duration estimate and start offset.
	RunSLAStates map[scheduler.JobRunKey]*scheduler.JobSLAState
	// BreachStates maps every breaching job to its state; Status is
	// SLABreachCauseRunningLate or SLABreachCauseNotStarted.
	BreachStates map[scheduler.JobName]*scheduler.JobState
	// RunBreachStates maps every breaching job run to its state.
	RunBreachStates map[scheduler.JobRunKey]*scheduler.JobState
	// Origins are the breaching jobs with no breaching direct upstream — the graph-rule
	// root causes — sorted by job name (then schedule) for deterministic results.
	Origins []*scheduler.JobState
	// OriginPaths holds, parallel to Origins, the target-to-origin path via the bottleneck
	// chain; entries are nil when no path could be reconstructed.
	OriginPaths [][]*scheduler.JobState
	// NodeByName indexes the lineage nodes visited during the search.
	NodeByName map[scheduler.JobName]*scheduler.JobLineageSummary
	// NodeByKey indexes the lineage nodes visited during the search by run key.
	NodeByKey map[scheduler.JobRunKey]*scheduler.JobLineageSummary
}

// FindInferredBreaches computes the inferred SLAs for a target's lineage, flags every
// breaching job run and derives the breach origins. It does not classify reasons; the caller
// decides what to do with the origins. An empty Origins slice means nothing is breaching.
func (i *Identifier) FindInferredBreaches(
	ctx context.Context,
	jobTarget *scheduler.JobLineageSummary,
	jobDurations map[scheduler.JobName]*time.Duration,
	startOffsets map[scheduler.JobName]*time.Duration,
	targetedSLA *time.Time,
	skipJobNames map[scheduler.JobName]bool,
	damperFactor scheduler.DamperFactor,
	referenceTime time.Time,
) *BreachFinding {
	finding := &BreachFinding{
		InferredSLAs:    map[scheduler.JobName]*time.Time{},
		RunInferredSLAs: map[scheduler.JobRunKey]*time.Time{},
		Bottleneck: BottleneckPath{
			Pred:  map[scheduler.JobName]scheduler.JobName{},
			Level: map[scheduler.JobName]int{},
		},
		RunBottleneck: RunBottleneckPath{
			Pred:  map[scheduler.JobRunKey]scheduler.JobRunKey{},
			Level: map[scheduler.JobRunKey]int{},
		},
		SLAStates:       map[scheduler.JobName]*scheduler.JobSLAState{},
		RunSLAStates:    map[scheduler.JobRunKey]*scheduler.JobSLAState{},
		BreachStates:    map[scheduler.JobName]*scheduler.JobState{},
		RunBreachStates: map[scheduler.JobRunKey]*scheduler.JobState{},
		NodeByName:      map[scheduler.JobName]*scheduler.JobLineageSummary{},
		NodeByKey:       map[scheduler.JobRunKey]*scheduler.JobLineageSummary{},
	}
	if jobTarget == nil || targetedSLA == nil {
		return finding
	}

	// the target finished before its SLA: there is no breach to find
	if targetRun := jobTarget.GetTargetRun(); targetRun != nil {
		if endTime := targetRun.GetActualEndTime(); endTime != nil && !endTime.After(*targetedSLA) {
			i.l.Info("target job finished before SLA, skipping breach detection",
				"job", jobTarget.JobName, "end_time", endTime, "sla", targetedSLA)
			return finding
		}
	}

	runInferredSLAs, runBottleneck := i.CalculateRunInferredSLAs(jobTarget, jobDurations, targetedSLA, damperFactor)
	inferredSLAs, bottleneck := projectInferredSLAsToJobs(runInferredSLAs, runBottleneck)
	runSLAStates := populateRunSLAStates(jobDurations, startOffsets, runInferredSLAs)
	slaStates := populateJobSLAStates(jobDurations, startOffsets, inferredSLAs)
	runBreachStates, nodeByKey, nodeByName := i.findRunBreachStates(ctx, jobTarget, runSLAStates, runBottleneck, skipJobNames, referenceTime)
	breachStates := projectBreachStatesToJobs(runBreachStates)
	origins, originPaths := runBreachOrigins(jobTarget.TargetRunKey(), runBottleneck, runSLAStates, runBreachStates, nodeByKey)

	finding.InferredSLAs = inferredSLAs
	finding.RunInferredSLAs = runInferredSLAs
	finding.Bottleneck = bottleneck
	finding.RunBottleneck = runBottleneck
	finding.SLAStates = slaStates
	finding.RunSLAStates = runSLAStates
	finding.BreachStates = breachStates
	finding.RunBreachStates = runBreachStates
	finding.Origins = origins
	finding.OriginPaths = originPaths
	finding.NodeByName = nodeByName
	finding.NodeByKey = nodeByKey
	return finding
}

// runBreachOrigins returns the breaching job runs with no breaching direct upstream run on that
// run's dependency edge — sorted by (job name, scheduled at) so repeated runs produce identical
// results. Each breach origin comes with its target-to-origin path via the bottleneck chain.
func runBreachOrigins(targetKey scheduler.JobRunKey, bottleneck RunBottleneckPath, runSLAStates map[scheduler.JobRunKey]*scheduler.JobSLAState,
	runBreachStates map[scheduler.JobRunKey]*scheduler.JobState, nodeByKey map[scheduler.JobRunKey]*scheduler.JobLineageSummary,
) ([]*scheduler.JobState, [][]*scheduler.JobState) {
	origins := make([]*scheduler.JobState, 0, len(runBreachStates))
	originPaths := make([][]*scheduler.JobState, 0, len(runBreachStates))

	keys := make([]scheduler.JobRunKey, 0, len(runBreachStates))
	for runKey := range runBreachStates {
		keys = append(keys, runKey)
	}
	sortJobRunKeys(keys)

	for _, runKey := range keys {
		breachState := runBreachStates[runKey]
		node, ok := nodeByKey[runKey]
		if !ok {
			continue
		}

		hasBreachingUpstream := false
		for _, upstream := range node.Upstreams {
			upstreamRun := upstream.ResolveUpstreamRun(runKey)
			if upstreamRun == nil {
				continue
			}
			upstreamKey := scheduler.NewJobRunKey(upstream.JobName, upstreamRun.ScheduledAt)
			if _, breaching := runBreachStates[upstreamKey]; breaching {
				hasBreachingUpstream = true
				break
			}
		}
		if hasBreachingUpstream {
			continue
		}
		origins = append(origins, breachState)
		path := reconstructRunStatePath(targetKey, runKey, bottleneck, runSLAStates, runBreachStates, nodeByKey)
		originPaths = append(originPaths, path)
	}
	return origins, originPaths
}
