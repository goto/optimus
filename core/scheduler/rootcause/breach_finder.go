package rootcause

import (
	"context"
	"sort"
	"time"

	"github.com/goto/optimus/core/scheduler"
)

// BreachFinding is the inferred-SLA breach analysis of one target, produced before any
// reason classification. It is the shared result behind the potential-SLA API and the
// root-cause engine, so both agree on which upstream jobs are breaching.
type BreachFinding struct {
	// InferredSLAs is the tightest inferred SLA per job across all paths.
	InferredSLAs map[scheduler.JobName]*time.Time
	// Bottleneck records the immediate downstream predecessor and depth on the
	// tightest-SLA path (see CalculateInferredSLAs).
	Bottleneck BottleneckPath
	// SLAStates carries the per-job inferred SLA, duration estimate and start offset.
	SLAStates map[scheduler.JobName]*scheduler.JobSLAState
	// BreachStates maps every breaching job to its state; Status is
	// SLABreachCauseRunningLate or SLABreachCauseNotStarted.
	BreachStates map[scheduler.JobName]*scheduler.JobState
	// Origins are the breaching jobs with no breaching direct upstream — the graph-rule
	// root causes — sorted by job name for deterministic results.
	Origins []*scheduler.JobState
	// OriginPaths holds, parallel to Origins, the target-to-origin path via the bottleneck
	// chain; entries are nil when no path could be reconstructed.
	OriginPaths [][]*scheduler.JobState
	// NodeByName indexes the lineage nodes visited during the search.
	NodeByName map[scheduler.JobName]*scheduler.JobLineageSummary
}

// FindInferredBreaches computes the inferred SLAs for a target's lineage, flags every
// breaching job and derives the breach origins. It does not classify reasons; the caller
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
		InferredSLAs: map[scheduler.JobName]*time.Time{},
		Bottleneck: BottleneckPath{
			Pred:  map[scheduler.JobName]scheduler.JobName{},
			Level: map[scheduler.JobName]int{},
		},
		SLAStates:    map[scheduler.JobName]*scheduler.JobSLAState{},
		BreachStates: map[scheduler.JobName]*scheduler.JobState{},
		NodeByName:   map[scheduler.JobName]*scheduler.JobLineageSummary{},
	}
	if jobTarget == nil || targetedSLA == nil {
		return finding
	}

	// the target finished before its SLA: there is no breach to find
	if targetRun, ok := jobTarget.JobRuns[jobTarget.JobName]; ok && targetRun != nil {
		if endTime := targetRun.GetActualEndTime(); endTime != nil && !endTime.After(*targetedSLA) {
			i.l.Info("target job finished before SLA, skipping breach detection",
				"job", jobTarget.JobName, "end_time", endTime, "sla", targetedSLA)
			return finding
		}
	}

	inferredSLAs, bottleneck := i.CalculateInferredSLAs(jobTarget, jobDurations, targetedSLA, damperFactor)
	slaStates := populateJobSLAStates(jobDurations, startOffsets, inferredSLAs)
	breachStates, nodeByName := i.findBreachStates(ctx, jobTarget, slaStates, bottleneck, skipJobNames, referenceTime)
	origins, originPaths := breachOrigins(jobTarget.JobName, bottleneck, slaStates, breachStates, nodeByName)

	finding.InferredSLAs = inferredSLAs
	finding.Bottleneck = bottleneck
	finding.SLAStates = slaStates
	finding.BreachStates = breachStates
	finding.Origins = origins
	finding.OriginPaths = originPaths
	finding.NodeByName = nodeByName
	return finding
}

// breachOrigins returns the breaching jobs with no breaching direct upstream — the graph-rule
// root causes — sorted by job name so repeated runs produce identical results. Each breach origin
// comes with its target-to-origin path via the bottleneck chain when one can be reconstructed.
func breachOrigins(targetName scheduler.JobName, bottleneck BottleneckPath, jobSLAStates map[scheduler.JobName]*scheduler.JobSLAState,
	breachStates map[scheduler.JobName]*scheduler.JobState, nodeByName map[scheduler.JobName]*scheduler.JobLineageSummary,
) ([]*scheduler.JobState, [][]*scheduler.JobState) {
	origins := make([]*scheduler.JobState, 0, len(breachStates))
	originPaths := make([][]*scheduler.JobState, 0, len(breachStates))

	names := make([]scheduler.JobName, 0, len(breachStates))
	for jobName := range breachStates {
		names = append(names, jobName)
	}
	sort.Slice(names, func(a, b int) bool { return names[a] < names[b] })

	for _, jobName := range names {
		breachState := breachStates[jobName]
		node, ok := nodeByName[jobName]
		if !ok {
			continue
		}

		hasBreachingUpstream := false
		for _, upstream := range node.Upstreams {
			if _, breaching := breachStates[upstream.JobName]; breaching {
				hasBreachingUpstream = true
				break
			}
		}
		if hasBreachingUpstream {
			continue
		}
		origins = append(origins, breachState)
		path := reconstructStatePath(targetName, jobName, bottleneck, jobSLAStates, breachStates, nodeByName)
		originPaths = append(originPaths, path)
	}
	return origins, originPaths
}
