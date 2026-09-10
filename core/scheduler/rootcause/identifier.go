package rootcause

import (
	"context"
	"slices"
	"sort"
	"time"

	"github.com/goto/salt/log"

	"github.com/goto/optimus/core/scheduler"
	"github.com/goto/optimus/core/tenant"
)

// ScheduledChangeGetter exists so runs whose inferred SLA came from a since-changed
// schedule can be skipped rather than reported as breaching.
type ScheduledChangeGetter interface {
	GetRecentScheduleChange(ctx context.Context, jobName scheduler.JobName, tnnt tenant.Tenant, startTime time.Time) (string, error)
}

type JobRunKey struct {
	Project     tenant.ProjectName
	JobName     scheduler.JobName
	ScheduledAt time.Time
}

// String is the map key used across this package: time.Time is unsafe as a map key
// because equality also compares monotonic reading and location.
func (k JobRunKey) String() string {
	return k.Project.String() + "/" + k.JobName.String() + "/" + k.ScheduledAt.UTC().Format(time.RFC3339)
}

// PendingSensorGetter returns the sensor task names still waiting on each run,
// keyed by JobRunKey.String().
type PendingSensorGetter interface {
	GetPendingSensors(ctx context.Context, keys []JobRunKey) (map[string][]string, error)
}

type Identifier struct {
	l                     log.Logger
	scheduledChangeGetter ScheduledChangeGetter
	pendingSensorGetter   PendingSensorGetter
	detectors             []ReasonDetector
}

// A nil pendingSensorGetter is allowed; RAW_DATA_DELAY then never fires and those
// causes fall through to UNKNOWN.
func NewIdentifier(l log.Logger, scheduledChangeGetter ScheduledChangeGetter, pendingSensorGetter PendingSensorGetter, thirdPartyTypes []string, detectors ...ReasonDetector) *Identifier {
	if len(detectors) == 0 {
		detectors = DefaultDetectors(thirdPartyTypes)
	}
	return &Identifier{
		l:                     l,
		scheduledChangeGetter: scheduledChangeGetter,
		pendingSensorGetter:   pendingSensorGetter,
		detectors:             detectors,
	}
}

func (i *Identifier) Identify(ctx context.Context, jobTarget *scheduler.JobLineageSummary, jobDurations map[scheduler.JobName]*time.Duration, targetedSLA *time.Time, skipJobNames map[scheduler.JobName]bool, damperFactor scheduler.DamperFactor, referenceTime time.Time) (map[scheduler.JobName]*scheduler.JobState, map[scheduler.JobName][]*scheduler.JobState) {
	// note: no need to realert again on target job which does not breached its SLA
	if targetRun, ok := jobTarget.JobRuns[jobTarget.JobName]; ok && targetRun != nil {
		if endTime := targetRun.GetActualEndTime(); endTime != nil && !endTime.After(*targetedSLA) {
			i.l.Info("target job finished before SLA, skipping breach detection",
				"job", jobTarget.JobName, "end_time", endTime, "sla", targetedSLA)
			return make(map[scheduler.JobName]*scheduler.JobState), make(map[scheduler.JobName][]*scheduler.JobState)
		}
	}

	// calculate inferred SLAs and record the tightest-path predecessor chain for level/path reporting
	// S(u|j) = S(j) - D(u)
	inferredSLAsByJobTarget, bottleneck := i.CalculateInferredSLAs(jobTarget, jobDurations, targetedSLA, damperFactor)

	// populate jobSLAStatesByJobTargetName
	jobSLAStates := populateJobSLAStates(jobDurations, inferredSLAsByJobTarget)

	// identify jobs that might breach their SLAs based on current time and inferred SLAs
	// T(now)>= S(u|j) and the job u has not completed yet
	// T(now)>= S(u|j) - D(u) and the job u has not started yet
	rootCauses, breachFullPaths := i.identifyRootCauses(ctx, jobTarget, jobSLAStates, bottleneck, skipJobNames, referenceTime)

	// populate breachesCauses
	breachesCauses := make(map[scheduler.JobName]*scheduler.JobState)
	for _, causes := range rootCauses {
		if len(causes) == 0 {
			continue
		}
		cause := causes[len(causes)-1] // root cause is the last element in the path
		breachesCauses[cause.JobName] = cause
	}

	// populate fullBreachesCauses
	fullBreachesCauses := make(map[scheduler.JobName][]*scheduler.JobState)
	for _, causes := range breachFullPaths {
		if len(causes) == 0 {
			continue
		}
		cause := causes[len(causes)-1] // root cause is the last element in the path (as a unique identifier)
		fullBreachesCauses[cause.JobName] = causes
	}

	// states are shared pointers with the reconstructed paths, so this also labels
	// the copies inside fullBreachesCauses
	i.classify(ctx, breachesCauses, referenceTime)

	return breachesCauses, fullBreachesCauses
}

func (i *Identifier) classify(ctx context.Context, causes map[scheduler.JobName]*scheduler.JobState, referenceTime time.Time) {
	pending := i.pendingSensors(ctx, causes)
	for _, state := range causes {
		key := JobRunKey{Project: state.Tenant.ProjectName(), JobName: state.JobName, ScheduledAt: state.JobRun.ScheduledAt}
		state.Reason, state.Evidence = classify(i.detectors, Candidate{
			State:          state,
			PendingSensors: pending[key.String()],
			ReferenceTime:  referenceTime,
		})
		if state.Reason == scheduler.ReasonUnknown && state.JobRun.TaskStartTime != nil {
			// sensors should have cleared before the task began; surface the inconsistency
			// rather than letting it silently shape the reason
			i.l.Warn("root cause started while classification found no reason", "job", state.JobName)
		}
	}
}

// Only unstarted causes can be sensor-blocked, so the query is limited to those.
func (i *Identifier) pendingSensors(ctx context.Context, causes map[scheduler.JobName]*scheduler.JobState) map[string][]string {
	if i.pendingSensorGetter == nil {
		return nil
	}
	keys := make([]JobRunKey, 0, len(causes))
	for _, state := range causes {
		if state.JobRun.TaskStartTime != nil {
			continue
		}
		keys = append(keys, JobRunKey{Project: state.Tenant.ProjectName(), JobName: state.JobName, ScheduledAt: state.JobRun.ScheduledAt})
	}
	if len(keys) == 0 {
		return nil
	}
	pending, err := i.pendingSensorGetter.GetPendingSensors(ctx, keys)
	if err != nil {
		i.l.Error("failed to fetch pending sensors, classifying without them", "error", err)
		return nil
	}
	return pending
}

// BottleneckPath records, for each job in the lineage, the immediate downstream predecessor
// and depth on the path that produced the tightest (earliest) inferred SLA.
// Used for level reporting and bottleneck-path reconstruction during breach detection.
type BottleneckPath struct {
	Pred  map[scheduler.JobName]scheduler.JobName
	Level map[scheduler.JobName]int
}

// CalculateInferredSLAs traverses bottom-up inferred-SLA calculation as a
// by following tightest-SLA rule. A job reachable through several downstream paths (a diamond
// in the lineage) is anchored to the earliest SLA any path implies —
// i.e. its longest/furthest path, which is where the real bottleneck sits.
//
// The damper only depends on the level (alpha^level), so a (job, level) pair fully determines
// the multiplier. Once a tighter SLA for a (job, level) is known, any looser arrival at the
// same (job, level) can only produce looser inferred SLA and wont be considered.
//
// Returns:
//   - inferredSLAs: tightest inferred SLA per job across all paths
//   - BottleneckPath:
//   - winningPred:  immediate downstream predecessor on the tightest path
//   - winningLevel: depth of that tightest arrival (distance from the target)
func (i *Identifier) CalculateInferredSLAs(jobTarget *scheduler.JobLineageSummary, jobDurations map[scheduler.JobName]*time.Duration, targetedSLA *time.Time, damperFactor scheduler.DamperFactor) (map[scheduler.JobName]*time.Time, BottleneckPath) {
	inferredSLAs := make(map[scheduler.JobName]*time.Time)
	bottleneck := BottleneckPath{
		Pred:  map[scheduler.JobName]scheduler.JobName{},
		Level: map[scheduler.JobName]int{},
	}
	if jobTarget == nil || targetedSLA == nil {
		return inferredSLAs, bottleneck
	}

	lowestDamperCoeff := damperFactor.InitialDamper()

	i.l.Info("damper coefficient used for inferred SLA calculation", "damper_coeff", damperFactor.Alpha)

	// bestByNodeLevel holds the tightest inferred SLA seen for a (job, level) pair; it is the
	// pruning key that keeps the relaxation bounded even on graphs with many overlapping paths.
	type nodeLevel struct {
		jobName scheduler.JobName
		level   int
	}
	bestByNodeLevel := make(map[nodeLevel]time.Time)

	targetSLA := *targetedSLA
	inferredSLAs[jobTarget.JobName] = &targetSLA
	bottleneck.Level[jobTarget.JobName] = 0
	bestByNodeLevel[nodeLevel{jobTarget.JobName, 0}] = targetSLA

	type state struct {
		job    *scheduler.JobLineageSummary
		level  int
		damper float64
		sla    time.Time
		path   []scheduler.JobName // job names from target down to (and including) this entry
	}
	queue := []state{{job: jobTarget, level: 0, damper: damperFactor.InitialDamper(), sla: targetSLA, path: []scheduler.JobName{jobTarget.JobName}}}
	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]

		duration := jobDurations[current.job.JobName]
		if duration == nil {
			continue
		}
		if current.damper < lowestDamperCoeff {
			lowestDamperCoeff = current.damper
		}

		childSLA := current.sla.Add(-time.Duration(current.damper*float64(duration.Milliseconds())) * time.Millisecond)
		childLevel := current.level + 1
		childDamper := damperFactor.NextDamper(current.damper)

		for _, upstreamJob := range current.job.Upstreams {
			// do not set inferred sla from a job which has no valid job runs
			if len(upstreamJob.JobRuns) == 0 {
				i.l.Debug("upstream job does not have associated runs to attach SLA. skipping remaining upstreams for this table", "upstream", upstreamJob.JobName, "targetJob", jobTarget.JobName)
				continue
			}
			// cycle guard: if the upstream already appears on the path from the target
			// to the current entry, following it would form a back-edge.
			cyclic := slices.Contains(current.path, upstreamJob.JobName)
			if cyclic {
				i.l.Warn("cycle detected in lineage, skipping upstream", "upstream", upstreamJob.JobName, "path", current.path)
				continue
			}

			key := nodeLevel{upstreamJob.JobName, childLevel}
			if existing, ok := bestByNodeLevel[key]; ok && !childSLA.Before(existing) {
				continue
			}
			bestByNodeLevel[key] = childSLA

			// record the global-minimum (tightest) inferred SLA across all paths and levels
			if best, ok := inferredSLAs[upstreamJob.JobName]; !ok || childSLA.Before(*best) {
				sla := childSLA
				inferredSLAs[upstreamJob.JobName] = &sla
				bottleneck.Pred[upstreamJob.JobName] = current.job.JobName
				bottleneck.Level[upstreamJob.JobName] = childLevel
			}

			childPath := make([]scheduler.JobName, len(current.path)+1)
			copy(childPath, current.path)
			childPath[len(current.path)] = upstreamJob.JobName
			queue = append(queue, state{job: upstreamJob, level: childLevel, damper: childDamper, sla: childSLA, path: childPath})
		}
	}

	i.l.Info("lowest damper coefficient used in inferred SLA calculation", "damper_coeff", lowestDamperCoeff)

	return inferredSLAs, bottleneck
}

// identifySLABreachRootCauses identifies jobs that might breach their SLA and traces the
// root causes back to the source via the tightest-path predecessor chain recorded during
// inferred-SLA calculation.
//
// A job is a *true* root cause when it is breaching but none of its direct upstreams are
// also breaching. This graph-based rule collapses diamond lineages to the single deepest
// breaching job regardless of which traversal branch visits the shared ancestor first.
func (i *Identifier) identifyRootCauses(ctx context.Context, jobTarget *scheduler.JobLineageSummary, jobSLAStates map[scheduler.JobName]*scheduler.JobSLAState, bottleneck BottleneckPath, skipJobNames map[scheduler.JobName]bool, referenceTime time.Time) ([][]*scheduler.JobState, [][]*scheduler.JobState) {
	jobBreachStates := make(map[scheduler.JobName]*scheduler.JobState)
	nodeByName := make(map[scheduler.JobName]*scheduler.JobLineageSummary)

	// DFS to flag breaching jobs; each unique job is evaluated once.
	visited := make(map[scheduler.JobName]bool)
	stack := []*scheduler.JobLineageSummary{jobTarget}
	for len(stack) > 0 {
		job := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if visited[job.JobName] {
			continue
		}
		visited[job.JobName] = true
		nodeByName[job.JobName] = job

		if !job.IsEnabled {
			i.l.Info("skipping job for SLA breach check as it's disabled", "job", job.JobName)
			continue
		}
		if jobSLAStates[job.JobName] == nil || jobSLAStates[job.JobName].InferredSLA == nil || jobSLAStates[job.JobName].EstimatedDuration == nil { // less likely occur, but just in case
			continue
		}
		if len(job.JobRuns) == 0 {
			i.l.Info("skipping job for SLA breach check as it has no job run associated", "job", job.JobName)
			continue
		}

		if skipJobNames[job.JobName] {
			i.l.Info("skipping job for SLA breach check as it's in the skip list", "job", job.JobName)
			if job.JobName == jobTarget.JobName {
				continue
			}
		} else {
			inferredSLA := *jobSLAStates[job.JobName].InferredSLA
			estimatedDuration := *jobSLAStates[job.JobName].EstimatedDuration

			// A job reached via multiple downstream paths (a diamond) can carry more than one
			// distinct run in JobRuns - one per path, since different downstream jobs can imply
			// different actual schedules for the same shared upstream. Check every run so a
			// breach on any converging branch isn't silently dropped just because another
			// branch's run happens to look fine.
			var state *scheduler.JobState
			for _, jobRun := range sortedJobRuns(job.JobRuns) {
				if oldScheduled, err := i.scheduledChangeGetter.GetRecentScheduleChange(ctx, job.JobName, job.Tenant, jobRun.ScheduledAt); err != nil {
					i.l.Error("failed to get recent schedule change for job, check the breach anyway", "job", job.JobName, "error", err)
				} else if oldScheduled != "" {
					i.l.Info("skipping run for SLA breach check as it has recent schedule change", "job", job.JobName, "old_scheduled_at", oldScheduled, "new_scheduled_at", jobRun.ScheduledAt)
					continue
				}

				// a single run can satisfy both conditions at once; condition 2 takes precedence,
				// matching the original single-run check order.
				var runState *scheduler.JobState
				// condition 1: T(now)>= S(u|j) and the job u has not completed yet
				if (referenceTime.After(inferredSLA) && jobRun.JobEndTime == nil) || (jobRun.JobEndTime != nil && jobRun.JobEndTime.After(inferredSLA)) {
					runState = &scheduler.JobState{
						JobSLAState:   *jobSLAStates[job.JobName],
						JobName:       job.JobName,
						JobRun:        *jobRun,
						Tenant:        job.Tenant,
						RelativeLevel: bottleneck.Level[job.JobName],
						Status:        scheduler.SLABreachCauseRunningLate,
					}
				}
				// condition 2: T(now)>= S(u|j) - D(u) and the job u has not started yet
				if referenceTime.After(inferredSLA.Add(-estimatedDuration)) && jobRun.TaskStartTime == nil {
					runState = &scheduler.JobState{
						JobSLAState:   *jobSLAStates[job.JobName],
						JobName:       job.JobName,
						JobRun:        *jobRun,
						Tenant:        job.Tenant,
						RelativeLevel: bottleneck.Level[job.JobName],
						Status:        scheduler.SLABreachCauseNotStarted,
					}
				}
				if runState != nil {
					state = runState
					break
				}
			}
			if state != nil {
				jobBreachStates[job.JobName] = state
				i.l.Info("potential SLA breach found", "job", job.JobName, "inferred_sla", inferredSLA, "duration", jobSLAStates[job.JobName].EstimatedDuration, "level", bottleneck.Level[job.JobName])
			}
		}

		stack = append(stack, job.Upstreams...)
	}

	// find exact root causes:
	// breaching jobs with no breaching direct upstream
	// this logic below makes propagated upstream breaches do not appear
	// as root cause of the target job breach
	rootCauses := make([][]*scheduler.JobState, 0)
	fullPaths := make([][]*scheduler.JobState, 0)
	for jobName, breachState := range jobBreachStates {
		node, ok := nodeByName[jobName]
		if !ok {
			continue
		}

		hasBreachingUpstream := false
		for _, upstream := range node.Upstreams {
			if _, breaching := jobBreachStates[upstream.JobName]; breaching {
				hasBreachingUpstream = true
				break
			}
		}
		if hasBreachingUpstream {
			continue
		}
		rootCauses = append(rootCauses, []*scheduler.JobState{breachState})
		path := reconstructStatePath(jobTarget.JobName, jobName, bottleneck, jobSLAStates, jobBreachStates, nodeByName)
		if len(path) > 0 {
			fullPaths = append(fullPaths, path)
		}
	}

	return rootCauses, fullPaths
}

// reconstructStatePath rebuilds the path from the target down to breachName by following the
// strictest-SLA predecessor chain, returning ordered list with target job first.
func reconstructStatePath(targetName, breachName scheduler.JobName, bottleneck BottleneckPath, jobSLAStates map[scheduler.JobName]*scheduler.JobSLAState, jobBreachStates map[scheduler.JobName]*scheduler.JobState, nodeByName map[scheduler.JobName]*scheduler.JobLineageSummary) []*scheduler.JobState {
	// walk from the breached jobs to the target job
	names := []scheduler.JobName{}
	seen := make(map[scheduler.JobName]bool)
	for cur := breachName; ; {
		// cyclic case: break
		if seen[cur] {
			break
		}
		seen[cur] = true
		names = append(names, cur)
		if cur == targetName {
			break
		}
		pred, ok := bottleneck.Pred[cur]
		if !ok {
			break
		}
		cur = pred
	}

	// reverse into target-first order and materialize states
	path := make([]*scheduler.JobState, 0, len(names))
	for i := len(names) - 1; i >= 0; i-- {
		name := names[i]
		if breachState, ok := jobBreachStates[name]; ok {
			path = append(path, breachState)
			continue
		}
		slaState := jobSLAStates[name]
		if slaState == nil {
			continue
		}
		plain := &scheduler.JobState{
			JobSLAState:   *slaState,
			JobName:       name,
			RelativeLevel: bottleneck.Level[name],
		}
		if node, ok := nodeByName[name]; ok {
			plain.Tenant = node.Tenant
		}
		path = append(path, plain)
	}
	return path
}

// sortedJobRuns returns a job's runs ordered by ScheduledAt ascending, so a diamond-shared job
// with multiple candidate runs (one per downstream path) is checked deterministically, earliest
// (most likely overdue) first.
func sortedJobRuns(jobRuns map[scheduler.JobName]*scheduler.JobRunSummary) []*scheduler.JobRunSummary {
	runs := make([]*scheduler.JobRunSummary, 0, len(jobRuns))
	for _, run := range jobRuns {
		runs = append(runs, run)
	}
	sort.Slice(runs, func(i, j int) bool {
		return runs[i].ScheduledAt.Before(runs[j].ScheduledAt)
	})
	unique := runs[:0]
	for i, run := range runs {
		if i == 0 || !run.ScheduledAt.Equal(runs[i-1].ScheduledAt) {
			unique = append(unique, run)
		}
	}
	return unique
}

func populateJobSLAStates(jobDurations map[scheduler.JobName]*time.Duration, jobSLAsByJobName map[scheduler.JobName]*time.Time) map[scheduler.JobName]*scheduler.JobSLAState {
	jobSLAStatesByJobName := make(map[scheduler.JobName]*scheduler.JobSLAState)
	for jobName, inferredSLA := range jobSLAsByJobName {
		jobSLAStatesByJobName[jobName] = &scheduler.JobSLAState{
			EstimatedDuration: jobDurations[jobName],
			InferredSLA:       inferredSLA,
		}
	}
	return jobSLAStatesByJobName
}
