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

// defaultMaxRootCauseClimb is used when MaxRootCauseClimbDepth is unset (<=0)
const defaultMaxRootCauseClimb = 5

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
	l                        log.Logger
	scheduledChangeGetter    ScheduledChangeGetter
	pendingSensorGetter      PendingSensorGetter
	detectors                []ReasonDetector
	thirdPartyDelayStartHour int
	thirdPartyDelayThreshold time.Duration
	minRootCauseDelay        time.Duration
	maxRootCauseClimb        int
}

// IdentifierConfig tunes delay scoring.
type IdentifierConfig struct {
	ThirdPartyDelayStartHourUTC     int
	ThirdPartyDelayThresholdSeconds int
	MinRootCauseDelaySeconds        int
	MaxRootCauseClimbDepth          int
}

func (c IdentifierConfig) delayStartHourUTC() int {
	if c.ThirdPartyDelayStartHourUTC < 0 || c.ThirdPartyDelayStartHourUTC > 23 {
		return defaultThirdPartyDelayStartHour
	}
	return c.ThirdPartyDelayStartHourUTC
}

func (c IdentifierConfig) delayThreshold() time.Duration {
	if c.ThirdPartyDelayThresholdSeconds < 0 {
		return 0
	}
	return time.Duration(c.ThirdPartyDelayThresholdSeconds) * time.Second
}

func (c IdentifierConfig) minRootCauseDelay() time.Duration {
	if c.MinRootCauseDelaySeconds < 0 {
		return 0
	}
	return time.Duration(c.MinRootCauseDelaySeconds) * time.Second
}

func (c IdentifierConfig) maxRootCauseClimb() int {
	if c.MaxRootCauseClimbDepth <= 0 {
		return defaultMaxRootCauseClimb
	}
	return c.MaxRootCauseClimbDepth
}

// A nil pendingSensorGetter is allowed; pending THIRD_PARTY_DELAY then never fires and those
// causes fall through to UNKNOWN.
func NewIdentifier(l log.Logger, scheduledChangeGetter ScheduledChangeGetter, pendingSensorGetter PendingSensorGetter, thirdPartyTypes []string, cfg IdentifierConfig, detectors ...ReasonDetector) *Identifier {
	if len(detectors) == 0 {
		detectors = DefaultDetectors(thirdPartyTypes)
	}
	return &Identifier{
		l:                        l,
		scheduledChangeGetter:    scheduledChangeGetter,
		pendingSensorGetter:      pendingSensorGetter,
		detectors:                detectors,
		thirdPartyDelayStartHour: cfg.delayStartHourUTC(),
		thirdPartyDelayThreshold: cfg.delayThreshold(),
		minRootCauseDelay:        cfg.minRootCauseDelay(),
		maxRootCauseClimb:        cfg.maxRootCauseClimb(),
	}
}

func (i *Identifier) Identify(
	ctx context.Context,
	jobTarget *scheduler.JobLineageSummary,
	jobDurations map[scheduler.JobName]*time.Duration,
	startOffsets map[scheduler.JobName]*time.Duration,
	targetedSLA *time.Time,
	skipJobNames map[scheduler.JobName]bool,
	damperFactor scheduler.DamperFactor,
	referenceTime time.Time,
) (
	map[scheduler.JobName]*scheduler.JobState,
	map[scheduler.JobName][]*scheduler.JobState,
) {
	// find every breaching job and its breach origins via the shared inferred-SLA analysis
	finding := i.FindInferredBreaches(ctx, jobTarget, jobDurations, startOffsets, targetedSLA, skipJobNames, damperFactor, referenceTime)
	if len(finding.Origins) == 0 {
		return make(map[scheduler.JobName]*scheduler.JobState), make(map[scheduler.JobName][]*scheduler.JobState)
	}

	// classify (and, where ambiguous, escalate to the upstream that actually caused the
	// delay) before picking the max-delay cause: a THIRD_PARTY_DELAY cause is keyed
	// by the sensor, and when several origins remain, only the max-delay one is returned.
	rootCauses := make([][]*scheduler.JobState, 0, len(finding.Origins))
	for _, origin := range finding.Origins {
		rootCauses = append(rootCauses, []*scheduler.JobState{origin})
	}
	rootCauses, breachFullPaths := i.classify(ctx, rootCauses, finding.OriginPaths, finding.NodeByKey, finding.NodeByName, finding.RunSLAStates, finding.SLAStates, referenceTime)

	candidates := make([]*scheduler.JobState, 0, len(rootCauses))
	pathByCause := make(map[*scheduler.JobState][]*scheduler.JobState, len(rootCauses))
	for idx, causes := range rootCauses {
		if len(causes) == 0 {
			continue
		}
		cause := causes[len(causes)-1] // root cause is the last element in the path
		candidates = append(candidates, cause)
		if idx < len(breachFullPaths) {
			pathByCause[cause] = breachFullPaths[idx]
		}
	}

	chosen := pickMaxDelay(candidates)
	if chosen == nil {
		return make(map[scheduler.JobName]*scheduler.JobState), make(map[scheduler.JobName][]*scheduler.JobState)
	}
	path := pathByCause[chosen]
	if demoted := demoteBelowMinRootCauseDelay(chosen, i.minRootCauseDelay); demoted != chosen {
		i.l.Info("root cause delay below min_root_cause_delay_seconds, keeping unknown reason",
			"job", chosen.JobName, "classified_reason", chosen.Reason,
			"induced_delay", chosen.Evidence.InducedDelay, "min_root_cause_delay", i.minRootCauseDelay)
		chosen = demoted
		if len(path) > 0 {
			path = append([]*scheduler.JobState(nil), path...)
			path[len(path)-1] = chosen
		}
	}
	return map[scheduler.JobName]*scheduler.JobState{chosen.JobName: chosen}, map[scheduler.JobName][]*scheduler.JobState{chosen.JobName: path}
}

// classify resolves the reason behind every path's root cause, escalating to an upstream where
// the reason turns out to be a relayed delay. It origins the states it is given untouched and
// returns both path sets with the resolved causes substituted in, since a cause can be shared
// between the two sets and can appear mid-path as well as at the end.
func (i *Identifier) classify(ctx context.Context, rootCauses, breachFullPaths [][]*scheduler.JobState, nodeByKey map[scheduler.JobRunKey]*scheduler.JobLineageSummary, nodeByName map[scheduler.JobName]*scheduler.JobLineageSummary, runSLAStates map[scheduler.JobRunKey]*scheduler.JobSLAState, jobSLAStates map[scheduler.JobName]*scheduler.JobSLAState, referenceTime time.Time) (resolvedRootCauses, resolvedFullPaths [][]*scheduler.JobState) {
	causes := make(map[scheduler.JobRunKey]*scheduler.JobState, len(rootCauses))
	for _, path := range rootCauses {
		if len(path) == 0 {
			continue
		}
		state := path[len(path)-1]
		causes[scheduler.NewJobRunKey(state.JobName, state.JobRun.ScheduledAt)] = state
	}

	pending := i.pendingSensors(ctx, causes)
	classifiedByRawState := make(map[*scheduler.JobState]*scheduler.JobState, len(causes))
	for runKey, state := range causes {
		key := JobRunKey{Project: state.Tenant.ProjectName(), JobName: state.JobName, ScheduledAt: state.JobRun.ScheduledAt}
		explanations := i.explanationsFor(*state, pending[key.String()], referenceTime)
		pool := make([]*scheduler.JobState, 0, len(explanations)+i.maxRootCauseClimb)
		startedLate := false
		for idx := range explanations {
			exp := explanations[idx]
			pool = append(pool, &explanations[idx])
			if exp.Reason == scheduler.ReasonStartedLate {
				startedLate = true
			}
		}
		if startedLate || state.Status == scheduler.SLABreachCauseNotStarted {
			node := nodeByKey[runKey]
			if node == nil {
				node = nodeByName[state.JobName]
			}
			upstreams := i.escalate(ctx, *state, node, runKey, runSLAStates, jobSLAStates, referenceTime)
			for idx := range upstreams {
				pool = append(pool, &upstreams[idx])
			}
		}

		best := pickMaxDelay(pool)
		if best == nil {
			unknown := *state
			unknown.Reason = scheduler.ReasonUnknown
			best = &unknown
		}

		// a started job with a known estimate cannot breach without being slow or late,
		// so landing on UNKNOWN here means the run data disagrees with itself
		if best.Reason == scheduler.ReasonUnknown && best.JobRun.TaskStartTime != nil && best.EstimatedDuration != nil {
			i.l.Warn("started root cause classified as unknown", "job", best.JobName,
				"task_start", best.JobRun.TaskStartTime, "task_end", best.JobRun.TaskEndTime,
				"estimated_duration", best.EstimatedDuration, "inferred_sla", best.InferredSLA)
		}

		// STARTED_LATE surviving escalation means no upstream explains it either; report
		// UNKNOWN rather than implying we know the cause.
		best = demoteStartedLateToUnknown(best)

		classifiedByRawState[state] = best
	}

	return applyClassifiedCauses(rootCauses, classifiedByRawState), applyClassifiedCauses(breachFullPaths, classifiedByRawState)
}

func (i *Identifier) explanationsFor(state scheduler.JobState, pending []string, referenceTime time.Time) []scheduler.JobState {
	matches := classifyAll(i.detectors, Candidate{
		State:                       &state,
		PendingSensors:              pending,
		ReferenceTime:               referenceTime,
		ThirdPartyDelayStartHourUTC: i.thirdPartyDelayStartHour,
		ThirdPartyDelayThreshold:    i.thirdPartyDelayThreshold,
	})
	if len(matches) == 0 {
		state.Reason = scheduler.ReasonUnknown
		if state.HistoricalStartOffset != nil {
			safeStart := effectiveSafeStart(state)
			state.Evidence.LatestSafeStartAt = &safeStart
			// measures InducedDelay by the actual start (nil TaskStartTime reads as the
			// reference time, i.e. "still waiting") against the safe start, clamped at zero.
			state.Evidence.InducedDelay = startedLateDelay(state, referenceTime)
			return []scheduler.JobState{state}
		}
		return []scheduler.JobState{i.annotateInducedDelay(state, referenceTime)}
	}
	out := make([]scheduler.JobState, 0, len(matches))
	for _, match := range matches {
		cause := state
		cause.Reason = match.Reason
		cause.Evidence = match.Evidence
		cause = attributeThirdPartyIdentity(cause)
		out = append(out, i.annotateInducedDelay(cause, referenceTime))
	}
	return out
}

// applyClassifiedCauses rebuilds paths with every state that has a classified counterpart
// swapped in, leaving the input paths/states untouched.
//
// Example: path = [target, mid_job, raw_root_cause] and
// classifiedByRawState = {raw_root_cause: classified_root_cause} (classified_root_cause may
// be a different job entirely, when escalate found the true upstream cause) produces
// [target, mid_job, classified_root_cause]. Every other, non-root-cause entry in the path
// (here, mid_job) has no key in the map and is copied through as-is.
func applyClassifiedCauses(paths [][]*scheduler.JobState, classifiedByRawState map[*scheduler.JobState]*scheduler.JobState) [][]*scheduler.JobState {
	out := make([][]*scheduler.JobState, 0, len(paths))
	for _, path := range paths {
		substituted := make([]*scheduler.JobState, len(path))
		for idx, state := range path {
			if replacement, ok := classifiedByRawState[state]; ok {
				substituted[idx] = replacement
				continue
			}
			substituted[idx] = state
		}
		out = append(out, substituted)
	}
	return out
}

// escalate climbs from a STARTED_LATE job toward the gating upstream, scoring every matching
// reason on each hop. It returns all abnormal explanations found on the climb (not only the
// first) so the caller can pick the max induced delay.
func (i *Identifier) escalate(ctx context.Context, state scheduler.JobState, node *scheduler.JobLineageSummary, runKey scheduler.JobRunKey, runSLAStates map[scheduler.JobRunKey]*scheduler.JobSLAState, jobSLAStates map[scheduler.JobName]*scheduler.JobSLAState, referenceTime time.Time) []scheduler.JobState {
	if node == nil {
		return nil
	}
	visited := map[scheduler.JobRunKey]bool{runKey: true}
	current := node
	currentRunKey := runKey
	level := state.RelativeLevel
	climbThroughWaiting := state.Status == scheduler.SLABreachCauseNotStarted
	found := make([]scheduler.JobState, 0, i.maxRootCauseClimb)

	for depth := 0; depth < i.maxRootCauseClimb; depth++ {
		upstream, run := gatingUpstream(current, currentRunKey, visited)
		if upstream == nil || run == nil {
			return found
		}
		upstreamRunKey := scheduler.NewJobRunKey(upstream.JobName, run.ScheduledAt)
		slaState := runSLAStates[upstreamRunKey]
		if slaState == nil && len(runSLAStates) == 0 {
			slaState = jobSLAStates[upstream.JobName]
		}
		if slaState == nil {
			return found
		}
		visited[upstreamRunKey] = true

		candidateState := scheduler.JobState{
			JobSLAState:   *slaState,
			JobName:       upstream.JobName,
			JobRun:        *run,
			Tenant:        upstream.Tenant,
			RelativeLevel: level + 1,
			Status:        scheduler.SLABreachCauseRunningLate,
		}
		if run.TaskStartTime == nil {
			candidateState.Status = scheduler.SLABreachCauseNotStarted
		}

		hop := i.explanationsFor(candidateState,
			i.pendingSensorsFor(ctx, upstream.JobName, upstream.Tenant, run.ScheduledAt, run.TaskStartTime),
			referenceTime)

		// An upstream that is itself still not started may simply be waiting on something
		// further up; only the NOT_STARTED investigation follows that chain speculatively.
		keepClimbing := climbThroughWaiting && candidateState.Status == scheduler.SLABreachCauseNotStarted
		for _, exp := range hop {
			if exp.Reason == scheduler.ReasonUnknown {
				continue
			}
			found = append(found, exp)
			switch exp.Reason {
			case scheduler.ReasonStartedLate:
				keepClimbing = true
				level = exp.RelativeLevel
			case scheduler.ReasonRunningLong, scheduler.ReasonThirdPartyDelay, scheduler.ReasonUpstreamFailed:
				// A definite, currently-binding answer: this hop's job has already started
				// (or, for UPSTREAM_FAILED, already ended), so anything further upstream of
				// it is stale history, not a bigger present-day cause. Stop here rather than
				// climbing past it to compare against a hop several levels further up.
				keepClimbing = false
			}
		}
		if !keepClimbing {
			return found
		}
		current = upstream
		currentRunKey = upstreamRunKey
	}
	return found
}

// gatingUpstream returns the direct upstream of node (for currentRunKey) most likely to have
// determined when node actually started, reusing scheduler.SelectUpstreamsForRun.
func gatingUpstream(node *scheduler.JobLineageSummary, currentRunKey scheduler.JobRunKey, visited map[scheduler.JobRunKey]bool) (*scheduler.JobLineageSummary, *scheduler.JobRunSummary) {
	candidates := make([]*scheduler.JobLineageSummary, 0, len(node.Upstreams))
	for _, upstream := range node.Upstreams {
		run := upstream.ResolveUpstreamRun(currentRunKey)
		if run == nil {
			continue
		}
		if !visited[scheduler.NewJobRunKey(upstream.JobName, run.ScheduledAt)] {
			candidates = append(candidates, upstream)
		}
	}
	if len(candidates) == 0 {
		return nil, nil
	}

	selected := scheduler.SelectUpstreamsForRun(&scheduler.JobLineageSummary{JobName: node.JobName, Upstreams: candidates}, currentRunKey, 1)
	if len(selected) == 0 {
		return nil, nil
	}
	upstream := selected[0]
	run := upstream.ResolveUpstreamRun(currentRunKey)
	return upstream, run
}

// Only unstarted candidates can be sensor-blocked, so pending sensors are fetched for them alone.
func (i *Identifier) pendingSensorsFor(ctx context.Context, jobName scheduler.JobName, tnnt tenant.Tenant, scheduledAt time.Time, taskStartTime *time.Time) []string {
	if i.pendingSensorGetter == nil || taskStartTime != nil {
		return nil
	}
	key := JobRunKey{Project: tnnt.ProjectName(), JobName: jobName, ScheduledAt: scheduledAt}
	pending, err := i.pendingSensorGetter.GetPendingSensors(ctx, []JobRunKey{key})
	if err != nil {
		i.l.Error("failed to fetch pending sensors while escalating root cause, classifying without them", "job", jobName, "error", err)
		return nil
	}
	return pending[key.String()]
}

// Only unstarted causes can be sensor-blocked, so the query is limited to those.
func (i *Identifier) pendingSensors(ctx context.Context, causes map[scheduler.JobRunKey]*scheduler.JobState) map[string][]string {
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

// RunBottleneckPath records, for each job run (JobName, ScheduledAt UTC) in the lineage, the
// immediate downstream run predecessor and depth on the path that produced the tightest
// (earliest) inferred SLA for that specific run.
type RunBottleneckPath struct {
	Pred  map[scheduler.JobRunKey]scheduler.JobRunKey
	Level map[scheduler.JobRunKey]int
}

// CalculateInferredSLAs traverses bottom-up inferred-SLA calculation per job run and projects
// the tightest inferred SLA per job name for callers that consume job-keyed maps.
func (i *Identifier) CalculateInferredSLAs(jobTarget *scheduler.JobLineageSummary, jobDurations map[scheduler.JobName]*time.Duration, targetedSLA *time.Time, damperFactor scheduler.DamperFactor) (map[scheduler.JobName]*time.Time, BottleneckPath) {
	runSLAs, runBottleneck := i.CalculateRunInferredSLAs(jobTarget, jobDurations, targetedSLA, damperFactor)
	return projectInferredSLAsToJobs(runSLAs, runBottleneck)
}

// CalculateRunInferredSLAs traverses bottom-up inferred-SLA calculation per (JobName, ScheduledAt)
// run by following the tightest-SLA rule. Separate scheduled runs of the same upstream job (e.g.
// B@7am via A and B@3am via C) each keep their own independent inferred SLA and bottleneck path.
func (i *Identifier) CalculateRunInferredSLAs(jobTarget *scheduler.JobLineageSummary, jobDurations map[scheduler.JobName]*time.Duration, targetedSLA *time.Time, damperFactor scheduler.DamperFactor) (map[scheduler.JobRunKey]*time.Time, RunBottleneckPath) {
	inferredSLAs := make(map[scheduler.JobRunKey]*time.Time)
	bottleneck := RunBottleneckPath{
		Pred:  map[scheduler.JobRunKey]scheduler.JobRunKey{},
		Level: map[scheduler.JobRunKey]int{},
	}
	if jobTarget == nil || targetedSLA == nil {
		return inferredSLAs, bottleneck
	}

	lowestDamperCoeff := damperFactor.InitialDamper()

	i.l.Info("damper coefficient used for inferred SLA calculation", "damper_coeff", damperFactor.Alpha)

	// bestByRunLevel holds the tightest inferred SLA seen for a (run, level) pair; it is the
	// pruning key that keeps the relaxation bounded even on graphs with many overlapping paths.
	type runLevel struct {
		runKey scheduler.JobRunKey
		level  int
	}
	bestByRunLevel := make(map[runLevel]time.Time)

	targetSLA := *targetedSLA
	targetKey := jobTarget.TargetRunKey()
	inferredSLAs[targetKey] = &targetSLA
	bottleneck.Level[targetKey] = 0
	bestByRunLevel[runLevel{targetKey, 0}] = targetSLA

	type state struct {
		job    *scheduler.JobLineageSummary
		runKey scheduler.JobRunKey
		level  int
		damper float64
		sla    time.Time
		path   []scheduler.JobRunKey // run keys from target down to (and including) this entry
	}
	queue := []state{{job: jobTarget, runKey: targetKey, level: 0, damper: damperFactor.InitialDamper(), sla: targetSLA, path: []scheduler.JobRunKey{targetKey}}}
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
			upstreamRun := upstreamJob.ResolveUpstreamRun(current.runKey)
			// do not set inferred sla from a job which has no valid job run for this downstream run
			if upstreamRun == nil {
				i.l.Debug("upstream job does not have associated runs to attach SLA. skipping remaining upstreams for this table", "upstream", upstreamJob.JobName, "targetJob", jobTarget.JobName)
				continue
			}

			upstreamRunKey := scheduler.NewJobRunKey(upstreamJob.JobName, upstreamRun.ScheduledAt)
			// cycle guard: if the upstream run already appears on the path from the target
			// to the current entry, following it would form a back-edge.
			cyclic := slices.Contains(current.path, upstreamRunKey)
			if cyclic {
				i.l.Warn("cycle detected in lineage, skipping upstream", "upstream", upstreamJob.JobName, "path", current.path)
				continue
			}

			key := runLevel{upstreamRunKey, childLevel}
			if existing, ok := bestByRunLevel[key]; ok && !childSLA.Before(existing) {
				continue
			}
			bestByRunLevel[key] = childSLA

			// record the global-minimum (tightest) inferred SLA for this specific run across all paths
			if best, ok := inferredSLAs[upstreamRunKey]; !ok || childSLA.Before(*best) {
				sla := childSLA
				inferredSLAs[upstreamRunKey] = &sla
				bottleneck.Pred[upstreamRunKey] = current.runKey
				bottleneck.Level[upstreamRunKey] = childLevel
			}

			childPath := make([]scheduler.JobRunKey, len(current.path)+1)
			copy(childPath, current.path)
			childPath[len(current.path)] = upstreamRunKey
			queue = append(queue, state{job: upstreamJob, runKey: upstreamRunKey, level: childLevel, damper: childDamper, sla: childSLA, path: childPath})
		}
	}

	i.l.Info("lowest damper coefficient used in inferred SLA calculation", "damper_coeff", lowestDamperCoeff)

	return inferredSLAs, bottleneck
}

func sortJobRunKeys(keys []scheduler.JobRunKey) {
	sort.Slice(keys, func(a, b int) bool {
		if keys[a].JobName != keys[b].JobName {
			return keys[a].JobName < keys[b].JobName
		}
		return keys[a].ScheduledAt.Before(keys[b].ScheduledAt)
	})
}

func projectInferredSLAsToJobs(runSLAs map[scheduler.JobRunKey]*time.Time, runBottleneck RunBottleneckPath) (map[scheduler.JobName]*time.Time, BottleneckPath) {
	jobSLAs := make(map[scheduler.JobName]*time.Time)
	bottleneck := BottleneckPath{
		Pred:  map[scheduler.JobName]scheduler.JobName{},
		Level: map[scheduler.JobName]int{},
	}

	keys := make([]scheduler.JobRunKey, 0, len(runSLAs))
	for key := range runSLAs {
		keys = append(keys, key)
	}
	sortJobRunKeys(keys)

	for _, key := range keys {
		sla := runSLAs[key]
		if sla == nil {
			continue
		}
		if existing, ok := jobSLAs[key.JobName]; !ok || sla.Before(*existing) {
			slaCopy := *sla
			jobSLAs[key.JobName] = &slaCopy
			bottleneck.Level[key.JobName] = runBottleneck.Level[key]
			if pred, ok := runBottleneck.Pred[key]; ok {
				bottleneck.Pred[key.JobName] = pred.JobName
			}
		}
	}
	return jobSLAs, bottleneck
}

// findRunBreachStates flags every job run that might breach its run-specific inferred SLA
// based on current time:
//
//	T(now) >= S(u|j) and the job u has not completed yet
//	T(now) >= S(u|j) - D(u) and the job u has not started yet
func (i *Identifier) findRunBreachStates(
	ctx context.Context,
	jobTarget *scheduler.JobLineageSummary,
	runSLAStates map[scheduler.JobRunKey]*scheduler.JobSLAState,
	runBottleneck RunBottleneckPath,
	skipJobNames map[scheduler.JobName]bool,
	referenceTime time.Time,
) (
	map[scheduler.JobRunKey]*scheduler.JobState,
	map[scheduler.JobRunKey]*scheduler.JobLineageSummary,
	map[scheduler.JobName]*scheduler.JobLineageSummary,
) {
	runBreachStates := make(map[scheduler.JobRunKey]*scheduler.JobState)
	nodeByKey := make(map[scheduler.JobRunKey]*scheduler.JobLineageSummary)
	nodeByName := make(map[scheduler.JobName]*scheduler.JobLineageSummary)

	if jobTarget == nil {
		return runBreachStates, nodeByKey, nodeByName
	}

	type stackItem struct {
		job    *scheduler.JobLineageSummary
		run    *scheduler.JobRunSummary
		runKey scheduler.JobRunKey
	}

	targetRun := jobTarget.GetTargetRun()
	if targetRun == nil {
		nodeByName[jobTarget.JobName] = jobTarget
		if !jobTarget.IsEnabled {
			i.l.Info("skipping job for SLA breach check as it's disabled", "job", jobTarget.JobName)
		} else {
			i.l.Info("skipping job for SLA breach check as it has no job run associated", "job", jobTarget.JobName)
		}
		return runBreachStates, nodeByKey, nodeByName
	}

	visited := make(map[scheduler.JobRunKey]bool)
	stack := []stackItem{{
		job:    jobTarget,
		run:    targetRun,
		runKey: scheduler.NewJobRunKey(jobTarget.JobName, targetRun.ScheduledAt),
	}}
	for len(stack) > 0 {
		item := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		job, jobRun, runKey := item.job, item.run, item.runKey
		if visited[runKey] {
			continue
		}
		visited[runKey] = true
		nodeByKey[runKey] = job
		nodeByName[job.JobName] = job

		if !job.IsEnabled {
			i.l.Info("skipping job for SLA breach check as it's disabled", "job", job.JobName)
			continue
		}
		slaState := runSLAStates[runKey]
		if slaState == nil || slaState.InferredSLA == nil || slaState.EstimatedDuration == nil { // less likely occur, but just in case
			continue
		}
		if jobRun == nil {
			i.l.Info("skipping job for SLA breach check as it has no job run associated", "job", job.JobName)
			continue
		}

		if skipJobNames[job.JobName] {
			i.l.Info("skipping job for SLA breach check as it's in the skip list", "job", job.JobName)
			if job.JobName == jobTarget.JobName {
				continue
			}
		} else {
			inferredSLA := *slaState.InferredSLA
			estimatedDuration := *slaState.EstimatedDuration

			if oldScheduled, err := i.scheduledChangeGetter.GetRecentScheduleChange(ctx, job.JobName, job.Tenant, jobRun.ScheduledAt); err != nil {
				i.l.Error("failed to get recent schedule change for job, check the breach anyway", "job", job.JobName, "error", err)
			} else if oldScheduled != "" {
				i.l.Info("skipping run for SLA breach check as it has recent schedule change", "job", job.JobName, "old_scheduled_at", oldScheduled, "new_scheduled_at", jobRun.ScheduledAt)
			} else {
				// a single run can satisfy both conditions at once; condition 2 takes precedence,
				// matching the original single-run check order.
				var runState *scheduler.JobState
				// condition 1: T(now)>= S(u|j) and the job u has not completed yet
				if (referenceTime.After(inferredSLA) && jobRun.JobEndTime == nil) || (jobRun.JobEndTime != nil && jobRun.JobEndTime.After(inferredSLA)) {
					runState = &scheduler.JobState{
						JobSLAState:   *slaState,
						JobName:       job.JobName,
						JobRun:        *jobRun,
						Tenant:        job.Tenant,
						RelativeLevel: runBottleneck.Level[runKey],
						Status:        scheduler.SLABreachCauseRunningLate,
					}
				}
				// condition 2: T(now)>= S(u|j) - D(u) and the job u has not started yet
				if referenceTime.After(inferredSLA.Add(-estimatedDuration)) && jobRun.TaskStartTime == nil {
					runState = &scheduler.JobState{
						JobSLAState:   *slaState,
						JobName:       job.JobName,
						JobRun:        *jobRun,
						Tenant:        job.Tenant,
						RelativeLevel: runBottleneck.Level[runKey],
						Status:        scheduler.SLABreachCauseNotStarted,
					}
				}
				if runState != nil {
					runBreachStates[runKey] = runState
					i.l.Info("potential SLA breach found", "job", job.JobName, "scheduled_at", jobRun.ScheduledAt, "inferred_sla", inferredSLA, "duration", slaState.EstimatedDuration, "level", runBottleneck.Level[runKey])
				}
			}
		}

		for _, upstream := range job.Upstreams {
			upstreamRun := upstream.ResolveUpstreamRun(runKey)
			if upstreamRun == nil {
				continue
			}
			stack = append(stack, stackItem{
				job:    upstream,
				run:    upstreamRun,
				runKey: scheduler.NewJobRunKey(upstream.JobName, upstreamRun.ScheduledAt),
			})
		}
	}

	return runBreachStates, nodeByKey, nodeByName
}

func projectBreachStatesToJobs(runBreachStates map[scheduler.JobRunKey]*scheduler.JobState) map[scheduler.JobName]*scheduler.JobState {
	jobBreachStates := make(map[scheduler.JobName]*scheduler.JobState)
	keys := make([]scheduler.JobRunKey, 0, len(runBreachStates))
	for key := range runBreachStates {
		keys = append(keys, key)
	}
	sortJobRunKeys(keys)

	for _, key := range keys {
		if _, exists := jobBreachStates[key.JobName]; !exists {
			jobBreachStates[key.JobName] = runBreachStates[key]
		}
	}
	return jobBreachStates
}

// reconstructRunStatePath rebuilds the path from the target down to breachKey by following the
// strictest-SLA predecessor chain, returning ordered list with target job first.
func reconstructRunStatePath(targetKey, breachKey scheduler.JobRunKey, bottleneck RunBottleneckPath, runSLAStates map[scheduler.JobRunKey]*scheduler.JobSLAState, runBreachStates map[scheduler.JobRunKey]*scheduler.JobState, nodeByKey map[scheduler.JobRunKey]*scheduler.JobLineageSummary) []*scheduler.JobState {
	keys := []scheduler.JobRunKey{}
	seen := make(map[scheduler.JobRunKey]bool)
	for cur := breachKey; ; {
		// cyclic case: break
		if seen[cur] {
			break
		}
		seen[cur] = true
		keys = append(keys, cur)
		if cur == targetKey {
			break
		}
		pred, ok := bottleneck.Pred[cur]
		if !ok {
			break
		}
		cur = pred
	}

	// reverse into target-first order and materialize states
	path := make([]*scheduler.JobState, 0, len(keys))
	for i := len(keys) - 1; i >= 0; i-- {
		key := keys[i]
		if breachState, ok := runBreachStates[key]; ok {
			path = append(path, breachState)
			continue
		}
		slaState := runSLAStates[key]
		if slaState == nil {
			continue
		}
		plain := &scheduler.JobState{
			JobSLAState: *slaState,
			JobName:     key.JobName,
			JobRun: scheduler.JobRunSummary{
				JobName:     key.JobName,
				ScheduledAt: key.ScheduledAt,
			},
			RelativeLevel: bottleneck.Level[key],
		}
		if node, ok := nodeByKey[key]; ok {
			plain.Tenant = node.Tenant
			if key == targetKey {
				if run := node.GetTargetRun(); run != nil {
					plain.JobRun = *run
				}
			} else if predKey, hasPred := bottleneck.Pred[key]; hasPred {
				if run := node.ResolveUpstreamRun(predKey); run != nil {
					plain.JobRun = *run
				}
			}
		}
		path = append(path, plain)
	}
	return path
}

func populateRunSLAStates(jobDurations, startOffsets map[scheduler.JobName]*time.Duration, runSLAs map[scheduler.JobRunKey]*time.Time) map[scheduler.JobRunKey]*scheduler.JobSLAState {
	states := make(map[scheduler.JobRunKey]*scheduler.JobSLAState, len(runSLAs))
	for runKey, inferredSLA := range runSLAs {
		states[runKey] = &scheduler.JobSLAState{
			EstimatedDuration:     jobDurations[runKey.JobName],
			InferredSLA:           inferredSLA,
			HistoricalStartOffset: startOffsets[runKey.JobName],
		}
	}
	return states
}

func populateJobSLAStates(jobDurations, startOffsets map[scheduler.JobName]*time.Duration, jobSLAsByJobName map[scheduler.JobName]*time.Time) map[scheduler.JobName]*scheduler.JobSLAState {
	jobSLAStatesByJobName := make(map[scheduler.JobName]*scheduler.JobSLAState)
	for jobName, inferredSLA := range jobSLAsByJobName {
		jobSLAStatesByJobName[jobName] = &scheduler.JobSLAState{
			EstimatedDuration:     jobDurations[jobName],
			InferredSLA:           inferredSLA,
			HistoricalStartOffset: startOffsets[jobName],
		}
	}
	return jobSLAStatesByJobName
}
