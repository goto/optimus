package rootcause

import (
	"sort"
	"time"

	"github.com/goto/optimus/core/scheduler"
	"github.com/goto/optimus/core/tenant"
)

type PublisherConfig struct {
	RuntimeDelayThreshold    time.Duration
	SchedulerDelayPerDepth   time.Duration
	ThirdPartyDelayThreshold time.Duration
	// ThirdPartyDelayStartHourUTC anchors the daily sensor-incident boundary; see
	// ThirdPartyDailyBoundary.
	ThirdPartyDelayStartHourUTC int
	ThirdPartyTypes             []string
}

func (c PublisherConfig) withDefaults() PublisherConfig {
	if c.RuntimeDelayThreshold <= 0 {
		c.RuntimeDelayThreshold = 30 * time.Minute
	}
	if c.SchedulerDelayPerDepth <= 0 {
		c.SchedulerDelayPerDepth = 150 * time.Second // 2.5 minutes
	}
	if c.ThirdPartyDelayThreshold <= 0 {
		c.ThirdPartyDelayThreshold = 15 * time.Minute
	}
	if c.ThirdPartyDelayStartHourUTC < 0 || c.ThirdPartyDelayStartHourUTC > 23 {
		c.ThirdPartyDelayStartHourUTC = 0
	}
	return c
}

type Publisher struct {
	config             PublisherConfig
	thirdPartyByPrefix map[string]string
}

func NewPublisher(config PublisherConfig) *Publisher {
	config = config.withDefaults()
	return &Publisher{
		config:             config,
		thirdPartyByPrefix: thirdPartyPrefixes(config.ThirdPartyTypes),
	}
}

type TargetEvaluation struct {
	// Target is the SLA-carrying lineage whose breach is being explained.
	Target *scheduler.JobLineageSummary
	// HistoricalByJob holds the historical task/hook baseline per job reachable from the target
	HistoricalByJob map[scheduler.JobName]scheduler.JobHistoricalDuration
	// Finding is the shared inferred-SLA breach analysis of the target; the gate below
	// requires at least one breach origin before any reason is evaluated.
	Finding       *BreachFinding
	ReferenceTime time.Time
	WalkOpts      scheduler.LineageWalkOptions
}

// Evaluate runs the breach gate and the reason detectors for one target. An empty
// result means the target did not breach or produced no explanation.
func (p *Publisher) Evaluate(ev TargetEvaluation) []scheduler.RootCauseDetection {
	if ev.Target == nil {
		return nil
	}

	walk := ev.Target.GetLineageNodes(ev.WalkOpts)
	if len(walk.Nodes) == 0 {
		return nil
	}
	targetNode := walk.Nodes[0]
	targetRun := targetNode.JobRunSummary
	if targetRun == nil || targetRun.SLATime == nil {
		return nil
	}
	// the resolver stamps SLATime = scheduled_at + SLA duration; a job without an SLA ends
	// up with SLATime == scheduled_at and must not be evaluated at all
	if targetRun.SLATime.Equal(targetRun.ScheduledAt) {
		return nil
	}

	// inferred-breach gate: potential-first — without at least one node breaching its
	// inferred SLA there is nothing to explain, so no detection is raised or recorded
	if ev.Finding == nil || len(ev.Finding.Origins) == 0 {
		return nil
	}
	// an actual breach outranks a predicted one
	breached := breachedSLA(targetRun, ev.ReferenceTime)
	breachStatus := scheduler.BreachStatusPotential
	if breached {
		breachStatus = scheduler.BreachStatusActual
	}

	impacted := []scheduler.ImpactedJob{{
		ProjectName: ev.Target.Tenant.ProjectName(),
		JobName:     ev.Target.JobName,
		ScheduledAt: targetRun.ScheduledAt,
	}}

	// the cause chain covers what the inferred-breach leaves are actually waiting on;
	// scheduler delay is measured in the next step over the same chain
	chain := p.causeChain(walk, ev.Finding)

	detections := p.tierFailedUpstream(walk, impacted)
	detections = append(detections, p.tierWaitingUpstream(walk, ev.Finding, impacted)...)
	detections = append(detections, p.tierThirdPartyDelay(chain, ev.ReferenceTime, impacted)...)
	detections = append(detections, p.tierSchedulerDelay(walk, impacted)...)
	detections = append(detections, p.tierRuntimeDelay(chain, ev, impacted)...)
	for i := range detections {
		detections[i].BreachStatus = breachStatus
	}

	// if nothing explains the breach, the SLA is too aggressive for what the lineage
	// delivers — but only when the target is past its SLA and is itself the only breach origin
	// cause: a potential miss with no cause says nothing about the SLA yet, and a breach origin
	// elsewhere would have been detected above
	if len(detections) == 0 {
		if !breached || len(ev.Finding.Origins) != 1 || ev.Finding.Origins[0].JobName != ev.Target.JobName {
			return nil
		}
		return []scheduler.RootCauseDetection{{
			IncidentKey:  incidentKey(targetNode.ProjectName, targetNode.JobName, targetRun.ScheduledAt),
			Reason:       scheduler.RootCauseReasonDetail{Reason: scheduler.TierAggressiveSLA, Status: scheduler.ReasonStatusActive},
			ImpactedJobs: impacted,
			BreachStatus: breachStatus,
		}}
	}
	return detections
}

func breachedSLA(run *scheduler.JobRunSummary, referenceTime time.Time) bool {
	sla := run.SLATime
	if sla == nil {
		return false
	}
	if endTime := run.GetActualEndTime(); endTime != nil {
		return endTime.After(*sla)
	}
	return referenceTime.After(*sla)
}

// waitChain is the effective wait chain the target is waiting on. The gating path covers
// finished lineages; for an in-flight lineage it stops at the first unfinished upstream, so
// each blocking frontier node is appended together with the latest-finishing finished
// upstream chain behind it. Branches are deduplicated by run key.
func (*Publisher) waitChain(walk *scheduler.LineageWalkResult) []*scheduler.JobExecutionSummary {
	path := walk.GatingPath()
	chain := make([]*scheduler.JobExecutionSummary, len(path), len(path)+len(walk.Nodes))
	copy(chain, path)
	seen := map[scheduler.JobRunKey]bool{}
	for _, node := range path {
		seen[node.Key()] = true
	}

	for _, node := range walk.Nodes {
		if !node.IsBlocking {
			continue
		}
		for current := node; !seen[current.Key()]; {
			seen[current.Key()] = true
			chain = append(chain, current)
			next := latestFinishedUpstream(walk, current)
			if next == nil {
				break
			}
			current = next
		}
	}
	return chain
}

// causeChain is the upstream closure of the inferred-breach breach origins: each breach origin plus
// every run reachable from it through real upstream edges. It anchors runtime and
// third-party attribution on the jobs actually causing the breach — including running
// ancestors a single latest-finished thread would miss — instead of the target's whole
// walk. Falls back to the full wait chain when a breach origin has no matching run in the walk
// (a diamond may have resolved a different run on the tightest path).
func (p *Publisher) causeChain(walk *scheduler.LineageWalkResult, finding *BreachFinding) []*scheduler.JobExecutionSummary {
	if finding == nil || len(finding.Origins) == 0 {
		return p.waitChain(walk)
	}

	chain := make([]*scheduler.JobExecutionSummary, 0, len(walk.Nodes))
	seen := map[scheduler.JobRunKey]bool{}
	for _, origin := range finding.Origins {
		originNode, ok := walk.NodesByKey[scheduler.NewJobRunKey(origin.JobName, origin.JobRun.ScheduledAt)]
		if !ok {
			return p.waitChain(walk)
		}
		stack := []*scheduler.JobExecutionSummary{originNode}
		for len(stack) > 0 {
			node := stack[len(stack)-1]
			stack = stack[:len(stack)-1]
			if seen[node.Key()] {
				continue
			}
			seen[node.Key()] = true
			chain = append(chain, node)
			for _, upstreamKey := range walk.UpstreamsByKey[node.Key()] {
				if upstream, ok := walk.NodesByKey[upstreamKey]; ok {
					stack = append(stack, upstream)
				}
			}
		}
	}
	return chain
}

func latestFinishedUpstream(walk *scheduler.LineageWalkResult, node *scheduler.JobExecutionSummary) *scheduler.JobExecutionSummary {
	var latest *scheduler.JobExecutionSummary
	for _, upstreamKey := range walk.UpstreamsByKey[node.Key()] {
		upstream, ok := walk.NodesByKey[upstreamKey]
		if !ok || !upstream.JobRunSummary.IsFinished() {
			continue
		}
		if latest == nil || laterFinisher(upstream, latest) {
			latest = upstream
		}
	}
	return latest
}

func laterFinisher(candidate, best *scheduler.JobExecutionSummary) bool {
	candidateEnd, bestEnd := candidate.JobRunSummary.GetActualEndTime(), best.JobRunSummary.GetActualEndTime()
	if !candidateEnd.Equal(*bestEnd) {
		return candidateEnd.After(*bestEnd)
	}
	return candidate.JobName < best.JobName
}

// tierFailedUpstream scans the whole walked lineage: any failed ancestor, regardless of
// its scheduled date, becomes its own incident keyed on the failed job.
func (*Publisher) tierFailedUpstream(walk *scheduler.LineageWalkResult, impacted []scheduler.ImpactedJob) []scheduler.RootCauseDetection {
	var detections []scheduler.RootCauseDetection
	for _, node := range walk.Nodes {
		if node.Level == 0 || node.State != scheduler.StateFailed {
			continue
		}
		detections = append(detections, scheduler.RootCauseDetection{
			IncidentKey:  nodeIncidentKey(node),
			Reason:       scheduler.RootCauseReasonDetail{Reason: scheduler.TierFailedUpstream, Status: scheduler.ReasonStatusActive},
			ImpactedJobs: impacted,
		})
	}
	return detections
}

// tierWaitingUpstream catches structural stuck states: the node is not running and not
// finished, is past its latest safe start (its inferred SLA minus its own duration — the
// NOT_STARTED breach condition), yet every resolved upstream is either successful or
// missing entirely (uncreated runs, depends_on_past self-waits).
// Nodes whose upstreams failed are FAILED_UPSTREAM's finding; nodes waiting on a
// still-running upstream are just normal waiting.
func (p *Publisher) tierWaitingUpstream(walk *scheduler.LineageWalkResult, finding *BreachFinding, impacted []scheduler.ImpactedJob) []scheduler.RootCauseDetection {
	var detections []scheduler.RootCauseDetection
	for _, node := range walk.Nodes {
		if !isStuckState(node) {
			continue
		}
		if p.hasPendingThirdPartySensor(node) {
			continue
		}
		if !upstreamsSuccessOrMissing(walk, node) {
			continue
		}
		// the node must be past its latest safe start per the inferred-SLA analysis, so a
		// node still within its own slack is not stuck yet
		if !isPastSafeStart(node, finding) {
			continue
		}
		detections = append(detections, scheduler.RootCauseDetection{
			IncidentKey:  nodeIncidentKey(node),
			Reason:       scheduler.RootCauseReasonDetail{Reason: scheduler.TierWaitingUpstream, Status: scheduler.ReasonStatusActive},
			ImpactedJobs: impacted,
		})
	}
	return detections
}

// isPastSafeStart reports whether the node's run is flagged NOT_STARTED by the inferred-SLA
// analysis; the run must match so a diamond's other runs don't qualify the node.
func isPastSafeStart(node *scheduler.JobExecutionSummary, finding *BreachFinding) bool {
	if finding == nil {
		return false
	}
	state, ok := finding.BreachStates[node.JobName]
	if !ok || state.Status != scheduler.SLABreachCauseNotStarted {
		return false
	}
	return state.JobRun.ScheduledAt.UTC().Equal(node.JobRunSummary.ScheduledAt.UTC())
}

func isStuckState(node *scheduler.JobExecutionSummary) bool {
	return node.State == scheduler.StateWaitUpstream || node.State == scheduler.StateNotScheduled
}

// hasPendingThirdPartySensor reports whether the node is currently waiting on one of its own
// third-party sensors — a wait THIRD_PARTY_DELAY explains, so this detector must not
// double-report it
func (p *Publisher) hasPendingThirdPartySensor(node *scheduler.JobExecutionSummary) bool {
	if node.JobRunSummary.TaskStartTime != nil || node.JobRunSummary.SensorName == nil {
		return false
	}
	return p.matchThirdPartySensor(*node.JobRunSummary.SensorName) != ""
}

func upstreamsSuccessOrMissing(walk *scheduler.LineageWalkResult, node *scheduler.JobExecutionSummary) bool {
	for _, upstreamKey := range walk.UpstreamsByKey[node.Key()] {
		upstream, ok := walk.NodesByKey[upstreamKey]
		if !ok {
			continue
		}
		switch upstream.State {
		case scheduler.StateSuccess, scheduler.StateNotScheduled:
			// successful, or an expected run missing from the database
		default:
			return false
		}
	}
	return true
}

// tierThirdPartyDelay measures sensor waits along the wait chain. Wait time is counted from
// the later of the sensor's wait start and the daily boundary.
func (p *Publisher) tierThirdPartyDelay(chain []*scheduler.JobExecutionSummary, referenceTime time.Time, impacted []scheduler.ImpactedJob) []scheduler.RootCauseDetection {
	if len(p.thirdPartyByPrefix) == 0 {
		return nil
	}

	boundary := ThirdPartyDailyBoundary(referenceTime, p.config.ThirdPartyDelayStartHourUTC)
	seen := map[scheduler.JobRunKey]bool{}
	var detections []scheduler.RootCauseDetection

	for _, node := range chain {
		if node == nil || seen[node.Key()] {
			continue
		}
		seen[node.Key()] = true
		sensorName := node.JobRunSummary.SensorName
		if sensorName == nil {
			continue
		}
		thirdPartyType := p.matchThirdPartySensor(*sensorName)
		if thirdPartyType == "" {
			continue
		}

		waitStart := boundary
		if node.JobRunSummary.WaitStartTime != nil && node.JobRunSummary.WaitStartTime.After(boundary) {
			waitStart = *node.JobRunSummary.WaitStartTime
		}
		waitEnd := referenceTime
		if node.JobRunSummary.WaitEndTime != nil {
			waitEnd = *node.JobRunSummary.WaitEndTime
		}
		wait := waitEnd.Sub(waitStart)
		if wait <= p.config.ThirdPartyDelayThreshold {
			continue
		}

		detections = append(detections, scheduler.RootCauseDetection{
			IncidentKey: scheduler.SLABreachIncidentKey{
				ProjectName: node.ProjectName,
				JobName:     node.JobName,
				// sensors have no native run: all of today's sensor waits group into one
				// daily incident, so the recipients get a single alert per sensor
				ScheduledAt: boundary,
			},
			Reason: scheduler.RootCauseReasonDetail{
				Reason:              scheduler.TierThirdPartyDelay,
				Status:              scheduler.ReasonStatusActive,
				InducedDelaySeconds: int(wait.Seconds()),
				BlockedOnSensors:    []string{*sensorName},
				SourceType:          thirdPartyType,
				// a sensor wait is finished once its wait end time exists
				IsJobFinished: node.JobRunSummary.WaitEndTime != nil,
			},
			ImpactedJobs: impacted,
		})
	}
	return detections
}

func (p *Publisher) matchThirdPartySensor(sensorName string) string {
	for prefix, thirdPartyType := range p.thirdPartyByPrefix {
		if len(sensorName) >= len(prefix) && sensorName[:len(prefix)] == prefix {
			return thirdPartyType
		}
	}
	return ""
}

// thirdPartyDailyBoundary was moved to ThirdPartyDailyBoundary so the identifier and the
// publisher measure sensor waits against the same daily window.

// tierSchedulerDelay attributes the scheduler for the total scheduling wait accumulated
// along the target's critical path: the finished gating path plus any started-but-running
// node hanging off its end (its delay is exact once the task has started). The budget
// scales with the measured path depth, so a healthy deep lineage stays silent. Nodes that
// have not started yet are skipped — their final delay is still unknown (stated
// limitation). The incident is keyed on the target's run, so the alert keeps the target
// identity and the alert_logs deduplication applies like any other reason.
//
// The delay is computed here (not read from DelaySummary, which only GenerateLineage-
// ExecutionSummary fills for finished gating paths) with the same formula the summary
// uses: the wait from the later of the node's schedule and its latest-finished upstream's
// end until the task start.
func (p *Publisher) tierSchedulerDelay(walk *scheduler.LineageWalkResult, impacted []scheduler.ImpactedJob) []scheduler.RootCauseDetection {
	gatingPath := walk.GatingPath()
	if len(gatingPath) == 0 {
		return nil
	}

	// the measured chain: started nodes of the critical path, plus started running heads
	// directly upstream of its end
	measured := make([]*scheduler.JobExecutionSummary, 0, len(gatingPath)+1)
	for _, node := range gatingPath {
		if node.JobRunSummary.TaskStartTime != nil {
			measured = append(measured, node)
		}
	}
	pathEnd := gatingPath[len(gatingPath)-1]
	for _, upstreamKey := range walk.UpstreamsByKey[pathEnd.Key()] {
		upstream, ok := walk.NodesByKey[upstreamKey]
		if ok && !upstream.JobRunSummary.IsFinished() && upstream.JobRunSummary.TaskStartTime != nil {
			measured = append(measured, upstream)
		}
	}
	if len(measured) == 0 {
		return nil
	}

	var total time.Duration
	for _, node := range measured {
		total += nodeSchedulingDelay(walk, node)
	}
	if budget := time.Duration(p.schedulerDelayBudget(len(measured))) * time.Second; total <= budget {
		return nil
	}

	targetNode := walk.Nodes[0]
	return []scheduler.RootCauseDetection{{
		IncidentKey: incidentKey(targetNode.ProjectName, targetNode.JobName, targetNode.JobRunSummary.ScheduledAt),
		Reason: scheduler.RootCauseReasonDetail{
			Reason:              scheduler.TierSchedulerDelay,
			Status:              scheduler.ReasonStatusActive,
			InducedDelaySeconds: int(total.Seconds()),
			IsJobFinished:       targetNode.JobRunSummary.IsFinished(),
		},
		ImpactedJobs: impacted,
	}}
}

// nodeSchedulingDelay is one node's scheduler-owned wait: from the later of its own
// schedule and its latest-finished upstream's end until the task started. A node with no
// finished upstream contributes nothing — a wait with no completed dependency behind it
// belongs to the missing upstream, not the scheduler; the same holds for a wait behind a
// failed upstream, whose end time precedes the wait and clamps the delay to zero.
func nodeSchedulingDelay(walk *scheduler.LineageWalkResult, node *scheduler.JobExecutionSummary) time.Duration {
	run := node.JobRunSummary
	if run.TaskStartTime == nil {
		return 0
	}
	upstream := latestFinishedUpstream(walk, node)
	if upstream == nil {
		return 0
	}
	delay := run.TaskStartTime.Sub(run.ScheduledAt)
	if upstreamEnd := upstream.JobRunSummary.GetActualEndTime(); upstreamEnd.After(run.ScheduledAt) {
		delay = run.TaskStartTime.Sub(*upstreamEnd)
	}
	if delay < 0 {
		return 0
	}
	return delay
}

// schedulerDelayBudget is the total scheduling wait a measured path of the given depth
// may accumulate before SCHEDULER_DELAY is recorded.
func (p *Publisher) schedulerDelayBudget(depth int) int64 {
	return int64(p.config.SchedulerDelayPerDepth.Seconds() * float64(depth))
}

// tierRuntimeDelay measures induced delay — observed task/hook runtime minus the historical
// baseline — along the wait chain, with a three-condition cascade:
//
//  1. any individual job on the path with induced delay over the threshold is its own
//     incident (one per job);
//  2. otherwise, a project whose flat sum over its jobs on the path exceeds the threshold
//     is the incident, keyed on the project's max-delay job;
//  3. when several projects qualify, only the max-delayed project is detected.
func (p *Publisher) tierRuntimeDelay(path []*scheduler.JobExecutionSummary, ev TargetEvaluation, impacted []scheduler.ImpactedJob) []scheduler.RootCauseDetection {
	inducedByProject := map[tenant.ProjectName]time.Duration{}
	var individuallyDelayed []jobDelay

	for _, node := range path {
		induced := inducedRuntimeDelay(node, ev.HistoricalByJob, ev.ReferenceTime)
		if induced <= 0 {
			continue
		}
		inducedByProject[node.ProjectName] += induced

		if induced > p.config.RuntimeDelayThreshold {
			individuallyDelayed = append(individuallyDelayed, jobDelay{node: node, induced: induced})
		}
	}

	detectedProjects := map[tenant.ProjectName]bool{}
	var detections []scheduler.RootCauseDetection
	for _, delayed := range individuallyDelayed {
		// condition 1: the job itself is slow enough to be the incident
		incident := nodeIncidentKey(delayed.node)
		detectedProjects[incident.ProjectName] = true
		detections = append(detections, scheduler.RootCauseDetection{
			IncidentKey: incident,
			Reason: scheduler.RootCauseReasonDetail{
				Reason:              scheduler.TierRuntimeDelay,
				Status:              scheduler.ReasonStatusActive,
				InducedDelaySeconds: int(delayed.induced.Seconds()),
				IsJobFinished:       delayed.node.JobRunSummary.IsFinished(),
			},
			ImpactedJobs: impacted,
		})
	}

	// conditions 2 and 3: among the projects with no individually-delayed job, take the one
	// with the largest flat-summed induced delay when it crosses the threshold; ties on
	// the total resolve to the lowest project name
	var maxProject tenant.ProjectName
	var maxTotal, maxJobDelay time.Duration
	var maxNode *scheduler.JobExecutionSummary
	projectNames := make([]tenant.ProjectName, 0, len(inducedByProject))
	for projectName := range inducedByProject {
		projectNames = append(projectNames, projectName)
	}
	sort.Slice(projectNames, func(i, j int) bool { return projectNames[i] < projectNames[j] })
	for _, projectName := range projectNames {
		total := inducedByProject[projectName]
		if detectedProjects[projectName] || total <= p.config.RuntimeDelayThreshold {
			continue
		}
		if maxNode == nil || total > maxTotal {
			maxProject, maxTotal = projectName, total
		}
	}
	if maxTotal > 0 {
		// key the project-level incident on the project's max-delay job on the path; the
		// reason's induced delay is the project total — that sum is what crossed the
		// threshold, not the representative job's own delay
		for _, delayed := range inducedByJobOnPath(path, ev, maxProject) {
			if maxNode == nil || delayed.induced > maxJobDelay {
				maxNode, maxJobDelay = delayed.node, delayed.induced
			}
		}
	}
	if maxNode != nil {
		detections = append(detections, scheduler.RootCauseDetection{
			IncidentKey: nodeIncidentKey(maxNode),
			Reason: scheduler.RootCauseReasonDetail{
				Reason:              scheduler.TierRuntimeDelay,
				Status:              scheduler.ReasonStatusActive,
				InducedDelaySeconds: int(maxTotal.Seconds()),
				IsJobFinished:       maxNode.JobRunSummary.IsFinished(),
			},
			ImpactedJobs: impacted,
		})
	}
	return detections
}

type jobDelay = struct {
	node    *scheduler.JobExecutionSummary
	induced time.Duration
}

// inducedRuntimeDelay is one job's induced runtime delay: observed task and hook runtime
// (end time, or the reference time for a still-running task) minus the historical baseline.
// Components are clamped at zero individually so a fast task cannot cancel out a slow hook.
func inducedRuntimeDelay(node *scheduler.JobExecutionSummary, historicalByJob map[scheduler.JobName]scheduler.JobHistoricalDuration, referenceTime time.Time) time.Duration {
	run := node.JobRunSummary
	if run.TaskStartTime == nil {
		return 0
	}
	baseline := historicalByJob[node.JobName]

	induced := time.Duration(0)
	if taskEnd := run.TaskEndTime; taskEnd != nil {
		induced += maxDuration(0, taskEnd.Sub(*run.TaskStartTime)-baseline.TaskDuration)
	} else {
		induced += maxDuration(0, referenceTime.Sub(*run.TaskStartTime)-baseline.TaskDuration)
	}
	if run.HookStartTime != nil {
		hookEnd := referenceTime
		if run.HookEndTime != nil {
			hookEnd = *run.HookEndTime
		}
		induced += maxDuration(0, hookEnd.Sub(*run.HookStartTime)-baseline.HookDuration)
	}
	return induced
}

func inducedByJobOnPath(path []*scheduler.JobExecutionSummary, ev TargetEvaluation, projectName tenant.ProjectName) []jobDelay {
	var delayed []jobDelay
	for _, node := range path {
		if node.ProjectName != projectName {
			continue
		}
		if induced := inducedRuntimeDelay(node, ev.HistoricalByJob, ev.ReferenceTime); induced > 0 {
			delayed = append(delayed, jobDelay{node: node, induced: induced})
		}
	}
	return delayed
}

func maxDuration(a, b time.Duration) time.Duration {
	if a > b {
		return a
	}
	return b
}

// nodeIncidentKey keys an incident on a lineage node's entity, normalizing the scheduled
// time so identical instants compare equal regardless of location or monotonic clock.
func nodeIncidentKey(node *scheduler.JobExecutionSummary) scheduler.SLABreachIncidentKey {
	return incidentKey(node.ProjectName, node.JobName, node.JobRunSummary.ScheduledAt)
}

func incidentKey(projectName tenant.ProjectName, jobName scheduler.JobName, scheduledAt time.Time) scheduler.SLABreachIncidentKey {
	return scheduler.SLABreachIncidentKey{
		ProjectName: projectName,
		JobName:     jobName,
		ScheduledAt: scheduledAt.Round(0).UTC(),
	}
}
