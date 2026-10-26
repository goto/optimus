package rootcause

import (
	"time"

	"github.com/goto/optimus/core/scheduler"
	"github.com/goto/optimus/core/tenant"
)

// Publisher evaluates one target lineage per call and emits tier detections for the
// root-cause SLA breach engine (the Publisher of the Publisher-Subscriber alerting model).
// It is a pure function of the lineage walk: persistence, alerting and deduplication live
// one layer up.
//
// Every tier is evaluated independently, so one evaluation can yield detections for
// several tiers and several offending entities; GroupDetections merges them into incidents.
type PublisherConfig struct {
	// TaskHookDelayThreshold is the induced-delay floor for TierRuntimeDelay, both for an
	// individual job and for a project's flat sum along the bottleneck path.
	TaskHookDelayThreshold time.Duration
	// SchedulerDelayPerDepth is the per-lineage-level scheduler delay budget for
	// TierSchedulerDelay: a node whose system scheduling delay exceeds this times its level
	// (and whose upstreams all succeeded) flags the tier.
	SchedulerDelayPerDepth time.Duration
	// ThirdPartyDelayThreshold is the wait floor for TierThirdPartyDelay beyond the daily
	// 01:00 JKT boundary.
	ThirdPartyDelayThreshold time.Duration
	// ThirdPartyTypes are the upstream_resolvers types with wait_<type>_* sensors.
	ThirdPartyTypes []string
}

func (c PublisherConfig) withDefaults() PublisherConfig {
	if c.TaskHookDelayThreshold <= 0 {
		c.TaskHookDelayThreshold = 30 * time.Minute
	}
	if c.SchedulerDelayPerDepth <= 0 {
		c.SchedulerDelayPerDepth = 150 * time.Second // 2.5 minutes
	}
	if c.ThirdPartyDelayThreshold <= 0 {
		c.ThirdPartyDelayThreshold = 10 * time.Minute
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

// TargetEvaluation carries everything one target's evaluation needs.
type TargetEvaluation struct {
	// Target is the SLA-carrying lineage whose breach is being explained.
	Target *scheduler.JobLineageSummary
	// HistoricalByJob holds the historical task/hook baseline per job reachable from the
	// target; Tier 1 induced delay is measured against it. Missing entries count as zero
	// baseline (e.g. a job without history yet).
	HistoricalByJob map[scheduler.JobName]scheduler.JobHistoricalDuration
	// ReferenceTime is "now" for this evaluation tick.
	ReferenceTime time.Time
	// WalkOpts bound the lineage walk; zero values use the scheduler defaults.
	WalkOpts scheduler.LineageWalkOptions
}

// Evaluate runs the breach gate and the six tiers for one target. An empty result means the
// target did not breach or produced no explanation.
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

	// breach gate (the Tier 6 condition): the target missed its SLA deadline — finished
	// after it, or still running past it
	if !breachedSLA(targetRun, ev.ReferenceTime) {
		return nil
	}

	impacted := []scheduler.ImpactedJob{{
		ProjectName: ev.Target.Tenant.ProjectName(),
		JobName:     ev.Target.JobName,
		ScheduledAt: targetRun.ScheduledAt,
	}}

	// the gating path covers the finished portion of the wait chain; for an in-flight
	// lineage the chain continues through the blocking frontier and the finished upstreams
	// behind it, so the tiers below see the full chain the target is waiting on
	chain := p.waitChain(walk)

	// Tier 4: any ancestor in the whole lineage failed, of any scheduled date
	detections := p.tierFailedUpstream(walk, impacted)
	// Tier 5: a stuck node whose upstreams are all successful or missing entirely
	detections = append(detections, p.tierWaitingUpstream(walk, ev.ReferenceTime, impacted)...)
	// Tier 3: third-party sensor waits along the effective wait chain
	detections = append(detections, p.tierThirdPartyDelay(chain, ev.ReferenceTime, impacted)...)
	// Tier 2: the scheduler itself was slow to start a node whose upstreams all succeeded
	detections = append(detections, p.tierSchedulerDelay(walk, walk.GatingPath(), impacted)...)
	// Tier 1: induced task/hook runtime along the wait chain
	detections = append(detections, p.tierRuntimeDelay(chain, ev, impacted)...)

	// Tier 6: the target breached but nothing upstream explains it — the SLA is too
	// aggressive for what the lineage actually delivers
	if len(detections) == 0 {
		return []scheduler.RootCauseDetection{{
			IncidentKey:  incidentKey(targetNode.ProjectName, targetNode.JobName, targetRun.ScheduledAt),
			Reason:       scheduler.RootCauseReasonDetail{Reason: scheduler.TierAggressiveSLA, Status: scheduler.ReasonStatusActive},
			ImpactedJobs: impacted,
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
func (p *Publisher) waitChain(walk *scheduler.LineageWalkResult) []*scheduler.JobExecutionSummary {
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

// latestFinishedUpstream picks the upstream a run actually waited on among its finished
// upstreams, mirroring the gating path's selection rule.
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
func (p *Publisher) tierFailedUpstream(walk *scheduler.LineageWalkResult, impacted []scheduler.ImpactedJob) []scheduler.RootCauseDetection {
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
// finished, yet every resolved upstream is either successful or missing entirely (uncreated
// runs, depends_on_past self-waits). Nodes whose upstreams failed belong to Tier 4; nodes
// waiting on a still-running upstream are just normal waiting; a node stuck on its own
// pending third-party sensor is Tier 3's finding.
func (p *Publisher) tierWaitingUpstream(walk *scheduler.LineageWalkResult, referenceTime time.Time, impacted []scheduler.ImpactedJob) []scheduler.RootCauseDetection {
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
		// a mid-lineage node must be past its own SLA to count as stuck; the target is
		// already past the breach gate, so it needs no extra check
		if node.Level > 0 {
			run := node.JobRunSummary
			if run.SLATime == nil || !referenceTime.After(*run.SLATime) {
				continue
			}
		}
		detections = append(detections, scheduler.RootCauseDetection{
			IncidentKey:  nodeIncidentKey(node),
			Reason:       scheduler.RootCauseReasonDetail{Reason: scheduler.TierWaitingUpstream, Status: scheduler.ReasonStatusActive},
			ImpactedJobs: impacted,
		})
	}
	return detections
}

func isStuckState(node *scheduler.JobExecutionSummary) bool {
	return node.State == scheduler.StateWaitUpstream || node.State == scheduler.StateNotScheduled
}

// hasPendingThirdPartySensor reports whether the node is currently waiting on one of its
// own third-party sensors — a wait Tier 3 explains, so Tier 5 must not double-report it.
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
// the later of the sensor's wait start and the daily 01:00 JKT boundary.
func (p *Publisher) tierThirdPartyDelay(chain []*scheduler.JobExecutionSummary, referenceTime time.Time, impacted []scheduler.ImpactedJob) []scheduler.RootCauseDetection {
	if len(p.thirdPartyByPrefix) == 0 {
		return nil
	}

	boundary := thirdPartyDailyBoundary(referenceTime)
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
				// daily incident, so the streaming team gets a single alert per sensor
				ScheduledAt: boundary,
			},
			Reason: scheduler.RootCauseReasonDetail{
				Reason:              scheduler.TierThirdPartyDelay,
				Status:              scheduler.ReasonStatusActive,
				InducedDelaySeconds: int(wait.Seconds()),
				BlockedOnSensors:    []string{*sensorName},
				SourceType:          thirdPartyType,
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

// thirdPartyDailyBoundary is the 01:00 JKT instant of the reference time's JKT date, in
// UTC. Sensors lack a native scheduled_at, so this boundary doubles as the incident key's
// scheduled_at and as the floor of the measured wait.
func thirdPartyDailyBoundary(referenceTime time.Time) time.Time {
	jkt := time.FixedZone("JKT", 7*60*60)
	year, month, day := referenceTime.In(jkt).Date()
	return time.Date(year, month, day, 1, 0, 0, 0, jkt).UTC()
}

// tierSchedulerDelay attributes the scheduler itself for nodes on the gating path whose
// system scheduling delay exceeds the per-level budget, and whose upstreams all succeeded —
// a wait caused by a failed upstream is Tier 4's finding, not the scheduler's.
//
// The delay is computed here (not read from DelaySummary, which only GenerateLineage-
// ExecutionSummary fills) with the same formula the summary uses: the wait from the later
// of the node's schedule and its gating upstream's finish until the task start.
func (p *Publisher) tierSchedulerDelay(walk *scheduler.LineageWalkResult, path []*scheduler.JobExecutionSummary, impacted []scheduler.ImpactedJob) []scheduler.RootCauseDetection {
	var detections []scheduler.RootCauseDetection
	for i, node := range path {
		// the target itself has no gating upstream, so its wait is indistinguishable from
		// ordinary queueing; only lineage nodes carry the scheduler-delay signal
		if node.Level == 0 || node.JobRunSummary.TaskStartTime == nil {
			continue
		}
		if !upstreamsAllSuccess(walk, node) {
			continue
		}

		delay := node.JobRunSummary.TaskStartTime.Sub(node.JobRunSummary.ScheduledAt)
		if i < len(path)-1 {
			if upstreamEnd := path[i+1].JobRunSummary.GetActualEndTime(); upstreamEnd != nil && upstreamEnd.After(node.JobRunSummary.ScheduledAt) {
				delay = node.JobRunSummary.TaskStartTime.Sub(*upstreamEnd)
			}
		}
		if delay < 0 {
			delay = 0
		}
		if budget := p.schedulerDelayBudget(node.Level); int64(delay.Seconds()) <= budget {
			continue
		}
		detections = append(detections, scheduler.RootCauseDetection{
			IncidentKey: nodeIncidentKey(node),
			Reason: scheduler.RootCauseReasonDetail{
				Reason:              scheduler.TierSchedulerDelay,
				Status:              scheduler.ReasonStatusActive,
				InducedDelaySeconds: int(delay.Seconds()),
			},
			ImpactedJobs: impacted,
		})
	}
	return detections
}

func (p *Publisher) schedulerDelayBudget(level int) int64 {
	return int64(p.config.SchedulerDelayPerDepth.Seconds() * float64(level))
}

func upstreamsAllSuccess(walk *scheduler.LineageWalkResult, node *scheduler.JobExecutionSummary) bool {
	for _, upstreamKey := range walk.UpstreamsByKey[node.Key()] {
		upstream, ok := walk.NodesByKey[upstreamKey]
		if !ok || upstream.State != scheduler.StateSuccess {
			return false
		}
	}
	return true
}

// tierRuntimeDelay measures induced delay — observed task/hook runtime minus the historical
// baseline — along the bottleneck path, with the confirmed three-condition cascade:
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

		if induced > p.config.TaskHookDelayThreshold {
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
			},
			ImpactedJobs: impacted,
		})
	}

	// conditions 2 and 3: among the projects with no individually-delayed job, take the one
	// with the largest flat-summed induced delay when it crosses the threshold
	var maxProject tenant.ProjectName
	var maxTotal, maxJobDelay time.Duration
	var maxNode *scheduler.JobExecutionSummary
	for projectName, total := range inducedByProject {
		if detectedProjects[projectName] || total <= p.config.TaskHookDelayThreshold {
			continue
		}
		if total > maxTotal {
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
