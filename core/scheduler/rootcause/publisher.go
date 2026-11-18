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
	// HistoricalByJob holds the historical task+hook runtime baseline per job reachable
	// from the target
	HistoricalByJob map[scheduler.JobName]time.Duration
	// Finding is the shared inferred-SLA breach analysis of the target; the caller gates
	// on it (at least one breach origin) before calling Evaluate.
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

	// the caller gates on the Finding: Evaluate expects at least one breach origin
	// (potential-first) and reports no detection otherwise

	impacted := []scheduler.ImpactedJob{{
		ProjectName: ev.Target.Tenant.ProjectName(),
		JobName:     ev.Target.JobName,
		ScheduledAt: targetRun.ScheduledAt,
	}}

	// the cause chain covers what the inferred-breach leaves are actually waiting on;
	// scheduler delay is measured in the next step over the same chain
	chain := p.causeChain(walk, ev.Finding)

	detections := p.tierFailedUpstream(walk, impacted)
	detections = append(detections, p.tierWaitingUpstream(walk, ev.Finding, ev.ReferenceTime, impacted)...)
	detections = append(detections, p.tierThirdPartyDelay(chain, ev.ReferenceTime, impacted)...)
	detections = append(detections, p.tierSchedulerDelay(walk, ev.Finding, ev.ReferenceTime, impacted)...)
	detections = append(detections, p.tierRuntimeDelay(chain, ev, impacted)...)

	sources := breachSourcesEvidence(ev)

	// if nothing explains the breach, the SLA is too aggressive for what the lineage
	// delivers — but only when the target is past its SLA and is itself the only breach
	// origin: a potential miss with no cause says nothing about the SLA yet, and an
	// origin elsewhere would have been detected above
	if len(detections) == 0 {
		if !breachedSLA(targetRun, ev.ReferenceTime) || len(ev.Finding.Origins) != 1 || ev.Finding.Origins[0].JobName != ev.Target.JobName {
			return nil
		}
		detections = []scheduler.RootCauseDetection{{
			IncidentKey: incidentKey(targetNode.ProjectName, targetNode.JobName, targetRun.ScheduledAt),
			Reason: scheduler.RootCauseReasonDetail{
				Reason:   scheduler.TierAggressiveSLA,
				Status:   scheduler.ReasonStatusActive,
				Evidence: aggressiveSLAEvidence(targetNode, targetRun, ev),
			},
			ImpactedJobs: impacted,
		}}
	}
	attachBreachSources(detections, sources, ev.ReferenceTime)
	return detections
}

// breachSourcesEvidence records the inferred-SLA breach sources of the impacted target
// behind this detection: which job is breaching its inferred SLA, around what time it
// crossed (its inferred deadline, or its latest safe start when it never started), at
// which level relative to the target, and the source→target chain for level-by-level
// tracing.
func breachSourcesEvidence(ev TargetEvaluation) []scheduler.EvidenceBreachSource {
	finding := ev.Finding
	if finding == nil || len(finding.Origins) == 0 {
		return nil
	}
	sources := make([]scheduler.EvidenceBreachSource, 0, len(finding.Origins))
	for i, state := range finding.Origins {
		source := scheduler.EvidenceBreachSource{
			Project:     state.Tenant.ProjectName().String(),
			Job:         state.JobName.String(),
			ScheduledAt: scheduler.TimePtr(state.JobRun.ScheduledAt),
			Level:       finding.Bottleneck.Level[state.JobName],
			Status:      string(state.Status),
			InferredSLA: state.InferredSLA,
		}
		if state.Status == scheduler.SLABreachCauseNotStarted {
			source.LatestSafeStart = state.Evidence.LatestSafeStartAt
		}
		if state.EstimatedDuration != nil {
			source.EstimatedDurationSeconds = scheduler.IntPtr(int(state.EstimatedDuration.Seconds()))
		}
		if i < len(finding.OriginPaths) && finding.OriginPaths[i] != nil {
			source.Chain = chainEvidence(finding, finding.OriginPaths[i])
		}
		sources = append(sources, source)
	}
	return sources
}

// chainEvidence reverses the target→origin path into an origin→target trace with each
// node's target-relative level and inferred SLA, keeping the origin side when capping.
func chainEvidence(finding *BreachFinding, path []*scheduler.JobState) []scheduler.EvidenceChainNode {
	chain := make([]scheduler.EvidenceChainNode, 0, len(path))
	for i := len(path) - 1; i >= 0; i-- {
		state := path[i]
		if state == nil {
			continue
		}
		chain = append(chain, scheduler.EvidenceChainNode{
			Project:     state.Tenant.ProjectName().String(),
			Job:         state.JobName.String(),
			Level:       finding.Bottleneck.Level[state.JobName],
			InferredSLA: state.InferredSLA,
		})
	}
	if len(chain) > scheduler.MaxEvidenceComponents {
		chain = chain[:scheduler.MaxEvidenceComponents]
	}
	return chain
}

// attachBreachSources records the breach sources block on every detection's evidence,
// so each stored reason explains which job breached the inferred SLA, at which level,
// and around when.
func attachBreachSources(detections []scheduler.RootCauseDetection, sources []scheduler.EvidenceBreachSource, referenceTime time.Time) {
	for i := range detections {
		if detections[i].Reason.Evidence == nil {
			detections[i].Reason.Evidence = &scheduler.ReasonEvidence{
				ReferenceTime: referenceTime.UTC(),
			}
		}
		detections[i].Reason.Evidence.BreachSources = sources
	}
}

// aggressiveSLAEvidence records the facts behind an AGGRESSIVE_SLA finding: the target
// is past its SLA with no reason explanation, so the evidence shows the SLA, when the
// run ended (or how far it would have to run), and its own baseline for comparison.
func aggressiveSLAEvidence(targetNode *scheduler.JobExecutionSummary, targetRun *scheduler.JobRunSummary, ev TargetEvaluation) *scheduler.ReasonEvidence {
	endTime := ev.ReferenceTime
	endSource := scheduler.EvidenceSourceReferenceTime
	if actualEnd := targetRun.GetActualEndTime(); actualEnd != nil {
		endTime = *actualEnd
		endSource = scheduler.EvidenceSourceActualEnd
	}
	section := &scheduler.AggressiveSLAEvidence{
		TargetSLATime: *targetRun.SLATime,
		End:           scheduler.TimePtr(endTime),
		EndSource:     endSource,
		LateBySeconds: int(endTime.Sub(*targetRun.SLATime).Seconds()),
	}
	if targetRun.TaskStartTime != nil {
		section.TaskStart = targetRun.TaskStartTime
	}
	if baseline, found := ev.HistoricalByJob[targetNode.JobName]; found && baseline > 0 {
		section.BaselineSeconds = scheduler.IntPtr(int(baseline.Seconds()))
	}
	return &scheduler.ReasonEvidence{
		ReferenceTime: ev.ReferenceTime.UTC(),
		Rule:          scheduler.EvidenceRuleAggressiveSLANoCause,
		AggressiveSLA: section,
	}
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
func (p *Publisher) tierWaitingUpstream(walk *scheduler.LineageWalkResult, finding *BreachFinding, referenceTime time.Time, impacted []scheduler.ImpactedJob) []scheduler.RootCauseDetection {
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
		// the induced delay is how far past its latest safe start the node is — the same
		// wait-family measure as the other delay reasons; it grows while the node stays
		// stuck and freezes at the last observation once the node finally starts
		evidence, overdueSeconds := waitingUpstreamEvidence(walk, node, finding, referenceTime)
		detections = append(detections, scheduler.RootCauseDetection{
			IncidentKey: nodeIncidentKey(node),
			Reason: scheduler.RootCauseReasonDetail{
				Reason:              scheduler.TierWaitingUpstream,
				Status:              scheduler.ReasonStatusActive,
				InducedDelaySeconds: overdueSeconds,
				Evidence:            evidence,
			},
			ImpactedJobs: impacted,
		})
	}
	return detections
}

// waitingUpstreamEvidence records the facts behind a WAITING_UPSTREAM finding: why the
// node counts as stuck and how far past its latest safe start it is. The overdue time
// doubles as the reason's induced delay.
func waitingUpstreamEvidence(walk *scheduler.LineageWalkResult, node *scheduler.JobExecutionSummary, finding *BreachFinding, referenceTime time.Time) (*scheduler.ReasonEvidence, int) {
	section := &scheduler.WaitingUpstreamEvidence{NodeState: string(node.State)}
	overdueSeconds := 0
	if state := finding.BreachStates[node.JobName]; state != nil {
		if state.InferredSLA != nil {
			section.InferredSLA = state.InferredSLA
		}
		if state.EstimatedDuration != nil {
			section.EstimatedDurationSeconds = scheduler.IntPtr(int(state.EstimatedDuration.Seconds()))
		}
		if state.InferredSLA != nil && state.EstimatedDuration != nil {
			safeStart := state.InferredSLA.Add(-*state.EstimatedDuration)
			section.LatestSafeStart = scheduler.TimePtr(safeStart)
			overdueSeconds = int(referenceTime.Sub(safeStart).Seconds())
			if overdueSeconds < 0 {
				overdueSeconds = 0
			}
		}
	}
	// the rule fires when every upstream is successful or never created;
	// HasMissingUpstreamRun distinguishes the two cases so the reader knows
	// whether the node is waiting on an upstream run that was never created
	for _, upstreamKey := range walk.UpstreamsByKey[node.Key()] {
		if upstream, ok := walk.NodesByKey[upstreamKey]; ok && upstream.State == scheduler.StateNotScheduled {
			section.HasMissingUpstreamRun = true
			break
		}
	}
	return &scheduler.ReasonEvidence{
		ReferenceTime:   referenceTime.UTC(),
		Rule:            scheduler.EvidenceRuleWaitingUpstreamPastSafeStart,
		WaitingUpstream: section,
	}, overdueSeconds
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
		waitFromSource := scheduler.EvidenceSourceDailyBoundary
		if node.JobRunSummary.WaitStartTime != nil && node.JobRunSummary.WaitStartTime.After(boundary) {
			waitStart = *node.JobRunSummary.WaitStartTime
			waitFromSource = scheduler.EvidenceSourceSensorWaitStart
		}
		waitEnd := referenceTime
		endSource := scheduler.EvidenceSourceReferenceTime
		if node.JobRunSummary.WaitEndTime != nil {
			waitEnd = *node.JobRunSummary.WaitEndTime
			endSource = scheduler.EvidenceSourceWaitEnd
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
				Evidence: &scheduler.ReasonEvidence{
					ReferenceTime:    referenceTime.UTC(),
					Rule:             scheduler.EvidenceRuleThirdPartyWaitOverThreshold,
					ThresholdSeconds: int(p.config.ThirdPartyDelayThreshold.Seconds()),
					ThirdParty: &scheduler.ThirdPartyEvidence{
						Sensor:         *sensorName,
						Boundary:       boundary.UTC(),
						WaitFrom:       waitStart.UTC(),
						WaitFromSource: waitFromSource,
						End:            waitEnd.UTC(),
						EndSource:      endSource,
					},
				},
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
func (p *Publisher) tierSchedulerDelay(walk *scheduler.LineageWalkResult, finding *BreachFinding, referenceTime time.Time, impacted []scheduler.ImpactedJob) []scheduler.RootCauseDetection {
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
	components := make([]scheduler.SchedulerComponent, 0, len(measured))
	for _, node := range measured {
		obs := observeScheduling(walk, node)
		total += obs.Delay
		components = append(components, schedulingComponent(node, obs, finding))
	}
	budget := time.Duration(p.schedulerDelayBudget(len(measured))) * time.Second
	if total <= budget {
		return nil
	}
	capped, componentsTotal := scheduler.CapComponents(components)

	targetNode := walk.Nodes[0]
	return []scheduler.RootCauseDetection{{
		IncidentKey: incidentKey(targetNode.ProjectName, targetNode.JobName, targetNode.JobRunSummary.ScheduledAt),
		Reason: scheduler.RootCauseReasonDetail{
			Reason:              scheduler.TierSchedulerDelay,
			Status:              scheduler.ReasonStatusActive,
			InducedDelaySeconds: int(total.Seconds()),
			IsJobFinished:       targetNode.JobRunSummary.IsFinished(),
			Evidence: &scheduler.ReasonEvidence{
				ReferenceTime: referenceTime.UTC(),
				Rule:          scheduler.EvidenceRuleSchedulerPathOverBudget,
				// the path's allowance is the reason's threshold: the per-depth
				// minutes scaled by the number of measured nodes
				ThresholdSeconds: int(budget.Seconds()),
				Scheduler: &scheduler.SchedulerEvidence{
					CalculationComponentsTotal: componentsTotal,
					CalculationComponents:      capped,
				},
			},
		},
		ImpactedJobs: impacted,
	}}
}

// schedulingObservation carries the pieces behind one node's scheduler-owned wait, so the
// evidence can show which upstream end (or schedule) the wait was measured from.
type schedulingObservation struct {
	Delay          time.Duration
	Upstream       *scheduler.JobExecutionSummary
	UpstreamEnd    *time.Time
	WaitFrom       time.Time
	WaitFromSource string
}

// observeScheduling is one node's scheduler-owned wait: from the later of its own
// schedule and its latest-finished upstream's end until the task started. A node with no
// finished upstream contributes nothing — a wait with no completed dependency behind it
// belongs to the missing upstream, not the scheduler; the same holds for a wait behind a
// failed upstream, whose end time precedes the wait and clamps the delay to zero.
func observeScheduling(walk *scheduler.LineageWalkResult, node *scheduler.JobExecutionSummary) schedulingObservation {
	obs := schedulingObservation{}
	run := node.JobRunSummary
	if run.TaskStartTime == nil {
		return obs
	}
	obs.Upstream = latestFinishedUpstream(walk, node)
	if obs.Upstream == nil {
		return obs
	}
	obs.UpstreamEnd = obs.Upstream.JobRunSummary.GetActualEndTime()
	obs.WaitFrom = run.ScheduledAt
	obs.WaitFromSource = scheduler.EvidenceSourceScheduledAt
	if obs.UpstreamEnd != nil && obs.UpstreamEnd.After(run.ScheduledAt) {
		obs.WaitFrom = *obs.UpstreamEnd
		obs.WaitFromSource = scheduler.EvidenceSourceUpstreamEnd
	}
	obs.Delay = run.TaskStartTime.Sub(obs.WaitFrom)
	if obs.Delay < 0 {
		obs.Delay = 0
	}
	return obs
}

// schedulingComponent is one measured node's scheduler wait as an evidence component;
// zero-wait nodes are kept (they still count toward the depth budget).
func schedulingComponent(node *scheduler.JobExecutionSummary, obs schedulingObservation, finding *BreachFinding) scheduler.SchedulerComponent {
	component := scheduler.SchedulerComponent{
		ComponentBase: componentBase(node, int(obs.Delay.Seconds()), finding),
	}
	if node.JobRunSummary.TaskStartTime != nil {
		component.TaskStart = node.JobRunSummary.TaskStartTime
	}
	if obs.Upstream != nil {
		component.UpstreamJob = obs.Upstream.JobName.String()
		component.UpstreamEnd = obs.UpstreamEnd
		component.WaitFrom = scheduler.TimePtr(obs.WaitFrom)
		component.WaitFromSource = obs.WaitFromSource
	}
	return component
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
	observations := make(map[*scheduler.JobExecutionSummary]runtimeObservation, len(path))

	for _, node := range path {
		obs := observeRuntime(node, ev.HistoricalByJob, ev.ReferenceTime)
		observations[node] = obs
		if obs.Induced <= 0 {
			continue
		}
		inducedByProject[node.ProjectName] += obs.Induced

		if obs.Induced > p.config.RuntimeDelayThreshold {
			individuallyDelayed = append(individuallyDelayed, jobDelay{node: node, induced: obs.Induced})
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
				DelayScope:          scheduler.DelayScopeJob,
				Evidence:            runtimeJobEvidence(delayed.node, observations[delayed.node], ev.Finding, ev.ReferenceTime, p.config.RuntimeDelayThreshold),
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
				Reason:                     scheduler.TierRuntimeDelay,
				Status:                     scheduler.ReasonStatusActive,
				InducedDelaySeconds:        int(maxTotal.Seconds()),
				IsJobFinished:              maxNode.JobRunSummary.IsFinished(),
				DelayScope:                 scheduler.DelayScopeProject,
				MaxNodeInducedDelaySeconds: int(maxJobDelay.Seconds()),
				Evidence:                   runtimeProjectEvidence(path, observations, maxProject, maxNode, ev.Finding, ev.ReferenceTime, p.config.RuntimeDelayThreshold),
			},
			ImpactedJobs: impacted,
		})
	}
	return detections
}

// runtimeJobEvidence records a single-job runtime measurement: the observed runtime with
// its start/end sources and the baseline it was compared against.
func runtimeJobEvidence(node *scheduler.JobExecutionSummary, obs runtimeObservation, finding *BreachFinding, referenceTime time.Time, threshold time.Duration) *scheduler.ReasonEvidence {
	return &scheduler.ReasonEvidence{
		ReferenceTime:    referenceTime.UTC(),
		Rule:             scheduler.EvidenceRuleRuntimeJobOverThreshold,
		ThresholdSeconds: int(threshold.Seconds()),
		Runtime: &scheduler.RuntimeEvidence{
			CalculationComponentsTotal: 1,
			CalculationComponents:      []scheduler.RuntimeComponent{runtimeComponent(node, obs, finding)},
		},
	}
}

// runtimeProjectEvidence records a project-level runtime roll-up: every project job on
// the path that contributed to the sum, plus the jobs skipped for having no baseline.
func runtimeProjectEvidence(path []*scheduler.JobExecutionSummary, observations map[*scheduler.JobExecutionSummary]runtimeObservation, projectName tenant.ProjectName, maxNode *scheduler.JobExecutionSummary, finding *BreachFinding, referenceTime time.Time, threshold time.Duration) *scheduler.ReasonEvidence {
	var components []scheduler.RuntimeComponent
	var skippedNoBaseline []string
	for _, node := range path {
		if node.ProjectName != projectName {
			continue
		}
		obs := observations[node]
		if obs.TaskStart == nil {
			continue // never started: no runtime to measure
		}
		if !obs.BaselineFound {
			skippedNoBaseline = append(skippedNoBaseline, node.ProjectName.String()+"/"+node.JobName.String())
			continue
		}
		if obs.Induced <= 0 {
			continue // within its baseline: contributes nothing to the sum
		}
		components = append(components, runtimeComponent(node, obs, finding))
	}
	sort.Strings(skippedNoBaseline)
	if len(skippedNoBaseline) > scheduler.MaxEvidenceComponents {
		skippedNoBaseline = skippedNoBaseline[:scheduler.MaxEvidenceComponents]
	}
	capped, componentsTotal := scheduler.CapComponents(components)

	return &scheduler.ReasonEvidence{
		ReferenceTime:    referenceTime.UTC(),
		Rule:             scheduler.EvidenceRuleRuntimeProjectSumOverThreshold,
		ThresholdSeconds: int(threshold.Seconds()),
		Runtime: &scheduler.RuntimeEvidence{
			MaxNode:                    projectName.String() + "/" + maxNode.JobName.String(),
			CalculationComponentsTotal: componentsTotal,
			CalculationComponents:      capped,
			SkippedNoBaseline:          skippedNoBaseline,
		},
	}
}

// componentBase builds the shared part of a calculation component: the job, its
// contribution, and where it sits relative to the target — its level and whether the
// job is itself one of the breach sources.
func componentBase(node *scheduler.JobExecutionSummary, inducedSeconds int, finding *BreachFinding) scheduler.ComponentBase {
	base := scheduler.ComponentBase{
		Project:        node.ProjectName.String(),
		Job:            node.JobName.String(),
		InducedSeconds: inducedSeconds,
		ScheduledAt:    scheduler.TimePtr(node.JobRunSummary.ScheduledAt),
	}
	if finding == nil {
		return base
	}
	base.Level = finding.Bottleneck.Level[node.JobName]
	for _, state := range finding.Origins {
		if state.JobName == node.JobName {
			base.IsBreachSource = true
			break
		}
	}
	return base
}

// runtimeComponent is one job's runtime measurement as an evidence component.
func runtimeComponent(node *scheduler.JobExecutionSummary, obs runtimeObservation, finding *BreachFinding) scheduler.RuntimeComponent {
	return scheduler.RuntimeComponent{
		ComponentBase:   componentBase(node, int(obs.Induced.Seconds()), finding),
		TaskStart:       obs.TaskStart,
		End:             scheduler.TimePtr(obs.End),
		EndSource:       obs.EndSource,
		ObservedSeconds: scheduler.IntPtr(int(obs.Observed.Seconds())),
		BaselineSeconds: scheduler.IntPtr(int(obs.Baseline.Seconds())),
	}
}

type jobDelay = struct {
	node    *scheduler.JobExecutionSummary
	induced time.Duration
}

// runtimeObservation carries everything one job's runtime measurement used, that will to be evidence.
type runtimeObservation struct {
	TaskStart     *time.Time
	End           time.Time
	EndSource     string // "actual_end" | "reference_time"
	Observed      time.Duration
	Baseline      time.Duration
	BaselineFound bool
	Induced       time.Duration
}

// observeRuntime measures one job's runtime against its historical task+hook baseline.
//
// The SLA cares about the job's total completion, so a fast phase may offset a slow one.
// A job whose baseline is missing (or zero, which is unusable data) is skipped: no induced delay is attributed to it.
func observeRuntime(node *scheduler.JobExecutionSummary, historicalByJob map[scheduler.JobName]time.Duration, referenceTime time.Time) runtimeObservation {
	run := node.JobRunSummary
	obs := runtimeObservation{EndSource: scheduler.EvidenceSourceReferenceTime}
	if run.TaskStartTime == nil {
		return obs
	}
	obs.TaskStart = run.TaskStartTime
	obs.End = referenceTime
	if actualEnd := run.GetActualEndTime(); actualEnd != nil {
		obs.End = *actualEnd
		obs.EndSource = scheduler.EvidenceSourceActualEnd
	}
	obs.Observed = obs.End.Sub(*run.TaskStartTime)
	baseline, found := historicalByJob[node.JobName]
	obs.Baseline = baseline
	obs.BaselineFound = found && baseline > 0
	if !obs.BaselineFound {
		return obs
	}
	obs.Induced = obs.Observed - baseline
	if obs.Induced < 0 {
		obs.Induced = 0
	}
	return obs
}

// inducedRuntimeDelay is one job's induced runtime delay; see observeRuntime.
func inducedRuntimeDelay(node *scheduler.JobExecutionSummary, historicalByJob map[scheduler.JobName]time.Duration, referenceTime time.Time) time.Duration {
	return observeRuntime(node, historicalByJob, referenceTime).Induced
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
