package scheduler

import (
	"sort"
	"time"

	"github.com/goto/optimus/core/tenant"
	"github.com/goto/optimus/internal/lib/window"
)

const (
	// MaxLineageDepth is a safeguard to avoid infinite recursion in case of unexpected cycles
	// generally we don't expect lineage to be deeper than 20 levels
	MaxLineageDepth = 25

	// DefaultMaxLineageNodes bounds how many runs a single lineage walk returns. Deduplication
	// already rules out the combinatorial blow-up of re-walking diamonds, so this only guards
	// against a genuinely enormous lineage - a hub job can reach a large share of the graph.
	// TODO: provisional. Set this from the lineage sizing measurements once they are in.
	DefaultMaxLineageNodes = 5000
)

type JobRunKey struct {
	JobName     JobName
	ScheduledAt time.Time
}

func NewJobRunKey(jobName JobName, scheduledAt time.Time) JobRunKey {
	return JobRunKey{JobName: jobName, ScheduledAt: scheduledAt.UTC()}
}

type LineageWalkOptions struct {
	MaxNodes int
	MaxDepth int
	// TopUpstreamsPerJob, when positive, follows only the N latest-finishing direct upstreams
	// of each job, all the way to the root. Zero follows every upstream.
	// For lineage evaluation of finished jobs, having a low number is best
	// so the walk only consider last finishing upstream for each level
	TopUpstreamsPerJob int
}

// LineageSummaryOptions is what a caller asks the lineage summary for. It differs from
// LineageWalkOptions in carrying the window, which prunes inside the resolver before any runs
// are fetched, rather than in the walk.
type LineageSummaryOptions struct {
	MaxNodes           int
	TopUpstreamsPerJob int
	WindowHours        int
}

// LineageWalkResult is the deduplicated set of runs reachable from a target, together with
// whether the node budget cut the walk short.
type LineageWalkResult struct {
	Nodes      []*JobExecutionSummary
	TotalNodes int
	Truncated  bool

	NodesByKey     map[JobRunKey]*JobExecutionSummary
	UpstreamsByKey map[JobRunKey][]JobRunKey
}

// GatingPath returns the chain of runs the target actually waited on: from the target, each
// step is the direct upstream that finished last. It follows real edges, so consecutive
// entries are always genuinely adjacent.
// The walk stops at the first step with no finished upstream. For a target whose lineage has
// completed that is the deepest run on the critical path; for one still in flight it is the
// point where the lineage is currently blocked.
func (r *LineageWalkResult) GatingPath() []*JobExecutionSummary {
	if len(r.Nodes) == 0 {
		return nil
	}

	// Nodes is ordered by level, so the first entry is the target itself
	current := r.Nodes[0]
	path := []*JobExecutionSummary{current}
	visited := map[JobRunKey]bool{current.Key(): true}

	for {
		var latest *JobExecutionSummary
		for _, upstreamKey := range r.UpstreamsByKey[current.Key()] {
			upstream, ok := r.NodesByKey[upstreamKey]
			if !ok || visited[upstreamKey] || !upstream.JobRunSummary.IsFinished() {
				continue
			}
			if latest == nil || isLaterFinisher(upstream, latest) {
				latest = upstream
			}
		}

		if latest == nil {
			return path
		}

		visited[latest.Key()] = true
		path = append(path, latest)
		current = latest
	}
}

// isLaterFinisher orders two finished runs by end time, falling back to the job name so that
// upstreams finishing in the same instant do not reorder between requests.
func isLaterFinisher(candidate, best *JobExecutionSummary) bool {
	candidateEnd, bestEnd := candidate.JobRunSummary.GetActualEndTime(), best.JobRunSummary.GetActualEndTime()
	if !candidateEnd.Equal(*bestEnd) {
		return candidateEnd.After(*bestEnd)
	}

	return candidate.JobName < best.JobName
}

type JobSchedule struct {
	JobName     JobName
	ScheduledAt time.Time
}

type JobLineageSummary struct {
	JobName   JobName
	IsEnabled bool
	Upstreams []*JobLineageSummary

	Tenant           tenant.Tenant
	ScheduleInterval string
	SLA              SLAConfig
	Window           *window.Config

	// targetRun holds the root job's own run when this node is the lineage root.
	targetRun *JobRunSummary

	// runsByDownstream maps each immediate downstream run (JobName, ScheduledAt UTC)
	// to the upstream run it depends on, avoiding collisions when a downstream job
	// appears at multiple scheduled times in the same lineage.
	runsByDownstream map[JobRunKey]*JobRunSummary
}

// RecordTargetRun attaches the root job's own run summary to this node.
func (j *JobLineageSummary) RecordTargetRun(run *JobRunSummary) {
	if j == nil {
		return
	}
	j.targetRun = run
}

// GetTargetRun returns the run summary when this node is the lineage root.
func (j *JobLineageSummary) GetTargetRun() *JobRunSummary {
	if j == nil {
		return nil
	}
	if j.targetRun != nil {
		return j.targetRun
	}
	if run, exists := j.runsByDownstream[JobRunKey{JobName: j.JobName}]; exists {
		return run
	}
	return nil
}

// TargetRunKey returns the normalized UTC JobRunKey identifying this node when it is the lineage root.
func (j *JobLineageSummary) TargetRunKey() JobRunKey {
	if j == nil {
		return JobRunKey{}
	}
	if targetRun := j.GetTargetRun(); targetRun != nil {
		return NewJobRunKey(j.JobName, targetRun.ScheduledAt)
	}
	return NewJobRunKey(j.JobName, time.Time{})
}

// RecordUpstreamRun records the run summary of this upstream node depended on by downstream.
func (j *JobLineageSummary) RecordUpstreamRun(downstream JobRunKey, run *JobRunSummary) {
	if j == nil {
		return
	}
	if j.runsByDownstream == nil {
		j.runsByDownstream = make(map[JobRunKey]*JobRunSummary)
	}
	j.runsByDownstream[NewJobRunKey(downstream.JobName, downstream.ScheduledAt)] = run
}

// GetRunForDownstream returns the upstream run associated with the exact immediate downstream run.
func (j *JobLineageSummary) GetRunForDownstream(downstream JobRunKey) *JobRunSummary {
	if j == nil || len(j.runsByDownstream) == 0 {
		return nil
	}
	return j.runsByDownstream[NewJobRunKey(downstream.JobName, downstream.ScheduledAt)]
}

// ResolveUpstreamRun returns the upstream run associated with the immediate downstream run.
func (j *JobLineageSummary) ResolveUpstreamRun(downstream JobRunKey) *JobRunSummary {
	return j.GetRunForDownstream(downstream)
}

// DownstreamRunCount returns the number of distinct immediate downstream run edges recorded on this node.
func (j *JobLineageSummary) DownstreamRunCount() int {
	if j == nil {
		return 0
	}
	return len(j.runsByDownstream)
}

// HasRuns reports whether any run summary has been attached to this node.
func (j *JobLineageSummary) HasRuns() bool {
	if j == nil {
		return false
	}
	return j.targetRun != nil || len(j.runsByDownstream) > 0
}

// AllRuns returns every distinct run summary attached to this node, sorted by ScheduledAt.
func (j *JobLineageSummary) AllRuns() []*JobRunSummary {
	if j == nil {
		return nil
	}
	seen := make(map[time.Time]*JobRunSummary)
	j.ForEachRun(func(run *JobRunSummary) {
		key := run.ScheduledAt.UTC()
		if _, exists := seen[key]; !exists {
			seen[key] = run
		}
	})
	runs := make([]*JobRunSummary, 0, len(seen))
	for _, run := range seen {
		runs = append(runs, run)
	}
	sort.Slice(runs, func(i, k int) bool {
		return runs[i].ScheduledAt.Before(runs[k].ScheduledAt)
	})
	return runs
}

// ForEachRun visits every non-nil run summary stored on this node at most once per pointer.
func (j *JobLineageSummary) ForEachRun(fn func(*JobRunSummary)) {
	if j == nil || fn == nil {
		return
	}
	visited := make(map[*JobRunSummary]struct{})
	visit := func(run *JobRunSummary) {
		if run == nil {
			return
		}
		if _, seen := visited[run]; seen {
			return
		}
		visited[run] = struct{}{}
		fn(run)
	}
	visit(j.targetRun)
	for _, run := range j.runsByDownstream {
		visit(run)
	}
}

// CloneMetadata returns a shallow structural copy of this node with empty run maps and pre-sized Upstreams.
func (j *JobLineageSummary) CloneMetadata() *JobLineageSummary {
	if j == nil {
		return nil
	}
	return &JobLineageSummary{
		JobName:          j.JobName,
		Tenant:           j.Tenant,
		IsEnabled:        j.IsEnabled,
		Window:           j.Window,
		ScheduleInterval: j.ScheduleInterval,
		SLA:              j.SLA,
		Upstreams:        make([]*JobLineageSummary, len(j.Upstreams)),
	}
}

// RebindRuns populates this node's run references from src, substituting enriched run details
// keyed by ScheduledAt UTC when jobRunsBySchedule is non-nil.
func (j *JobLineageSummary) RebindRuns(src *JobLineageSummary, jobRunsBySchedule map[time.Time]*JobRunSummary) {
	if j == nil || src == nil {
		return
	}
	if jobRunsBySchedule == nil {
		j.targetRun = src.targetRun
		if len(src.runsByDownstream) > 0 {
			j.runsByDownstream = make(map[JobRunKey]*JobRunSummary, len(src.runsByDownstream))
			for k, v := range src.runsByDownstream {
				j.runsByDownstream[k] = v
			}
		}
		return
	}

	if src.targetRun != nil {
		if detailed, ok := jobRunsBySchedule[src.targetRun.ScheduledAt.UTC()]; ok {
			j.targetRun = detailed
		}
	}
	if len(src.runsByDownstream) > 0 {
		j.runsByDownstream = make(map[JobRunKey]*JobRunSummary, len(src.runsByDownstream))
		for downstreamKey, jobRun := range src.runsByDownstream {
			if detailed, ok := jobRunsBySchedule[jobRun.ScheduledAt.UTC()]; ok {
				j.runsByDownstream[downstreamKey] = detailed
			}
		}
	}
}

// ClipLineageRunsToReferenceTime walks the lineage and hides any observed run timestamp that
// falls at or after referenceTime
func ClipLineageRunsToReferenceTime(lineages map[JobName]*JobLineageSummary, referenceTime time.Time) {
	visited := map[*JobLineageSummary]bool{}
	for _, lineage := range lineages {
		clipLineageNode(lineage, referenceTime, visited)
	}
}

func clipLineageNode(node *JobLineageSummary, referenceTime time.Time, visited map[*JobLineageSummary]bool) {
	if node == nil || visited[node] {
		return
	}
	visited[node] = true

	node.ForEachRun(func(run *JobRunSummary) {
		clipJobRunToReferenceTime(run, referenceTime)
	})
	for _, upstream := range node.Upstreams {
		clipLineageNode(upstream, referenceTime, visited)
	}
}

func clipJobRunToReferenceTime(run *JobRunSummary, referenceTime time.Time) {
	if run == nil {
		return
	}

	clipped := false
	for _, field := range []**time.Time{
		&run.JobStartTime, &run.JobEndTime, &run.WaitStartTime, &run.WaitEndTime,
		&run.TaskStartTime, &run.TaskEndTime, &run.HookStartTime, &run.HookEndTime,
	} {
		if *field != nil && !(*field).Before(referenceTime) {
			*field = nil
			clipped = true
		}
	}

	if clipped {
		run.JobStatus = ""
	}
}

func (j *JobLineageSummary) GenerateLineageExecutionSummary(opts LineageWalkOptions) *JobRunLineage {
	if j == nil {
		return nil
	}

	walk := j.GetLineageNodes(opts)
	executionSummaries := walk.Nodes
	lineageSummary := &LineageExecutionSummary{}
	var largestScheduledWayTooLate, largestSystemSchedulingDelay *JobExecutionSummary
	var largestWayTooLateUpstream, largestSystemSchedulingUpstream *JobRunSummary

	var taskDurationJobs, hookDurationJobs []JobWithTaskDuration

	// the delay attribution below reads each entry's successor as the upstream that gated it,
	// so the chain has to follow real edges. Picking the latest finisher per level instead
	// would pair runs off neighbouring branches that never waited on each other.
	gatingPath := walk.GatingPath()

	for _, exec := range executionSummaries {
		currentRun := exec.JobRunSummary

		if !currentRun.IsFinished() {
			continue
		}

		if currentRun.TaskEndTime != nil && currentRun.TaskStartTime != nil {
			taskDurationJobs = append(taskDurationJobs, JobWithTaskDuration{
				JobName:      currentRun.JobName,
				ScheduledAt:  currentRun.ScheduledAt,
				TaskDuration: currentRun.GetTaskDuration(),
				Level:        exec.Level,
			})
		}

		if currentRun.HookEndTime != nil && currentRun.HookStartTime != nil {
			hookDurationJobs = append(hookDurationJobs, JobWithTaskDuration{
				JobName:      currentRun.JobName,
				ScheduledAt:  currentRun.ScheduledAt,
				TaskDuration: currentRun.GetHookDuration(),
				Level:        exec.Level,
			})
		}
	}

	for i := range gatingPath {
		currentExec := gatingPath[i]
		currentRun := currentExec.JobRunSummary

		if currentRun.TaskStartTime == nil {
			continue
		}

		scheduledToTaskStartDuration := currentRun.TaskStartTime.Sub(currentRun.ScheduledAt)

		var upstreamRun *JobRunSummary
		hasUpstream := i < len(gatingPath)-1
		if hasUpstream {
			upstreamRun = gatingPath[i+1].JobRunSummary
		}

		// the run was scheduled way too late when it was scheduled after its gating upstream
		// had already finished: the gap between the upstream's end and this run's schedule is
		// schedule misalignment, not work in progress
		if hasUpstream {
			if upstreamEnd := upstreamRun.GetActualEndTime(); upstreamEnd != nil && currentRun.ScheduledAt.After(*upstreamEnd) {
				currentScheduledWayTooLate := currentRun.ScheduledAt.Sub(*upstreamEnd)
				currentExec.DelaySummary.ScheduledWayTooLateSeconds = int64(currentScheduledWayTooLate.Seconds())
				lineageSummary.TotalScheduledWayTooLateSeconds += currentExec.DelaySummary.ScheduledWayTooLateSeconds

				if largestScheduledWayTooLate == nil || currentScheduledWayTooLate.Seconds() > float64(largestScheduledWayTooLate.DelaySummary.ScheduledWayTooLateSeconds) {
					largestScheduledWayTooLate = currentExec
					largestWayTooLateUpstream = upstreamRun
				}
			}
		}

		// system scheduling delay is the wait the scheduler itself is responsible for: from
		// the later of the run's own schedule and its gating upstream's finish, up to when the
		// task actually started. Waiting on upstream work is deliberately excluded.
		schedulerDelay := scheduledToTaskStartDuration
		if hasUpstream {
			if upstreamEnd := upstreamRun.GetActualEndTime(); upstreamEnd != nil && upstreamEnd.After(currentRun.ScheduledAt) {
				schedulerDelay = currentRun.TaskStartTime.Sub(*upstreamEnd)
			}
		}
		if schedulerDelay < 0 {
			schedulerDelay = 0
		}
		currentExec.DelaySummary.SystemSchedulingDelaySeconds = int64(schedulerDelay.Seconds())

		lineageSummary.TotalSystemSchedulingDelaySeconds += currentExec.DelaySummary.SystemSchedulingDelaySeconds
		if largestSystemSchedulingDelay == nil || currentExec.DelaySummary.SystemSchedulingDelaySeconds > largestSystemSchedulingDelay.DelaySummary.SystemSchedulingDelaySeconds {
			largestSystemSchedulingDelay = currentExec
			largestSystemSchedulingUpstream = upstreamRun
		}
	}

	sort.Slice(taskDurationJobs, func(i, j int) bool {
		return taskDurationJobs[i].TaskDuration > taskDurationJobs[j].TaskDuration
	})
	if len(taskDurationJobs) > 3 {
		taskDurationJobs = taskDurationJobs[:3]
	}
	lineageSummary.TopLongestTaskDurationJobs = taskDurationJobs

	sort.Slice(hookDurationJobs, func(i, j int) bool {
		return hookDurationJobs[i].TaskDuration > hookDurationJobs[j].TaskDuration
	})
	if len(hookDurationJobs) > 3 {
		hookDurationJobs = hookDurationJobs[:3]
	}
	lineageSummary.TopLongestHookDurationJobs = hookDurationJobs

	if len(gatingPath) > 0 {
		lineageSummary.AverageSystemSchedulingDelaySeconds = lineageSummary.TotalSystemSchedulingDelaySeconds / int64(len(gatingPath))
	}

	lineageSummary.TotalLineageDelaySeconds = lineageSummary.TotalScheduledWayTooLateSeconds + lineageSummary.TotalSystemSchedulingDelaySeconds
	if len(gatingPath) > 0 {
		firstJob := gatingPath[0].JobRunSummary
		lastJob := gatingPath[len(gatingPath)-1].JobRunSummary
		if firstJob.GetActualEndTime() != nil && lastJob.TaskStartTime != nil {
			lineageSummary.TotalLineageDurationSeconds = int64(firstJob.GetActualEndTime().Sub(*lastJob.TaskStartTime).Seconds())
		}
	}

	if largestScheduledWayTooLate != nil {
		lineageSummary.LargestScheduledWayTooLateJob = LineageDelaySummary{
			JobName:       largestScheduledWayTooLate.JobName,
			ScheduledAt:   largestScheduledWayTooLate.JobRunSummary.ScheduledAt,
			DelayDuration: largestScheduledWayTooLate.DelaySummary.ScheduledWayTooLateSeconds,
		}
		if largestWayTooLateUpstream != nil {
			lineageSummary.LargestScheduledWayTooLateJob.UpstreamJobName = largestWayTooLateUpstream.JobName
			lineageSummary.LargestScheduledWayTooLateJob.UpstreamScheduledAt = largestWayTooLateUpstream.ScheduledAt
		}
	}

	if largestSystemSchedulingDelay != nil {
		lineageSummary.LargestSystemSchedulingDelayJob = LineageDelaySummary{
			JobName:       largestSystemSchedulingDelay.JobName,
			ScheduledAt:   largestSystemSchedulingDelay.JobRunSummary.ScheduledAt,
			DelayDuration: largestSystemSchedulingDelay.DelaySummary.SystemSchedulingDelaySeconds,
		}
		if largestSystemSchedulingUpstream != nil {
			lineageSummary.LargestSystemSchedulingDelayJob.UpstreamJobName = largestSystemSchedulingUpstream.JobName
			lineageSummary.LargestSystemSchedulingDelayJob.UpstreamScheduledAt = largestSystemSchedulingUpstream.ScheduledAt
		}
	}

	lineage := &JobRunLineage{
		JobName:          j.JobName,
		ProjectName:      j.Tenant.ProjectName(),
		JobRuns:          executionSummaries,
		ExecutionSummary: lineageSummary,
		TotalNodes:       walk.TotalNodes,
		Truncated:        walk.Truncated,
	}
	if targetRun := j.GetTargetRun(); targetRun != nil {
		lineage.ScheduledAt = targetRun.ScheduledAt
	}

	return lineage
}

// GetLineageNodes walks the lineage breadth-first and returns every reachable run exactly
// once, keyed by (job name, scheduled at) - the same pair the resolver dedups its traversal
// on. A job reached from several downstreams appears as one node carrying several
// DownstreamRefs rather than once per path, so a diamond-shaped lineage cannot blow the
// result up combinatorially.
//
// Nothing is dropped for being unfinished: a run that is still executing, or has not started
// at all, is returned with the state it is currently in. Breadth-first order means the first
// visit to a node is necessarily via a shortest path, so Level is correct when assigned and
// never needs revising.
func (j *JobLineageSummary) GetLineageNodes(opts LineageWalkOptions) *LineageWalkResult {
	if j == nil {
		return &LineageWalkResult{}
	}

	maxNodes, maxDepth := opts.MaxNodes, opts.MaxDepth
	if maxNodes <= 0 {
		maxNodes = DefaultMaxLineageNodes
	}
	if maxDepth <= 0 {
		maxDepth = MaxLineageDepth
	}

	nodesByKey := map[JobRunKey]*JobExecutionSummary{}
	upstreamsByKey := map[JobRunKey][]JobRunKey{}
	var nodes []*JobExecutionSummary
	truncated := false

	type queueItem struct {
		lineage *JobLineageSummary
		from    *JobRunKey
		depth   int
	}

	queue := []queueItem{{lineage: j, depth: 0}}
	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]

		var run *JobRunSummary
		if current.from == nil {
			run = current.lineage.GetTargetRun()
		} else {
			run = current.lineage.GetRunForDownstream(*current.from)
		}
		if run == nil {
			continue
		}
		key := NewJobRunKey(current.lineage.JobName, run.ScheduledAt)

		node, seen := nodesByKey[key]
		if !seen {
			if len(nodes) >= maxNodes {
				truncated = true
				continue
			}

			downstreamPathName := current.lineage.JobName.String()
			if current.from != nil {
				downstreamPathName = current.from.JobName.String()
			}
			projectName := current.lineage.Tenant.ProjectName()
			node = &JobExecutionSummary{
				JobName:            current.lineage.JobName,
				ProjectName:        projectName,
				SLA:                current.lineage.SLA,
				Level:              current.depth,
				JobRunSummary:      run,
				State:              run.GetState(),
				DownstreamPathName: downstreamPathName,
				DelaySummary:       &JobRunDelaySummary{},
			}
			nodesByKey[key] = node
			nodes = append(nodes, node)
		}

		// the edge is recorded whether or not the node is new, so that a shared upstream
		// keeps every downstream that depends on it
		if current.from != nil {
			node.DownstreamRefs = append(node.DownstreamRefs, *current.from)
			upstreamsByKey[*current.from] = append(upstreamsByKey[*current.from], key)
		}

		if seen || current.depth >= maxDepth {
			continue
		}

		for _, upstream := range SelectUpstreamsForRun(current.lineage, key, opts.TopUpstreamsPerJob) {
			queue = append(queue, queueItem{
				lineage: upstream,
				from:    &key,
				depth:   current.depth + 1,
			})
		}
	}

	markBlockingNodes(nodes, nodesByKey, upstreamsByKey)
	sortLineageNodes(nodes)

	return &LineageWalkResult{
		Nodes:          nodes,
		TotalNodes:     len(nodes),
		Truncated:      truncated,
		NodesByKey:     nodesByKey,
		UpstreamsByKey: upstreamsByKey,
	}
}

// SelectUpstreams returns the upstreams of job to follow. With topN at zero that is all of
// them; otherwise it is the N that finished last, which for a completed lineage are the runs
// that actually held the job up
func SelectUpstreams(job *JobLineageSummary, topN int) []*JobLineageSummary {
	return SelectUpstreamsForRun(job, job.TargetRunKey(), topN)
}

// SelectUpstreamsForRun ranks job's upstreams for the specific downstream run key.
func SelectUpstreamsForRun(job *JobLineageSummary, parentKey JobRunKey, topN int) []*JobLineageSummary {
	if job == nil || topN <= 0 || len(job.Upstreams) <= topN {
		if job == nil {
			return nil
		}
		return job.Upstreams
	}

	ranked := make([]*JobLineageSummary, len(job.Upstreams))
	copy(ranked, job.Upstreams)

	sort.SliceStable(ranked, func(i, k int) bool {
		iRun, kRun := ranked[i].GetRunForDownstream(parentKey), ranked[k].GetRunForDownstream(parentKey)

		// runs with no finish time cannot be ranked on one, so they sort after those that have
		// one, newest first
		iEnd, kEnd := finishTimeForRanking(iRun), finishTimeForRanking(kRun)
		switch {
		case iEnd == nil && kEnd == nil:
			return ranked[i].JobName < ranked[k].JobName
		case iEnd == nil:
			return false
		case kEnd == nil:
			return true
		case !iEnd.Equal(*kEnd):
			return iEnd.After(*kEnd)
		default:
			return ranked[i].JobName < ranked[k].JobName
		}
	})

	return ranked[:topN]
}

func finishTimeForRanking(run *JobRunSummary) *time.Time {
	if !run.IsFinished() {
		return nil
	}

	return run.GetActualEndTime()
}

// markBlockingNodes flags the runs the lineage is currently waiting on: those that have not
// finished and whose own upstreams have all finished. An unfinished run that is itself
// waiting on an unfinished upstream is blocked, not blocking.
//
// When the walk was truncated the upstream edges of dropped nodes are unknown, so a node on
// the truncation boundary may be flagged as blocking without being so.
func markBlockingNodes(nodes []*JobExecutionSummary, nodesByKey map[JobRunKey]*JobExecutionSummary, upstreamsByKey map[JobRunKey][]JobRunKey) {
	for _, node := range nodes {
		if node.JobRunSummary.IsFinished() {
			continue
		}

		node.IsBlocking = true
		for _, upstreamKey := range upstreamsByKey[node.Key()] {
			if upstream, ok := nodesByKey[upstreamKey]; ok && !upstream.JobRunSummary.IsFinished() {
				node.IsBlocking = false
				break
			}
		}
	}
}

// sortLineageNodes orders the walk for presentation only - it no longer decides what the
// traversal reaches. Nodes are grouped by distance from the target, then by how much
// attention the run warrants, then by the earliest one to have got going.
func sortLineageNodes(nodes []*JobExecutionSummary) {
	sort.SliceStable(nodes, func(i, k int) bool {
		if nodes[i].Level != nodes[k].Level {
			return nodes[i].Level < nodes[k].Level
		}
		if statePriority(nodes[i].State) != statePriority(nodes[k].State) {
			return statePriority(nodes[i].State) < statePriority(nodes[k].State)
		}
		iStart, kStart := nodes[i].JobRunSummary.effectiveStartTime(), nodes[k].JobRunSummary.effectiveStartTime()
		if !iStart.Equal(kStart) {
			return iStart.Before(kStart)
		}
		return nodes[i].JobName < nodes[k].JobName
	})
}

func statePriority(state State) int {
	switch state {
	case StateFailed:
		return 0
	case StateRunning:
		return 1
	case StateWaitUpstream:
		return 2
	case StateNotScheduled:
		return 3
	default:
		return 4
	}
}

type JobRunLineage struct {
	JobName          JobName
	ProjectName      tenant.ProjectName
	ScheduledAt      time.Time
	JobRuns          []*JobExecutionSummary
	ExecutionSummary *LineageExecutionSummary
	// TotalNodes is how many runs the walk returned, and Truncated whether the node budget
	// stopped it short of the full lineage.
	TotalNodes int
	Truncated  bool
}

type LineageExecutionSummary struct {
	TotalScheduledWayTooLateSeconds     int64
	TotalSystemSchedulingDelaySeconds   int64
	AverageSystemSchedulingDelaySeconds int64

	TotalLineageDelaySeconds    int64
	TotalLineageDurationSeconds int64

	LargestScheduledWayTooLateJob   LineageDelaySummary
	LargestSystemSchedulingDelayJob LineageDelaySummary

	TopLongestTaskDurationJobs []JobWithTaskDuration
	TopLongestHookDurationJobs []JobWithTaskDuration
}

type JobWithTaskDuration struct {
	JobName      JobName
	ScheduledAt  time.Time
	TaskDuration time.Duration
	Level        int
}

type LineageDelaySummary struct {
	JobName             JobName
	ScheduledAt         time.Time
	UpstreamJobName     JobName
	UpstreamScheduledAt time.Time
	DelayDuration       int64
}

// JobExecutionSummary is a flattened version of JobLineageSummary
type JobExecutionSummary struct {
	JobName     JobName
	ProjectName tenant.ProjectName
	SLA         SLAConfig
	// Level marks the shortest distance from the original job in question
	Level         int
	JobRunSummary *JobRunSummary
	// DownstreamPathName is the job this run was first reached through. Kept for the existing
	// response field; DownstreamRefs carries the full picture for a shared upstream.
	DownstreamPathName string
	// DownstreamRefs are the runs that depend on this one - the lineage's edges.
	DownstreamRefs []JobRunKey
	// State is the run's current position, see JobRunSummary.GetState.
	State State
	// IsBlocking marks a run the lineage is actively waiting on, see markBlockingNodes.
	IsBlocking        bool
	DelaySummary      *JobRunDelaySummary
	HistoricalSummary JobHistoricalDuration
}

func (j *JobExecutionSummary) Key() JobRunKey {
	return NewJobRunKey(j.JobName, j.JobRunSummary.ScheduledAt)
}

type JobHistoricalDuration struct {
	TaskDuration time.Duration
	HookDuration time.Duration
}

type SLAConfig struct {
	Duration time.Duration
}

type JobRunSummary struct {
	JobName     JobName
	ScheduledAt time.Time
	SLATime     *time.Time

	JobStartTime  *time.Time
	JobEndTime    *time.Time
	JobStatus     string
	WaitStartTime *time.Time
	WaitEndTime   *time.Time
	TaskStartTime *time.Time
	TaskEndTime   *time.Time
	HookStartTime *time.Time
	HookEndTime   *time.Time

	SensorName   *string
	TaskName     *string
	HookName     *string
	TaskAttempts int
	HookAttempts int
}

func (j *JobRunSummary) GetActualEndTime() *time.Time {
	if j.HookEndTime != nil {
		return j.HookEndTime
	}
	return j.TaskEndTime
}

func (j *JobRunSummary) IsFinished() bool {
	if j == nil {
		return false
	}

	if j.HookStartTime != nil && j.HookEndTime == nil {
		return false
	}

	return j.GetActualEndTime() != nil
}

func (j *JobRunSummary) GetState() State {
	if j == nil || j.JobStatus == "" {
		return StateNotScheduled
	}

	if j.IsFinished() {
		if state, err := StateFromString(j.JobStatus); err == nil && state.IsTerminal() {
			return state
		}
		return StateSuccess
	}

	switch {
	case j.TaskStartTime != nil:
		return StateRunning
	case j.WaitStartTime != nil:
		return StateWaitUpstream
	default:
		return StateNotScheduled
	}
}

// effectiveStartTime is when the run last got going, falling back through the operators to
// its schedule for a run that has not started at all. Used for ordering only.
func (j *JobRunSummary) effectiveStartTime() time.Time {
	switch {
	case j.TaskStartTime != nil:
		return *j.TaskStartTime
	case j.WaitStartTime != nil:
		return *j.WaitStartTime
	default:
		return j.ScheduledAt
	}
}

func (j *JobRunSummary) GetTaskDuration() time.Duration {
	if j.TaskStartTime == nil || j.TaskEndTime == nil {
		return 0
	}

	return j.TaskEndTime.Sub(*j.TaskStartTime)
}

func (j *JobRunSummary) GetHookDuration() time.Duration {
	if j.HookStartTime == nil || j.HookEndTime == nil {
		return 0
	}

	return j.HookEndTime.Sub(*j.HookStartTime)
}

type JobRunDelaySummary struct {
	ScheduledWayTooLateSeconds   int64
	SystemSchedulingDelaySeconds int64
}
