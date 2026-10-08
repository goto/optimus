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

// JobLineageSummary is one job's node within a resolved lineage tree. Its upstream edges and
// runs are reachable only through methods (Upstreams, Runs, and Occurrence-based navigation),
// never as raw fields: a job reached via more than one downstream path (a diamond), or reached
// twice by the same downstream job on different days, can carry more than one run, and getting
// the keying wrong silently drops or conflates real data. Routing every access through the
// RunSet/Occurrence API keeps that keying scheme in one place instead of re-implemented ad hoc
// at every call site.
type JobLineageSummary struct {
	JobName   JobName
	IsEnabled bool

	Tenant           tenant.Tenant
	ScheduleInterval string
	SLA              SLAConfig
	Window           *window.Config

	upstreams []*JobLineageSummary
	runs      RunSet
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

	for _, entry := range node.Runs().Entries() {
		clipJobRunToReferenceTime(entry.Run, referenceTime)
	}
	for _, upstream := range node.Upstreams() {
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

// NewJobLineageSummary constructs a lineage node for jobName. Upstream edges and runs are added
// afterward via AddUpstream and RecordRun/RecordOwnRun.
func NewJobLineageSummary(jobName JobName, tnnt tenant.Tenant, scheduleInterval string, sla SLAConfig, jobWindow *window.Config, isEnabled bool) *JobLineageSummary {
	return &JobLineageSummary{
		JobName:          jobName,
		Tenant:           tnnt,
		ScheduleInterval: scheduleInterval,
		SLA:              sla,
		Window:           jobWindow,
		IsEnabled:        isEnabled,
	}
}

// AddUpstream records upstream as one of j's immediate upstream jobs in the static topology.
func (j *JobLineageSummary) AddUpstream(upstream *JobLineageSummary) {
	j.upstreams = append(j.upstreams, upstream)
}

// Upstreams returns j's immediate upstream jobs in the static topology - structure only, no run
// resolution. Used while a lineage is still being constructed, before any runs are known;
// consumers of an already-built lineage should navigate via Occurrence.Upstreams instead, which
// returns only the upstreams relevant to one specific run.
func (j *JobLineageSummary) Upstreams() []*JobLineageSummary {
	return j.upstreams
}

// RecordRun records run as the one required by the specific downstream run identified by parent.
func (j *JobLineageSummary) RecordRun(parent JobRunIdentifier, run *JobRunSummary) {
	j.runs.add(parent, run)
}

// RecordOwnRun records run as j's own run. Only meaningful for a lineage's root node, which has
// no downstream of its own and is therefore self-keyed.
func (j *JobLineageSummary) RecordOwnRun(run *JobRunSummary) {
	j.runs.add(JobRunIdentifier{JobName: j.JobName, ScheduledAt: run.ScheduledAt}, run)
}

// Runs returns j's set of recorded runs.
func (j *JobLineageSummary) Runs() RunSet {
	return j.runs
}

// RootOccurrence returns the lineage's single starting point - the one schedule being analyzed.
// Every traversal over a resolved lineage starts here.
func (j *JobLineageSummary) OccurrenceFor(id JobRunIdentifier) Occurrence {
	return Occurrence{node: j, run: j.runs.For(id)}
}
func (j *JobLineageSummary) RootOccurrence() Occurrence {
	return Occurrence{node: j, run: j.runs.Own()}
}

// RunEntry pairs a run with the specific downstream run that required it.
type RunEntry struct {
	Parent JobRunIdentifier
	Run    *JobRunSummary
}

// RunSet holds every distinct run of a job relevant to the current lineage analysis, keyed by
// the identifier of the specific downstream run that required it. A job reached via more than
// one downstream path, or reached twice by the same downstream job on two different days,
// carries more than one entry.
type RunSet struct {
	byParent map[JobRunIdentifier]*JobRunSummary
}

func (r *RunSet) add(parent JobRunIdentifier, run *JobRunSummary) {
	if r.byParent == nil {
		r.byParent = make(map[JobRunIdentifier]*JobRunSummary)
	}
	r.byParent[parent] = run
}

// For returns the run required by the specific downstream run identified by parent, or nil.
func (r RunSet) For(parent JobRunIdentifier) *JobRunSummary {
	return r.byParent[parent]
}

// Own returns the sole run in the set. It is only meaningful for a lineage's root node, which by
// construction always carries exactly one run (the single schedule being analyzed)
func (r RunSet) Own() *JobRunSummary {
	for _, run := range r.byParent {
		return run
	}
	return nil
}

// Len returns the number of distinct runs in the set.
func (r RunSet) Len() int {
	return len(r.byParent)
}

// Entries returns every (parent, run) pair in the set, in no particular order.
func (r RunSet) Entries() []RunEntry {
	entries := make([]RunEntry, 0, len(r.byParent))
	for parent, run := range r.byParent {
		entries = append(entries, RunEntry{Parent: parent, Run: run})
	}
	return entries
}

// SortedEntries returns Entries ordered by run ScheduledAt ascending and deduped by ScheduledAt,
// so a diamond-shared job with multiple candidate runs is checked deterministically, earliest
// (most likely overdue) first.
func (r RunSet) SortedEntries() []RunEntry {
	entries := r.Entries()
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Run.ScheduledAt.Before(entries[j].Run.ScheduledAt)
	})
	unique := entries[:0]
	for i, entry := range entries {
		if i == 0 || !entry.Run.ScheduledAt.Equal(entries[i-1].Run.ScheduledAt) {
			unique = append(unique, entry)
		}
	}
	return unique
}

// Occurrence identifies one specific real run within a resolved lineage tree: a job together
// with one of its runs. A job reached via more than one downstream path, or reached twice by the
// same downstream job on different days, corresponds to more than one Occurrence.
type Occurrence struct {
	node *JobLineageSummary
	run  *JobRunSummary
}

func (o Occurrence) JobName() JobName      { return o.node.JobName }
func (o Occurrence) Run() *JobRunSummary   { return o.run }
func (o Occurrence) IsEnabled() bool       { return o.node.IsEnabled }
func (o Occurrence) SLA() SLAConfig        { return o.node.SLA }
func (o Occurrence) Tenant() tenant.Tenant { return o.node.Tenant }

// Identifier returns the (job, scheduled time) identifier of this specific occurrence.
func (o Occurrence) Identifier() JobRunIdentifier {
	return JobRunIdentifier{JobName: o.node.JobName, ScheduledAt: o.run.ScheduledAt}
}

// LatestFinishTime returns the occurrence's actual finish time (hook end, falling back to task
// end), or nil if it hasn't finished yet.
func (o Occurrence) LatestFinishTime() *time.Time {
	return o.run.GetActualEndTime()
}

// Upstreams returns, for each of the job's upstream jobs, the specific occurrence that this
// occurrence required - already resolved, so callers never construct a JobRunIdentifier or
// touch a RunSet themselves. An upstream with no run recorded for this specific occurrence is
// omitted.
func (o Occurrence) Upstreams() []Occurrence {
	var result []Occurrence
	for _, upstream := range o.node.upstreams {
		if run := upstream.runs.For(o.Identifier()); run != nil {
			result = append(result, Occurrence{node: upstream, run: run})
		}
	}
	return result
}

// sortedUpstreams ranks o's upstream occurrences by how their run compares to o's own run.
// Upstreams that haven't finished yet are excluded, since there is nothing to rank them by.
// Among the rest, an upstream scheduled after o's own scheduled time sorts last; otherwise the
// latest-finishing upstream sorts first.
func (o Occurrence) sortedUpstreams() []Occurrence {
	var candidates []Occurrence
	for _, upstream := range o.Upstreams() {
		if upstream.LatestFinishTime() == nil {
			continue
		}
		candidates = append(candidates, upstream)
	}

	sort.Slice(candidates, func(i, j int) bool {
		iAfterCurrent := candidates[i].run.ScheduledAt.After(o.run.ScheduledAt)
		jAfterCurrent := candidates[j].run.ScheduledAt.After(o.run.ScheduledAt)
		if iAfterCurrent && !jAfterCurrent {
			return false
		}
		if !iAfterCurrent && jAfterCurrent {
			return true
		}

		return candidates[i].LatestFinishTime().After(*candidates[j].LatestFinishTime())
	})

	return candidates
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
	wayTooLateCount := 0

	var taskDurationJobs, hookDurationJobs []JobWithTaskDuration

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

		var upstreamLastTaskEndToCurrentTaskStartDuration time.Duration
		var upstreamRun *JobRunSummary
		hasUpstream := i < len(gatingPath)-1

		if hasUpstream {
			upstreamExec := gatingPath[i+1]
			upstreamRun := upstreamExec.JobRunSummary
			if upstreamRun.GetActualEndTime() != nil {
				upstreamLastTaskEndToCurrentTaskStartDuration = currentRun.TaskStartTime.Sub(*upstreamRun.GetActualEndTime())
			}
		}

		if hasUpstream && upstreamLastTaskEndToCurrentTaskStartDuration > scheduledToTaskStartDuration {
			currentScheduledWayTooLate := upstreamLastTaskEndToCurrentTaskStartDuration - scheduledToTaskStartDuration
			currentExec.DelaySummary.ScheduledWayTooLateSeconds = int64(currentScheduledWayTooLate.Seconds())
			lineageSummary.TotalScheduledWayTooLateSeconds += currentExec.DelaySummary.ScheduledWayTooLateSeconds
			wayTooLateCount++

			if largestScheduledWayTooLate == nil || currentScheduledWayTooLate.Seconds() > float64(largestScheduledWayTooLate.DelaySummary.ScheduledWayTooLateSeconds) {
				largestScheduledWayTooLate = currentExec
				largestWayTooLateUpstream = upstreamRun
			}

			currentExec.DelaySummary.SystemSchedulingDelaySeconds = int64(scheduledToTaskStartDuration.Seconds())
		} else if hasUpstream {
			currentExec.DelaySummary.SystemSchedulingDelaySeconds = int64(upstreamLastTaskEndToCurrentTaskStartDuration.Seconds())
		} else {
			currentExec.DelaySummary.SystemSchedulingDelaySeconds = int64(scheduledToTaskStartDuration.Seconds())
		}

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
		ScheduledAt:      j.RootOccurrence().Run().ScheduledAt,
		JobRuns:          executionSummaries,
		ExecutionSummary: lineageSummary,
		TotalNodes:       walk.TotalNodes,
		Truncated:        walk.Truncated,
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

	root := j.RootOccurrence()

	nodesByKey := map[JobRunKey]*JobExecutionSummary{}
	upstreamsByKey := map[JobRunKey][]JobRunKey{}
	var nodes []*JobExecutionSummary
	truncated := false

	type queueItem struct {
		occ    Occurrence
		from   *JobRunKey
		depth  int
	}

	queue := []queueItem{{occ: root, depth: 0}}
	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]

		run := current.occ.Run()
		if run == nil {
			continue
		}
		key := NewJobRunKey(current.occ.JobName(), run.ScheduledAt)

		node, seen := nodesByKey[key]
		if !seen {
			if len(nodes) >= maxNodes {
				truncated = true
				continue
			}

			downstreamPathName := current.occ.JobName().String()
			if current.from != nil {
				downstreamPathName = current.from.JobName.String()
			}
			node = &JobExecutionSummary{
				JobName:            current.occ.JobName(),
				SLA:                current.occ.SLA(),
				Level:              current.depth,
				JobRunSummary:      run,
				State:              run.GetState(),
				DownstreamPathName: downstreamPathName,
				DelaySummary:       &JobRunDelaySummary{},
			}
			nodesByKey[key] = node
			nodes = append(nodes, node)
		}

		if current.from != nil {
			node.DownstreamRefs = append(node.DownstreamRefs, *current.from)
			upstreamsByKey[*current.from] = append(upstreamsByKey[*current.from], key)
		}

		if seen || current.depth >= maxDepth {
			continue
		}

		for _, upstream := range SelectUpstreams(current.occ, opts.TopUpstreamsPerJob) {
			queue = append(queue, queueItem{
				occ:   upstream,
				from:  &key,
				depth: current.depth + 1,
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
func SelectUpstreams(occ Occurrence, topN int) []Occurrence {
	upstreams := occ.Upstreams()
	if topN <= 0 || len(upstreams) <= topN {
		return upstreams
	}

	ranked := make([]Occurrence, len(upstreams))
	copy(ranked, upstreams)

	sort.SliceStable(ranked, func(i, k int) bool {
		endI := ranked[i].LatestFinishTime()
		endK := ranked[k].LatestFinishTime()
		
		if endI == nil && endK == nil {
			return ranked[i].JobName() < ranked[k].JobName()
		}
		if endI == nil {
			return false
		}
		if endK == nil {
			return true
		}
		if !endI.Equal(*endK) {
			return endI.After(*endK)
		}
		return ranked[i].JobName() < ranked[k].JobName()
	})

	return ranked[:topN]
}
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
	JobName JobName
	SLA     SLAConfig
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

	SensorName *string
	TaskName   *string
	HookName   *string
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