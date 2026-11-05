package service

import (
	"context"
	"slices"
	"strings"
	"time"

	"github.com/goto/salt/log"

	"github.com/goto/optimus/config"
	"github.com/goto/optimus/core/scheduler"
	"github.com/goto/optimus/core/scheduler/rootcause"
)

// Contract that can be used by other callers to fetch job lineage information
type JobLineageFetcher interface {
	GetJobLineage(ctx context.Context, jobSchedules map[scheduler.JobName]*scheduler.JobSchedule, validLineageIntervalInHours int) (map[scheduler.JobName]*scheduler.JobLineageSummary, error)
}

type LineageBuilder interface {
	BuildLineage(context.Context, []*scheduler.JobSchedule, int) (map[*scheduler.JobSchedule]*scheduler.JobLineageSummary, error)
}

const DefaultLineageWindowHours = 24

type JobLineageService struct {
	l                            log.Logger
	lineageBuilder               LineageBuilder
	durationEstimator            DurationEstimatorRepo
	maxLineageDepth              int
	lineageWindowHours           int
	historicalDurationLastNRuns  int
	historicalDurationPercentile int
	// potential-breach prediction (see config.JobExecutionSummaryConfig.PotentialBreach)
	potentialBreachEnabled bool
	damperFactor           scheduler.DamperFactor
	now                    func() time.Time
}

// GetJobExecutionSummary returns the upstream lineage of each target run, deduplicated per
// (job, schedule).
//
// maxNodes caps how many runs a single lineage returns, keeping the ones closest to the
// target; zero asks for the whole lineage, bounded only by scheduler.DefaultMaxLineageNodes.
//
// windowHours leaves out any upstream run scheduled more than that many hours before the
// target's own schedule; zero uses the server's configured window.
func (j *JobLineageService) GetJobExecutionSummary(ctx context.Context, jobSchedules []*scheduler.JobSchedule, opts scheduler.LineageSummaryOptions) ([]*scheduler.JobRunLineage, error) {
	windowHours := opts.WindowHours
	if windowHours <= 0 {
		windowHours = j.lineageWindowHours
	}

	downstreamLineages, err := j.lineageBuilder.BuildLineage(ctx, jobSchedules, windowHours)
	if err != nil {
		j.l.Error("failed to get job lineage", "error", err)
		return nil, err
	}

	return j.generateLineageExecutionSummary(ctx, downstreamLineages, scheduler.LineageWalkOptions{
		MaxNodes:           opts.MaxNodes,
		MaxDepth:           j.maxLineageDepth,
		TopUpstreamsPerJob: opts.TopUpstreamsPerJob,
	})
}

func (j *JobLineageService) generateLineageExecutionSummary(ctx context.Context, lineagesMap map[*scheduler.JobSchedule]*scheduler.JobLineageSummary, walkOpts scheduler.LineageWalkOptions) ([]*scheduler.JobRunLineage, error) {
	type pair struct {
		tree    *scheduler.JobLineageSummary
		lineage *scheduler.JobRunLineage
	}

	var result []*scheduler.JobRunLineage
	pairs := make([]pair, 0, len(lineagesMap))
	for _, lineage := range lineagesMap {
		newDownstreamLineage := lineage
		jobRunLineage := newDownstreamLineage.GenerateLineageExecutionSummary(walkOpts)
		result = append(result, jobRunLineage)
		pairs = append(pairs, pair{tree: lineage, lineage: jobRunLineage})
	}

	if err := j.enrichWithHistoricalDurations(ctx, result); err != nil {
		j.l.Error("failed to enrich job run lineage with historical durations", "error", err)
		// prioritize returning the lineage information even if the enrichment fails
	}

	// the prediction runs after the enrichment: the duration estimates it needs are the
	// per-node historical baselines filled in above
	if j.potentialBreachEnabled {
		for _, p := range pairs {
			j.annotatePotentialBreaches(p.tree, p.lineage)
		}
	}

	return result, nil
}

type durationLookup struct {
	referenceTime time.Time
	hookName      string // empty for a task lookup
}

func (d durationLookup) operators() map[string][]string {
	if d.hookName == "" {
		return map[string][]string{"task": {}}
	}

	return map[string][]string{"hook": {d.hookName}}
}

// enrichWithHistoricalDurations fills in each run's historical task and hook durations for
// every lineage at once
func (j *JobLineageService) enrichWithHistoricalDurations(ctx context.Context, lineages []*scheduler.JobRunLineage) error {
	durationLookupMap := collectDurationLookups(lineages)
	if len(durationLookupMap) == 0 {
		return nil
	}

	var firstErr error
	fetched := make(map[durationLookup]map[scheduler.JobName]*time.Duration, len(durationLookupMap))
	for lookup, jobNames := range durationLookupMap {
		durations, err := j.durationEstimator.GetPercentileDurationByJobNames(ctx, jobNames, lookup.operators(),
			lookup.referenceTime, j.historicalDurationLastNRuns, j.historicalDurationPercentile)
		if err != nil {
			j.l.Error("failed to get historical durations",
				"hook", lookup.hookName, "reference_time", lookup.referenceTime, "error", err)
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		fetched[lookup] = durations
	}

	for _, lineage := range lineages {
		referenceTime := lineage.ScheduledAt.UTC()
		for _, run := range lineage.JobRuns {
			run.HistoricalSummary = scheduler.JobHistoricalDuration{}

			if duration, ok := fetched[durationLookup{referenceTime: referenceTime}][run.JobName]; ok && duration != nil {
				run.HistoricalSummary.TaskDuration = *duration
			}

			if run.JobRunSummary.HookName == nil {
				continue
			}
			hookLookup := durationLookup{referenceTime: referenceTime, hookName: *run.JobRunSummary.HookName}
			if duration, ok := fetched[hookLookup][run.JobName]; ok && duration != nil {
				run.HistoricalSummary.HookDuration = *duration
			}
		}
	}

	return firstErr
}

// collectDurationLookups gathers the unique job names each batch needs. Job names are sorted so
// that repeated requests issue identical queries.
func collectDurationLookups(lineages []*scheduler.JobRunLineage) map[durationLookup][]scheduler.JobName {
	seen := map[durationLookup]map[scheduler.JobName]struct{}{}
	add := func(lookup durationLookup, jobName scheduler.JobName) {
		if _, ok := seen[lookup]; !ok {
			seen[lookup] = map[scheduler.JobName]struct{}{}
		}
		seen[lookup][jobName] = struct{}{}
	}

	for _, lineage := range lineages {
		referenceTime := lineage.ScheduledAt.UTC()
		for _, run := range lineage.JobRuns {
			add(durationLookup{referenceTime: referenceTime}, run.JobName)
			if run.JobRunSummary.HookName != nil {
				add(durationLookup{referenceTime: referenceTime, hookName: *run.JobRunSummary.HookName}, run.JobName)
			}
		}
	}

	lookups := make(map[durationLookup][]scheduler.JobName, len(seen))
	for lookup, jobNameSet := range seen {
		jobNames := make([]scheduler.JobName, 0, len(jobNameSet))
		for jobName := range jobNameSet {
			jobNames = append(jobNames, jobName)
		}
		// sorting is here to avoid flakiness behavior only
		slices.Sort(jobNames)
		lookups[lookup] = jobNames
	}

	return lookups
}

func (j *JobLineageService) GetJobLineage(ctx context.Context, jobSchedules map[scheduler.JobName]*scheduler.JobSchedule, validLineageIntervalInHours int) (map[scheduler.JobName]*scheduler.JobLineageSummary, error) {
	lineageToJobName := make(map[scheduler.JobName]*scheduler.JobLineageSummary)
	schedules := make([]*scheduler.JobSchedule, 0, len(jobSchedules))
	for _, schedule := range jobSchedules {
		schedules = append(schedules, schedule)
	}
	jobLineages, err := j.lineageBuilder.BuildLineage(ctx, schedules, validLineageIntervalInHours)
	if err != nil {
		j.l.Error("failed to get job lineage", "error", err)
		return nil, err
	}

	for _, lineage := range jobLineages {
		lineageToJobName[lineage.JobName] = lineage
	}

	return lineageToJobName, nil
}

func NewJobLineageService(
	l log.Logger,
	lineageBuilder LineageBuilder,
	durationEstimator DurationEstimatorRepo,
	historicalDurationLastNRuns int,
	historicalDurationPercentile int,
	maxLineageDepth int,
	lineageWindowHours int,
	potentialBreach config.PotentialBreachPredictionConfig,
) *JobLineageService {
	if lineageWindowHours <= 0 {
		lineageWindowHours = DefaultLineageWindowHours
	}

	return &JobLineageService{
		l:                            l,
		lineageBuilder:               lineageBuilder,
		durationEstimator:            durationEstimator,
		historicalDurationLastNRuns:  historicalDurationLastNRuns,
		historicalDurationPercentile: historicalDurationPercentile,
		maxLineageDepth:              maxLineageDepth,
		lineageWindowHours:           lineageWindowHours,
		potentialBreachEnabled:       potentialBreach.Enable,
		damperFactor:                 damperFactorFromConfig(potentialBreach.DamperFactor),
		now:                          time.Now,
	}
}

// damperFactorFromConfig maps the config's method string onto the damper behavior; an
// unknown method falls back to the exponential default.
func damperFactorFromConfig(damper config.DamperPredictionConfig) scheduler.DamperFactor {
	method := scheduler.DamperFactorMethodExponential
	switch strings.ToUpper(damper.Method) {
	case "APPLY_METHOD_DECREMENT":
		method = scheduler.DamperFactorMethodDecrement
	case "APPLY_METHOD_CONSTANT":
		method = scheduler.DamperFactorMethodConstant
	}
	return scheduler.DamperFactor{
		BaseValue: damper.BaseValue,
		Alpha:     damper.Alpha,
		MinValue:  damper.MinValue,
		Method:    method,
	}
}

// annotatePotentialBreaches predicts whether the target run is heading for an SLA miss:
// every wait-chain node gets an inferred deadline (the target's SLA minus damped estimated
// durations down the chain) and is flagged when its current state endangers that deadline.
// A predicted miss with the target's own deadline still in the future marks the lineage
// IsPotentiallyBreaching; once the deadline has passed the breach is actual and the
// root-cause publisher's reactive semantics take over. The duration estimates come from
// the enrichment above.
func (j *JobLineageService) annotatePotentialBreaches(tree *scheduler.JobLineageSummary, result *scheduler.JobRunLineage) {
	durations := make(map[scheduler.JobName]*time.Duration, len(result.JobRuns))
	for _, node := range result.JobRuns {
		if node.HistoricalSummary.TaskDuration > 0 {
			duration := node.HistoricalSummary.TaskDuration
			durations[node.JobName] = &duration
		}
	}

	j.annotateTree(tree, durations, j.now())

	// mirror the tree annotation onto the flattened response nodes
	result.IsPotentiallyBreaching = tree.IsPotentiallyBreaching
	for _, node := range result.JobRuns {
		if treeNode := findTreeNode(tree, node.JobName); treeNode != nil {
			node.InferredSLA = treeNode.InferredSLA
			node.IsPotentialBreachCause = treeNode.IsPotentialBreachCause
		}
	}
}

// AnnotatePotentialBreaches computes the potential-breach prediction directly on lineage
// trees, using caller-provided duration estimates (the publisher's baselines). Trees
// without a target SLA are left untouched, as is everything when the prediction is
// disabled in the config.
func (j *JobLineageService) AnnotatePotentialBreaches(roots map[scheduler.JobName]*scheduler.JobLineageSummary, durationsByJob map[scheduler.JobName]*time.Duration, referenceTime time.Time) {
	if !j.potentialBreachEnabled {
		return
	}
	for _, tree := range roots {
		j.annotateTree(tree, durationsByJob, referenceTime)
	}
}

// annotateTree flags the tree's nodes against the inferred deadlines the target's SLA
// imposes, and marks the root IsPotentiallyBreaching when a cause exists while the
// target's own deadline has not passed yet.
func (j *JobLineageService) annotateTree(tree *scheduler.JobLineageSummary, durationsByJob map[scheduler.JobName]*time.Duration, now time.Time) {
	targetRun := tree.GetRunForJob(tree.JobName)
	if targetRun == nil || targetRun.SLATime == nil || targetRun.SLATime.Equal(targetRun.ScheduledAt) {
		return // no SLA configured: nothing to predict against
	}

	identifier := rootcause.NewIdentifier(j.l, nil, nil, nil, rootcause.IdentifierConfig{})
	inferredSLAs, _ := identifier.CalculateInferredSLAs(tree, durationsByJob, targetRun.SLATime, j.damperFactor)

	potentialCause := false
	for _, node := range collectTreeNodes(tree) {
		inferredSLA, ok := inferredSLAs[node.JobName]
		if !ok {
			continue
		}
		node.InferredSLA = inferredSLA
		durationEstimate := time.Duration(0)
		if estimate := durationsByJob[node.JobName]; estimate != nil {
			durationEstimate = *estimate
		}
		nodeRun := node.GetRunForJob(node.JobName)
		if nodeRun == nil {
			continue
		}
		if potentialBreachCause(nodeRun, durationEstimate, inferredSLA, now) {
			node.IsPotentialBreachCause = true
			potentialCause = true
		}
	}

	if potentialCause && !jobRunBreached(targetRun, now) {
		tree.IsPotentiallyBreaching = true
	}
}

// findTreeNode locates a job's node in the tree by depth-first search; a job reached
// through several paths shares one tree node.
func findTreeNode(tree *scheduler.JobLineageSummary, jobName scheduler.JobName) *scheduler.JobLineageSummary {
	if tree == nil {
		return nil
	}
	if tree.JobName == jobName {
		return tree
	}
	for _, upstream := range tree.Upstreams {
		if found := findTreeNode(upstream, jobName); found != nil {
			return found
		}
	}
	return nil
}

// collectTreeNodes gathers every node in the tree exactly once (cycles guarded by the
// caller's path pruning, but the visited set keeps this cheap and safe).
func collectTreeNodes(tree *scheduler.JobLineageSummary) []*scheduler.JobLineageSummary {
	visited := map[*scheduler.JobLineageSummary]bool{}
	var nodes []*scheduler.JobLineageSummary
	var walk func(*scheduler.JobLineageSummary)
	walk = func(node *scheduler.JobLineageSummary) {
		if node == nil || visited[node] {
			return
		}
		visited[node] = true
		nodes = append(nodes, node)
		for _, upstream := range node.Upstreams {
			walk(upstream)
		}
	}
	walk(tree)
	return nodes
}

// potentialBreachCause flags a run whose current state endangers its inferred deadline:
// finished after it, still running past it, or not started by the latest start its
// duration estimate allows.
func potentialBreachCause(run *scheduler.JobRunSummary, durationEstimate time.Duration, inferredSLA *time.Time, now time.Time) bool {
	switch {
	case run.IsFinished():
		end := run.GetActualEndTime()
		return end.After(*inferredSLA)
	case run.TaskStartTime != nil:
		return now.After(*inferredSLA)
	default:
		return now.After(inferredSLA.Add(-durationEstimate))
	}
}

// jobRunBreached mirrors the root-cause publisher's breach gate for a single run.
func jobRunBreached(run *scheduler.JobRunSummary, now time.Time) bool {
	if run.SLATime == nil {
		return false
	}
	if end := run.GetActualEndTime(); end != nil {
		return end.After(*run.SLATime)
	}
	return now.After(*run.SLATime)
}
