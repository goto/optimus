package service

import (
	"context"
	"slices"
	"time"

	"github.com/goto/salt/log"

	"github.com/goto/optimus/core/scheduler"
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
	var result []*scheduler.JobRunLineage
	for _, lineage := range lineagesMap {
		newDownstreamLineage := lineage
		jobRunLineage := newDownstreamLineage.GenerateLineageExecutionSummary(walkOpts)
		result = append(result, jobRunLineage)
	}

	if err := j.enrichWithHistoricalDurations(ctx, result); err != nil {
		j.l.Error("failed to enrich job run lineage with historical durations", "error", err)
		// prioritize returning the lineage information even if the enrichment fails
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
	}
}
