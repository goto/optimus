package service

import (
	"context"
	"time"

	"github.com/goto/salt/log"

	"github.com/goto/optimus/core/scheduler"
)

// Contract that can be used by other callers to fetch job lineage information
type JobLineageFetcher interface {
	GetJobLineage(ctx context.Context, jobSchedules map[scheduler.JobName]*scheduler.JobSchedule) (map[scheduler.JobName]*scheduler.JobLineageSummary, error)
}

type LineageBuilder interface {
	BuildLineage(context.Context, []*scheduler.JobSchedule, int) (map[*scheduler.JobSchedule]*scheduler.JobLineageSummary, error)
}

type JobLineageService struct {
	l                            log.Logger
	lineageBuilder               LineageBuilder
	durationEstimator            DurationEstimatorRepo
	maxLineageDepth              int
	historicalDurationLastNRuns  int
	historicalDurationPercentile int
}

func (j *JobLineageService) GetJobExecutionSummary(ctx context.Context, jobSchedules []*scheduler.JobSchedule, numberOfUpstreamPerLevel int) ([]*scheduler.JobRunLineage, error) {
	downstreamLineages, err := j.lineageBuilder.BuildLineage(ctx, jobSchedules, 24)
	if err != nil {
		j.l.Error("failed to get job lineage", "error", err)
		return nil, err
	}

	return j.generateLineageExecutionSummary(ctx, downstreamLineages, numberOfUpstreamPerLevel)
}

func (j *JobLineageService) generateLineageExecutionSummary(ctx context.Context, lineagesMap map[*scheduler.JobSchedule]*scheduler.JobLineageSummary, numberOfUpstreamPerLevel int) ([]*scheduler.JobRunLineage, error) {
	var result []*scheduler.JobRunLineage
	for _, lineage := range lineagesMap {
		newDownstreamLineage := lineage
		jobRunLineage := newDownstreamLineage.GenerateLineageExecutionSummary(numberOfUpstreamPerLevel, j.maxLineageDepth)
		if err := j.enrichWithHistoricalDurations(ctx, jobRunLineage); err != nil {
			j.l.Error("failed to enrich job run lineage with historical durations", "error", err)
			// prioritize returning the lineage information even if the enrichment fails
		}

		result = append(result, jobRunLineage)
	}

	return result, nil
}

func (j *JobLineageService) enrichWithHistoricalDurations(ctx context.Context, jobRunLineage *scheduler.JobRunLineage) error {
	// fetch historical task durations
	jobNames := jobRunLineage.GetAllJobNames()
	historicalTaskDurations, err := j.durationEstimator.GetPercentileDurationByJobNames(ctx, jobNames, map[string][]string{"task": {}},
		jobRunLineage.ScheduledAt, j.historicalDurationLastNRuns, j.historicalDurationPercentile)
	if err != nil {
		j.l.Error("failed to get task historical durations", "error", err)
		return err
	}

	// fetch historical hook durations:
	// from the lineage job run summary, get the highlighted hooks and fetch their historical durations
	// ensuring that if there are multiple hooks registered for a job, it will only fetch the ones highlighted in the lineage
	jobNamesByHook := jobRunLineage.GroupJobsInLineageByHookNames()
	hookDurationsByHookName := make(map[string]map[scheduler.JobName]*time.Duration)
	for hookName, jobNames := range jobNamesByHook {
		historicalHookDurations, err := j.durationEstimator.GetPercentileDurationByJobNames(ctx, jobNames, map[string][]string{"hook": {hookName}},
			jobRunLineage.ScheduledAt, j.historicalDurationLastNRuns, j.historicalDurationPercentile)
		if err != nil {
			j.l.Error("failed to get hook historical durations", "error", err)
			return err
		}
		hookDurationsByHookName[hookName] = historicalHookDurations
	}

	// populate the historical summary for each job run in the lineage with the fetched historical durations
	for i := range jobRunLineage.JobRuns {
		jobRunLineage.JobRuns[i].HistoricalSummary = scheduler.JobHistoricalDuration{}
		if duration, ok := historicalTaskDurations[jobRunLineage.JobRuns[i].JobName]; ok && duration != nil {
			jobRunLineage.JobRuns[i].HistoricalSummary.TaskDuration = *duration
		}
		if jobRunLineage.JobRuns[i].JobRunSummary.HookName != nil {
			if hookDurations, ok := hookDurationsByHookName[*jobRunLineage.JobRuns[i].JobRunSummary.HookName]; ok {
				if duration, ok := hookDurations[jobRunLineage.JobRuns[i].JobName]; ok && duration != nil {
					jobRunLineage.JobRuns[i].HistoricalSummary.HookDuration = *duration
				}
			}
		}
	}

	return nil
}

func (j *JobLineageService) GetJobLineage(ctx context.Context, jobSchedules map[scheduler.JobName]*scheduler.JobSchedule) (map[scheduler.JobName]*scheduler.JobLineageSummary, error) {
	lineageToJobName := make(map[scheduler.JobName]*scheduler.JobLineageSummary)
	schedules := make([]*scheduler.JobSchedule, 0, len(jobSchedules))
	for _, schedule := range jobSchedules {
		schedules = append(schedules, schedule)
	}
	jobLineages, err := j.lineageBuilder.BuildLineage(ctx, schedules, j.maxLineageDepth)
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
) *JobLineageService {
	return &JobLineageService{
		l:                            l,
		lineageBuilder:               lineageBuilder,
		durationEstimator:            durationEstimator,
		historicalDurationLastNRuns:  historicalDurationLastNRuns,
		historicalDurationPercentile: historicalDurationPercentile,
		maxLineageDepth:              maxLineageDepth,
	}
}
