package service

import (
	"context"

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
	l              log.Logger
	lineageBuilder LineageBuilder
}

func (j *JobLineageService) GetJobExecutionSummary(ctx context.Context, jobSchedules []*scheduler.JobSchedule, numberOfUpstreamPerLevel int) ([]*scheduler.JobRunLineage, error) {
	downstreamLineages, err := j.lineageBuilder.BuildLineage(ctx, jobSchedules, numberOfUpstreamPerLevel)
	if err != nil {
		j.l.Error("failed to get job lineage", "error", err)
		return nil, err
	}

	var result []*scheduler.JobRunLineage
	for _, lineage := range downstreamLineages {
		flattenedLineage := lineage.Flatten(scheduler.MaxLineageDepth)
		result = append(result, &scheduler.JobRunLineage{
			JobName:     lineage.JobName,
			ProjectName: lineage.Tenant.ProjectName(),
			// index 0 should contain the original job in question
			ScheduledAt: flattenedLineage[0].JobRunSummary.ScheduledAt,
			JobRuns:     flattenedLineage,
		})
	}

	return result, nil
}

func (j *JobLineageService) GetJobLineage(ctx context.Context, jobSchedules map[scheduler.JobName]*scheduler.JobSchedule) (map[scheduler.JobName]*scheduler.JobLineageSummary, error) {
	lineageToJobName := make(map[scheduler.JobName]*scheduler.JobLineageSummary)
	schedules := make([]*scheduler.JobSchedule, 0, len(jobSchedules))
	for _, schedule := range jobSchedules {
		schedules = append(schedules, schedule)
	}
	jobLineages, err := j.lineageBuilder.BuildLineage(ctx, schedules, 0)
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
) *JobLineageService {
	return &JobLineageService{
		l:              l,
		lineageBuilder: lineageBuilder,
	}
}
