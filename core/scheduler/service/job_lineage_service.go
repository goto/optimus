package service

import (
	"context"

	"github.com/goto/salt/log"

	"github.com/goto/optimus/core/scheduler"
)

// Contract that can be used by other callers to fetch job lineage information
type JobLineageFetcher interface {
	GetJobLineage(ctx context.Context, jobSchedules []*scheduler.JobSchedule) ([]*scheduler.JobLineageSummary, error)
}

type LineageBuilder interface {
	BuildLineage(context.Context, []*scheduler.JobSchedule) ([]*scheduler.JobLineageSummary, error)
}

type JobLineageService struct {
	l              log.Logger
	lineageBuilder LineageBuilder
}

func (j *JobLineageService) GetJobExecutionSummary(ctx context.Context, jobSchedules []*scheduler.JobSchedule, numberOfUpstreamPerLevel int) ([]*scheduler.JobRunLineage, error) {
	downstreamLineages, err := j.GetJobLineage(ctx, jobSchedules)
	if err != nil {
		j.l.Error("failed to get job lineage", "error", err)
		return nil, err
	}

	var result []*scheduler.JobRunLineage
	for _, lineage := range downstreamLineages {
		prunedLineage := lineage.PruneUpstreamLineage(numberOfUpstreamPerLevel)
		if len(prunedLineage) == 0 {
			continue
		}

		result = append(result, &scheduler.JobRunLineage{
			JobName: lineage.JobName,
			// index 0 should contain the original job in question
			ScheduledAt: prunedLineage[0].JobRunSummary.ScheduledAt,
			JobRuns:     prunedLineage,
		})
	}

	return result, nil
}

func (j *JobLineageService) GetJobLineage(ctx context.Context, jobSchedules []*scheduler.JobSchedule) ([]*scheduler.JobLineageSummary, error) {
	return j.lineageBuilder.BuildLineage(ctx, jobSchedules)
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
