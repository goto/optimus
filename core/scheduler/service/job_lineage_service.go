package service

import (
	"context"

	"github.com/goto/optimus/core/scheduler"
)

// Contract that can be used by other callers to fetch job lineage information
type JobLineageFetcher interface {
	GetJobLineage(ctx context.Context, jobSchedules []*scheduler.JobSchedule) ([]*scheduler.JobLineageSummary, error)
}

type LineageBuilder interface {
	BuildLineage(jobSchedules []*scheduler.JobSchedule) ([]*scheduler.JobLineageSummary, error)
}

type JobLineageService struct {
	lineageBuilder LineageBuilder
}

func NewJobLineageService(
	lineageBuilder LineageBuilder,
) *JobLineageService {
	return &JobLineageService{
		lineageBuilder: lineageBuilder,
	}
}

func (j *JobLineageService) GetJobLineage(ctx context.Context, jobSchedules []*scheduler.JobSchedule) ([]*scheduler.JobLineageSummary, error) {
	jobSummaries := make([]*scheduler.JobLineageSummary, 0, len(jobSchedules))

	return jobSummaries, nil
}
