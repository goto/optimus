package service

import (
	"context"

	"github.com/goto/optimus/core/scheduler"
)

// Contract that can be used by other callers to fetch job lineage information
type JobLineageFetcher interface {
	GetJobLineage(ctx context.Context, jobSchedules []*scheduler.JobSchedule) ([]*scheduler.JobLineageSummary, error)
}

type JobLineageService struct {
}

func NewJobLineageService() *JobLineageService {
	return &JobLineageService{}
}

func (j *JobLineageService) GetJobLineage(ctx context.Context, jobSchedules []*scheduler.JobSchedule) ([]*scheduler.JobLineageSummary, error) {
	jobSummaries := make([]*scheduler.JobLineageSummary, 0, len(jobSchedules))

	for _, jobSchedule := range jobSchedules {
		jobSummary := &scheduler.JobLineageSummary{
			JobName: jobSchedule.JobName,
		}
		jobSummaries = append(jobSummaries, jobSummary)
	}

	return jobSummaries, nil
}
