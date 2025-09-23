package service

import (
	"context"

	"github.com/goto/optimus/core/scheduler"
)

// Contract that can be used by other callers to fetch job run lineage information
type JobRunLineageFetcher interface {
	GetJobRunLineage(ctx context.Context, jobSchedules []*scheduler.JobSchedule) ([]*scheduler.JobRunLineageSummary, error)
}

type JobRunLineageService struct {
}

func NewJobRunLineageService() *JobRunLineageService {
	return &JobRunLineageService{}
}

func (j *JobRunLineageService) GetJobRunLineage(ctx context.Context, jobSchedules []*scheduler.JobSchedule) ([]*scheduler.JobRunLineageSummary, error) {
	jobSummaries := make([]*scheduler.JobRunLineageSummary, 0, len(jobSchedules))

	for _, jobSchedule := range jobSchedules {
		jobSummary := &scheduler.JobRunLineageSummary{
			JobName: jobSchedule.JobName,
		}
		jobSummaries = append(jobSummaries, jobSummary)
	}

	return jobSummaries, nil
}
