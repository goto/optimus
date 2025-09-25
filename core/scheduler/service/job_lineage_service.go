package service

import (
	"context"

	"github.com/goto/optimus/core/scheduler"
	"github.com/goto/salt/log"
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
