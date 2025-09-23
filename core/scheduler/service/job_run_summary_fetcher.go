package service

import (
	"context"
	"time"

	"github.com/goto/optimus/core/scheduler"
	"github.com/goto/optimus/internal/lib/window"
)

// Contract that can be used
type JobSummaryInterface interface {
	GetTargetJobSummary(ctx context.Context, jobName string, scheduledAt time.Time) (*scheduler.JobSummary, error)
}

type JobRunSummaryFetcher struct {
}

func NewJobRunSummaryFetcher() *JobRunSummaryFetcher {
	return &JobRunSummaryFetcher{}
}

func (j *JobRunSummaryFetcher) GetTargetJobSummary(ctx context.Context, jobName string, scheduledAt time.Time) (*scheduler.JobSummary, error) {
	return &scheduler.JobSummary{
		JobName: scheduler.JobName(jobName),
		Window:  window.Config{},
		JobRuns: []scheduler.JobRunSummary{
			{
				ScheduledAt:   scheduledAt,
				WaitStartTime: scheduledAt.Add(1 * time.Minute),
				WaitEndTime:   scheduledAt.Add(2 * time.Minute),
				TaskStartTime: scheduledAt.Add(2 * time.Minute),
				TaskEndTime:   scheduledAt.Add(30 * time.Minute),
				HookStartTime: scheduledAt.Add(31 * time.Minute),
				HookEndTime:   scheduledAt.Add(32 * time.Minute),
			},
		},
		Upstreams: []scheduler.JobSummary{},
	}, nil
}
