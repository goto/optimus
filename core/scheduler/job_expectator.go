package scheduler

import (
	"time"

	"github.com/goto/optimus/core/tenant"
)

type JobFilterRequest struct {
	ProjectName tenant.ProjectName
	JobNames    []JobName
	Labels      map[string][]string
}

type JobCompletionTimeReport struct {
	ProjectName        tenant.ProjectName
	JobName            JobName
	ScheduledAt        time.Time
	ExpectedFinishTime time.Time
	ActualFinishTime   *time.Time
}

type JobCompletionTimeReports []JobCompletionTimeReport

type JobCompletionTimeSummary struct {
	Reports   []JobCompletionTimeReport
	MeanDelay *time.Duration
}

func (r JobCompletionTimeReports) ComputeMeanDelay() *time.Duration {
	var sum time.Duration
	var count int
	for _, rep := range r {
		if rep.ActualFinishTime == nil {
			continue
		}
		sum += rep.ActualFinishTime.Sub(rep.ExpectedFinishTime)
		count++
	}
	if count == 0 {
		return nil
	}
	mean := sum / time.Duration(count)
	return &mean
}
