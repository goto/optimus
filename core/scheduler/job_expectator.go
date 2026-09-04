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

type JobCompletionTimeDetail struct {
	ProjectName        tenant.ProjectName
	JobName            JobName
	ScheduledAt        time.Time
	ExpectedFinishTime time.Time
	ActualFinishTime   *time.Time
	Delay              *time.Duration
}

type JobCompletionTimeDetails []JobCompletionTimeDetail

type JobCompletionTimeSummary struct {
	MeanDelay                 *time.Duration
	MaxDelay                  *time.Duration
	MaxExpectedCompletionTime time.Time
	MaxActualCompletionTime   *time.Time
}

type JobCompletionTimeReport struct {
	Details []JobCompletionTimeDetail
	Summary JobCompletionTimeSummary
}

func (r JobCompletionTimeDetails) GenerateSummary() JobCompletionTimeSummary {
	var summary JobCompletionTimeSummary
	var sum time.Duration
	var count int
	for _, rep := range r {
		if summary.MaxExpectedCompletionTime.IsZero() || rep.ExpectedFinishTime.After(summary.MaxExpectedCompletionTime) {
			summary.MaxExpectedCompletionTime = rep.ExpectedFinishTime
		}
		if rep.ActualFinishTime != nil && (summary.MaxActualCompletionTime == nil || rep.ActualFinishTime.After(*summary.MaxActualCompletionTime)) {
			summary.MaxActualCompletionTime = rep.ActualFinishTime
		}

		if rep.ActualFinishTime == nil {
			continue
		}
		// only consider delays from finish times which are delayed than expected
		if rep.ActualFinishTime.Before(rep.ExpectedFinishTime) {
			continue
		}

		delay := rep.ActualFinishTime.Sub(rep.ExpectedFinishTime)
		sum += delay
		count++

		if summary.MaxDelay == nil || delay > *summary.MaxDelay {
			summary.MaxDelay = &delay
		}
	}
	if count == 0 {
		return summary
	}
	mean := sum / time.Duration(count)
	summary.MeanDelay = &mean
	return summary
}
