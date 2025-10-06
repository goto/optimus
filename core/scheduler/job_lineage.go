package scheduler

import (
	"time"

	"github.com/goto/optimus/core/tenant"
	"github.com/goto/optimus/internal/lib/window"
)

type JobSchedule struct {
	JobName     JobName
	ScheduledAt time.Time
}

type JobLineageSummary struct {
	JobName   JobName
	Upstreams []*JobLineageSummary

	Tenant           tenant.Tenant
	ScheduleInterval string
	SLA              SLAConfig
	Window           *window.Config

	// JobRuns is a map of job's scheduled time to its run summary
	JobRuns map[string]*JobRunSummary
}

func (j *JobLineageSummary) Flatten() []*JobExecutionSummary {
	var result []*JobExecutionSummary
	queue := []*JobLineageSummary{j}
	level := 0

	for len(queue) > 0 {
		levelSize := len(queue)

		for range levelSize {
			current := queue[0]
			queue = queue[1:]

			var latestJobRun *JobRunSummary
			var latestScheduledAt time.Time

			for _, jobRun := range current.JobRuns {
				if jobRun.ScheduledAt.After(latestScheduledAt) {
					latestScheduledAt = jobRun.ScheduledAt
					latestJobRun = jobRun
				}
			}

			if latestJobRun != nil {
				result = append(result, &JobExecutionSummary{
					JobName:       current.JobName,
					SLA:           current.SLA,
					Level:         level,
					JobRunSummary: latestJobRun,
				})
			}

			queue = append(queue, current.Upstreams...)
		}

		level++
	}

	return result
}

type JobRunLineage struct {
	JobName     JobName
	ScheduledAt time.Time
	JobRuns     []*JobExecutionSummary
}

// JobExecutionSummary is a flattened version of JobLineageSummary
type JobExecutionSummary struct {
	JobName JobName
	SLA     SLAConfig
	// Level marks the distance from the original job in question
	Level         int
	JobRunSummary *JobRunSummary
}

type SLAConfig struct {
	Duration time.Duration
}

type JobRunSummary struct {
	JobName     JobName
	ScheduledAt time.Time
	SLATime     *time.Time

	JobStartTime  *time.Time
	JobEndTime    *time.Time
	WaitStartTime *time.Time
	WaitEndTime   *time.Time
	TaskStartTime *time.Time
	TaskEndTime   *time.Time
	HookStartTime *time.Time
	HookEndTime   *time.Time
}
