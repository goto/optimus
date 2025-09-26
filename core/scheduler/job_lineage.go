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

func (j *JobLineageSummary) PruneUpstreamLineage(numberOfUpstreamPerLevel int) []*JobExecutionSummary {
	if numberOfUpstreamPerLevel < 0 {
		return nil
	}

	var result []*JobExecutionSummary

	queue := []*JobLineageSummary{j}
	level := 0

	for len(queue) > 0 {
		levelSize := len(queue)
		var currentLevelJobs []*JobExecutionSummary
		var nextLevelUpstreams []*JobLineageSummary

		for i := range levelSize {
			current := queue[i]

			for _, jobRun := range current.JobRuns {
				if jobRun.JobStartTime != nil && jobRun.JobEndTime != nil {
					execSummary := &JobExecutionSummary{
						JobName:       current.JobName,
						SLA:           current.SLA,
						JobRunSummary: jobRun,
						Level:         level,
					}
					currentLevelJobs = append(currentLevelJobs, execSummary)
				}
			}
		}

		if len(currentLevelJobs) > 0 {
			for i := 0; i < len(currentLevelJobs)-1; i++ {
				for k := i + 1; k < len(currentLevelJobs); k++ {
					duration1 := currentLevelJobs[i].JobRunSummary.JobEndTime.Sub(*currentLevelJobs[i].JobRunSummary.JobStartTime)
					duration2 := currentLevelJobs[k].JobRunSummary.JobEndTime.Sub(*currentLevelJobs[k].JobRunSummary.JobStartTime)
					if duration1 < duration2 {
						currentLevelJobs[i], currentLevelJobs[k] = currentLevelJobs[k], currentLevelJobs[i]
					}
				}
			}

			var selectedJobs []*JobExecutionSummary
			if numberOfUpstreamPerLevel == 0 {
				selectedJobs = currentLevelJobs
			} else {
				limit := min(len(currentLevelJobs), numberOfUpstreamPerLevel)
				selectedJobs = currentLevelJobs[:limit]
			}
			result = append(result, selectedJobs...)

			for _, selectedJob := range selectedJobs {
				for i := range levelSize {
					if queue[i].JobName == selectedJob.JobName {
						nextLevelUpstreams = append(nextLevelUpstreams, queue[i].Upstreams...)
						break
					}
				}
			}
		}

		queue = nextLevelUpstreams
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

	JobStartTime  *time.Time
	JobEndTime    *time.Time
	WaitStartTime *time.Time
	WaitEndTime   *time.Time
	TaskStartTime *time.Time
	TaskEndTime   *time.Time
	HookStartTime *time.Time
	HookEndTime   *time.Time
}
