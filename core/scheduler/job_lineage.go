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
	visited := make(map[JobName]bool)
	queue := []*JobLineageSummary{j}
	level := 0
	maxLevels := 50

	for len(queue) > 0 && level < maxLevels {
		levelSize := len(queue)
		var currentLevelJobs []*JobExecutionSummary
		var nextLevelUpstreams []*JobLineageSummary
		currentQueue := make([]*JobLineageSummary, levelSize)
		copy(currentQueue, queue)

		for i := 0; i < levelSize; i++ {
			current := currentQueue[i]

			if visited[current.JobName] {
				continue
			}
			visited[current.JobName] = true

			var latestJobRun *JobRunSummary
			var latestScheduledAt time.Time

			for _, jobRun := range current.JobRuns {
				if jobRun.JobStartTime != nil && jobRun.JobEndTime != nil {
					if latestJobRun == nil || jobRun.ScheduledAt.After(latestScheduledAt) {
						latestJobRun = jobRun
						latestScheduledAt = jobRun.ScheduledAt
					}
				}
			}

			if latestJobRun != nil {
				execSummary := &JobExecutionSummary{
					JobName:       current.JobName,
					SLA:           current.SLA,
					JobRunSummary: latestJobRun,
					Level:         level,
				}
				currentLevelJobs = append(currentLevelJobs, execSummary)
			}

			for _, upstream := range current.Upstreams {
				if upstream != nil && !visited[upstream.JobName] {
					nextLevelUpstreams = append(nextLevelUpstreams, upstream)
				}
			}
		}

		if len(currentLevelJobs) > 0 {
			sortJobsByDurationDesc(currentLevelJobs)

			var selectedJobs []*JobExecutionSummary
			if numberOfUpstreamPerLevel == 0 {
				selectedJobs = currentLevelJobs
			} else {
				limit := min(len(currentLevelJobs), numberOfUpstreamPerLevel)
				selectedJobs = currentLevelJobs[:limit]
			}
			result = append(result, selectedJobs...)
		}

		queue = nextLevelUpstreams
		level++
	}

	return result
}

func sortJobsByDurationDesc(jobs []*JobExecutionSummary) {
	for i := 1; i < len(jobs); i++ {
		key := jobs[i]
		keyDuration := key.JobRunSummary.JobEndTime.Sub(*key.JobRunSummary.JobStartTime)
		j := i - 1

		for j >= 0 {
			currentDuration := jobs[j].JobRunSummary.JobEndTime.Sub(*jobs[j].JobRunSummary.JobStartTime)
			if currentDuration >= keyDuration {
				break
			}
			jobs[j+1] = jobs[j]
			j--
		}
		jobs[j+1] = key
	}
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
