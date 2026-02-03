package scheduler

import (
	"sort"
	"time"

	"github.com/goto/optimus/core/tenant"
	"github.com/goto/optimus/internal/lib/window"
)

const (
	// MaxLineageDepth is a safeguard to avoid infinite recursion in case of unexpected cycles
	// generally we don't expect lineage to be deeper than 20 levels
	MaxLineageDepth = 25
)

type JobSchedule struct {
	JobName     JobName
	ScheduledAt time.Time
}

type JobLineageSummary struct {
	JobName   JobName
	IsEnabled bool
	Upstreams []*JobLineageSummary

	Tenant           tenant.Tenant
	ScheduleInterval string
	SLA              SLAConfig
	Window           *window.Config

	// JobRuns contain the mapping of downstream's job name to their respective job run summaries
	JobRuns map[JobName]*JobRunSummary
}

type upstreamCandidate struct {
	JobName     JobName
	ScheduledAt time.Time
	EndTime     time.Time
}

func (j *JobLineageSummary) sortUpstreamCandidates(jobName JobName) []upstreamCandidate {
	candidates := []upstreamCandidate{}

	currentJobRun := j.GetRunForJob(jobName)

	for _, upstream := range j.Upstreams {
		latestFinishTime, latestScheduledAt := getLatestFinishTime(jobName, upstream.JobRuns)
		if latestFinishTime == nil {
			continue
		}

		candidates = append(candidates, upstreamCandidate{
			JobName:     upstream.JobName,
			ScheduledAt: latestScheduledAt,
			EndTime:     *latestFinishTime,
		})
	}

	sort.Slice(candidates, func(i, j int) bool {
		iAfterCurrent := candidates[i].ScheduledAt.After(currentJobRun.ScheduledAt)
		jAfterCurrent := candidates[j].ScheduledAt.After(currentJobRun.ScheduledAt)
		if iAfterCurrent && !jAfterCurrent {
			return false
		}
		if !iAfterCurrent && jAfterCurrent {
			return true
		}

		return candidates[i].EndTime.After(candidates[j].EndTime)
	})

	return candidates
}

func getLatestFinishTime(currentJobName JobName, jobRuns map[JobName]*JobRunSummary) (*time.Time, time.Time) {
	jobRun := getRunForJob(currentJobName, jobRuns)
	if jobRun == nil {
		return nil, time.Time{}
	}

	latestTaskEndTime := jobRun.HookEndTime
	if latestTaskEndTime == nil {
		latestTaskEndTime = jobRun.TaskEndTime
	}

	return latestTaskEndTime, jobRun.ScheduledAt
}

func (j *JobLineageSummary) GetRunForJob(jobName JobName) *JobRunSummary {
	return getRunForJob(jobName, j.JobRuns)
}

func getRunForJob(jobName JobName, jobRuns map[JobName]*JobRunSummary) *JobRunSummary {
	if run, exists := jobRuns[jobName]; exists {
		return run
	}

	return nil
}

func (j *JobLineageSummary) GenerateLineageExecutionSummary(maxUpstreamsPerLevel, maxDepth int) *JobRunLineage {
	if j == nil {
		return nil
	}

	executionSummaries := j.GetFlattenedSummaries(maxUpstreamsPerLevel, maxDepth)
	lineageSummary := &LineageExecutionSummary{}
	var largestScheduledWayTooLate, largestSystemSchedulingDelay *JobExecutionSummary
	var largestWayTooLateUpstream, largestSystemSchedulingUpstream *JobRunSummary
	wayTooLateCount := 0

	var taskDurationJobs, hookDurationJobs []JobWithTaskDuration

	levelGroups := make(map[int][]*JobExecutionSummary)
	for _, exec := range executionSummaries {
		levelGroups[exec.Level] = append(levelGroups[exec.Level], exec)
	}

	var sortedLevels []int
	for level := range levelGroups {
		sortedLevels = append(sortedLevels, level)
	}
	sort.Ints(sortedLevels)

	var latestEndTimeByLevel []*JobExecutionSummary
	for _, level := range sortedLevels {
		jobs := levelGroups[level]
		var latestJob *JobExecutionSummary
		var latestEndTime *time.Time

		for _, job := range jobs {
			if job.JobRunSummary.GetActualEndTime() == nil {
				continue
			}
			if latestEndTime == nil || job.JobRunSummary.GetActualEndTime().After(*latestEndTime) {
				latestJob = job
				latestEndTime = job.JobRunSummary.GetActualEndTime()
			}
		}

		if latestJob != nil {
			latestEndTimeByLevel = append(latestEndTimeByLevel, latestJob)
		}
	}

	for _, exec := range executionSummaries {
		currentRun := exec.JobRunSummary

		if currentRun.GetActualEndTime() == nil {
			continue
		}

		if currentRun.TaskEndTime != nil && currentRun.TaskStartTime != nil {
			taskDurationJobs = append(taskDurationJobs, JobWithTaskDuration{
				JobName:      currentRun.JobName,
				ScheduledAt:  currentRun.ScheduledAt,
				TaskDuration: currentRun.GetTaskDuration(),
				Level:        exec.Level,
			})
		}

		if currentRun.HookEndTime != nil && currentRun.HookStartTime != nil {
			hookDurationJobs = append(hookDurationJobs, JobWithTaskDuration{
				JobName:      currentRun.JobName,
				ScheduledAt:  currentRun.ScheduledAt,
				TaskDuration: currentRun.GetHookDuration(),
				Level:        exec.Level,
			})
		}
	}

	for i := range latestEndTimeByLevel {
		currentExec := latestEndTimeByLevel[i]
		currentRun := currentExec.JobRunSummary

		if currentRun.TaskStartTime == nil {
			continue
		}

		scheduledToTaskStartDuration := currentRun.TaskStartTime.Sub(currentRun.ScheduledAt)

		var upstreamLastTaskEndToCurrentTaskStartDuration time.Duration
		var upstreamRun *JobRunSummary
		hasUpstream := i < len(latestEndTimeByLevel)-1

		if hasUpstream {
			upstreamExec := latestEndTimeByLevel[i+1]
			upstreamRun = upstreamExec.JobRunSummary
			if upstreamRun.GetActualEndTime() != nil {
				upstreamLastTaskEndToCurrentTaskStartDuration = currentRun.TaskStartTime.Sub(*upstreamRun.GetActualEndTime())
			}
		}

		if hasUpstream && upstreamLastTaskEndToCurrentTaskStartDuration > scheduledToTaskStartDuration {
			currentScheduledWayTooLate := upstreamLastTaskEndToCurrentTaskStartDuration - scheduledToTaskStartDuration
			currentExec.DelaySummary.ScheduledWayTooLateSeconds = int64(currentScheduledWayTooLate.Seconds())
			lineageSummary.TotalScheduledWayTooLateSeconds += currentExec.DelaySummary.ScheduledWayTooLateSeconds
			wayTooLateCount++

			if largestScheduledWayTooLate == nil || currentScheduledWayTooLate.Seconds() > float64(largestScheduledWayTooLate.DelaySummary.ScheduledWayTooLateSeconds) {
				largestScheduledWayTooLate = currentExec
				largestWayTooLateUpstream = upstreamRun
			}

			currentExec.DelaySummary.SystemSchedulingDelaySeconds = int64(scheduledToTaskStartDuration.Seconds())
		} else if hasUpstream {
			currentExec.DelaySummary.SystemSchedulingDelaySeconds = int64(upstreamLastTaskEndToCurrentTaskStartDuration.Seconds())
		} else {
			currentExec.DelaySummary.SystemSchedulingDelaySeconds = int64(scheduledToTaskStartDuration.Seconds())
		}

		lineageSummary.TotalSystemSchedulingDelaySeconds += currentExec.DelaySummary.SystemSchedulingDelaySeconds
		if largestSystemSchedulingDelay == nil || currentExec.DelaySummary.SystemSchedulingDelaySeconds > largestSystemSchedulingDelay.DelaySummary.SystemSchedulingDelaySeconds {
			largestSystemSchedulingDelay = currentExec
			largestSystemSchedulingUpstream = upstreamRun
		}
	}

	sort.Slice(taskDurationJobs, func(i, j int) bool {
		return taskDurationJobs[i].TaskDuration > taskDurationJobs[j].TaskDuration
	})
	if len(taskDurationJobs) > 3 {
		taskDurationJobs = taskDurationJobs[:3]
	}
	lineageSummary.TopLongestTaskDurationJobs = taskDurationJobs

	sort.Slice(hookDurationJobs, func(i, j int) bool {
		return hookDurationJobs[i].TaskDuration > hookDurationJobs[j].TaskDuration
	})
	if len(hookDurationJobs) > 3 {
		hookDurationJobs = hookDurationJobs[:3]
	}
	lineageSummary.TopLongestHookDurationJobs = hookDurationJobs

	if len(latestEndTimeByLevel) > 0 {
		lineageSummary.AverageSystemSchedulingDelaySeconds = lineageSummary.TotalSystemSchedulingDelaySeconds / int64(len(latestEndTimeByLevel))
	}

	lineageSummary.TotalLineageDelaySeconds = lineageSummary.TotalScheduledWayTooLateSeconds + lineageSummary.TotalSystemSchedulingDelaySeconds
	if len(latestEndTimeByLevel) > 0 {
		firstJob := latestEndTimeByLevel[0].JobRunSummary
		lastJob := latestEndTimeByLevel[len(latestEndTimeByLevel)-1].JobRunSummary
		if firstJob.GetActualEndTime() != nil && lastJob.TaskStartTime != nil {
			lineageSummary.TotalLineageDurationSeconds = int64(firstJob.GetActualEndTime().Sub(*lastJob.TaskStartTime).Seconds())
		}
	}

	if largestScheduledWayTooLate != nil {
		lineageSummary.LargestScheduledWayTooLateJob = LineageDelaySummary{
			JobName:       largestScheduledWayTooLate.JobName,
			ScheduledAt:   largestScheduledWayTooLate.JobRunSummary.ScheduledAt,
			DelayDuration: largestScheduledWayTooLate.DelaySummary.ScheduledWayTooLateSeconds,
		}
		if largestWayTooLateUpstream != nil {
			lineageSummary.LargestScheduledWayTooLateJob.UpstreamJobName = largestWayTooLateUpstream.JobName
			lineageSummary.LargestScheduledWayTooLateJob.UpstreamScheduledAt = largestWayTooLateUpstream.ScheduledAt
		}
	}

	if largestSystemSchedulingDelay != nil {
		lineageSummary.LargestSystemSchedulingDelayJob = LineageDelaySummary{
			JobName:       largestSystemSchedulingDelay.JobName,
			ScheduledAt:   largestSystemSchedulingDelay.JobRunSummary.ScheduledAt,
			DelayDuration: largestSystemSchedulingDelay.DelaySummary.SystemSchedulingDelaySeconds,
		}
		if largestSystemSchedulingUpstream != nil {
			lineageSummary.LargestSystemSchedulingDelayJob.UpstreamJobName = largestSystemSchedulingUpstream.JobName
			lineageSummary.LargestSystemSchedulingDelayJob.UpstreamScheduledAt = largestSystemSchedulingUpstream.ScheduledAt
		}
	}

	return &JobRunLineage{
		JobName:          j.JobName,
		ScheduledAt:      j.GetRunForJob(j.JobName).ScheduledAt,
		JobRuns:          executionSummaries,
		ExecutionSummary: lineageSummary,
	}
}

func (j *JobLineageSummary) GetFlattenedSummaries(maxUpstreamsPerLevel, maxDepth int) []*JobExecutionSummary {
	if maxUpstreamsPerLevel <= 0 {
		maxUpstreamsPerLevel = 20
	}

	var result []*JobExecutionSummary
	result = append(result, &JobExecutionSummary{
		JobName:            j.JobName,
		SLA:                j.SLA,
		Level:              0,
		JobRunSummary:      j.GetRunForJob(j.JobName),
		DownstreamPathName: j.JobName.String(),
		DelaySummary: &JobRunDelaySummary{
			ScheduledWayTooLateSeconds:   0,
			SystemSchedulingDelaySeconds: 0,
		},
	})

	type nodeInfo struct {
		original *JobLineageSummary
		depth    int
	}
	queue := []*nodeInfo{{
		original: j,
		depth:    0,
	}}

	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]

		if current.depth >= maxDepth {
			continue
		}

		candidates := current.original.sortUpstreamCandidates(j.JobName)
		upstreamLevel := current.depth + 1

		for i := 0; i < maxUpstreamsPerLevel && i < len(candidates); i++ {
			targetJobName := candidates[i].JobName
			for _, upstream := range current.original.Upstreams {
				if upstream.JobName != targetJobName {
					continue
				}

				latestJobRun := upstream.GetRunForJob(j.JobName)
				if latestJobRun != nil {
					result = append(result, &JobExecutionSummary{
						JobName:            upstream.JobName,
						SLA:                upstream.SLA,
						Level:              upstreamLevel,
						JobRunSummary:      latestJobRun,
						DownstreamPathName: current.original.JobName.String(),
						DelaySummary: &JobRunDelaySummary{
							ScheduledWayTooLateSeconds:   0,
							SystemSchedulingDelaySeconds: 0,
						},
					})

					if i == 0 {
						queue = append(queue, &nodeInfo{
							original: upstream,
							depth:    upstreamLevel,
						})
					}
				}
			}
		}
	}

	return result
}

type JobRunLineage struct {
	JobName          JobName
	ScheduledAt      time.Time
	JobRuns          []*JobExecutionSummary
	ExecutionSummary *LineageExecutionSummary
}

type LineageExecutionSummary struct {
	TotalScheduledWayTooLateSeconds     int64
	TotalSystemSchedulingDelaySeconds   int64
	AverageSystemSchedulingDelaySeconds int64

	TotalLineageDelaySeconds    int64
	TotalLineageDurationSeconds int64

	LargestScheduledWayTooLateJob   LineageDelaySummary
	LargestSystemSchedulingDelayJob LineageDelaySummary

	TopLongestTaskDurationJobs []JobWithTaskDuration
	TopLongestHookDurationJobs []JobWithTaskDuration
}

type JobWithTaskDuration struct {
	JobName      JobName
	ScheduledAt  time.Time
	TaskDuration time.Duration
	Level        int
}

type LineageDelaySummary struct {
	JobName             JobName
	ScheduledAt         time.Time
	UpstreamJobName     JobName
	UpstreamScheduledAt time.Time
	DelayDuration       int64
}

// JobExecutionSummary is a flattened version of JobLineageSummary
type JobExecutionSummary struct {
	JobName JobName
	SLA     SLAConfig
	// Level marks the distance from the original job in question
	Level              int
	JobRunSummary      *JobRunSummary
	DownstreamPathName string
	DelaySummary       *JobRunDelaySummary
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
	JobStatus     string
	WaitStartTime *time.Time
	WaitEndTime   *time.Time
	TaskStartTime *time.Time
	TaskEndTime   *time.Time
	HookStartTime *time.Time
	HookEndTime   *time.Time
}

func (j *JobRunSummary) GetActualEndTime() *time.Time {
	if j.HookEndTime != nil {
		return j.HookEndTime
	}
	return j.TaskEndTime
}

func (j *JobRunSummary) GetTaskDuration() time.Duration {
	if j.TaskStartTime == nil || j.TaskEndTime == nil {
		return 0
	}

	return j.TaskEndTime.Sub(*j.TaskStartTime)
}

func (j *JobRunSummary) GetHookDuration() time.Duration {
	if j.HookStartTime == nil || j.HookEndTime == nil {
		return 0
	}

	return j.HookEndTime.Sub(*j.HookStartTime)
}

type JobRunDelaySummary struct {
	ScheduledWayTooLateSeconds   int64
	SystemSchedulingDelaySeconds int64
}
