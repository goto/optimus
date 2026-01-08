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
	MaxLineageDepth = 20
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

// PruneLineage prunes the upstream lineage to limit the number of upstreams per level
// by selecting maxUpstreamsPerLevel upstreams based on their latest job run finish time
func (j *JobLineageSummary) PruneLineage(maxUpstreamsPerLevel, maxDepth int) *JobLineageSummary {
	type nodeInfo struct {
		original *JobLineageSummary
		pruned   *JobLineageSummary
		depth    int
	}

	rootPruned := &JobLineageSummary{
		JobName:          j.JobName,
		Tenant:           j.Tenant,
		Window:           j.Window,
		ScheduleInterval: j.ScheduleInterval,
		SLA:              j.SLA,
		JobRuns:          j.JobRuns,
	}

	queue := []*nodeInfo{{
		original: j,
		pruned:   rootPruned,
		depth:    0,
	}}

	processed := make(map[JobName]*JobLineageSummary)
	processed[j.JobName] = queue[0].pruned

	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]

		if current.depth >= maxDepth {
			continue
		}

		candidates := current.original.sortUpstreamCandidates(j.JobName)

		for i := 0; i < maxUpstreamsPerLevel && i < len(candidates); i++ {
			targetJobName := candidates[i].JobName
			for _, upstream := range current.original.Upstreams {
				if upstream.JobName == targetJobName {
					if existingPruned, exists := processed[upstream.JobName]; exists {
						current.pruned.Upstreams = append(current.pruned.Upstreams, existingPruned)
					} else {
						prunedUpstream := &JobLineageSummary{
							JobName:          upstream.JobName,
							Tenant:           upstream.Tenant,
							Window:           upstream.Window,
							ScheduleInterval: upstream.ScheduleInterval,
							SLA:              upstream.SLA,
							JobRuns:          upstream.JobRuns,
						}
						current.pruned.Upstreams = append(current.pruned.Upstreams, prunedUpstream)
						processed[upstream.JobName] = prunedUpstream
						queue = append(queue, &nodeInfo{
							original: upstream,
							pruned:   prunedUpstream,
							depth:    current.depth + 1,
						})
					}
					break
				}
			}
		}
	}

	return rootPruned
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

func (j *JobLineageSummary) GenerateLineageExecutionSummary(maxDepth int) *JobRunLineage {
	if j == nil {
		return nil
	}

	executionSummaries := j.flatten(maxDepth)
	// determine lineage execution summary
	lineageSummary := &LineageExecutionSummary{}
	var largestScheduledWayTooLate, largestSystemSchedulingDelay *JobExecutionSummary
	var largestWayTooLateUpstream, largestSystemSchedulingUpstream *JobRunSummary
	wayTooLateCount := 0

	for i := len(executionSummaries) - 1; i >= 0; i-- {
		currentExec := executionSummaries[i]
		currentRun := currentExec.JobRunSummary

		if currentRun.GetActualEndTime() == nil {
			continue
		}

		scheduledToTaskStartDuration := currentRun.TaskStartTime.Sub(currentRun.ScheduledAt)

		var upstreamLastTaskEndToCurrentTaskStartDuration time.Duration
		var upstreamRun *JobRunSummary
		if i < len(executionSummaries)-1 {
			for j := i + 1; j < len(executionSummaries); j++ {
				upstreamRun = executionSummaries[j].JobRunSummary
				if upstreamRun.GetActualEndTime() != nil {
					upstreamLastTaskEndToCurrentTaskStartDuration = currentRun.TaskStartTime.Sub(*upstreamRun.GetActualEndTime())
					break
				}
			}
		}

		if upstreamLastTaskEndToCurrentTaskStartDuration > scheduledToTaskStartDuration {
			currentScheduledWayTooLate := upstreamLastTaskEndToCurrentTaskStartDuration - scheduledToTaskStartDuration
			currentExec.DelaySummary.ScheduledWayTooLateSeconds = int64(currentScheduledWayTooLate.Seconds())
			lineageSummary.TotalScheduledWayTooLateSeconds += currentExec.DelaySummary.ScheduledWayTooLateSeconds
			wayTooLateCount++

			if largestScheduledWayTooLate == nil || currentScheduledWayTooLate.Seconds() > float64(largestScheduledWayTooLate.DelaySummary.ScheduledWayTooLateSeconds) {
				largestScheduledWayTooLate = currentExec
				largestWayTooLateUpstream = upstreamRun
			}

			currentExec.DelaySummary.SystemSchedulingDelaySeconds = int64(scheduledToTaskStartDuration.Seconds())
		} else {
			currentExec.DelaySummary.SystemSchedulingDelaySeconds = int64(upstreamLastTaskEndToCurrentTaskStartDuration.Seconds())
		}

		lineageSummary.TotalSystemSchedulingDelaySeconds += currentExec.DelaySummary.SystemSchedulingDelaySeconds
		if largestSystemSchedulingDelay == nil || currentExec.DelaySummary.SystemSchedulingDelaySeconds > largestSystemSchedulingDelay.DelaySummary.SystemSchedulingDelaySeconds {
			largestSystemSchedulingDelay = currentExec
			largestSystemSchedulingUpstream = upstreamRun
		}
	}

	if len(executionSummaries) > 0 {
		lineageSummary.AverageSystemSchedulingDelaySeconds = lineageSummary.TotalSystemSchedulingDelaySeconds / int64(len(executionSummaries))
	}

	lineageSummary.TotalLineageDelaySeconds = lineageSummary.TotalScheduledWayTooLateSeconds + lineageSummary.TotalSystemSchedulingDelaySeconds
	lineageSummary.TotalLineageDurationSeconds = int64(executionSummaries[0].JobRunSummary.GetActualEndTime().Sub(*executionSummaries[len(executionSummaries)-1].JobRunSummary.TaskStartTime).Seconds())

	if largestScheduledWayTooLate != nil {
		lineageSummary.LargestScheduledWayTooLateJob = LineageDelaySummary{
			JobName:             largestScheduledWayTooLate.JobName,
			ScheduledAt:         largestScheduledWayTooLate.JobRunSummary.ScheduledAt,
			DelayDuration:       largestScheduledWayTooLate.DelaySummary.ScheduledWayTooLateSeconds,
			UpstreamJobName:     largestWayTooLateUpstream.JobName,
			UpstreamScheduledAt: largestWayTooLateUpstream.ScheduledAt,
		}
	}

	if largestSystemSchedulingDelay != nil {
		lineageSummary.LargestSystemSchedulingDelayJob = LineageDelaySummary{
			JobName:             largestSystemSchedulingDelay.JobName,
			ScheduledAt:         largestSystemSchedulingDelay.JobRunSummary.ScheduledAt,
			DelayDuration:       largestSystemSchedulingDelay.DelaySummary.SystemSchedulingDelaySeconds,
			UpstreamJobName:     largestSystemSchedulingUpstream.JobName,
			UpstreamScheduledAt: largestSystemSchedulingUpstream.ScheduledAt,
		}
	}

	return &JobRunLineage{
		JobName:          j.JobName,
		ScheduledAt:      j.GetRunForJob(j.JobName).ScheduledAt,
		JobRuns:          executionSummaries,
		ExecutionSummary: lineageSummary,
	}
}

func (j *JobLineageSummary) flatten(maxDepth int) []*JobExecutionSummary {
	var result []*JobExecutionSummary
	queue := []*JobLineageSummary{j}
	level := 0

	for len(queue) > 0 && level < maxDepth {
		levelSize := len(queue)

		for range levelSize {
			current := queue[0]
			queue = queue[1:]

			latestJobRun := current.GetRunForJob(j.JobName)
			if latestJobRun != nil {
				result = append(result, &JobExecutionSummary{
					JobName:            current.JobName,
					SLA:                current.SLA,
					Level:              level,
					JobRunSummary:      latestJobRun,
					DownstreamPathName: j.JobName.String(),
					DelaySummary: &JobRunDelaySummary{
						ScheduledWayTooLateSeconds:   0,
						SystemSchedulingDelaySeconds: 0,
					},
				})
			}

			queue = append(queue, current.Upstreams...)
		}

		level++
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

type JobRunDelaySummary struct {
	ScheduledWayTooLateSeconds   int64
	SystemSchedulingDelaySeconds int64
}
