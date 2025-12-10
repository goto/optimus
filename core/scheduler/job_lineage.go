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

func (j *JobLineageSummary) Flatten(maxDepth int) []*JobExecutionSummary {

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
