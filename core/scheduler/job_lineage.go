package scheduler

import (
	"sort"
	"time"

	"github.com/goto/optimus/core/tenant"
	"github.com/goto/optimus/internal/lib/window"
)

const (
	// MaxLineageDepth is a safeguard to avoid infinite recursion in case of unexpected cycles
	MaxLineageDepth = 20
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

type upstreamCandidate struct {
	JobName JobName
	EndTime time.Time
}

func (j *JobLineageSummary) PruneLineage(maxUpstreamsPerLevel int) *JobLineageSummary {
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

		if current.depth >= MaxLineageDepth {
			continue
		}

		upstreams := current.original.Upstreams
		candidates := extractUpstreamCandidatesSortedByDuration(current.original)

		for i := 0; i < maxUpstreamsPerLevel && i < len(candidates); i++ {
			targetJobName := candidates[i].JobName
			for _, upstream := range upstreams {
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

func extractUpstreamCandidatesSortedByDuration(lineage *JobLineageSummary) []upstreamCandidate {
	candidates := []upstreamCandidate{}

	for _, upstream := range lineage.Upstreams {
		latestFinishTime := getLatestFinishTime(upstream.JobRuns)
		candidates = append(candidates, upstreamCandidate{
			JobName: upstream.JobName,
			EndTime: latestFinishTime,
		})
	}

	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].EndTime.After(candidates[j].EndTime)
	})

	return candidates
}

func getLatestFinishTime(jobRuns map[string]*JobRunSummary) time.Time {
	var latestFinishTime time.Time

	for _, jobRun := range jobRuns {
		if jobRun.JobEndTime != nil && (latestFinishTime.IsZero() || jobRun.JobEndTime.After(latestFinishTime)) {
			latestFinishTime = *jobRun.JobEndTime
		}
	}

	return latestFinishTime
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
