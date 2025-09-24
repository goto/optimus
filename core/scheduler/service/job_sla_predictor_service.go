package service

import (
	"context"
	"time"

	"github.com/goto/optimus/core/scheduler"
)

type DurationEstimator interface {
	GetP95DurationByJobNames(ctx context.Context, jobNames []scheduler.JobName, lastNDays int) (map[scheduler.JobName]time.Duration, error)
}

type JobSLAPredictorService struct {
	jobLineageFetcher JobLineageFetcher
	durationEstimator DurationEstimator
}

func NewJobSLAPredictorService(jobLineageFetcher JobLineageFetcher) *JobSLAPredictorService {
	return &JobSLAPredictorService{
		jobLineageFetcher: jobLineageFetcher,
	}
}

func (s *JobSLAPredictorService) PredictJobSLAs(ctx context.Context, jobs []*scheduler.JobSchedule, targetedSLA time.Time) ([]*scheduler.JobLineageSummary, map[scheduler.JobName][]scheduler.JobName, error) {
	// get job lineage first
	jobsWithLineage, err := s.jobLineageFetcher.GetJobLineage(ctx, jobs)
	if err != nil {
		return nil, nil, err
	}
	// recursively get all upstream job names
	jobNames := []scheduler.JobName{}
	queue := jobsWithLineage
	for len(queue) > 0 {
		job := queue[0]
		queue = queue[1:]
		jobNames = append(jobNames, job.JobName)
		for _, upstreamJob := range job.Upstreams {
			queue = append(queue, upstreamJob)
		}
	}
	// calculate estimated duration for each job, P95 of 7 last runs + buffer, D(j) = P95(j) + buffer
	jobDurations, err := s.durationEstimator.GetP95DurationByJobNames(ctx, jobNames, 7)
	if err != nil {
		return nil, nil, err
	}
	// assign duration to each job in the lineage
	buffer := 10 * time.Minute // TODO: configurable
	queue = jobsWithLineage
	for len(queue) > 0 {
		job := queue[0]
		queue = queue[1:]
		if duration, ok := jobDurations[job.JobName]; ok {
			estimatedDuration := duration + buffer
			job.EstimatedDuration = &estimatedDuration
		}
		for _, upstreamJob := range job.Upstreams {
			queue = append(queue, upstreamJob)
		}
	}

	// infer SLA for each job based on its jobs and their inferred SLAs. bottom up calculation, leaf node should meet targetedSLA
	// for an upstream job u and a downstream critical job j with SLA S(j) and average duration D(j), the inferred SLA for u induced by j (S(u|j)) = S(j) - D(u)
	// suppose, there's a chain of jobs: u2 -> u1 -> j, where u2 is upstream of u1, and u1 is upstream of j. The inferred SLA for u2 induced by j would be:
	// S(u2|j) = S(u1|j) - D(u1)
	// such that, the inferred SLA for any upstream job in level n un induced by a downstream job j as:
	// S(un|j) = S(un-1|j) - D(un-1)
	// where, S(u0|j) = S(j), D(u0) = D(j)
	for _, jobTarget := range jobsWithLineage {
		// inferred SLA for leaf node = targetedSLA S(j)
		jobTarget.InferredSLAByJobName = make(map[scheduler.JobName]*time.Time)
		jobTarget.InferredSLAByJobName[jobTarget.JobName] = &targetedSLA
		// bottom up calculation of inferred SLA for upstream jobs
		stack := []*scheduler.JobLineageSummary{jobTarget}
		for len(stack) > 0 {
			job := stack[len(stack)-1]
			stack = stack[:len(stack)-1]
			targetedInferredSLA := job.InferredSLAByJobName[jobTarget.JobName]
			if job.EstimatedDuration == nil || targetedInferredSLA == nil {
				continue
			}
			inferredSLA := targetedInferredSLA.Add(-*job.EstimatedDuration)
			for _, upstreamJob := range job.Upstreams {
				upstreamJob.InferredSLAByJobName[jobTarget.JobName] = &inferredSLA
				stack = append(stack, upstreamJob)
			}
		}
	}

	// if any upstream job u of a critical downstream job j meets either of the following conditions, it means job j might breach its SLA:
	// - Given current time in UTC T(now), T(now)>= S(u|j) (the inferred SLA for u induced by j has passed) and the upstream job u has not completed yet. Or,
	// - Given current time in UTC T(now), T(now)>= S(u|j) - D(u) (the inferred SLA for u induced by j minus the average duration of u has passed) and the upstream job u has not started yet.
	// return the job that might breach its SLA
	currentTime := time.Now().UTC()
	potentialBreachJobs := []*scheduler.JobLineageSummary{}
	paths := make(map[scheduler.JobName][]scheduler.JobName)
	for _, jobTarget := range jobsWithLineage {
		// DFS to traverse all upstream jobs
		stack := []*scheduler.JobLineageSummary{jobTarget}
		path := []scheduler.JobName{}
		for len(stack) > 0 {
			job := stack[len(stack)-1]
			stack = stack[:len(stack)-1]
			path = append(path, job.JobName)
			if job.InferredSLAByJobName[jobTarget.JobName] == nil || job.EstimatedDuration == nil {
				continue
			}
			for _, upstreamJob := range job.Upstreams {
				stack = append(stack, upstreamJob)
			}
			if job.JobName == jobTarget.JobName {
				// skip the target job itself
				continue
			}
			inferredSLA := *job.InferredSLAByJobName[jobTarget.JobName]
			// check if job meets either of the conditions
			// condition 1: T(now)>= S(u|j) and the job u has not completed yet
			if currentTime.After(inferredSLA) && job.JobRuns[jobTarget.JobName.String()].TaskEndTime == nil {
				// found a job that might breach its SLA, return the jobTarget
				potentialBreachJobs = append(potentialBreachJobs, jobTarget)
				paths[jobTarget.JobName] = make([]scheduler.JobName, len(path))
				copy(paths[jobTarget.JobName], path)
				break
			}
			// condition 2: T(now)>= S(u|j) - D(u) and the job u has not started yet
			if currentTime.After(inferredSLA.Add(-*job.EstimatedDuration)) && job.JobRuns[jobTarget.JobName.String()].TaskStartTime == nil {
				// found a job that might breach its SLA, return the jobTarget
				potentialBreachJobs = append(potentialBreachJobs, jobTarget)
				paths[jobTarget.JobName] = make([]scheduler.JobName, len(path))
				copy(paths[jobTarget.JobName], path)
				break
			}
			path = path[:len(path)-1] // backtrack
		}
	}
	return potentialBreachJobs, paths, nil
}
