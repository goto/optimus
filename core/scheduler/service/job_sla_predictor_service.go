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
	for _, job := range jobsWithLineage {
		// inferred SLA for leaf node = targetedSLA S(j)
		job.InferredSLA = &targetedSLA
	}
	stack := jobsWithLineage
	for len(stack) > 0 {
		job := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		targetedInferredSLA := job.InferredSLA
		for _, upstreamJob := range job.Upstreams {
			if upstreamJob.EstimatedDuration == nil || targetedInferredSLA == nil {
				continue
			}
			inferredSLA := targetedInferredSLA.Add(-*upstreamJob.EstimatedDuration)
			// take the earliest inferred SLA if there are multiple downstream jobs referencing the same upstream job
			if upstreamJob.InferredSLA == nil || inferredSLA.Before(*upstreamJob.InferredSLA) {
				upstreamJob.InferredSLA = &inferredSLA
			}
			stack = append(stack, upstreamJob)
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
		jobNameTarget := jobTarget.JobName
		stack = []*scheduler.JobLineageSummary{jobTarget}
		path := []scheduler.JobName{}
		// DFS to traverse all upstream jobs
		for len(stack) > 0 {
			job := stack[len(stack)-1]
			stack = stack[:len(stack)-1]
			path = append(path, job.JobName)
			if job.InferredSLA == nil || job.EstimatedDuration == nil {
				continue
			}
			for _, upstreamJob := range job.Upstreams {
				stack = append(stack, upstreamJob)
			}
			if job.JobName == jobNameTarget {
				// skip the target job itself
				continue
			}
			// check if job meets either of the conditions
			// condition 1: T(now)>= S(u|j) and the job u has not completed yet
			if currentTime.After(*job.InferredSLA) && jobTarget.JobRuns[jobNameTarget.String()].TaskEndTime == nil {
				// found a job that might breach its SLA, return the jobTarget
				potentialBreachJobs = append(potentialBreachJobs, jobTarget)
				paths[jobNameTarget] = path
				break
			}
			// condition 2: T(now)>= S(u|j) - D(u) and the job u has not started yet
			if currentTime.After(job.InferredSLA.Add(-*job.EstimatedDuration)) && jobTarget.JobRuns[jobNameTarget.String()].TaskStartTime == nil {
				// found a job that might breach its SLA, return the jobTarget
				potentialBreachJobs = append(potentialBreachJobs, jobTarget)
				paths[jobNameTarget] = path
				break
			}
			path = path[:len(path)-1] // backtrack
		}
	}
	return potentialBreachJobs, paths, nil
}
