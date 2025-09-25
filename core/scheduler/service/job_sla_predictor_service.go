package service

import (
	"context"
	"time"

	"github.com/goto/optimus/core/scheduler"
	"github.com/goto/salt/log"
)

type DurationEstimator interface {
	GetP95DurationByJobNames(ctx context.Context, jobNames []scheduler.JobName, lastNDays int) (map[scheduler.JobName]time.Duration, error)
}

type jobSLAState struct {
	EstimatedDuration    *time.Duration
	InferredSLAByJobName map[scheduler.JobName]*time.Time // inferred SLA for this job induced by each downstream critical job
}

type JobSLAPredictorService struct {
	l                 log.Logger
	buffer            time.Duration
	lastNDays         int
	jobLineageFetcher JobLineageFetcher
	durationEstimator DurationEstimator
}

func NewJobSLAPredictorService(l log.Logger, buffer time.Duration, lastNDays int, jobLineageFetcher JobLineageFetcher, durationEstimator DurationEstimator) *JobSLAPredictorService {
	return &JobSLAPredictorService{
		l:                 l,
		buffer:            buffer,
		lastNDays:         lastNDays,
		jobLineageFetcher: jobLineageFetcher,
		durationEstimator: durationEstimator,
	}
}

// PredictJobSLAs predicts job SLAs for the given jobs based on their lineage and estimated durations.
// It returns a list of jobs that might breach their SLAs along with the paths of upstream jobs causing the potential breach.
// The prediction is based on the following logic:
// 1. Fetch job lineage for the given jobs.
// 2. Estimate the duration for each job using P95 of the last N days plus a buffer.
// 3. Infer SLAs for each job based on their downstream critical jobs and estimated durations.
// 4. Identify jobs that might breach their SLAs based on current time and inferred SLAs.
// Precondition: cyclic dependency should be handled in validation, so here we can safely assume no cyclic dependency
func (s *JobSLAPredictorService) PredictJobSLAs(ctx context.Context, jobs []*scheduler.JobSchedule, targetedSLA time.Time) (map[*scheduler.JobLineageSummary][]*scheduler.JobLineageSummary, error) {
	// get job lineage first
	jobsWithLineage, err := s.jobLineageFetcher.GetJobLineage(ctx, jobs)
	if err != nil {
		return nil, err
	}

	// recursively get all upstream job names
	jobNamesMap := map[scheduler.JobName]bool{}
	queue := jobsWithLineage
	for len(queue) > 0 {
		job := queue[0]
		queue = queue[1:]
		jobNamesMap[job.JobName] = true
		queue = append(queue, job.Upstreams...)
	}
	jobNames := make([]scheduler.JobName, 0, len(jobNamesMap))
	for jobName := range jobNamesMap {
		jobNames = append(jobNames, jobName)
	}

	// initialize jobSLAStates to hold estimated duration and inferred SLAs for each job
	jobSLAStates := make(map[scheduler.JobName]*jobSLAState)
	for jobName := range jobNamesMap {
		jobSLAStates[jobName] = &jobSLAState{
			EstimatedDuration:    nil,
			InferredSLAByJobName: make(map[scheduler.JobName]*time.Time),
		}
	}

	// calculate estimated duration for each job, P95 of N last runs + buffer, D(j) = P95(j) + buffer
	jobSLAStates, err = s.calculateEstimatedDuration(ctx, jobNames, jobsWithLineage, jobSLAStates, targetedSLA)
	if err != nil {
		return nil, err
	}

	// calculate inferred SLAs for each job based on their downstream critical jobs and estimated durations
	// S(u|j) = S(j) - D(u)
	jobSLAStates, err = s.calculateInferredSLAs(jobsWithLineage, jobSLAStates, targetedSLA)
	if err != nil {
		return nil, err
	}

	// identify jobs that might breach their SLAs based on current time and inferred SLAs
	// T(now)>= S(u|j) and the job u has not completed yet
	// T(now)>= S(u|j) - D(u) and the job u has not started yet
	return s.identifyPotentialBreachJobs(jobsWithLineage, jobSLAStates, time.Now().UTC())
}

// calculateEstimatedDuration calculates the estimated duration for each job using P95 of the last N days plus a buffer.
// It updates the jobSLAStates map with the estimated durations.
func (s *JobSLAPredictorService) calculateEstimatedDuration(ctx context.Context, jobNames []scheduler.JobName, jobsWithLineage []*scheduler.JobLineageSummary, jobSLAStates map[scheduler.JobName]*jobSLAState, targetedSLA time.Time) (map[scheduler.JobName]*jobSLAState, error) {
	jobDurations, err := s.durationEstimator.GetP95DurationByJobNames(ctx, jobNames, s.lastNDays)
	if err != nil {
		return nil, err
	}
	// assign duration to each job in the lineage
	queue := jobsWithLineage
	for len(queue) > 0 {
		job := queue[0]
		queue = queue[1:]
		if duration, ok := jobDurations[job.JobName]; ok {
			estimatedDuration := duration + s.buffer
			jobSLAStates[job.JobName].EstimatedDuration = &estimatedDuration
		}
		queue = append(queue, job.Upstreams...)
	}
	return jobSLAStates, nil
}

// calculateInferredSLAs calculates the inferred SLAs for each job based on their downstream critical jobs and estimated durations.
// infer SLA for each job based on its jobs and their inferred SLAs. bottom up calculation, leaf node should meet targetedSLA
// for an upstream job u and a downstream critical job j with SLA S(j) and average duration D(j), the inferred SLA for u induced by j (S(u|j)) = S(j) - D(u)
// suppose, there's a chain of jobs: u2 -> u1 -> j, where u2 is upstream of u1, and u1 is upstream of j. The inferred SLA for u2 induced by j would be:
// S(u2|j) = S(u1|j) - D(u1)
// such that, the inferred SLA for any upstream job in level n un induced by a downstream job j as:
// S(un|j) = S(un-1|j) - D(un-1)
// where, S(u0|j) = S(j), D(u0) = D(j)
func (s *JobSLAPredictorService) calculateInferredSLAs(jobsWithLineage []*scheduler.JobLineageSummary, jobSLAStates map[scheduler.JobName]*jobSLAState, targetedSLA time.Time) (map[scheduler.JobName]*jobSLAState, error) {
	for _, jobTarget := range jobsWithLineage {
		// inferred SLA for leaf node = targetedSLA S(j)
		jobSLAStates[jobTarget.JobName].InferredSLAByJobName[jobTarget.JobName] = &targetedSLA
		// bottom up calculation of inferred SLA for upstream jobs
		stack := []*scheduler.JobLineageSummary{jobTarget}
		for len(stack) > 0 {
			job := stack[len(stack)-1]
			stack = stack[:len(stack)-1]
			targetedInferredSLA := jobSLAStates[job.JobName].InferredSLAByJobName[jobTarget.JobName]
			if jobSLAStates[job.JobName].EstimatedDuration == nil || targetedInferredSLA == nil {
				continue
			}
			inferredSLA := targetedInferredSLA.Add(-*jobSLAStates[job.JobName].EstimatedDuration)
			for _, upstreamJob := range job.Upstreams {
				jobSLAStates[upstreamJob.JobName].InferredSLAByJobName[jobTarget.JobName] = &inferredSLA
				stack = append(stack, upstreamJob)
			}
		}
	}
	return jobSLAStates, nil
}

// identifyPotentialBreachJobs identifies jobs that might breach their SLAs based on current time and inferred SLAs.
// if any upstream job u of a critical downstream job j meets either of the following conditions, it means job j might breach its SLA:
// - Given current time in UTC T(now), T(now)>= S(u|j) (the inferred SLA for u induced by j has passed) and the upstream job u has not completed yet. Or,
// - Given current time in UTC T(now), T(now)>= S(u|j) - D(u) (the inferred SLA for u induced by j minus the average duration of u has passed) and the upstream job u has not started yet.
// return the job that might breach its SLA
func (s *JobSLAPredictorService) identifyPotentialBreachJobs(jobsWithLineage []*scheduler.JobLineageSummary, jobSLAStates map[scheduler.JobName]*jobSLAState, referenceTime time.Time) (map[*scheduler.JobLineageSummary][]*scheduler.JobLineageSummary, error) {
	potentialBreachPaths := make(map[*scheduler.JobLineageSummary][][]*scheduler.JobLineageSummary)
	for _, jobTarget := range jobsWithLineage {
		potentialBreachPaths[jobTarget] = [][]*scheduler.JobLineageSummary{}
		// DFS to traverse all upstream jobs with paths
		type state struct {
			job   *scheduler.JobLineageSummary
			paths []*scheduler.JobLineageSummary
		}
		stack := []*state{{job: jobTarget, paths: []*scheduler.JobLineageSummary{}}}
		for len(stack) > 0 {
			jobWithState := stack[len(stack)-1]
			stack = stack[:len(stack)-1]
			job := jobWithState.job
			paths := append(jobWithState.paths, job)
			if jobSLAStates[job.JobName].InferredSLAByJobName[jobTarget.JobName] == nil || jobSLAStates[job.JobName].EstimatedDuration == nil {
				continue
			}
			for _, upstreamJob := range job.Upstreams {
				stack = append(stack, &state{job: upstreamJob, paths: paths})
			}
			if job.JobName == jobTarget.JobName {
				// skip the target job itself
				continue
			}
			inferredSLA := *jobSLAStates[job.JobName].InferredSLAByJobName[jobTarget.JobName]
			// check if job meets either of the conditions
			// condition 1: T(now)>= S(u|j) and the job u has not completed yet
			if referenceTime.After(inferredSLA) && job.JobRuns[jobTarget.JobName.String()].TaskEndTime == nil {
				// found a job that might breach its SLA, return the jobTarget
				potentialBreachPaths[jobTarget] = append(potentialBreachPaths[jobTarget], paths)
			}
			// condition 2: T(now)>= S(u|j) - D(u) and the job u has not started yet
			if referenceTime.After(inferredSLA.Add(-*jobSLAStates[job.JobName].EstimatedDuration)) && job.JobRuns[jobTarget.JobName.String()].TaskStartTime == nil {
				// found a job that might breach its SLA, return the jobTarget
				potentialBreachPaths[jobTarget] = append(potentialBreachPaths[jobTarget], paths)
			}
		}
	}
	// find root causes from potentialBreachPaths
	rootCauses := make(map[*scheduler.JobLineageSummary][]*scheduler.JobLineageSummary)
	for jobTarget, paths := range potentialBreachPaths {
		leaves := findLeaves(paths)
		if len(leaves) > 0 {
			rootCauses[jobTarget] = leaves
		}
	}
	return rootCauses, nil
}

// findLeaves finds the leaf nodes from the given paths.
// For example, given paths:
// A->B
// A->B->C
// A->B->C->D
// A->Z->C
// A->Z->X
// A->B->Y
// The leaf nodes are D, Y, X
func findLeaves(paths [][]*scheduler.JobLineageSummary) []*scheduler.JobLineageSummary {
	endings := make(map[*scheduler.JobLineageSummary]bool)
	prefixes := make(map[*scheduler.JobLineageSummary]bool)

	for _, path := range paths {
		for i, node := range path {
			if i < len(path)-1 { // prefix
				prefixes[node] = true
			} else { // last element
				endings[node] = true
			}
		}
	}

	// result = endings - prefixes
	var result []*scheduler.JobLineageSummary
	for node := range endings {
		if !prefixes[node] {
			result = append(result, node)
		}
	}
	return result
}
