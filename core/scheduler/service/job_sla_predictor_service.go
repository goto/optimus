package service

import (
	"context"
	"time"

	"github.com/goto/optimus/core/scheduler"
	"github.com/goto/optimus/core/tenant"
	"github.com/goto/salt/log"
)

type DurationEstimator interface {
	GetP95DurationByJobNames(ctx context.Context, jobNames []scheduler.JobName) (map[scheduler.JobName]*time.Duration, error)
}

type JobDetailsGetter interface {
	GetJobWithDetails(ctx context.Context, projectName tenant.ProjectName, jobNames []scheduler.JobName, labels map[string]string) ([]*scheduler.JobWithDetails, error)
}

type JobState struct {
	JobSLAState
	JobName       scheduler.JobName
	Tenant        tenant.Tenant
	RelativeLevel int
	Status        string
}

type JobSLAState struct {
	EstimatedDuration *time.Duration
	InferredSLA       *time.Time
}

type JobSLAPredictorService struct {
	l                 log.Logger
	jobLineageFetcher JobLineageFetcher
	durationEstimator DurationEstimator
	jobDetailsGetter  JobDetailsGetter
}

func NewJobSLAPredictorService(l log.Logger, jobLineageFetcher JobLineageFetcher, durationEstimator DurationEstimator, jobDetailsGetter JobDetailsGetter) *JobSLAPredictorService {
	return &JobSLAPredictorService{
		l:                 l,
		jobLineageFetcher: jobLineageFetcher,
		durationEstimator: durationEstimator,
		jobDetailsGetter:  jobDetailsGetter,
	}
}

func (s *JobSLAPredictorService) IdentifySLABreaches(ctx context.Context, projectName tenant.ProjectName, nextScheduleRangeInHours time.Duration, jobNames []scheduler.JobName, labels map[string]string) (map[scheduler.JobName]map[scheduler.JobName]*JobState, error) {
	jobsWithDetails, err := s.jobDetailsGetter.GetJobWithDetails(ctx, projectName, jobNames, labels)
	if err != nil {
		return nil, err
	}

	referenceTime := time.Now().UTC()

	// get targetedSLA
	targetedSLA, err := s.getTargetedSLA(jobsWithDetails)
	if err != nil {
		return nil, err
	}

	// get scheduled at
	jobSchedules, err := s.getJobSchedules(jobsWithDetails, nextScheduleRangeInHours, referenceTime)
	if err != nil {
		return nil, err
	}

	// get lineage
	jobsWithLineageMap, err := s.jobLineageFetcher.GetJobLineage(ctx, jobSchedules)
	if err != nil {
		return nil, err
	}

	uniqueJobNames := collectJobNames(jobsWithLineageMap)

	// get job durations estimation
	jobDurations, err := s.durationEstimator.GetP95DurationByJobNames(ctx, uniqueJobNames)

	resp := make(map[scheduler.JobName]map[scheduler.JobName]*JobState)
	for _, jobSchedule := range jobSchedules {
		// identify potential breach
		jobWithLineage, ok := jobsWithLineageMap[jobSchedule]
		if !ok || jobWithLineage == nil {
			continue
		}
		targetedSLA, ok := targetedSLA[jobSchedule.JobName]
		if !ok || targetedSLA == nil {
			continue
		}
		breachesCauses, err := s.identifySLABreach(jobWithLineage, jobDurations, targetedSLA, referenceTime)
		if err != nil {
			return nil, err
		}
		if len(breachesCauses) > 0 {
			resp[jobSchedule.JobName] = breachesCauses
		}
	}

	return resp, nil
}

func (s *JobSLAPredictorService) identifySLABreach(jobTarget *scheduler.JobLineageSummary, jobDurations map[scheduler.JobName]*time.Duration, targetedSLA *time.Time, referenceTime time.Time) (map[scheduler.JobName]*JobState, error) {
	// calculate inferred SLAs for each job based on their downstream critical jobs and estimated durations
	// S(u|j) = S(j) - D(u)
	jobSLAStatesByJobTarget, err := s.calculateInferredSLAs(jobTarget, jobDurations, targetedSLA)
	if err != nil {
		return nil, err
	}

	// populate jobSLAStatesByJobTargetName
	jobSLAStates := s.populateJobSLAStates(jobDurations, jobSLAStatesByJobTarget)

	// identify jobs that might breach their SLAs based on current time and inferred SLAs
	// T(now)>= S(u|j) and the job u has not completed yet
	// T(now)>= S(u|j) - D(u) and the job u has not started yet
	jobSLABreachCauses, err := s.identifySLABreachRootCauses(jobTarget, jobSLAStates, referenceTime)
	if err != nil {
		return nil, err
	}

	return jobSLABreachCauses, nil
}

func (s *JobSLAPredictorService) getTargetedSLA(jobs []*scheduler.JobWithDetails) (map[scheduler.JobName]*time.Time, error) {
	targetedSLAByJobName := make(map[scheduler.JobName]*time.Time)
	for _, job := range jobs {
		if job.Schedule == nil {
			continue
		}
		slaDuration, err := job.SLADuration()
		if err != nil {
			return nil, err
		}
		if slaDuration == 0 {
			continue
		}
		scheduledAt, err := job.Schedule.GetScheduleStartTime()
		if err != nil {
			return nil, err
		}
		sla := scheduledAt.Add(time.Duration(slaDuration) * time.Second)
		targetedSLAByJobName[job.Name] = &sla
	}

	return targetedSLAByJobName, nil
}

func (s *JobSLAPredictorService) getJobSchedules(jobs []*scheduler.JobWithDetails, nextScheduleRangeInHours time.Duration, referenceTime time.Time) ([]*scheduler.JobSchedule, error) {
	jobSchedules := make([]*scheduler.JobSchedule, 0, len(jobs))
	for _, job := range jobs {
		if job.Schedule == nil {
			continue
		}
		scheduledAt, err := job.Schedule.GetScheduleStartTime()
		if err != nil {
			return nil, err
		}
		if scheduledAt.After(referenceTime.Add(nextScheduleRangeInHours)) {
			continue
		}
		jobSchedules = append(jobSchedules, &scheduler.JobSchedule{
			JobName:     job.Name,
			ScheduledAt: scheduledAt,
		})
	}
	return jobSchedules, nil
}

// calculateInferredSLAs calculates the inferred SLAs for each job based on their downstream critical jobs and estimated durations.
// infer SLA for each job based on its jobs and their inferred SLAs. bottom up calculation, leaf node should meet targetedSLA
// for an upstream job u and a downstream critical job j with SLA S(j) and average duration D(j), the inferred SLA for u induced by j (S(u|j)) = S(j) - D(u)
// suppose, there's a chain of jobs: u2 -> u1 -> j, where u2 is upstream of u1, and u1 is upstream of j. The inferred SLA for u2 induced by j would be:
// S(u2|j) = S(u1|j) - D(u1)
// such that, the inferred SLA for any upstream job in level n un induced by a downstream job j as:
// S(un|j) = S(un-1|j) - D(un-1)
// where, S(u0|j) = S(j), D(u0) = D(j)
func (s *JobSLAPredictorService) calculateInferredSLAs(jobTarget *scheduler.JobLineageSummary, jobDurations map[scheduler.JobName]*time.Duration, targetedSLA *time.Time) (map[scheduler.JobName]*time.Time, error) {
	jobSLAs := make(map[scheduler.JobName]*time.Time)
	// inferred SLA for leaf node = targetedSLA S(j)
	jobSLAs[jobTarget.JobName] = targetedSLA
	// bottom up calculation of inferred SLA for upstream jobs
	stack := []*scheduler.JobLineageSummary{jobTarget}
	for len(stack) > 0 {
		job := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		targetedInferredSLA := jobSLAs[job.JobName]
		if jobDurations[job.JobName] == nil || targetedInferredSLA == nil {
			continue
		}
		inferredSLA := targetedInferredSLA.Add(-*jobDurations[job.JobName])
		for _, upstreamJob := range job.Upstreams {
			jobSLAs[upstreamJob.JobName] = &inferredSLA
			stack = append(stack, upstreamJob)
		}
	}

	return jobSLAs, nil
}

// identifySLABreachRootCauses identifies if the given job might breach its SLA based on its upstream jobs and their inferred SLAs.
// if any upstream job u of a critical downstream job j meets either of the following conditions, it means job j might breach its SLA:
// - Given current time in UTC T(now), T(now)>= S(u|j) (the inferred SLA for u induced by j has passed) and the upstream job u has not completed yet. Or,
// - Given current time in UTC T(now), T(now)>= S(u|j) - D(u) (the inferred SLA for u induced by j minus the average duration of u has passed) and the upstream job u has not started yet.
// return the job that might breach its SLA
func (s *JobSLAPredictorService) identifySLABreachRootCauses(jobTarget *scheduler.JobLineageSummary, jobSLAStates map[scheduler.JobName]*JobSLAState, referenceTime time.Time) (map[scheduler.JobName]*JobState, error) {
	jobStateByName := make(map[scheduler.JobName]*JobState)
	potentialBreachPaths := make([][]scheduler.JobName, 0)
	// DFS to traverse all upstream jobs with paths
	type state struct {
		job   *scheduler.JobLineageSummary
		paths []scheduler.JobName
		level int
	}
	stack := []*state{{job: jobTarget, paths: []scheduler.JobName{}, level: 0}}
	for len(stack) > 0 {
		jobWithState := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		job := jobWithState.job
		paths := append(jobWithState.paths, job.JobName)
		if jobSLAStates[job.JobName].InferredSLA == nil || jobSLAStates[job.JobName].EstimatedDuration == nil {
			continue
		}
		for _, upstreamJob := range job.Upstreams {
			stack = append(stack, &state{job: upstreamJob, paths: paths, level: jobWithState.level + 1})
		}
		if job.JobName == jobTarget.JobName {
			// skip the target job itself
			continue
		}
		inferredSLA := *jobSLAStates[job.JobName].InferredSLA
		// check if job meets either of the conditions
		// condition 1: T(now)>= S(u|j) and the job u has not completed yet
		if referenceTime.After(inferredSLA) && job.JobRuns[jobTarget.JobName.String()].TaskEndTime == nil {
			// found a job that might breach its SLA
			potentialBreachPaths = append(potentialBreachPaths, paths)
			// add to jobStateByName
			if _, ok := jobStateByName[job.JobName]; !ok {
				jobStateByName[job.JobName] = &JobState{
					JobSLAState:   *jobSLAStates[job.JobName],
					JobName:       job.JobName,
					Tenant:        job.Tenant,
					RelativeLevel: jobWithState.level,
					Status:        "RUNNING_LATE",
				}
			}
			// update relative level if current level is higher or equal, because we want to know the farthest upstream job
			if jobWithState.level >= jobStateByName[job.JobName].RelativeLevel {
				jobStateByName[job.JobName].RelativeLevel = jobWithState.level
			}
		}
		// condition 2: T(now)>= S(u|j) - D(u) and the job u has not started yet
		if referenceTime.After(inferredSLA.Add(-*jobSLAStates[job.JobName].EstimatedDuration)) && job.JobRuns[jobTarget.JobName.String()].TaskStartTime == nil {
			// found a job that might breach its SLA
			potentialBreachPaths = append(potentialBreachPaths, paths)
			// add to jobStateByName
			if _, ok := jobStateByName[job.JobName]; !ok {
				jobStateByName[job.JobName] = &JobState{
					JobSLAState:   *jobSLAStates[job.JobName],
					JobName:       job.JobName,
					Tenant:        job.Tenant,
					RelativeLevel: jobWithState.level,
					Status:        "NOT_STARTED",
				}
			}
			// update relative level if current level is higher or equal, because we want to know the farthest upstream job
			if jobWithState.level >= jobStateByName[job.JobName].RelativeLevel {
				jobStateByName[job.JobName].RelativeLevel = jobWithState.level
			}
		}
	}

	// find root causes from potentialBreachPaths
	// root causes are the leaf nodes in the potentialBreachPaths
	rootCauses := make(map[scheduler.JobName]*JobState)
	leaves := findLeaves(potentialBreachPaths)
	if len(leaves) > 0 {
		for _, leaf := range leaves {
			if jobState, ok := jobStateByName[leaf]; ok {
				rootCauses[leaf] = jobState
			}
		}
	}
	return rootCauses, nil
}

// populateJobSLAStates populates the jobSLAStatesByJobName map with the estimated durations and inferred SLAs for each job.
func (s *JobSLAPredictorService) populateJobSLAStates(jobDurations map[scheduler.JobName]*time.Duration, jobSLAsByJobName map[scheduler.JobName]*time.Time) map[scheduler.JobName]*JobSLAState {
	JobSLAStatesByJobName := make(map[scheduler.JobName]*JobSLAState)
	for jobName, inferredSLA := range jobSLAsByJobName {
		JobSLAStatesByJobName[jobName] = &JobSLAState{
			EstimatedDuration: jobDurations[jobName],
			InferredSLA:       inferredSLA,
		}
	}
	return JobSLAStatesByJobName
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
func findLeaves(paths [][]scheduler.JobName) []scheduler.JobName {
	endings := make(map[scheduler.JobName]bool)
	prefixes := make(map[scheduler.JobName]bool)

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
	var result []scheduler.JobName
	for node := range endings {
		if !prefixes[node] {
			result = append(result, node)
		}
	}
	return result
}

func collectJobNames(jobsWithLineage map[*scheduler.JobSchedule]*scheduler.JobLineageSummary) []scheduler.JobName {
	jobNamesMap := map[scheduler.JobName]bool{}
	queue := []*scheduler.JobLineageSummary{}
	for _, job := range jobsWithLineage {
		queue = append(queue, job)
	}
	// BFS to traverse all jobs
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
	return jobNames
}
