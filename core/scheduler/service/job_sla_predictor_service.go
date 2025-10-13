package service

import (
	"context"
	"time"

	"github.com/goto/salt/log"

	"github.com/goto/optimus/core/scheduler"
	"github.com/goto/optimus/core/tenant"
)

type SLABreachCause string

const (
	SLABreachCauseNotStarted  SLABreachCause = "NOT_STARTED"
	SLABreachCauseRunningLate SLABreachCause = "RUNNING_LATE"
)

type PotentialSLANotifier interface {
	SendPotentialSLABreach(attr *scheduler.PotentialSLABreachAttrs)
}

type DurationEstimator interface {
	GetPercentileDurationByJobNames(ctx context.Context, jobNames []scheduler.JobName) (map[scheduler.JobName]*time.Duration, error)
	GetPercentileDurationByJobNamesByTask(ctx context.Context, jobNames []scheduler.JobName) (map[scheduler.JobName]*time.Duration, error)
	GetPercentileDurationByJobNamesByHookName(ctx context.Context, jobNames []scheduler.JobName, hookNames []string) (map[scheduler.JobName]*time.Duration, error)
}

type JobDetailsGetter interface {
	GetJobs(ctx context.Context, projectName tenant.ProjectName, jobs []string) ([]*scheduler.JobWithDetails, error)
	GetJobsByLabels(ctx context.Context, projectName tenant.ProjectName, labels map[string]string) ([]*scheduler.JobWithDetails, error)
}

type JobState struct {
	JobSLAState
	JobName       scheduler.JobName
	Tenant        tenant.Tenant
	RelativeLevel int
	Status        SLABreachCause
}

type JobSLAState struct {
	EstimatedDuration *time.Duration
	InferredSLA       *time.Time
}

type JobSLAPredictorService struct {
	l                 log.Logger
	jobDetailsGetter  JobDetailsGetter
	jobLineageFetcher JobLineageFetcher
	durationEstimator DurationEstimator
	tenantGetter      TenantGetter
	// alerting purpose
	potentialSLANotifier PotentialSLANotifier
}

func NewJobSLAPredictorService(l log.Logger, jobLineageFetcher JobLineageFetcher, durationEstimator DurationEstimator, jobDetailsGetter JobDetailsGetter, potentialSLANotifier PotentialSLANotifier, tenantGetter TenantGetter) *JobSLAPredictorService {
	return &JobSLAPredictorService{
		l:                    l,
		jobLineageFetcher:    jobLineageFetcher,
		durationEstimator:    durationEstimator,
		jobDetailsGetter:     jobDetailsGetter,
		tenantGetter:         tenantGetter,
		potentialSLANotifier: potentialSLANotifier,
	}
}

func (s *JobSLAPredictorService) IdentifySLABreaches(ctx context.Context, projectName tenant.ProjectName, nextScheduleRangeInHours time.Duration, jobNames []scheduler.JobName, labels map[string]string, enableAlert bool, severity string) (map[scheduler.JobName]map[scheduler.JobName]*JobState, error) {
	// map of jobName -> map of upstreamJobName -> JobState
	jobBreaches := make(map[scheduler.JobName]map[scheduler.JobName]*JobState)

	if len(jobNames) == 0 && len(labels) == 0 {
		s.l.Warn("no job names or labels provided, skipping SLA prediction")
		return jobBreaches, nil
	}

	// reference time is now in UTC
	referenceTime := time.Now().UTC()

	// get jobs with details
	jobsWithDetails, err := s.getJobWithDetails(ctx, projectName, jobNames, labels)
	if err != nil {
		s.l.Error("failed to get jobs with details, skipping SLA prediction", "error", err)
		return nil, err
	}
	if len(jobsWithDetails) == 0 {
		return jobBreaches, nil
	}

	// get targetedSLA
	targetedSLA := s.getTargetedSLA(jobsWithDetails, referenceTime)
	if len(targetedSLA) == 0 {
		s.l.Warn("no targeted SLA found for the given jobs, skipping SLA prediction")
		return jobBreaches, nil
	}

	// get scheduled at
	jobSchedules := s.getJobSchedules(jobsWithDetails, nextScheduleRangeInHours, referenceTime)
	if len(jobSchedules) == 0 {
		s.l.Warn("no job schedules found for the given jobs in the next schedule range, skipping SLA prediction")
		return jobBreaches, nil
	}

	// get lineage
	jobsWithLineageMap, err := s.jobLineageFetcher.GetJobLineage(ctx, jobSchedules)
	if err != nil {
		s.l.Error("failed to get job lineage, skipping SLA prediction", "error", err)
		return nil, err
	}

	uniqueJobNames := collectJobNames(jobsWithLineageMap)

	// get job durations estimation
	jobDurations, err := s.durationEstimator.GetPercentileDurationByJobNames(ctx, uniqueJobNames)
	if err != nil {
		s.l.Error("failed to get job duration estimation, skipping SLA prediction", "error", err)
		return nil, err
	}

	for _, jobSchedule := range jobSchedules {
		// identify potential breach
		jobWithLineage, ok := jobsWithLineageMap[jobSchedule.JobName]
		if !ok || jobWithLineage == nil {
			continue
		}
		targetedSLA, ok := targetedSLA[jobSchedule.JobName]
		if !ok || targetedSLA == nil {
			continue
		}
		breachesCauses := s.identifySLABreach(jobWithLineage, jobDurations, targetedSLA, referenceTime)
		if len(breachesCauses) > 0 {
			jobBreaches[jobSchedule.JobName] = breachesCauses
		}
	}

	if enableAlert && len(jobBreaches) > 0 {
		s.l.Info("potential SLA breaches found", "count", len(jobBreaches))
		s.sendAlert(ctx, jobBreaches, severity)
	}

	return jobBreaches, nil
}

func (s *JobSLAPredictorService) identifySLABreach(jobTarget *scheduler.JobLineageSummary, jobDurations map[scheduler.JobName]*time.Duration, targetedSLA *time.Time, referenceTime time.Time) map[scheduler.JobName]*JobState {
	// calculate inferred SLAs for each job based on their downstream critical jobs and estimated durations
	// S(u|j) = S(j) - D(u)
	jobSLAStatesByJobTarget := s.calculateInferredSLAs(jobTarget, jobDurations, targetedSLA)

	// populate jobSLAStatesByJobTargetName
	jobSLAStates := s.populateJobSLAStates(jobDurations, jobSLAStatesByJobTarget)

	// identify jobs that might breach their SLAs based on current time and inferred SLAs
	// T(now)>= S(u|j) and the job u has not completed yet
	// T(now)>= S(u|j) - D(u) and the job u has not started yet
	jobSLABreachCauses := s.identifySLABreachRootCauses(jobTarget, jobSLAStates, referenceTime)

	return jobSLABreachCauses
}

func (s JobSLAPredictorService) getJobWithDetails(ctx context.Context, projectName tenant.ProjectName, jobNames []scheduler.JobName, labels map[string]string) ([]*scheduler.JobWithDetails, error) {
	filteredJobSchedules := []*scheduler.JobWithDetails{}

	if len(jobNames) > 0 {
		jobNameStr := []string{}
		for _, jn := range jobNames {
			jobNameStr = append(jobNameStr, string(jn))
		}
		jobsWithDetails, err := s.jobDetailsGetter.GetJobs(ctx, projectName, jobNameStr)
		if err != nil {
			return nil, err
		}
		filteredJobSchedules = append(filteredJobSchedules, jobsWithDetails...)
	}

	if len(labels) > 0 {
		jobsWithDetails, err := s.jobDetailsGetter.GetJobsByLabels(ctx, projectName, labels)
		if err != nil {
			return nil, err
		}
		filteredJobSchedules = append(filteredJobSchedules, jobsWithDetails...)
	}

	return filteredJobSchedules, nil
}

func (s *JobSLAPredictorService) getTargetedSLA(jobs []*scheduler.JobWithDetails, referenceTime time.Time) map[scheduler.JobName]*time.Time {
	targetedSLAByJobName := make(map[scheduler.JobName]*time.Time)
	for _, job := range jobs {
		if job.Schedule == nil {
			s.l.Warn("job does not have schedule, skipping SLA prediction", "job", job.Name)
			continue
		}
		slaDuration, err := job.SLADuration()
		if err != nil {
			s.l.Warn("failed to get SLA duration for job", "job", job.Name, "error", err)
			continue
		}
		if slaDuration == 0 {
			s.l.Warn("SLA duration is not set for job, skipping SLA prediction", "job", job.Name)
			continue
		}
		scheduledAt, err := job.Schedule.GetNextSchedule(referenceTime)
		if err != nil {
			s.l.Warn("failed to get scheduled at for job", "job", job.Name, "error", err)
			continue
		}
		sla := scheduledAt.Add(time.Duration(slaDuration) * time.Second)
		targetedSLAByJobName[job.Name] = &sla
	}

	return targetedSLAByJobName
}

func (s *JobSLAPredictorService) getJobSchedules(jobs []*scheduler.JobWithDetails, nextScheduleRangeInHours time.Duration, referenceTime time.Time) []*scheduler.JobSchedule {
	jobSchedules := make([]*scheduler.JobSchedule, 0, len(jobs))
	for _, job := range jobs {
		if job.Schedule == nil {
			continue
		}
		scheduledAt, err := job.Schedule.GetNextSchedule(referenceTime)
		if err != nil {
			s.l.Warn("failed to get scheduled at for job, skipping SLA prediction", "job", job.Name, "error", err)
			continue
		}
		if scheduledAt.After(referenceTime.Add(nextScheduleRangeInHours)) {
			continue
		}
		jobSchedules = append(jobSchedules, &scheduler.JobSchedule{
			JobName:     job.Name,
			ScheduledAt: scheduledAt,
		})
	}
	return jobSchedules
}

// calculateInferredSLAs calculates the inferred SLAs for each job based on their downstream critical jobs and estimated durations.
// infer SLA for each job based on its jobs and their inferred SLAs. bottom up calculation, leaf node should meet targetedSLA
// for an upstream job u and a downstream critical job j with SLA S(j) and average duration D(j), the inferred SLA for u induced by j (S(u|j)) = S(j) - D(u)
// suppose, there's a chain of jobs: u2 -> u1 -> j, where u2 is upstream of u1, and u1 is upstream of j. The inferred SLA for u2 induced by j would be:
// S(u2|j) = S(u1|j) - D(u1)
// such that, the inferred SLA for any upstream job in level n un induced by a downstream job j as:
// S(un|j) = S(un-1|j) - D(un-1)
// where, S(u0|j) = S(j), D(u0) = D(j)
func (*JobSLAPredictorService) calculateInferredSLAs(jobTarget *scheduler.JobLineageSummary, jobDurations map[scheduler.JobName]*time.Duration, targetedSLA *time.Time) map[scheduler.JobName]*time.Time {
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
			if _, ok := jobSLAs[upstreamJob.JobName]; ok {
				// already calculated, skip
				continue
			}
			jobSLAs[upstreamJob.JobName] = &inferredSLA
			stack = append(stack, upstreamJob)
		}
	}

	return jobSLAs
}

// identifySLABreachRootCauses identifies if the given job might breach its SLA based on its upstream jobs and their inferred SLAs.
// if any upstream job u of a critical downstream job j meets either of the following conditions, it means job j might breach its SLA:
// - Given current time in UTC T(now), T(now)>= S(u|j) (the inferred SLA for u induced by j has passed) and the upstream job u has not completed yet. Or,
// - Given current time in UTC T(now), T(now)>= S(u|j) - D(u) (the inferred SLA for u induced by j minus the average duration of u has passed) and the upstream job u has not started yet.
// return the job that might breach its SLA
func (s *JobSLAPredictorService) identifySLABreachRootCauses(jobTarget *scheduler.JobLineageSummary, jobSLAStates map[scheduler.JobName]*JobSLAState, referenceTime time.Time) map[scheduler.JobName]*JobState {
	jobStateByName := make(map[scheduler.JobName]*JobState)
	potentialBreachPaths := make([][]scheduler.JobName, 0)
	// DFS to traverse all upstream jobs with paths
	type state struct {
		job   *scheduler.JobLineageSummary
		paths []scheduler.JobName
		level int
	}
	stack := []*state{}
	// start from immediate upstream jobs
	for _, upstreamJob := range jobTarget.Upstreams {
		stack = append(stack, &state{job: upstreamJob, paths: []scheduler.JobName{jobTarget.JobName}, level: 1})
	}
	visited := make(map[scheduler.JobName]bool)
	for len(stack) > 0 {
		jobWithState := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		job := jobWithState.job
		paths := make([]scheduler.JobName, len(jobWithState.paths))
		copy(paths, jobWithState.paths)
		paths = append(paths, job.JobName) //nolint:makezero

		if visited[job.JobName] {
			continue
		}
		visited[job.JobName] = true

		if jobSLAStates[job.JobName].InferredSLA == nil || jobSLAStates[job.JobName].EstimatedDuration == nil { // less likely occur, but just in case
			continue
		}

		inferredSLA := *jobSLAStates[job.JobName].InferredSLA

		isPotentialBreach := false
		for _, jobRun := range job.JobRuns {
			// check if job meets either of the conditions
			// condition 1: T(now)>= S(u|j) and the job u has not completed yet
			if referenceTime.After(inferredSLA) && jobRun != nil && (jobRun.JobEndTime == nil || inferredSLA.Before(*jobRun.JobEndTime)) {
				// found a job that might breach its SLA
				potentialBreachPaths = append(potentialBreachPaths, paths)
				// add to jobStateByName
				jobStateByName[job.JobName] = &JobState{
					JobSLAState:   *jobSLAStates[job.JobName],
					JobName:       job.JobName,
					Tenant:        job.Tenant,
					RelativeLevel: jobWithState.level,
					Status:        SLABreachCauseRunningLate,
				}

				isPotentialBreach = true
			}

			// condition 2: T(now)>= S(u|j) - D(u) and the job u has not started yet
			if referenceTime.After(inferredSLA.Add(-*jobSLAStates[job.JobName].EstimatedDuration)) && (jobRun == nil || jobRun.TaskStartTime == nil) {
				// found a job that might breach its SLA
				potentialBreachPaths = append(potentialBreachPaths, paths)
				// add to jobStateByName
				jobStateByName[job.JobName] = &JobState{
					JobSLAState:   *jobSLAStates[job.JobName],
					JobName:       job.JobName,
					Tenant:        job.Tenant,
					RelativeLevel: jobWithState.level,
					Status:        SLABreachCauseNotStarted,
				}

				isPotentialBreach = true
			}
		}

		if isPotentialBreach {
			s.l.Info("potential SLA breach found", "job", job.JobName, "inferred_sla", inferredSLA, "duration", jobSLAStates[job.JobName].EstimatedDuration, "path", paths)
		}

		for _, upstreamJob := range job.Upstreams {
			stack = append(stack, &state{job: upstreamJob, paths: paths, level: jobWithState.level + 1})
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
	return rootCauses
}

// populateJobSLAStates populates the jobSLAStatesByJobName map with the estimated durations and inferred SLAs for each job.
func (*JobSLAPredictorService) populateJobSLAStates(jobDurations map[scheduler.JobName]*time.Duration, jobSLAsByJobName map[scheduler.JobName]*time.Time) map[scheduler.JobName]*JobSLAState {
	jobSLAStatesByJobName := make(map[scheduler.JobName]*JobSLAState)
	for jobName, inferredSLA := range jobSLAsByJobName {
		jobSLAStatesByJobName[jobName] = &JobSLAState{
			EstimatedDuration: jobDurations[jobName],
			InferredSLA:       inferredSLA,
		}
	}
	return jobSLAStatesByJobName
}

// sendAlert sends an alert to the potentialSLANotifier with the job breaches information.
func (s *JobSLAPredictorService) sendAlert(ctx context.Context, jobBreaches map[scheduler.JobName]map[scheduler.JobName]*JobState, severity string) {
	jobToUpstreamsCause := make(map[string][]scheduler.UpstreamAttrs)
	for jobName, upstreamCauses := range jobBreaches {
		upstreamAttrs := make([]scheduler.UpstreamAttrs, 0, len(upstreamCauses))
		for _, cause := range upstreamCauses {
			upstreamAttrs = append(upstreamAttrs, scheduler.UpstreamAttrs{
				JobName:       jobName.String(),
				RelativeLevel: cause.RelativeLevel,
				Status:        string(cause.Status),
			})
		}
		jobToUpstreamsCause[jobName.String()] = upstreamAttrs
	}

	jobToUpstreamsCauseByTenant := make(map[tenant.Tenant]map[string][]scheduler.UpstreamAttrs)
	for jobName, upstreamCauses := range jobBreaches {
		for _, upstreamCause := range upstreamCauses {
			if _, ok := jobToUpstreamsCauseByTenant[upstreamCause.Tenant]; !ok {
				jobToUpstreamsCauseByTenant[upstreamCause.Tenant] = make(map[string][]scheduler.UpstreamAttrs)
			}
			upstreamAttr := scheduler.UpstreamAttrs{
				JobName:       jobName.String(),
				RelativeLevel: upstreamCause.RelativeLevel,
				Status:        string(upstreamCause.Status),
			}
			jobToUpstreamsCauseByTenant[upstreamCause.Tenant][jobName.String()] = append(jobToUpstreamsCauseByTenant[upstreamCause.Tenant][jobName.String()], upstreamAttr)
		}
	}

	for t, jobToUpstreamsCause := range jobToUpstreamsCauseByTenant {
		tenantWithDetails, err := s.tenantGetter.GetDetails(ctx, t)
		if err != nil {
			s.l.Error("failed to get tenant details for tenant %s: %v", t.String(), err)
			continue
		}
		teamName, err := tenantWithDetails.GetConfig(tenant.ProjectAlertManagerTeam)
		if err != nil {
			s.l.Error("failed to get default team for tenant %s: %v", t.String(), err)
			continue
		}
		if teamName == "" {
			s.l.Warn("no default team configured for tenant %s, skip sending alert", t.String())
			continue
		}
		s.potentialSLANotifier.SendPotentialSLABreach(&scheduler.PotentialSLABreachAttrs{
			TeamName:            teamName,
			JobToUpstreamsCause: jobToUpstreamsCause,
			Severity:            severity,
		})
	}
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

	// result is equal to endings minus prefixes
	var result []scheduler.JobName
	for node := range endings {
		if !prefixes[node] {
			result = append(result, node)
		}
	}
	return result
}

func collectJobNames(jobsWithLineage map[scheduler.JobName]*scheduler.JobLineageSummary) []scheduler.JobName {
	jobNamesMap := map[scheduler.JobName]bool{}
	stack := []*scheduler.JobLineageSummary{}
	for _, job := range jobsWithLineage {
		stack = append(stack, job)
	}
	// BFS to traverse all jobs
	for len(stack) > 0 {
		job := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if _, ok := jobNamesMap[job.JobName]; ok {
			continue
		}
		jobNamesMap[job.JobName] = true
		stack = append(stack, job.Upstreams...)
	}
	jobNames := make([]scheduler.JobName, 0, len(jobNamesMap))
	for jobName := range jobNamesMap {
		jobNames = append(jobNames, jobName)
	}
	return jobNames
}
