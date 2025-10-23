package service

import (
	"context"
	"encoding/json"
	"time"

	"github.com/goto/salt/log"

	"github.com/goto/optimus/config"
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
	GetPercentileDurationByJobNames(ctx context.Context, referenceTime time.Time, jobNames []scheduler.JobName) (map[scheduler.JobName]*time.Duration, error)
	GetPercentileDurationByJobNamesByTask(ctx context.Context, referenceTime time.Time, jobNames []scheduler.JobName) (map[scheduler.JobName]*time.Duration, error)
	GetPercentileDurationByJobNamesByHookName(ctx context.Context, referenceTime time.Time, jobNames []scheduler.JobName, hookNames []string) (map[scheduler.JobName]*time.Duration, error)
}

type JobDetailsGetter interface {
	GetJobs(ctx context.Context, projectName tenant.ProjectName, jobs []string) ([]*scheduler.JobWithDetails, error)
	GetJobsByLabels(ctx context.Context, projectName tenant.ProjectName, labels map[string]string) ([]*scheduler.JobWithDetails, error)
}

type SLAPredictorRepository interface {
	StorePredictedSLABreach(ctx context.Context, jobTargetName, jobCauseName scheduler.JobName, jobScheduledAt time.Time, cause string, referenceTime time.Time, config map[string]interface{}, lineages []interface{}) error
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
	config            config.PotentialSLABreachConfig
	repo              SLAPredictorRepository
	jobDetailsGetter  JobDetailsGetter
	jobLineageFetcher JobLineageFetcher
	durationEstimator DurationEstimator
	tenantGetter      TenantGetter
	// alerting purpose
	potentialSLANotifier PotentialSLANotifier
}

func NewJobSLAPredictorService(l log.Logger, config config.PotentialSLABreachConfig, slaPredictorRepo SLAPredictorRepository, jobLineageFetcher JobLineageFetcher, durationEstimator DurationEstimator, jobDetailsGetter JobDetailsGetter, potentialSLANotifier PotentialSLANotifier, tenantGetter TenantGetter) *JobSLAPredictorService {
	return &JobSLAPredictorService{
		l:                    l,
		config:               config,
		repo:                 slaPredictorRepo,
		jobLineageFetcher:    jobLineageFetcher,
		durationEstimator:    durationEstimator,
		jobDetailsGetter:     jobDetailsGetter,
		tenantGetter:         tenantGetter,
		potentialSLANotifier: potentialSLANotifier,
	}
}

func (s *JobSLAPredictorService) IdentifySLABreaches(ctx context.Context, projectName tenant.ProjectName, referenceTime time.Time, scheduleRangeInHours time.Duration, jobNames []scheduler.JobName, labels map[string]string, enableAlert bool, severity string) (map[scheduler.JobName]map[scheduler.JobName]*JobState, error) {
	// map of jobName -> map of upstreamJobName -> JobState
	jobBreaches := make(map[scheduler.JobName]map[scheduler.JobName]*JobState)

	if len(jobNames) == 0 && len(labels) == 0 {
		s.l.Warn("no job names or labels provided, skipping SLA prediction")
		return jobBreaches, nil
	}

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
	jobSchedules := s.getJobSchedules(jobsWithDetails, scheduleRangeInHours, referenceTime)
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
	jobDurations, err := s.durationEstimator.GetPercentileDurationByJobNames(ctx, referenceTime, uniqueJobNames)
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
		breachesCausesPaths := s.identifySLABreach(jobWithLineage, jobDurations, targetedSLA, referenceTime)
		// populate jobBreaches
		if len(breachesCausesPaths) > 0 {
			jobBreaches[jobSchedule.JobName] = make(map[scheduler.JobName]*JobState)
			for _, causes := range breachesCausesPaths {
				if len(causes) == 0 {
					continue
				}
				cause := causes[len(causes)-1] // root cause is the last element in the path
				jobBreaches[jobSchedule.JobName][cause.JobName] = cause
			}
		}

		// store predicted SLA breach
		if s.config.EnablePersistentLogging {
			if err := s.storePredictedSLABreach(ctx, jobWithLineage, breachesCausesPaths, referenceTime); err != nil {
				s.l.Error("failed to store predicted SLA breaches", "error", err)
			}
		}
	}

	if enableAlert && len(jobBreaches) > 0 {
		s.l.Info("potential SLA breaches found", "count", len(jobBreaches))
		s.sendAlert(ctx, jobBreaches, severity)
	}

	return jobBreaches, nil
}

func (s *JobSLAPredictorService) identifySLABreach(jobTarget *scheduler.JobLineageSummary, jobDurations map[scheduler.JobName]*time.Duration, targetedSLA *time.Time, referenceTime time.Time) [][]*JobState {
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

func (s *JobSLAPredictorService) getJobSchedules(jobs []*scheduler.JobWithDetails, scheduleRangeInHours time.Duration, referenceTime time.Time) []*scheduler.JobSchedule {
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
		if scheduledAt.After(referenceTime.Add(scheduleRangeInHours)) || scheduledAt.Before(referenceTime.Add(-scheduleRangeInHours)) {
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
func (s *JobSLAPredictorService) calculateInferredSLAs(jobTarget *scheduler.JobLineageSummary, jobDurations map[scheduler.JobName]*time.Duration, targetedSLA *time.Time) map[scheduler.JobName]*time.Time {
	jobSLAs := make(map[scheduler.JobName]*time.Time)
	alpha := s.config.DamperCoeff // damper factor to reduce the impact of upstream jobs in higher levels
	// inferred SLA for leaf node = targetedSLA S(j)
	jobSLAs[jobTarget.JobName] = targetedSLA
	// bottom up calculation of inferred SLA for upstream jobs
	type state struct {
		job    *scheduler.JobLineageSummary
		damper float64
	}
	stack := []*state{{job: jobTarget, damper: alpha}}
	for len(stack) > 0 {
		current := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		job := current.job

		targetedInferredSLA := jobSLAs[job.JobName]
		if jobDurations[job.JobName] == nil || targetedInferredSLA == nil {
			continue
		}
		duration := jobDurations[job.JobName].Milliseconds()
		duration = int64(float64(duration) * current.damper)

		inferredSLA := targetedInferredSLA.Add(-time.Duration(duration) * time.Millisecond)
		for _, upstreamJob := range job.Upstreams {
			if _, ok := jobSLAs[upstreamJob.JobName]; ok {
				// already calculated, skip
				continue
			}
			jobSLAs[upstreamJob.JobName] = &inferredSLA
			stack = append(stack, &state{job: upstreamJob, damper: current.damper * alpha})
		}
	}

	return jobSLAs
}

// identifySLABreachRootCauses identifies if the given job might breach its SLA based on its upstream jobs and their inferred SLAs.
// if any upstream job u of a critical downstream job j meets either of the following conditions, it means job j might breach its SLA:
// - Given current time in UTC T(now), T(now)>= S(u|j) (the inferred SLA for u induced by j has passed) and the upstream job u has not completed yet. Or,
// - Given current time in UTC T(now), T(now)>= S(u|j) - D(u) (the inferred SLA for u induced by j minus the average duration of u has passed) and the upstream job u has not started yet.
// return the job that might breach its SLA
func (s *JobSLAPredictorService) identifySLABreachRootCauses(jobTarget *scheduler.JobLineageSummary, jobSLAStates map[scheduler.JobName]*JobSLAState, referenceTime time.Time) [][]*JobState {
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
	jobsWithLineageVisitedMap := make(map[scheduler.JobName]*scheduler.JobLineageSummary)
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
		jobsWithLineageVisitedMap[job.JobName] = job
		visited[job.JobName] = true

		if jobSLAStates[job.JobName] == nil || jobSLAStates[job.JobName].InferredSLA == nil || jobSLAStates[job.JobName].EstimatedDuration == nil { // less likely occur, but just in case
			continue
		}

		inferredSLA := *jobSLAStates[job.JobName].InferredSLA

		// check if job meets either of the conditions
		isPotentialBreach := false
		for _, jobRun := range job.JobRuns {
			// condition 1: T(now)>= S(u|j) and the job u has not completed yet
			if (referenceTime.After(inferredSLA) && jobRun.JobEndTime == nil) || (jobRun.JobEndTime != nil && jobRun.JobEndTime.After(inferredSLA)) {
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
	rootCauses := make([][]*JobState, 0)
	compactedPaths := compactingPaths(potentialBreachPaths)
	if len(compactedPaths) > 0 {
		for _, path := range compactedPaths {
			rootCause := make([]*JobState, 0)
			for _, jobName := range path {
				if jobState, ok := jobStateByName[jobName]; ok {
					rootCause = append(rootCause, jobState)
				}
			}
			rootCauses = append(rootCauses, rootCause)
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

// storePredictedSLABreach stores the predicted SLA breaches in the repository for further analysis.
func (s *JobSLAPredictorService) storePredictedSLABreach(ctx context.Context, jobTarget *scheduler.JobLineageSummary, paths [][]*JobState, referenceTime time.Time) error {
	for _, path := range paths {
		if len(path) == 0 {
			continue
		}
		scheduledAt := time.Time{}
		for _, jobRun := range jobTarget.JobRuns {
			scheduledAt = jobRun.ScheduledAt
			break
		}
		config := map[string]interface{}{}
		rawConfig, err := json.Marshal(s.config)
		if err != nil {
			return err
		}
		if err := json.Unmarshal(rawConfig, &config); err != nil {
			return err
		}
		cause := path[len(path)-1]

		lineages := []interface{}{}
		rawLineage, err := json.Marshal(path)
		if err != nil {
			return err
		}
		if err := json.Unmarshal(rawLineage, &lineages); err != nil {
			return err
		}
		err = s.repo.StorePredictedSLABreach(ctx, jobTarget.JobName, cause.JobName, scheduledAt, string(cause.Status), referenceTime, config, lineages)
		if err != nil {
			return err
		}
	}
	return nil
}

// sendAlert sends an alert to the potentialSLANotifier with the job breaches information.
func (s *JobSLAPredictorService) sendAlert(ctx context.Context, jobBreaches map[scheduler.JobName]map[scheduler.JobName]*JobState, severity string) {
	jobToUpstreamsCauseByTenant := make(map[tenant.Tenant]map[string][]scheduler.UpstreamAttrs)
	for jobName, upstreamCauses := range jobBreaches {
		for _, upstreamCause := range upstreamCauses {
			if _, ok := jobToUpstreamsCauseByTenant[upstreamCause.Tenant]; !ok {
				jobToUpstreamsCauseByTenant[upstreamCause.Tenant] = make(map[string][]scheduler.UpstreamAttrs)
			}
			upstreamAttr := scheduler.UpstreamAttrs{
				JobName:       upstreamCause.JobName.String(),
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

// compactingPaths compacts the given paths to only include the leaf nodes.
// For example, given paths:
// A->B
// A->B->C
// A->B->C->D
// A->Z->C
// A->Z->X
// A->B->Y
// The result will be:
// A->B->C->D
// A->Z->X
// A->B->Y
func compactingPaths(paths [][]scheduler.JobName) [][]scheduler.JobName {
	prefixes := make(map[scheduler.JobName]bool)

	for _, path := range paths {
		for i, node := range path {
			if i < len(path)-1 { // prefix
				prefixes[node] = true
			}
		}
	}

	// result is equal to paths that contains ending nodes that are not prefixes
	var result [][]scheduler.JobName
	for _, path := range paths {
		ending := path[len(path)-1]
		if _, isPrefix := prefixes[ending]; !isPrefix {
			result = append(result, path)
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
