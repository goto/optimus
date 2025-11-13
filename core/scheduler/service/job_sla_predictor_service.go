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

type JobSLAPredictorRequestConfig struct {
	ReferenceTime        time.Time
	ScheduleRangeInHours time.Duration
	EnableAlert          bool
	EnableDeduplication  bool
	DamperCoeff          float64
	Severity             string
}

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
	GetPredictedSLAJobNamesWithinTimeRange(ctx context.Context, from, to time.Time) ([]scheduler.JobName, error)
}

type JobState struct {
	JobSLAState
	JobName       scheduler.JobName
	JobRun        scheduler.JobRunSummary
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

func (s *JobSLAPredictorService) IdentifySLABreaches(ctx context.Context, projectName tenant.ProjectName, jobNames []scheduler.JobName, labels map[string]string, reqConfig JobSLAPredictorRequestConfig) (map[scheduler.JobName]map[scheduler.JobName]*JobState, error) {
	// map of jobName -> map of upstreamJobName -> JobState
	jobBreachCauses := make(map[scheduler.JobName]map[scheduler.JobName]*JobState)
	jobFullBreachCauses := make(map[scheduler.JobName]map[scheduler.JobName][]*JobState)

	// damper coefficient to use default if not provided
	damperCoeff := reqConfig.DamperCoeff
	if damperCoeff <= 0 {
		damperCoeff = s.config.DamperCoeff
	}

	if len(jobNames) == 0 && len(labels) == 0 {
		s.l.Warn("no job names or labels provided, skipping SLA prediction")
		return jobBreachCauses, nil
	}

	// get jobs with details
	jobsWithDetails, err := s.getJobWithDetails(ctx, projectName, jobNames, labels)
	if err != nil {
		s.l.Error("failed to get jobs with details, skipping SLA prediction", "error", err)
		return nil, err
	}
	if len(jobsWithDetails) == 0 {
		return jobBreachCauses, nil
	}

	// get scheduled at
	jobSchedules := s.getJobSchedules(jobsWithDetails, reqConfig.ScheduleRangeInHours, reqConfig.ReferenceTime)
	if len(jobSchedules) == 0 {
		s.l.Warn("no job schedules found for the given jobs in the next schedule range, skipping SLA prediction")
		return jobBreachCauses, nil
	}

	// get targetedSLA
	targetedSLA := s.getTargetedSLA(jobsWithDetails, jobSchedules)
	if len(targetedSLA) == 0 {
		s.l.Warn("no targeted SLA found for the given jobs, skipping SLA prediction")
		return jobBreachCauses, nil
	}

	// get lineage
	jobsWithLineageMap, err := s.jobLineageFetcher.GetJobLineage(ctx, jobSchedules)
	if err != nil {
		s.l.Error("failed to get job lineage, skipping SLA prediction", "error", err)
		return nil, err
	}

	uniqueJobNames := collectJobNames(jobsWithLineageMap)

	// get job durations estimation
	jobDurations, err := s.durationEstimator.GetPercentileDurationByJobNames(ctx, reqConfig.ReferenceTime, uniqueJobNames)
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
		breachesCauses, fullBreachesCauses := s.identifySLABreach(jobWithLineage, jobDurations, targetedSLA, reqConfig.ReferenceTime, damperCoeff)
		// populate jobBreachCauses
		jobBreachCauses[jobSchedule.JobName] = breachesCauses
		// populate jobFullBreachCauses for logging and storage purpose
		jobFullBreachCauses[jobSchedule.JobName] = fullBreachesCauses
	}

	// send alert if enabled
	if reqConfig.EnableAlert && len(jobBreachCauses) > 0 {
		s.l.Info("potential SLA breaches found", "count", len(jobBreachCauses))
		jobBreachCausesForAlert := jobBreachCauses
		// deduplicate based on jobName if enabled
		if reqConfig.EnableDeduplication {
			jobBreachCausesForAlert, err = s.deduplicateJobBreaches(ctx, reqConfig.ScheduleRangeInHours, reqConfig.ReferenceTime, jobBreachCauses)
			if err != nil {
				s.l.Error("failed to deduplicate job breaches, sending alerts without deduplication", "error", err)
			}
		}
		s.sendAlert(ctx, jobBreachCausesForAlert, reqConfig.Severity)
	}

	// store predicted SLA breach
	if s.config.EnablePersistentLogging {
		for jobName, fullBreachesCausesPaths := range jobFullBreachCauses {
			jobTarget := jobsWithLineageMap[jobName]
			if err := s.storePredictedSLABreach(ctx, jobTarget, fullBreachesCausesPaths, reqConfig.ReferenceTime); err != nil {
				s.l.Error("failed to store predicted SLA breaches", "error", err)
			}
		}
	}

	return jobBreachCauses, nil
}

func (s *JobSLAPredictorService) identifySLABreach(jobTarget *scheduler.JobLineageSummary, jobDurations map[scheduler.JobName]*time.Duration, targetedSLA *time.Time, referenceTime time.Time, damperCoeff float64) (map[scheduler.JobName]*JobState, map[scheduler.JobName][]*JobState) {
	// calculate inferred SLAs for each job based on their downstream critical jobs and estimated durations
	// S(u|j) = S(j) - D(u)
	jobSLAStatesByJobTarget := s.calculateInferredSLAs(jobTarget, jobDurations, targetedSLA, damperCoeff)

	// populate jobSLAStatesByJobTargetName
	jobSLAStates := s.populateJobSLAStates(jobDurations, jobSLAStatesByJobTarget)

	// identify jobs that might breach their SLAs based on current time and inferred SLAs
	// T(now)>= S(u|j) and the job u has not completed yet
	// T(now)>= S(u|j) - D(u) and the job u has not started yet
	rootCauses, fullRootCauses := s.identifySLABreachRootCauses(jobTarget, jobSLAStates, referenceTime)

	// populate breachesCauses
	breachesCauses := make(map[scheduler.JobName]*JobState)
	for _, causes := range rootCauses {
		if len(causes) == 0 {
			continue
		}
		cause := causes[len(causes)-1] // root cause is the last element in the path
		breachesCauses[cause.JobName] = cause
	}

	// populate fullBreachesCauses
	fullBreachesCauses := make(map[scheduler.JobName][]*JobState)
	for _, causes := range fullRootCauses {
		if len(causes) == 0 {
			continue
		}
		cause := causes[len(causes)-1] // root cause is the last element in the path (as a unique identifier)
		fullBreachesCauses[cause.JobName] = causes
	}

	return breachesCauses, fullBreachesCauses
}

func (s JobSLAPredictorService) getJobWithDetails(ctx context.Context, projectName tenant.ProjectName, jobNames []scheduler.JobName, labels map[string]string) ([]*scheduler.JobWithDetails, error) {
	filteredJobsByName := map[scheduler.JobName]*scheduler.JobWithDetails{}
	filteredJobByLabel := map[scheduler.JobName]*scheduler.JobWithDetails{}
	filteredJobMerged := map[scheduler.JobName]*scheduler.JobWithDetails{}

	if len(jobNames) > 0 {
		jobNameStr := []string{}
		for _, jn := range jobNames {
			jobNameStr = append(jobNameStr, string(jn))
		}
		jobsWithDetails, err := s.jobDetailsGetter.GetJobs(ctx, projectName, jobNameStr)
		if err != nil {
			return nil, err
		}
		for _, job := range jobsWithDetails {
			filteredJobsByName[job.Name] = job
			filteredJobMerged[job.Name] = job
		}
		s.l.Info("fetched jobs by names", "count", len(filteredJobsByName))
		s.l.Info("jobs fetched by names", "jobs", filteredJobsByName)
	}

	if len(labels) > 0 {
		jobsWithDetails, err := s.jobDetailsGetter.GetJobsByLabels(ctx, projectName, labels)
		if err != nil {
			return nil, err
		}
		for _, job := range jobsWithDetails {
			filteredJobByLabel[job.Name] = job
			filteredJobMerged[job.Name] = job
		}
		s.l.Info("fetched jobs by labels", "count", len(filteredJobByLabel))
		s.l.Info("jobs fetched by labels", "jobs", filteredJobByLabel)
	}

	filteredJobSchedules := []*scheduler.JobWithDetails{}
	for _, job := range filteredJobMerged {
		filteredJobSchedules = append(filteredJobSchedules, job)
	}
	s.l.Info("total jobs fetched after merging by names and labels", "count", len(filteredJobSchedules))

	return filteredJobSchedules, nil
}

func (s *JobSLAPredictorService) getTargetedSLA(jobs []*scheduler.JobWithDetails, jobSchedules map[scheduler.JobName]*scheduler.JobSchedule) map[scheduler.JobName]*time.Time {
	targetedSLAByJobName := make(map[scheduler.JobName]*time.Time)
	s.l.Info("getting targeted SLAs for jobs", "count", len(jobs))
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
		schedule, ok := jobSchedules[job.Name]
		if !ok {
			s.l.Warn("failed to get scheduled at for job", "job", job.Name)
			continue
		}
		sla := schedule.ScheduledAt.Add(time.Duration(slaDuration) * time.Second)
		targetedSLAByJobName[job.Name] = &sla
	}
	s.l.Info("total targeted SLAs found", "count", len(targetedSLAByJobName))
	// jobs not having targeted SLA will be skipped
	jobsSkipped := []string{}
	for _, job := range jobs {
		if _, ok := targetedSLAByJobName[job.Name]; !ok {
			jobsSkipped = append(jobsSkipped, job.Name.String())
		}
	}
	if len(jobsSkipped) > 0 {
		s.l.Info("jobs skipped due to no targeted SLA found", "jobs", jobsSkipped)
	}

	return targetedSLAByJobName
}

func (s *JobSLAPredictorService) getJobSchedules(jobs []*scheduler.JobWithDetails, scheduleRangeInHours time.Duration, referenceTime time.Time) map[scheduler.JobName]*scheduler.JobSchedule {
	jobSchedules := make(map[scheduler.JobName]*scheduler.JobSchedule)
	s.l.Info("jobs to get schedules for", "count", len(jobs))
	for _, job := range jobs {
		if job.Schedule == nil {
			continue
		}
		nextScheduledAt, err := job.Schedule.GetNextSchedule(referenceTime)
		if err != nil {
			s.l.Warn("failed to get scheduled at for job, skipping SLA prediction", "job", job.Name, "error", err)
			continue
		}

		prevScheduledAt, err := job.Schedule.GetPreviousSchedule(referenceTime)
		if err != nil {
			s.l.Warn("failed to get previous scheduled at for job, skipping SLA prediction", "job", job.Name, "error", err)
			continue
		}

		var scheduledAt time.Time
		if nextScheduledAt.Sub(referenceTime).Milliseconds() < scheduleRangeInHours.Milliseconds() {
			s.l.Debug("using next scheduled at for job within schedule range", "job", job.Name)
			scheduledAt = nextScheduledAt
		} else if referenceTime.Sub(prevScheduledAt).Milliseconds() < scheduleRangeInHours.Milliseconds() {
			s.l.Debug("using previous scheduled at for job within schedule range", "job", job.Name)
			scheduledAt = prevScheduledAt
		}

		if scheduledAt.IsZero() {
			s.l.Warn("no scheduled at found for job in the next schedule range, skipping SLA prediction", "job", job.Name)
			continue
		}

		jobSchedules[job.Name] = &scheduler.JobSchedule{
			JobName:     job.Name,
			ScheduledAt: scheduledAt,
		}
	}
	s.l.Info("total job schedules found", "count", len(jobSchedules))
	// jobs not having schedule within the range will be skipped
	jobsSkipped := []string{}
	for _, job := range jobs {
		if _, ok := jobSchedules[job.Name]; !ok {
			jobsSkipped = append(jobsSkipped, job.Name.String())
		}
	}
	if len(jobsSkipped) > 0 {
		s.l.Info("jobs skipped due to no schedule within the range", "jobs", jobsSkipped)
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
func (s *JobSLAPredictorService) calculateInferredSLAs(jobTarget *scheduler.JobLineageSummary, jobDurations map[scheduler.JobName]*time.Duration, targetedSLA *time.Time, damperCoeff float64) map[scheduler.JobName]*time.Time {
	jobSLAs := make(map[scheduler.JobName]*time.Time)
	alpha := damperCoeff // damper factor to reduce the impact of upstream jobs in higher levels
	lowestDamperCoeff := damperCoeff
	// inferred SLA for leaf node = targetedSLA S(j)
	jobSLAs[jobTarget.JobName] = targetedSLA
	// bottom up calculation of inferred SLA for upstream jobs
	s.l.Info("damper coefficient used for inferred SLA calculation", "damper_coeff", alpha)
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
		if current.damper < lowestDamperCoeff {
			lowestDamperCoeff = current.damper
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

	s.l.Info("lowest damper coefficient used in inferred SLA calculation", "damper_coeff", lowestDamperCoeff)

	return jobSLAs
}

// identifySLABreachRootCauses identifies if the given job might breach its SLA based on its upstream jobs and their inferred SLAs.
// if any upstream job u of a critical downstream job j meets either of the following conditions, it means job j might breach its SLA:
// - Given current time in UTC T(now), T(now)>= S(u|j) (the inferred SLA for u induced by j has passed) and the upstream job u has not completed yet. Or,
// - Given current time in UTC T(now), T(now)>= S(u|j) - D(u) (the inferred SLA for u induced by j minus the average duration of u has passed) and the upstream job u has not started yet.
// return the job that might breach its SLA
func (s *JobSLAPredictorService) identifySLABreachRootCauses(jobTarget *scheduler.JobLineageSummary, jobSLAStates map[scheduler.JobName]*JobSLAState, referenceTime time.Time) ([][]*JobState, [][]*JobState) {
	jobStateByName := make(map[scheduler.JobName]*JobState)
	potentialBreachPaths := make([][]scheduler.JobName, 0)
	// DFS to traverse all upstream jobs with paths
	type state struct {
		job   *scheduler.JobLineageSummary
		paths []scheduler.JobName
		level int
	}
	stack := []*state{}
	// start from targeted job
	stack = append(stack, &state{job: jobTarget, paths: []scheduler.JobName{}, level: 0})

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
		estimatedDuration := *jobSLAStates[job.JobName].EstimatedDuration

		// check if job meets either of the conditions
		isPotentialBreach := false
		for _, jobRun := range job.JobRuns {
			// condition 1: T(now)>= S(u|j) and the job u has not completed yet
			if (referenceTime.After(inferredSLA) && jobRun != nil && jobRun.JobEndTime == nil) || (jobRun != nil && jobRun.JobEndTime != nil && jobRun.JobEndTime.After(inferredSLA)) {
				// found a job that might breach its SLA
				potentialBreachPaths = append(potentialBreachPaths, paths)
				// add to jobStateByName
				jobStateByName[job.JobName] = &JobState{
					JobSLAState:   *jobSLAStates[job.JobName],
					JobName:       job.JobName,
					JobRun:        *jobRun,
					Tenant:        job.Tenant,
					RelativeLevel: jobWithState.level,
					Status:        SLABreachCauseRunningLate,
				}

				isPotentialBreach = true
			}

			// condition 2: T(now)>= S(u|j) - D(u) and the job u has not started yet
			if referenceTime.After(inferredSLA.Add(-estimatedDuration)) && (jobRun != nil && jobRun.TaskStartTime == nil) {
				// found a job that might breach its SLA
				potentialBreachPaths = append(potentialBreachPaths, paths)
				// add to jobStateByName
				jobStateByName[job.JobName] = &JobState{
					JobSLAState:   *jobSLAStates[job.JobName],
					JobName:       job.JobName,
					JobRun:        *jobRun,
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

	// full root causes
	fullRootCauses := make([][]*JobState, len(potentialBreachPaths))
	for i, path := range potentialBreachPaths {
		jobStatesPath := make([]*JobState, 0)
		for _, jobName := range path {
			if jobState, ok := jobStateByName[jobName]; ok {
				jobStatesPath = append(jobStatesPath, jobState)
			}
		}
		fullRootCauses[i] = jobStatesPath
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

	return rootCauses, fullRootCauses
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
func (s *JobSLAPredictorService) storePredictedSLABreach(ctx context.Context, jobTarget *scheduler.JobLineageSummary, paths map[scheduler.JobName][]*JobState, referenceTime time.Time) error {
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
			ProjectName:         t.ProjectName().String(),
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

// deduplicateJobBreaches deduplicates the job breaches based on the targeted job names.
// if job target already exists in DB and it's as part of job breaches don't include it again.
func (s *JobSLAPredictorService) deduplicateJobBreaches(ctx context.Context, scheduleRangeInHours time.Duration, referenceTime time.Time, jobBreaches map[scheduler.JobName]map[scheduler.JobName]*JobState) (map[scheduler.JobName]map[scheduler.JobName]*JobState, error) {
	if !s.config.EnablePersistentLogging {
		s.l.Warn("persistent logging is disabled, cannot perform deduplication")
		return jobBreaches, nil
	}

	// define time range to check existing job names
	from := referenceTime.Add(-scheduleRangeInHours)
	to := referenceTime.Add(scheduleRangeInHours)

	// get existing job names from repository
	existingJobNames, err := s.repo.GetPredictedSLAJobNamesWithinTimeRange(ctx, from, to)
	if err != nil {
		s.l.Error("failed to get existing predicted SLA job names from repository, skipping deduplication", "error", err)
		return nil, err
	}

	existingJobNamesMap := make(map[scheduler.JobName]bool)
	for _, jobName := range existingJobNames {
		existingJobNamesMap[jobName] = true
	}

	deduplicatedJobBreaches := make(map[scheduler.JobName]map[scheduler.JobName]*JobState)
	for jobName, upstreamCauses := range jobBreaches {
		if _, exists := existingJobNamesMap[jobName]; exists {
			s.l.Info("skipping job breach as it already exists in repository", "job", jobName, "from", from, "to", to)
			continue
		}
		deduplicatedJobBreaches[jobName] = upstreamCauses
	}

	s.l.Info("deduplicated job breaches", "original_count", len(jobBreaches), "deduplicated_count", len(deduplicatedJobBreaches))

	return deduplicatedJobBreaches, nil
}
