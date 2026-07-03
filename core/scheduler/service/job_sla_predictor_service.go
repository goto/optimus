package service

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/goto/salt/log"

	"github.com/goto/optimus/config"
	"github.com/goto/optimus/core/scheduler"
	"github.com/goto/optimus/core/tenant"
	"github.com/goto/optimus/internal/errors"
)

type JobSLAPredictorRequestConfig struct {
	ReferenceTime        time.Time
	ScheduleRangeInHours time.Duration
	SkipJobNames         []string
	EnableAlert          bool
	EnableDeduplication  bool
	DamperCoeff          float64
	Severity             string
}

// comboBreachResult holds the computed breaches for one combo, retained so that
// deduplication, alerting and storage can each run once across all combos.
type comboBreachResult struct {
	combo               scheduler.SLABreachCombo
	jobBreachCauses     map[scheduler.JobName]map[scheduler.JobName]*scheduler.JobState
	jobFullBreachCauses map[scheduler.JobName]map[scheduler.JobName][]*scheduler.JobState
	targetedSLA         map[scheduler.JobName]*time.Time
	jobsWithLineageMap  map[scheduler.JobName]*scheduler.JobLineageSummary
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
	StorePredictedSLABreach(ctx context.Context, jobTargetName, jobCauseName scheduler.JobName, targetedSLA, jobScheduledAt time.Time, cause string, referenceTime time.Time, config map[string]interface{}, lineages []interface{}) error
	GetPredictedSLAJobNamesWithinTimeRange(ctx context.Context, from, to time.Time) ([]scheduler.JobName, error)
}

type ScheduledChangeGetter interface {
	GetRecentScheduleChange(ctx context.Context, jobName scheduler.JobName, tnnt tenant.Tenant, startTime time.Time) (string, error)
}

type JobSLAPredictorService struct {
	l                     log.Logger
	config                config.PotentialSLABreachConfig
	repo                  SLAPredictorRepository
	jobDetailsGetter      JobDetailsGetter
	jobLineageFetcher     JobLineageFetcher
	durationEstimator     DurationEstimator
	tenantGetter          TenantGetter
	scheduledChangeGetter ScheduledChangeGetter
	// alerting purpose
	potentialSLANotifier PotentialSLANotifier
}

func NewJobSLAPredictorService(l log.Logger, config config.PotentialSLABreachConfig, slaPredictorRepo SLAPredictorRepository, jobLineageFetcher JobLineageFetcher, durationEstimator DurationEstimator, jobDetailsGetter JobDetailsGetter, potentialSLANotifier PotentialSLANotifier, tenantGetter TenantGetter, scheduledChangeGetter ScheduledChangeGetter) *JobSLAPredictorService {
	return &JobSLAPredictorService{
		l:                     l,
		config:                config,
		repo:                  slaPredictorRepo,
		jobLineageFetcher:     jobLineageFetcher,
		durationEstimator:     durationEstimator,
		jobDetailsGetter:      jobDetailsGetter,
		tenantGetter:          tenantGetter,
		scheduledChangeGetter: scheduledChangeGetter,
		potentialSLANotifier:  potentialSLANotifier,
	}
}

// IdentifySLABreaches evaluates a single (project, labels) combo. Kept for
// backward compatibility; it now shares the same consolidation pipeline as the
// batch entrypoint, so alerts are still grouped into one message per team.
func (s *JobSLAPredictorService) IdentifySLABreaches(ctx context.Context, projectName tenant.ProjectName, jobNames []scheduler.JobName, labels map[string]string, reqConfig JobSLAPredictorRequestConfig) (map[scheduler.JobName]map[scheduler.JobName]*scheduler.JobState, error) {
	combo := scheduler.SLABreachCombo{
		ProjectName: projectName,
		JobNames:    jobNames,
		Labels:      labels,
	}
	result, err := s.computeBreaches(ctx, combo, reqConfig)
	if err != nil {
		return nil, err
	}

	s.processBreachResults(ctx, []*comboBreachResult{result}, reqConfig)

	return result.jobBreachCauses, nil
}

// IdentifySLABreachesBatch evaluates many (project, label-group) combos and
// consolidates the results into exactly one alert per team. A failure in one
// combo is logged and skipped so the rest of the batch still produces alerts.
func (s *JobSLAPredictorService) IdentifySLABreachesBatch(ctx context.Context, combos []scheduler.SLABreachCombo, reqConfig JobSLAPredictorRequestConfig) (map[string]*scheduler.TargetBreach, error) {
	results := make([]*comboBreachResult, 0, len(combos))
	me := errors.NewMultiError("IdentifySLABreachesBatch")
	for _, combo := range combos {
		result, err := s.computeBreaches(ctx, combo, reqConfig)
		if err != nil {
			s.l.Error("failed to compute SLA breaches for combo, skipping", "project", combo.ProjectName.String(), "group", combo.GroupName, "error", err)
			me.Append(err)
			continue
		}
		results = append(results, result)
	}

	s.processBreachResults(ctx, results, reqConfig)

	// build project-qualified response keyed by "<project>/<job>"
	response := map[string]*scheduler.TargetBreach{}
	for _, r := range results {
		for targetName, upstreams := range r.jobBreachCauses {
			key := r.combo.ProjectName.String() + "/" + targetName.String()
			response[key] = &scheduler.TargetBreach{
				TargetProject: r.combo.ProjectName.String(),
				TargetJobName: targetName,
				Upstreams:     upstreams,
			}
		}
	}

	// only surface an error when nothing succeeded, so partial failures still alert
	if len(response) == 0 {
		return response, me.ToErr()
	}
	return response, nil
}

// computeBreaches runs the pure breach-detection pipeline for a single combo. It
// does not alert or persist; callers aggregate results and do that once.
func (s *JobSLAPredictorService) computeBreaches(ctx context.Context, combo scheduler.SLABreachCombo, reqConfig JobSLAPredictorRequestConfig) (*comboBreachResult, error) {
	// map of jobName -> map of upstreamJobName -> scheduler.JobState
	jobBreachCauses := make(map[scheduler.JobName]map[scheduler.JobName]*scheduler.JobState)
	jobFullBreachCauses := make(map[scheduler.JobName]map[scheduler.JobName][]*scheduler.JobState)
	emptyResult := &comboBreachResult{combo: combo, jobBreachCauses: jobBreachCauses, jobFullBreachCauses: jobFullBreachCauses}

	// damper coefficient to use default if not provided
	damperCoeff := reqConfig.DamperCoeff
	if damperCoeff <= 0 {
		damperCoeff = s.config.DamperCoeff
	}

	// job names to be skipped for checking
	skipJobNames := map[scheduler.JobName]bool{}
	for _, skipJobName := range reqConfig.SkipJobNames {
		skipJobNames[scheduler.JobName(skipJobName)] = true
	}

	if len(combo.JobNames) == 0 && len(combo.Labels) == 0 {
		s.l.Warn("no job names or labels provided, skipping SLA prediction")
		return emptyResult, nil
	}

	// get jobs with details
	jobsWithDetails, err := getJobWithDetails(ctx, s.l, s.jobDetailsGetter, combo.ProjectName, combo.JobNames, combo.Labels)
	if err != nil {
		s.l.Error("failed to get jobs with details, skipping SLA prediction", "error", err)
		return nil, err
	}
	if len(jobsWithDetails) == 0 {
		return emptyResult, nil
	}

	// get scheduled at
	jobSchedules := getJobSchedules(s.l, jobsWithDetails, reqConfig.ScheduleRangeInHours, reqConfig.ReferenceTime)
	if len(jobSchedules) == 0 {
		s.l.Warn("no job schedules found for the given jobs in the next schedule range, skipping SLA prediction")
		return emptyResult, nil
	}

	// get targetedSLA
	targetedSLA := s.getTargetedSLA(jobsWithDetails, jobSchedules)
	if len(targetedSLA) == 0 {
		s.l.Warn("no targeted SLA found for the given jobs, skipping SLA prediction")
		return emptyResult, nil
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
		targetSLA, ok := targetedSLA[jobSchedule.JobName]
		if !ok || targetSLA == nil {
			continue
		}
		breachesCauses, fullBreachesCauses := s.IdentifySLABreach(ctx, jobWithLineage, jobDurations, targetSLA, skipJobNames, damperCoeff, reqConfig.ReferenceTime)
		// populate jobBreachCauses
		if len(breachesCauses) > 0 {
			jobBreachCauses[jobSchedule.JobName] = breachesCauses
		}
		// populate jobFullBreachCauses for logging and storage purpose
		if len(fullBreachesCauses) > 0 {
			jobFullBreachCauses[jobSchedule.JobName] = fullBreachesCauses
		}
	}

	return &comboBreachResult{
		combo:               combo,
		jobBreachCauses:     jobBreachCauses,
		jobFullBreachCauses: jobFullBreachCauses,
		targetedSLA:         targetedSLA,
		jobsWithLineageMap:  jobsWithLineageMap,
	}, nil
}

// processBreachResults consolidates all combo results into one alert per team
// and persists the predicted breaches. Deduplication is read before storing so
// the current run's own writes never suppress its alerts.
func (s *JobSLAPredictorService) processBreachResults(ctx context.Context, results []*comboBreachResult, reqConfig JobSLAPredictorRequestConfig) {
	if reqConfig.EnableAlert {
		s.sendConsolidatedAlerts(ctx, results, reqConfig)
	}

	if s.config.EnablePersistentLogging {
		s.storeBreachResults(ctx, results, reqConfig)
	}
}

func (s *JobSLAPredictorService) IdentifySLABreach(ctx context.Context, jobTarget *scheduler.JobLineageSummary, jobDurations map[scheduler.JobName]*time.Duration, targetedSLA *time.Time, skipJobNames map[scheduler.JobName]bool, damperCoeff float64, referenceTime time.Time) (map[scheduler.JobName]*scheduler.JobState, map[scheduler.JobName][]*scheduler.JobState) {
	// calculate inferred SLAs for each job based on their downstream critical jobs and estimated durations
	// S(u|j) = S(j) - D(u)
	jobSLAStatesByJobTarget := s.CalculateInferredSLAs(jobTarget, jobDurations, targetedSLA, damperCoeff)

	// populate jobSLAStatesByJobTargetName
	jobSLAStates := s.populateJobSLAStates(jobDurations, jobSLAStatesByJobTarget)

	// identify jobs that might breach their SLAs based on current time and inferred SLAs
	// T(now)>= S(u|j) and the job u has not completed yet
	// T(now)>= S(u|j) - D(u) and the job u has not started yet
	rootCauses, allUpstreamStates := s.identifySLABreachRootCauses(ctx, jobTarget, jobSLAStates, skipJobNames, referenceTime)

	// populate breachesCauses
	breachesCauses := make(map[scheduler.JobName]*scheduler.JobState)
	for _, causes := range rootCauses {
		if len(causes) == 0 {
			continue
		}
		cause := causes[len(causes)-1] // root cause is the last element in the path
		breachesCauses[cause.JobName] = cause
	}

	// populate fullBreachesCauses
	fullBreachesCauses := make(map[scheduler.JobName][]*scheduler.JobState)
	for _, causes := range allUpstreamStates {
		if len(causes) == 0 {
			continue
		}
		cause := causes[len(causes)-1] // root cause is the last element in the path (as a unique identifier)
		fullBreachesCauses[cause.JobName] = causes
	}

	return breachesCauses, fullBreachesCauses
}

func getJobWithDetails(ctx context.Context, l log.Logger, jobDetailsGetter JobDetailsGetter, projectName tenant.ProjectName, jobNames []scheduler.JobName, labels map[string]string) ([]*scheduler.JobWithDetails, error) {
	filteredJobsByName := map[scheduler.JobName]*scheduler.JobWithDetails{}
	filteredJobByLabel := map[scheduler.JobName]*scheduler.JobWithDetails{}
	filteredJobMerged := map[scheduler.JobName]*scheduler.JobWithDetails{}

	if len(jobNames) > 0 {
		jobNameStr := []string{}
		for _, jn := range jobNames {
			jobNameStr = append(jobNameStr, string(jn))
		}
		jobsWithDetails, err := jobDetailsGetter.GetJobs(ctx, projectName, jobNameStr)
		if err != nil {
			if jobsWithDetails == nil {
				return nil, err
			}
			l.Error("[getJobWithDetails] encountered non-blocking error when fetching jobs by names: %s", err.Error())
		}
		for _, job := range jobsWithDetails {
			filteredJobsByName[job.Name] = job
			filteredJobMerged[job.Name] = job
		}
		l.Info("[getJobWithDetails] fetched jobs by names", "count", len(filteredJobsByName))
		l.Info("[getJobWithDetails] jobs fetched by names", "jobs", filteredJobsByName)
	}

	if len(labels) > 0 {
		jobsWithDetails, err := jobDetailsGetter.GetJobsByLabels(ctx, projectName, labels)
		if err != nil {
			if jobsWithDetails == nil {
				return nil, err
			}
			l.Error("[getJobWithDetails] encountered non-blocking error when fetching jobs by labels: %s", err.Error())
		}
		for _, job := range jobsWithDetails {
			filteredJobByLabel[job.Name] = job
			filteredJobMerged[job.Name] = job
		}
		l.Info("[getJobWithDetails] fetched jobs by labels", "count", len(filteredJobByLabel))
		l.Info("[getJobWithDetails] jobs fetched by labels", "jobs", filteredJobByLabel)
	}

	filteredJobSchedules := []*scheduler.JobWithDetails{}
	for _, job := range filteredJobMerged {
		filteredJobSchedules = append(filteredJobSchedules, job)
	}
	l.Info("[getJobWithDetails] total jobs fetched after merging by names and labels", "count", len(filteredJobSchedules))

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

func getJobSchedules(l log.Logger, jobs []*scheduler.JobWithDetails, scheduleRangeInHours time.Duration, referenceTime time.Time) map[scheduler.JobName]*scheduler.JobSchedule {
	jobSchedules := make(map[scheduler.JobName]*scheduler.JobSchedule)
	l.Info("jobs to get schedules for", "count", len(jobs))
	for _, job := range jobs {
		if job.Schedule == nil {
			continue
		}
		nextScheduledAt, err := job.Schedule.GetNextSchedule(referenceTime)
		if err != nil {
			l.Warn("failed to get scheduled at for job, skipping SLA prediction", "job", job.Name, "error", err)
			continue
		}

		prevScheduledAt, err := job.Schedule.GetPreviousSchedule(referenceTime)
		if err != nil {
			l.Warn("failed to get previous scheduled at for job, skipping SLA prediction", "job", job.Name, "error", err)
			continue
		}

		var scheduledAt time.Time
		if nextScheduledAt.Sub(referenceTime).Milliseconds() < scheduleRangeInHours.Milliseconds() {
			l.Debug("using next scheduled at for job within schedule range", "job", job.Name)
			scheduledAt = nextScheduledAt
		} else if referenceTime.Sub(prevScheduledAt).Milliseconds() < scheduleRangeInHours.Milliseconds() {
			l.Debug("using previous scheduled at for job within schedule range", "job", job.Name)
			scheduledAt = prevScheduledAt
		}

		if scheduledAt.IsZero() {
			l.Warn("no scheduled at found for job in the next schedule range, skipping SLA prediction", "job", job.Name)
			continue
		}

		jobSchedules[job.Name] = &scheduler.JobSchedule{
			JobName:     job.Name,
			ScheduledAt: scheduledAt,
		}
	}
	l.Info("total job schedules found", "count", len(jobSchedules))
	// jobs not having schedule within the range will be skipped
	jobsSkipped := []string{}
	for _, job := range jobs {
		if _, ok := jobSchedules[job.Name]; !ok {
			jobsSkipped = append(jobsSkipped, job.Name.String())
		}
	}
	if len(jobsSkipped) > 0 {
		l.Info("jobs skipped due to no schedule within the range", "jobs", jobsSkipped)
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
func (s *JobSLAPredictorService) CalculateInferredSLAs(jobTarget *scheduler.JobLineageSummary, jobDurations map[scheduler.JobName]*time.Duration, targetedSLA *time.Time, damperCoeff float64) map[scheduler.JobName]*time.Time {
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
	stack := []*state{{job: jobTarget, damper: 1.0}}
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
			existingSLA, ok := jobSLAs[upstreamJob.JobName]
			// only reassign SLA if it does not exist yet,
			// or if the newly calculated inferred SLA is earlier than the currently tracked ones
			if ok && existingSLA.Before(inferredSLA) {
				continue
			}

			jobSLAs[upstreamJob.JobName] = &inferredSLA
			// export dampening factor calculation as a new function
			// even do not make it exponential
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
func (s *JobSLAPredictorService) identifySLABreachRootCauses(ctx context.Context, jobTarget *scheduler.JobLineageSummary, jobSLAStates map[scheduler.JobName]*scheduler.JobSLAState, skipJobNames map[scheduler.JobName]bool, referenceTime time.Time) ([][]*scheduler.JobState, [][]*scheduler.JobState) {
	jobBreachStates := make(map[scheduler.JobName]*scheduler.JobState)
	allUpstreamStates := make([][]*scheduler.JobState, 0)

	// DFS to traverse all upstream jobs with paths
	type state struct {
		job    *scheduler.JobLineageSummary
		paths  []scheduler.JobName
		states []*scheduler.JobState
		level  int
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
		states := make([]*scheduler.JobState, len(jobWithState.states))
		copy(paths, jobWithState.paths)
		copy(states, jobWithState.states)
		paths = append(paths, job.JobName) //nolint:makezero

		if visited[job.JobName] {
			continue
		}
		jobsWithLineageVisitedMap[job.JobName] = job
		visited[job.JobName] = true

		if !job.IsEnabled {
			s.l.Info("skipping job for SLA breach check as it's disabled", "job", job.JobName)
			continue
		}

		if jobSLAStates[job.JobName] == nil || jobSLAStates[job.JobName].InferredSLA == nil || jobSLAStates[job.JobName].EstimatedDuration == nil { // less likely occur, but just in case
			continue
		}

		inferredSLA := *jobSLAStates[job.JobName].InferredSLA
		estimatedDuration := *jobSLAStates[job.JobName].EstimatedDuration

		// check if job meets either of the conditions
		isPotentialBreach := false

		if _, ok := job.JobRuns[jobTarget.JobName]; !ok {
			s.l.Info("skipping job for SLA breach check as it has no job run for the targeted job", "job", job.JobName, "targeted_job", jobTarget.JobName)
			continue
		}

		jobRun := job.JobRuns[jobTarget.JobName]

		// skip detection for jobs in skip list
		if skipJobNames[job.JobName] {
			s.l.Info("skipping job for SLA breach check as it's in the skip list", "job", job.JobName)
			if job.JobName == jobTarget.JobName {
				// if the targeted job is in the skip list, we stop traversing upstream jobs
				continue
			}
		} else {
			if oldScheduled, err := s.scheduledChangeGetter.GetRecentScheduleChange(ctx, job.JobName, job.Tenant, jobRun.ScheduledAt); err != nil {
				s.l.Error("failed to get recent schedule change for job, check the breach anyway", "job", job.JobName, "error", err)
			} else if oldScheduled != "" {
				s.l.Info("skipping job for SLA breach check as it has recent schedule change", "job", job.JobName, "old_scheduled_at", oldScheduled, "new_scheduled_at", jobRun.ScheduledAt)
				continue
			}
			var state *scheduler.JobState
			// condition 1: T(now)>= S(u|j) and the job u has not completed yet
			if (referenceTime.After(inferredSLA) && jobRun != nil && jobRun.JobEndTime == nil) || (jobRun != nil && jobRun.JobEndTime != nil && jobRun.JobEndTime.After(inferredSLA)) {
				// add to jobStatePaths
				state = &scheduler.JobState{
					JobSLAState:   *jobSLAStates[job.JobName],
					JobName:       job.JobName,
					JobRun:        *jobRun,
					Tenant:        job.Tenant,
					RelativeLevel: jobWithState.level,
					Status:        scheduler.SLABreachCauseRunningLate,
				}
			}

			// condition 2: T(now)>= S(u|j) - D(u) and the job u has not started yet
			if referenceTime.After(inferredSLA.Add(-estimatedDuration)) && (jobRun != nil && jobRun.TaskStartTime == nil) {
				// add to jobStatePaths
				state = &scheduler.JobState{
					JobSLAState:   *jobSLAStates[job.JobName],
					JobName:       job.JobName,
					JobRun:        *jobRun,
					Tenant:        job.Tenant,
					RelativeLevel: jobWithState.level,
					Status:        scheduler.SLABreachCauseNotStarted,
				}
			}

			if state != nil {
				states = append(states, state) //nolint:makezero
				allUpstreamStates = append(allUpstreamStates, states)
				// add to jobStateByName
				jobBreachStates[job.JobName] = state

				isPotentialBreach = true
			}
		}

		if isPotentialBreach {
			s.l.Info("potential SLA breach found", "job", job.JobName, "inferred_sla", inferredSLA, "duration", jobSLAStates[job.JobName].EstimatedDuration, "path", paths)
		} else {
			// no potential breach, continue to traverse upstream jobs and add current job state
			states = append(states, &scheduler.JobState{ //nolint:makezero
				JobSLAState:   *jobSLAStates[job.JobName],
				JobName:       job.JobName,
				Tenant:        job.Tenant,
				RelativeLevel: jobWithState.level,
			})
		}

		for _, upstreamJob := range job.Upstreams {
			stack = append(stack, &state{job: upstreamJob, paths: paths, states: states, level: jobWithState.level + 1})
		}
	}

	// find *true* root causes among the breaching jobs, where none of its direct upstreams are
	// breaching too; otherwise its breach is only propagated from an upstream. This
	// relies on the global breach set (jobBreachStates) and the lineage graph rather than on
	// the traversal path leaves, so shared-ancestor (diamond) lineages collapse to the deepest
	// breaching job regardless of which branch the traversal happened to explore first.
	rootCauseNames := make(map[scheduler.JobName]bool)
	rootCauses := make([][]*scheduler.JobState, 0)
	for jobName, breachState := range jobBreachStates {
		hasBreachingUpstream := false
		if lineage, ok := jobsWithLineageVisitedMap[jobName]; ok && lineage != nil {
			for _, upstream := range lineage.Upstreams {
				if _, breaching := jobBreachStates[upstream.JobName]; breaching {
					hasBreachingUpstream = true
					break
				}
			}
		}
		if !hasBreachingUpstream {
			rootCauseNames[jobName] = true
			rootCauses = append(rootCauses, []*scheduler.JobState{breachState})
		}
	}

	// keep only the full lineage paths that terminate at a true root cause, so the stored/
	// logged paths stay consistent with the reported root causes.
	compactedAllUpstreamStates := compactingStatePaths(allUpstreamStates)
	fullPaths := make([][]*scheduler.JobState, 0, len(compactedAllUpstreamStates))
	for _, upstreamStates := range compactedAllUpstreamStates {
		if len(upstreamStates) == 0 {
			continue
		}
		leaf := upstreamStates[len(upstreamStates)-1]
		if rootCauseNames[leaf.JobName] {
			fullPaths = append(fullPaths, upstreamStates)
		}
	}

	return rootCauses, fullPaths
}

// populateJobSLAStates populates the jobSLAStatesByJobName map with the estimated durations and inferred SLAs for each job.
func (*JobSLAPredictorService) populateJobSLAStates(jobDurations map[scheduler.JobName]*time.Duration, jobSLAsByJobName map[scheduler.JobName]*time.Time) map[scheduler.JobName]*scheduler.JobSLAState {
	jobSLAStatesByJobName := make(map[scheduler.JobName]*scheduler.JobSLAState)
	for jobName, inferredSLA := range jobSLAsByJobName {
		jobSLAStatesByJobName[jobName] = &scheduler.JobSLAState{
			EstimatedDuration: jobDurations[jobName],
			InferredSLA:       inferredSLA,
		}
	}
	return jobSLAStatesByJobName
}

// storePredictedSLABreach stores the predicted SLA breaches in the repository for further analysis.
func (s *JobSLAPredictorService) storePredictedSLABreach(ctx context.Context, jobTarget *scheduler.JobLineageSummary, slaTarget time.Time, paths map[scheduler.JobName][]*scheduler.JobState, reqConfig JobSLAPredictorRequestConfig) error {
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
		config["server_config"] = s.config
		config["request_config"] = reqConfig
		rawConfig, err := json.Marshal(config)
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
		err = s.repo.StorePredictedSLABreach(ctx, jobTarget.JobName, cause.JobName, slaTarget, scheduledAt, string(cause.Status), reqConfig.ReferenceTime, config, lineages)
		if err != nil {
			return err
		}
	}
	return nil
}

// sendConsolidatedAlerts aggregates breaches from all combos and emits exactly
// one alert per cause-owner team. The alert body is organized as
// project -> SLA group (with severity) -> target -> causes owned by that team.
func (s *JobSLAPredictorService) sendConsolidatedAlerts(ctx context.Context, results []*comboBreachResult, reqConfig JobSLAPredictorRequestConfig) {
	totalBreaches := 0
	for _, r := range results {
		totalBreaches += len(r.jobBreachCauses)
	}
	if totalBreaches == 0 {
		return
	}
	s.l.Info("potential SLA breaches found", "count", totalBreaches)

	// deduplicate target job names against previously predicted breaches
	suppressed := map[scheduler.JobName]bool{}
	if reqConfig.EnableDeduplication {
		existing, err := s.deduplicateTargetNames(ctx, reqConfig.ScheduleRangeInHours, reqConfig.ReferenceTime)
		if err != nil {
			s.l.Error("failed to compute deduplication set, sending alerts without deduplication", "error", err)
		} else {
			suppressed = existing
		}
	}

	agg := scheduler.NewTeamBreachAggregator()
	teamCache := map[tenant.Tenant]string{}
	for _, r := range results {
		groupName := r.combo.GroupName
		if groupName == "" {
			groupName = deriveGroupName(r.combo.Labels)
		}
		severity := reqConfig.Severity
		for targetName, upstreamCauses := range r.jobBreachCauses {
			if suppressed[targetName] {
				s.l.Info("skipping target for alerting as it was recently predicted", "job", targetName.String())
				continue
			}
			for _, upstreamCause := range upstreamCauses {
				team := s.resolveTeam(ctx, upstreamCause.Tenant, teamCache)
				if team == "" {
					continue
				}
				agg.Add(team, r.combo.ProjectName.String(), groupName, severity, targetName.String(), scheduler.UpstreamAttrs{
					JobName:       upstreamCause.JobName.String(),
					RelativeLevel: upstreamCause.RelativeLevel,
					Status:        string(upstreamCause.Status),
				})
			}
		}
	}

	for _, attr := range agg.Build() {
		s.potentialSLANotifier.SendPotentialSLABreach(attr)
	}
}

// resolveTeam looks up the alertmanager team for a tenant, caching the result
// (including empty results) to avoid repeated lookups across combos.
func (s *JobSLAPredictorService) resolveTeam(ctx context.Context, t tenant.Tenant, cache map[tenant.Tenant]string) string {
	if team, ok := cache[t]; ok {
		return team
	}
	team := ""
	tenantWithDetails, err := s.tenantGetter.GetDetails(ctx, t)
	if err != nil {
		s.l.Error("failed to get tenant details for tenant %s: %v", t.String(), err)
	} else if teamName, err := tenantWithDetails.GetConfig(tenant.ProjectAlertManagerTeam); err != nil {
		s.l.Error("failed to get default team for tenant %s: %v", t.String(), err)
	} else if teamName == "" {
		s.l.Warn("no default team configured for tenant %s, skip sending alert", t.String())
	} else {
		team = teamName
	}
	cache[t] = team
	return team
}

// storeBreachResults persists the predicted breaches for every combo.
func (s *JobSLAPredictorService) storeBreachResults(ctx context.Context, results []*comboBreachResult, reqConfig JobSLAPredictorRequestConfig) {
	for _, r := range results {
		for jobName, fullBreachesCausesPaths := range r.jobFullBreachCauses {
			jobTarget := r.jobsWithLineageMap[jobName]
			if jobTarget == nil {
				continue
			}
			slaTarget := time.Time{}
			if sla, ok := r.targetedSLA[jobName]; ok && sla != nil {
				slaTarget = *sla
			}
			if err := s.storePredictedSLABreach(ctx, jobTarget, slaTarget.UTC(), fullBreachesCausesPaths, reqConfig); err != nil {
				s.l.Error("failed to store predicted SLA breaches", "error", err)
			}
		}
	}
}

// deriveGroupName builds a stable display name from label key:value pairs when
// no explicit group name was provided.
func deriveGroupName(labels map[string]string) string {
	if len(labels) == 0 {
		return "default"
	}
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, fmt.Sprintf("%s=%s", k, labels[k]))
	}
	return strings.Join(parts, ", ")
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
func compactingStatePaths(allUpstreamStates [][]*scheduler.JobState) [][]*scheduler.JobState {
	prefixes := make(map[scheduler.JobName]bool)

	for _, path := range allUpstreamStates {
		for i, node := range path {
			if i < len(path)-1 { // prefix
				prefixes[node.JobName] = true
			}
		}
	}

	// result is equal to paths that contains ending nodes that are not prefixes
	var result [][]*scheduler.JobState
	for _, path := range allUpstreamStates {
		ending := path[len(path)-1]
		if _, isPrefix := prefixes[ending.JobName]; !isPrefix {
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

// deduplicateTargetNames returns the set of target job names that were already
// predicted to breach within the time window, so they can be skipped when
// alerting. Deduplication is name-based; it relies on Optimus job names being
// globally unique. Requires persistent logging to be enabled.
func (s *JobSLAPredictorService) deduplicateTargetNames(ctx context.Context, scheduleRangeInHours time.Duration, referenceTime time.Time) (map[scheduler.JobName]bool, error) {
	if !s.config.EnablePersistentLogging {
		s.l.Warn("persistent logging is disabled, cannot perform deduplication")
		return map[scheduler.JobName]bool{}, nil
	}

	// define time range to check existing job names
	from := referenceTime.Add(-scheduleRangeInHours)
	to := referenceTime.Add(scheduleRangeInHours)

	existingJobNames, err := s.repo.GetPredictedSLAJobNamesWithinTimeRange(ctx, from, to)
	if err != nil {
		s.l.Error("failed to get existing predicted SLA job names from repository, skipping deduplication", "error", err)
		return nil, err
	}

	suppressed := make(map[scheduler.JobName]bool, len(existingJobNames))
	for _, jobName := range existingJobNames {
		suppressed[jobName] = true
	}
	s.l.Info("computed deduplication set", "count", len(suppressed), "from", from, "to", to)

	return suppressed, nil
}
