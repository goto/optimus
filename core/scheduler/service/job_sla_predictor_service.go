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
	"github.com/goto/optimus/core/scheduler/rootcause"
	"github.com/goto/optimus/core/tenant"
	"github.com/goto/optimus/internal/errors"
)

type JobSLAPredictorRequestConfig struct {
	ReferenceTime        time.Time
	ScheduleRangeInHours time.Duration
	SkipJobNames         []string
	EnableAlert          bool
	Severity             string
	DamperFactor         scheduler.DamperFactor
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
	SendPotentialSLABreach(alert *scheduler.PotentialSLABreachAlert)
}

type DurationEstimator interface {
	GetPercentileDurationByJobNames(ctx context.Context, referenceTime time.Time, jobNames []scheduler.JobName) (map[scheduler.JobName]*time.Duration, error)
	GetPercentileDurationByJobNamesByTask(ctx context.Context, referenceTime time.Time, jobNames []scheduler.JobName) (map[scheduler.JobName]*time.Duration, error)
	GetPercentileDurationByJobNamesByHookName(ctx context.Context, referenceTime time.Time, jobNames []scheduler.JobName, hookNames []string) (map[scheduler.JobName]*time.Duration, error)
}

type JobDetailsGetter interface {
	GetJobs(ctx context.Context, projectName tenant.ProjectName, jobs []string) ([]*scheduler.JobWithDetails, error)
	GetJobsByLabels(ctx context.Context, projectName tenant.ProjectName, labels map[string]string) ([]*scheduler.JobWithDetails, error)
	// GetJobsByLabelsMultiValue is like GetJobsByLabels but each key may match any of several
	// values (OR'd within a key, AND'd across keys).
	GetJobsByLabelsMultiValue(ctx context.Context, projectName tenant.ProjectName, labels map[string][]string) ([]*scheduler.JobWithDetails, error)
}

type SLAPredictorRepository interface {
	StorePredictedSLABreach(ctx context.Context, jobTargetName, jobCauseName scheduler.JobName, targetedSLA, jobScheduledAt time.Time, cause string, referenceTime time.Time, config map[string]interface{}, lineages []interface{}) error
}

type ScheduledChangeGetter interface {
	GetRecentScheduleChange(ctx context.Context, jobName scheduler.JobName, tnnt tenant.Tenant, startTime time.Time) (string, error)
}

type JobSLAPredictorService struct {
	l                 log.Logger
	config            config.PotentialSLABreachConfig
	repo              SLAPredictorRepository
	jobDetailsGetter  JobDetailsGetter
	jobLineageFetcher JobLineageFetcher
	durationEstimator DurationEstimator
	tenantGetter      TenantGetter
	rootCause         RootCauseIdentifier
	// alerting purpose
	potentialSLANotifier PotentialSLANotifier
}

type RootCauseIdentifier interface {
	Identify(ctx context.Context, jobTarget *scheduler.JobLineageSummary, jobDurations map[scheduler.JobName]*time.Duration, targetedSLA *time.Time, skipJobNames map[scheduler.JobName]bool, damperFactor scheduler.DamperFactor, referenceTime time.Time) (map[scheduler.JobName]*scheduler.JobState, map[scheduler.JobName][]*scheduler.JobState)
	CalculateInferredSLAs(jobTarget *scheduler.JobLineageSummary, jobDurations map[scheduler.JobName]*time.Duration, targetedSLA *time.Time, damperFactor scheduler.DamperFactor) (map[scheduler.JobName]*time.Time, rootcause.BottleneckPath)
}

func NewJobSLAPredictorService(l log.Logger, config config.PotentialSLABreachConfig, slaPredictorRepo SLAPredictorRepository, jobLineageFetcher JobLineageFetcher, durationEstimator DurationEstimator, jobDetailsGetter JobDetailsGetter, potentialSLANotifier PotentialSLANotifier, tenantGetter TenantGetter, scheduledChangeGetter ScheduledChangeGetter, pendingSensorGetter rootcause.PendingSensorGetter, thirdPartyTypes []string) *JobSLAPredictorService {
	return &JobSLAPredictorService{
		l:                    l,
		config:               config,
		repo:                 slaPredictorRepo,
		jobLineageFetcher:    jobLineageFetcher,
		durationEstimator:    durationEstimator,
		jobDetailsGetter:     jobDetailsGetter,
		tenantGetter:         tenantGetter,
		rootCause:            rootcause.NewIdentifier(l, scheduledChangeGetter, pendingSensorGetter, thirdPartyTypes),
		potentialSLANotifier: potentialSLANotifier,
	}
}

// Kept on the service so existing callers and tests keep working after the move.
func (s *JobSLAPredictorService) IdentifySLABreach(ctx context.Context, jobTarget *scheduler.JobLineageSummary, jobDurations map[scheduler.JobName]*time.Duration, targetedSLA *time.Time, skipJobNames map[scheduler.JobName]bool, damperFactor scheduler.DamperFactor, referenceTime time.Time) (map[scheduler.JobName]*scheduler.JobState, map[scheduler.JobName][]*scheduler.JobState) {
	return s.rootCause.Identify(ctx, jobTarget, jobDurations, targetedSLA, skipJobNames, damperFactor, referenceTime)
}

func (s *JobSLAPredictorService) CalculateInferredSLAs(jobTarget *scheduler.JobLineageSummary, jobDurations map[scheduler.JobName]*time.Duration, targetedSLA *time.Time, damperFactor scheduler.DamperFactor) (map[scheduler.JobName]*time.Time, rootcause.BottleneckPath) {
	return s.rootCause.CalculateInferredSLAs(jobTarget, jobDurations, targetedSLA, damperFactor)
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

	// damper factor to use; fall back to config alpha if not provided
	damperFactor := reqConfig.DamperFactor
	if damperFactor.Alpha <= 0 {
		damperFactor.Alpha = s.config.DamperCoeff
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
	jobsWithLineageMap, err := s.jobLineageFetcher.GetJobLineage(ctx, jobSchedules, int(reqConfig.ScheduleRangeInHours.Hours()))
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
		breachesCauses, fullBreachesCauses := s.IdentifySLABreach(ctx, jobWithLineage, jobDurations, targetSLA, skipJobNames, damperFactor, reqConfig.ReferenceTime)
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
		s.sendBreachAlerts(ctx, results, reqConfig)
	}

	if s.config.EnablePersistentLogging {
		s.storeBreachResults(ctx, results, reqConfig)
	}
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

// sendBreachAlerts emits one alert per (team, root cause, scheduled at, reason), which is
// the key deduplication reads -- so one alert must never carry more than one root cause.
// Routing still follows the cause-owning team; moving it to the impacted team is a
// separate change so it can be reverted without losing this deduplication.
func (s *JobSLAPredictorService) sendBreachAlerts(ctx context.Context, results []*comboBreachResult, reqConfig JobSLAPredictorRequestConfig) {
	totalBreaches := 0
	for _, r := range results {
		totalBreaches += len(r.jobBreachCauses)
	}
	if totalBreaches == 0 {
		return
	}
	s.l.Info("potential SLA breaches found", "count", totalBreaches)

	agg := scheduler.NewBreachAlertAggregator()
	teamCache := map[tenant.Tenant]string{}
	for _, r := range results {
		for targetName, upstreamCauses := range r.jobBreachCauses {
			for _, upstreamCause := range upstreamCauses {
				team := s.resolveTeam(ctx, upstreamCause.Tenant, teamCache)
				if team == "" {
					continue
				}
				agg.Add(team, targetName.String(), upstreamCause, r.combo.ProjectName.String(), reqConfig.Severity)
			}
		}
	}

	for _, alert := range agg.Build() {
		s.potentialSLANotifier.SendPotentialSLABreach(alert)
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
