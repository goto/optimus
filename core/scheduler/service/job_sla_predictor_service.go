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

type StartDelayEstimator interface {
	GetPercentileStartDelayByJobNames(ctx context.Context, jobNames []scheduler.JobName, referenceTime time.Time, lastNRuns, percentile int) (map[scheduler.JobName]*time.Duration, error)
}

type SLAPredictorRepository interface {
	StorePredictedSLABreach(ctx context.Context, jobTargetName, jobCauseName scheduler.JobName, targetedSLA, jobScheduledAt time.Time, cause string, referenceTime time.Time, config map[string]interface{}, lineages []interface{}) error
	GetPredictedTargetJobNames(ctx context.Context, targets []*scheduler.JobSchedule) ([]scheduler.JobName, error)
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
	startDelayEstimator  StartDelayEstimator
}

type RootCauseIdentifier interface {
	Identify(ctx context.Context, jobTarget *scheduler.JobLineageSummary, jobDurations, startOffsets map[scheduler.JobName]*time.Duration, targetedSLA *time.Time, skipJobNames map[scheduler.JobName]bool, damperFactor scheduler.DamperFactor, referenceTime time.Time) (map[scheduler.JobName]*scheduler.JobState, map[scheduler.JobName][]*scheduler.JobState)
	CalculateInferredSLAs(jobTarget *scheduler.JobLineageSummary, jobDurations map[scheduler.JobName]*time.Duration, targetedSLA *time.Time, damperFactor scheduler.DamperFactor) (map[scheduler.JobName]*time.Time, rootcause.BottleneckPath)
}

func NewJobSLAPredictorService(l log.Logger, config config.PotentialSLABreachConfig, rootCauseConfig config.RootCauseConfig, slaPredictorRepo SLAPredictorRepository, jobLineageFetcher JobLineageFetcher, durationEstimator DurationEstimator, jobDetailsGetter JobDetailsGetter, potentialSLANotifier PotentialSLANotifier, tenantGetter TenantGetter, scheduledChangeGetter ScheduledChangeGetter, pendingSensorGetter rootcause.PendingSensorGetter, thirdPartyTypes []string, startDelayEstimator StartDelayEstimator) *JobSLAPredictorService {
	return &JobSLAPredictorService{
		l:                    l,
		config:               config,
		repo:                 slaPredictorRepo,
		jobLineageFetcher:    jobLineageFetcher,
		durationEstimator:    durationEstimator,
		jobDetailsGetter:     jobDetailsGetter,
		tenantGetter:         tenantGetter,
		rootCause:            rootcause.NewIdentifier(l, scheduledChangeGetter, pendingSensorGetter, thirdPartyTypes, identifierConfig(rootCauseConfig)),
		potentialSLANotifier: potentialSLANotifier,
		startDelayEstimator:  startDelayEstimator,
	}
}

func identifierConfig(cfg config.RootCauseConfig) rootcause.IdentifierConfig {
	return rootcause.IdentifierConfig{
		ThirdPartyDelayStartHourUTC:     cfg.ThirdPartyDelayStartHourUTC,
		ThirdPartyDelayThresholdSeconds: cfg.ThirdPartyDelayThresholdSeconds,
		MinRootCauseDelaySeconds:        cfg.MinRootCauseDelaySeconds,
		MaxRootCauseClimbDepth:          cfg.MaxRootCauseClimbDepth,
	}
}

// Kept on the service so existing callers and tests keep working after the move.
func (s *JobSLAPredictorService) IdentifySLABreach(ctx context.Context, jobTarget *scheduler.JobLineageSummary, jobDurations, startOffsets map[scheduler.JobName]*time.Duration, targetedSLA *time.Time, skipJobNames map[scheduler.JobName]bool, damperFactor scheduler.DamperFactor, referenceTime time.Time) (map[scheduler.JobName]*scheduler.JobState, map[scheduler.JobName][]*scheduler.JobState) {
	return s.rootCause.Identify(ctx, jobTarget, jobDurations, startOffsets, targetedSLA, skipJobNames, damperFactor, referenceTime)
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
	targetedSLA := getTargetedSLA(s.l, jobsWithDetails, jobSchedules)
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

	// get historical start-time offsets: the preferred (schedule-rhythm-aware) basis for each
	// job's "safe start" inside rootcause.Identify (see effectiveSafeStart).
	jobStartOffsets := map[scheduler.JobName]*time.Duration{}
	if s.startDelayEstimator != nil {
		jobStartOffsets, err = s.startDelayEstimator.GetPercentileStartDelayByJobNames(ctx, uniqueJobNames, reqConfig.ReferenceTime,
			s.config.HistoricalStartConfig.LastNRuns, s.config.HistoricalStartConfig.Percentile)
		if err != nil {
			s.l.Error("failed to get historical start offsets, falling back to deadline-driven safe start", "error", err)
			jobStartOffsets = map[scheduler.JobName]*time.Duration{}
		}
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
		breachesCauses, fullBreachesCauses := s.IdentifySLABreach(ctx, jobWithLineage, jobDurations, jobStartOffsets, targetSLA, skipJobNames, damperFactor, reqConfig.ReferenceTime)
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

// storePredictedSLABreach stores the predicted SLA breaches in the repository for further analysis.
func (s *JobSLAPredictorService) storePredictedSLABreach(ctx context.Context, jobTarget *scheduler.JobLineageSummary, slaTarget time.Time, paths map[scheduler.JobName][]*scheduler.JobState, reqConfig JobSLAPredictorRequestConfig) error {
	for _, path := range paths {
		if len(path) == 0 {
			continue
		}
		scheduledAt := targetScheduledAt(jobTarget)
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
		err = s.repo.StorePredictedSLABreach(ctx, jobTarget.JobName, cause.JobName, slaTarget, scheduledAt, storedCause(cause), reqConfig.ReferenceTime, config, lineages)
		if err != nil {
			return err
		}
	}
	return nil
}

// targetScheduledAt is the run stored as sla_predictor.job_scheduled_at, and therefore the
// run that impacted-job deduplication keys on; both must resolve it the same way.
func targetScheduledAt(jobTarget *scheduler.JobLineageSummary) time.Time {
	if run := jobTarget.JobRuns[jobTarget.JobName]; run != nil {
		return run.ScheduledAt
	}
	for _, jobRun := range jobTarget.JobRuns {
		return jobRun.ScheduledAt
	}
	return time.Time{}
}

func storedCause(cause *scheduler.JobState) string {
	if cause == nil {
		return ""
	}
	if cause.Reason == "" {
		return string(cause.Status)
	}
	if cause.Status == "" {
		return string(cause.Reason)
	}
	return string(cause.Status) + "/" + string(cause.Reason)
}

// sendBreachAlerts Alerts go to the team owning the at-risk SLA job, not the team owning the root cause:
// upstream anomalies already reach that team through task duration, sensor and failure
// alerts, so alerting them again here would just duplicate a signal they have.
func (s *JobSLAPredictorService) sendBreachAlerts(ctx context.Context, results []*comboBreachResult, reqConfig JobSLAPredictorRequestConfig) {
	totalBreaches := 0
	for _, r := range results {
		totalBreaches += len(r.jobBreachCauses)
	}
	if totalBreaches == 0 {
		return
	}
	s.l.Info("potential SLA breaches found", "count", totalBreaches)

	suppressed := s.predictedTargets(ctx, results)

	agg := scheduler.NewBreachAlertAggregator()
	teamCache := map[tenant.Tenant]string{}
	for _, r := range results {
		for targetName, upstreamCauses := range r.jobBreachCauses {
			target := r.jobsWithLineageMap[targetName]
			if target == nil {
				continue
			}
			if suppressed[targetName] {
				s.l.Info("skipping impacted job for alerting as its run was already predicted to breach", "job", targetName.String())
				continue
			}
			team := s.resolveTeam(ctx, target.Tenant, teamCache)
			if team == "" {
				continue
			}
			for _, upstreamCause := range upstreamCauses {
				agg.Add(team, targetName.String(), upstreamCause, r.combo.ProjectName.String(), reqConfig.Severity)
			}
		}
	}

	alerts := agg.Build()
	s.enrichBreachAlerts(alerts, reqConfig)
	for _, alert := range alerts {
		s.potentialSLANotifier.SendPotentialSLABreach(alert)
	}
}

// enrichBreachAlerts mirrors each root cause's safe-start evidence onto the alert's display
// fields just before sending. Value:
//   - HistoricalStartTime: the safe start (historical percentile offset when available, floored
//     at the job's scheduled_at).
//   - CurrentStartTime: the actual start time for a started cause, otherwise the reference
//     ("now") time.
//   - TotalInducedDelay: the cause's induced delay measured against that safe start.
//
// Causes without safe-start evidence (UPSTREAM_FAILED, THIRD_PARTY_DELAY) measure delay against
// a different basis; their delay reaches the payload through the evidence fields instead, and
// no start-time fields are set here.
func (*JobSLAPredictorService) enrichBreachAlerts(alerts []*scheduler.PotentialSLABreachAlert, reqConfig JobSLAPredictorRequestConfig) {
	for _, alert := range alerts {
		if alert.Evidence.LatestSafeStartAt == nil {
			continue
		}
		historicalStart := *alert.Evidence.LatestSafeStartAt
		alert.HistoricalStartTime = &historicalStart

		currentStart := reqConfig.ReferenceTime
		if alert.Evidence.StartedAt != nil {
			currentStart = *alert.Evidence.StartedAt
		}
		alert.CurrentStartTime = &currentStart

		delay := alert.Evidence.InducedDelay
		alert.TotalInducedDelay = &delay
	}
}

// predictedTargets returns the impacted jobs whose current run already has a stored breach
// prediction. Matching is by job name alone across projects, as job names are unique.
func (s *JobSLAPredictorService) predictedTargets(ctx context.Context, results []*comboBreachResult) map[scheduler.JobName]bool {
	suppressed := map[scheduler.JobName]bool{}
	if !s.config.DedupImpactedJobs {
		return suppressed
	}
	if !s.config.EnablePersistentLogging {
		s.l.Warn("persistent logging is disabled, cannot deduplicate impacted jobs")
		return suppressed
	}

	var targets []*scheduler.JobSchedule
	for _, r := range results {
		for targetName := range r.jobBreachCauses {
			target := r.jobsWithLineageMap[targetName]
			if target == nil {
				continue
			}
			targets = append(targets, &scheduler.JobSchedule{JobName: targetName, ScheduledAt: targetScheduledAt(target)})
		}
	}

	predicted, err := s.repo.GetPredictedTargetJobNames(ctx, targets)
	if err != nil {
		s.l.Error("failed to get predicted SLA targets, sending alerts without impacted job deduplication", "error", err)
		return suppressed
	}
	for _, jobName := range predicted {
		suppressed[jobName] = true
	}
	s.l.Info("computed impacted job deduplication set", "count", len(suppressed))
	return suppressed
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
