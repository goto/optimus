package service

import (
	"context"
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

// RootCauseRequestConfig is the service-level request for the root-cause publisher.
type RootCauseRequestConfig struct {
	ProjectNames         []tenant.ProjectName
	JobNames             []scheduler.JobName
	Labels               map[string][]string
	ReferenceTime        time.Time
	EnableAlert          bool
	ScheduleRangeInHours time.Duration
}

// JobRootCauseService is the Publisher of the Publisher-Subscriber SLA alerting model: it
// evaluates project-level targets, classifies why each SLA breach is happening into the six
// tiers, groups the findings into incidents keyed by the offending entity, and persists
// them. Alerting and deduplication are layered on top of the persisted incidents.
type JobRootCauseService struct {
	l                 log.Logger
	config            config.RootCauseAlertConfig
	jobDetailsGetter  JobDetailsGetter
	jobLineageFetcher JobLineageFetcher
	// durationEstimator feeds both the inferred-SLA calculation and the RUNTIME_DELAY
	// induced-delay baseline, with the potential-SLA API's settings (single source of
	// truth: alerting.potential_sla_breach_config.duration_estimator_config)
	durationEstimator     DurationEstimator
	potentialConfig       config.PotentialSLABreachConfig
	startDelayEstimator   StartDelayEstimator
	scheduledChangeGetter rootcause.ScheduledChangeGetter
	incidentRepo          scheduler.SLABreachIncidentRepository
	publisher             *rootcause.Publisher
	// alert routing
	rootCauseNotifier RootCauseBreachNotifier
	projectTeamGetter ProjectTeamGetter
	teamCache         map[tenant.ProjectName]string
	// lineageBatchSize caps how many targets get their upstream lineage built at once
	// (root_cause_alert.lineage_batch_size, defaultLineageBatchSize when unset); keeps
	// peak memory proportional to the batch
	lineageBatchSize int
}

// RootCauseBreachNotifier relays one root-cause breach alert; implemented by the
// alertmanager notifier.
type RootCauseBreachNotifier interface {
	SendRootCauseBreach(alert *scheduler.RootCauseBreachAlert)
}

// ProjectTeamGetter resolves the alerting team configured on a project.
type ProjectTeamGetter interface {
	Get(ctx context.Context, projectName tenant.ProjectName) (*tenant.Project, error)
}

// defaultLineageBatchSize caps how many targets get their upstream lineage built at once.
// A single identify request over a large label match used to hold every target's full
// graph in memory simultaneously and could exhaust the server; each batch is fetched,
// evaluated and released before the next one is built.
const defaultLineageBatchSize = 50

func NewJobRootCauseService(l log.Logger, alertConfig config.RootCauseAlertConfig, potentialConfig config.PotentialSLABreachConfig,
	jobDetailsGetter JobDetailsGetter, jobLineageFetcher JobLineageFetcher,
	durationEstimator DurationEstimator, startDelayEstimator StartDelayEstimator,
	scheduledChangeGetter rootcause.ScheduledChangeGetter,
	incidentRepo scheduler.SLABreachIncidentRepository, thirdPartyTypes []string,
	rootCauseNotifier RootCauseBreachNotifier, projectTeamGetter ProjectTeamGetter,
) *JobRootCauseService {
	batchSize := alertConfig.LineageBatchSize
	if batchSize <= 0 {
		batchSize = defaultLineageBatchSize
	}
	return &JobRootCauseService{
		l:                     l,
		config:                alertConfig,
		potentialConfig:       potentialConfig,
		jobDetailsGetter:      jobDetailsGetter,
		jobLineageFetcher:     jobLineageFetcher,
		durationEstimator:     durationEstimator,
		startDelayEstimator:   startDelayEstimator,
		scheduledChangeGetter: scheduledChangeGetter,
		incidentRepo:          incidentRepo,
		rootCauseNotifier:     rootCauseNotifier,
		projectTeamGetter:     projectTeamGetter,
		teamCache:             map[tenant.ProjectName]string{},
		lineageBatchSize:      batchSize,
		publisher: rootcause.NewPublisher(rootcause.PublisherConfig{
			RuntimeDelayThreshold:       time.Duration(alertConfig.RuntimeDelayThresholdMinutes) * time.Minute,
			SchedulerDelayPerDepth:      time.Duration(alertConfig.SchedulerDelayMinutesPerDepth * float64(time.Minute)),
			ThirdPartyDelayThreshold:    time.Duration(alertConfig.ThirdPartyDelayThresholdSeconds) * time.Second,
			ThirdPartyDelayStartHourUTC: alertConfig.ThirdPartyDelayStartHourUTC,
			ThirdPartyTypes:             thirdPartyTypes,
		}),
	}
}

// IdentifySLABreachRootCause evaluates every (project, labels) target set, groups the tier
// detections into incidents and — unless running in observe mode — persists them and alerts
// the responsible teams. The returned incidents carry the full assembled details and blast
// radius.
func (s *JobRootCauseService) IdentifySLABreachRootCause(ctx context.Context, reqConfig RootCauseRequestConfig) ([]*scheduler.SLABreachIncidentWithImpacts, error) {
	excluded := map[tenant.ProjectName]bool{}
	for _, projectName := range s.config.ExcludeProjects {
		excluded[tenant.ProjectName(projectName)] = true
	}

	me := errors.NewMultiError("IdentifySLABreachRootCause")
	var allDetections []scheduler.RootCauseDetection

	for _, projectName := range reqConfig.ProjectNames {
		if excluded[projectName] {
			s.l.Info("project excluded from root-cause evaluation, skipping", "project", projectName.String())
			continue
		}
		detections, err := s.evaluateProject(ctx, projectName, reqConfig)
		if err != nil {
			s.l.Error("failed to evaluate project for root causes, skipping", "project", projectName.String(), "error", err)
			me.Append(err)
			continue
		}
		allDetections = append(allDetections, detections...)
	}

	incidents := scheduler.GroupDetections(allDetections)
	s.l.Info("root-cause evaluation produced incidents", "detections", len(allDetections), "incidents", len(incidents))

	s.recordEvaluation(me.ToErr() != nil, len(incidents))
	for _, detection := range allDetections {
		rootCauseDetectionsTotal.WithLabelValues(string(detection.Reason.Reason), detection.IncidentKey.ProjectName.String()).Inc()
	}

	if reqConfig.EnableAlert {
		previousByKey := s.persistIncidents(ctx, reqConfig, incidents)
		s.sendRootCauseAlerts(ctx, incidents, previousByKey)
	}
	return incidents, me.ToErr()
}

// evaluateProject resolves the project's targets and evaluates each one's lineage.
func (s *JobRootCauseService) evaluateProject(ctx context.Context, projectName tenant.ProjectName, reqConfig RootCauseRequestConfig) ([]scheduler.RootCauseDetection, error) {
	jobsWithDetails, err := getJobWithDetailsMultiValue(ctx, s.l, s.jobDetailsGetter, projectName, reqConfig.JobNames, reqConfig.Labels)
	if err != nil {
		return nil, err
	}
	if len(jobsWithDetails) == 0 {
		s.l.Warn("no jobs found for root-cause evaluation, skipping", "project", projectName.String())
		return nil, nil
	}

	jobSchedules := getJobSchedules(s.l, jobsWithDetails, reqConfig.ScheduleRangeInHours, reqConfig.ReferenceTime)
	if len(jobSchedules) == 0 {
		s.l.Warn("no job schedules found in the schedule range, skipping root-cause evaluation", "project", projectName.String())
		return nil, nil
	}

	targetNames := make([]scheduler.JobName, 0, len(jobSchedules))
	for name := range jobSchedules {
		targetNames = append(targetNames, name)
	}
	sort.Slice(targetNames, func(i, j int) bool { return targetNames[i] < targetNames[j] })

	// one SLA deadline per target, resolved once for the whole project
	targetedSLA := getTargetedSLA(s.l, jobsWithDetails, jobSchedules)
	if len(targetedSLA) == 0 {
		s.l.Warn("no targeted SLA found in the schedule range, skipping root-cause evaluation", "project", projectName.String())
		return nil, nil
	}

	var detections []scheduler.RootCauseDetection
	for batchStart := 0; batchStart < len(targetNames); batchStart += s.lineageBatchSize {
		batchEnd := min(batchStart+s.lineageBatchSize, len(targetNames))
		batch := make(map[scheduler.JobName]*scheduler.JobSchedule, batchEnd-batchStart)
		for _, name := range targetNames[batchStart:batchEnd] {
			batch[name] = jobSchedules[name]
		}
		batchDetections, err := s.evaluateBatch(ctx, batch, targetedSLA, reqConfig)
		if err != nil {
			// one oversized or broken batch should not stop the remaining batches
			s.l.Error("failed to evaluate lineage batch for root causes, skipping",
				"project", projectName.String(), "targets", batchEnd-batchStart, "error", err)
			continue
		}
		detections = append(detections, batchDetections...)
	}
	return detections, nil
}

// evaluateBatch builds the upstream lineage for one batch of targets, runs the shared
// inferred-SLA breach analysis per target and hands the finding to the publisher.
func (s *JobRootCauseService) evaluateBatch(ctx context.Context, jobSchedules map[scheduler.JobName]*scheduler.JobSchedule, targetedSLA map[scheduler.JobName]*time.Time, reqConfig RootCauseRequestConfig) ([]scheduler.RootCauseDetection, error) {
	jobsWithLineageMap, err := s.jobLineageFetcher.GetJobLineage(ctx, jobSchedules, int(reqConfig.ScheduleRangeInHours.Hours()))
	if err != nil {
		return nil, err
	}

	uniqueJobNames := collectJobNames(jobsWithLineageMap)
	// one estimator fetch serves both the inferred-SLA calculation and the induced-delay
	// baseline — one config (the potential-SLA API's settings), one source of truth
	durationEstimates, err := s.durationEstimator.GetPercentileDurationByJobNames(ctx, reqConfig.ReferenceTime, uniqueJobNames)
	if err != nil {
		return nil, err
	}
	historicalByJob := make(map[scheduler.JobName]time.Duration, len(durationEstimates))
	for jobName, duration := range durationEstimates {
		if duration != nil {
			historicalByJob[jobName] = *duration
		}
	}
	startOffsets := map[scheduler.JobName]*time.Duration{}
	if s.startDelayEstimator != nil {
		startOffsets, err = s.startDelayEstimator.GetPercentileStartDelayByJobNames(ctx, uniqueJobNames, reqConfig.ReferenceTime,
			s.potentialConfig.HistoricalStartConfig.LastNRuns, s.potentialConfig.HistoricalStartConfig.Percentile)
		if err != nil {
			s.l.Error("failed to get historical start offsets, falling back to deadline-driven safe start", "error", err)
			startOffsets = map[scheduler.JobName]*time.Duration{}
		}
	}

	// one memoized getter per batch: the finder asks for the same (job, run) pair once
	// per target sharing that upstream, which otherwise multiplies into repeated queries
	changeGetter := newMemoizedScheduleGetter(s.scheduledChangeGetter)
	finder := rootcause.NewIdentifier(s.l, changeGetter, nil, nil, rootcause.IdentifierConfig{})
	damperFactor := s.damperFactor()

	var detections []scheduler.RootCauseDetection
	for _, jobSchedule := range jobSchedules {
		target, ok := jobsWithLineageMap[jobSchedule.JobName]
		if !ok || target == nil {
			continue
		}
		sla, ok := targetedSLA[jobSchedule.JobName]
		if !ok || sla == nil {
			continue
		}
		finding := finder.FindInferredBreaches(ctx, target, durationEstimates, startOffsets, sla, nil, damperFactor, reqConfig.ReferenceTime)
		detections = append(detections, s.publisher.Evaluate(rootcause.TargetEvaluation{
			Target:          target,
			HistoricalByJob: historicalByJob,
			Finding:         finding,
			ReferenceTime:   reqConfig.ReferenceTime,
		})...)
	}
	return detections, nil
}

// damperFactor mirrors the potential-SLA API's zero-value fallback so both engines infer
// identical deadlines from the same damper coefficient.
func (s *JobRootCauseService) damperFactor() scheduler.DamperFactor {
	return scheduler.DamperFactor{Alpha: s.potentialConfig.DamperCoeff}
}

// activeIncidentScanWindow is how far back the resolution pass looks for incidents that
// were still active before this tick. It mirrors the subscriber's freshness window: older
// rows are considered stale regardless and will age out anyway.
const activeIncidentScanWindow = 24 * time.Hour

// persistIncidents writes each fully assembled incident, then runs a resolution pass:
// incidents that were active before this tick but received no fresh detection have their
// remaining active reasons flipped to resolved. A failure on one incident is logged and
// skipped so the rest still persists; the tick's findings remain the source of truth.
// It returns the previously stored incident per key (nil entries for first-time
// incidents) so the alert router can detect state transitions, such as a root cause
// finishing.
func (s *JobRootCauseService) persistIncidents(ctx context.Context, reqConfig RootCauseRequestConfig, incidents []*scheduler.SLABreachIncidentWithImpacts) map[scheduler.SLABreachIncidentKey]*scheduler.SLABreachIncident {
	previousByKey := map[scheduler.SLABreachIncidentKey]*scheduler.SLABreachIncident{}
	freshKeys := map[scheduler.SLABreachIncidentKey]bool{}
	for _, incident := range incidents {
		freshKeys[incident.Incident.Key] = true

		previous, err := s.incidentRepo.GetIncidentByKey(ctx, incident.Incident.Key)
		switch {
		case err == nil:
			// found: merge against the stored state
		case errors.Is(err, scheduler.ErrSLABreachIncidentNotFound):
			// first write of this incident: nothing to resolve yet
			previous = nil
		default:
			s.l.Error("failed to read previous incident state, recording without resolution flip",
				"project", incident.Incident.Key.ProjectName.String(),
				"job", incident.Incident.Key.JobName.String(),
				"error", err)
			previous = nil
		}
		previousByKey[incident.Incident.Key] = previous

		merged := scheduler.MergeResolvedReasons(incident.Incident, previous)
		if err := s.incidentRepo.UpsertIncident(ctx, merged, incident.Impacts); err != nil {
			s.l.Error("failed to persist SLA breach incident",
				"project", incident.Incident.Key.ProjectName.String(),
				"job", incident.Incident.Key.JobName.String(),
				"error", err)
		}
	}

	s.resolveUntouchedIncidents(ctx, reqConfig, freshKeys)
	return previousByKey
}

// resolveUntouchedIncidents flips the remaining active reasons of every tracked incident
// that received no fresh detection this tick — the case where an incident disappears
// entirely (its root cause recovered and no target breaches anymore), which the fresh
// merge cannot see because there is no fresh incident to merge into. Freshly detected
// incidents are skipped: their own merge already flips partially-disappeared reasons.
// Fully resolved incidents drop out of the active scan, so they are written exactly once.
func (s *JobRootCauseService) resolveUntouchedIncidents(ctx context.Context, reqConfig RootCauseRequestConfig, freshKeys map[scheduler.SLABreachIncidentKey]bool) {
	tracked, err := s.incidentRepo.GetActiveIncidents(ctx, reqConfig.ReferenceTime.Add(-activeIncidentScanWindow))
	if err != nil {
		s.l.Error("failed to scan active incidents for resolution, deferring their flip to a later tick", "error", err)
		return
	}

	for _, trackedIncident := range tracked {
		if freshKeys[trackedIncident.Incident.Key] {
			continue
		}

		// an empty fresh observation over the tracked incident resolves everything it
		// still had active; the empty impact list clears the blast radius as well
		emptyObservation := &scheduler.SLABreachIncident{
			ID:        trackedIncident.Incident.ID,
			Key:       trackedIncident.Incident.Key,
			CreatedAt: trackedIncident.Incident.CreatedAt,
			UpdatedAt: trackedIncident.Incident.UpdatedAt,
		}
		merged := scheduler.MergeResolvedReasons(emptyObservation, trackedIncident.Incident)

		if err := s.incidentRepo.UpsertIncident(ctx, merged, nil); err != nil {
			s.l.Error("failed to persist resolved SLA breach incident",
				"project", merged.Key.ProjectName.String(),
				"job", merged.Key.JobName.String(),
				"error", err)
			continue
		}
		s.l.Info("root-cause incident fully resolved, no longer observed in any lineage",
			"project", merged.Key.ProjectName.String(),
			"job", merged.Key.JobName.String())
	}
}

// sendRootCauseAlerts routes one alert per active reason: platform-attributed reasons go to
// their configured team, everything else to the root-cause project's owner. An empty team
// (unset config or no ALERTMANAGER_TEAM on the project) disables that route. Repetition is
// suppressed downstream by the alert_logs deduplication for the rootcause_breach type —
// except for a finished root cause, which notifies exactly once per incident through the
// run-state transition instead of repeating hourly.
func (s *JobRootCauseService) sendRootCauseAlerts(ctx context.Context, incidents []*scheduler.SLABreachIncidentWithImpacts, previousByKey map[scheduler.SLABreachIncidentKey]*scheduler.SLABreachIncident) {
	if s.rootCauseNotifier == nil {
		return
	}
	for _, incident := range incidents {
		for _, reason := range incident.Incident.ActiveReasons() {
			if s.suppressAlert(reason) {
				rootCauseAlertsSuppressedTotal.WithLabelValues(string(reason.Reason)).Inc()
				s.l.Info("root-cause alert suppressed by configuration, incident still recorded",
					"reason", string(reason.Reason),
					"project", incident.Incident.Key.ProjectName.String(),
					"job", incident.Incident.Key.JobName.String(),
					"induced_delay_seconds", reason.InducedDelaySeconds)
				continue
			}
			// a delay-measuring reason whose offending run has finished gets exactly one
			// notification: the first finished-state observation. Later ticks see the
			// stored finished flag and stay silent until the incident resolves.
			if isDelayMeasuredReason(reason.Reason) && reason.IsJobFinished {
				if previousReason, hadPrevious := previousReasonOf(previousByKey, incident.Incident.Key, reason.Reason); hadPrevious && previousReason.IsJobFinished {
					rootCauseAlertsSuppressedTotal.WithLabelValues(string(reason.Reason)).Inc()
					s.l.Debug("root-cause alert suppressed: the finished cause was already notified",
						"reason", string(reason.Reason),
						"project", incident.Incident.Key.ProjectName.String(),
						"job", incident.Incident.Key.JobName.String())
					continue
				}
			}
			team := s.routeTeam(ctx, incident.Incident.Key.ProjectName, reason.Reason)
			if team == "" {
				s.l.Debug("no routing team for root-cause reason, skipping alert",
					"reason", string(reason.Reason),
					"project", incident.Incident.Key.ProjectName.String())
				continue
			}
			s.rootCauseNotifier.SendRootCauseBreach(func() *scheduler.RootCauseBreachAlert {
				alert := &scheduler.RootCauseBreachAlert{
					Team:                 team,
					Project:              incident.Incident.Key.ProjectName.String(),
					RootCauseProject:     incident.Incident.Key.ProjectName.String(),
					RootCauseJob:         incident.Incident.Key.JobName.String(),
					RootCauseScheduledAt: &incident.Incident.Key.ScheduledAt,
					Reason:               reason,
					ImpactedJobs:         impactedJobNames(incident.Impacts),
					Severity:             s.config.AlertSeverity,
					BreachStatus:         incident.Incident.Details.BreachStatus,
				}
				return alert
			}())
		}
	}
}

func (s *JobRootCauseService) suppressAlert(reason scheduler.RootCauseReasonDetail) bool {
	for _, skipped := range s.config.AlertSuppression.SkipReasons {
		if strings.EqualFold(strings.TrimSpace(skipped), string(reason.Reason)) {
			return true
		}
	}

	if !isDelayMeasuredReason(reason.Reason) {
		return false
	}
	minSeconds := s.config.AlertSuppression.MinTotalInducedDelayMinutes * 60
	return reason.InducedDelaySeconds < minSeconds
}

func isDelayMeasuredReason(reason scheduler.SLABreachTier) bool {
	switch reason {
	case scheduler.TierRuntimeDelay, scheduler.TierSchedulerDelay, scheduler.TierThirdPartyDelay:
		return true
	default:
		return false
	}
}

func (s *JobRootCauseService) routeTeam(ctx context.Context, projectName tenant.ProjectName, reason scheduler.SLABreachTier) string {
	switch reason {
	case scheduler.TierSchedulerDelay:
		return s.config.InfraTeam
	case scheduler.TierThirdPartyDelay:
		return s.config.ThirdPartyTeam
	default:
		return s.projectOwnerTeam(ctx, projectName)
	}
}

// projectOwnerTeam reads ALERTMANAGER_TEAM from the project config, caching the result
// (including misses) so repeated incidents of one tick do not re-query.
func (s *JobRootCauseService) projectOwnerTeam(ctx context.Context, projectName tenant.ProjectName) string {
	if team, ok := s.teamCache[projectName]; ok {
		return team
	}
	team := ""
	project, err := s.projectTeamGetter.Get(ctx, projectName)
	if err != nil {
		s.l.Error("failed to get project for root-cause routing", "project", projectName.String(), "error", err)
	} else if configured, err := project.GetConfig(tenant.ProjectAlertManagerTeam); err != nil || configured == "" {
		s.l.Warn("no alertmanager team configured for project, skipping root-cause alert", "project", projectName.String())
	} else {
		team = configured
	}
	s.teamCache[projectName] = team
	return team
}

// previousReasonOf looks up how a reason looked in the previously stored incident.
func previousReasonOf(previousByKey map[scheduler.SLABreachIncidentKey]*scheduler.SLABreachIncident, key scheduler.SLABreachIncidentKey, reason scheduler.SLABreachTier) (scheduler.RootCauseReasonDetail, bool) {
	previous := previousByKey[key]
	if previous == nil {
		return scheduler.RootCauseReasonDetail{}, false
	}
	for _, previousReason := range previous.Details.Reasons {
		if previousReason.Reason == reason {
			return previousReason, true
		}
	}
	return scheduler.RootCauseReasonDetail{}, false
}

// impactedJobNames renders the blast radius as project-qualified job names for the payload.
func impactedJobNames(impacts []scheduler.ImpactedJob) []string {
	names := make([]string, 0, len(impacts))
	for _, impacted := range impacts {
		names = append(names, impacted.ProjectName.String()+"/"+impacted.JobName.String())
	}
	return names
}

// memoizedScheduleGetter caches recent-schedule-change lookups for the lifetime of one
// evaluation batch: the inferred-SLA finder asks for the same (job, run) pair once per
// target sharing that upstream, which otherwise multiplies into repeated queries. Errors
// are not cached so a transient failure retries on the next target.
type memoizedScheduleGetter struct {
	inner rootcause.ScheduledChangeGetter
	cache map[string]string
}

func newMemoizedScheduleGetter(inner rootcause.ScheduledChangeGetter) *memoizedScheduleGetter {
	return &memoizedScheduleGetter{inner: inner, cache: map[string]string{}}
}

func (m *memoizedScheduleGetter) GetRecentScheduleChange(ctx context.Context, jobName scheduler.JobName, tnnt tenant.Tenant, scheduledAt time.Time) (string, error) {
	key := tnnt.ProjectName().String() + "/" + jobName.String() + "/" + scheduledAt.UTC().Format(time.RFC3339)
	if hit, ok := m.cache[key]; ok {
		return hit, nil
	}
	value, err := m.inner.GetRecentScheduleChange(ctx, jobName, tnnt, scheduledAt)
	if err != nil {
		return value, err
	}
	m.cache[key] = value
	return value, nil
}
