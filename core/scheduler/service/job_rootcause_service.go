package service

import (
	"context"
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
	durationEstimator DurationEstimator
	incidentRepo      scheduler.SLABreachIncidentRepository
	publisher         *rootcause.Publisher
	// alert routing
	rootCauseNotifier RootCauseBreachNotifier
	projectTeamGetter ProjectTeamGetter
	teamCache         map[tenant.ProjectName]string
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

func NewJobRootCauseService(l log.Logger, config config.RootCauseAlertConfig, jobDetailsGetter JobDetailsGetter,
	jobLineageFetcher JobLineageFetcher, durationEstimator DurationEstimator,
	incidentRepo scheduler.SLABreachIncidentRepository, thirdPartyTypes []string,
	rootCauseNotifier RootCauseBreachNotifier, projectTeamGetter ProjectTeamGetter,
) *JobRootCauseService {
	return &JobRootCauseService{
		l:                 l,
		config:            config,
		jobDetailsGetter:  jobDetailsGetter,
		jobLineageFetcher: jobLineageFetcher,
		durationEstimator: durationEstimator,
		incidentRepo:      incidentRepo,
		rootCauseNotifier: rootCauseNotifier,
		projectTeamGetter: projectTeamGetter,
		teamCache:         map[tenant.ProjectName]string{},
		publisher: rootcause.NewPublisher(rootcause.PublisherConfig{
			RuntimeDelayThreshold:       time.Duration(config.RuntimeDelayThresholdMinutes) * time.Minute,
			SchedulerDelayPerDepth:      time.Duration(config.SchedulerDelayMinutesPerDepth * float64(time.Minute)),
			ThirdPartyDelayThreshold:    time.Duration(config.ThirdPartyDelayThresholdSeconds) * time.Second,
			ThirdPartyDelayStartHourUTC: config.ThirdPartyDelayStartHourUTC,
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
		s.persistIncidents(ctx, incidents)
		s.sendRootCauseAlerts(ctx, incidents)
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

	jobsWithLineageMap, err := s.jobLineageFetcher.GetJobLineage(ctx, jobSchedules, int(reqConfig.ScheduleRangeInHours.Hours()))
	if err != nil {
		return nil, err
	}

	uniqueJobNames := collectJobNames(jobsWithLineageMap)
	historicalByJob, err := s.fetchHistoricalDurations(ctx, reqConfig.ReferenceTime, uniqueJobNames, jobsWithLineageMap)
	if err != nil {
		return nil, err
	}

	var detections []scheduler.RootCauseDetection
	for _, jobSchedule := range jobSchedules {
		target, ok := jobsWithLineageMap[jobSchedule.JobName]
		if !ok || target == nil {
			continue
		}
		detections = append(detections, s.publisher.Evaluate(rootcause.TargetEvaluation{
			Target:          target,
			HistoricalByJob: historicalByJob,
			ReferenceTime:   reqConfig.ReferenceTime,
		})...)
	}
	return detections, nil
}

// fetchHistoricalDurations builds the per-job historical task/hook baseline the Tier 1
// induced-delay math compares against. Jobs without history evaluate against a zero baseline.
func (s *JobRootCauseService) fetchHistoricalDurations(ctx context.Context, referenceTime time.Time, jobNames []scheduler.JobName, jobsWithLineage map[scheduler.JobName]*scheduler.JobLineageSummary) (map[scheduler.JobName]scheduler.JobHistoricalDuration, error) {
	historical := make(map[scheduler.JobName]scheduler.JobHistoricalDuration, len(jobNames))

	taskDurations, err := s.durationEstimator.GetPercentileDurationByJobNamesByTask(ctx, referenceTime, jobNames)
	if err != nil {
		return nil, err
	}
	for _, jobName := range jobNames {
		if duration := taskDurations[jobName]; duration != nil {
			historical[jobName] = scheduler.JobHistoricalDuration{TaskDuration: *duration}
		}
	}

	hookNames := collectHookNames(jobsWithLineage)
	if len(hookNames) == 0 {
		return historical, nil
	}
	hookDurations, err := s.durationEstimator.GetPercentileDurationByJobNamesByHookName(ctx, referenceTime, jobNames, hookNames)
	if err != nil {
		return nil, err
	}
	for _, jobName := range jobNames {
		if duration := hookDurations[jobName]; duration != nil {
			entry := historical[jobName]
			entry.HookDuration = *duration
			historical[jobName] = entry
		}
	}
	return historical, nil
}

// collectHookNames gathers every distinct hook name in the lineages so the hook baseline
// can be fetched in one batch.
func collectHookNames(jobsWithLineage map[scheduler.JobName]*scheduler.JobLineageSummary) []string {
	seen := map[string]bool{}
	stack := []*scheduler.JobLineageSummary{}
	for _, lineage := range jobsWithLineage {
		stack = append(stack, lineage)
	}
	for len(stack) > 0 {
		lineage := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		for _, run := range lineage.JobRuns {
			if run.HookName != nil && !seen[*run.HookName] {
				seen[*run.HookName] = true
			}
		}
		stack = append(stack, lineage.Upstreams...)
	}
	hookNames := make([]string, 0, len(seen))
	for hookName := range seen {
		hookNames = append(hookNames, hookName)
	}
	return hookNames
}

// persistIncidents writes each fully assembled incident. A failure on one incident is logged
// and skipped so the rest still persists; the tick's findings remain the source of truth.
func (s *JobRootCauseService) persistIncidents(ctx context.Context, incidents []*scheduler.SLABreachIncidentWithImpacts) {
	for _, incident := range incidents {
		if err := s.incidentRepo.UpsertIncident(ctx, incident.Incident, incident.Impacts); err != nil {
			s.l.Error("failed to persist SLA breach incident",
				"project", incident.Incident.Key.ProjectName.String(),
				"job", incident.Incident.Key.JobName.String(),
				"error", err)
		}
	}
}

// sendRootCauseAlerts routes one alert per active reason: platform-attributed reasons go to
// their configured team, everything else to the root-cause project's owner. An empty team
// (unset config or no ALERTMANAGER_TEAM on the project) disables that route. Repetition is
// suppressed downstream by the alert_logs deduplication for the rootcause_breach type.
func (s *JobRootCauseService) sendRootCauseAlerts(ctx context.Context, incidents []*scheduler.SLABreachIncidentWithImpacts) {
	if s.rootCauseNotifier == nil {
		return
	}
	for _, incident := range incidents {
		for _, reason := range incident.Incident.ActiveReasons() {
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
				}
				// scheduler-attributed alerts drop the per-run identity so the alert_logs
				// deduplication collapses every backed-up run into one platform alert
				if reason.Reason == scheduler.TierSchedulerDelay {
					alert.RootCauseJob = ""
					alert.RootCauseScheduledAt = nil
				}
				return alert
			}())
		}
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

// impactedJobNames renders the blast radius as project-qualified job names for the payload.
func impactedJobNames(impacts []scheduler.ImpactedJob) []string {
	names := make([]string, 0, len(impacts))
	for _, impacted := range impacts {
		names = append(names, impacted.ProjectName.String()+"/"+impacted.JobName.String())
	}
	return names
}
