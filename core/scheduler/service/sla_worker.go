package service

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/goto/salt/log"

	"github.com/goto/optimus/config"
	"github.com/goto/optimus/core/scheduler"
	"github.com/goto/optimus/core/tenant"
)

type JobRepo interface {
	GetJob(ctx context.Context, projectName tenant.ProjectName, jobName scheduler.JobName) (*scheduler.Job, error)
	GetJobDetails(ctx context.Context, projectName tenant.ProjectName, jobName scheduler.JobName) (*scheduler.JobWithDetails, error)
}

type OperatorSLARepo interface {
	GetExpiredSLAsForProcessing(ctx context.Context, signature string, processingDuration time.Duration) ([]*scheduler.OperatorsSLA, error)
	RemoveProcessedSLA(ctx context.Context, slaID uuid.UUID) error
}

type RunScheduler interface {
	GetOperatorInstance(ctx context.Context, tnnt tenant.Tenant, jobName scheduler.JobName, jobRunID, operatorName string) (*scheduler.OperatorRunInstance, error)
}

// jobLabel is one key:value pair parsed from alerting.operator_sla_miss.job_labels
type jobLabel struct {
	key   string
	value string
}

type SLAWorker struct {
	alertManager        AlertManager
	operatorSLARepo     OperatorSLARepo
	jobRepo             JobRepo
	jobRunRepo          JobRunRepository
	operatorRunRepo     OperatorRunRepository
	tenantService       TenantService
	scheduler           RunScheduler
	logger              log.Logger
	warningOnlyProjects map[string]struct{}
	jobLabelGroups      [][]jobLabel
	activeWindowLoc     *time.Location
	activeWindowStart   int
	activeWindowEnd     int
}

func NewSLAWorker(logger log.Logger, alertManager AlertManager, operatorSLARepo OperatorSLARepo, jobRepo JobRepo,
	jobRunRepo JobRunRepository, operatorRunRepo OperatorRunRepository, tenantService TenantService, scheduler RunScheduler,
	operatorSLAMissConfig config.OperatorSLAMissConfig,
) *SLAWorker {
	warningOnlyProjects := map[string]struct{}{}
	for _, projectName := range operatorSLAMissConfig.WarningOnlyProjects {
		warningOnlyProjects[projectName] = struct{}{}
	}

	loc, err := time.LoadLocation(operatorSLAMissConfig.ActiveWindowTimezone)
	if err != nil {
		if operatorSLAMissConfig.ActiveWindowTimezone != "" {
			logger.Warn("invalid active_window_timezone %s in alerting.operator_sla_miss, falling back to UTC", operatorSLAMissConfig.ActiveWindowTimezone)
		}
		loc = time.UTC
	}
	if end := operatorSLAMissConfig.ActiveWindowEndHour; end != 0 && end < operatorSLAMissConfig.ActiveWindowStartHour {
		logger.Warn("active window end hour %d is before start hour %d in alerting.operator_sla_miss, the window never matches (cross-midnight windows are not supported)", end, operatorSLAMissConfig.ActiveWindowStartHour)
	}

	return &SLAWorker{
		logger:              logger,
		alertManager:        alertManager,
		operatorSLARepo:     operatorSLARepo,
		jobRepo:             jobRepo,
		jobRunRepo:          jobRunRepo,
		operatorRunRepo:     operatorRunRepo,
		tenantService:       tenantService,
		scheduler:           scheduler,
		warningOnlyProjects: warningOnlyProjects,
		jobLabelGroups:      parseJobLabelGroups(logger, operatorSLAMissConfig.JobLabels),
		activeWindowLoc:     loc,
		activeWindowStart:   operatorSLAMissConfig.ActiveWindowStartHour,
		activeWindowEnd:     operatorSLAMissConfig.ActiveWindowEndHour,
	}
}

// parseJobLabelGroups reads job_labels each entry is a comma-separated AND of "key:value" pairs, entries are OR.
// Entries with no valid pair are dropped with a warning.
func parseJobLabelGroups(logger log.Logger, raw []string) [][]jobLabel {
	var groups [][]jobLabel
	for _, entry := range raw {
		var group []jobLabel
		for _, pair := range strings.Split(entry, ",") {
			key, value, found := strings.Cut(strings.TrimSpace(pair), ":")
			if !found || key == "" || value == "" {
				logger.Warn("ignoring malformed job label %q in alerting.operator_sla_miss.job_labels, expected key:value", pair)
				continue
			}
			group = append(group, jobLabel{key: key, value: value})
		}
		if len(group) > 0 {
			groups = append(groups, group)
		}
	}
	return groups
}

func (w *SLAWorker) ScheduleSLAHandling(ctx context.Context, interval, processDuration time.Duration) {
	ticker := time.NewTicker(interval)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				signature := uuid.New().String()
				w.process(ctx, signature, processDuration)
			}
		}
	}()
}

func getAlertAttributes(teanatWithDetails *tenant.WithDetails, team, severity, msg string, currentState scheduler.State, slaObj *scheduler.OperatorsSLA) *scheduler.OperatorSLAAlertAttrs {
	alertAttr := scheduler.OperatorSLAAlertAttrs{
		Team:               team,
		Project:            teanatWithDetails.Project().Name().String(),
		Namespace:          teanatWithDetails.Namespace().Name().String(),
		JobName:            slaObj.JobName.String(),
		OperatorType:       slaObj.OperatorType.String(),
		OperatorName:       slaObj.OperatorName,
		Message:            msg,
		Severity:           severity,
		ScheduledAt:        slaObj.ScheduledAt,
		StartTime:          slaObj.OperatorStartTime,
		ExpectedSLAEndTime: slaObj.SLATime,
		CurrentState:       currentState,
		DagRunID:           slaObj.RunID,

		AlertManager: getAlertManagerProjectConfig(teanatWithDetails),
	}
	return &alertAttr
}

func (w *SLAWorker) SendOperatorSLAEvent(ctx context.Context, job *scheduler.Job, slaObj *scheduler.OperatorsSLA) error {
	jobRun, err := w.jobRunRepo.GetByScheduledAt(ctx, job.Tenant, job.Name, slaObj.ScheduledAt)
	if err != nil {
		return err
	}

	tenantWithDetails, err := w.tenantService.GetDetails(ctx, job.Tenant)
	if err != nil {
		return err
	}

	operatorRun, err := w.operatorRunRepo.GetOperatorRun(ctx, slaObj.OperatorName, slaObj.OperatorType, jobRun.ID)
	if err != nil {
		return err
	}

	alertConfig := job.GetOperatorAlertConfigByName(slaObj.OperatorType, slaObj.OperatorName)
	if alertConfig == nil {
		w.logger.Warn("no alert config found for job %s operator %s", job.Name, slaObj.OperatorName)
		return nil
	}

	var alertTeam string
	var alertSeverity scheduler.Severity
	var alertMsg string
	alertTeam = job.Tenant.NamespaceName().String()
	alertSeverity = scheduler.Warning
	if alertConfig.Team != "" {
		alertTeam = alertConfig.Team
	}
	slaAlertConfig := alertConfig.GetSLAOperatorAlertConfigByTag(slaObj.AlertTag)
	if slaAlertConfig != nil {
		alertSeverity = slaAlertConfig.Severity
		if slaAlertConfig.Team != "" {
			alertTeam = slaAlertConfig.Team
		}
		alertMsg = fmt.Sprintf("%s: %s", slaObj.OperatorType.String(), slaObj.OperatorName)
		if slaAlertConfig.AutoThreshold {
			autoThresholdDuration := slaObj.SLATime.Sub(slaObj.OperatorStartTime)
			alertMsg += fmt.Sprintf(" Automatic Trend based SLA alert, Determined Duration Threshold: %s", autoThresholdDuration.String())
		} else {
			alertMsg += fmt.Sprintf(" Configured SLA Alert Duration: %s", slaAlertConfig.DurationThreshold.String())
		}
	}

	alertAttr := getAlertAttributes(tenantWithDetails, alertTeam, w.cappedSeverity(ctx, job, slaObj, alertSeverity).String(), alertMsg, operatorRun.Status, slaObj)
	w.alertManager.SendOperatorSLAEvent(alertAttr)
	return nil
}

// cappedSeverity downgrades CRITICAL to WARNING for projects listed in alerting.operator_sla_miss,
// when the alert's SLA deadline falls inside the active window and the job matches the label filter.
func (w *SLAWorker) cappedSeverity(ctx context.Context, job *scheduler.Job, slaObj *scheduler.OperatorsSLA, severity scheduler.Severity) scheduler.Severity {
	if severity != scheduler.Critical {
		return severity
	}
	projectName := job.Tenant.ProjectName().String()
	if _, ok := w.warningOnlyProjects[projectName]; !ok {
		return severity
	}
	if !w.withinActiveWindow(slaObj.SLATime) {
		w.logger.Debug("operator SLA miss cap skipped, SLA deadline outside active window, project: %s, job: %s, sla_time: %s", projectName, slaObj.JobName, slaObj.SLATime)
		return severity
	}
	if !w.matchesJobLabels(ctx, job) {
		w.logger.Debug("operator SLA miss cap skipped, job labels do not match the filter, project: %s, job: %s", projectName, slaObj.JobName)
		return severity
	}
	w.logger.Info("operator SLA miss alert severity capped to WARNING by project config, project: %s", projectName)
	return scheduler.Warning
}

// withinActiveWindow reports whether the SLA deadline's local hour falls inside the
// configured daily window (both ends inclusive). An end hour of 0 disables windowing.
func (w *SLAWorker) withinActiveWindow(slaTime time.Time) bool {
	if w.activeWindowEnd == 0 {
		return true
	}
	hour := slaTime.In(w.activeWindowLoc).Hour()
	return hour >= w.activeWindowStart && hour <= w.activeWindowEnd
}

// matchesJobLabels reports whether the job fully matches any DNF label group
// (labels within a group are ANDed, groups are ORed). An empty filter matches
// everything; a lookup failure fails open (no cap).
func (w *SLAWorker) matchesJobLabels(ctx context.Context, job *scheduler.Job) bool {
	if len(w.jobLabelGroups) == 0 {
		return true
	}
	details, err := w.jobRepo.GetJobDetails(ctx, job.Tenant.ProjectName(), job.Name)
	if err != nil {
		w.logger.Error("failed to fetch job details for label filter, keeping job-spec severity, job: %s, error: %v", job.Name.String(), err)
		return false
	}
	if details == nil || details.JobMetadata == nil {
		return false
	}
	labels := details.JobMetadata.Labels
	for _, group := range w.jobLabelGroups {
		matched := true
		for _, l := range group {
			if labels[l.key] != l.value {
				matched = false
				break
			}
		}
		if matched {
			return true
		}
	}
	return false
}

func (w *SLAWorker) process(ctx context.Context, signature string, processDuration time.Duration) {
	expiredSLAs, err := w.operatorSLARepo.GetExpiredSLAsForProcessing(ctx, signature, processDuration)
	if err != nil {
		w.logger.Error("[processSLABreach] failed to fetch expired SLAs: %v", err)
		return
	}
	for _, slaObj := range expiredSLAs {
		if err := w.processSLABreach(ctx, slaObj); err == nil {
			if err := w.operatorSLARepo.RemoveProcessedSLA(ctx, slaObj.ID); err != nil {
				w.logger.Error("[processSLABreach] failed to update SLA processed for %s: %v", slaObj, err)
			}
		}
	}
}

func (w *SLAWorker) processSLABreach(ctx context.Context, slaObj *scheduler.OperatorsSLA) error {
	job, err := w.jobRepo.GetJob(ctx, slaObj.ProjectName, slaObj.JobName)
	if err != nil {
		w.logger.Error("[processSLABreach] failed to fetch job %s for SLA %s: %v", slaObj.JobName, slaObj.ID, err)
		return err
	}

	taskInstance, err := w.scheduler.GetOperatorInstance(ctx, job.Tenant, job.Name, slaObj.RunID, slaObj.OperatorName)
	if err != nil {
		w.logger.Error("[processSLABreach] failed to fetch operator instance from scheduler, proceeding to alert the user about SLA breach err: %v, %s", err, slaObj)
	}
	if taskInstance != nil {
		if taskInstance.IsTerminated() && taskInstance.EndTime != nil && taskInstance.EndTime.Before(slaObj.SLATime) {
			w.logger.Info("[processSLABreach] Operator Finished Before SLA time, removing it from SLA records table, %s", slaObj)
			return nil
		}
	}
	if err := w.SendOperatorSLAEvent(ctx, job, slaObj); err != nil {
		w.logger.Error("[processSLABreach] failed to notify SLA for %s: %v", slaObj, err)
		return err
	}
	return nil
}
