package alertmanager

import (
	"fmt"
	"net/url"
	"regexp"
	"strings"
	"time"

	"github.com/goto/optimus/core/job"
	"github.com/goto/optimus/core/resource"
	"github.com/goto/optimus/core/scheduler"
	"github.com/goto/optimus/internal/utils"
)

const (
	radarTimeFormat = "2006/01/02 15:04:05"

	// impactedJobsPreviewLimit caps how many impacted job names are named inline in
	// the alert body; the rest are only reachable via the impacted_jobs_count.
	impactedJobsPreviewLimit = 3

	AlertTypeJobReplay          = "job_replay"
	AlertTypeChange             = "change"
	AlertTypeExternalTable      = "external_table"
	AlertTypeJobFailure         = "job_failure"
	AlertTypeJobSLAAlert        = "job_sla_miss"
	AlertTypeJobSuccess         = "job_success"
	AlertTypeOperatorSLAMiss    = "operator_sla_miss"
	AlertTypePotentialSLABreach = "potential_sla_breach"

	OptimusReplayTemplate              = "optimus-job-replay"
	OptimusChangeTemplate              = "optimus-change"
	OptimusExternalTablesTemplate      = "external-tables"
	OptimusFailureAlertTemplate        = "optimus-job-failure"
	OptimusSLAAlertTemplate            = "optimus-job-sla-miss"
	OptimusSuccessNotificationTemplate = "optimus-job-success"
	OptimusOperatorSLAMissTemplate     = "optimus-operator-sla-miss"
	OptimusPotentialSLABreachTemplate  = "optimus-potential-sla-breach"

	InfoSeverity     = "INFO"
	WarningSeverity  = "WARNING"
	CriticalSeverity = "CRITICAL"
	DefaultSeverity  = WarningSeverity

	DefaultChannelLabel = "team"
	SeverityLabel       = "severity"
	EnvironmentLabel    = "environment"
)

func EventTypeToAlertType(e scheduler.JobEventType) string {
	switch e {
	case scheduler.JobFailureEvent:
		return AlertTypeJobFailure
	case scheduler.JobSuccessEvent:
		return AlertTypeJobSuccess
	case scheduler.SLAMissEvent:
		return AlertTypeJobSLAAlert
	case scheduler.ReplayEvent:
		return AlertTypeJobReplay
	}

	return ""
}

type ReplayEventType string

func (j ReplayEventType) String() string {
	return string(j)
}

func (a *AlertManager) getJobConsoleLink(project, job string) string {
	return fmt.Sprintf("%s/%s/%s:%s", a.dataConsole, "optimus", project, job)
}

func getSeverity(severity string) string {
	switch strings.ToUpper(severity) {
	case InfoSeverity, WarningSeverity, CriticalSeverity:
		return strings.ToUpper(severity)
	default:
		return DefaultSeverity
	}
}

func getSpecBasedAlerts(jobDetails *scheduler.JobWithDetails, eventType scheduler.JobEventType, basePayload *AlertPayload) []*AlertPayload {
	var alertPayloads []*AlertPayload
	for _, notify := range jobDetails.Alerts {
		if eventType.IsOfType(notify.On) {
			payload := *basePayload
			payload.Labels = make(map[string]string, len(basePayload.Labels))
			for k, v := range basePayload.Labels {
				payload.Labels[k] = v
			}

			severity := getSeverity(notify.Severity)
			if len(notify.Team) > 0 {
				payload.Labels[DefaultChannelLabel] = notify.Team
			} else {
				payload.Labels[DefaultChannelLabel] = jobDetails.Job.Tenant.NamespaceName().String()
			}
			payload.Labels[SeverityLabel] = severity
			if severity == CriticalSeverity {
				payload.Labels[EnvironmentLabel] = "production"
			}
			alertPayloads = append(alertPayloads, &payload)
		}
	}
	return alertPayloads
}

func (a *AlertManager) SendOperatorSLAEvent(attr *scheduler.OperatorSLAAlertAttrs) {
	alertPayload := &AlertPayload{
		Project:           attr.Project,
		JobRunScheduledAt: attr.ScheduledAt,
		LogTag:            attr.OperatorType,
		Data: map[string]interface{}{
			"operator_name":       attr.OperatorName,
			"operator_type":       attr.OperatorType,
			"project":             attr.Project,
			"namespace":           attr.Namespace,
			"Message":             attr.Message,
			"job_name":            attr.JobName,
			"scheduled_at":        attr.ScheduledAt.String(),
			"operator_started_at": attr.StartTime.String(),
			"state":               attr.CurrentState.String(),
		},
		Template: OptimusOperatorSLAMissTemplate,
		Labels: map[string]string{
			DefaultChannelLabel: attr.Team,
			SeverityLabel:       attr.Severity,
		},
		Endpoint:  utils.GetFirstNonEmpty(attr.AlertManager.Endpoint, a.endpoint),
		AlertType: AlertTypeOperatorSLAMiss,
	}
	if attr.Severity == CriticalSeverity {
		alertPayload.Labels[EnvironmentLabel] = "production"
	}

	a.relay(alertPayload)
}

func (a *AlertManager) SendJobRunEvent(e *scheduler.AlertAttrs) {
	projectName := e.JobEvent.Tenant.ProjectName().String()
	jobName := e.JobEvent.JobName.String()
	dashURL, _ := url.Parse(a.dashboard)
	q := dashURL.Query()
	q.Set("var-project", projectName)
	q.Set("var-namespace", e.JobEvent.Tenant.NamespaceName().String())
	q.Set("var-job", jobName)
	q.Set("var-schedule_time", e.JobEvent.JobScheduledAt.Format(radarTimeFormat))
	dashURL.RawQuery = q.Encode()
	templateContext := map[string]interface{}{
		"project":      projectName,
		"namespace":    e.JobEvent.Tenant.NamespaceName().String(),
		"job_name":     jobName,
		"owner":        e.Owner,
		"scheduled_at": e.JobEvent.JobScheduledAt.Format(radarTimeFormat),
		"console_link": a.getJobConsoleLink(projectName, jobName),
		"dashboard":    dashURL.String(),
	}

	httpRegex := regexp.MustCompile(`^(http|https)://`)
	if httpRegex.MatchString(e.SchedulerHost) {
		templateContext["airflow_logs"] = fmt.Sprintf("%s/dags/%s/grid", e.SchedulerHost, jobName)
	}

	var (
		template  string
		alertType string
	)

	switch e.JobEvent.Type {
	case scheduler.JobFailureEvent:
		template = OptimusFailureAlertTemplate
		alertType = AlertTypeJobFailure
		templateContext["task_id"] = e.JobEvent.OperatorName
	case scheduler.SLAMissEvent:
		template = OptimusSLAAlertTemplate
		alertType = AlertTypeJobSLAAlert
		templateContext["state"] = e.JobEvent.Status.String()
	case scheduler.JobSuccessEvent:
		template = OptimusSuccessNotificationTemplate
		alertType = AlertTypeJobSuccess
		templateContext["state"] = e.JobEvent.Status.String()
	}
	baseAlertPayload := &AlertPayload{
		Project:  projectName,
		LogTag:   e.JobURN,
		Data:     templateContext,
		Template: template,
		Labels: map[string]string{
			"identifier": e.JobURN,
			"event_type": e.JobEvent.Type.String(),
		},
		Endpoint:  utils.GetFirstNonEmpty(e.AlertManager.Endpoint, a.endpoint),
		AlertType: alertType,
	}
	alertPayloads := getSpecBasedAlerts(e.JobWithDetails, e.JobEvent.Type, baseAlertPayload)

	for _, alertPayload := range alertPayloads {
		a.relay(alertPayload)
	}
}

func (a *AlertManager) SendJobEvent(attr *job.AlertAttrs) {
	projectName := attr.Tenant.ProjectName().String()
	jobName := attr.Name.String()
	a.relay(&AlertPayload{
		Project: projectName,
		LogTag:  attr.URN,
		Data: map[string]interface{}{
			"project":      projectName,
			"namespace":    attr.Tenant.NamespaceName().String(),
			"job_name":     jobName,
			"entity_type":  "Job",
			"change_type":  attr.ChangeType.String(),
			"console_link": a.getJobConsoleLink(projectName, jobName),
		},
		Template: OptimusChangeTemplate,
		Labels: map[string]string{
			"identifier": attr.URN,
			"event_type": strings.ToLower(attr.ChangeType.String()),
		},
		Endpoint:  utils.GetFirstNonEmpty(attr.AlertManagerEndpoint, a.endpoint),
		AlertType: AlertTypeChange,
	})
}

func (a *AlertManager) SendReplayEvent(attr *scheduler.ReplayNotificationAttrs) {
	projectName := attr.Tenant.ProjectName().String()
	baseAlertPayload := AlertPayload{
		Project: projectName,
		LogTag:  attr.JobURN,
		Data: map[string]interface{}{
			"job_name":     attr.JobName,
			"project":      projectName,
			"namespace":    attr.Tenant.NamespaceName().String(),
			"state":        attr.State.String(),
			"replay_id":    attr.ReplayID,
			"console_link": a.getJobConsoleLink(projectName, attr.JobName),
		},
		Template: OptimusReplayTemplate,
		Labels: map[string]string{
			"identifier": attr.JobURN,
			"event_type": strings.ToLower(scheduler.ReplayEvent.String()),
		},
		Endpoint:  utils.GetFirstNonEmpty(attr.AlertManager.Endpoint, a.endpoint),
		AlertType: AlertTypeJobReplay,
	}
	alertPayloads := getSpecBasedAlerts(attr.JobWithDetails, scheduler.ReplayEvent, &baseAlertPayload)
	for _, alertPayload := range alertPayloads {
		a.relay(alertPayload)
	}
}

func (a *AlertManager) SendResourceEvent(attr *resource.AlertAttrs) {
	projectName := attr.Tenant.ProjectName().String()
	resourceName := attr.Name.String()

	a.relay(&AlertPayload{
		Project: projectName,
		LogTag:  attr.URN,
		Data: map[string]interface{}{
			"project":      projectName,
			"namespace":    attr.Tenant.NamespaceName().String(),
			"job_name":     resourceName,
			"entity_type":  "Resource",
			"change_type":  attr.EventType.String(),
			"console_link": a.getJobConsoleLink(projectName, resourceName),
		},
		Template: OptimusChangeTemplate,
		Labels: map[string]string{
			"identifier": attr.URN,
			"event_type": strings.ToLower(attr.EventType.String()),
		},
		Endpoint:  utils.GetFirstNonEmpty(attr.AlertManagerEndpoint, a.endpoint),
		AlertType: AlertTypeChange,
	})
}

func (a *AlertManager) SendExternalTableEvent(attr *resource.ETAlertAttrs) {
	a.relay(&AlertPayload{
		Project: attr.Tenant.ProjectName().String(),
		LogTag:  attr.EventType + "-" + attr.URN,
		Data: map[string]interface{}{
			"table_name": attr.URN,
			"event_type": attr.EventType,
			"message":    attr.Message,
		},
		Template: OptimusExternalTablesTemplate,
		Labels: map[string]string{
			DefaultChannelLabel: attr.Tenant.NamespaceName().String(),
			SeverityLabel:       WarningSeverity,
		},
		Endpoint:  utils.GetFirstNonEmpty(attr.AlertManagerEndpoint, a.endpoint),
		AlertType: AlertTypeExternalTable,
	})
}

func (a *AlertManager) SendPotentialSLABreach(alert *scheduler.PotentialSLABreachAlert) {
	a.relay(a.buildPotentialSLABreachPayload(alert))
}

// buildPotentialSLABreachPayload flattens one root cause into a payload. Dedup keys
// (root_cause_job, reason, team; scheduled_at when set) must stay top-level strings.
func (a *AlertManager) buildPotentialSLABreachPayload(alert *scheduler.PotentialSLABreachAlert) *AlertPayload {
	severity := getSeverity(alert.Severity)

	data := map[string]interface{}{
		"team":                  alert.Team,
		"project":               alert.Project,
		"root_cause_job":        alert.RootCauseJob,
		"root_cause_project":    alert.RootCauseProject,
		"reason":                string(alert.Reason),
		"status":                string(alert.Status),
		"relative_level":        alert.RelativeLevel,
		"impacted_jobs":         alert.ImpactedJobs,
		"impacted_jobs_count":   len(alert.ImpactedJobs),
		"impacted_jobs_preview": formatJobsPreview(alert.ImpactedJobs, impactedJobsPreviewLimit),
	}
	if alert.RootCauseScheduledAt != nil {
		data["root_cause_scheduled_at"] = alert.RootCauseScheduledAt.UTC().Format(time.RFC3339)
	}
	for key, value := range evidenceFields(alert.Evidence) {
		data[key] = value
	}

	alertPayload := &AlertPayload{
		Project:  alert.Project,
		Data:     data,
		Template: OptimusPotentialSLABreachTemplate,
		Labels: map[string]string{
			DefaultChannelLabel: alert.Team,
			SeverityLabel:       severity,
		},
		Endpoint:  a.endpoint,
		AlertType: AlertTypePotentialSLABreach,
	}

	if severity == CriticalSeverity {
		alertPayload.Labels[EnvironmentLabel] = "production"
	}

	return alertPayload
}

// formatJobsPreview renders up to limit job names as "A, B & C" for an inline summary,
// leaving the full list/count to impacted_jobs/impacted_jobs_count.
func formatJobsPreview(jobs []string, limit int) string {
	if len(jobs) == 0 {
		return ""
	}
	n := len(jobs)
	if n > limit {
		n = limit
	}
	preview := jobs[:n]
	if len(preview) == 1 {
		return preview[0]
	}
	return strings.Join(preview[:len(preview)-1], ", ") + " & " + preview[len(preview)-1]
}

// evidenceFields omits anything unset so the template can test presence rather than
// render zero timestamps.
func evidenceFields(evidence scheduler.RootCauseEvidence) map[string]interface{} {
	fields := map[string]interface{}{}
	if evidence.StartedAt != nil {
		fields["started_at"] = evidence.StartedAt.UTC().Format(time.RFC3339)
	}
	if evidence.ExpectedFinishAt != nil {
		fields["expected_finish_at"] = evidence.ExpectedFinishAt.UTC().Format(time.RFC3339)
	}
	if evidence.LatestSafeStartAt != nil {
		fields["latest_safe_start_at"] = evidence.LatestSafeStartAt.UTC().Format(time.RFC3339)
	}
	if len(evidence.BlockedOnSensors) > 0 {
		fields["blocked_on_sensors"] = evidence.BlockedOnSensors
	}
	if evidence.SourceType != "" {
		fields["source_type"] = evidence.SourceType
	}
	if evidence.InducedDelay > 0 {
		fields["induced_delay"] = evidence.InducedDelay.String()
	}
	return fields
}
