package alertmanager

import (
	"fmt"
	"net/url"
	"regexp"
	"strings"

	"github.com/goto/optimus/core/job"
	"github.com/goto/optimus/core/resource"
	"github.com/goto/optimus/core/scheduler"
	"github.com/goto/optimus/internal/utils"
)

const (
	radarTimeFormat = "2006/01/02 15:04:05"

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

func getSpecBasedAlerts(jobDetails *scheduler.JobWithDetails, eventType scheduler.JobEventType, alertPayload *AlertPayload) []*AlertPayload {
	var alertPayloads []*AlertPayload
	for _, notify := range jobDetails.Alerts {
		if eventType.IsOfType(notify.On) {
			severity := getSeverity(notify.Severity)
			if len(notify.Team) > 0 {
				alertPayload.Labels[DefaultChannelLabel] = notify.Team
			} else {
				alertPayload.Labels[DefaultChannelLabel] = jobDetails.Job.Tenant.NamespaceName().String()
			}
			alertPayload.Labels[SeverityLabel] = severity
			if severity == CriticalSeverity {
				alertPayload.Labels[EnvironmentLabel] = "production"
			}
			alertPayload.JobWithDetails = jobDetails
			alertPayloads = append(alertPayloads, alertPayload)
		}
	}
	return alertPayloads
}

func (a *AlertManager) SendOperatorSLAEvent(attr *scheduler.OperatorSLAAlertAttrs) {
	alertPayload := &AlertPayload{
		Project: attr.Project,
		LogTag:  attr.OperatorType,
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
		Endpoint: utils.GetFirstNonEmpty(attr.AlertManager.Endpoint, a.endpoint),
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

	var template string
	switch e.JobEvent.Type {
	case scheduler.JobFailureEvent:
		template = OptimusFailureAlertTemplate
		templateContext["task_id"] = e.JobEvent.OperatorName
	case scheduler.SLAMissEvent:
		template = OptimusSLAAlertTemplate
		templateContext["state"] = e.JobEvent.Status.String()
	case scheduler.JobSuccessEvent:
		template = OptimusSuccessNotificationTemplate
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
		Endpoint: utils.GetFirstNonEmpty(e.AlertManager.Endpoint, a.endpoint),
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
		Endpoint: utils.GetFirstNonEmpty(attr.AlertManagerEndpoint, a.endpoint),
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
		Endpoint: utils.GetFirstNonEmpty(attr.AlertManager.Endpoint, a.endpoint),
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
		Endpoint: utils.GetFirstNonEmpty(attr.AlertManagerEndpoint, a.endpoint),
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
		Endpoint: utils.GetFirstNonEmpty(attr.AlertManagerEndpoint, a.endpoint),
	})
}

func (a *AlertManager) SendPotentialSLABreach(attr *scheduler.PotentialSLABreachAttrs) {
	content := map[string][]string{}
	for jobName, upstreamCauses := range attr.JobToUpstreamsCause {
		content[jobName] = []string{}
		for _, cause := range upstreamCauses {
			content[jobName] = append(content[jobName], fmt.Sprintf("- %s (level: %d) (status: %s)", cause.JobName, cause.RelativeLevel, cause.Status))
		}
	}

	severity := attr.Severity
	if severity == "" || (severity != InfoSeverity && severity != WarningSeverity && severity != CriticalSeverity) {
		severity = WarningSeverity
	}

	a.relay(&AlertPayload{
		Project: attr.ProjectName,
		Data: map[string]interface{}{
			"content": content,
		},
		Template: OptimusPotentialSLABreachTemplate,
		Labels: map[string]string{
			DefaultChannelLabel: attr.TeamName,
			SeverityLabel:       severity,
		},
		Endpoint: a.endpoint,
	})
}
