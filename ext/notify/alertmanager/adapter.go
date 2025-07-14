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

	replayTemplate              = "optimus-job-replay"
	optimusChangeTemplate       = "optimus-change"
	externalTables              = "external-tables"
	failureAlertTemplate        = "optimus-job-failure"
	slaAlertTemplate            = "optimus-job-sla-miss"
	successNotificationTemplate = "optimus-job-success"

	ReplayLifeCycle ReplayEventType = "replay-lifecycle"

	InfoSeverity     = "INFO"
	WarningSeverity  = "WARNING"
	CriticalSeverity = "CRITICAL"
	DefaultSeverity  = WarningSeverity
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

func handleSpecBasedAlerts(jobDetails *scheduler.JobWithDetails, eventType string, alertPayload *AlertPayload) {
	var severity string
	for _, notify := range jobDetails.Alerts {
		if strings.EqualFold(eventType, notify.On.String()) {
			severity = getSeverity(notify.Severity)
			if len(notify.Team) > 0 {
				alertPayload.Labels["team"] = notify.Team
			} else {
				alertPayload.Labels["team"] = jobDetails.Job.Tenant.NamespaceName().String()
			}
			alertPayload.Labels["severity"] = severity
			if severity == CriticalSeverity {
				alertPayload.Labels["environment"] = "production"
			}
			return
		}
	}
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
	templateContext := map[string]string{
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
		template = failureAlertTemplate
		templateContext["task_id"] = e.JobEvent.OperatorName
	case scheduler.SLAMissEvent:
		template = slaAlertTemplate
		templateContext["state"] = e.JobEvent.Status.String()
	case scheduler.JobSuccessEvent:
		template = successNotificationTemplate
		templateContext["state"] = e.JobEvent.Status.String()
	}
	alertPayload := &AlertPayload{
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
	handleSpecBasedAlerts(e.JobWithDetails, e.JobEvent.Type.String(), alertPayload)
	a.relay(alertPayload)
}

func (a *AlertManager) SendJobEvent(attr *job.AlertAttrs) {
	projectName := attr.Tenant.ProjectName().String()
	jobName := attr.Name.String()
	consoleLink := a.getJobConsoleLink(projectName, jobName)
	a.relay(&AlertPayload{
		Project: projectName,
		LogTag:  attr.URN,
		Data: map[string]string{
			"project":      projectName,
			"namespace":    attr.Tenant.NamespaceName().String(),
			"job_name":     jobName,
			"entity_type":  "Job",
			"change_type":  attr.ChangeType.String(),
			"console_link": consoleLink,
		},
		Template: optimusChangeTemplate,
		Labels: map[string]string{
			"identifier": attr.URN,
			"event_type": strings.ToLower(attr.ChangeType.String()),
		},
		Endpoint: utils.GetFirstNonEmpty(attr.AlertManagerEndpoint, a.endpoint),
	})
}

func (a *AlertManager) SendReplayEvent(attr *scheduler.ReplayNotificationAttrs) {
	projectName := attr.Tenant.ProjectName().String()
	alertPayload := AlertPayload{
		Project: projectName,
		LogTag:  attr.JobURN,
		Data: map[string]string{
			"job_name":     attr.JobName,
			"project":      projectName,
			"namespace":    attr.Tenant.NamespaceName().String(),
			"state":        attr.State.String(),
			"replay_id":    attr.ReplayID,
			"console_link": a.getJobConsoleLink(projectName, attr.JobName),
		},
		Template: replayTemplate,
		Labels: map[string]string{
			"identifier": attr.JobURN,
			"event_type": strings.ToLower(ReplayLifeCycle.String()),
		},
		Endpoint: utils.GetFirstNonEmpty(attr.AlertManager.Endpoint, a.endpoint),
	}
	handleSpecBasedAlerts(attr.JobWithDetails, ReplayLifeCycle.String(), &alertPayload)
	a.relay(&alertPayload)
}

func (a *AlertManager) SendResourceEvent(attr *resource.AlertAttrs) {
	projectName := attr.Tenant.ProjectName().String()
	resourceName := attr.Name.String()

	a.relay(&AlertPayload{
		Project: projectName,
		LogTag:  attr.URN,
		Data: map[string]string{
			"project":      projectName,
			"namespace":    attr.Tenant.NamespaceName().String(),
			"job_name":     resourceName,
			"entity_type":  "Resource",
			"change_type":  attr.EventType.String(),
			"console_link": a.getJobConsoleLink(projectName, resourceName),
		},
		Template: optimusChangeTemplate,
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
		Data: map[string]string{
			"table_name": attr.URN,
			"event_type": attr.EventType,
			"message":    attr.Message,
		},
		Template: externalTables,
		Labels: map[string]string{
			"team":     attr.Tenant.NamespaceName().String(),
			"severity": "WARNING",
		},
		Endpoint: utils.GetFirstNonEmpty(attr.AlertManagerEndpoint, a.endpoint),
	})
}
