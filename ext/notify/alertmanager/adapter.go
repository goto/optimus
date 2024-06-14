package alertmanager

import (
	"fmt"
	"net/url"
	"regexp"
	"strings"

	"github.com/goto/optimus/core/job"
	"github.com/goto/optimus/core/resource"
	"github.com/goto/optimus/core/scheduler"
)

const (
	radarTimeFormat = "2006/01/02 15:04:05"

	replayTemplate              = "optimus-job-replay"
	optimusChangeTemplate       = "optimus-change"
	failureAlertTemplate        = "optimus-job-failure"
	slaAlertTemplate            = "optimus-job-sla-miss"
	successNotificationTemplate = "optimus-job-success"
)

func (a *AlertManager) SendJobRunEvent(e *scheduler.AlertAttrs) {
	dashURL, _ := url.Parse(a.dashboard)
	q := dashURL.Query()
	q.Set("var-project", e.JobEvent.Tenant.ProjectName().String())
	q.Set("var-namespace", e.JobEvent.Tenant.NamespaceName().String())
	q.Set("var-job", e.JobEvent.JobName.String())
	q.Set("var-schedule_time", e.JobEvent.JobScheduledAt.Format(radarTimeFormat))
	dashURL.RawQuery = q.Encode()
	templateContext := map[string]string{
		"project":      e.JobEvent.Tenant.ProjectName().String(),
		"namespace":    e.JobEvent.Tenant.NamespaceName().String(),
		"job_name":     e.JobEvent.JobName.String(),
		"owner":        e.Owner,
		"scheduled_at": e.JobEvent.JobScheduledAt.Format(radarTimeFormat),
		"console_link": fmt.Sprintf("%s/%s/%s", a.dataConsole, "optimus", e.JobEvent.JobName),
		"dashboard":    dashURL.String(),
	}

	httpRegex := regexp.MustCompile(`^(http|https)://`)
	if httpRegex.MatchString(e.SchedulerHost) {
		templateContext["airflow_logs"] = fmt.Sprintf("%s/dags/%s/grid", e.SchedulerHost, e.JobEvent.JobName)
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

	a.relay(&AlertPayload{
		Project:  e.JobEvent.Tenant.ProjectName().String(),
		LogTag:   e.JobEvent.JobName.String(),
		Data:     templateContext,
		Template: template,
		Labels: map[string]string{
			"identifier": e.JobURN,
			"event_type": e.JobEvent.Type.String(),
		},
	})
}

func (a *AlertManager) SendJobEvent(attr *job.AlertAttrs) {
	a.relay(&AlertPayload{
		Project: attr.Tenant.ProjectName().String(),
		LogTag:  attr.Name.String(),
		Data: map[string]string{
			"project":      attr.Tenant.ProjectName().String(),
			"namespace":    attr.Tenant.NamespaceName().String(),
			"job_name":     attr.Name.String(),
			"entity_type":  "Job",
			"change_type":  attr.EventType.String(),
			"console_link": fmt.Sprintf("%s/%s/%s", a.dataConsole, "optimus", attr.Name.String()),
		},
		Template: optimusChangeTemplate,
		Labels: map[string]string{
			"identifier": attr.URN,
			"event_type": strings.ToLower(attr.EventType.String()),
		},
	})
}

func (a *AlertManager) SendReplayEvent(attr *scheduler.ReplayNotificationAttrs) {
	a.relay(&AlertPayload{
		Project: attr.Tenant.ProjectName().String(),
		LogTag:  attr.JobURN,
		Data: map[string]string{
			"job_name":     attr.JobName,
			"project":      attr.Tenant.ProjectName().String(),
			"namespace":    attr.Tenant.NamespaceName().String(),
			"console_link": fmt.Sprintf("%s/%s/%s", a.dataConsole, "optimus", attr.JobName),
		},
		Template: replayTemplate,
		Labels: map[string]string{
			"identifier": attr.JobURN,
			"event_type": strings.ToLower(attr.EventType.String()),
		},
	})
}

func (a *AlertManager) SendResourceEvent(attr *resource.AlertAttrs) {
	a.relay(&AlertPayload{
		Project: attr.Tenant.ProjectName().String(),
		LogTag:  attr.Name.String(),
		Data: map[string]string{
			"project":      attr.Tenant.ProjectName().String(),
			"namespace":    attr.Tenant.NamespaceName().String(),
			"job_name":     attr.Name.String(),
			"entity_type":  "Resource",
			"change_type":  attr.EventType.String(),
			"console_link": fmt.Sprintf("%s/%s/%s", a.dataConsole, "tables", attr.URN),
		},
		Template: optimusChangeTemplate,
		Labels: map[string]string{
			"identifier": attr.URN,
			"event_type": strings.ToLower(attr.EventType.String()),
		},
	})
}
