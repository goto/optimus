package service

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/goto/salt/log"

	"github.com/goto/optimus/core/scheduler"
	"github.com/goto/optimus/core/tenant"
	"github.com/goto/optimus/ext/notify/alertmanager"
	"github.com/goto/optimus/internal/compiler"
	"github.com/goto/optimus/internal/errors"
	"github.com/goto/optimus/internal/telemetry"
)

const (
	NotificationSchemeSlack     = "slack"
	NotificationSchemePagerDuty = "pagerduty"
)

type Notifier interface {
	io.Closer
	Notify(ctx context.Context, attr scheduler.NotifyAttrs) error
}

type Webhook interface {
	io.Closer
	Trigger(attr scheduler.WebhookAttrs)
}

type AlertManager interface {
	io.Closer
	Relay(attr *alertmanager.AlertEvent)
}

type EventsService struct {
	notifyChannels map[string]Notifier
	webhookChannel Webhook
	alertManager   AlertManager
	compiler       TemplateCompiler
	jobRepo        JobRepository
	tenantService  TenantService
	l              log.Logger
}

func GetReplayEventObj(tnnt tenant.Tenant, jobName string, stateChangeTo scheduler.ReplayState, replayID string) *alertmanager.AlertEvent {
	event := &alertmanager.AlertEvent{
		JobName:   jobName,
		Tenant:    tnnt,
		EventType: stateChangeTo.String(),
		TemplateContext: map[string]string{
			"replay_id": replayID,
			"project":   tnnt.ProjectName().String(),
			"namespace": tnnt.NamespaceName().String(),
			"job_name":  jobName,
			"event":     stateChangeTo.String(),
		},
		Template: alertmanager.ReplayTemplate,
	}

	switch stateChangeTo {
	case scheduler.ReplayStateFailed, scheduler.ReplayStateCancelled:
		event.TemplateContext["is_alert"] = alertmanager.MsgTypeAlert
	}
	return event
}

func GetJobRunEventObj(tnnt tenant.Tenant, schedulerHost, jobName, owner string, event *scheduler.Event) *alertmanager.AlertEvent {
	alertEvent := alertmanager.AlertEvent{
		JobName:       jobName,
		SchedulerHost: schedulerHost,
		Tenant:        tnnt,
		EventType:     event.Type.String(),
		TemplateContext: map[string]string{
			"is_alert":     alertmanager.MsgTypeAlert,
			"project":      tnnt.ProjectName().String(),
			"namespace":    tnnt.NamespaceName().String(),
			"job_name":     jobName,
			"owner":        owner,
			"scheduled_at": event.JobScheduledAt.Format(alertmanager.RadarTimeFormat),
		},
	}
	switch event.Type {
	case scheduler.JobFailureEvent:
		alertEvent.Template = alertmanager.FailureAlertTemplate
		alertEvent.TemplateContext["task_id"] = event.OperatorName
	case scheduler.SLAMissEvent:
		alertEvent.Template = alertmanager.SlaAlertTemplate
		alertEvent.TemplateContext["state"] = event.Status.String()
	}
	return &alertEvent
}

func (e *EventsService) Relay(ctx context.Context, event *scheduler.Event) error {
	jobDetails, err := e.jobRepo.GetJobDetails(ctx, event.Tenant.ProjectName(), event.JobName)
	if err != nil {
		e.l.Error("error getting detail for job [%s]: %s", event.JobName, err)
		return err
	}
	tenantWithDetails, err := e.tenantService.GetDetails(ctx, event.Tenant)
	if err != nil {
		return err
	}
	schedulerHost, err := tenantWithDetails.GetConfig(tenant.ProjectSchedulerHost)
	if err != nil {
		return err
	}
	if event.Type == scheduler.JobFailureEvent || event.Type == scheduler.SLAMissEvent {
		e.alertManager.Relay(GetJobRunEventObj(tenantWithDetails.ToTenant(), schedulerHost, jobDetails.Name.String(), jobDetails.JobMetadata.Owner, event))
	}
	return nil
}

func (e *EventsService) Webhook(ctx context.Context, event *scheduler.Event) error {
	jobDetails, err := e.jobRepo.GetJobDetails(ctx, event.Tenant.ProjectName(), event.JobName)
	if err != nil {
		e.l.Error("error getting detail for job [%s]: %s", event.JobName, err)
		return err
	}
	multierror := errors.NewMultiError("ErrorsInNotifyPush")
	var secretMap tenant.SecretMap
	var plainTextSecretsList []*tenant.PlainTextSecret

	for _, webhook := range jobDetails.Webhook {
		if event.Type.IsOfType(webhook.On) {
			if plainTextSecretsList == nil {
				plainTextSecretsList, err = e.tenantService.GetSecrets(ctx, event.Tenant)
				if err != nil {
					e.l.Error("error getting secrets for project [%s] namespace [%s]: %s",
						event.Tenant.ProjectName().String(), event.Tenant.NamespaceName().String(), err)
					multierror.Append(err)
					continue
				}
				secretMap = tenant.PlainTextSecrets(plainTextSecretsList).ToSecretMap()
			}
			headerContext := compiler.PrepareContext(compiler.From(secretMap).WithName(contextSecret))

			for _, endpoint := range webhook.Endpoints {
				webhookAttr := scheduler.WebhookAttrs{
					Owner:    jobDetails.JobMetadata.Owner,
					JobEvent: event,
					Meta: &scheduler.JobRunMeta{
						Labels:         jobDetails.JobMetadata.Labels,
						DestinationURN: jobDetails.Job.Destination,
					},
					Route: endpoint.URL,
				}
				if len(endpoint.Headers) > 0 {
					compiledHeaders, err := e.compiler.Compile(endpoint.Headers, headerContext)
					if err != nil {
						multierror.Append(fmt.Errorf("error compiling template with config: %w", err))
						continue
					}
					webhookAttr.Headers = compiledHeaders
				}
				e.webhookChannel.Trigger(webhookAttr)
			}
		}
	}
	return multierror.ToErr()
}

func (e *EventsService) Push(ctx context.Context, event *scheduler.Event) error {
	jobDetails, err := e.jobRepo.GetJobDetails(ctx, event.Tenant.ProjectName(), event.JobName)
	if err != nil {
		e.l.Error("error getting detail for job [%s]: %s", event.JobName, err)
		return err
	}
	notificationConfig := jobDetails.Alerts
	multierror := errors.NewMultiError("ErrorsInNotifyPush")
	var secretMap tenant.SecretMap
	var plainTextSecretsList []*tenant.PlainTextSecret

	for _, notify := range notificationConfig {
		if event.Type.IsOfType(notify.On) {
			for _, channel := range notify.Channels {
				chanParts := strings.Split(channel, "://")
				scheme := chanParts[0]
				route := chanParts[1]

				e.l.Debug("notification event for job: %s , event: %+v", event.JobName, event)
				if plainTextSecretsList == nil {
					plainTextSecretsList, err = e.tenantService.GetSecrets(ctx, event.Tenant)
					if err != nil {
						e.l.Error("error getting secrets for project [%s] namespace [%s]: %s",
							event.Tenant.ProjectName().String(), event.Tenant.NamespaceName().String(), err)
						multierror.Append(err)
						continue
					}
					secretMap = tenant.PlainTextSecrets(plainTextSecretsList).ToSecretMap()
				}

				var secretName string
				switch scheme {
				case NotificationSchemeSlack:
					secretName = tenant.SecretNotifySlack
				case NotificationSchemePagerDuty:
					secretName = strings.ReplaceAll(route, "#", "notify_")
				}
				secret, err := secretMap.Get(secretName)
				if err != nil {
					return err
				}

				if notifyChannel, ok := e.notifyChannels[scheme]; ok {
					if currErr := notifyChannel.Notify(ctx, scheduler.NotifyAttrs{
						Owner:    jobDetails.JobMetadata.Owner,
						JobEvent: event,
						Secret:   secret,
						Route:    route,
					}); currErr != nil {
						e.l.Error("Error: No notification event for job current error: %s", currErr)
						multierror.Append(fmt.Errorf("notifyChannel.Notify: %s: %w", channel, currErr))
					}
				}
			}
			telemetry.NewCounter("jobrun_alerts_total", map[string]string{
				"project":   event.Tenant.ProjectName().String(),
				"namespace": event.Tenant.NamespaceName().String(),
				"type":      event.Type.String(),
			}).Inc()
		}
	}
	return multierror.ToErr()
}

func (e *EventsService) Close() error {
	me := errors.NewMultiError("ErrorsInNotifyClose")
	for _, notify := range e.notifyChannels {
		if cerr := notify.Close(); cerr != nil {
			e.l.Error("error closing notificication channel: %s", cerr)
			me.Append(cerr)
		}
	}
	return me.ToErr()
}

func NewEventsService(l log.Logger, jobRepo JobRepository, tenantService TenantService, notifyChan map[string]Notifier, webhookNotifier Webhook, compiler TemplateCompiler, alertsHandler AlertManager) *EventsService {
	return &EventsService{
		l:              l,
		jobRepo:        jobRepo,
		tenantService:  tenantService,
		notifyChannels: notifyChan,
		webhookChannel: webhookNotifier,
		compiler:       compiler,
		alertManager:   alertsHandler,
	}
}
