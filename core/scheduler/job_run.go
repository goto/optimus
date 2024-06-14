package scheduler

import (
	"time"

	"github.com/google/uuid"

	"github.com/goto/optimus/core/resource"
	"github.com/goto/optimus/core/tenant"
	"github.com/goto/optimus/internal/errors"
)

type JobRunID uuid.UUID

func JobRunIDFromString(runID string) (JobRunID, error) {
	if runID == "" {
		return JobRunID(uuid.Nil), nil
	}

	parsed, err := uuid.Parse(runID)
	if err != nil {
		return JobRunID{}, errors.InvalidArgument(EntityJobRun, "invalid value for job run id "+runID)
	}

	return JobRunID(parsed), nil
}

func (i JobRunID) UUID() uuid.UUID {
	return uuid.UUID(i)
}

func (i JobRunID) IsEmpty() bool {
	return i.UUID() == uuid.Nil
}

type JobRun struct {
	ID uuid.UUID

	JobName       JobName
	Tenant        tenant.Tenant
	State         State
	ScheduledAt   time.Time
	SLAAlert      bool
	StartTime     time.Time
	EndTime       *time.Time
	SLADefinition int64

	Monitoring map[string]any
}

type JobRunMeta struct {
	Labels         map[string]string
	DestinationURN resource.URN
}

func (j *JobRun) HasSLABreached() bool {
	if j.EndTime != nil {
		return j.EndTime.After(j.StartTime.Add(time.Second * time.Duration(j.SLADefinition)))
	}
	return time.Now().After(j.StartTime.Add(time.Second * time.Duration(j.SLADefinition)))
}

type OperatorRun struct {
	ID           uuid.UUID
	Name         string
	JobRunID     uuid.UUID
	OperatorType OperatorType
	Status       State
	StartTime    time.Time
	EndTime      *time.Time
}

type AlertAttrs struct {
	Owner         string
	JobURN        string
	Title         string
	SchedulerHost string
	Status        EventStatus
	JobEvent      *Event
}

type ReplayNotificationAttrs struct {
	JobName   string
	ReplayID  string
	Tenant    tenant.Tenant
	JobURN    string
	EventType ReplayEventType
}

type WebhookAttrs struct {
	Owner    string
	JobEvent *Event
	Meta     *JobRunMeta
	Route    string
	Headers  map[string]string
}

type NotifyAttrs struct {
	Owner    string
	JobEvent *Event
	Route    string
	Secret   string
}

const (
	MetricNotificationQueue         = "notification_queue_total"
	MetricNotificationWorkerBatch   = "notification_worker_batch_total"
	MetricNotificationWorkerSendErr = "notification_worker_send_err_total"
	MetricNotificationSend          = "notification_worker_send_total"
)
