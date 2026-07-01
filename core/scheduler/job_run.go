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
	WindowStart   *time.Time
	WindowEnd     *time.Time
	SLADefinition int64

	Monitoring map[string]any
}

type JobRunMeta struct {
	Labels         map[string]string
	DestinationURN resource.URN
}

func (j *JobRun) HasSLABreached() bool {
	if j.EndTime != nil {
		return j.EndTime.After(j.ScheduledAt.Add(time.Second * time.Duration(j.SLADefinition)))
	}
	return time.Now().After(j.ScheduledAt.Add(time.Second * time.Duration(j.SLADefinition)))
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

	JobWithDetails *JobWithDetails

	AlertManager AlertManagerConfig
}

type ReplayNotificationAttrs struct {
	JobName  string
	ReplayID string
	Tenant   tenant.Tenant
	JobURN   string
	State    ReplayState

	JobWithDetails *JobWithDetails

	AlertManager AlertManagerConfig
}

type OperatorSLAAlertAttrs struct {
	Team               string
	Project            string
	Namespace          string
	JobName            string
	OperatorName       string
	OperatorType       string
	Message            string
	Severity           string
	ScheduledAt        time.Time
	StartTime          time.Time
	ExpectedSLAEndTime time.Time
	CurrentState       State

	AlertManager AlertManagerConfig
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

type UpstreamAttrs struct {
	JobName       string
	RelativeLevel int
	Status        string
}

// SLABreachTarget is a single SLA-bearing (target) job that may breach, along
// with the upstream cause jobs owned by the alerted team.
type SLABreachTarget struct {
	JobName string
	Causes  []UpstreamAttrs
}

// SLABreachGroup groups targets that share the same SLA target (label group),
// carrying its own severity so different SLA buckets can be distinguished within
// a single team's message.
type SLABreachGroup struct {
	Name     string
	Severity string
	Targets  []SLABreachTarget
}

// SLABreachProject groups the breaching targets by the target job's project.
type SLABreachProject struct {
	Name   string
	Groups []SLABreachGroup
}

// PotentialSLABreachAttrs is a single consolidated alert for one team. It is
// routed to the team owning the root-cause (upstream) jobs, and its body is
// organized as project -> SLA group (with severity) -> target -> causes.
type PotentialSLABreachAttrs struct {
	TeamName string
	Projects []SLABreachProject
}

const (
	MetricNotificationQueue         = "notification_queue_total"
	MetricNotificationWorkerBatch   = "notification_worker_batch_total"
	MetricNotificationWorkerSendErr = "notification_worker_send_err_total"
	MetricNotificationSend          = "notification_worker_send_total"
)

// AlertManagerConfig holds the configuration for the AlertManager endpoint.
// we can add more fields in the future if needed, such as dashboard url or console url
type AlertManagerConfig struct {
	Endpoint string
}

type JobRunIdentifier struct {
	JobName     JobName
	ScheduledAt time.Time
}
