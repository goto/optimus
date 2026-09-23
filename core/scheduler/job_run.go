package scheduler

import (
	"slices"
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

	// UpdatedAt is only populated by GetByScheduledAt today, not every JobRunRepository
	// method -- it exists for the airflow-sync reconciler to compare against an external
	// change's timestamp so a stale write never overwrites fresher data. Don't assume it is
	// set on a JobRun obtained from another method.
	UpdatedAt time.Time

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

	// UpdatedAt is only populated by GetOperatorRun today. See JobRun.UpdatedAt for why.
	UpdatedAt time.Time
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

// PotentialSLABreachAlert is one alert: a single root cause, and the SLA-bearing jobs
// it threatens for one impacted team.
type PotentialSLABreachAlert struct {
	Team    string
	Project string // impacted (at-risk) SLA job's project

	RootCauseJob         string
	RootCauseProject     string // root cause job's own project, may differ from Project
	RootCauseScheduledAt *time.Time
	Reason               RootCauseReason
	Evidence             RootCauseEvidence
	RelativeLevel        int
	Status               SLABreachCause

	Severity     string
	ImpactedJobs []string
}

type SLABreachCause string

const (
	SLABreachCauseNotStarted  SLABreachCause = "NOT_STARTED"
	SLABreachCauseRunningLate SLABreachCause = "RUNNING_LATE"
)

type JobSLAState struct {
	EstimatedDuration *time.Duration
	InferredSLA       *time.Time
}

// RootCauseReason explains why a breaching job is late.
type RootCauseReason string

const (
	ReasonRunningLong     RootCauseReason = "RUNNING_LONG"
	ReasonStartedLate     RootCauseReason = "STARTED_LATE"
	ReasonThirdPartyDelay RootCauseReason = "THIRD_PARTY_DELAY"
	ReasonUpstreamFailed  RootCauseReason = "UPSTREAM_FAILED"
	ReasonUnknown         RootCauseReason = "UNKNOWN"
)

type RootCauseEvidence struct {
	StartedAt         *time.Time
	ExpectedFinishAt  *time.Time
	LatestSafeStartAt *time.Time
	BlockedOnSensors  []string
	// SourceType is the upstream_resolvers type behind a THIRD_PARTY_DELAY, e.g. "dex".
	SourceType string
	// InducedDelay is how much this cause overran its baseline (historical duration,
	// latest safe start, or third-party wait after delay_start_hour). Used to pick the max-delay cause.
	InducedDelay time.Duration
}

type JobState struct {
	JobSLAState
	JobName       JobName
	JobRun        JobRunSummary
	Tenant        tenant.Tenant
	RelativeLevel int
	Status        SLABreachCause
	Reason        RootCauseReason
	Evidence      RootCauseEvidence
}

// TargetBreach is the per-target result returned to the caller for building the
// API response. It is project-qualified to avoid clashes across projects.
type TargetBreach struct {
	TargetProject string
	TargetJobName JobName
	Upstreams     map[JobName]*JobState
}

// SLABreachCombo is a single (project, label-group) unit of work. The batch
// entrypoint evaluates the cross product of projects x label-groups as combos
// and consolidates the results into one alert per team.
type SLABreachCombo struct {
	ProjectName tenant.ProjectName
	JobNames    []JobName
	Labels      map[string]string
	GroupName   string // display name for the SLA group; derived from labels if empty
}

// BreachAlertAggregator groups breaches into one alert per deduplication key, in
// insertion order so repeated evaluations emit alerts in a stable sequence.
type BreachAlertAggregator struct {
	alerts map[string]*PotentialSLABreachAlert
	order  []string
}

func NewBreachAlertAggregator() *BreachAlertAggregator {
	return &BreachAlertAggregator{alerts: map[string]*PotentialSLABreachAlert{}}
}

// Add records that rootCause threatens impactedJob for team. Repeated calls for the
// same key append the job rather than producing a second alert.
// THIRD_PARTY_DELAY drops scheduled_at from the key so every waiter on the same
// sensor collapses to one incident.
func (a *BreachAlertAggregator) Add(team, impactedJob string, rootCause *JobState, project, severity string) {
	scheduledKey := ""
	if t := alertScheduledAt(rootCause); t != nil {
		scheduledKey = t.UTC().Format(time.RFC3339)
	}
	key := team + "/" + rootCause.JobName.String() + "/" + scheduledKey + "/" + string(rootCause.Reason)

	alert, ok := a.alerts[key]
	if !ok {
		alert = &PotentialSLABreachAlert{
			Team:                 team,
			Project:              project,
			RootCauseJob:         rootCause.JobName.String(),
			RootCauseProject:     rootCause.Tenant.ProjectName().String(),
			RootCauseScheduledAt: alertScheduledAt(rootCause),
			Reason:               rootCause.Reason,
			Evidence:             rootCause.Evidence,
			RelativeLevel:        rootCause.RelativeLevel,
			Status:               rootCause.Status,
			Severity:             severity,
		}
		a.alerts[key] = alert
		a.order = append(a.order, key)
	} else if rootCause.Evidence.InducedDelay > alert.Evidence.InducedDelay {
		alert.Evidence = rootCause.Evidence
		alert.RelativeLevel = rootCause.RelativeLevel
		alert.Status = rootCause.Status
	}
	if !slices.Contains(alert.ImpactedJobs, impactedJob) {
		alert.ImpactedJobs = append(alert.ImpactedJobs, impactedJob)
	}
}

// alertScheduledAt is nil for THIRD_PARTY_DELAY: the waiter's schedule is not
// the sensor's identity, and including it would split one DEX delay into many alerts.
func alertScheduledAt(rootCause *JobState) *time.Time {
	if rootCause == nil || rootCause.Reason == ReasonThirdPartyDelay {
		return nil
	}
	if rootCause.JobRun.ScheduledAt.IsZero() {
		return nil
	}
	t := rootCause.JobRun.ScheduledAt
	return &t
}

func (a *BreachAlertAggregator) Build() []*PotentialSLABreachAlert {
	out := make([]*PotentialSLABreachAlert, 0, len(a.order))
	for _, key := range a.order {
		out = append(out, a.alerts[key])
	}
	return out
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

type DamperFactorMethod int

const (
	DamperFactorMethodExponential DamperFactorMethod = iota
	DamperFactorMethodDecrement
	DamperFactorMethodConstant
)

type DamperFactor struct {
	BaseValue float64
	Alpha     float64
	MinValue  float64
	Method    DamperFactorMethod
}

func (d DamperFactor) InitialDamper() float64 {
	switch d.Method {
	case DamperFactorMethodConstant:
		return d.BaseValue
	default:
		if d.BaseValue > 0 {
			return d.BaseValue
		}
		return 1.0
	}
}

func (d DamperFactor) NextDamper(current float64) float64 {
	switch d.Method {
	case DamperFactorMethodDecrement:
		if next := current - d.Alpha; next > d.MinValue {
			return next
		}
		return d.MinValue
	case DamperFactorMethodConstant:
		return d.BaseValue
	default: // DamperFactorMethodExponential
		if next := current * d.Alpha; next > d.MinValue {
			return next
		}
		return d.MinValue
	}
}
