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

type SLABreachCause string

const (
	SLABreachCauseNotStarted  SLABreachCause = "NOT_STARTED"
	SLABreachCauseRunningLate SLABreachCause = "RUNNING_LATE"
)

type JobSLAState struct {
	EstimatedDuration *time.Duration
	InferredSLA       *time.Time
}

type JobState struct {
	JobSLAState
	JobName       JobName
	JobRun        JobRunSummary
	Tenant        tenant.Tenant
	RelativeLevel int
	Status        SLABreachCause
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

// TeamBreachAggregator accumulates breaches into a deterministic, insertion-
// ordered team -> project -> group -> target -> causes structure and builds one
// PotentialSLABreachAttrs per team.
type TeamBreachAggregator struct {
	teams     map[string]*teamAgg
	teamOrder []string
}

type teamAgg struct {
	name         string
	projects     map[string]*projectAgg
	projectOrder []string
}

type projectAgg struct {
	name       string
	groups     map[string]*groupAgg
	groupOrder []string
}

type groupAgg struct {
	name        string
	severity    string
	targets     map[string]*targetAgg
	targetOrder []string
}

type targetAgg struct {
	name       string
	causes     map[string]UpstreamAttrs
	causeOrder []string
}

func NewTeamBreachAggregator() *TeamBreachAggregator {
	return &TeamBreachAggregator{teams: map[string]*teamAgg{}}
}

func (a *TeamBreachAggregator) Add(team, project, group, severity, target string, cause UpstreamAttrs) {
	t, ok := a.teams[team]
	if !ok {
		t = &teamAgg{name: team, projects: map[string]*projectAgg{}}
		a.teams[team] = t
		a.teamOrder = append(a.teamOrder, team)
	}
	p, ok := t.projects[project]
	if !ok {
		p = &projectAgg{name: project, groups: map[string]*groupAgg{}}
		t.projects[project] = p
		t.projectOrder = append(t.projectOrder, project)
	}
	g, ok := p.groups[group]
	if !ok {
		g = &groupAgg{name: group, severity: severity, targets: map[string]*targetAgg{}}
		p.groups[group] = g
		p.groupOrder = append(p.groupOrder, group)
	}
	tg, ok := g.targets[target]
	if !ok {
		tg = &targetAgg{name: target, causes: map[string]UpstreamAttrs{}}
		g.targets[target] = tg
		g.targetOrder = append(g.targetOrder, target)
	}
	if _, ok := tg.causes[cause.JobName]; !ok {
		tg.causes[cause.JobName] = cause
		tg.causeOrder = append(tg.causeOrder, cause.JobName)
	}
}

func (a *TeamBreachAggregator) Build() []*PotentialSLABreachAttrs {
	out := make([]*PotentialSLABreachAttrs, 0, len(a.teamOrder))
	for _, teamName := range a.teamOrder {
		t := a.teams[teamName]
		attr := &PotentialSLABreachAttrs{TeamName: t.name}
		for _, projectName := range t.projectOrder {
			p := t.projects[projectName]
			project := SLABreachProject{Name: p.name}
			for _, groupName := range p.groupOrder {
				g := p.groups[groupName]
				group := SLABreachGroup{Name: g.name, Severity: g.severity}
				for _, targetName := range g.targetOrder {
					tg := g.targets[targetName]
					target := SLABreachTarget{JobName: tg.name}
					for _, causeName := range tg.causeOrder {
						target.Causes = append(target.Causes, tg.causes[causeName])
					}
					group.Targets = append(group.Targets, target)
				}
				project.Groups = append(project.Groups, group)
			}
			attr.Projects = append(attr.Projects, project)
		}
		out = append(out, attr)
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
