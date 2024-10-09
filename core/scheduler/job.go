package scheduler

import (
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/goto/optimus/core/resource"
	"github.com/goto/optimus/core/tenant"
	"github.com/goto/optimus/internal/errors"
	"github.com/goto/optimus/internal/lib/window"
)

type (
	JobName      string
	OperatorType string
)

func (o OperatorType) String() string {
	return string(o)
}

const (
	EntityJobRun = "jobRun"

	urnContext = "optimus"

	OperatorTask   OperatorType = "task"
	OperatorSensor OperatorType = "sensor"
	OperatorHook   OperatorType = "hook"

	UpstreamTypeStatic   = "static"
	UpstreamTypeInferred = "inferred"
)

func JobNameFrom(name string) (JobName, error) {
	if name == "" {
		return "", errors.InvalidArgument(EntityJobRun, "job name is empty")
	}

	return JobName(name), nil
}

func (n JobName) String() string {
	return string(n)
}

func (n JobName) GetJobURN(tnnt tenant.Tenant) string {
	return fmt.Sprintf("urn:optimus:%s:job:%s.%s.%s", tnnt.ProjectName(), tnnt.ProjectName(), tnnt.NamespaceName(), n)
}

type Job struct {
	ID     uuid.UUID
	Name   JobName
	Tenant tenant.Tenant

	Destination resource.URN
	Task        *Task
	Hooks       []*Hook

	WindowConfig window.Config
	Assets       map[string]string
}

func (j *Job) GetHook(hookName string) (*Hook, error) {
	for _, hook := range j.Hooks {
		if hook.Name == hookName {
			return hook, nil
		}
	}
	return nil, errors.NotFound(EntityJobRun, "hook:"+hookName)
}

func (j *Job) URN() string {
	return fmt.Sprintf("urn:%s:%s:job:%s.%s.%s", urnContext, j.Tenant.ProjectName(), j.Tenant.ProjectName(), j.Tenant.NamespaceName(), j.Name)
}

type Task struct {
	Name   string
	Config map[string]string
}

type Hook struct {
	Name   string
	Config map[string]string
}

// JobWithDetails contains the details for a job
type JobWithDetails struct {
	Name JobName

	Job           *Job
	JobMetadata   *JobMetadata
	Schedule      *Schedule
	Retry         Retry
	Alerts        []Alert
	Webhook       []Webhook
	RuntimeConfig RuntimeConfig
	Priority      int
	Upstreams     Upstreams
}

func (j *JobWithDetails) GetName() string {
	return j.Name.String()
}

func GroupJobsByTenant(j []*JobWithDetails) map[tenant.Tenant][]*JobWithDetails {
	jobsGroup := make(map[tenant.Tenant][]*JobWithDetails)
	for _, job := range j {
		tnnt := job.Job.Tenant
		jobsGroup[tnnt] = append(jobsGroup[tnnt], job)
	}
	return jobsGroup
}

func (j *JobWithDetails) SLADuration() (int64, error) {
	for _, notify := range j.Alerts {
		if notify.On == EventCategorySLAMiss {
			if _, ok := notify.Config["duration"]; !ok {
				continue
			}

			dur, err := time.ParseDuration(notify.Config["duration"])
			if err != nil {
				return 0, fmt.Errorf("failed to parse sla_miss duration %s: %w", notify.Config["duration"], err)
			}
			return int64(dur.Seconds()), nil
		}
	}
	return 0, nil
}

type JobMetadata struct {
	Version     int
	Owner       string
	Description string
	Labels      map[string]string
}

type Schedule struct {
	DependsOnPast bool
	CatchUp       bool
	StartDate     time.Time
	EndDate       *time.Time
	Interval      string
}

func (j *JobWithDetails) GetLabelsAsString() string {
	labels := ""
	for k, v := range j.JobMetadata.Labels {
		labels += fmt.Sprintf("%s=%s,", strings.TrimSpace(k), strings.TrimSpace(v))
	}
	return strings.TrimRight(labels, ",")
}

func (j *JobWithDetails) GetUniqueLabelValues() []string {
	labelValues := []string{}
	m := map[string]bool{}
	for _, v := range j.JobMetadata.Labels {
		if _, ok := m[v]; !ok {
			labelValues = append(labelValues, v)
		}
		m[v] = true
	}
	return labelValues
}

func (j *JobWithDetails) GetSafeLabels() map[string]string {
	emptyOutput := make(map[string]string)
	if j == nil {
		return emptyOutput
	}

	if j.JobMetadata == nil {
		return emptyOutput
	}

	if j.JobMetadata.Labels == nil {
		return emptyOutput
	}

	return j.JobMetadata.Labels
}

type Retry struct {
	ExponentialBackoff bool
	Count              int
	Delay              int32
}

type Alert struct {
	On       JobEventCategory
	Channels []string
	Config   map[string]string
}

type WebhookEndPoint struct {
	URL     string
	Headers map[string]string
}

type Webhook struct {
	On        JobEventCategory
	Endpoints []WebhookEndPoint
}

type RuntimeConfig struct {
	Resource  *Resource
	Scheduler map[string]string
}

type Resource struct {
	Request *ResourceConfig
	Limit   *ResourceConfig
}

type ResourceConfig struct {
	CPU    string
	Memory string
}

type Upstreams struct {
	HTTP         []*HTTPUpstreams
	UpstreamJobs []*JobUpstream
}

type HTTPUpstreams struct {
	Name    string
	URL     string
	Headers map[string]string
	Params  map[string]string
}

type JobUpstream struct {
	JobName        string
	Host           string
	TaskName       string        // TODO: remove after airflow migration
	DestinationURN resource.URN  //- bigquery://pilot.playground.table
	Tenant         tenant.Tenant // Current or external tenant
	Type           string
	External       bool
	State          string
}
