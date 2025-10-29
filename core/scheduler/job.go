package scheduler

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/goto/optimus/core/resource"
	"github.com/goto/optimus/core/tenant"
	"github.com/goto/optimus/internal/errors"
	"github.com/goto/optimus/internal/lib/cron"
	"github.com/goto/optimus/internal/lib/window"
	"github.com/goto/optimus/internal/utils"
)

type (
	JobName      string
	OperatorType string
)

func (o OperatorType) String() string {
	return string(o)
}

func NewOperatorType(op string) (OperatorType, error) {
	switch strings.ToLower(op) {
	case OperatorTask.String():
		return OperatorTask, nil
	case OperatorSensor.String():
		return OperatorSensor, nil
	case OperatorHook.String():
		return OperatorHook, nil
	default:
		return OperatorType(op), errors.InvalidArgument(EntityEvent, fmt.Sprintf("invalid operator type: [%s], supported : task, sensor, hook", op))
	}
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

func (j *Job) IsDryRun() bool {
	if j.Task == nil {
		return false
	}
	if val, ok := j.Task.Config["DRY_RUN"]; ok {
		return utils.ConvertToBoolean(strings.ToLower(val))
	}
	return false
}

func (j *Job) GetTaskAlertConfig() *OperatorAlertConfig {
	if j.Task == nil {
		return nil
	}
	return j.Task.AlertConfig
}

func (j *Job) GetHookAlertConfigByName(hookName string) *OperatorAlertConfig {
	for _, hook := range j.Hooks {
		if hook.Name == hookName {
			return hook.AlertConfig
		}
	}
	return nil
}

func (j *Job) GetOperatorAlertConfigByName(operatorType OperatorType, operatorName string) *OperatorAlertConfig {
	switch operatorType {
	case OperatorTask:
		return j.GetTaskAlertConfig()
	case OperatorHook:
		operatorName = strings.TrimPrefix(operatorName, "hook_")
		return j.GetHookAlertConfigByName(operatorName)
	default:
		return nil
	}
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

type Severity string

const (
	Critical Severity = "CRITICAL"
	Warning  Severity = "WARNING"
	Info     Severity = "INFO"
)

func SeverityFromString(severity string) (Severity, error) {
	switch {
	case strings.EqualFold(severity, Critical.String()):
		return Critical, nil
	case strings.EqualFold(severity, Warning.String()):
		return Warning, nil
	case strings.EqualFold(severity, Info.String()):
		return Info, nil
	default:
		return "", errors.InvalidArgument(EntityJobRun, "invalid severity, expected on of 'WARNING', 'INFO' or 'CRITICAL'")
	}
}

func (s Severity) String() string {
	return string(s)
}

type SLAAlertConfig struct {
	DurationThreshold time.Duration `json:"duration_threshold,omitempty"`
	Severity          Severity      `json:"severity,omitempty"`
	Team              string        `json:"team,omitempty"`
	AutoThreshold     bool          `json:"auto_threshold,omitempty"`
}

func (s SLAAlertConfig) Tag() string {
	tagStr := fmt.Sprintf("%s:%s", s.Severity, s.DurationThreshold)
	hash := sha256.Sum256([]byte(tagStr))
	return hex.EncodeToString(hash[:])
}

type OperatorAlertConfig struct {
	SLAAlertConfigs []*SLAAlertConfig `json:"sla_alert_configs,omitempty"`
	Team            string            `json:"team,omitempty"`
}

func (o OperatorAlertConfig) GetSLAOperatorAlertConfigByTag(alertTag string) *SLAAlertConfig {
	for _, config := range o.SLAAlertConfigs {
		if config.Tag() == alertTag {
			return config
		}
	}
	return nil
}

type Task struct {
	Name        string               `json:"name,omitempty"`
	Version     string               `json:"version,omitempty"`
	Config      map[string]string    `json:"config,omitempty"`
	AlertConfig *OperatorAlertConfig `json:"alert_config,omitempty"`
}

type Hook struct {
	Name        string               `json:"name,omitempty"`
	Version     string               `json:"version,omitempty"`
	Config      map[string]string    `json:"config,omitempty"`
	AlertConfig *OperatorAlertConfig `json:"alert_config,omitempty"`
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
	return GetSLADuration(j.Alerts)
}

func GetSLADuration(alerts []Alert) (int64, error) {
	for _, notify := range alerts {
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

func (s *Schedule) GetLogicalStartTime() (time.Time, error) {
	interval := s.Interval
	if interval == "" {
		return time.Time{}, errors.InvalidArgument(EntityJobRun, "cannot get job schedule start date, job interval is empty")
	}
	jobCron, err := cron.ParseCronSchedule(interval)
	if err != nil {
		msg := fmt.Sprintf("unable to parse job cron interval: %s", err)
		return time.Time{}, errors.InvalidArgument(EntityJobRun, msg)
	}

	logicalStartTime := jobCron.Next(s.StartDate.Add(-time.Second * 1))
	return logicalStartTime, nil
}

func (s *Schedule) GetScheduleStartTime() (time.Time, error) {
	if s.StartDate.IsZero() {
		return time.Time{}, errors.InvalidArgument(EntityJobRun, "job schedule startDate not found in job")
	}
	interval := s.Interval
	if interval == "" {
		return time.Time{}, errors.InvalidArgument(EntityJobRun, "cannot get job schedule start date, job interval is empty")
	}
	jobCron, err := cron.ParseCronSchedule(interval)
	if err != nil {
		msg := fmt.Sprintf("unable to parse job cron interval: %s", err)
		return time.Time{}, errors.InvalidArgument(EntityJobRun, msg)
	}

	logicalStartTime := jobCron.Next(s.StartDate.Add(-time.Second * 1))
	scheduleStartTime := jobCron.Next(logicalStartTime)
	return scheduleStartTime, nil
}

func (s *Schedule) GetNextSchedule(after time.Time) (time.Time, error) {
	if s.StartDate.IsZero() {
		return time.Time{}, errors.InvalidArgument(EntityJobRun, "job schedule startDate not found in job")
	}
	interval := s.Interval
	if interval == "" {
		return time.Time{}, errors.InvalidArgument(EntityJobRun, "cannot get job schedule start date, job interval is empty")
	}
	jobCron, err := cron.ParseCronSchedule(interval)
	if err != nil {
		msg := fmt.Sprintf("unable to parse job cron interval: %s", err)
		return time.Time{}, errors.InvalidArgument(EntityJobRun, msg)
	}

	if after.Before(s.StartDate) {
		return s.GetScheduleStartTime()
	}

	return jobCron.Next(after), nil
}

func (s *Schedule) GetPreviousSchedule(before time.Time) (time.Time, error) {
	if s.StartDate.IsZero() {
		return time.Time{}, errors.InvalidArgument(EntityJobRun, "job schedule startDate not found in job")
	}
	interval := s.Interval
	if interval == "" {
		return time.Time{}, errors.InvalidArgument(EntityJobRun, "cannot get job schedule start date, job interval is empty")
	}
	jobCron, err := cron.ParseCronSchedule(interval)
	if err != nil {
		msg := fmt.Sprintf("unable to parse job cron interval: %s", err)
		return time.Time{}, errors.InvalidArgument(EntityJobRun, msg)
	}

	if before.Before(s.StartDate) {
		return time.Time{}, errors.InvalidArgument(EntityJobRun, "cannot get previous schedule before job start date")
	}

	return jobCron.Prev(before), nil
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
	On       EventCategory
	Channels []string
	Config   map[string]string
	Severity string
	Team     string
}

type WebhookEndPoint struct {
	URL     string
	Headers map[string]string
}

type Webhook struct {
	On        EventCategory
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

type JobSummary struct {
	JobName          JobName
	Tenant           tenant.Tenant
	ScheduleInterval string
	SLA              SLAConfig
	Window           window.Config
}
