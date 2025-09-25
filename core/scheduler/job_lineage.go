package scheduler

import (
	"time"

	"github.com/goto/optimus/core/tenant"
	"github.com/goto/optimus/internal/lib/window"
)

type JobSchedule struct {
	JobName     JobName
	ScheduledAt time.Time
}

type JobLineageSummary struct {
	JobName   JobName
	Upstreams []*JobLineageSummary

	Tenant           tenant.Tenant
	ScheduleInterval string
	SLA              SLAConfig
	Window           *window.Config

	InferredSLAByJobName map[JobName]*time.Time
	EstimatedDuration    *time.Duration

	JobRuns map[string]*JobRunSummary
}

type SLAConfig struct {
	Duration time.Duration
}

type JobRunSummary struct {
	JobName     JobName
	ScheduledAt time.Time

	JobStartTime  *time.Time
	JobEndTime    *time.Time
	WaitStartTime *time.Time
	WaitEndTime   *time.Time
	TaskStartTime *time.Time
	TaskEndTime   *time.Time
	HookStartTime *time.Time
	HookEndTime   *time.Time
}

type JobUpstreamPair struct {
	JobName         JobName
	UpstreamJobName JobName
}

type JobScheduleDetail struct {
	JobName JobName
	Tenant  tenant.Tenant

	ScheduleInterval string
	SLA              SLAConfig
	Window           *window.Config
}
