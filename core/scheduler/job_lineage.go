package scheduler

import (
	"time"

	"github.com/goto/optimus/core/tenant"
)

type JobSchedule struct {
	JobName     JobName
	ScheduledAt time.Time
}

type JobLineageSummary struct {
	JobName JobName
	Tenant  tenant.Tenant

	ScheduleInterval  string
	SLA               SLAConfig
	InferredSLA       *time.Time
	EstimatedDuration *time.Duration

	JobRuns   map[string]*JobRunSummary
	Upstreams []*JobLineageSummary
}

type SLAConfig struct {
	Duration time.Duration
}

type JobRunSummary struct {
	ScheduledAt time.Time

	WaitStartTime time.Time
	WaitEndTime   time.Time
	TaskStartTime *time.Time
	TaskEndTime   *time.Time
	HookStartTime *time.Time
	HookEndTime   *time.Time
}
