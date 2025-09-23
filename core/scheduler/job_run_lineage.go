package scheduler

import (
	"time"

	"github.com/goto/optimus/core/tenant"
)

type JobSchedule struct {
	JobName     JobName
	ScheduledAt time.Time
}

type JobRunLineageSummary struct {
	JobName JobName
	Tenant  tenant.Tenant

	SLA              SLAConfig
	ScheduleInterval string

	JobRuns   map[string]*JobRunSummary
	Upstreams []*JobRunLineageSummary
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
