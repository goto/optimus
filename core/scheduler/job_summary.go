package scheduler

import (
	"time"

	"github.com/goto/optimus/core/tenant"
	"github.com/goto/optimus/internal/lib/window"
)

type JobSummary struct {
	JobName JobName
	Tenant  tenant.Tenant

	Window window.Config

	JobRuns   []JobRunSummary
	Upstreams []JobSummary
}

type JobRunSummary struct {
	ScheduledAt time.Time

	WaitStartTime time.Time
	WaitEndTime   time.Time
	TaskStartTime time.Time
	TaskEndTime   time.Time
	HookStartTime time.Time
	HookEndTime   time.Time
}
