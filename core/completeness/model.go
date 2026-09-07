package completeness

import (
	"time"

	"github.com/goto/optimus/core/scheduler"
)

type OverallStatus string

const (
	OverallStatusComplete    OverallStatus = "COMPLETE"
	OverallStatusNotComplete OverallStatus = "NOT_COMPLETE"
)

// RunStatus is the run selected by SelectScheduledAt. A nil *RunStatus on ManagedTable
// means the job hasn't reached its relevant occurrence yet, or no run was recorded for
// it -- both map to NOT_COMPLETE.
type RunStatus struct {
	State       scheduler.State
	ScheduledAt time.Time
	StartTime   time.Time
	EndTime     *time.Time
}

type ManagedTable struct {
	TableName        string
	OptimusProject   string
	OptimusNamespace string
	JobName          string
	Run              *RunStatus
	IsActive         bool // false if the job is currently disabled/paused
}

type UnmanagedTable struct {
	TableName    string
	ManagedByDex bool
}

type Result struct {
	OverallStatus   OverallStatus
	UnmanagedTables []UnmanagedTable
	ManagedTables   []ManagedTable
}
