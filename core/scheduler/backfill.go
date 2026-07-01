package scheduler

import (
	"time"

	"github.com/google/uuid"

	"github.com/goto/optimus/core/tenant"
)

const (
	// BackfillStateCreated is an initial state which indicates the backfil has been created but not picked up yet
	BackfillStateCreated BackfillState = "created"

	// BackfillStateInProgress indicates the backfil is being executed
	BackfillStateInProgress BackfillState = "in progress"

	// BackfillStateSuccess is a terminal state which occurs when the backfil execution finished with successful job runs
	BackfillStateSuccess BackfillState = "success"

	BackfillStateTimeout BackfillState = "timeout"

	// BackfillStateFailed is a terminal state which occurs when the backfil execution failed, timed out, or finished with one of the run fails
	BackfillStateFailed BackfillState = "failed"

	// BackfillStateCancelled is a terminal state which occurs when the backfil is cancelled by user
	BackfillStateCancelled BackfillState = "cancelled"

	EntityBackfill = "backfill"
)

var (
	BackfillTerminalStates    = []BackfillState{BackfillStateSuccess, BackfillStateFailed, BackfillStateCancelled}
	BackfillNonTerminalStates = []BackfillState{BackfillStateCreated, BackfillStateInProgress}
)

type (
	BackfillState string
)

func (j BackfillState) String() string {
	return string(j)
}

type BackfilConfig struct {
	Dstart      time.Time
	Dend        time.Time
	JobConfig   map[string]string
	Assets      map[string]string
	Description string
	Category    string
	ApprovalID  string
	UserID      string
}

type Backfill struct {
	id             uuid.UUID
	SchedulerRunID string
	CanceledBy     string

	jobName JobName
	tenant  tenant.Tenant
	config  *BackfilConfig

	state   BackfillState
	message string

	createdAt time.Time
	updatedAt time.Time
}

func (r *Backfill) ID() uuid.UUID {
	return r.id
}

func (r *Backfill) JobName() JobName {
	return r.jobName
}

func (r *Backfill) Tenant() tenant.Tenant {
	return r.tenant
}

func (r *Backfill) UpdatedAt() time.Time {
	return r.updatedAt
}

func (r *Backfill) Config() *BackfilConfig {
	return r.config
}

func (r *Backfill) SetJobConfig(config map[string]string) {
	r.config.JobConfig = config
}

func (r *Backfill) State() BackfillState {
	return r.state
}

func (r *Backfill) Message() string {
	return r.message
}

func (r *Backfill) CreatedAt() time.Time {
	return r.createdAt
}

func (r *Backfill) GetStartTime() time.Time {
	return r.config.Dstart
}

func (r *Backfill) GetEndTime() time.Time {
	return r.config.Dend
}

func (r *Backfill) GetCategory() string {
	return r.config.Category
}

func (r *Backfill) GetApprovalID() string {
	return r.config.ApprovalID
}

func (r *Backfill) GetUserID() string {
	return r.config.UserID
}

func (r *Backfill) GetCanceledBy() string {
	return r.CanceledBy
}

func (r *Backfill) GetJobConfig() map[string]string {
	return r.config.JobConfig
}

func (r *Backfill) IsTerminated() bool {
	for _, terminalState := range BackfillTerminalStates {
		if r.State() == terminalState {
			return true
		}
	}
	return false
}

func NewBackfill(id uuid.UUID, jobName JobName, tenant tenant.Tenant, config *BackfilConfig, state BackfillState, createdAt, updatedAt time.Time, message string) *Backfill {
	return &Backfill{id: id, jobName: jobName, tenant: tenant, config: config, state: state, createdAt: createdAt, updatedAt: updatedAt, message: message}
}

func NewBackfillRequest(jobName JobName, tenant tenant.Tenant, config *BackfilConfig, state BackfillState, message string) *Backfill {
	return &Backfill{id: uuid.New(), jobName: jobName, tenant: tenant, config: config, state: state, message: message}
}

func NewBackfillConfig(startTime, endTime time.Time, jobConfig, assets map[string]string, description, category, approvalID, userID string) *BackfilConfig {
	return &BackfilConfig{
		Dstart:      startTime.UTC(),
		Dend:        endTime.UTC(),
		JobConfig:   jobConfig,
		Assets:      assets,
		Description: description,
		Category:    category,
		ApprovalID:  approvalID,
		UserID:      userID,
	}
}
