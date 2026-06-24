package scheduler

import (
	"time"

	"github.com/google/uuid"

	"github.com/goto/optimus/core/tenant"
)

const (
	// BackfilStateCreated is an initial state which indicates the backfil has been created but not picked up yet
	BackfilStateCreated BackfilState = "created"

	// BackfilStateInProgress indicates the backfil is being executed
	BackfilStateInProgress BackfilState = "in progress"

	// BackfilStateSuccess is a terminal state which occurs when the backfil execution finished with successful job runs
	BackfilStateSuccess BackfilState = "success"

	BackfilStateTimeout BackfilState = "timeout"

	// BackfilStateFailed is a terminal state which occurs when the backfil execution failed, timed out, or finished with one of the run fails
	BackfilStateFailed BackfilState = "failed"

	// BackfilStateCancelled is a terminal state which occurs when the backfil is cancelled by user
	BackfilStateCancelled BackfilState = "cancelled"

	EntityBackfill = "backfill"
)

var (
	BackfilTerminalStates    = []BackfilState{BackfilStateSuccess, BackfilStateFailed, BackfilStateCancelled}
	BackfilNonTerminalStates = []BackfilState{BackfilStateCreated, BackfilStateInProgress}
)

type (
	BackfilState string
)

// func BackfilStateFromString(state string) (BackfilState, error) {
//	switch strings.ToLower(state) {
//	case string(BackfilStateCreated):
//		return BackfilStateCreated, nil
//	case string(BackfilStateInProgress):
//		return BackfilStateInProgress, nil
//	case string(BackfilStateSuccess):
//		return BackfilStateSuccess, nil
//	case string(BackfilStateFailed):
//		return BackfilStateFailed, nil
//	case string(BackfilStateCancelled):
//		return BackfilStateCancelled, nil
//	default:
//		return "", errors.InvalidArgument(EntityBackfil, "invalid state for backfil "+state)
//	}
//}

func (j BackfilState) String() string {
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

	state   BackfilState
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

func (r *Backfill) State() BackfilState {
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
	for _, terminalState := range BackfilTerminalStates {
		if r.State() == terminalState {
			return true
		}
	}
	return false
}

func NewBackfill(id uuid.UUID, jobName JobName, tenant tenant.Tenant, config *BackfilConfig, state BackfilState, createdAt, updatedAt time.Time, message string) *Backfill {
	return &Backfill{id: id, jobName: jobName, tenant: tenant, config: config, state: state, createdAt: createdAt, updatedAt: updatedAt, message: message}
}

func NewBackfillRequest(jobName JobName, tenant tenant.Tenant, config *BackfilConfig, state BackfilState, message string) *Backfill {
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
