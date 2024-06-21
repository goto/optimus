package scheduler

import (
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/goto/optimus/core/tenant"
	"github.com/goto/optimus/internal/errors"
)

const (
	// ReplayCreated is an event which indicates the replay has been created but not picked up yet
	ReplayCreated ReplayEventType = "replay_created"

	// ReplayStateCreated is an initial state which indicates the replay has been created but not picked up yet
	ReplayStateCreated ReplayState = "created"

	// ReplayStateInProgress indicates the replay is being executed
	ReplayStateInProgress ReplayState = "in progress"

	// ReplayStateSuccess is a terminal state which occurs when the replay execution finished with successful job runs
	ReplayStateSuccess ReplayState = "success"

	// ReplayStateFailed is a terminal state which occurs when the replay execution failed, timed out, or finished with one of the run fails
	ReplayStateFailed ReplayState = "failed"

	// ReplayStateCancelled is a terminal state which occurs when the replay is cancelled by user
	ReplayStateCancelled ReplayState = "cancelled"

	EntityReplay = "replay"
)

var (
	ReplayTerminalStates    = []ReplayState{ReplayStateSuccess, ReplayStateFailed, ReplayStateCancelled}
	ReplayNonTerminalStates = []ReplayState{ReplayStateCreated, ReplayStateInProgress}
)

type (
	ReplayEventType string
	ReplayState     string // contract status for business layer
	ReplayUserState string // contract status for presentation layer
)

func ReplayStateFromString(state string) (ReplayState, error) {
	switch strings.ToLower(state) {
	case string(ReplayStateCreated):
		return ReplayStateCreated, nil
	case string(ReplayStateInProgress):
		return ReplayStateInProgress, nil
	case string(ReplayStateSuccess):
		return ReplayStateSuccess, nil
	case string(ReplayStateFailed):
		return ReplayStateFailed, nil
	case string(ReplayStateCancelled):
		return ReplayStateCancelled, nil
	default:
		return "", errors.InvalidArgument(EntityJobRun, "invalid state for replay "+state)
	}
}

func (j ReplayEventType) String() string {
	return string(j)
}

func (j ReplayState) String() string {
	return string(j)
}

func (j ReplayUserState) String() string {
	return string(j)
}

type Replay struct {
	id uuid.UUID

	jobName JobName
	tenant  tenant.Tenant
	config  *ReplayConfig

	state   ReplayState
	message string

	createdAt time.Time
}

func (r *Replay) ID() uuid.UUID {
	return r.id
}

func (r *Replay) JobName() JobName {
	return r.jobName
}

func (r *Replay) Tenant() tenant.Tenant {
	return r.tenant
}

func (r *Replay) Config() *ReplayConfig {
	return r.config
}

func (r *Replay) State() ReplayState {
	return r.state
}

func (r *Replay) Message() string {
	return r.message
}

func (r *Replay) CreatedAt() time.Time {
	return r.createdAt
}

func (r *Replay) IsTerminated() bool {
	for _, terminalState := range ReplayTerminalStates {
		if r.State() == terminalState {
			return true
		}
	}
	return false
}

func NewReplayRequest(jobName JobName, tenant tenant.Tenant, config *ReplayConfig, state ReplayState) *Replay {
	return &Replay{jobName: jobName, tenant: tenant, config: config, state: state}
}

func NewReplay(id uuid.UUID, jobName JobName, tenant tenant.Tenant, config *ReplayConfig, state ReplayState, createdAt time.Time, message string) *Replay {
	return &Replay{id: id, jobName: jobName, tenant: tenant, config: config, state: state, createdAt: createdAt, message: message}
}

type ReplayWithRun struct {
	Replay *Replay
	Runs   []*JobRunStatus // TODO: JobRunStatus does not have `message/log`
}

func (r *ReplayWithRun) GetFirstExecutableRun() *JobRunStatus {
	runs := JobRunStatusList(r.Runs).GetSortedRunsByStates([]State{StatePending})
	if len(runs) > 0 {
		return runs[0]
	}
	return nil
}

func (r *ReplayWithRun) GetLastExecutableRun() *JobRunStatus {
	runs := JobRunStatusList(r.Runs).GetSortedRunsByStates([]State{StatePending})
	if len(runs) > 0 {
		return runs[len(runs)-1]
	}
	return nil
}

type ReplayConfig struct {
	StartTime   time.Time
	EndTime     time.Time
	Parallel    bool
	JobConfig   map[string]string
	Description string
}

func NewReplayConfig(startTime, endTime time.Time, parallel bool, jobConfig map[string]string, description string) *ReplayConfig {
	return &ReplayConfig{StartTime: startTime.UTC(), EndTime: endTime.UTC(), Parallel: parallel, JobConfig: jobConfig, Description: description}
}
