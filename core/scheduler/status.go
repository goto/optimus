package scheduler

import (
	"sort"
	"strings"
	"time"

	"github.com/goto/optimus/internal/errors"
	"github.com/goto/optimus/internal/lib/cron"
)

const (
	StatePending State = "pending"

	StateAccepted State = "accepted"
	StateRunning  State = "running"
	StateQueued   State = "queued"

	StateRetry State = "retried"

	StateSuccess State = "success"
	StateFailed  State = "failed"

	StateWaitUpstream State = "wait_upstream"
	StateInProgress   State = "in_progress"

	StateMissing State = "missing"
)

var TaskEndStates = []State{StateSuccess, StateFailed, StateRetry}

type State string

func StateFromString(state string) (State, error) {
	switch strings.ToLower(state) {
	case string(StatePending):
		return StatePending, nil
	case string(StateAccepted):
		return StateAccepted, nil
	case string(StateRunning):
		return StateRunning, nil
	case string(StateRetry):
		return StateRetry, nil
	case string(StateQueued):
		return StateQueued, nil
	case string(StateSuccess):
		return StateSuccess, nil
	case string(StateFailed):
		return StateFailed, nil
	case string(StateWaitUpstream):
		return StateWaitUpstream, nil
	case string(StateInProgress):
		return StateInProgress, nil
	default:
		return "", errors.InvalidArgument(EntityJobRun, "invalid state for run "+state)
	}
}

func (j State) String() string {
	return string(j)
}

type JobRunStatus struct {
	ScheduledAt time.Time
	State       State
}

func JobRunStatusFrom(scheduledAt time.Time, state string) (JobRunStatus, error) {
	runState, err := StateFromString(state)
	if err != nil {
		return JobRunStatus{}, err
	}

	return JobRunStatus{
		ScheduledAt: scheduledAt,
		State:       runState,
	}, nil
}

func (j JobRunStatus) GetLogicalTime(jobCron *cron.ScheduleSpec) time.Time {
	return jobCron.Prev(j.ScheduledAt)
}

type JobRunStatusList []*JobRunStatus

func (j JobRunStatusList) GetSortedRunsByStates(states []State) []*JobRunStatus {
	stateMap := make(map[State]bool, len(states))
	for _, state := range states {
		stateMap[state] = true
	}

	var result []*JobRunStatus
	for _, run := range j {
		if stateMap[run.State] {
			result = append(result, run)
		}
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].ScheduledAt.Before(result[j].ScheduledAt)
	})
	return result
}

func (j JobRunStatusList) GetSortedRunsByScheduledAt() []*JobRunStatus {
	result := []*JobRunStatus(j)
	sort.Slice(result, func(i, j int) bool {
		return result[i].ScheduledAt.Before(result[j].ScheduledAt)
	})
	return result
}

func (j JobRunStatusList) MergeWithUpdatedRuns(updatedRunMap map[time.Time]State) []*JobRunStatus {
	var updatedRuns []*JobRunStatus
	for _, run := range j {
		if updatedStatus, ok := updatedRunMap[run.ScheduledAt.UTC()]; ok {
			updatedRun := run
			updatedRun.State = updatedStatus
			updatedRuns = append(updatedRuns, updatedRun)
			continue
		}
		updatedRuns = append(updatedRuns, run)
	}
	return updatedRuns
}

func (j JobRunStatusList) ToRunStatusMap() map[time.Time]State {
	runStatusMap := make(map[time.Time]State, len(j))
	for _, run := range j {
		runStatusMap[run.ScheduledAt.UTC()] = run.State
	}
	return runStatusMap
}

func (j JobRunStatusList) OverrideWithStatus(status State) []*JobRunStatus {
	overridedRuns := make([]*JobRunStatus, len(j))
	for i, run := range j {
		overridedRuns[i] = run
		overridedRuns[i].State = status
	}
	return overridedRuns
}

type Time time.Time

// NewTimeFrom creates scheduler context time from optimus context time
// it always 1 shift late
func NewTimeFrom(t time.Time, cron *cron.ScheduleSpec) time.Time {
	return cron.Prev(t)
}

// JobRunsCriteria represents the filter condition to get run status from scheduler
type JobRunsCriteria struct {
	Name        string
	StartDate   time.Time
	EndDate     time.Time
	Filter      []string
	OnlyLastRun bool
}

func (c *JobRunsCriteria) ExecutionStart(jobCron *cron.ScheduleSpec) time.Time {
	startDate := jobCron.Next(c.StartDate.Add(-time.Second * 1)) // normalize
	return NewTimeFrom(startDate, jobCron)
}

func (c *JobRunsCriteria) ExecutionEndDate(jobCron *cron.ScheduleSpec) time.Time {
	endDate := jobCron.Prev(c.EndDate.Add(time.Second * 1)) // normalize
	return NewTimeFrom(endDate, jobCron)
}
