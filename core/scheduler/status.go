package scheduler

import (
	"fmt"
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
	StateCanceled State = "canceled"

	StateRetry State = "retried"

	StateSuccess State = "success"
	StateFailed  State = "failed"

	StateNotScheduled State = "waiting_to_schedule"
	StateWaitUpstream State = "wait_upstream"
	StateInProgress   State = "in_progress"
	StateUpForRetry   State = "up_for_retry"
	StateRestarting   State = "restarting"
	StateMissing      State = "missing"
)

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
	case string(StateNotScheduled):
		return StateNotScheduled, nil
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

func (j JobRunStatus) GetState() State {
	return j.State
}

func (j JobRunStatus) GetScheduledAt() time.Time {
	return j.ScheduledAt
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

type JobRunWithDetails struct {
	ScheduledAt     time.Time
	State           State
	RunType         string
	ExternalTrigger bool
	DagRunID        string
	DagID           string
}

func (j JobRunWithDetails) GetState() State {
	return j.State
}

type JobRunDetailsList []*JobRunWithDetails

func (j JobRunDetailsList) GetSortedRunsByStates(states []State) []*JobRunWithDetails {
	stateMap := make(map[State]bool, len(states))
	for _, state := range states {
		stateMap[state] = true
	}

	var result []*JobRunWithDetails
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

func (j JobRunDetailsList) FilterRunsManagedByReplay(runs []*JobRunStatus) JobRunDetailsList {
	runMap := make(map[time.Time]bool, len(runs))
	for _, state := range runs {
		runMap[state.ScheduledAt] = true
	}

	var result []*JobRunWithDetails
	for _, run := range j {
		if runMap[run.ScheduledAt] {
			result = append(result, run)
		}
	}
	return result
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

func (j JobRunStatusList) GetJobRunStatusSummary() string {
	runStatusSummaryMap := j.getJobRunStatusSummaryMap()
	var statusSummary string
	for state, countRun := range runStatusSummaryMap {
		currentState := fmt.Sprintf("%s(%d)", state.String(), countRun)
		if statusSummary == "" {
			statusSummary = currentState
		} else {
			statusSummary = fmt.Sprintf("%s, %s", statusSummary, currentState)
		}
	}
	return statusSummary
}

func (j JobRunStatusList) getJobRunStatusSummaryMap() map[State]int {
	stateMap := make(map[State]int)
	for _, run := range j {
		stateMap[run.State]++
	}
	return stateMap
}

func (j JobRunStatusList) GetSuccessRuns() int {
	count := 0
	for _, run := range j {
		if run.State == StateSuccess {
			count++
		}
	}
	return count
}

func (j JobRunStatusList) IsAllTerminated() bool {
	for _, run := range j {
		if run.State == StateSuccess || run.State == StateFailed {
			continue
		}
		return false
	}
	return true
}

func (j JobRunStatusList) IsAnyFailure() bool {
	for _, run := range j {
		if run.State == StateFailed {
			return true
		}
	}
	return false
}

func (j JobRunStatusList) GetOnlyDifferedRuns(runsComparator []*JobRunStatus) JobRunStatusList {
	var differedRuns []*JobRunStatus
	runMap := j.ToRunStatusMap()
	for _, comparatorRun := range runsComparator {
		runState, ok := runMap[comparatorRun.ScheduledAt.UTC()]
		if !ok {
			continue
		}
		if runState != comparatorRun.State {
			differedRun := &JobRunStatus{
				ScheduledAt: comparatorRun.ScheduledAt,
				State:       runState,
			}
			differedRuns = append(differedRuns, differedRun)
		}
	}
	return differedRuns
}

func (j JobRunStatusList) String() string {
	var sb strings.Builder
	for i, run := range j {
		sb.WriteString(fmt.Sprintf("%s(%s)", run.ScheduledAt.Format(time.RFC3339), run.State.String()))
		if i < len(j)-1 {
			sb.WriteString(", ")
		}
	}

	return sb.String()
}

// JobRunsCriteria represents the filter condition to get run status from scheduler
type JobRunsCriteria struct {
	Name        string
	StartDate   time.Time
	EndDate     time.Time
	Filter      []string
	OnlyLastRun bool
}

func (c *JobRunsCriteria) ExecutionStart(cron *cron.ScheduleSpec) time.Time {
	return cron.Prev(c.StartDate)
}

func (c *JobRunsCriteria) ExecutionEndDate(jobCron *cron.ScheduleSpec) time.Time {
	scheduleEndTime := c.EndDate

	// when the current time matches one of the schedule times execution time means previous schedule.
	if jobCron.Next(scheduleEndTime.Add(-time.Second * 1)).Equal(scheduleEndTime) {
		return jobCron.Prev(scheduleEndTime)
	}
	// else it is previous to previous schedule.
	return jobCron.Prev(jobCron.Prev(scheduleEndTime))
}
