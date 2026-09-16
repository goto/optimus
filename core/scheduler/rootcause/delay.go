package rootcause

import (
	"time"

	"github.com/goto/optimus/core/scheduler"
)

const defaultThirdPartyChargeHour = 0

func inducedDelay(state scheduler.JobState, referenceTime time.Time, chargeHourUTC int) time.Duration {
	switch state.Reason {
	case scheduler.ReasonRunningLong:
		return runningLongDelay(state, referenceTime)
	case scheduler.ReasonStartedLate:
		return startedLateDelay(state, referenceTime)
	case scheduler.ReasonThirdPartyDelay:
		return thirdPartyInducedDelay(state, referenceTime, chargeHourUTC)
	default:
		return 0
	}
}

func runningLongDelay(state scheduler.JobState, referenceTime time.Time) time.Duration {
	start := state.JobRun.TaskStartTime
	if start == nil || state.EstimatedDuration == nil {
		return 0
	}
	end := referenceTime
	if actual := state.JobRun.GetActualEndTime(); actual != nil {
		end = *actual
	}
	overrun := end.Sub(*start) - *state.EstimatedDuration
	if overrun < 0 {
		return 0
	}
	return overrun
}

func startedLateDelay(state scheduler.JobState, referenceTime time.Time) time.Duration {
	if state.EstimatedDuration == nil || state.InferredSLA == nil {
		return 0
	}
	safeStart := state.InferredSLA.Add(-*state.EstimatedDuration)
	start := referenceTime
	if state.JobRun.TaskStartTime != nil {
		start = *state.JobRun.TaskStartTime
	}
	if !start.After(safeStart) {
		return 0
	}
	return start.Sub(safeStart)
}

// thirdPartyInducedDelay is sensor completion (or now) minus max(sensor start, charge hour
// on the completion calendar day in UTC). Wait before that UTC hour is not treated as impact.
func thirdPartyInducedDelay(state scheduler.JobState, referenceTime time.Time, chargeHourUTC int) time.Duration {
	end := referenceTime
	if state.JobRun.WaitEndTime != nil {
		end = *state.JobRun.WaitEndTime
	}
	chargeFrom := chargeableWaitStartUTC(end, chargeHourUTC)
	start := chargeFrom
	if state.JobRun.WaitStartTime != nil {
		start = *state.JobRun.WaitStartTime
	} else if !state.JobRun.ScheduledAt.IsZero() {
		start = state.JobRun.ScheduledAt
	}
	if chargeFrom.After(start) {
		start = chargeFrom
	}
	if !end.After(start) {
		return 0
	}
	return end.Sub(start)
}

func thirdPartyDelayExceedsThreshold(state scheduler.JobState, referenceTime time.Time, chargeHourUTC int, threshold time.Duration) bool {
	if threshold < 0 {
		threshold = 0
	}
	return thirdPartyInducedDelay(state, referenceTime, chargeHourUTC) > threshold
}

func chargeableWaitStartUTC(at time.Time, hour int) time.Time {
	if hour < 0 || hour > 23 {
		hour = defaultThirdPartyChargeHour
	}
	utc := at.UTC()
	return time.Date(utc.Year(), utc.Month(), utc.Day(), hour, 0, 0, 0, time.UTC)
}

func (i *Identifier) annotateInducedDelay(state scheduler.JobState, referenceTime time.Time) scheduler.JobState {
	state.Evidence.InducedDelay = inducedDelay(state, referenceTime, i.thirdPartyChargeHour)
	return state
}

// attributeThirdPartyIdentity makes the cause's JobName the blocking sensor so twenty
// Optimus jobs waiting on wait_dex_foo collapse to one root-cause identity. The waiter
// is preserved on JobRun.JobName.
func attributeThirdPartyIdentity(state scheduler.JobState) scheduler.JobState {
	if state.Reason != scheduler.ReasonThirdPartyDelay || len(state.Evidence.BlockedOnSensors) == 0 {
		return state
	}
	if state.JobRun.JobName == "" {
		state.JobRun.JobName = state.JobName
	}
	state.JobName = scheduler.JobName(state.Evidence.BlockedOnSensors[0])
	return state
}

func pickMaxDelay(causes []*scheduler.JobState) *scheduler.JobState {
	var chosen *scheduler.JobState
	for _, cause := range causes {
		if cause == nil {
			continue
		}
		if chosen == nil {
			chosen = cause
			continue
		}
		if cause.Evidence.InducedDelay > chosen.Evidence.InducedDelay {
			chosen = cause
			continue
		}
		if cause.Evidence.InducedDelay == chosen.Evidence.InducedDelay && cause.JobName.String() < chosen.JobName.String() {
			chosen = cause
		}
	}
	return chosen
}

func pickMaxDelayCause(causes map[scheduler.JobName]*scheduler.JobState) *scheduler.JobState {
	pool := make([]*scheduler.JobState, 0, len(causes))
	for _, cause := range causes {
		pool = append(pool, cause)
	}
	return pickMaxDelay(pool)
}
