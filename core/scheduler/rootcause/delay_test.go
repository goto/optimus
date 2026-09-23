//go:build unit_test

package rootcause

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/goto/optimus/core/scheduler"
)

func TestInducedDelay(t *testing.T) {
	ref := time.Date(2026, 9, 10, 8, 0, 0, 0, time.UTC)
	at := func(h, m int) *time.Time {
		v := time.Date(2026, 9, 10, h, m, 0, 0, time.UTC)
		return &v
	}
	dur := func(d time.Duration) *time.Duration { return &d }

	t.Run("running long is overrun versus historical duration", func(t *testing.T) {
		state := scheduler.JobState{
			Reason:      scheduler.ReasonRunningLong,
			JobSLAState: scheduler.JobSLAState{EstimatedDuration: dur(30 * time.Minute)},
			JobRun: scheduler.JobRunSummary{
				TaskStartTime: at(5, 0),
				TaskEndTime:   at(6, 10),
			},
		}
		assert.Equal(t, 40*time.Minute, inducedDelay(state, ref, defaultThirdPartyDelayStartHour))
	})

	t.Run("third party wait before delay start hour UTC is not counted", func(t *testing.T) {
		state := scheduler.JobState{
			Reason: scheduler.ReasonThirdPartyDelay,
			JobRun: scheduler.JobRunSummary{
				WaitStartTime: at(0, 10),
				WaitEndTime:   at(3, 0),
			},
		}
		assert.Equal(t, 2*time.Hour, inducedDelay(state, ref, 1))
	})

	t.Run("pending third party wait uses reference time as completion", func(t *testing.T) {
		state := scheduler.JobState{
			Reason: scheduler.ReasonThirdPartyDelay,
			JobRun: scheduler.JobRunSummary{
				WaitStartTime: at(6, 0),
			},
		}
		assert.Equal(t, 2*time.Hour, inducedDelay(state, ref, defaultThirdPartyDelayStartHour))
	})

	t.Run("missing wait start counts from the UTC delay start hour", func(t *testing.T) {
		state := scheduler.JobState{
			Reason: scheduler.ReasonThirdPartyDelay,
			JobRun: scheduler.JobRunSummary{},
		}
		assert.Equal(t, 8*time.Hour, inducedDelay(state, ref, defaultThirdPartyDelayStartHour))
	})

	t.Run("configured UTC delay start hour is the wait floor", func(t *testing.T) {
		state := scheduler.JobState{
			Reason: scheduler.ReasonThirdPartyDelay,
			JobRun: scheduler.JobRunSummary{
				WaitStartTime: at(0, 10),
				WaitEndTime:   at(8, 0),
			},
		}
		assert.Equal(t, 2*time.Hour, inducedDelay(state, ref, 6))
	})

	t.Run("started late delay is floored at the job's own schedule, not the naive cascade safe start", func(t *testing.T) {
		// estimate 40m, inferred SLA 06:00 -> naive safe start 05:20; but the job's own
		// schedule is 05:40 (SLA budget too tight for the full upstream chain). Started
		// right on that schedule -> zero induced delay, not ~20m against the naive value.
		state := scheduler.JobState{
			Reason:      scheduler.ReasonStartedLate,
			JobSLAState: scheduler.JobSLAState{EstimatedDuration: dur(40 * time.Minute), InferredSLA: at(6, 0)},
			JobRun: scheduler.JobRunSummary{
				ScheduledAt:   *at(5, 40),
				TaskStartTime: at(5, 40),
			},
		}
		assert.Equal(t, time.Duration(0), inducedDelay(state, ref, defaultThirdPartyDelayStartHour))
	})

	t.Run("started late delay above the floored schedule is measured from the schedule", func(t *testing.T) {
		// same setup as above, but this run started 05:50, 10m after its own 05:40 schedule.
		state := scheduler.JobState{
			Reason:      scheduler.ReasonStartedLate,
			JobSLAState: scheduler.JobSLAState{EstimatedDuration: dur(40 * time.Minute), InferredSLA: at(6, 0)},
			JobRun: scheduler.JobRunSummary{
				ScheduledAt:   *at(5, 40),
				TaskStartTime: at(5, 50),
			},
		}
		assert.Equal(t, 10*time.Minute, inducedDelay(state, ref, defaultThirdPartyDelayStartHour))
	})

	t.Run("upstream failed delay grows with how long ago the run ended", func(t *testing.T) {
		state := scheduler.JobState{
			Reason: scheduler.ReasonUpstreamFailed,
			JobRun: scheduler.JobRunSummary{JobEndTime: at(5, 0)},
		}
		assert.Equal(t, 3*time.Hour, inducedDelay(state, ref, defaultThirdPartyDelayStartHour))
	})

	t.Run("upstream failed delay is zero without a job end time", func(t *testing.T) {
		state := scheduler.JobState{
			Reason: scheduler.ReasonUpstreamFailed,
			JobRun: scheduler.JobRunSummary{},
		}
		assert.Equal(t, time.Duration(0), inducedDelay(state, ref, defaultThirdPartyDelayStartHour))
	})
}

func TestAttributeThirdPartyIdentity(t *testing.T) {
	state := scheduler.JobState{
		JobName: "job-B",
		Reason:  scheduler.ReasonThirdPartyDelay,
		Evidence: scheduler.RootCauseEvidence{
			BlockedOnSensors: []string{"wait_dex_orders"},
		},
	}
	got := attributeThirdPartyIdentity(state)
	assert.Equal(t, scheduler.JobName("wait_dex_orders"), got.JobName)
	assert.Equal(t, scheduler.JobName("job-B"), got.JobRun.JobName)
}

func TestDemoteBelowMinRootCauseDelay(t *testing.T) {
	cause := &scheduler.JobState{
		JobName:  "job-B",
		Reason:   scheduler.ReasonRunningLong,
		Evidence: scheduler.RootCauseEvidence{InducedDelay: 12 * time.Minute},
	}

	t.Run("below floor drops the classified reason", func(t *testing.T) {
		got := demoteBelowMinRootCauseDelay(cause, 13*time.Minute)
		assert.Equal(t, scheduler.ReasonUnknown, got.Reason)
		assert.Equal(t, scheduler.JobName("job-B"), got.JobName)
		assert.Equal(t, 12*time.Minute, got.Evidence.InducedDelay)
		assert.Equal(t, scheduler.ReasonRunningLong, cause.Reason)
	})

	t.Run("equal to floor keeps the classified reason", func(t *testing.T) {
		got := demoteBelowMinRootCauseDelay(cause, 12*time.Minute)
		assert.Same(t, cause, got)
		assert.Equal(t, scheduler.ReasonRunningLong, got.Reason)
	})

	t.Run("zero floor is a no-op", func(t *testing.T) {
		got := demoteBelowMinRootCauseDelay(cause, 0)
		assert.Same(t, cause, got)
	})
}
