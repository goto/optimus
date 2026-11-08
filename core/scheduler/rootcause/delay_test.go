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

	t.Run("third party wait ending after midnight UTC spans back to the previous day's boundary", func(t *testing.T) {
		// wait 2026-09-09 20:00 -> 2026-09-10 01:30 UTC with the 18:00 UTC boundary: the
		// boundary is the previous day's 18:00, so the wait is measured in full (5h30m).
		// The old per-calendar-day boundary measured zero for this wait.
		waitStart := time.Date(2026, 9, 9, 20, 0, 0, 0, time.UTC)
		waitEnd := time.Date(2026, 9, 10, 1, 30, 0, 0, time.UTC)
		state := scheduler.JobState{
			Reason: scheduler.ReasonThirdPartyDelay,
			JobRun: scheduler.JobRunSummary{
				WaitStartTime: &waitStart,
				WaitEndTime:   &waitEnd,
			},
		}
		assert.Equal(t, 5*time.Hour+30*time.Minute, inducedDelay(state, waitEnd, 18))
	})

	t.Run("third party wait within the same boundary day is measured from the boundary", func(t *testing.T) {
		// wait 18:30 -> 19:00 UTC with the 18:00 boundary: counted from the wait start
		state := scheduler.JobState{
			Reason: scheduler.ReasonThirdPartyDelay,
			JobRun: scheduler.JobRunSummary{
				WaitStartTime: at(18, 30),
				WaitEndTime:   at(19, 0),
			},
		}
		assert.Equal(t, 30*time.Minute, inducedDelay(state, ref, 18))
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

	t.Run("started late delay prefers the historical P50 start offset over the deadline-driven safe start", func(t *testing.T) {
		// deadline-driven safe start would be 05:20 (06:00 - 40m), but this job's own rhythm
		// (median start offset of 10m from its 05:00 schedule) says 05:10 is already late for
		// it, even though the SLA chain wouldn't be at risk yet.
		state := scheduler.JobState{
			Reason: scheduler.ReasonStartedLate,
			JobSLAState: scheduler.JobSLAState{
				EstimatedDuration:     dur(40 * time.Minute),
				InferredSLA:           at(6, 0),
				HistoricalStartOffset: dur(10 * time.Minute),
			},
			JobRun: scheduler.JobRunSummary{
				ScheduledAt:   *at(5, 0),
				TaskStartTime: at(5, 25),
			},
		}
		assert.Equal(t, 15*time.Minute, inducedDelay(state, ref, defaultThirdPartyDelayStartHour))
	})

	t.Run("started late delay falls back to the deadline-driven safe start when there's no run history yet", func(t *testing.T) {
		state := scheduler.JobState{
			Reason: scheduler.ReasonStartedLate,
			JobSLAState: scheduler.JobSLAState{
				EstimatedDuration:     dur(40 * time.Minute),
				InferredSLA:           at(6, 0),
				HistoricalStartOffset: nil, // new job, no successful runs yet
			},
			JobRun: scheduler.JobRunSummary{
				ScheduledAt:   *at(5, 0),
				TaskStartTime: at(5, 25),
			},
		}
		// naive safe start 05:20 (06:00 - 40m), which is after the 05:00 schedule, so it's used
		// as-is: 05:25 start is 5m past it.
		assert.Equal(t, 5*time.Minute, inducedDelay(state, ref, defaultThirdPartyDelayStartHour))
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

// TestPickMaxDelayTieBreak verifies equal-delay causes resolve deterministically by
// job name instead of map iteration order.
func TestPickMaxDelayTieBreak(t *testing.T) {
	state := func(name string, delay time.Duration) *scheduler.JobState {
		return &scheduler.JobState{
			JobName:  scheduler.JobName(name),
			Evidence: scheduler.RootCauseEvidence{InducedDelay: delay},
		}
	}

	t.Run("higher delay wins", func(t *testing.T) {
		got := pickMaxDelay([]*scheduler.JobState{state("job-A", time.Minute), state("job-B", 2*time.Minute)})
		assert.Equal(t, scheduler.JobName("job-B"), got.JobName)
	})

	t.Run("equal delay picks the lowest job name regardless of order", func(t *testing.T) {
		forward := pickMaxDelay([]*scheduler.JobState{state("job-B", time.Minute), state("job-A", time.Minute)})
		reverse := pickMaxDelay([]*scheduler.JobState{state("job-A", time.Minute), state("job-B", time.Minute)})
		assert.Equal(t, scheduler.JobName("job-A"), forward.JobName)
		assert.Equal(t, forward.JobName, reverse.JobName)
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
