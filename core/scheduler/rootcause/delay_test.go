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
