//go:build unit_test

package rootcause_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/goto/optimus/core/scheduler"
	"github.com/goto/optimus/core/scheduler/rootcause"
)

func TestDefaultDetectors(t *testing.T) {
	ref := time.Date(2026, 9, 10, 6, 0, 0, 0, time.UTC)
	detectors := rootcause.DefaultDetectors([]string{"dex"})

	// first detector in the chain that claims the candidate wins
	detect := func(c rootcause.Candidate) (scheduler.RootCauseReason, scheduler.RootCauseEvidence) {
		for _, d := range detectors {
			if reason, evidence, ok := d.Detect(c); ok {
				return reason, evidence
			}
		}
		return scheduler.ReasonUnknown, scheduler.RootCauseEvidence{}
	}

	state := func(startedAt *time.Time, estimated *time.Duration, inferredSLA *time.Time) *scheduler.JobState {
		return &scheduler.JobState{
			JobName:     "job-A",
			JobSLAState: scheduler.JobSLAState{EstimatedDuration: estimated, InferredSLA: inferredSLA},
			JobRun:      scheduler.JobRunSummary{TaskStartTime: startedAt},
		}
	}
	at := func(h, m int) *time.Time {
		v := time.Date(2026, 9, 10, h, m, 0, 0, time.UTC)
		return &v
	}
	dur := func(d time.Duration) *time.Duration { return &d }

	t.Run("running past its estimated duration is RUNNING_LONG", func(t *testing.T) {
		// started 05:00, estimate 30m, now 06:00 -> 60m elapsed
		reason, evidence := detect(rootcause.Candidate{
			State:         state(at(5, 0), dur(30*time.Minute), at(5, 45)),
			ReferenceTime: ref,
		})

		assert.Equal(t, scheduler.ReasonRunningLong, reason)
		assert.Equal(t, *at(5, 0), *evidence.StartedAt)
		assert.Equal(t, *at(5, 30), *evidence.ExpectedFinishAt)
	})

	t.Run("normal speed but begun after the latest safe start is STARTED_LATE", func(t *testing.T) {
		// estimate 40m, deadline 06:00 -> had to start by 05:20; actually started 05:40
		reason, evidence := detect(rootcause.Candidate{
			State:         state(at(5, 40), dur(40*time.Minute), at(6, 0)),
			ReferenceTime: ref,
		})

		assert.Equal(t, scheduler.ReasonStartedLate, reason)
		assert.Equal(t, *at(5, 20), *evidence.LatestSafeStartAt)
		assert.Equal(t, *at(6, 20), *evidence.ExpectedFinishAt)
	})

	t.Run("a run that finished late but ran normally is STARTED_LATE, not RUNNING_LONG", func(t *testing.T) {
		// started 05:40, ran its usual 10m, done 05:50, deadline was 05:45.
		// evaluated at 06:00 -- elapsed must be the 10m it ran, not the 20m since it began
		finished := state(at(5, 40), dur(30*time.Minute), at(5, 45))
		finished.JobRun.TaskEndTime = at(5, 50)

		reason, _ := detect(rootcause.Candidate{State: finished, ReferenceTime: ref})

		assert.Equal(t, scheduler.ReasonStartedLate, reason)
	})

	t.Run("hook time is excluded, matching the task-only estimate", func(t *testing.T) {
		// task ran its usual 10m (05:40-05:50); a slow hook then pushed the run to 07:00.
		// the estimate is task-only, so the hook must not make this look like an overrun
		hookHeavy := state(at(5, 40), dur(30*time.Minute), at(5, 45))
		hookHeavy.JobRun.TaskEndTime = at(5, 50)
		hookHeavy.JobRun.HookEndTime = at(7, 0)

		reason, _ := detect(rootcause.Candidate{State: hookHeavy, ReferenceTime: ref})

		assert.Equal(t, scheduler.ReasonStartedLate, reason)
	})

	t.Run("every blocking third-party sensor is captured across providers", func(t *testing.T) {
		multi := rootcause.NewThirdPartySensorDetector([]string{"dex", "kafka"})

		_, evidence, ok := multi.Detect(rootcause.Candidate{
			State:          state(nil, dur(30*time.Minute), at(6, 0)),
			PendingSensors: []string{"wait_dex_orders", "wait_kafka_topic", "wait_upstream_job"},
			ReferenceTime:  ref,
		})

		assert.True(t, ok)
		// SourceType names one provider; BlockedOnSensors carries the full truth
		assert.Equal(t, []string{"wait_dex_orders", "wait_kafka_topic"}, evidence.BlockedOnSensors)
		assert.Contains(t, []string{"dex", "kafka"}, evidence.SourceType)
	})

	t.Run("overrunning takes precedence over having started late", func(t *testing.T) {
		// started late AND already past its estimate; the growing problem wins
		reason, _ := detect(rootcause.Candidate{
			State:         state(at(4, 0), dur(30*time.Minute), at(6, 0)),
			ReferenceTime: ref,
		})

		assert.Equal(t, scheduler.ReasonRunningLong, reason)
	})

	t.Run("unstarted job blocked on a third-party sensor is THIRD_PARTY_DELAY", func(t *testing.T) {
		reason, evidence := detect(rootcause.Candidate{
			State:          state(nil, dur(30*time.Minute), at(6, 0)),
			PendingSensors: []string{"wait_upstream_job", "wait_dex_p_gopay_id_raw.gpppo.order_final_log"},
			ReferenceTime:  ref,
		})

		assert.Equal(t, scheduler.ReasonThirdPartyDelay, reason)
		assert.Equal(t, []string{"wait_dex_p_gopay_id_raw.gpppo.order_final_log"}, evidence.BlockedOnSensors)
		assert.Equal(t, "dex", evidence.SourceType)
	})

	t.Run("no configured resolvers means third-party delay never fires", func(t *testing.T) {
		// the open-source default: nothing in upstream_resolvers, so the same pending
		// sensor is not attributable and must fall through rather than be guessed at
		bare := rootcause.DefaultDetectors(nil)
		var reason scheduler.RootCauseReason = scheduler.ReasonUnknown
		for _, d := range bare {
			if r, _, ok := d.Detect(rootcause.Candidate{
				State:          state(nil, dur(30*time.Minute), at(6, 0)),
				PendingSensors: []string{"wait_dex_p_gopay_id_raw.gpppo.order_final_log"},
				ReferenceTime:  ref,
			}); ok {
				reason = r
				break
			}
		}

		assert.Equal(t, scheduler.ReasonUnknown, reason)
	})

	t.Run("a resolver type other than dex is matched from config", func(t *testing.T) {
		kafka := rootcause.NewThirdPartySensorDetector([]string{"kafka"})

		reason, evidence, ok := kafka.Detect(rootcause.Candidate{
			State:          state(nil, dur(30*time.Minute), at(6, 0)),
			PendingSensors: []string{"wait_kafka_orders_topic"},
			ReferenceTime:  ref,
		})

		assert.True(t, ok)
		assert.Equal(t, scheduler.ReasonThirdPartyDelay, reason)
		assert.Equal(t, "kafka", evidence.SourceType)
	})

	t.Run("unstarted job blocked only on optimus upstreams is UNKNOWN", func(t *testing.T) {
		reason, _ := detect(rootcause.Candidate{
			State:          state(nil, dur(30*time.Minute), at(6, 0)),
			PendingSensors: []string{"wait_upstream_job"},
			ReferenceTime:  ref,
		})

		assert.Equal(t, scheduler.ReasonUnknown, reason)
	})

	t.Run("a started job is never blamed on raw data", func(t *testing.T) {
		// sensors cleared by definition once the task began, so a stale pending row
		// must not route this to the streaming owner
		reason, _ := detect(rootcause.Candidate{
			State:          state(at(5, 55), dur(30*time.Minute), at(6, 30)),
			PendingSensors: []string{"wait_dex_p_gopay_id_raw.gpppo.order_final_log"},
			ReferenceTime:  ref,
		})

		assert.Equal(t, scheduler.ReasonUnknown, reason)
	})

	t.Run("missing duration estimate falls through to UNKNOWN", func(t *testing.T) {
		reason, _ := detect(rootcause.Candidate{
			State:         state(at(5, 0), nil, at(6, 0)),
			ReferenceTime: ref,
		})

		assert.Equal(t, scheduler.ReasonUnknown, reason)
	})
}
