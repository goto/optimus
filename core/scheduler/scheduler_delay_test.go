package scheduler_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/goto/optimus/core/scheduler"
)

func TestComputeSchedulerDelayPhases(t *testing.T) {
	scheduledAt := time.Date(2026, 11, 17, 5, 0, 0, 0, time.UTC)

	// A canonical happy path: the upstream DAG closed at 5:10, the sensor
	// noticed at 5:12, the task was queued at 5:13 and a worker picked it up
	// at 5:14.
	gateEnd := scheduledAt.Add(10 * time.Minute)
	sensorEnd := scheduledAt.Add(12 * time.Minute)
	queued := scheduledAt.Add(13 * time.Minute)
	taskStart := scheduledAt.Add(14 * time.Minute)

	earlyGateEnd := scheduledAt.Add(-5 * time.Minute) // upstream finished before scheduled_at
	lateGateEnd := scheduledAt.Add(13 * time.Minute)  // upstream finished after the sensor
	lateSensor := taskStart.Add(time.Minute)          // sensor "ended" after the task started
	earlyQueued := scheduledAt.Add(11 * time.Minute)  // queued before the sensor finished
	lateQueued := taskStart.Add(time.Minute)          // queued after the task started
	zeroTime := time.Time{}                           // absent value arriving as zero

	minute := time.Minute
	seconds := func(s int) *time.Duration {
		d := time.Duration(s) * time.Second
		return &d
	}

	tests := map[string]struct {
		input scheduler.DelayInput
		want  scheduler.DelayPhases
		skip  scheduler.DelaySkip
	}{
		"full happy path with upstream gate and queued time": {
			input: scheduler.DelayInput{
				ScheduledAt:   scheduledAt,
				TaskStart:     taskStart,
				Gate:          scheduler.DelayGateUpstream,
				GateEnd:       &gateEnd,
				LastSensorEnd: &sensorEnd,
				Queued:        &queued,
			},
			want: scheduler.DelayPhases{
				Gate:       scheduler.DelayGateUpstream,
				Detection:  seconds(2 * 60), // gate end 5:10 -> sensor 5:12
				Scheduling: seconds(60),     // sensor 5:12 -> queued 5:13
				Pickup:     seconds(60),     // queued 5:13 -> start 5:14
				Total:      4 * minute,      // gate end 5:10 -> start 5:14
			},
		},
		"no queued time falls back to handoff": {
			input: scheduler.DelayInput{
				ScheduledAt:   scheduledAt,
				TaskStart:     taskStart,
				Gate:          scheduler.DelayGateUpstream,
				GateEnd:       &gateEnd,
				LastSensorEnd: &sensorEnd,
			},
			want: scheduler.DelayPhases{
				Gate:      scheduler.DelayGateUpstream,
				Detection: seconds(2 * 60),
				Handoff:   seconds(2 * 60), // sensor 5:12 -> start 5:14
				Total:     4 * minute,
			},
		},
		"upstream ended before scheduled_at falls back to scheduled_at as gate": {
			input: scheduler.DelayInput{
				ScheduledAt:   scheduledAt,
				TaskStart:     taskStart,
				Gate:          scheduler.DelayGateUpstream,
				GateEnd:       &earlyGateEnd,
				LastSensorEnd: &sensorEnd,
				Queued:        &queued,
			},
			want: scheduler.DelayPhases{
				Gate:       scheduler.DelayGateUpstream,
				Detection:  seconds(12 * 60), // from scheduled_at 5:00 -> sensor 5:12
				Scheduling: seconds(60),
				Pickup:     seconds(60),
				Total:      14 * minute, // must run from scheduled_at, not the early gate end
			},
		},
		"root node without sensor measures one handoff from scheduled_at": {
			input: scheduler.DelayInput{
				ScheduledAt: scheduledAt,
				TaskStart:   taskStart,
				Gate:        scheduler.DelayGateNone,
			},
			want: scheduler.DelayPhases{
				Gate:    scheduler.DelayGateNone,
				Handoff: seconds(14 * 60),
				Total:   14 * minute,
			},
		},
		"root node with a sensor waits from scheduled_at": {
			input: scheduler.DelayInput{
				ScheduledAt:   scheduledAt,
				TaskStart:     taskStart,
				Gate:          scheduler.DelayGateNone,
				LastSensorEnd: &sensorEnd,
				Queued:        &queued,
			},
			want: scheduler.DelayPhases{
				Gate:       scheduler.DelayGateNone,
				Detection:  seconds(12 * 60), // scheduled 5:00 -> sensor 5:12
				Scheduling: seconds(60),
				Pickup:     seconds(60),
				Total:      14 * minute,
			},
		},
		"external gate skips detection but keeps handoff and total": {
			input: scheduler.DelayInput{
				ScheduledAt:   scheduledAt,
				TaskStart:     taskStart,
				Gate:          scheduler.DelayGateExternal,
				LastSensorEnd: &sensorEnd,
			},
			want: scheduler.DelayPhases{
				Gate:    scheduler.DelayGateExternal,
				Handoff: seconds(2 * 60),
				Total:   14 * minute, // from scheduled_at; external end is unknown
			},
		},
		"external host gate skips detection but keeps handoff and total": {
			input: scheduler.DelayInput{
				ScheduledAt:   scheduledAt,
				TaskStart:     taskStart,
				Gate:          scheduler.DelayGateExternalHost,
				LastSensorEnd: &sensorEnd,
			},
			want: scheduler.DelayPhases{
				Gate:    scheduler.DelayGateExternalHost,
				Handoff: seconds(2 * 60),
				Total:   14 * minute,
			},
		},
		"missing timestamps are skipped": {
			input: scheduler.DelayInput{Gate: scheduler.DelayGateNone},
			skip:  scheduler.DelaySkipMissingTime,
		},
		"upstream gate without gate end is skipped": {
			input: scheduler.DelayInput{
				ScheduledAt: scheduledAt,
				TaskStart:   taskStart,
				Gate:        scheduler.DelayGateUpstream,
			},
			skip: scheduler.DelaySkipMissingGateEnd,
		},
		"task start before scheduled_at is skipped": {
			input: scheduler.DelayInput{
				ScheduledAt: scheduledAt,
				TaskStart:   scheduledAt.Add(-time.Minute),
				Gate:        scheduler.DelayGateNone,
			},
			skip: scheduler.DelaySkipNegativePhase,
		},
		"sensor success before the gate is skipped": {
			input: scheduler.DelayInput{
				ScheduledAt:   scheduledAt,
				TaskStart:     taskStart,
				Gate:          scheduler.DelayGateUpstream,
				GateEnd:       &lateGateEnd,
				LastSensorEnd: &sensorEnd,
			},
			skip: scheduler.DelaySkipNegativePhase,
		},
		"sensor end after task start without queued is skipped": {
			input: scheduler.DelayInput{
				ScheduledAt:   scheduledAt,
				TaskStart:     taskStart,
				Gate:          scheduler.DelayGateNone,
				LastSensorEnd: &lateSensor,
			},
			skip: scheduler.DelaySkipNegativePhase,
		},
		"queued before sensor end is skipped": {
			input: scheduler.DelayInput{
				ScheduledAt:   scheduledAt,
				TaskStart:     taskStart,
				Gate:          scheduler.DelayGateUpstream,
				GateEnd:       &gateEnd,
				LastSensorEnd: &sensorEnd,
				Queued:        &earlyQueued,
			},
			skip: scheduler.DelaySkipBadQueueTime,
		},
		"queued after task start is skipped": {
			input: scheduler.DelayInput{
				ScheduledAt:   scheduledAt,
				TaskStart:     taskStart,
				Gate:          scheduler.DelayGateUpstream,
				GateEnd:       &gateEnd,
				LastSensorEnd: &sensorEnd,
				Queued:        &lateQueued,
			},
			skip: scheduler.DelaySkipBadQueueTime,
		},
		"zero-valued optional times count as absent": {
			input: scheduler.DelayInput{
				ScheduledAt:   scheduledAt,
				TaskStart:     taskStart,
				Gate:          scheduler.DelayGateNone,
				LastSensorEnd: &zeroTime,
				Queued:        &zeroTime,
			},
			want: scheduler.DelayPhases{
				Gate:    scheduler.DelayGateNone,
				Handoff: seconds(14 * 60),
				Total:   14 * minute,
			},
		},
		"zero gate end on an upstream gate counts as scheduled_at gate": {
			input: scheduler.DelayInput{
				ScheduledAt: scheduledAt,
				TaskStart:   taskStart,
				Gate:        scheduler.DelayGateUpstream,
				GateEnd:     &zeroTime,
			},
			want: scheduler.DelayPhases{
				Gate:    scheduler.DelayGateUpstream,
				Handoff: seconds(14 * 60),
				Total:   14 * minute,
			},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			phases, skip := scheduler.ComputeSchedulerDelayPhases(tc.input)
			assert.Equal(t, tc.skip, skip)
			if tc.skip != scheduler.DelaySkipOK {
				return
			}
			assert.Equal(t, tc.want.Gate, phases.Gate)
			assert.Equal(t, tc.want.Total, phases.Total)

			optional := func(name string, want, got *time.Duration) {
				if want == nil {
					assert.Nil(t, got, name)
					return
				}
				if assert.NotNil(t, got, name) {
					assert.Equal(t, *want, *got, name)
				}
			}
			optional("detection", tc.want.Detection, phases.Detection)
			optional("scheduling", tc.want.Scheduling, phases.Scheduling)
			optional("pickup", tc.want.Pickup, phases.Pickup)
			optional("handoff", tc.want.Handoff, phases.Handoff)
		})
	}
}

func TestSchedulerDelaySkipValuesAreStable(t *testing.T) {
	// These strings become Prometheus label values; renaming them breaks
	// dashboards and alerts. If a rename is truly needed, add a new value and
	// stop emitting the old one instead.
	assert.Equal(t, "", string(scheduler.DelaySkipOK))
	assert.Equal(t, "missing_timestamp", string(scheduler.DelaySkipMissingTime))
	assert.Equal(t, "missing_gate_end", string(scheduler.DelaySkipMissingGateEnd))
	assert.Equal(t, "negative_phase", string(scheduler.DelaySkipNegativePhase))
	assert.Equal(t, "bad_queue_time", string(scheduler.DelaySkipBadQueueTime))
}
