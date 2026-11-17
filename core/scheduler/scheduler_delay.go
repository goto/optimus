package scheduler

import "time"

// This file holds the pure, side-effect-free core of the scheduler delay
// decomposition. It turns a set of timestamps for one task-start sample into
// per-phase waits. It knows nothing about the database, events or metrics:
// the wiring that gathers the timestamps lives in the service layer, and the
// Prometheus recording lives next to it. Keeping the maths here means it can
// be exhaustively table-tested.
//
// The phases, on one downstream node:
//
//	scheduled_at ──▶ [ gate ] ──▶ sensor end ──▶ queued ──▶ task start
//	                detection              scheduling  pickup
//	                └──────────── handoff (when queued is unknown) ───────┘
//	                └────────────────── total ───────────────────────────┘
//
//   - detection: how long between the moment the node was allowed to start
//     (the gating upstream's DAG end, or the run's scheduled_at when there is
//     no upstream) and the moment its sensor reported success.
//   - scheduling: sensor end -> queued_dttm (Airflow moved the task into the
//     queue). This is where scheduler-loop and pool-slot waiting shows up.
//   - pickup: queued_dttm -> task start (a worker picked it up and started).
//   - handoff: sensor end -> task start, used only when queued_dttm is not
//     available, so scheduling and pickup stay lumped together instead of
//     being guessed apart.
//   - total: gate -> task start, always measurable.

// SchedulerDelayGate describes what a run was waiting on before its task could
// start. It becomes the `gate` label of the scheduler delay metric.
type DelayGate string

const (
	// DelayGateNone: no upstream, so the wait starts at scheduled_at.
	DelayGateNone DelayGate = "none"
	// DelayGateUpstream: an Optimus-managed upstream DAG, and we know
	// when its DAG-level end callback arrived (GateEnd).
	DelayGateUpstream DelayGate = "upstream"
	// DelayGateExternal: the gating upstream is a third-party sensor
	// (for example DEX). Its finish time is not visible to Optimus, so phase
	// detection cannot be measured honestly and is left out.
	DelayGateExternal DelayGate = "external"
	// DelayGateExternalHost: the gating upstream runs on another
	// Optimus host. Same limitation as DelayGateExternal today.
	DelayGateExternalHost DelayGate = "external_host"
)

// DelaySkip explains why a sample was dropped instead of measured.
// The empty value means the sample is valid. Values become the `reason` label
// of the skipped-samples counter, so keep them short and stable.
type DelaySkip string

const (
	DelaySkipOK             DelaySkip = ""
	DelaySkipMissingTime    DelaySkip = "missing_timestamp"
	DelaySkipMissingGateEnd DelaySkip = "missing_gate_end"
	DelaySkipNegativePhase  DelaySkip = "negative_phase"
	DelaySkipBadQueueTime   DelaySkip = "bad_queue_time"
)

// DelayInput carries the raw timestamps of one sample. Nil pointers
// mean "not available", which is different from a zero time.
type DelayInput struct {
	// ScheduledAt is the run's logical schedule time. Always required.
	ScheduledAt time.Time
	// TaskStart is when the task start callback arrived. Always required.
	TaskStart time.Time
	// GateEnd is when the gating upstream finished (its DAG-level end). Only
	// used when Gate is DelayGateUpstream.
	GateEnd *time.Time
	// Gate says what the run was waiting on.
	Gate DelayGate
	// LastSensorEnd is the latest end time among this run's successful
	// sensors, or nil when the run has no sensor rows.
	LastSensorEnd *time.Time
	// Queued is the task's queued_dttm from the start-event payload, or nil
	// when the payload did not carry it.
	Queued *time.Time
}

// DelayPhases holds the measured waits. Optional phases are nil when
// they cannot be measured for this sample; Total is always set.
type DelayPhases struct {
	Gate DelayGate

	// Detection is waitStart -> sensor end, where waitStart is the later of
	// scheduled_at and the gating upstream's end. Nil when there is no sensor,
	// or when the gate is external/external_host (we cannot see when those
	// finished, so waitStart would be a lie).
	Detection *time.Duration
	// Scheduling is sensor end -> queued. Nil when queued is unknown or there
	// is no sensor.
	Scheduling *time.Duration
	// Pickup is queued -> task start. Nil when queued is unknown.
	Pickup *time.Duration
	// Handoff is sensor end -> task start when queued is unknown, or
	// waitStart -> task start when the run has no sensor at all. Nil when
	// queued is known (scheduling + pickup cover that stretch instead).
	Handoff *time.Duration
	// Total is waitStart -> task start.
	Total time.Duration
}

// waitStart returns the moment this node became free to start: the gating
// upstream's DAG end when that landed after scheduled_at (mirroring the
// root-cause formula), otherwise scheduled_at itself.
func (in DelayInput) waitStart() time.Time {
	if in.Gate == DelayGateUpstream && in.GateEnd != nil && in.GateEnd.After(in.ScheduledAt) {
		return *in.GateEnd
	}
	return in.ScheduledAt
}

// ComputeSchedulerDelayPhases derives the phase durations for one sample. It
// returns a skip reason instead of an error: every rejection is a data
// condition the caller counts, not something to retry.
func ComputeSchedulerDelayPhases(in DelayInput) (DelayPhases, DelaySkip) {
	phases := DelayPhases{Gate: in.Gate}

	switch {
	case in.ScheduledAt.IsZero() || in.TaskStart.IsZero():
		return phases, DelaySkipMissingTime
	case in.Gate == DelayGateUpstream && in.GateEnd == nil:
		return phases, DelaySkipMissingGateEnd
	}

	waitStart := in.waitStart()
	phases.Total = in.TaskStart.Sub(waitStart)
	if phases.Total < 0 {
		// The task started before its own schedule: clock skew between
		// workers, or a cleared/replayed run. Not a real measurement.
		return phases, DelaySkipNegativePhase
	}

	hasSensor := in.LastSensorEnd != nil && !in.LastSensorEnd.IsZero()
	measurableGate := in.Gate == DelayGateNone || in.Gate == DelayGateUpstream

	if hasSensor {
		if measurableGate {
			detection := in.LastSensorEnd.Sub(waitStart)
			if detection < 0 {
				// The sensor reported success before the node was free --
				// a bypassed sensor or skewed clocks. Skip the whole sample:
				// a broken boundary makes every later phase meaningless.
				return phases, DelaySkipNegativePhase
			}
			phases.Detection = &detection
		}

		if in.Queued != nil && !in.Queued.IsZero() {
			switch {
			case in.Queued.Before(*in.LastSensorEnd):
				// queued cannot predate the sensor finishing.
				return phases, DelaySkipBadQueueTime
			case in.Queued.After(in.TaskStart):
				// queued cannot be in the future relative to task start.
				return phases, DelaySkipBadQueueTime
			default:
				scheduling := in.Queued.Sub(*in.LastSensorEnd)
				pickup := in.TaskStart.Sub(*in.Queued)
				phases.Scheduling = &scheduling
				phases.Pickup = &pickup
			}
		} else {
			handoff := in.TaskStart.Sub(*in.LastSensorEnd)
			if handoff < 0 {
				return phases, DelaySkipNegativePhase
			}
			phases.Handoff = &handoff
		}
	} else if measurableGate {
		// No sensor at all: the whole wait from gate to task start arrives as
		// one handoff phase, matching what the existing schedule_delay gauge
		// measures today.
		handoff := in.TaskStart.Sub(waitStart)
		phases.Handoff = &handoff
	}
	// No sensor and an external gate: nothing honest is left to emit besides
	// the total, which is still measured from scheduled_at.

	return phases, DelaySkipOK
}
