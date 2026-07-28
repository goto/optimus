package scheduler

import (
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/goto/optimus/core/tenant"
	"github.com/goto/optimus/internal/errors"
)

// EntityAirflowSync is used for error wrapping across the manual-state-override
// reconciliation feature (see docs/docs/rfcs/20260727_manual_state_override_reconciliation.md).
const EntityAirflowSync = "airflowSync"

// Airflow run_id prefixes the manual-state-override reconciler recognises. This is an
// allow-list, not a deny-list -- an unrecognised prefix is skipped and counted
// (reason="run_type"), never guessed at, so an Airflow convention we haven't seen gets
// noticed rather than mis-parsed. Shared between ext/scheduler/airflow (which filters on
// these) and this package's ExecutionDate (which parses them), so the convention lives in
// exactly one place.
//
// Originally only RunIDScheduledPrefix was in scope, out of concern that the replay/backfill
// workers might race the reconciler writing to the same job_run/task_run rows. That concern
// didn't hold up: replayRepo/backfillRepo only ever write to their own tracking tables
// (replay_request/replay_run, backfill) -- job_run/task_run/sensor_run/hook_run are updated
// exclusively via the normal Airflow callback path regardless of whether a run was
// scheduled, replayed, or backfilled. So the only race here is the same one that already
// exists for scheduled__ runs (a normal callback landing around the same time as a manual
// override), which the updated_at freshness check already covers.
const (
	RunIDScheduledPrefix      = "scheduled__"
	RunIDManualPrefix         = "manual__"
	RunIDReplayedPrefix       = "replayed__"
	RunIDCustomBackfillPrefix = "custom-backfill_"
)

var runIDPrefixes = []string{RunIDScheduledPrefix, RunIDManualPrefix, RunIDReplayedPrefix, RunIDCustomBackfillPrefix}

// HasRecognisedRunIDPrefix reports whether RunID matches one of the run_id conventions this
// reconciler knows how to parse. Callers (ext/scheduler/airflow's GetEventLogs) should skip
// and count anything that doesn't match, rather than attempt ExecutionDate on it.
func (m ManualOverrideEvent) HasRecognisedRunIDPrefix() bool {
	for _, p := range runIDPrefixes {
		if strings.HasPrefix(m.RunID, p) {
			return true
		}
	}
	return false
}

type AirflowSyncStatus string

const (
	AirflowSyncInProgress AirflowSyncStatus = "in_progress"
	AirflowSyncSuccess    AirflowSyncStatus = "success"
	AirflowSyncFailed     AirflowSyncStatus = "failed"
)

func (s AirflowSyncStatus) String() string {
	return string(s)
}

// AirflowSyncWindow is one claimed [StartTime, EndTime) slice of an Airflow project's
// audit log that a worker has processed, or is processing. The composite
// (ProjectName, StartTime, EndTime) is the claim itself: a row existing at all means some
// worker already owns or has finished that window, so callers should INSERT ... ON
// CONFLICT DO NOTHING rather than read-then-write.
type AirflowSyncWindow struct {
	ID          uuid.UUID
	ProjectName tenant.ProjectName
	StartTime   time.Time
	EndTime     time.Time

	Status       AirflowSyncStatus
	AttemptCount int
	LastError    string

	// WorkerID/LockedUntil fence a crashed worker: a window left InProgress past
	// LockedUntil is re-claimable by another replica, and only the replica whose
	// WorkerID still matches may mark it Success/Failed.
	WorkerID    uuid.UUID
	LockedUntil time.Time

	// Observability only, not required for correctness. RunsReconciled deliberately
	// separate from EventsMatched: matched>0 with reconciled=0 means resolution is
	// silently broken (unknown dag_id, croniter shift mismatch, no matching job_run, ...).
	MaxProcessedLogID int64
	EventsMatched     int
	RunsReconciled    int

	CreatedAt time.Time
	UpdatedAt time.Time
}

// ManualOverrideEvent is a single Airflow `log` row identified as a human-driven state
// change: a bare success/failed/dagrun_success/dagrun_failed event (the legacy `www` mark
// endpoints the Airflow 2.9 grid UI posts to) with the confirmed=true extra JSON.
// See the RFC for why this is sufficient to distinguish it from an ordinary worker
// transition (which never has extra populated at all) without needing owner_display_name,
// which the eventLogs REST API does not expose.
type ManualOverrideEvent struct {
	LogID int64
	Event string // one of: success, failed, dagrun_success, dagrun_failed

	DagID  string
	TaskID string // empty for dagrun_* events
	RunID  string // e.g. "scheduled__2026-07-24T18:00:00+00:00" -- see runIDPrefixes for recognised shapes

	Owner string
	When  time.Time
}

// IsDagRunLevel reports whether this event targets the whole DAG run rather than a
// single task instance.
func (m ManualOverrideEvent) IsDagRunLevel() bool {
	return m.Event == "dagrun_success" || m.Event == "dagrun_failed"
}

// ExecutionDate parses the Airflow execution_date embedded in RunID. RunID is expected to
// carry one of runIDPrefixes already -- ext/scheduler/airflow's GetEventLogs only returns
// events matching HasRecognisedRunIDPrefix -- so this only needs to strip the prefix and
// parse the remaining timestamp.
//
// custom-backfill_ is the one irregular shape: it embeds a UUID before the timestamp
// (custom-backfill_<uuid>__<timestamp>), so it splits on the *last* "__" rather than
// stripping a fixed prefix. That's safe because a UUID's hyphens never produce a literal
// "__", confirmed against a real run_id
// (custom-backfill_76515cc3-b3aa-440f-b998-04e6f4935ea3__2026-07-25T09:38:53+00:00).
func (m ManualOverrideEvent) ExecutionDate() (time.Time, error) {
	raw := m.RunID
	switch {
	case strings.HasPrefix(raw, RunIDCustomBackfillPrefix):
		idx := strings.LastIndex(raw, "__")
		if idx < 0 {
			return time.Time{}, errors.InvalidArgument(EntityAirflowSync, fmt.Sprintf("malformed custom-backfill run_id %q: no timestamp separator", raw))
		}
		raw = raw[idx+2:]
	case strings.HasPrefix(raw, RunIDScheduledPrefix):
		raw = strings.TrimPrefix(raw, RunIDScheduledPrefix)
	case strings.HasPrefix(raw, RunIDManualPrefix):
		raw = strings.TrimPrefix(raw, RunIDManualPrefix)
	case strings.HasPrefix(raw, RunIDReplayedPrefix):
		raw = strings.TrimPrefix(raw, RunIDReplayedPrefix)
	default:
		return time.Time{}, errors.InvalidArgument(EntityAirflowSync, "unrecognised run_id prefix: "+raw)
	}

	t, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		return time.Time{}, errors.InvalidArgument(EntityAirflowSync, fmt.Sprintf("unparseable execution date in run_id %q: %s", m.RunID, err))
	}
	return t, nil
}

// TargetState is the state the manual action set. For the four bare legacy-endpoint
// events this is encoded directly in the event name itself -- unlike the REST /api/v1
// endpoints (api.patch_task_instance etc., out of scope for v1), the target state there is
// not in the event name and must be parsed out of `extra["new_state"]`/`extra["state"]`.
func (m ManualOverrideEvent) TargetState() (State, error) {
	switch m.Event {
	case "success", "dagrun_success":
		return StateSuccess, nil
	case "failed", "dagrun_failed":
		return StateFailed, nil
	default:
		return "", errors.InvalidArgument(EntityAirflowSync, "unrecognized manual override event: "+m.Event)
	}
}
