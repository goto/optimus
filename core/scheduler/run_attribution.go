package scheduler

import (
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/goto/optimus/core/tenant"
)

const (
	EntityRunAttribution = "runAttribution"

	// RunTypeScheduled is a run the Airflow scheduler started off the DAG's cron.
	RunTypeScheduled RunType = "scheduled"
	// RunTypeReplay is a run created or cleared by an Optimus replay request.
	RunTypeReplay RunType = "replay"
	// RunTypeBackfill is a run created by an Optimus custom backfill request.
	RunTypeBackfill RunType = "backfill"
	// RunTypeManual is a run triggered or cleared by a human directly in Airflow.
	RunTypeManual RunType = "manual"

	// TriggeredByScheduler is the trigger recorded for ordinary cron-driven runs.
	TriggeredByScheduler = "scheduler"
	// TriggeredByUnidentified is recorded when a run is known to be manual but the actor
	// could not be established from the Airflow audit log.
	TriggeredByUnidentified = "unidentified_user"

	// SourceTypeReplay et al. identify which Optimus entity, if any, caused the run.
	SourceTypeReplay   = "replay"
	SourceTypeBackfill = "backfill"
	SourceTypeManual   = "manual"

	// AttributionOptimusBackfill: scheduler run id carried the backfill UUID. Exact.
	AttributionOptimusBackfill = "optimus_backfill"
	// AttributionOptimusReplay: scheduled_at fell inside a recent replay window. Exact.
	AttributionOptimusReplay = "optimus_replay"
	// AttributionAuditRunID: an Airflow audit row matched on both dag_id and run_id. Exact.
	AttributionAuditRunID = "airflow_audit_run_id"
	// AttributionAuditDagID: an Airflow audit row named this DAG but not which run of it, so the
	// correlation window is what ties it to this attempt. Weaker than a run_id match, far stronger
	// than a dag-less bulk action.
	AttributionAuditDagID = "airflow_audit_dag_id"
	// AttributionAuditHeuristic: correlated only by time against audit rows that carry no
	// dag_id or run_id, as produced by Airflow's bulk list-page clears. Low confidence.
	AttributionAuditHeuristic = "airflow_audit_heuristic"
	// AttributionInherited: copied from the previous attempt, which the scheduler retried.
	AttributionInherited = "inherited"
	// AttributionUnidentified: no usable signal, or several candidate actors.
	AttributionUnidentified = "unidentified"
	// AttributionPending: the resolver goroutine has not written a result. Terminal in
	// practice, since nothing rescans these.
	AttributionPending = "pending"

	// dagRunIDPrefixManual is the prefix Airflow puts on run ids it generates for runs
	// triggered outside the scheduler, e.g. from the UI's Trigger DAG button.
	// DagRunType.generate_run_id formats as "<type>__<logical date>", so the separator is
	// two underscores, not one.
	dagRunIDPrefixManual = "manual__"
)

// RunType records what caused a task or hook run to execute.
type RunType string

func (r RunType) String() string { return string(r) }

// RunAttribution is the outcome of deciding why an operator run is executing and who is
// answerable for it.
type RunAttribution struct {
	RunType     RunType
	TriggeredBy string
	SourceType  string
	Attribution string

	ReplayID   *uuid.UUID
	BackfillID *uuid.UUID

	AuditEvent   string
	AuditEventID *int64
	AuditExtra   string
}

// ScheduledAttribution is the default for a run the scheduler started on its own. No
// operator_run_trigger_source row is written for these.
func ScheduledAttribution() RunAttribution {
	return RunAttribution{
		RunType:     RunTypeScheduled,
		TriggeredBy: TriggeredByScheduler,
	}
}

// IsScheduled reports whether this attribution needs no link row.
func (r RunAttribution) IsScheduled() bool { return r.RunType == RunTypeScheduled }

// NeedsAuditResolution reports whether the actor still has to be looked up in Airflow's
// audit log.
func (r RunAttribution) NeedsAuditResolution() bool { return r.Attribution == AttributionPending }

// IsManualDagRunID reports whether the Airflow dag run id was generated for a run started
// outside the scheduler, which in practice means somebody used the Trigger DAG button.
// Optimus's own replay and backfill run ids use their own prefixes and are matched first,
// so they do not reach this check.
func IsManualDagRunID(schedulerRunID string) bool {
	return strings.HasPrefix(schedulerRunID, dagRunIDPrefixManual)
}

// TriggerSource is a persisted link from an operator run to the cause of that run.
type TriggerSource struct {
	ID uuid.UUID

	OperatorRunID  uuid.UUID
	OperatorType   OperatorType
	JobRunID       uuid.UUID
	SchedulerRunID string

	Attribution RunAttribution

	ResolveAttempts int

	CreatedAt time.Time
	UpdatedAt time.Time
}

// AuditEvent is one row of Airflow's event log, i.e. its record of a user action.
//
// dag_id, task_id and run_id are populated by Airflow only when the originating HTTP
// request carried them as query or form parameters, so they are absent for bulk actions
// taken from the /dagrun/list/ and /taskinstance/list/ pages and for REST calls that pass
// their arguments in a JSON body.
type AuditEvent struct {
	EventLogID    int64
	When          time.Time
	Event         string
	Owner         string
	DagID         string
	TaskID        string
	RunID         string
	ExecutionDate *time.Time
	Extra         string
}

// HasDagContext reports whether this audit row can be tied to a specific DAG, which
// decides whether it is usable for exact matching or only for temporal correlation.
func (a *AuditEvent) HasDagContext() bool { return a.DagID != "" }

// AuditEventFilter is a query against Airflow's event log.
//
// Empty string and zero time fields are omitted from the request. IncludedEvents and
// ExcludedEvents require Airflow 2.9.0 or later.
type AuditEventFilter struct {
	Tenant tenant.Tenant

	DagID  string
	RunID  string
	TaskID string

	After  time.Time
	Before time.Time

	IncludedEvents []string
	ExcludedEvents []string

	Limit int
}
