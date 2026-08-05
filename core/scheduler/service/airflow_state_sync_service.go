package service

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/goto/salt/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/goto/optimus/core/event"
	"github.com/goto/optimus/core/event/moderator"
	"github.com/goto/optimus/core/scheduler"
	"github.com/goto/optimus/core/tenant"
	"github.com/goto/optimus/internal/errors"
)

// AirflowEventLogFetcher is satisfied by *ext/scheduler/airflow.Scheduler. Declared here
// rather than imported, matching this package's existing convention of declaring narrow
// interfaces where they're used (see JobRepository/JobRunRepository above in
// job_run_service.go).
type AirflowEventLogFetcher interface {
	GetManualEventLogs(ctx context.Context, projectName tenant.ProjectName, after, before time.Time) ([]scheduler.ManualOverrideEvent, error)
}

// AirflowSyncJobRepository is the subset of the wider JobRepository the reconciler needs.
type AirflowSyncJobRepository interface {
	GetJobDetails(ctx context.Context, projectName tenant.ProjectName, jobName scheduler.JobName) (*scheduler.JobWithDetails, error)
}

// AirflowSyncJobRunRepository is deliberately narrower than JobRunRepository: reconciliation
// never creates a job_run row (see the RFC's write-scope rules -- v1 does not fabricate rows
// or timing for runs Optimus never saw), so only GetByScheduledAt/Update are needed.
type AirflowSyncJobRunRepository interface {
	GetByScheduledAt(ctx context.Context, tnnt tenant.Tenant, jobName scheduler.JobName, scheduledAt time.Time) (*scheduler.JobRun, error)
	Update(ctx context.Context, jobRunID uuid.UUID, endTime time.Time, status scheduler.State) error
}

// AirflowSyncOperatorRunRepository mirrors the same non-creating restriction as
// AirflowSyncJobRunRepository, plus ListLatestOperatorRunsByJobRunID which the dagrun-level
// cascade needs to see every child's current status before deciding what to touch.
type AirflowSyncOperatorRunRepository interface {
	GetOperatorRun(ctx context.Context, name string, operatorType scheduler.OperatorType, jobRunID uuid.UUID) (*scheduler.OperatorRun, error)
	ListLatestOperatorRunsByJobRunID(ctx context.Context, operatorType scheduler.OperatorType, jobRunID uuid.UUID) ([]*scheduler.OperatorRun, error)
	UpdateOperatorRun(ctx context.Context, operatorType scheduler.OperatorType, operatorRunID uuid.UUID, eventTime time.Time, state scheduler.State) error
}

var (
	airflowSyncReconciledTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "airflow_state_reconciled_total",
		Help: "run rows overwritten by the manual-state-override reconciler, by originating airflow event and the state it was set to",
	}, []string{"project", "event", "to_state"})

	// airflowSyncSkippedTotal is the load-bearing observability signal for this feature:
	// without it, "no manual overrides found" and "N found but every one was skipped" are
	// indistinguishable, which defeats the point of a feature whose job is to stop false
	// positives silently persisting. See the RFC's per-scope-decision skip reasons.
	airflowSyncSkippedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "airflow_state_reconcile_skipped_total",
		Help: "airflow manual-override audit rows the reconciler chose not to act on, by reason",
	}, []string{"project", "reason"})
)

type AirflowStateSyncService struct {
	l log.Logger

	eventLogFetcher AirflowEventLogFetcher
	jobRepo         AirflowSyncJobRepository
	jobRunRepo      AirflowSyncJobRunRepository
	operatorRunRepo AirflowSyncOperatorRunRepository
	eventHandler    EventHandler
}

func NewAirflowStateSyncService(l log.Logger, eventLogFetcher AirflowEventLogFetcher, jobRepo AirflowSyncJobRepository,
	jobRunRepo AirflowSyncJobRunRepository, operatorRunRepo AirflowSyncOperatorRunRepository, eventHandler EventHandler,
) *AirflowStateSyncService {
	return &AirflowStateSyncService{
		l:               l,
		eventLogFetcher: eventLogFetcher,
		jobRepo:         jobRepo,
		jobRunRepo:      jobRunRepo,
		operatorRunRepo: operatorRunRepo,
		eventHandler:    eventHandler,
	}
}

// ReconcileWindowResult feeds the airflow_sync_state observability columns (see the
// repository/RFC) so a gap between EventsMatched and RunsReconciled is visible without
// having to cross-reference metrics against a specific window.
type ReconcileWindowResult struct {
	MaxProcessedLogID *int64
	EventsMatched     int
	RunsReconciled    int
}

// ReconcileWindow fetches every manual override in [startTime, endTime) for the project and
// applies the ones that pass the write-scope rules in
// docs/docs/rfcs/20260727_manual_state_override_reconciliation.md. A per-event error is
// logged and skipped rather than aborting the whole window, since one bad row should not
// block every other row in the same window from being reconciled.
func (s *AirflowStateSyncService) ReconcileWindow(ctx context.Context, projectName tenant.ProjectName, startTime, endTime time.Time) (ReconcileWindowResult, error) {
	events, err := s.eventLogFetcher.GetManualEventLogs(ctx, projectName, startTime, endTime)
	if err != nil {
		return ReconcileWindowResult{}, errors.Wrap(scheduler.EntityAirflowSync, "error fetching airflow event logs", err)
	}

	result := ReconcileWindowResult{EventsMatched: len(events)}
	// Cache job details per window: several events for the same job in one window (e.g. a
	// task mark followed by a dagrun mark) should not re-fetch the job spec each time.
	jobDetailsCache := map[scheduler.JobName]*scheduler.JobWithDetails{}

	for i := range events {
		evt := events[i]
		if result.MaxProcessedLogID == nil || evt.LogID > *result.MaxProcessedLogID {
			logID := evt.LogID
			result.MaxProcessedLogID = &logID
		}

		reconciled, err := s.reconcileEvent(ctx, projectName, evt, jobDetailsCache)
		if err != nil {
			s.l.Warn("error reconciling manual override event_log_id [%d] for dag [%s]: %s", evt.LogID, evt.DagID, err)
			continue
		}
		if reconciled {
			result.RunsReconciled++
		}
	}
	return result, nil
}

func (s *AirflowStateSyncService) reconcileEvent(ctx context.Context, projectName tenant.ProjectName, evt scheduler.ManualOverrideEvent, jobDetailsCache map[scheduler.JobName]*scheduler.JobWithDetails) (bool, error) {
	skip := func(reason string) (bool, error) {
		airflowSyncSkippedTotal.WithLabelValues(projectName.String(), reason).Inc()
		return false, nil
	}

	jobName, err := scheduler.JobNameFrom(evt.DagID)
	if err != nil {
		return skip("invalid_dag_id")
	}

	jobDetails, ok := jobDetailsCache[jobName]
	if !ok {
		jobDetails, err = s.jobRepo.GetJobDetails(ctx, projectName, jobName)
		if err != nil {
			if errors.IsErrorType(err, errors.ErrNotFound) {
				return skip("unknown_job")
			}
			return false, err
		}
		jobDetailsCache[jobName] = jobDetails
	}
	if jobDetails.Schedule == nil {
		return skip("no_schedule")
	}

	executionDate, err := evt.ExecutionDate()
	if err != nil {
		return skip("unparseable_run_id")
	}
	// Mirrors __lib.py's get_scheduled_at: Airflow's execution_date is not Optimus's
	// scheduled_at, it is the *previous* tick. Skipping this shift lands every reconciled
	// row one interval off.
	scheduledAt, err := jobDetails.Schedule.GetNextSchedule(executionDate)
	if err != nil {
		return skip("schedule_shift_error")
	}

	targetState, err := evt.TargetState()
	if err != nil {
		return skip("unrecognized_event")
	}

	jobRun, err := s.jobRunRepo.GetByScheduledAt(ctx, jobDetails.Job.Tenant, jobName, scheduledAt)
	if err != nil {
		if errors.IsErrorType(err, errors.ErrNotFound) {
			// v1 does not create job_run rows -- nothing to reconcile against.
			return skip("no_job_run")
		}
		return false, err
	}

	// Never regress fresher data: a callback may have landed after the manual action.
	if !jobRun.UpdatedAt.IsZero() && jobRun.UpdatedAt.After(evt.When) {
		return skip("stale_event")
	}

	if evt.IsDagRunLevel() {
		return s.reconcileDagRun(ctx, projectName, evt, jobRun, targetState)
	}
	return s.reconcileTask(ctx, projectName, evt, jobRun, targetState)
}

// reconcileDagRun handles dagrun_success/dagrun_failed. It mirrors Airflow's own asymmetric
// cascade (airflow/api/common/mark_tasks.py, see the RFC's finding V5) rather than force-
// setting every child: set_dag_run_state_to_success sets every task instance to success,
// but set_dag_run_state_to_failed only touches non-terminal instances and leaves already-
// finished ones alone. Blindly overwriting a genuinely-successful task to failed would
// corrupt the duration/percentile history the SLA predictor depends on.
func (s *AirflowStateSyncService) reconcileDagRun(ctx context.Context, projectName tenant.ProjectName, evt scheduler.ManualOverrideEvent, jobRun *scheduler.JobRun, targetState scheduler.State) (bool, error) {
	anyChanged := false

	if jobRun.State != targetState {
		if err := s.jobRunRepo.Update(ctx, jobRun.ID, evt.When, targetState); err != nil {
			return false, err
		}
		anyChanged = true
		airflowSyncReconciledTotal.WithLabelValues(projectName.String(), evt.Event, targetState.String()).Inc()

		reconciledJobRun := *jobRun
		reconciledJobRun.State = targetState
		s.emitJobRunStateChange(&reconciledJobRun)
	}

	for _, operatorType := range []scheduler.OperatorType{scheduler.OperatorTask, scheduler.OperatorSensor, scheduler.OperatorHook} {
		operators, err := s.operatorRunRepo.ListLatestOperatorRunsByJobRunID(ctx, operatorType, jobRun.ID)
		if err != nil {
			s.l.Warn("error listing %s runs for job run [%s] dagrun cascade: %s", operatorType, jobRun.ID, err)
			continue
		}
		for _, operator := range operators {
			if operator.Status == targetState {
				continue
			}
			if targetState == scheduler.StateFailed && operator.Status.IsTerminal() {
				// leaves already-terminal tasks alone, matching set_dag_run_state_to_failed
				continue
			}
			if !operator.UpdatedAt.IsZero() && operator.UpdatedAt.After(evt.When) {
				continue
			}
			if err := s.operatorRunRepo.UpdateOperatorRun(ctx, operatorType, operator.ID, evt.When, targetState); err != nil {
				s.l.Warn("error cascading dagrun state to %s run [%s]: %s", operatorType, operator.ID, err)
				continue
			}
			anyChanged = true
			airflowSyncReconciledTotal.WithLabelValues(projectName.String(), evt.Event, targetState.String()).Inc()
		}
	}

	return anyChanged, nil
}

// reconcileTask handles the single-task success/failed events. Unlike the dagrun cascade,
// a direct task-level mark is an explicit override of that one task, so it applies
// regardless of the task's current state (subject only to the already-matches and
// freshness checks) -- there is no "leave terminal tasks alone" rule here, that rule exists
// specifically for a dagrun-level side effect touching tasks the human did not target.
func (s *AirflowStateSyncService) reconcileTask(ctx context.Context, projectName tenant.ProjectName, evt scheduler.ManualOverrideEvent, jobRun *scheduler.JobRun, targetState scheduler.State) (bool, error) {
	if evt.TaskID == "" {
		airflowSyncSkippedTotal.WithLabelValues(projectName.String(), "missing_task_id").Inc()
		return false, nil
	}
	operatorType := scheduler.OperatorTypeFromTaskID(evt.TaskID)

	operatorRun, err := s.operatorRunRepo.GetOperatorRun(ctx, evt.TaskID, operatorType, jobRun.ID)
	if err != nil {
		if errors.IsErrorType(err, errors.ErrNotFound) {
			// v1 does not fabricate a row/timing for a task Optimus never saw start.
			airflowSyncSkippedTotal.WithLabelValues(projectName.String(), "no_operator_run").Inc()
			return false, nil
		}
		return false, err
	}

	if operatorRun.Status == targetState {
		airflowSyncSkippedTotal.WithLabelValues(projectName.String(), "already_matches").Inc()
		return false, nil
	}
	if !operatorRun.UpdatedAt.IsZero() && operatorRun.UpdatedAt.After(evt.When) {
		airflowSyncSkippedTotal.WithLabelValues(projectName.String(), "stale_event").Inc()
		return false, nil
	}

	if err := s.operatorRunRepo.UpdateOperatorRun(ctx, operatorType, operatorRun.ID, evt.When, targetState); err != nil {
		return false, err
	}
	airflowSyncReconciledTotal.WithLabelValues(projectName.String(), evt.Event, targetState.String()).Inc()
	return true, nil
}

// emitJobRunStateChange mirrors JobRunService.raiseJobRunStateChangeEvent so downstream
// consumers of the job-run-state-change event stream don't desync just because this state
// change came from reconciliation instead of a live Airflow callback. This intentionally
// does not go through the gRPC handler's notifier fan-out (ext/notify/alertmanager etc.), so
// no user-facing alert is produced -- see the RFC's "Alerting" section for why that's the
// deliberate v1 behaviour, not an oversight.
func (s *AirflowStateSyncService) emitJobRunStateChange(jobRun *scheduler.JobRun) {
	var schedulerEvent moderator.Event
	var err error
	switch jobRun.State {
	case scheduler.StateSuccess:
		schedulerEvent, err = event.NewJobRunSuccessEvent(jobRun)
	case scheduler.StateFailed:
		schedulerEvent, err = event.NewJobRunFailedEvent(jobRun)
	default:
		return
	}
	if err != nil {
		s.l.Error("error creating event for reconciled job run [%s] state change: %s", jobRun.ID, err)
		return
	}
	s.eventHandler.HandleEvent(schedulerEvent)
}
