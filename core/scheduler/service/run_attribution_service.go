package service

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/goto/salt/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"golang.org/x/sync/singleflight"

	"github.com/goto/optimus/config"
	"github.com/goto/optimus/core/scheduler"
	"github.com/goto/optimus/core/tenant"
	"github.com/goto/optimus/internal/errors"
	"github.com/goto/optimus/internal/utils"
)

// Airflow 2.9 audit event names. Not configurable: these are properties of the Airflow version
// being talked to, established by reading its source, and a wrong value would silently yield
// unattributed runs rather than an error.
//
// exactMatchEvents identify a specific dag run, because Airflow populates the audit row's
// dag_id and run_id from the request's form or query parameters. heuristicEvents are what the
// /dagrun/list/ and /taskinstance/list/ bulk actions emit; those requests carry only opaque row
// ids, so Airflow records no dag_id or run_id and the rows can be correlated by time alone.
var (
	exactMatchEvents = []string{
		"clear",        // grid page, clear a task instance
		"dagrun_clear", // grid page, clear a whole dag run
		"trigger",      // Trigger DAG button
		"api.clear_dag_run",
		"api.post_clear_task_instances",
		"api.post_dag_run",
		"api.update_dag_run_state",
		"api.patch_task_instance",
	}

	heuristicEvents = []string{
		"action_clear",
		"action_clear_downstream",
		"action_set_failed",
		"action_set_success",
		"action_set_retry",
		"action_set_queued",
		"action_set_running",
	}
)

// Fallbacks for when this service is constructed with a zero config, e.g. in tests. Normal
// operation gets these from the config loader's struct tag defaults.
const (
	defaultResolveTimeout = 30 * time.Second
	defaultAuditLookback  = 30 * time.Minute
	defaultAuditPageLimit = 100
)

var runAttributionMetric = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "jobrun_attribution_total",
	Help: "Operator runs classified by why they ran and how that was decided",
}, []string{"run_type", "attribution"})

// AuditLogGetter reads Airflow's record of user actions. Deliberately narrow rather than
// another method on the already broad Scheduler interface.
type AuditLogGetter interface {
	GetEventLogs(ctx context.Context, filter scheduler.AuditEventFilter) ([]*scheduler.AuditEvent, error)
}

// RunAttributionRepository persists why an operator run happened. UpdateTriggerSourceResolution
// writes both the trigger source row and the operator run row, atomically — task_run and hook_run
// are what most consumers read, so a partial write would leave them disagreeing with the trigger
// source and nothing reconciles them afterwards.
type RunAttributionRepository interface {
	InsertTriggerSource(ctx context.Context, src *scheduler.TriggerSource) (uuid.UUID, error)
	UpdateTriggerSourceResolution(ctx context.Context, triggerSourceID uuid.UUID, operatorType scheduler.OperatorType,
		operatorRunID uuid.UUID, attribution scheduler.RunAttribution, resolveAttempts int) error
}

type ReplayAttributionGetter interface {
	GetReplayAttributionByScheduledAt(ctx context.Context, jobTenant tenant.Tenant, jobName scheduler.JobName, scheduledAt time.Time) (uuid.UUID, string, error)
}

type BackfillAttributionGetter interface {
	GetBackfillDetails(ctx context.Context, backfillID uuid.UUID) (*scheduler.Backfill, error)
}

// AttributionInput is everything known about an operator run at the moment it starts.
type AttributionInput struct {
	Tenant         tenant.Tenant
	JobName        scheduler.JobName
	OperatorName   string
	OperatorType   scheduler.OperatorType
	JobRunID       uuid.UUID
	ScheduledAt    time.Time
	SchedulerRunID string
	Attempt        int
	StartTime      time.Time

	// PreviousRun is the most recent earlier attempt of this same operator, if any. Its status
	// is the only reliable way to tell a scheduler retry from a manual clear: Airflow fires
	// on_retry_callback for its own retries and not for a clear, so a predecessor left in
	// 'retried' means the scheduler is responsible for this attempt.
	PreviousRun *scheduler.OperatorRun
}

type RunAttributionService struct {
	l    log.Logger
	conf config.RunAttributionConfig

	repo         RunAttributionRepository
	replayRepo   ReplayAttributionGetter
	backfillRepo BackfillAttributionGetter
	auditGetter  AuditLogGetter

	// sem caps concurrent resolutions. A full semaphore sheds the resolution rather than
	// queueing it, so a slow Airflow cannot pile up goroutines on the event ingestion path.
	sem   chan struct{}
	group singleflight.Group

	serviceAccounts map[string]bool

	// resolved is signalled after every detached resolution finishes. Tests join on it so
	// they never have to sleep; it is nil in production.
	resolved *sync.WaitGroup
}

func NewRunAttributionService(l log.Logger, conf config.RunAttributionConfig, repo RunAttributionRepository,
	replayRepo ReplayAttributionGetter, backfillRepo BackfillAttributionGetter, auditGetter AuditLogGetter,
) *RunAttributionService {
	serviceAccounts := map[string]bool{}
	for _, owner := range conf.ServiceAccountOwners {
		serviceAccounts[strings.ToLower(strings.TrimSpace(owner))] = true
	}

	maxConcurrent := conf.MaxConcurrentResolves
	if maxConcurrent < 1 {
		maxConcurrent = 1
	}

	return &RunAttributionService{
		l:               l,
		conf:            conf,
		repo:            repo,
		replayRepo:      replayRepo,
		backfillRepo:    backfillRepo,
		auditGetter:     auditGetter,
		sem:             make(chan struct{}, maxConcurrent),
		serviceAccounts: serviceAccounts,
	}
}

// Classify decides why a run is executing, using only local state. It never calls Airflow, so
// it is safe on the event ingestion path.
//
// Order matters. Optimus's own replay and backfill are checked first because a replay clears
// an existing dag run in place, leaving Airflow's run id reading as an ordinary scheduled run
// that would otherwise be mistaken for a manual clear.
func (s *RunAttributionService) Classify(ctx context.Context, in AttributionInput) scheduler.RunAttribution {
	// 1. Optimus custom backfill. The backfill id is embedded in the dag run id, so this is an
	// exact match with no scan. Note we do not filter the backfill by status: BackfillService
	// sets 'in progress' only after the Airflow dag run has been created, so a fast-starting
	// task can legitimately observe 'created'.
	if ok, backfillID := isRunCustomBackfillType(in.SchedulerRunID); ok {
		if attribution, err := s.attributeToBackfill(ctx, backfillID); err == nil {
			return attribution
		}
		s.l.Warn("run attribution: dag run %s names backfill %s but it could not be read", in.SchedulerRunID, backfillID)
	}

	// 2. Optimus replay. Matched on the replay's time window rather than the dag run id,
	// because a replay of an existing run does not change that run's id.
	if attribution, ok := s.attributeToReplay(ctx, in); ok {
		return attribution
	}

	// 3. Somebody used Airflow's Trigger DAG button.
	if scheduler.IsManualDagRunID(in.SchedulerRunID) {
		return pendingManualAttribution()
	}

	// 4. An earlier attempt of this operator exists.
	if in.PreviousRun != nil {
		if in.PreviousRun.Status == scheduler.StateRetry {
			// The scheduler retried it. Carry the earlier attempt's verdict forward.
			return inheritedAttribution(in.PreviousRun)
		}
		// The previous attempt had already finished, so something outside the scheduler made
		// this one happen: a task or dag run cleared in Airflow.
		return pendingManualAttribution()
	}

	// 5. Nothing suggests otherwise; the scheduler started it on its own.
	return scheduler.ScheduledAttribution()
}

// Record persists the cause of a run, and where the actor is still unknown, starts a detached
// resolution against Airflow's audit log.
//
// operatorRunID must be the row this attribution belongs to. Scheduled runs write nothing.
func (s *RunAttributionService) Record(ctx context.Context, in AttributionInput, operatorRunID uuid.UUID, attribution scheduler.RunAttribution) error {
	runAttributionMetric.WithLabelValues(attribution.RunType.String(), attribution.Attribution).Inc()

	if attribution.IsScheduled() {
		return nil
	}

	src := &scheduler.TriggerSource{
		OperatorRunID:  operatorRunID,
		OperatorType:   in.OperatorType,
		JobRunID:       in.JobRunID,
		SchedulerRunID: in.SchedulerRunID,
		Attribution:    attribution,
	}
	triggerSourceID, err := s.repo.InsertTriggerSource(ctx, src)
	if err != nil {
		return err
	}

	// With audit resolution off the row keeps its pending values, which still answers "was this a
	// manual action" even though it cannot answer "who did it".
	if attribution.NeedsAuditResolution() && s.conf.AuditResolutionEnabled {
		s.resolveDetached(ctx, in, triggerSourceID, operatorRunID)
	}
	return nil
}

func (s *RunAttributionService) attributeToBackfill(ctx context.Context, backfillID uuid.UUID) (scheduler.RunAttribution, error) {
	backfill, err := s.backfillRepo.GetBackfillDetails(ctx, backfillID)
	if err != nil {
		return scheduler.RunAttribution{}, err
	}
	id := backfillID
	return scheduler.RunAttribution{
		RunType:     scheduler.RunTypeBackfill,
		TriggeredBy: backfill.GetUserID(),
		SourceType:  scheduler.SourceTypeBackfill,
		Attribution: scheduler.AttributionOptimusBackfill,
		BackfillID:  &id,
	}, nil
}

func (s *RunAttributionService) attributeToReplay(ctx context.Context, in AttributionInput) (scheduler.RunAttribution, bool) {
	replayID, userID, err := s.replayRepo.GetReplayAttributionByScheduledAt(ctx, in.Tenant, in.JobName, in.ScheduledAt)
	if err != nil {
		if !errors.IsErrorType(err, errors.ErrNotFound) {
			s.l.Error("run attribution: unable to look up replay for job %s at %s: %s", in.JobName, in.ScheduledAt, err)
		}
		return scheduler.RunAttribution{}, false
	}
	id := replayID
	return scheduler.RunAttribution{
		RunType:     scheduler.RunTypeReplay,
		TriggeredBy: userID,
		SourceType:  scheduler.SourceTypeReplay,
		Attribution: scheduler.AttributionOptimusReplay,
		ReplayID:    &id,
	}, true
}

func inheritedAttribution(previous *scheduler.OperatorRun) scheduler.RunAttribution {
	runType := previous.RunType
	if runType == "" {
		runType = scheduler.RunTypeScheduled
	}
	triggeredBy := previous.TriggeredBy
	if triggeredBy == "" {
		triggeredBy = scheduler.TriggeredByScheduler
	}
	if runType == scheduler.RunTypeScheduled {
		// A retry of an ordinary scheduled run is still just a scheduled run; nothing to link.
		return scheduler.ScheduledAttribution()
	}
	return scheduler.RunAttribution{
		RunType:     runType,
		TriggeredBy: triggeredBy,
		SourceType:  scheduler.SourceTypeManual,
		Attribution: scheduler.AttributionInherited,
	}
}

// pendingManualAttribution marks a run as manual with the actor not yet established. If
// resolution never completes the row keeps these values, which honestly says "a human did
// this and we could not tell who".
func pendingManualAttribution() scheduler.RunAttribution {
	return scheduler.RunAttribution{
		RunType:     scheduler.RunTypeManual,
		TriggeredBy: scheduler.TriggeredByUnidentified,
		SourceType:  scheduler.SourceTypeManual,
		Attribution: scheduler.AttributionPending,
	}
}

// resolveDetached runs the Airflow audit lookup outside the caller's request.
func (s *RunAttributionService) resolveDetached(parent context.Context, in AttributionInput, triggerSourceID, operatorRunID uuid.UUID) {
	select {
	case s.sem <- struct{}{}:
	default:
		// Shed rather than block event ingestion or grow goroutines without bound. The row
		// stays as a manual run with an unresolved actor.
		s.l.Warn("run attribution: resolver saturated, leaving %s unresolved", operatorRunID)
		runAttributionMetric.WithLabelValues(scheduler.RunTypeManual.String(), "shed").Inc()
		return
	}
	if s.resolved != nil {
		s.resolved.Add(1)
	}

	go func() {
		defer func() { <-s.sem }()
		if s.resolved != nil {
			defer s.resolved.Done()
		}
		defer func() {
			// A panic here would take down the server, and this runs once per manual task start.
			if r := recover(); r != nil {
				s.l.Error("run attribution: panic while resolving %s: %v", operatorRunID, r)
			}
		}()

		// WithoutCancel keeps trace and tenant values but detaches from the request's
		// cancellation, which fires as soon as the event handler returns.
		ctx, cancel := context.WithTimeout(context.WithoutCancel(parent), s.resolveTimeout())
		defer cancel()

		s.resolve(ctx, in, triggerSourceID, operatorRunID)
	}()
}

func (s *RunAttributionService) resolve(ctx context.Context, in AttributionInput, triggerSourceID, operatorRunID uuid.UUID) {
	attribution := pendingManualAttribution()
	attempts := 0

	// One dag run means one answer, however many of its tasks were cleared at once. singleflight
	// collapses the concurrent lookups; every caller then updates only its own row.
	key := in.Tenant.ProjectName().String() + "/" + in.JobName.String() + "/" + in.SchedulerRunID
	result, err, _ := s.group.Do(key, func() (any, error) {
		var resolved scheduler.RunAttribution
		err := utils.Retry(s.l, s.retryMax(), int64(s.conf.ResolveRetryBackoffMs), func() error {
			attempts++
			var retryErr error
			resolved, retryErr = s.resolveFromAudit(ctx, in)
			return retryErr
		})
		return resolved, err
	})

	switch {
	case err != nil:
		s.l.Error("run attribution: giving up on %s after %d attempts: %s", operatorRunID, attempts, err)
		attribution.Attribution = scheduler.AttributionUnidentified
	default:
		if resolved, ok := result.(scheduler.RunAttribution); ok {
			attribution = resolved
		}
	}

	if err := s.repo.UpdateTriggerSourceResolution(ctx, triggerSourceID, in.OperatorType, operatorRunID, attribution, attempts); err != nil {
		s.l.Error("run attribution: unable to persist resolution for %s: %s", operatorRunID, err)
		return
	}
	runAttributionMetric.WithLabelValues(attribution.RunType.String(), attribution.Attribution).Inc()
}

// resolveFromAudit asks Airflow who acted. It tries an exact match on the dag run first, then
// falls back to correlating by time against the bulk actions that record no dag run at all.
func (s *RunAttributionService) resolveFromAudit(ctx context.Context, in AttributionInput) (scheduler.RunAttribution, error) {
	after, before := s.auditWindow(in.StartTime)

	if in.SchedulerRunID != "" {
		events, err := s.auditGetter.GetEventLogs(ctx, scheduler.AuditEventFilter{
			Tenant:         in.Tenant,
			DagID:          in.JobName.String(),
			RunID:          in.SchedulerRunID,
			After:          after,
			Before:         before,
			IncludedEvents: exactMatchEvents,
			Limit:          s.auditPageLimit(),
		})
		if err != nil {
			return scheduler.RunAttribution{}, err
		}
		if event := s.pickLatestActionable(events); event != nil {
			return auditAttribution(event, scheduler.AttributionAuditRunID), nil
		}
	}

	// Nothing named this dag run. The bulk clear pages log no dag_id or run_id at all, so all
	// that is left is timing.
	events, err := s.auditGetter.GetEventLogs(ctx, scheduler.AuditEventFilter{
		Tenant:         in.Tenant,
		After:          after,
		Before:         before,
		IncludedEvents: heuristicEvents,
		Limit:          s.auditPageLimit(),
	})
	if err != nil {
		return scheduler.RunAttribution{}, err
	}
	return s.correlateByTime(events), nil
}

// pickLatestActionable returns the most recent audit row that names a real actor. Events are
// requested newest first. Airflow logs the Trigger DAG form's GET as well as its POST, so the
// same action can appear twice; taking the newest and ignoring blank owners handles both.
func (s *RunAttributionService) pickLatestActionable(events []*scheduler.AuditEvent) *scheduler.AuditEvent {
	for _, event := range events {
		if event.Owner == "" || s.isServiceAccount(event.Owner) {
			continue
		}
		return event
	}
	return nil
}

// correlateByTime attributes a run from audit rows that carry no dag context. This is a guess,
// and it is recorded as one: a single unambiguous actor is accepted, anything else is left
// unidentified. The candidate rows are kept on the record either way so a human can adjudicate.
func (s *RunAttributionService) correlateByTime(events []*scheduler.AuditEvent) scheduler.RunAttribution {
	attribution := pendingManualAttribution()
	attribution.Attribution = scheduler.AttributionUnidentified

	owners := map[string]bool{}
	var candidates []*scheduler.AuditEvent
	for _, event := range events {
		// A row that does name a dag is about some other dag; had it been about ours, the
		// exact-match query above would have found it.
		if event.HasDagContext() || event.Owner == "" || s.isServiceAccount(event.Owner) {
			continue
		}
		owners[event.Owner] = true
		candidates = append(candidates, event)
	}

	if extra, err := json.Marshal(candidates); err == nil && len(candidates) > 0 {
		attribution.AuditExtra = string(extra)
	}
	if len(owners) != 1 {
		return attribution
	}

	latest := candidates[0]
	resolved := auditAttribution(latest, scheduler.AttributionAuditHeuristic)
	resolved.AuditExtra = attribution.AuditExtra
	return resolved
}

func auditAttribution(event *scheduler.AuditEvent, how string) scheduler.RunAttribution {
	eventLogID := event.EventLogID
	return scheduler.RunAttribution{
		RunType:      scheduler.RunTypeManual,
		TriggeredBy:  event.Owner,
		SourceType:   scheduler.SourceTypeManual,
		Attribution:  how,
		AuditEvent:   event.Event,
		AuditEventID: &eventLogID,
		AuditExtra:   event.Extra,
	}
}

func (s *RunAttributionService) isServiceAccount(owner string) bool {
	return s.serviceAccounts[strings.ToLower(strings.TrimSpace(owner))]
}

// auditWindow brackets the search on when the run actually started, not on now: the gap between
// a user's click and the task starting is a scheduler loop plus queue and pool wait. The action
// necessarily precedes the run it caused, so the window ends at the start time.
func (s *RunAttributionService) auditWindow(startTime time.Time) (after, before time.Time) {
	if startTime.IsZero() {
		startTime = time.Now()
	}
	lookback := time.Duration(s.conf.AuditLookbackMinutes) * time.Minute
	if lookback <= 0 {
		lookback = defaultAuditLookback
	}
	return startTime.Add(-lookback), startTime
}

func (s *RunAttributionService) resolveTimeout() time.Duration {
	if s.conf.ResolveTimeoutSeconds > 0 {
		return time.Duration(s.conf.ResolveTimeoutSeconds) * time.Second
	}
	return defaultResolveTimeout
}

func (s *RunAttributionService) retryMax() int {
	if s.conf.ResolveRetryMax > 0 {
		return s.conf.ResolveRetryMax
	}
	return 1
}

func (s *RunAttributionService) auditPageLimit() int {
	if s.conf.AuditPageLimit > 0 {
		return s.conf.AuditPageLimit
	}
	return defaultAuditPageLimit
}
