package service_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/goto/salt/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/goto/optimus/config"
	"github.com/goto/optimus/core/scheduler"
	"github.com/goto/optimus/core/scheduler/service"
	"github.com/goto/optimus/core/tenant"
	"github.com/goto/optimus/internal/errors"
)

func TestRunAttributionService(t *testing.T) {
	logger := log.NewNoop()
	ctx := context.Background()
	tnnt, _ := tenant.NewTenant("test-proj", "test-ns")
	jobName := scheduler.JobName("a-job")
	jobRunID := uuid.New()
	scheduledAt := time.Date(2026, 7, 20, 12, 0, 0, 0, time.UTC)
	startTime := time.Date(2026, 7, 20, 13, 0, 0, 0, time.UTC)

	baseInput := func() service.AttributionInput {
		return service.AttributionInput{
			Tenant:       tnnt,
			JobName:      jobName,
			OperatorName: "a-job",
			OperatorType: scheduler.OperatorTask,
			JobRunID:     jobRunID,
			ScheduledAt:  scheduledAt,
			Attempt:      1,
			StartTime:    startTime,
		}
	}

	// Retry count of 1 so a failing test fails fast rather than backing off three times.
	conf := config.RunAttributionConfig{
		AuditResolutionEnabled: true,
		MaxConcurrentResolves:  4,
		ResolveRetryMax:        1,
		ResolveTimeoutSeconds:  5,
		AuditLookbackMinutes:   30,
		ServiceAccountOwners:   []string{"optimus"},
	}

	t.Run("Classify", func(t *testing.T) {
		t.Run("attributes an optimus custom backfill from the dag run id", func(t *testing.T) {
			backfillID := uuid.New()
			backfillRepo := new(mockBackfillAttributionGetter)
			backfillRepo.On("GetBackfillDetails", ctx, backfillID).Return(backfillWithUser("alice"), nil)
			defer backfillRepo.AssertExpectations(t)

			svc := service.NewRunAttributionService(logger, conf, new(mockRunAttributionRepository),
				noReplayMatch(), backfillRepo, new(mockAuditLogGetter))

			in := baseInput()
			in.SchedulerRunID = fmt.Sprintf("custom-backfill_%s__2026-07-20T13:00:00+00:00", backfillID)

			got := svc.Classify(ctx, in)
			assert.Equal(t, scheduler.RunTypeBackfill, got.RunType)
			assert.Equal(t, "alice", got.TriggeredBy)
			assert.Equal(t, scheduler.AttributionOptimusBackfill, got.Attribution)
			assert.Equal(t, backfillID, *got.BackfillID)
		})

		t.Run("does not blame the backfill requester when somebody re-runs their backfill", func(t *testing.T) {
			// The dag run id keeps naming the backfill forever, but Optimus never clears a backfill
			// run, so a further attempt that is not a scheduler retry was started by a person. The
			// link to the backfill is kept; the requester's name is not reused as the actor.
			backfillID := uuid.New()
			backfillRepo := new(mockBackfillAttributionGetter)
			backfillRepo.On("GetBackfillDetails", ctx, backfillID).Return(backfillWithUser("alice"), nil)

			svc := service.NewRunAttributionService(logger, conf, new(mockRunAttributionRepository),
				noReplayMatch(), backfillRepo, new(mockAuditLogGetter))

			in := baseInput()
			in.SchedulerRunID = fmt.Sprintf("custom-backfill_%s__2026-07-20T13:00:00+00:00", backfillID)
			in.Attempt = 2
			in.PreviousRun = &scheduler.OperatorRun{
				Status:      scheduler.StateSuccess,
				RunType:     scheduler.RunTypeBackfill,
				TriggeredBy: "alice",
			}

			got := svc.Classify(ctx, in)
			assert.Equal(t, scheduler.RunTypeManual, got.RunType)
			assert.NotEqual(t, "alice", got.TriggeredBy, "the backfill requester must not be blamed for someone else's clear")
			assert.Equal(t, scheduler.TriggeredByUnidentified, got.TriggeredBy)
			assert.True(t, got.NeedsAuditResolution(), "the real actor has to come from the audit log")
			// Linkage survives: the run still exists because of that backfill.
			require.NotNil(t, got.BackfillID)
			assert.Equal(t, backfillID, *got.BackfillID)
		})

		t.Run("still attributes a scheduler retry of a backfill run to the requester", func(t *testing.T) {
			// The counterpart to the above: a retry really is a continuation of the backfill.
			backfillID := uuid.New()
			repo := new(mockRunAttributionRepository)
			repo.On("GetTriggerSourceByOperatorRunID", ctx, mock.Anything).Return(&scheduler.TriggerSource{
				Attribution: scheduler.RunAttribution{
					SourceType: scheduler.SourceTypeBackfill,
					BackfillID: &backfillID,
				},
			}, nil)

			svc := service.NewRunAttributionService(logger, conf, repo, noReplayMatch(),
				new(mockBackfillAttributionGetter), new(mockAuditLogGetter))

			in := baseInput()
			in.SchedulerRunID = fmt.Sprintf("custom-backfill_%s__2026-07-20T13:00:00+00:00", backfillID)
			in.PreviousRun = &scheduler.OperatorRun{
				ID: uuid.New(), Status: scheduler.StateRetry,
				RunType: scheduler.RunTypeBackfill, TriggeredBy: "alice",
			}

			got := svc.Classify(ctx, in)
			assert.Equal(t, scheduler.RunTypeBackfill, got.RunType)
			assert.Equal(t, "alice", got.TriggeredBy)
			assert.Equal(t, scheduler.AttributionInherited, got.Attribution)
			// Inheritance must carry the link across, not just the run type and actor.
			require.NotNil(t, got.BackfillID)
			assert.Equal(t, backfillID, *got.BackfillID)
			assert.Equal(t, scheduler.SourceTypeBackfill, got.SourceType)
		})

		t.Run("does not blame the replay requester once their replay has finished", func(t *testing.T) {
			// The replay lookup only matches non-terminal replays, so a later manual clear of a run
			// the replay produced falls through to the audit log rather than reusing their name.
			svc := newClassifyOnlyService(logger, conf) // no in-flight replay matches

			in := baseInput()
			in.SchedulerRunID = "scheduled__2026-07-20T12:00:00+00:00"
			in.PreviousRun = &scheduler.OperatorRun{
				Status:      scheduler.StateSuccess,
				RunType:     scheduler.RunTypeReplay,
				TriggeredBy: "bob",
			}

			got := svc.Classify(ctx, in)
			assert.Equal(t, scheduler.RunTypeManual, got.RunType)
			assert.NotEqual(t, "bob", got.TriggeredBy)
			assert.True(t, got.NeedsAuditResolution())
		})

		t.Run("attributes a replay even when the dag run id looks scheduled", func(t *testing.T) {
			// A replay of an existing run clears it in place, so Airflow's run id keeps its
			// scheduled prefix. This is the case that must not be mistaken for a manual clear.
			replayID := uuid.New()
			replayRepo := new(mockReplayAttributionGetter)
			replayRepo.On("GetReplayAttributionByScheduledAt", ctx, tnnt, jobName, scheduledAt).
				Return(replayID, "bob", nil)
			defer replayRepo.AssertExpectations(t)

			svc := service.NewRunAttributionService(logger, conf, new(mockRunAttributionRepository),
				replayRepo, new(mockBackfillAttributionGetter), new(mockAuditLogGetter))

			in := baseInput()
			in.SchedulerRunID = "scheduled__2026-07-20T12:00:00+00:00"
			// A finished previous attempt would otherwise read as a manual clear.
			in.PreviousRun = &scheduler.OperatorRun{Status: scheduler.StateFailed}

			got := svc.Classify(ctx, in)
			assert.Equal(t, scheduler.RunTypeReplay, got.RunType)
			assert.Equal(t, "bob", got.TriggeredBy)
			assert.Equal(t, scheduler.AttributionOptimusReplay, got.Attribution)
			assert.Equal(t, replayID, *got.ReplayID)
		})

		t.Run("marks an airflow manual trigger for audit resolution", func(t *testing.T) {
			svc := newClassifyOnlyService(logger, conf)

			in := baseInput()
			in.SchedulerRunID = "manual__2026-07-20T13:00:00+00:00"

			got := svc.Classify(ctx, in)
			assert.Equal(t, scheduler.RunTypeManual, got.RunType)
			assert.Equal(t, scheduler.TriggeredByUnidentified, got.TriggeredBy)
			assert.True(t, got.NeedsAuditResolution())
		})

		t.Run("does not treat a single underscore manual prefix as a manual run", func(t *testing.T) {
			// Airflow generates manual__<iso>; a single underscore is not a run it produces.
			svc := newClassifyOnlyService(logger, conf)

			in := baseInput()
			in.SchedulerRunID = "manual_2026-07-20T13:00:00+00:00"

			assert.Equal(t, scheduler.RunTypeScheduled, svc.Classify(ctx, in).RunType)
		})

		t.Run("inherits from the previous attempt when the scheduler retried it", func(t *testing.T) {
			svc := newClassifyOnlyService(logger, conf)

			in := baseInput()
			in.SchedulerRunID = "scheduled__2026-07-20T12:00:00+00:00"
			in.Attempt = 3
			in.PreviousRun = &scheduler.OperatorRun{
				Status:      scheduler.StateRetry,
				RunType:     scheduler.RunTypeManual,
				TriggeredBy: "carol",
			}

			got := svc.Classify(ctx, in)
			assert.Equal(t, scheduler.RunTypeManual, got.RunType)
			assert.Equal(t, "carol", got.TriggeredBy)
			assert.Equal(t, scheduler.AttributionInherited, got.Attribution)
			assert.False(t, got.NeedsAuditResolution())
		})

		t.Run("keeps a retry of a scheduled run scheduled", func(t *testing.T) {
			svc := newClassifyOnlyService(logger, conf)

			in := baseInput()
			in.SchedulerRunID = "scheduled__2026-07-20T12:00:00+00:00"
			in.PreviousRun = &scheduler.OperatorRun{
				Status:      scheduler.StateRetry,
				RunType:     scheduler.RunTypeScheduled,
				TriggeredBy: scheduler.TriggeredByScheduler,
			}

			got := svc.Classify(ctx, in)
			assert.Equal(t, scheduler.RunTypeScheduled, got.RunType)
			assert.True(t, got.IsScheduled())
		})

		t.Run("treats a finished previous attempt as a manual clear", func(t *testing.T) {
			for _, previousState := range []scheduler.State{scheduler.StateSuccess, scheduler.StateFailed, scheduler.StateRunning} {
				svc := newClassifyOnlyService(logger, conf)

				in := baseInput()
				in.SchedulerRunID = "scheduled__2026-07-20T12:00:00+00:00"
				in.PreviousRun = &scheduler.OperatorRun{Status: previousState}

				got := svc.Classify(ctx, in)
				assert.Equal(t, scheduler.RunTypeManual, got.RunType, "previous state %s", previousState)
				assert.True(t, got.NeedsAuditResolution(), "previous state %s", previousState)
			}
		})

		t.Run("defaults to scheduled when nothing suggests otherwise", func(t *testing.T) {
			svc := newClassifyOnlyService(logger, conf)

			in := baseInput()
			in.SchedulerRunID = "scheduled__2026-07-20T12:00:00+00:00"

			got := svc.Classify(ctx, in)
			assert.Equal(t, scheduler.RunTypeScheduled, got.RunType)
			assert.Equal(t, scheduler.TriggeredByScheduler, got.TriggeredBy)
			assert.True(t, got.IsScheduled())
		})
	})

	t.Run("Record", func(t *testing.T) {
		t.Run("records a manual run but never calls airflow when audit resolution is off", func(t *testing.T) {
			// The kill switch: attribution to Optimus's own replay and backfill keeps working, and a
			// manual run is still recorded as manual, but its actor is left unidentified rather than
			// Optimus reaching out to Airflow.
			operatorRunID := uuid.New()

			repo := new(mockRunAttributionRepository)
			repo.On("InsertTriggerSource", ctx, mock.MatchedBy(func(src *scheduler.TriggerSource) bool {
				return src.Attribution.RunType == scheduler.RunTypeManual &&
					src.Attribution.TriggeredBy == scheduler.TriggeredByUnidentified &&
					src.Attribution.Attribution == scheduler.AttributionPending
			})).Return(uuid.New(), nil)

			auditGetter := new(mockAuditLogGetter)
			defer auditGetter.AssertExpectations(t) // asserts GetEventLogs was never called

			offConf := conf
			offConf.AuditResolutionEnabled = false
			svc := service.NewRunAttributionService(logger, offConf, repo, noReplayMatch(),
				new(mockBackfillAttributionGetter), auditGetter)
			done := svc.WaitGroupForTest()

			in := baseInput()
			in.SchedulerRunID = "manual__2026-07-20T13:00:00+00:00"
			assert.NoError(t, svc.Record(ctx, in, operatorRunID, svc.Classify(ctx, in)))

			done.Wait()
			repo.AssertExpectations(t)
		})

		t.Run("still attributes an optimus backfill when audit resolution is off", func(t *testing.T) {
			backfillID := uuid.New()
			backfillRepo := new(mockBackfillAttributionGetter)
			backfillRepo.On("GetBackfillDetails", ctx, backfillID).Return(backfillWithUser("alice"), nil)

			offConf := conf
			offConf.AuditResolutionEnabled = false
			svc := service.NewRunAttributionService(logger, offConf, new(mockRunAttributionRepository),
				noReplayMatch(), backfillRepo, new(mockAuditLogGetter))

			in := baseInput()
			in.SchedulerRunID = fmt.Sprintf("custom-backfill_%s__2026-07-20T13:00:00+00:00", backfillID)

			got := svc.Classify(ctx, in)
			assert.Equal(t, scheduler.RunTypeBackfill, got.RunType)
			assert.Equal(t, "alice", got.TriggeredBy)
			assert.False(t, got.NeedsAuditResolution())
		})

		t.Run("writes nothing for a scheduled run", func(t *testing.T) {
			repo := new(mockRunAttributionRepository)
			defer repo.AssertExpectations(t)

			svc := service.NewRunAttributionService(logger, conf, repo, noReplayMatch(),
				new(mockBackfillAttributionGetter), new(mockAuditLogGetter))

			assert.NoError(t, svc.Record(ctx, baseInput(), uuid.New(), scheduler.ScheduledAttribution()))
		})
	})

	t.Run("audit resolution", func(t *testing.T) {
		t.Run("resolves the actor from an audit row naming the dag run", func(t *testing.T) {
			operatorRunID := uuid.New()
			triggerSourceID := uuid.New()

			repo := new(mockRunAttributionRepository)
			repo.On("InsertTriggerSource", mock.Anything, mock.Anything).Return(triggerSourceID, nil)
			// The operator type and run id are asserted here because stamping the resolution onto
			// task_run now happens inside this one transactional call, not a separate write.
			repo.On("UpdateTriggerSourceResolution", mock.Anything, triggerSourceID, scheduler.OperatorTask, operatorRunID,
				mock.MatchedBy(func(a scheduler.RunAttribution) bool {
					return a.TriggeredBy == "dave" && a.Attribution == scheduler.AttributionAuditRunID && a.AuditEvent == "dagrun_clear"
				}), mock.Anything).Return(nil)

			auditGetter := new(mockAuditLogGetter)
			auditGetter.On("GetEventLogs", mock.Anything, mock.MatchedBy(func(f scheduler.AuditEventFilter) bool {
				// One dag-scoped query; run_id is a discriminator applied in code, not a filter.
				return f.DagID == jobName.String() && f.RunID == ""
			})).Return([]*scheduler.AuditEvent{{
				EventLogID: 42, Event: "dagrun_clear", Owner: "dave",
				DagID: jobName.String(), RunID: "manual__2026-07-20T13:00:00+00:00",
				When: startTime.Add(-time.Minute),
			}}, nil)

			svc := service.NewRunAttributionService(logger, conf, repo, noReplayMatch(),
				new(mockBackfillAttributionGetter), auditGetter)
			done := svc.WaitGroupForTest()

			in := baseInput()
			in.SchedulerRunID = "manual__2026-07-20T13:00:00+00:00"
			assert.NoError(t, svc.Record(ctx, in, operatorRunID, svc.Classify(ctx, in)))

			done.Wait()
			repo.AssertExpectations(t)
		})

		t.Run("finishes after the caller's context is cancelled", func(t *testing.T) {
			// The resolver is detached from the request that started it. If it inherited that
			// request's cancellation it would abort the moment the event handler returned, which
			// is the single most likely way for this feature to silently stop working.
			operatorRunID := uuid.New()
			triggerSourceID := uuid.New()

			var resolved atomic.Bool
			repo := new(mockRunAttributionRepository)
			repo.On("InsertTriggerSource", mock.Anything, mock.Anything).Return(triggerSourceID, nil)
			repo.On("UpdateTriggerSourceResolution", mock.Anything, triggerSourceID, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
				Run(func(args mock.Arguments) {
					// Prove the context handed to the repository is still alive.
					callCtx, _ := args.Get(0).(context.Context)
					assert.NoError(t, callCtx.Err())
					resolved.Store(true)
				}).Return(nil)

			release := make(chan struct{})
			auditGetter := new(mockAuditLogGetter)
			auditGetter.On("GetEventLogs", mock.Anything, mock.Anything).
				Run(func(mock.Arguments) { <-release }).
				Return([]*scheduler.AuditEvent{{EventLogID: 7, Event: "clear", Owner: "erin", DagID: jobName.String(), RunID: "manual__x"}}, nil)

			svc := service.NewRunAttributionService(logger, conf, repo, noReplayMatch(),
				new(mockBackfillAttributionGetter), auditGetter)
			done := svc.WaitGroupForTest()

			callerCtx, cancel := context.WithCancel(ctx)
			in := baseInput()
			in.SchedulerRunID = "manual__2026-07-20T13:00:00+00:00"
			assert.NoError(t, svc.Record(callerCtx, in, operatorRunID, svc.Classify(callerCtx, in)))

			// Cancel as the request handler would, then let the audit call proceed.
			cancel()
			close(release)
			done.Wait()

			assert.True(t, resolved.Load(), "resolution should complete after the caller's context is cancelled")
		})

		t.Run("collapses concurrent resolutions for one dag run into a single airflow call", func(t *testing.T) {
			// Clearing a dag run restarts every task in it. Without collapsing, a wide DAG would
			// fire one audit query per task and could reach inconsistent answers.
			const taskCount = 8
			var auditCalls atomic.Int32

			var resolvedRows atomic.Int32
			repo := new(mockRunAttributionRepository)
			repo.On("InsertTriggerSource", mock.Anything, mock.Anything).Return(uuid.New(), nil)
			repo.On("UpdateTriggerSourceResolution", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.MatchedBy(func(a scheduler.RunAttribution) bool {
				return a.TriggeredBy == "frank"
			}), mock.Anything).Run(func(mock.Arguments) { resolvedRows.Add(1) }).Return(nil)

			gate := make(chan struct{})
			auditGetter := new(mockAuditLogGetter)
			auditGetter.On("GetEventLogs", mock.Anything, mock.Anything).
				Run(func(mock.Arguments) {
					auditCalls.Add(1)
					// Hold the in-flight call open so the siblings arrive while it is running.
					<-gate
				}).
				Return([]*scheduler.AuditEvent{{
					EventLogID: 9, Event: "dagrun_clear", Owner: "frank",
					DagID: jobName.String(), RunID: "manual__2026-07-20T13:00:00+00:00",
				}}, nil)

			// Deliberately one Airflow slot for eight tasks: with the semaphore acquired inside the
			// singleflight closure, the seven waiters hold nothing and all eight rows still resolve.
			// This previously required MaxConcurrentResolves == taskCount, which was the symptom.
			conf := conf
			conf.MaxConcurrentResolves = 1
			svc := service.NewRunAttributionService(logger, conf, repo, noReplayMatch(),
				new(mockBackfillAttributionGetter), auditGetter)
			done := svc.WaitGroupForTest()

			for i := range taskCount {
				in := baseInput()
				in.OperatorName = fmt.Sprintf("task-%d", i)
				in.SchedulerRunID = "manual__2026-07-20T13:00:00+00:00"
				assert.NoError(t, svc.Record(ctx, in, uuid.New(), svc.Classify(ctx, in)))
			}

			// Give the goroutines time to reach singleflight before releasing the gate.
			assert.Eventually(t, func() bool { return auditCalls.Load() == 1 }, 2*time.Second, 10*time.Millisecond)
			close(gate)
			done.Wait()

			assert.Equal(t, int32(1), auditCalls.Load(), "one dag run should mean one audit query")
			assert.Equal(t, int32(taskCount), resolvedRows.Load(),
				"every task of the cleared dag run must be resolved, not just as many as there are Airflow slots")
		})

		t.Run("correlates by time when the audit rows carry no dag context", func(t *testing.T) {
			// This is the /taskinstance/list/ bulk clear: Airflow records no dag_id or run_id.
			operatorRunID := uuid.New()
			triggerSourceID := uuid.New()

			repo := new(mockRunAttributionRepository)
			repo.On("InsertTriggerSource", mock.Anything, mock.Anything).Return(triggerSourceID, nil)
			repo.On("UpdateTriggerSourceResolution", mock.Anything, triggerSourceID, mock.Anything, mock.Anything, mock.MatchedBy(func(a scheduler.RunAttribution) bool {
				return a.TriggeredBy == "grace" && a.Attribution == scheduler.AttributionAuditHeuristic
			}), mock.Anything).Return(nil)

			auditGetter := new(mockAuditLogGetter)
			// Exact match on the dag run finds nothing.
			auditGetter.On("GetEventLogs", mock.Anything, mock.MatchedBy(func(f scheduler.AuditEventFilter) bool {
				return f.DagID != ""
			})).Return([]*scheduler.AuditEvent{}, nil)
			// The time-correlated query finds one bulk clear, plus noise that must be ignored.
			auditGetter.On("GetEventLogs", mock.Anything, mock.MatchedBy(func(f scheduler.AuditEventFilter) bool {
				return f.DagID == ""
			})).Return([]*scheduler.AuditEvent{
				{EventLogID: 11, Event: "action_clear", Owner: "grace", When: startTime.Add(-2 * time.Minute)},
				{EventLogID: 10, Event: "action_clear", Owner: "optimus", When: startTime.Add(-3 * time.Minute)},
				{EventLogID: 9, Event: "clear", Owner: "heidi", DagID: "some-other-job", RunID: "manual__z"},
			}, nil)

			svc := service.NewRunAttributionService(logger, conf, repo, noReplayMatch(),
				new(mockBackfillAttributionGetter), auditGetter)
			done := svc.WaitGroupForTest()

			// A bulk clear re-runs an attempt that already finished, which is the only situation
			// time correlation is allowed to explain.
			in := baseInput()
			in.SchedulerRunID = "scheduled__2026-07-20T12:00:00+00:00"
			in.PreviousRun = &scheduler.OperatorRun{Status: scheduler.StateSuccess}
			assert.NoError(t, svc.Record(ctx, in, operatorRunID, svc.Classify(ctx, in)))

			done.Wait()
			repo.AssertExpectations(t)
		})

		t.Run("leaves the actor unidentified when several could be responsible", func(t *testing.T) {
			operatorRunID := uuid.New()
			triggerSourceID := uuid.New()

			repo := new(mockRunAttributionRepository)
			repo.On("InsertTriggerSource", mock.Anything, mock.Anything).Return(triggerSourceID, nil)
			repo.On("UpdateTriggerSourceResolution", mock.Anything, triggerSourceID, mock.Anything, mock.Anything, mock.MatchedBy(func(a scheduler.RunAttribution) bool {
				// The candidate rows are still recorded so a human can adjudicate.
				return a.TriggeredBy == scheduler.TriggeredByUnidentified &&
					a.Attribution == scheduler.AttributionUnidentified &&
					a.AuditExtra != ""
			}), mock.Anything).Return(nil)

			auditGetter := new(mockAuditLogGetter)
			auditGetter.On("GetEventLogs", mock.Anything, mock.MatchedBy(func(f scheduler.AuditEventFilter) bool {
				return f.DagID != ""
			})).Return([]*scheduler.AuditEvent{}, nil)
			auditGetter.On("GetEventLogs", mock.Anything, mock.MatchedBy(func(f scheduler.AuditEventFilter) bool {
				return f.DagID == ""
			})).Return([]*scheduler.AuditEvent{
				{EventLogID: 21, Event: "action_clear", Owner: "ivan", When: startTime.Add(-time.Minute)},
				{EventLogID: 20, Event: "action_clear", Owner: "judy", When: startTime.Add(-2 * time.Minute)},
			}, nil)

			svc := service.NewRunAttributionService(logger, conf, repo, noReplayMatch(),
				new(mockBackfillAttributionGetter), auditGetter)
			done := svc.WaitGroupForTest()

			in := baseInput()
			in.SchedulerRunID = "scheduled__2026-07-20T12:00:00+00:00"
			in.PreviousRun = &scheduler.OperatorRun{Status: scheduler.StateSuccess}
			assert.NoError(t, svc.Record(ctx, in, operatorRunID, svc.Classify(ctx, in)))

			done.Wait()
			repo.AssertExpectations(t)
		})

		t.Run("never blames a bulk clear for a manual trigger it cannot explain", func(t *testing.T) {
			// A manual__ dag run came from the Trigger DAG view, which always records dag_id. So if
			// the exact query misses — audit lag, log retention, a trigger older than the window —
			// time correlation cannot legitimately explain it, and must not name whoever happened to
			// bulk-clear an unrelated DAG in the same window.
			operatorRunID := uuid.New()
			triggerSourceID := uuid.New()

			repo := new(mockRunAttributionRepository)
			repo.On("InsertTriggerSource", mock.Anything, mock.Anything).Return(triggerSourceID, nil)
			repo.On("UpdateTriggerSourceResolution", mock.Anything, triggerSourceID, mock.Anything, mock.Anything,
				mock.MatchedBy(func(a scheduler.RunAttribution) bool {
					return a.TriggeredBy == scheduler.TriggeredByUnidentified &&
						a.Attribution == scheduler.AttributionUnidentified
				}), mock.Anything).Return(nil)

			auditGetter := new(mockAuditLogGetter)
			// The exact query finds nothing for this dag run...
			auditGetter.On("GetEventLogs", mock.Anything, mock.MatchedBy(func(f scheduler.AuditEventFilter) bool {
				return f.DagID != ""
			})).Return([]*scheduler.AuditEvent{}, nil)
			// ...and the time-correlated query must never be issued at all. An expectation is set so
			// that if it is, the returned actor would be "mallory" and the assertion above fails.
			auditGetter.On("GetEventLogs", mock.Anything, mock.MatchedBy(func(f scheduler.AuditEventFilter) bool {
				return f.DagID == ""
			})).Return([]*scheduler.AuditEvent{
				{EventLogID: 99, Event: "action_clear", Owner: "mallory", When: startTime.Add(-time.Minute)},
			}, nil).Maybe()

			svc := service.NewRunAttributionService(logger, conf, repo, noReplayMatch(),
				new(mockBackfillAttributionGetter), auditGetter)
			done := svc.WaitGroupForTest()

			in := baseInput()
			in.SchedulerRunID = "manual__2026-07-20T13:00:00+00:00" // first attempt, no PreviousRun
			assert.NoError(t, svc.Record(ctx, in, operatorRunID, svc.Classify(ctx, in)))

			done.Wait()
			repo.AssertExpectations(t)
			auditGetter.AssertNotCalled(t, "GetEventLogs", mock.Anything, mock.MatchedBy(func(f scheduler.AuditEventFilter) bool {
				return f.DagID == ""
			}))
		})

		t.Run("attributes a plain trigger whose audit row carries no run id", func(t *testing.T) {
			// The Trigger DAG form's Run Id field is optional, so the common case logs dag_id with
			// run_id NULL. Filtering the query on run_id dropped exactly these rows, which left the
			// most frequent manual action unattributed.
			operatorRunID := uuid.New()
			triggerSourceID := uuid.New()

			repo := new(mockRunAttributionRepository)
			repo.On("InsertTriggerSource", mock.Anything, mock.Anything).Return(triggerSourceID, nil)
			repo.On("UpdateTriggerSourceResolution", mock.Anything, triggerSourceID, mock.Anything, mock.Anything,
				mock.MatchedBy(func(a scheduler.RunAttribution) bool {
					return a.TriggeredBy == "nina" && a.Attribution == scheduler.AttributionAuditDagID
				}), mock.Anything).Return(nil)

			auditGetter := new(mockAuditLogGetter)
			auditGetter.On("GetEventLogs", mock.Anything, mock.Anything).Return([]*scheduler.AuditEvent{
				// Names the DAG, no run id — and a row about a different run of the same DAG that
				// must be discarded rather than picked as the newest.
				{
					EventLogID: 31, Event: "trigger", Owner: "nina", DagID: jobName.String(),
					When: startTime.Add(-2 * time.Minute),
				},
				{
					EventLogID: 30, Event: "dagrun_clear", Owner: "otto", DagID: jobName.String(),
					RunID: "scheduled__2020-01-01T00:00:00+00:00", When: startTime.Add(-time.Minute),
				},
			}, nil)

			svc := service.NewRunAttributionService(logger, conf, repo, noReplayMatch(),
				new(mockBackfillAttributionGetter), auditGetter)
			done := svc.WaitGroupForTest()

			in := baseInput()
			in.SchedulerRunID = "manual__2026-07-20T13:00:00+00:00"
			assert.NoError(t, svc.Record(ctx, in, operatorRunID, svc.Classify(ctx, in)))

			done.Wait()
			repo.AssertExpectations(t)
		})

		t.Run("ignores a run-agnostic row that is older than the correlation window", func(t *testing.T) {
			// A row naming the DAG but not the run is only tied to this attempt by timing, so an old
			// one must not be accepted the way an exact run_id match is.
			operatorRunID := uuid.New()
			triggerSourceID := uuid.New()

			repo := new(mockRunAttributionRepository)
			repo.On("InsertTriggerSource", mock.Anything, mock.Anything).Return(triggerSourceID, nil)
			repo.On("UpdateTriggerSourceResolution", mock.Anything, triggerSourceID, mock.Anything, mock.Anything,
				mock.MatchedBy(func(a scheduler.RunAttribution) bool {
					return a.Attribution == scheduler.AttributionUnidentified
				}), mock.Anything).Return(nil)

			auditGetter := new(mockAuditLogGetter)
			auditGetter.On("GetEventLogs", mock.Anything, mock.Anything).Return([]*scheduler.AuditEvent{
				// Inside identify_lookback_hours (24h) so the query returns it, but well outside
				// audit_lookback_minutes (30m) so it cannot be tied to this attempt.
				{
					EventLogID: 41, Event: "trigger", Owner: "pete", DagID: jobName.String(),
					When: startTime.Add(-6 * time.Hour),
				},
			}, nil)

			svc := service.NewRunAttributionService(logger, conf, repo, noReplayMatch(),
				new(mockBackfillAttributionGetter), auditGetter)
			done := svc.WaitGroupForTest()

			in := baseInput()
			in.SchedulerRunID = "manual__2026-07-20T13:00:00+00:00"
			assert.NoError(t, svc.Record(ctx, in, operatorRunID, svc.Classify(ctx, in)))

			done.Wait()
			repo.AssertExpectations(t)
		})

		t.Run("still matches a trigger far older than the correlation window", func(t *testing.T) {
			// A Trigger DAG action can precede its tasks by a long queue wait. The dag-scoped query
			// is windowed generously so the row is still returned, and a row naming this exact dag
			// run is then accepted regardless of age. Narrowing to the correlation window would hide
			// it and let a weaker guess win instead.
			operatorRunID := uuid.New()
			triggerSourceID := uuid.New()

			repo := new(mockRunAttributionRepository)
			repo.On("InsertTriggerSource", mock.Anything, mock.Anything).Return(triggerSourceID, nil)
			repo.On("UpdateTriggerSourceResolution", mock.Anything, triggerSourceID, mock.Anything, mock.Anything,
				mock.MatchedBy(func(a scheduler.RunAttribution) bool {
					return a.TriggeredBy == "dave" && a.Attribution == scheduler.AttributionAuditRunID
				}), mock.Anything).Return(nil)

			var exactFilter scheduler.AuditEventFilter
			auditGetter := new(mockAuditLogGetter)
			auditGetter.On("GetEventLogs", mock.Anything, mock.Anything).
				Run(func(args mock.Arguments) { exactFilter, _ = args.Get(1).(scheduler.AuditEventFilter) }).
				Return([]*scheduler.AuditEvent{{
					EventLogID: 7, Event: "trigger", Owner: "dave",
					DagID: jobName.String(), RunID: "manual__2026-07-20T13:00:00+00:00",
					// Far older than audit_lookback_minutes (30m), inside identify_lookback_hours (24h).
					When: startTime.Add(-6 * time.Hour),
				}}, nil)

			svc := service.NewRunAttributionService(logger, conf, repo, noReplayMatch(),
				new(mockBackfillAttributionGetter), auditGetter)
			done := svc.WaitGroupForTest()

			in := baseInput()
			in.SchedulerRunID = "manual__2026-07-20T13:00:00+00:00"
			assert.NoError(t, svc.Record(ctx, in, operatorRunID, svc.Classify(ctx, in)))

			done.Wait()
			// Bounded, so the query stays indexed on the log table's timestamp, but far wider than
			// the correlation window that decides which run a run-agnostic row belongs to.
			assert.Equal(t, startTime.Add(-24*time.Hour), exactFilter.After)
			assert.Equal(t, startTime, exactFilter.Before)
			assert.Equal(t, jobName.String(), exactFilter.DagID, "dag_id is always supplied when we have one")
			assert.Empty(t, exactFilter.RunID, "run_id is a discriminator applied in code, not a filter")
			repo.AssertExpectations(t)
		})

		t.Run("records unidentified when airflow cannot be reached", func(t *testing.T) {
			operatorRunID := uuid.New()
			triggerSourceID := uuid.New()

			repo := new(mockRunAttributionRepository)
			repo.On("InsertTriggerSource", mock.Anything, mock.Anything).Return(triggerSourceID, nil)
			repo.On("UpdateTriggerSourceResolution", mock.Anything, triggerSourceID, mock.Anything, mock.Anything, mock.MatchedBy(func(a scheduler.RunAttribution) bool {
				return a.Attribution == scheduler.AttributionUnidentified && a.RunType == scheduler.RunTypeManual
			}), mock.Anything).Return(nil)

			auditGetter := new(mockAuditLogGetter)
			auditGetter.On("GetEventLogs", mock.Anything, mock.Anything).Return(nil, fmt.Errorf("airflow is down"))

			svc := service.NewRunAttributionService(logger, conf, repo, noReplayMatch(),
				new(mockBackfillAttributionGetter), auditGetter)
			done := svc.WaitGroupForTest()

			in := baseInput()
			in.SchedulerRunID = "manual__2026-07-20T13:00:00+00:00"
			assert.NoError(t, svc.Record(ctx, in, operatorRunID, svc.Classify(ctx, in)))

			done.Wait()
			repo.AssertExpectations(t)
		})

		t.Run("sheds resolution without blocking the caller when saturated", func(t *testing.T) {
			// Event ingestion must never wait on Airflow. When the resolver is full the run stays
			// recorded as manual with an unresolved actor.
			repo := new(mockRunAttributionRepository)
			repo.On("InsertTriggerSource", mock.Anything, mock.Anything).Return(uuid.New(), nil)
			repo.On("UpdateTriggerSourceResolution", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()

			const occupyingRunID = "manual__2026-07-20T13:00:00+00:00"
			const shedRunID = "manual__2026-07-20T14:00:00+00:00"

			var (
				mu        sync.Mutex
				callCount int
				block     = make(chan struct{})
			)
			auditGetter := new(mockAuditLogGetter)
			auditGetter.On("GetEventLogs", mock.Anything, mock.Anything).
				Run(func(mock.Arguments) {
					mu.Lock()
					callCount++
					mu.Unlock()
					<-block
				}).
				Return([]*scheduler.AuditEvent{}, nil)
			calls := func() int {
				mu.Lock()
				defer mu.Unlock()
				return callCount
			}

			// One resolver goroutine allowed, so the second Record must shed rather than wait.
			conf := conf
			conf.MaxPendingResolves = 1
			svc := service.NewRunAttributionService(logger, conf, repo, noReplayMatch(),
				new(mockBackfillAttributionGetter), auditGetter)
			done := svc.WaitGroupForTest()

			// Occupy the single slot. A distinct dag run keeps singleflight from merging the two.
			first := baseInput()
			first.SchedulerRunID = occupyingRunID
			assert.NoError(t, svc.Record(ctx, first, uuid.New(), svc.Classify(ctx, first)))
			assert.Eventually(t, func() bool { return calls() == 1 }, 2*time.Second, 10*time.Millisecond)

			// This one must return immediately rather than waiting for the slot.
			returned := make(chan struct{})
			go func() {
				second := baseInput()
				second.SchedulerRunID = shedRunID
				assert.NoError(t, svc.Record(ctx, second, uuid.New(), svc.Classify(ctx, second)))
				close(returned)
			}()

			select {
			case <-returned:
			case <-time.After(2 * time.Second):
				t.Fatal("Record blocked while the resolver was saturated")
			}

			close(block)
			done.Wait()
			// The two Records use different dag run ids, so singleflight would not merge them: a
			// second query would mean the second resolution ran instead of being shed.
			assert.Equal(t, 1, calls(), "the shed resolution should not have queried airflow")
		})

		t.Run("survives a panic in the resolver", func(t *testing.T) {
			// This runs once per manual task start; an unrecovered panic would take the server down.
			repo := new(mockRunAttributionRepository)
			repo.On("InsertTriggerSource", mock.Anything, mock.Anything).Return(uuid.New(), nil)

			auditGetter := new(mockAuditLogGetter)
			auditGetter.On("GetEventLogs", mock.Anything, mock.Anything).
				Run(func(mock.Arguments) { panic("boom") }).
				Return(nil, nil)

			svc := service.NewRunAttributionService(logger, conf, repo, noReplayMatch(),
				new(mockBackfillAttributionGetter), auditGetter)
			done := svc.WaitGroupForTest()

			in := baseInput()
			in.SchedulerRunID = "manual__2026-07-20T13:00:00+00:00"
			assert.NoError(t, svc.Record(ctx, in, uuid.New(), svc.Classify(ctx, in)))

			done.Wait()
		})
	})
}

func newClassifyOnlyService(logger log.Logger, conf config.RunAttributionConfig) *service.RunAttributionService {
	// No replay or backfill matches, so Classify falls through to the run id and previous run.
	return service.NewRunAttributionService(logger, conf, new(mockRunAttributionRepository), noReplayMatch(),
		new(mockBackfillAttributionGetter), new(mockAuditLogGetter))
}

// noReplayMatch is the common case: the run's scheduled time falls inside no recent replay
// window, so Classify moves on to the dag run id and the previous attempt.
func noReplayMatch() *mockReplayAttributionGetter {
	repo := new(mockReplayAttributionGetter)
	repo.On("GetReplayAttributionByScheduledAt", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(uuid.Nil, "", errors.NotFound(scheduler.EntityReplay, "no replay")).Maybe()
	return repo
}

func backfillWithUser(userID string) *scheduler.Backfill {
	cfg := scheduler.NewBackfillConfig(time.Now(), time.Now(), nil, nil, "desc", "BACKFILL", "approval", userID)
	return scheduler.NewBackfillRequest("a-job", tenant.Tenant{}, cfg, scheduler.BackfillStateCreated, "")
}

type mockRunAttributionRepository struct {
	mock.Mock
}

func (m *mockRunAttributionRepository) InsertTriggerSource(ctx context.Context, src *scheduler.TriggerSource) (uuid.UUID, error) {
	args := m.Called(ctx, src)
	if args.Get(0) == nil {
		return uuid.Nil, args.Error(1)
	}
	return args.Get(0).(uuid.UUID), args.Error(1)
}

// GetTriggerSourceByOperatorRunID is only consulted on the scheduler-retry path, so tests that
// do not exercise inheritance are not obliged to expect it.
func (m *mockRunAttributionRepository) GetTriggerSourceByOperatorRunID(ctx context.Context, operatorRunID uuid.UUID) (*scheduler.TriggerSource, error) {
	for _, expected := range m.ExpectedCalls {
		if expected.Method == "GetTriggerSourceByOperatorRunID" {
			args := m.Called(ctx, operatorRunID)
			if args.Get(0) == nil {
				return nil, args.Error(1)
			}
			return args.Get(0).(*scheduler.TriggerSource), args.Error(1)
		}
	}
	return nil, errors.NotFound(scheduler.EntityRunAttribution, "no trigger source")
}

func (m *mockRunAttributionRepository) UpdateTriggerSourceResolution(ctx context.Context, triggerSourceID uuid.UUID,
	operatorType scheduler.OperatorType, operatorRunID uuid.UUID, attribution scheduler.RunAttribution, resolveAttempts int,
) error {
	return m.Called(ctx, triggerSourceID, operatorType, operatorRunID, attribution, resolveAttempts).Error(0)
}

type mockReplayAttributionGetter struct {
	mock.Mock
}

func (m *mockReplayAttributionGetter) GetReplayAttributionByScheduledAt(ctx context.Context, jobTenant tenant.Tenant, jobName scheduler.JobName, scheduledAt time.Time) (uuid.UUID, string, error) {
	args := m.Called(ctx, jobTenant, jobName, scheduledAt)
	return args.Get(0).(uuid.UUID), args.String(1), args.Error(2)
}

type mockBackfillAttributionGetter struct {
	mock.Mock
}

func (m *mockBackfillAttributionGetter) GetBackfillDetails(ctx context.Context, backfillID uuid.UUID) (*scheduler.Backfill, error) {
	args := m.Called(ctx, backfillID)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*scheduler.Backfill), args.Error(1)
}

type mockAuditLogGetter struct {
	mock.Mock
}

func (m *mockAuditLogGetter) GetEventLogs(ctx context.Context, filter scheduler.AuditEventFilter) ([]*scheduler.AuditEvent, error) {
	args := m.Called(ctx, filter)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]*scheduler.AuditEvent), args.Error(1)
}
