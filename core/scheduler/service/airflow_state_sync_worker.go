package service

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/goto/salt/log"

	"github.com/goto/optimus/core/scheduler"
	"github.com/goto/optimus/core/tenant"
)

// AirflowSyncProjectRepository lists every project the reconciler should sweep. In this
// deployment there is exactly one, but the worker fans out generically so other
// deployments with several projects (and therefore several independent Airflow instances,
// since SCHEDULER_HOST is per-project config) get isolated, parallel sync per project.
type AirflowSyncProjectRepository interface {
	GetAll(ctx context.Context) ([]*tenant.Project, error)
}

// AirflowSyncStateRepository backs the per-project window claim described in
// docs/docs/rfcs/20260727_manual_state_override_reconciliation.md. A claimed window row is
// the mutex -- ClaimWindow's INSERT ... ON CONFLICT DO NOTHING is what guarantees serial
// processing within a project while leaving different projects free to run in parallel.
type AirflowSyncStateRepository interface {
	GetWatermark(ctx context.Context, projectName tenant.ProjectName) (*time.Time, error)
	ClaimWindow(ctx context.Context, projectName tenant.ProjectName, startTime, endTime time.Time, workerID uuid.UUID, lockDuration time.Duration) (id uuid.UUID, claimed bool, err error)
	ReclaimStaleWindow(ctx context.Context, projectName tenant.ProjectName, workerID uuid.UUID, lockDuration time.Duration, maxAttempts int) (*scheduler.AirflowSyncWindow, error)
	FailExhaustedWindows(ctx context.Context, projectName tenant.ProjectName, maxAttempts int, lastError string) (int64, error)
	CompleteWindow(ctx context.Context, id, workerID uuid.UUID, maxProcessedLogID *int64, eventsMatched, runsReconciled int) (bool, error)
	RecordAttemptError(ctx context.Context, id, workerID uuid.UUID, lastError string) error
}

// WindowReconciler is satisfied by *AirflowStateSyncService.
type WindowReconciler interface {
	ReconcileWindow(ctx context.Context, projectName tenant.ProjectName, startTime, endTime time.Time) (ReconcileWindowResult, error)
}

type AirflowStateSyncConfig struct {
	// WindowInterval is both the ticker cadence and the size of each claimed window: every
	// tick, the worker advances each project's watermark by one window of this length
	// (more, if catching up -- see MaxWindowsPerTick).
	WindowInterval time.Duration
	// SettlingDelay keeps a window's end_time from being claimed until it is this far in
	// the past, so a row whose transaction had not committed yet when queried is not
	// permanently skipped (see the RFC's "windowing" section).
	SettlingDelay time.Duration
	// LockDuration bounds how long a claimed window may stay `in_progress` before another
	// replica is allowed to treat it as crashed and re-claim it.
	LockDuration time.Duration
	// InitialLookback caps how far back the very first window goes for a project with no
	// prior sync history, so a fresh deployment does not try to ingest all of history.
	InitialLookback time.Duration
	// OverlapEpsilon is subtracted from every window's start when querying Airflow, because
	// eventLogs' `after`/`before` bounds are both strict: without an overlap, a row whose
	// dttm exactly equals a window boundary is excluded by both the window that ends there
	// and the one that begins there. The resulting re-fetch of a few already-processed rows
	// is harmless: reconciliation is idempotent (it checks current-state-equals-target-state
	// before writing anything).
	OverlapEpsilon time.Duration
	// MaxAttempts is how many times a crashed/failed window is retried (via
	// ReclaimStaleWindow) before it is marked `failed` and the watermark advances past it
	// anyway -- see the RFC on why a `failed` window still counts towards the watermark:
	// excluding it would let one permanently-broken window block every later window for the
	// project from ever being attempted.
	MaxAttempts int
	// MaxWindowsPerTick bounds how many windows a single tick will claim and process for one
	// project, so a project catching up after downtime does not turn one tick into
	// unbounded work.
	MaxWindowsPerTick int
}

type AirflowStateSyncWorker struct {
	l log.Logger

	projectRepo   AirflowSyncProjectRepository
	syncStateRepo AirflowSyncStateRepository
	reconciler    WindowReconciler

	config AirflowStateSyncConfig
}

func NewAirflowStateSyncWorker(l log.Logger, projectRepo AirflowSyncProjectRepository, syncStateRepo AirflowSyncStateRepository,
	reconciler WindowReconciler, config AirflowStateSyncConfig,
) *AirflowStateSyncWorker {
	return &AirflowStateSyncWorker{
		l:             l,
		projectRepo:   projectRepo,
		syncStateRepo: syncStateRepo,
		reconciler:    reconciler,
		config:        config,
	}
}

// ScheduleAirflowStateSync starts the ticker loop. Modeled on SLAWorker.ScheduleSLAHandling:
// self-spawns its own goroutine and stops on ctx.Done(), so the caller just needs to wire a
// cancellable context into server shutdown (see server/optimus.go's cleanupFn pattern).
func (w *AirflowStateSyncWorker) ScheduleAirflowStateSync(ctx context.Context) {
	ticker := time.NewTicker(w.config.WindowInterval)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				w.tick(ctx)
			}
		}
	}()
}

func (w *AirflowStateSyncWorker) tick(ctx context.Context) {
	projects, err := w.projectRepo.GetAll(ctx)
	if err != nil {
		w.l.Error("[airflowStateSync] failed to list projects: %s", err)
		return
	}
	for _, p := range projects {
		if ctx.Err() != nil {
			return
		}
		w.processProject(ctx, p.Name())
	}
}

func (w *AirflowStateSyncWorker) processProject(ctx context.Context, projectName tenant.ProjectName) {
	workerID := uuid.New()

	// A worker that died mid-window gets priority: make progress on it (or fail it out once
	// attempts are exhausted) before claiming anything new, so a wedged window doesn't
	// starve forever behind an ever-advancing watermark.
	if reclaimed, err := w.syncStateRepo.ReclaimStaleWindow(ctx, projectName, workerID, w.config.LockDuration, w.config.MaxAttempts); err != nil {
		w.l.Error("[airflowStateSync] project [%s] failed to reclaim stale window: %s", projectName, err)
	} else if reclaimed != nil {
		w.processWindow(ctx, reclaimed)
	}

	if failedCount, err := w.syncStateRepo.FailExhaustedWindows(ctx, projectName, w.config.MaxAttempts, "max attempts exceeded"); err != nil {
		w.l.Error("[airflowStateSync] project [%s] failed to fail exhausted windows: %s", projectName, err)
	} else if failedCount > 0 {
		w.l.Error("[airflowStateSync] project [%s] gave up on %d window(s) after exhausting retries -- manual overrides in that span went unreconciled, investigate and consider re-running", projectName, failedCount)
	}

	for i := 0; i < w.config.MaxWindowsPerTick; i++ {
		if ctx.Err() != nil {
			return
		}
		if !w.claimAndProcessNextWindow(ctx, projectName, workerID) {
			return
		}
	}
}

// claimAndProcessNextWindow claims and processes exactly one window, returning false when
// there is nothing to do this tick (either nothing due yet, per SettlingDelay, or another
// replica already claimed it) so processProject's catch-up loop knows to stop early rather
// than spinning through MaxWindowsPerTick iterations for nothing.
func (w *AirflowStateSyncWorker) claimAndProcessNextWindow(ctx context.Context, projectName tenant.ProjectName, workerID uuid.UUID) bool {
	watermark, err := w.syncStateRepo.GetWatermark(ctx, projectName)
	if err != nil {
		w.l.Error("[airflowStateSync] project [%s] failed to get watermark: %s", projectName, err)
		return false
	}

	start := time.Now().Add(-w.config.InitialLookback)
	if watermark != nil {
		start = *watermark
	}
	end := start.Add(w.config.WindowInterval)

	settleBoundary := time.Now().Add(-w.config.SettlingDelay)
	if end.After(settleBoundary) {
		return false // nothing due yet
	}

	id, claimed, err := w.syncStateRepo.ClaimWindow(ctx, projectName, start, end, workerID, w.config.LockDuration)
	if err != nil {
		w.l.Error("[airflowStateSync] project [%s] failed to claim window [%s, %s): %s", projectName, start, end, err)
		return false
	}
	if !claimed {
		// another replica holds it, or it is already terminal -- either way, stop for now
		return false
	}

	w.processWindow(ctx, &scheduler.AirflowSyncWindow{ID: id, ProjectName: projectName, StartTime: start, EndTime: end, WorkerID: workerID})
	return true
}

func (w *AirflowStateSyncWorker) processWindow(ctx context.Context, win *scheduler.AirflowSyncWindow) {
	result, err := w.reconciler.ReconcileWindow(ctx, win.ProjectName, win.StartTime.Add(-w.config.OverlapEpsilon), win.EndTime)
	if err != nil {
		w.l.Error("[airflowStateSync] project [%s] failed reconciling window [%s, %s): %s", win.ProjectName, win.StartTime, win.EndTime, err)
		if err := w.syncStateRepo.RecordAttemptError(ctx, win.ID, win.WorkerID, err.Error()); err != nil {
			w.l.Error("[airflowStateSync] failed to record attempt error for window [%s]: %s", win.ID, err)
		}
		return
	}

	completed, err := w.syncStateRepo.CompleteWindow(ctx, win.ID, win.WorkerID, result.MaxProcessedLogID, result.EventsMatched, result.RunsReconciled)
	if err != nil {
		w.l.Error("[airflowStateSync] failed to complete window [%s]: %s", win.ID, err)
		return
	}
	if !completed {
		// lost the lease mid-window (another replica reclaimed it after our lock expired) --
		// our progress is discarded, and the reclaiming replica will redo this window.
		w.l.Warn("[airflowStateSync] lost lease on window [%s] before completion could be recorded, another replica will redo it", win.ID)
		return
	}
	if result.EventsMatched > 0 {
		w.l.Info("[airflowStateSync] project [%s] window [%s, %s): matched %d manual override(s), reconciled %d run(s)",
			win.ProjectName, win.StartTime, win.EndTime, result.EventsMatched, result.RunsReconciled)
	}
}
