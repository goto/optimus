package service

import (
	"context"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/goto/salt/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/goto/optimus/core/scheduler"
	"github.com/goto/optimus/core/tenant"
)

// airflowSyncWindowsFailedTotal counts windows a project gave up on after exhausting
// MaxAttempts -- each one is a span of time whose manual overrides were never reconciled.
var airflowSyncWindowsFailedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "airflow_sync_windows_failed_total",
	Help: "airflow-sync windows marked failed after exhausting retries, by project",
}, []string{"project"})

// AirflowSyncProjectRepository lists every project the reconciler should sweep. Each project
// has its own Airflow instance (SCHEDULER_HOST is per-project config), so sync runs
// independently per project.
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

const (
	// settlingDelay keeps a window's end_time from being claimed until it is this far in the
	// past, so a row whose transaction hadn't committed yet when queried isn't permanently
	// skipped.
	settlingDelay = 60 * time.Second
	// overlapEpsilon is subtracted from every window's start when querying Airflow: the
	// eventLogs `after`/`before` bounds are both strict, so without an overlap a row landing
	// exactly on a boundary is excluded by both the window before and the window after it.
	// Re-fetching a couple of already-processed rows is harmless since reconciliation is
	// idempotent.
	overlapEpsilon = 2 * time.Second
	// maxWindowsPerTick bounds how many windows a single tick claims and processes for one
	// project, so catching up after downtime doesn't turn one tick into unbounded work.
	maxWindowsPerTick = 12
)

type AirflowStateSyncConfig struct {
	// WindowInterval is both the ticker cadence and the size of each claimed window. A zero
	// value disables the worker entirely (see server/optimus.go).
	WindowInterval time.Duration
	// LockDuration bounds how long a claimed window may stay `in_progress` before another
	// replica may treat it as crashed and re-claim it. Also bounds how long a single window's
	// reconcile call is allowed to run (see processWindow).
	LockDuration time.Duration
	// MaxConcurrentProjects bounds how many projects one pod processes at once per tick, so a
	// project whose Airflow instance is slow or unreachable doesn't delay every project listed
	// after it in the same tick.
	MaxConcurrentProjects int
	// MaxAttempts is how many times a crashed/failed window is retried before it's marked
	// `failed` and the watermark advances past it anyway, so one broken window can't block a
	// project's sync forever.
	MaxAttempts int
	// ExcludeProjects lists project names to skip syncing entirely -- e.g. an unstable
	// instance, or a project not yet rolled out. Every project syncs by default.
	ExcludeProjects []string
}

type AirflowStateSyncWorker struct {
	l log.Logger

	projectRepo   AirflowSyncProjectRepository
	syncStateRepo AirflowSyncStateRepository
	reconciler    WindowReconciler

	config          AirflowStateSyncConfig
	excludedProject map[string]struct{}
}

func NewAirflowStateSyncWorker(l log.Logger, projectRepo AirflowSyncProjectRepository, syncStateRepo AirflowSyncStateRepository,
	reconciler WindowReconciler, config AirflowStateSyncConfig,
) *AirflowStateSyncWorker {
	excludedProject := make(map[string]struct{}, len(config.ExcludeProjects))
	for _, name := range config.ExcludeProjects {
		excludedProject[name] = struct{}{}
	}
	return &AirflowStateSyncWorker{
		l:               l,
		projectRepo:     projectRepo,
		syncStateRepo:   syncStateRepo,
		reconciler:      reconciler,
		config:          config,
		excludedProject: excludedProject,
	}
}

// initialLookback caps how far back the very first window goes for a project with no prior
// sync history, so a fresh project doesn't try to ingest all of history.
func (w *AirflowStateSyncWorker) initialLookback() time.Duration {
	return w.config.WindowInterval * time.Duration(maxWindowsPerTick)
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

// tick processes projects concurrently, up to MaxConcurrentProjects at a time, so a slow or
// unreachable project doesn't hold up the rest.
func (w *AirflowStateSyncWorker) tick(ctx context.Context) {
	projects, err := w.projectRepo.GetAll(ctx)
	if err != nil {
		w.l.Error("[airflowStateSync] failed to list projects: %s", err)
		return
	}

	sem := make(chan struct{}, w.config.MaxConcurrentProjects)
	var wg sync.WaitGroup
	for _, p := range projects {
		if ctx.Err() != nil {
			break
		}
		projectName := p.Name()
		if _, excluded := w.excludedProject[projectName.String()]; excluded {
			continue
		}

		sem <- struct{}{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			w.processProject(ctx, projectName)
		}()
	}
	wg.Wait()
}

func (w *AirflowStateSyncWorker) processProject(ctx context.Context, projectName tenant.ProjectName) {
	workerID := uuid.New()

	// Reclaim any window a crashed worker left stuck, before claiming anything new.
	if reclaimed, err := w.syncStateRepo.ReclaimStaleWindow(ctx, projectName, workerID, w.config.LockDuration, w.config.MaxAttempts); err != nil {
		w.l.Error("[airflowStateSync] project [%s] failed to reclaim stale window: %s", projectName, err)
	} else if reclaimed != nil {
		w.processWindow(ctx, reclaimed)
	}

	if failedCount, err := w.syncStateRepo.FailExhaustedWindows(ctx, projectName, w.config.MaxAttempts, "max attempts exceeded"); err != nil {
		w.l.Error("[airflowStateSync] project [%s] failed to fail exhausted windows: %s", projectName, err)
	} else if failedCount > 0 {
		airflowSyncWindowsFailedTotal.WithLabelValues(projectName.String()).Add(float64(failedCount))
		w.l.Error("[airflowStateSync] project [%s] gave up on %d window(s) after exhausting retries -- manual overrides in that span went unreconciled, investigate and consider re-running", projectName, failedCount)
	}

	for i := 0; i < maxWindowsPerTick; i++ {
		if ctx.Err() != nil {
			return
		}
		if !w.claimAndProcessNextWindow(ctx, projectName, workerID) {
			return
		}
	}
}

// claimAndProcessNextWindow claims and processes exactly one window, returning false when
// there's nothing to do (nothing due yet, or another replica already claimed it) so the
// caller's catch-up loop can stop early.
func (w *AirflowStateSyncWorker) claimAndProcessNextWindow(ctx context.Context, projectName tenant.ProjectName, workerID uuid.UUID) bool {
	watermark, err := w.syncStateRepo.GetWatermark(ctx, projectName)
	if err != nil {
		w.l.Error("[airflowStateSync] project [%s] failed to get watermark: %s", projectName, err)
		return false
	}

	start := time.Now().Add(-w.initialLookback())
	if watermark != nil {
		start = *watermark
	}
	end := start.Add(w.config.WindowInterval)

	settleBoundary := time.Now().Add(-settlingDelay)
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
	// The Airflow HTTP client has no request timeout of its own, so bound the reconcile call
	// to LockDuration -- otherwise a hung request blocks this goroutine indefinitely.
	reconcileCtx, cancel := context.WithTimeout(ctx, w.config.LockDuration)
	defer cancel()

	result, err := w.reconciler.ReconcileWindow(reconcileCtx, win.ProjectName, win.StartTime.Add(-overlapEpsilon), win.EndTime)
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
