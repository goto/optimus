package service

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/goto/salt/log"

	"github.com/goto/optimus/config"
	"github.com/goto/optimus/core/scheduler"
	"github.com/goto/optimus/core/tenant"
	"github.com/goto/optimus/internal/errors"
)

type BackfillWatcherRepository interface {
	GetBackfillDetails(ctx context.Context, backfillID uuid.UUID) (*scheduler.Backfill, error)
	UpdateBackfillStatus(ctx context.Context, backfillID uuid.UUID, state scheduler.BackfilState, message string) error
	UpdateBackfillHeartbeat(ctx context.Context, backfillID uuid.UUID) error
	ScanAbandonedBackfillRequests(ctx context.Context, unhandledClassifierDuration time.Duration) ([]*scheduler.Backfill, error)
	AcquireBackfillRequest(ctx context.Context, backfillID uuid.UUID, unhandledClassifierDuration time.Duration) error
}

type BackfillExecutor interface {
	GetRunStatusByDagRunID(ctx context.Context, tnnt tenant.Tenant, jobName scheduler.JobName, dagRunID string) (*scheduler.JobRunStatus, error)
}

type BackfillWorker struct {
	logger       log.Logger
	backfillRepo BackfillWatcherRepository
	executor     BackfillExecutor
	config       config.BackfillConfig
}

func NewBackfillWorker(logger log.Logger, backfillRepo BackfillWatcherRepository, executor BackfillExecutor, cfg config.BackfillConfig) *BackfillWorker {
	return &BackfillWorker{
		logger:       logger,
		backfillRepo: backfillRepo,
		executor:     executor,
		config:       cfg,
	}
}

func (w *BackfillWorker) WatchBackfillExecution(ctx context.Context, backfillID uuid.UUID) error {
	w.logger.Info("[BackfillID: %s] starting watcher goroutine", backfillID)
	iterationCount := 0
	for {
		select {
		case <-ctx.Done():
			w.logger.Error("[BackfillID: %s] deadline encountered...", backfillID)
			return ctx.Err()
		default:
		}

		if iterationCount > 0 {
			w.logger.Debug("[BackfillID: %s] waiting %ds before next poll (iteration %d)", backfillID, w.config.ExecutionIntervalInSeconds, iterationCount)
			time.Sleep(time.Duration(w.config.ExecutionIntervalInSeconds) * time.Second)
		}
		iterationCount++

		w.logger.Debug("[BackfillID: %s] fetching backfill details (iteration %d)", backfillID, iterationCount)
		backfill, err := w.backfillRepo.GetBackfillDetails(ctx, backfillID)
		if err != nil {
			w.logger.Error("[BackfillID: %s] unable to fetch backfill details: %s", backfillID, err)
			return err
		}
		w.logger.Debug("[BackfillID: %s] current state: %s, scheduler run ID: %s", backfillID, backfill.State(), backfill.SchedulerRunID)

		if backfill.State() == scheduler.BackfilStateCancelled {
			w.logger.Info("[BackfillID: %s] backfill was cancelled externally, stopping watcher", backfillID)
			return nil
		}

		if err := w.backfillRepo.UpdateBackfillHeartbeat(ctx, backfillID); err != nil {
			w.logger.Error("[BackfillID: %s] unable to update heartbeat: %s", backfillID, err)
			return err
		}
		w.logger.Debug("[BackfillID: %s] heartbeat updated", backfillID)

		w.logger.Info("[BackfillID: %s] polling scheduler run status for dag run ID [%s]", backfillID, backfill.SchedulerRunID)
		run, err := w.executor.GetRunStatusByDagRunID(ctx, backfill.Tenant(), backfill.JobName(), backfill.SchedulerRunID)
		if err != nil {
			w.logger.Error("[BackfillID: %s] unable to get run status from scheduler: %s", backfillID, err)
			return err
		}
		w.logger.Debug("[BackfillID: %s] received run status from scheduler: %s", backfillID, run.State)

		terminalState, msg := resolveBackfillTerminalState(run)
		if terminalState == "" {
			w.logger.Info("[BackfillID: %s] run still in progress, will recheck after %ds", backfillID, w.config.ExecutionIntervalInSeconds)
			continue
		}

		w.logger.Info("[BackfillID: %s] backfill run reached terminal state [%s]: %s", backfillID, terminalState, msg)
		if err := w.backfillRepo.UpdateBackfillStatus(ctx, backfillID, terminalState, msg); err != nil {
			w.logger.Error("[BackfillID: %s] unable to update final backfill status: %s", backfillID, err)
			return err
		}
		w.logger.Info("[BackfillID: %s] watcher completed, final status persisted", backfillID)
		return nil
	}
}

func resolveBackfillTerminalState(run *scheduler.JobRunStatus) (scheduler.BackfilState, string) {
	if run == nil {
		return "", ""
	}
	switch run.State {
	case scheduler.StateFailed:
		return scheduler.BackfilStateFailed, "backfill dag run failed"
	case scheduler.StateSuccess:
		return scheduler.BackfilStateSuccess, "backfill dag run completed successfully"
	default:
		return "", ""
	}
}

const backfillSyncMultiplier = 3

func (w *BackfillWorker) ScanBackfillRequest(ctx context.Context) {
	unhandledClassifierDuration := time.Duration(w.config.ExecutionIntervalInSeconds*backfillSyncMultiplier) * time.Second
	requestScanInterval := time.Duration(w.config.ExecutionIntervalInSeconds*backfillSyncMultiplier) * time.Second
	w.logger.Info("backfill worker started: scan interval=%s, unhandled threshold=%s", requestScanInterval, unhandledClassifierDuration)
	for {
		select {
		case <-ctx.Done():
			w.logger.Info("backfill worker shutting down")
			return
		default:
		}

		w.logger.Info("scanning for abandoned/unhandled backfill requests (threshold: %s)", unhandledClassifierDuration)
		backfills, err := w.backfillRepo.ScanAbandonedBackfillRequests(ctx, unhandledClassifierDuration)
		if err != nil {
			w.logger.Error("unable to scan for abandoned backfill requests: %s", err)
			continue
		}

		w.logger.Debug("scan complete: found %d candidate backfill(s)", len(backfills))
		if len(backfills) == 0 {
			time.Sleep(requestScanInterval)
			continue
		}

		toProcess := w.getBackfillRequestsToProcess(ctx, backfills, unhandledClassifierDuration)
		w.logger.Info("acquired %d backfill(s) to watch out of %d candidate(s)", len(toProcess), len(backfills))
		for _, backfill := range toProcess {
			w.logger.Debug("[BackfillID: %s] spawning watcher goroutine (job: %s, scheduler run: %s)", backfill.ID(), backfill.JobName(), backfill.SchedulerRunID)
			go w.WatchBackfillExecution(ctx, backfill.ID())
		}
	}
}

func (w *BackfillWorker) getBackfillRequestsToProcess(ctx context.Context, backfills []*scheduler.Backfill, unhandledClassifierDuration time.Duration) []*scheduler.Backfill {
	var toProcess []*scheduler.Backfill
	for _, backfill := range backfills {
		w.logger.Debug("[BackfillID: %s] trying to acquire lock (job: %s, state: %s)", backfill.ID(), backfill.JobName(), backfill.State())
		err := w.backfillRepo.AcquireBackfillRequest(ctx, backfill.ID(), unhandledClassifierDuration)
		if err != nil {
			if errors.IsErrorType(err, errors.ErrNotFound) {
				w.logger.Debug("[BackfillID: %s] could not acquire lock, already handled by another instance", backfill.ID())
				continue
			}
			w.logger.Error("[BackfillID: %s] unable to acquire lock on backfill request: %s", backfill.ID(), err)
			return toProcess
		}
		w.logger.Info("[BackfillID: %s] lock acquired successfully (job: %s)", backfill.ID(), backfill.JobName())
		toProcess = append(toProcess, backfill)
	}
	return toProcess
}
