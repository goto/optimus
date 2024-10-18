package service

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/goto/salt/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/goto/optimus/config"
	"github.com/goto/optimus/core/scheduler"
	"github.com/goto/optimus/core/tenant"
	"github.com/goto/optimus/internal/errors"
	"github.com/goto/optimus/internal/lib/cron"
)

const (
	prefixReplayed       = "replayed"
	replayCleanupTimeout = time.Minute
	replaySyncMultiplier = 3
)

var replayReqLag = promauto.NewGauge(prometheus.GaugeOpts{
	Name: "replay_request_lag",
	Help: "how old is the oldest unhandled replay request",
})

type ReplayWorker struct {
	logger       log.Logger
	replayRepo   ReplayRepository
	jobRepo      JobRepository
	scheduler    ReplayScheduler
	config       config.ReplayConfig
	alertManager AlertManager
}

type ReplayScheduler interface {
	Clear(ctx context.Context, t tenant.Tenant, jobName scheduler.JobName, scheduledAt time.Time) error
	ClearBatch(ctx context.Context, t tenant.Tenant, jobName scheduler.JobName, startTime, endTime time.Time) error

	CancelRun(ctx context.Context, tnnt tenant.Tenant, jobName scheduler.JobName, dagRunID string) error
	CreateRun(ctx context.Context, tnnt tenant.Tenant, jobName scheduler.JobName, executionTime time.Time, dagRunIDPrefix string) error
	GetJobRuns(ctx context.Context, t tenant.Tenant, criteria *scheduler.JobRunsCriteria, jobCron *cron.ScheduleSpec) ([]*scheduler.JobRunStatus, error)
	GetJobRunsWithDetails(ctx context.Context, t tenant.Tenant, criteria *scheduler.JobRunsCriteria, jobCron *cron.ScheduleSpec) ([]*scheduler.JobRunWithDetails, error)
}

func NewReplayWorker(logger log.Logger, replayRepository ReplayRepository, jobRepo JobRepository, scheduler ReplayScheduler, cfg config.ReplayConfig, alertManager AlertManager) *ReplayWorker {
	return &ReplayWorker{
		logger:       logger,
		jobRepo:      jobRepo,
		replayRepo:   replayRepository,
		config:       cfg,
		scheduler:    scheduler,
		alertManager: alertManager,
	}
}

func (w *ReplayWorker) Execute(ctxBack context.Context, replayID uuid.UUID, jobTenant tenant.Tenant, jobName scheduler.JobName) {
	ctx, cancelFn := context.WithTimeout(ctxBack, time.Minute*time.Duration(w.config.ReplayTimeoutInMinutes))
	defer cancelFn()

	w.logger.Info("[ReplayID: %s] starting to execute replay", replayID)
	project := jobTenant.ProjectName().String()
	namespace := jobTenant.NamespaceName().String()

	jobCron, err := getJobCron(ctx, w.logger, w.jobRepo, jobTenant, jobName)
	if err != nil {
		w.logger.Error("[ReplayID: %s] unable to get cron value for job [%s]: %s", replayID.String(), jobName.String(), err)
		if err := w.replayRepo.UpdateReplayStatus(ctx, replayID, scheduler.ReplayStateFailed, err.Error()); err != nil {
			w.logger.Error("[ReplayID: %s] unable to update replay to failed: %s", replayID, err.Error())
		}
		jobReplayMetric.WithLabelValues(project, namespace, jobName.String(), scheduler.ReplayStateFailed.String()).Inc()
		return
	}

	if err := w.startExecutionLoop(ctx, replayID, jobCron); err != nil {
		cleanupCtx, cleanupCancelFn := context.WithTimeout(ctx, replayCleanupTimeout)
		defer cleanupCancelFn()

		errMessage := err.Error()
		if errors.Is(err, context.DeadlineExceeded) {
			errMessage = "replay execution timed out"
			w.alertManager.SendReplayEvent(&scheduler.ReplayNotificationAttrs{
				JobName:  jobName.String(),
				ReplayID: replayID.String(),
				Tenant:   jobTenant,
				JobURN:   jobName.GetJobURN(jobTenant),
				State:    scheduler.ReplayStateTimeout,
			})
		} else {
			w.alertManager.SendReplayEvent(&scheduler.ReplayNotificationAttrs{
				JobName:  jobName.String(),
				ReplayID: replayID.String(),
				Tenant:   jobTenant,
				JobURN:   jobName.GetJobURN(jobTenant),
				State:    scheduler.ReplayStateFailed,
			})
		}
		w.logger.Error("[ReplayID: %s] unable to execute replay for job [%s]: %s", replayID.String(), jobName.String(), errMessage)

		if err := w.replayRepo.UpdateReplayStatus(cleanupCtx, replayID, scheduler.ReplayStateFailed, errMessage); err != nil {
			w.logger.Error("[ReplayID: %s] unable to set replay status to 'failed': %s", replayID, err.Error())
		}
		jobReplayMetric.WithLabelValues(project, namespace, jobName.String(), scheduler.ReplayStateFailed.String()).Inc()
	}
}

func (w *ReplayWorker) FetchAndSyncStatus(ctx context.Context, replayWithRun *scheduler.ReplayWithRun, jobCron *cron.ScheduleSpec) (scheduler.JobRunStatusList, error) {
	incomingRuns, err := w.fetchRuns(ctx, replayWithRun, jobCron)
	if err != nil {
		w.logger.Error("[ReplayID: %s] unable to get incoming runs: %s", replayWithRun.Replay.ID().String(), err)
		return scheduler.JobRunStatusList{}, err
	}
	return syncStatus(replayWithRun.Runs, incomingRuns), nil
}

func (w *ReplayWorker) isReplayCanceled(ctx context.Context, replayID uuid.UUID) (bool, error) {
	replayReq, err := w.replayRepo.GetReplayRequestByID(ctx, replayID)
	if err != nil {
		w.logger.Error("[ReplayID: %s] unable to get existing runs, err: %s", replayID.String(), err.Error())
		return false, err
	}
	return replayReq.State() == scheduler.ReplayStateCancelled, nil
}

func (w *ReplayWorker) startExecutionLoop(ctx context.Context, replayID uuid.UUID, jobCron *cron.ScheduleSpec) error {
	executionLoopCount := 0
	for {
		select {
		case <-ctx.Done():
			w.logger.Error("[ReplayID: %s] deadline encountered...", replayID)
			return ctx.Err()
		default:
		}

		executionLoopCount++
		// delay if not the first loop iteration
		if executionLoopCount > 1 {
			time.Sleep(time.Duration(w.config.ExecutionIntervalInSeconds) * time.Second)
		}

		w.logger.Info("[ReplayID: %s] processing replay...", replayID)

		// sync run first
		replayWithRun, err := w.replayRepo.GetReplayByID(ctx, replayID)
		if err != nil {
			w.logger.Error("[ReplayID: %s] unable to get existing runs, err: %s", replayID.String(), err.Error())
			return err
		}

		if replayWithRun.Replay.IsTerminated() {
			t := replayWithRun.Replay.Tenant()
			w.alertManager.SendReplayEvent(&scheduler.ReplayNotificationAttrs{
				JobName:  replayWithRun.Replay.JobName().String(),
				ReplayID: replayID.String(),
				Tenant:   t,
				JobURN:   replayWithRun.Replay.JobName().GetJobURN(t),
				State:    replayWithRun.Replay.State(),
			})
			w.logger.Info("[ReplayID: %s] replay is externally terminated with status [%s]", replayWithRun.Replay.ID().String(), replayWithRun.Replay.State().String())
			return nil
		}

		if executionLoopCount == 1 {
			err := w.replayRepo.UpdateReplayStatus(ctx, replayID, scheduler.ReplayStateInProgress, "started handling replay request")
			if err != nil {
				w.logger.Error("[ReplayID: %s] unable to set replay state in progress", replayID.String(), err)
				return err
			}
		}

		syncedRunStatus, err := w.FetchAndSyncStatus(ctx, replayWithRun, jobCron)
		if err != nil {
			w.logger.Error("[ReplayID: %s] unable to get incoming runs: %s", replayWithRun.Replay.ID().String(), err)
			return err
		}

		if err := w.replayRepo.UpdateReplayRuns(ctx, replayWithRun.Replay.ID(), syncedRunStatus); err != nil {
			w.logger.Error("[ReplayID: %s] unable to update replay state to failed: %s", replayWithRun.Replay.ID(), err)
			return err
		}

		runStatusSummary := syncedRunStatus.GetJobRunStatusSummary()
		w.logger.Info("[ReplayID: %s] synced %d replay runs with status: %s", replayID, len(syncedRunStatus), runStatusSummary)

		// check if replay request is on termination state
		if syncedRunStatus.IsAllTerminated() {
			return w.finishReplay(ctx, replayWithRun.Replay, syncedRunStatus, runStatusSummary)
		}

		// pick runs to be triggered
		statesForReplay := []scheduler.State{scheduler.StatePending, scheduler.StateMissing}
		toBeReplayedRuns := syncedRunStatus.GetSortedRunsByStates(statesForReplay)
		if len(toBeReplayedRuns) == 0 {
			continue
		}

		// execute replay run on scheduler
		canceled, err := w.isReplayCanceled(ctx, replayID)
		if err != nil {
			return err
		}
		if canceled {
			w.logger.Info("[ReplayID: %s] replay is externally canceled", replayID.String())
			return nil
		}
		var updatedRuns []*scheduler.JobRunStatus
		if replayWithRun.Replay.Config().Parallel {
			if err := w.replayRunOnScheduler(ctx, jobCron, replayWithRun.Replay, toBeReplayedRuns...); err != nil {
				return err
			}
			updatedRuns = scheduler.JobRunStatusList(toBeReplayedRuns).OverrideWithStatus(scheduler.StateInProgress)
		} else { // sequential should work when there's no in_progress state on existing runs
			inProgressRuns := syncedRunStatus.GetSortedRunsByStates([]scheduler.State{scheduler.StateInProgress})
			if len(inProgressRuns) > 0 {
				w.logger.Info("[ReplayID: %s] %d run is in progress, skip sequential iteration", replayWithRun.Replay.ID(), len(inProgressRuns))
				continue
			}
			if err := w.replayRunOnScheduler(ctx, jobCron, replayWithRun.Replay, toBeReplayedRuns[0]); err != nil {
				return err
			}
			updatedRuns = scheduler.JobRunStatusList(toBeReplayedRuns[:1]).OverrideWithStatus(scheduler.StateInProgress)
		}

		// update runs status
		if err := w.replayRepo.UpdateReplayRuns(ctx, replayWithRun.Replay.ID(), updatedRuns); err != nil {
			w.logger.Error("[ReplayID: %s] unable to update replay runs: %s", replayWithRun.Replay.ID(), err)
			return err
		}
	}
}

func (w *ReplayWorker) finishReplay(ctx context.Context, replay *scheduler.Replay, syncedRunStatus scheduler.JobRunStatusList, runStatusSummary string) error {
	replayID := replay.ID()
	replayState := scheduler.ReplayStateSuccess
	if syncedRunStatus.IsAnyFailure() {
		replayState = scheduler.ReplayStateFailed
	}
	w.alertManager.SendReplayEvent(&scheduler.ReplayNotificationAttrs{
		JobName:  replay.JobName().String(),
		ReplayID: replayID.String(),
		Tenant:   replay.Tenant(),
		JobURN:   replay.JobName().GetJobURN(replay.Tenant()),
		State:    replayState,
	})

	msg := fmt.Sprintf("replay is finished with run status: %s", runStatusSummary)
	w.logger.Info("[ReplayID: %s] replay finished with status %s", replayID, replayState)

	if err := w.replayRepo.UpdateReplay(ctx, replayID, replayState, syncedRunStatus, msg); err != nil {
		w.logger.Error("[ReplayID: %s] unable to update replay state to failed: %s", replayID, err)
		return err
	}
	return nil
}

func (w *ReplayWorker) FetchRunsWithDetails(ctx context.Context, replay *scheduler.Replay, jobCron *cron.ScheduleSpec) (scheduler.JobRunDetailsList, error) {
	jobRunCriteria := &scheduler.JobRunsCriteria{
		Name:      replay.JobName().String(),
		StartDate: replay.Config().StartTime.UTC(),
		EndDate:   replay.Config().EndTime.UTC(),
	}
	return w.scheduler.GetJobRunsWithDetails(ctx, replay.Tenant(), jobRunCriteria, jobCron)
}

func (w *ReplayWorker) fetchRuns(ctx context.Context, replayReq *scheduler.ReplayWithRun, jobCron *cron.ScheduleSpec) ([]*scheduler.JobRunStatus, error) {
	jobRunCriteria := &scheduler.JobRunsCriteria{
		Name:      replayReq.Replay.JobName().String(),
		StartDate: replayReq.Replay.Config().StartTime.UTC(),
		EndDate:   replayReq.Replay.Config().EndTime.UTC(),
	}
	return w.scheduler.GetJobRuns(ctx, replayReq.Replay.Tenant(), jobRunCriteria, jobCron)
}

func (w *ReplayWorker) CancelReplayRunsOnScheduler(ctx context.Context, replay *scheduler.Replay, jobCron *cron.ScheduleSpec, runs []*scheduler.JobRunWithDetails) []*scheduler.JobRunStatus {
	var canceledRuns []*scheduler.JobRunStatus
	for _, run := range runs {
		runState := scheduler.JobRunStatus{
			ScheduledAt: run.ScheduledAt,
			State:       run.State,
		}
		logicalTime := runState.GetLogicalTime(jobCron)

		w.logger.Info("[ReplayID: %s] Canceling run with logical time: %s", replay.ID(), logicalTime)
		if err := w.scheduler.CancelRun(ctx, replay.Tenant(), replay.JobName(), run.DagRunID); err != nil {
			w.logger.Error("[ReplayID: %s] unable to cancel job run for job: %s, Schedule Time: %s, err: %s", replay.ID(), replay.JobName(), run.ScheduledAt, err.Error())
			continue
		}
		canceledRuns = append(canceledRuns, &runState)
	}
	return canceledRuns
}

func (w *ReplayWorker) replayRunOnScheduler(ctx context.Context, jobCron *cron.ScheduleSpec, replayReq *scheduler.Replay, runs ...*scheduler.JobRunStatus) error {
	// clear runs
	pendingRuns := scheduler.JobRunStatusList(runs).GetSortedRunsByStates([]scheduler.State{scheduler.StatePending})
	if l := len(pendingRuns); l > 0 {
		startLogicalTime := pendingRuns[0].GetLogicalTime(jobCron)
		endLogicalTime := pendingRuns[l-1].GetLogicalTime(jobCron)
		w.logger.Info("[ReplayID: %s] clearing runs with startLogicalTime: %s, endLogicalTime: %s", replayReq.ID(), startLogicalTime, endLogicalTime)
		if err := w.scheduler.ClearBatch(ctx, replayReq.Tenant(), replayReq.JobName(), startLogicalTime, endLogicalTime); err != nil {
			w.logger.Error("[ReplayID: %s] unable to clear job run: %s", replayReq.ID(), err)
			return err
		}
	}

	// create missing runs
	missingRuns := scheduler.JobRunStatusList(runs).GetSortedRunsByStates([]scheduler.State{scheduler.StateMissing})
	me := errors.NewMultiError("create runs")
	for _, run := range missingRuns {
		logicalTime := run.GetLogicalTime(jobCron)
		w.logger.Info("[ReplayID: %s] creating a new run with logical time: %s", replayReq.ID(), logicalTime)
		if err := w.scheduler.CreateRun(ctx, replayReq.Tenant(), replayReq.JobName(), logicalTime, prefixReplayed); err != nil {
			w.logger.Error("[ReplayID: %s] unable to create job run: %s", replayReq.ID(), err)
			me.Append(err)
		}
	}

	return me.ToErr()
}

// syncStatus syncs existing and incoming runs
// replay status: created -> in_progress -> [success, failed]
// replay runs: [missing, pending] -> in_progress -> [success, failed]
func syncStatus(existingJobRuns, incomingJobRuns []*scheduler.JobRunStatus) scheduler.JobRunStatusList {
	incomingRunStatusMap := scheduler.JobRunStatusList(incomingJobRuns).ToRunStatusMap()
	existingRunStatusMap := scheduler.JobRunStatusList(existingJobRuns).ToRunStatusMap()

	updatedRunStatusMap := make(map[time.Time]scheduler.State)
	for scheduledAt, existingState := range existingRunStatusMap {
		switch existingState {
		case scheduler.StateSuccess, scheduler.StateFailed:
			updatedRunStatusMap[scheduledAt] = existingState
		case scheduler.StateInProgress:
			if incomingState, ok := incomingRunStatusMap[scheduledAt]; !ok {
				updatedRunStatusMap[scheduledAt] = scheduler.StatePending
			} else if incomingState == scheduler.StateSuccess || incomingState == scheduler.StateFailed {
				updatedRunStatusMap[scheduledAt] = incomingState
			} else {
				updatedRunStatusMap[scheduledAt] = scheduler.StateInProgress
			}
		default: // pending state
			if _, ok := incomingRunStatusMap[scheduledAt]; !ok {
				updatedRunStatusMap[scheduledAt] = scheduler.StateMissing
			} else {
				updatedRunStatusMap[scheduledAt] = scheduler.StatePending
			}
		}
	}

	var updatedJobRuns []*scheduler.JobRunStatus
	for scheduledAt, state := range updatedRunStatusMap {
		updatedJobRuns = append(updatedJobRuns, &scheduler.JobRunStatus{
			ScheduledAt: scheduledAt,
			State:       state,
		})
	}

	return updatedJobRuns
}

func (w *ReplayWorker) ScanReplayRequest(ctx context.Context) {
	unhandledClassifierDuration := time.Duration(w.config.ExecutionIntervalInSeconds*replaySyncMultiplier) * time.Second
	requestScanInterval := time.Duration(w.config.ExecutionIntervalInSeconds*replaySyncMultiplier) * time.Second
	for {
		select {
		case <-ctx.Done():
			return
		default:
			time.Sleep(requestScanInterval)
		}

		// get replay requests from DB
		replays, err := w.replayRepo.ScanAbandonedReplayRequests(ctx, unhandledClassifierDuration)
		if err != nil {
			w.logger.Error("unable to scan for replay requests")
			continue
		}
		if len(replays) == 0 {
			continue
		}
		requestsToProcess := w.getRequestsToProcess(ctx, replays)
		for _, req := range requestsToProcess {
			go w.Execute(ctx, req.ID(), req.Tenant(), req.JobName())
		}
	}
}

func (w *ReplayWorker) getRequestsToProcess(ctx context.Context, replays []*scheduler.Replay) []*scheduler.Replay {
	// how old the oldest unfinished replay request is
	var maxLag float64
	// add a prometheus metric for this
	var requestsToProcess []*scheduler.Replay

	unhandledClassifierDuration := time.Duration(w.config.ExecutionIntervalInSeconds*replaySyncMultiplier) * time.Second
	for _, replay := range replays {
		lag := time.Since(replay.UpdatedAt())
		if lag.Seconds() > maxLag {
			maxLag = lag.Seconds()
		}
		err := w.replayRepo.AcquireReplayRequest(ctx, replay.ID(), unhandledClassifierDuration)
		if err != nil {
			if errors.IsErrorType(err, errors.ErrNotFound) {
				continue
			}
			w.logger.Error("unable to acquire lock on replay request err: %s", err.Error())
		}
		requestsToProcess = append(requestsToProcess, replay)
	}
	replayReqLag.Set(maxLag)
	return requestsToProcess
}
