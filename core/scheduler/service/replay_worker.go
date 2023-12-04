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
	"github.com/goto/optimus/internal/lib/cron"
	"github.com/goto/optimus/internal/telemetry"
)

const (
	prefixReplayed = "replayed"
)

type ReplayWorker struct {
	logger     log.Logger
	replayRepo ReplayRepository
	jobRepo    JobRepository
	scheduler  ReplayScheduler
	config     config.ReplayConfig
}

type ReplayScheduler interface {
	Clear(ctx context.Context, t tenant.Tenant, jobName scheduler.JobName, scheduledAt time.Time) error
	ClearBatch(ctx context.Context, t tenant.Tenant, jobName scheduler.JobName, startTime, endTime time.Time) error

	CreateRun(ctx context.Context, tnnt tenant.Tenant, jobName scheduler.JobName, executionTime time.Time, dagRunIDPrefix string) error
	GetJobRuns(ctx context.Context, t tenant.Tenant, criteria *scheduler.JobRunsCriteria, jobCron *cron.ScheduleSpec) ([]*scheduler.JobRunStatus, error)
}

func NewReplayWorker(logger log.Logger, replayRepository ReplayRepository, jobRepo JobRepository, scheduler ReplayScheduler, cfg config.ReplayConfig) *ReplayWorker {
	return &ReplayWorker{
		logger:     logger,
		jobRepo:    jobRepo,
		replayRepo: replayRepository,
		config:     cfg,
		scheduler:  scheduler,
	}
}

func (w *ReplayWorker) Execute(replayID uuid.UUID, jobTenant tenant.Tenant, jobName scheduler.JobName) {
	ctx, cancelFn := context.WithTimeout(context.Background(), time.Minute*time.Duration(w.config.ReplayTimeoutInMinutes))
	defer cancelFn()

	w.logger.Info("[ReplayID: %s] starting to execute replay", replayID)

	jobCron, err := getJobCron(ctx, w.logger, w.jobRepo, jobTenant, jobName)
	if err != nil {
		w.logger.Error("[ReplayID: %s] unable to get cron value for job [%s]: %s", replayID.String(), jobName.String(), err)
		if err := w.replayRepo.UpdateReplayStatus(ctx, replayID, scheduler.ReplayStateFailed, err.Error()); err != nil {
			w.logger.Error("[ReplayID: %s] unable to update replay to failed: %s", replayID, err.Error())
		}
		raiseReplayMetric(jobTenant, jobName, scheduler.ReplayStateFailed)
		return
	}

	if err := w.startExecutionLoop(ctx, replayID, jobCron); err != nil {
		w.logger.Error("[ReplayID: %s] unable to execute replay for job [%s]: %s", replayID.String(), jobName.String(), err)
		if err := w.replayRepo.UpdateReplayStatus(ctx, replayID, scheduler.ReplayStateFailed, err.Error()); err != nil {
			w.logger.Error("[ReplayID: %s] unable to update replay to failed: %s", replayID, err.Error())
		}
		raiseReplayMetric(jobTenant, jobName, scheduler.ReplayStateFailed)
	}
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

		// delay if not the first loop
		executionLoopCount++
		if executionLoopCount > 1 {
			time.Sleep(time.Duration(w.config.ExecutionIntervalInSeconds) * time.Second)
		}

		w.logger.Info("[ReplayID: %s] executing replay...", replayID)

		// sync run first
		storedReplayWithRun, err := w.replayRepo.GetReplayByID(ctx, replayID)
		if err != nil {
			w.logger.Error("unable to get existing runs for replay [%s]: %s", replayID.String(), err)
			return err
		}
		replayWithRun := storedReplayWithRun

		incomingRuns, err := w.fetchRuns(ctx, replayWithRun, jobCron)
		if err != nil {
			w.logger.Error("unable to get incoming runs for replay [%s]: %s", replayWithRun.Replay.ID().String(), err)
			return err
		}
		existingRuns := replayWithRun.Runs
		syncedRunStatus := syncStatus(existingRuns, incomingRuns)
		if err := w.replayRepo.UpdateReplay(ctx, replayWithRun.Replay.ID(), scheduler.ReplayStateInProgress, syncedRunStatus, ""); err != nil {
			w.logger.Error("unable to update replay state to failed for replay_id [%s]: %s", replayWithRun.Replay.ID(), err)
			return err
		}

		// check if replay request is on termination state
		if isAllRunStatusTerminated(syncedRunStatus) {
			return w.finishReplay(ctx, replayWithRun.Replay.ID(), syncedRunStatus)
		}

		// pick runs to be triggered
		statesForReplay := []scheduler.State{scheduler.StatePending, scheduler.StateMissing}
		toBeReplayedRuns := scheduler.JobRunStatusList(syncedRunStatus).GetSortedRunsByStates(statesForReplay)
		w.logger.Info("[ReplayID: %s] found %d runs to be replayed", replayWithRun.Replay.ID(), len(toBeReplayedRuns))
		if len(toBeReplayedRuns) == 0 {
			continue
		}

		// execute replay run on scheduler
		var updatedRuns []*scheduler.JobRunStatus
		if replayWithRun.Replay.Config().Parallel {
			if err := w.replayRunOnScheduler(ctx, jobCron, replayWithRun.Replay, toBeReplayedRuns...); err != nil {
				return err
			}
			updatedRuns = scheduler.JobRunStatusList(toBeReplayedRuns).OverrideWithStatus(scheduler.StateInProgress)
		} else { // sequential should work when there's no in_progress state on existing runs
			inProgressRuns := scheduler.JobRunStatusList(syncedRunStatus).GetSortedRunsByStates([]scheduler.State{scheduler.StateInProgress})
			if len(inProgressRuns) > 0 {
				w.logger.Info("[ReplayID: %s] %d run is in progress, skip sequential iteration", len(inProgressRuns), replayWithRun.Replay.ID())
				continue
			}
			if err := w.replayRunOnScheduler(ctx, jobCron, replayWithRun.Replay, toBeReplayedRuns[0]); err != nil {
				return err
			}
			updatedRuns = scheduler.JobRunStatusList(toBeReplayedRuns[:1]).OverrideWithStatus(scheduler.StateInProgress)
		}

		// update runs status
		if err := w.replayRepo.UpdateReplay(ctx, replayWithRun.Replay.ID(), scheduler.ReplayStateInProgress, updatedRuns, ""); err != nil {
			w.logger.Error("unable to update replay runs for replay_id [%s]: %s", replayWithRun.Replay.ID(), err)
			return err
		}
	}
}

func (w *ReplayWorker) finishReplay(ctx context.Context, replayID uuid.UUID, syncedRunStatus []*scheduler.JobRunStatus) error {
	replayState := scheduler.ReplayStateSuccess
	msg := ""
	if isAnyFailure(syncedRunStatus) {
		replayState = scheduler.ReplayStateFailed
		msg = "replay is failed due to some of runs are in failed state" // TODO: find out how to pass the meaningful failed message here
	}
	w.logger.Info("[ReplayID: %s] replay finished with status %s", replayID, replayState)

	if err := w.replayRepo.UpdateReplay(ctx, replayID, replayState, syncedRunStatus, msg); err != nil {
		w.logger.Error("unable to update replay state to failed for replay_id [%s]: %s", replayID, err)
		return err
	}
	return nil
}

func (w *ReplayWorker) fetchRuns(ctx context.Context, replayReq *scheduler.ReplayWithRun, jobCron *cron.ScheduleSpec) ([]*scheduler.JobRunStatus, error) {
	jobRunCriteria := &scheduler.JobRunsCriteria{
		Name:      replayReq.Replay.JobName().String(),
		StartDate: replayReq.Replay.Config().StartTime.UTC(),
		EndDate:   replayReq.Replay.Config().EndTime.UTC(),
	}
	return w.scheduler.GetJobRuns(ctx, replayReq.Replay.Tenant(), jobRunCriteria, jobCron)
}

func (w *ReplayWorker) replayRunOnScheduler(ctx context.Context, jobCron *cron.ScheduleSpec, replayReq *scheduler.Replay, runs ...*scheduler.JobRunStatus) error {
	// clear runs
	pendingRuns := scheduler.JobRunStatusList(runs).GetSortedRunsByStates([]scheduler.State{scheduler.StatePending})
	if l := len(pendingRuns); l > 0 {
		startLogicalTime := pendingRuns[0].GetLogicalTime(jobCron)
		endLogicalTime := pendingRuns[l-1].GetLogicalTime(jobCron)
		w.logger.Info("[ReplayID: %s] clearing runs with startLogicalTime: %s, endLogicalTime: %s", replayReq.ID(), startLogicalTime, endLogicalTime)
		if err := w.scheduler.ClearBatch(ctx, replayReq.Tenant(), replayReq.JobName(), startLogicalTime, endLogicalTime); err != nil {
			w.logger.Error("unable to clear job run for replay with replay_id [%s]: %s", replayReq.ID(), err)
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
			w.logger.Error("unable to create job run for replay with replay_id [%s]: %s", replayReq.ID(), err)
			me.Append(err)
		}
	}

	return me.ToErr()
}

// syncStatus syncs existing and incoming runs
// replay status: created -> in_progress -> [success, failed]
// replay runs: [missing, pending] -> in_progress -> [success, failed]
func syncStatus(existingJobRuns, incomingJobRuns []*scheduler.JobRunStatus) []*scheduler.JobRunStatus {
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

	updatedJobRuns := []*scheduler.JobRunStatus{}
	for scheduledAt, state := range updatedRunStatusMap {
		updatedJobRuns = append(updatedJobRuns, &scheduler.JobRunStatus{
			ScheduledAt: scheduledAt,
			State:       state,
		})
	}

	return updatedJobRuns
}

func isAllRunStatusTerminated(runs []*scheduler.JobRunStatus) bool {
	for _, run := range runs {
		if run.State == scheduler.StateSuccess || run.State == scheduler.StateFailed {
			continue
		}
		return false
	}
	return true
}

func isAnyFailure(runs []*scheduler.JobRunStatus) bool {
	for _, run := range runs {
		if run.State == scheduler.StateFailed {
			return true
		}
	}
	return false
}

func raiseReplayMetric(t tenant.Tenant, jobName scheduler.JobName, state scheduler.ReplayState) {
	telemetry.NewCounter(metricJobReplay, map[string]string{
		"project":   t.ProjectName().String(),
		"namespace": t.NamespaceName().String(),
		"job":       jobName.String(),
		"status":    state.String(),
	}).Inc()
}
