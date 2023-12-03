package service

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	"time"

	"github.com/goto/salt/log"
	"github.com/robfig/cron/v3"

	"github.com/goto/optimus/config"
	"github.com/goto/optimus/core/scheduler"
	"github.com/goto/optimus/internal/errors"
	cronInternal "github.com/goto/optimus/internal/lib/cron"
)

const (
	syncInterval      = "@every 10s"
	executionInterval = 5 * time.Second
)

type ReplayWorker struct {
	logger     log.Logger
	replayRepo ReplayRepository
	jobRepo    JobRepository
	scheduler  ReplayScheduler
	schedule   *cron.Cron
	config     config.ReplayConfig
}

func NewReplayWorker(logger log.Logger, replayRepository ReplayRepository, jobRepo JobRepository, scheduler ReplayScheduler, cfg config.ReplayConfig) *ReplayWorker {
	return &ReplayWorker{
		logger:     logger,
		jobRepo:    jobRepo,
		replayRepo: replayRepository,
		config:     cfg,
		scheduler:  scheduler,
		schedule: cron.New(cron.WithChain(
			cron.SkipIfStillRunning(cron.DefaultLogger),
		)),
	}
}

func (w *ReplayWorker) Execute(ctx context.Context, replayReq *scheduler.ReplayWithRun) {
	w.logger.Debug("[ReplayID: %s] starting to execute replay", replayReq.Replay.ID())

	jobCron, err := getJobCron(ctx, w.logger, w.jobRepo, replayReq.Replay.Tenant(), replayReq.Replay.JobName())
	w.logger.Debug("[ReplayID: %s] get job cron", replayReq.Replay.ID())
	if err != nil {
		w.logger.Error("[ReplayID: %s] unable to get cron value for job [%s]: %s", replayReq.Replay.ID().String(), replayReq.Replay.JobName().String(), err)
		if err := w.replayRepo.UpdateReplayStatus(ctx, replayReq.Replay.ID(), scheduler.ReplayStateFailed, err.Error()); err != nil {
			w.logger.Error("[ReplayID: %s] unable to update replay to failed: %s", replayReq.Replay.ID(), err.Error())
		}
		raiseReplayMetric(replayReq.Replay.Tenant(), replayReq.Replay.JobName(), scheduler.ReplayStateFailed)
		return
	}

	if err := w.startExecutionLoop(ctx, replayReq, jobCron); err != nil {
		w.logger.Error("[ReplayID: %s] unable to execute replay for job [%s]: %s", replayReq.Replay.ID().String(), replayReq.Replay.JobName().String(), err)
		if err := w.replayRepo.UpdateReplayStatus(ctx, replayReq.Replay.ID(), scheduler.ReplayStateFailed, err.Error()); err != nil {
			w.logger.Error("[ReplayID: %s] unable to update replay to failed: %s", replayReq.Replay.ID(), err.Error())
		}
		raiseReplayMetric(replayReq.Replay.Tenant(), replayReq.Replay.JobName(), scheduler.ReplayStateFailed)
	}
}

func (w *ReplayWorker) startExecutionLoop(ctx context.Context, replayWithRun *scheduler.ReplayWithRun, jobCron *cronInternal.ScheduleSpec) error {
	for {
		w.logger.Debug("[ReplayID: %s] execute replay proceed...", replayWithRun.Replay.ID())
		// if replay timed out
		select {
		case <-ctx.Done():
			w.logger.Debug("[ReplayID: %s] deadline encountered...", replayWithRun.Replay.ID())
			return ctx.Err()
		default:
		}

		// artificial delay
		time.Sleep(w.config.ReplayTimeout)

		// sync run first
		if storedReplayWithRun, err := w.replayRepo.GetReplayByID(ctx, replayWithRun.Replay.ID()); err != nil {
			w.logger.Error("unable to get existing runs for replay [%s]: %s", replayWithRun.Replay.ID().String(), err)
		} else {
			replayWithRun = storedReplayWithRun
		}
		incomingRuns, err := w.fetchRuns(ctx, replayWithRun, jobCron)
		if err != nil {
			w.logger.Error("unable to get incoming runs for replay [%s]: %s", replayWithRun.Replay.ID().String(), err)
		}
		existingRuns := replayWithRun.Runs
		syncedRunStatus := syncStatus(existingRuns, incomingRuns)
		if err := w.replayRepo.UpdateReplay(ctx, replayWithRun.Replay.ID(), scheduler.ReplayStateInProgress, syncedRunStatus, ""); err != nil {
			w.logger.Error("unable to update replay state to failed for replay_id [%s]: %s", replayWithRun.Replay.ID(), err)
			continue
		}
		w.logger.Debug("[ReplayID: %s] sync replay status %s", replayWithRun.Replay.ID(), toString(syncedRunStatus))

		// check if replay request is on termination state
		if isAllRunStatusTerminated(syncedRunStatus) {
			return w.finishReplay(ctx, replayWithRun.Replay.ID(), syncedRunStatus)
		}

		// pick runs to be triggered
		statesForReplay := []scheduler.State{scheduler.StatePending, scheduler.StateMissing}
		toBeReplayedRuns := scheduler.JobRunStatusList(syncedRunStatus).GetSortedRunsByStates(statesForReplay)
		w.logger.Debug("[ReplayID: %s] run to be replayed %s", replayWithRun.Replay.ID(), toString(toBeReplayedRuns))
		if len(toBeReplayedRuns) == 0 {
			continue
		}

		// execute replay run on scheduler
		var updatedRuns []*scheduler.JobRunStatus
		w.logger.Debug("[ReplayID: %s] execute on scheduler", replayWithRun.Replay.ID())
		if replayWithRun.Replay.Config().Parallel {
			w.replayRunOnScheduler(ctx, jobCron, replayWithRun.Replay, toBeReplayedRuns...)
			updatedRuns = scheduler.JobRunStatusList(toBeReplayedRuns).OverrideWithStatus(scheduler.StateInProgress)
		} else { // sequential should work when there's no in_progress state on existing runs
			inProgressRuns := scheduler.JobRunStatusList(syncedRunStatus).GetSortedRunsByStates([]scheduler.State{scheduler.StateInProgress})
			if len(inProgressRuns) > 0 {
				w.logger.Debug("[ReplayID: %s] skip sequential iteration", replayWithRun.Replay.ID())
				continue
			}
			w.replayRunOnScheduler(ctx, jobCron, replayWithRun.Replay, toBeReplayedRuns[0])
			updatedRuns = scheduler.JobRunStatusList(toBeReplayedRuns[:1]).OverrideWithStatus(scheduler.StateInProgress)
		}

		// update runs status
		if err := w.replayRepo.UpdateReplay(ctx, replayWithRun.Replay.ID(), scheduler.ReplayStateInProgress, updatedRuns, ""); err != nil {
			w.logger.Error("unable to update replay runs for replay_id [%s]: %s", replayWithRun.Replay.ID(), err)
		}
	}
}

func (w *ReplayWorker) finishReplay(ctx context.Context, replayID uuid.UUID, syncedRunStatus []*scheduler.JobRunStatus) error {
	w.logger.Debug("[ReplayID: %s] end of replay", replayID)
	replayState := scheduler.ReplayStateSuccess
	msg := ""
	if isAnyFailure(syncedRunStatus) {
		replayState = scheduler.ReplayStateFailed
		msg = "replay is failed due to some of runs are in failed statue" // TODO: find out how to pass the meaningful failed message here
	}

	if err := w.replayRepo.UpdateReplay(ctx, replayID, replayState, syncedRunStatus, msg); err != nil {
		w.logger.Error("unable to update replay state to failed for replay_id [%s]: %s", replayID, err)
		return err
	}
	return nil
}

func (w *ReplayWorker) fetchRuns(ctx context.Context, replayReq *scheduler.ReplayWithRun, jobCron *cronInternal.ScheduleSpec) ([]*scheduler.JobRunStatus, error) {
	jobRunCriteria := &scheduler.JobRunsCriteria{
		Name:      replayReq.Replay.JobName().String(),
		StartDate: replayReq.Replay.Config().StartTime,
		EndDate:   replayReq.Replay.Config().EndTime,
	}
	return w.scheduler.GetJobRuns(ctx, replayReq.Replay.Tenant(), jobRunCriteria, jobCron)
}

func (w *ReplayWorker) replayRunOnScheduler(ctx context.Context, jobCron *cronInternal.ScheduleSpec, replayReq *scheduler.Replay, runs ...*scheduler.JobRunStatus) error {
	// clear runs
	pendingRuns := scheduler.JobRunStatusList(runs).GetSortedRunsByStates([]scheduler.State{scheduler.StatePending})
	if l := len(pendingRuns); l > 0 {
		startLogicalTime := pendingRuns[0].GetLogicalTime(jobCron)
		endLogicalTime := pendingRuns[l-1].GetLogicalTime(jobCron)
		w.logger.Debug("[ReplayID: %s] startLogicalTime: %s, endLogicalTime: %s", replayReq.ID(), startLogicalTime, endLogicalTime)
		if err := w.scheduler.ClearBatch(ctx, replayReq.Tenant(), replayReq.JobName(), startLogicalTime, endLogicalTime); err != nil {
			w.logger.Error("unable to clear job run for replay with replay_id [%s]: %s", replayReq.ID(), err)
			return err
		}
	}

	// create missing runs
	missingRuns := scheduler.JobRunStatusList(runs).GetSortedRunsByStates([]scheduler.State{scheduler.StateMissing})
	me := errors.NewMultiError("create runs")
	for _, run := range missingRuns {
		if err := w.scheduler.CreateRun(ctx, replayReq.Tenant(), replayReq.JobName(), run.GetLogicalTime(jobCron), prefixReplayed); err != nil {
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

func toString(runs []*scheduler.JobRunStatus) string {
	s := "\n"
	for _, run := range runs {
		s += fmt.Sprintf("[%s] %s\n", run.ScheduledAt, run.State.String())
	}
	return s
}
