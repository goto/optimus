package service

import (
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/goto/salt/log"
	"golang.org/x/net/context"

	"github.com/goto/optimus/config"
	"github.com/goto/optimus/core/scheduler"
	"github.com/goto/optimus/core/tenant"
	"github.com/goto/optimus/internal/errors"
	"github.com/goto/optimus/internal/lib/cron"
)

type ReplayScheduler interface {
	Clear(ctx context.Context, t tenant.Tenant, jobName scheduler.JobName, scheduledAt time.Time) error
	ClearBatch(ctx context.Context, t tenant.Tenant, jobName scheduler.JobName, startTime, endTime time.Time) error

	GetJobRuns(ctx context.Context, t tenant.Tenant, criteria *scheduler.JobRunsCriteria, jobCron *cron.ScheduleSpec) ([]*scheduler.JobRunStatus, error)
}

type ReplayWorker struct {
	l log.Logger

	replayRepo ReplayRepository
	scheduler  ReplayScheduler

	jobRepo JobRepository

	config config.ReplayConfig
}

func NewReplayWorker(l log.Logger, replayRepo ReplayRepository, scheduler ReplayScheduler, jobRepo JobRepository, config config.ReplayConfig) *ReplayWorker {
	return &ReplayWorker{l: l, replayRepo: replayRepo, scheduler: scheduler, jobRepo: jobRepo, config: config}
}

type JobReplayRunService interface {
	GetJobRuns(ctx context.Context, projectName tenant.ProjectName, jobName scheduler.JobName, criteria *scheduler.JobRunsCriteria) ([]*scheduler.JobRunStatus, error)
}

func (w ReplayWorker) Process(replayReq *scheduler.ReplayWithRun) {
	ctx := context.Background()

	w.l.Debug("processing replay request %s with status %s", replayReq.Replay.ID().String(), replayReq.Replay.State().String())
	jobCron, err := w.getJobCron(ctx, replayReq)
	if err != nil {
		w.l.Error("unable to get cron value for job [%s] replay id [%s]: %s", replayReq.Replay.JobName().String(), replayReq.Replay.ID().String(), err)
		w.updateReplayAsFailed(ctx, replayReq.Replay.ID(), err.Error())
		return
	}

	switch replayReq.Replay.State() {
	case scheduler.ReplayStateCreated:
		err = w.processNewReplayRequest(ctx, replayReq, jobCron)
	case scheduler.ReplayStatePartialReplayed:
		err = w.processPartialReplayedRequest(ctx, replayReq, jobCron)
	case scheduler.ReplayStateReplayed:
		err = w.processReplayedRequest(ctx, replayReq, jobCron)
	}

	if err != nil {
		w.l.Error("error encountered when processing replay request: %s", err)
		w.updateReplayAsFailed(ctx, replayReq.Replay.ID(), err.Error())
	}
}

func (w ReplayWorker) processNewReplayRequest(ctx context.Context, replayReq *scheduler.ReplayWithRun, jobCron *cron.ScheduleSpec) (err error) {
	state := scheduler.ReplayStateReplayed
	var updatedRuns []*scheduler.JobRunStatus
	if replayReq.Replay.Config().Parallel {
		updatedRuns, err = w.processNewReplayRequestParallel(ctx, replayReq, jobCron)
	} else {
		updatedRuns, err = w.processNewReplayRequestSequential(ctx, replayReq, jobCron)
		if len(replayReq.Runs) > 1 {
			state = scheduler.ReplayStatePartialReplayed
		}
	}
	if err != nil {
		w.l.Error("error processing new replay: %s", err)
		return err
	}
	if err := w.replayRepo.UpdateReplay(ctx, replayReq.Replay.ID(), state, updatedRuns, ""); err != nil {
		w.l.Error("unable to update replay state for replay_id [%s]: %s", replayReq.Replay.ID().String(), err)
		return err
	}
	return nil
}

func (w ReplayWorker) processNewReplayRequestParallel(ctx context.Context, replayReq *scheduler.ReplayWithRun, jobCron *cron.ScheduleSpec) ([]*scheduler.JobRunStatus, error) {
	startLogicalTime := replayReq.GetFirstExecutableRun().GetLogicalTime(jobCron)
	endLogicalTime := replayReq.GetLastExecutableRun().GetLogicalTime(jobCron)
	if err := w.scheduler.ClearBatch(ctx, replayReq.Replay.Tenant(), replayReq.Replay.JobName(), startLogicalTime, endLogicalTime); err != nil {
		w.l.Error("unable to clear job run for replay with replay_id [%s]: %s", replayReq.Replay.ID().String(), err)
		return nil, err
	}

	w.l.Info("cleared [%s] runs for replay [%s]", replayReq.Replay.JobName().String(), replayReq.Replay.ID().String())

	var updatedRuns []*scheduler.JobRunStatus
	for _, run := range replayReq.Runs {
		updatedRuns = append(updatedRuns, &scheduler.JobRunStatus{ScheduledAt: run.ScheduledAt, State: scheduler.StateReplayed})
	}
	return updatedRuns, nil
}

func (w ReplayWorker) processNewReplayRequestSequential(ctx context.Context, replayReq *scheduler.ReplayWithRun, jobCron *cron.ScheduleSpec) ([]*scheduler.JobRunStatus, error) {
	runToClear := replayReq.GetFirstExecutableRun()
	if err := w.scheduler.Clear(ctx, replayReq.Replay.Tenant(), replayReq.Replay.JobName(), runToClear.GetLogicalTime(jobCron)); err != nil {
		w.l.Error("unable to clear job run for replay with replay_id [%s]: %s", replayReq.Replay.ID().String(), err)
		return nil, err
	}

	w.l.Info("cleared [%s] [%s] run for replay %s", replayReq.Replay.JobName().String(), runToClear.ScheduledAt, replayReq.Replay.ID().String())
	updatedReplayMap := map[time.Time]scheduler.State{
		runToClear.ScheduledAt: scheduler.StateReplayed,
	}
	updatedRuns := scheduler.JobRunStatusList(replayReq.Runs).MergeWithUpdatedRuns(updatedReplayMap)
	return updatedRuns, nil
}

func (w ReplayWorker) processPartialReplayedRequest(ctx context.Context, replayReq *scheduler.ReplayWithRun, jobCron *cron.ScheduleSpec) error {
	incomingRuns, err := w.fetchRuns(ctx, replayReq, jobCron)
	if err != nil {
		w.l.Error("unable to get runs for replay [%s]: %s", replayReq.Replay.ID().String(), err)
		return err
	}

	updatedReplayMap := identifyUpdatedRunStatus(replayReq.Runs, incomingRuns)
	updatedRuns := scheduler.JobRunStatusList(replayReq.Runs).MergeWithUpdatedRuns(updatedReplayMap)

	replayedRuns := scheduler.JobRunStatusList(updatedRuns).GetSortedRunsByStates([]scheduler.State{scheduler.StateReplayed})
	toBeReplayedRuns := scheduler.JobRunStatusList(updatedRuns).GetSortedRunsByStates([]scheduler.State{scheduler.StatePending})

	replayState := scheduler.ReplayStatePartialReplayed
	if len(replayedRuns) == 0 && len(toBeReplayedRuns) > 0 {
		logicalTimeToClear := toBeReplayedRuns[0].GetLogicalTime(jobCron)
		if err := w.scheduler.Clear(ctx, replayReq.Replay.Tenant(), replayReq.Replay.JobName(), logicalTimeToClear); err != nil {
			w.l.Error("unable to clear job run for replay_id [%s]: %s", replayReq.Replay.ID().String(), err)
			return err
		}
		w.l.Info("cleared [%s] [%s] run for replay %s", replayReq.Replay.JobName().String(), toBeReplayedRuns[0].ScheduledAt, replayReq.Replay.ID().String())

		updatedReplayMap[toBeReplayedRuns[0].ScheduledAt] = scheduler.StateReplayed
		updatedRuns = scheduler.JobRunStatusList(updatedRuns).MergeWithUpdatedRuns(updatedReplayMap)
	}

	pendingRuns := scheduler.JobRunStatusList(updatedRuns).GetSortedRunsByStates([]scheduler.State{scheduler.StatePending})
	if len(pendingRuns) == 0 {
		replayState = scheduler.ReplayStateReplayed
	}

	if err := w.replayRepo.UpdateReplay(ctx, replayReq.Replay.ID(), replayState, updatedRuns, ""); err != nil {
		w.l.Error("unable to update replay state for replay_id [%s]: %s", replayReq.Replay.ID().String(), err)
		return err
	}
	return nil
}

func (w ReplayWorker) processReplayedRequest(ctx context.Context, replayReq *scheduler.ReplayWithRun, jobCron *cron.ScheduleSpec) error {
	incomingRuns, err := w.fetchRuns(ctx, replayReq, jobCron)
	if err != nil {
		w.l.Error("unable to get runs for replay with replay_id [%s]: %s", replayReq.Replay.ID().String(), err)
		return err
	}

	updatedReplayMap := identifyUpdatedRunStatus(replayReq.Runs, incomingRuns)
	updatedRuns := scheduler.JobRunStatusList(replayReq.Runs).MergeWithUpdatedRuns(updatedReplayMap)
	inProgressRuns := scheduler.JobRunStatusList(updatedRuns).GetSortedRunsByStates([]scheduler.State{scheduler.StateReplayed})
	failedRuns := scheduler.JobRunStatusList(updatedRuns).GetSortedRunsByStates([]scheduler.State{scheduler.StateFailed})

	var message string
	state := scheduler.ReplayStateReplayed
	if len(inProgressRuns) == 0 && len(failedRuns) == 0 {
		state = scheduler.ReplayStateSuccess
		w.l.Info("marking replay %s as success", replayReq.Replay.ID().String())
	} else if len(inProgressRuns) == 0 && len(failedRuns) > 0 {
		state = scheduler.ReplayStateFailed
		message = fmt.Sprintf("found %d failed runs.", len(failedRuns))
		w.l.Info("marking replay %s as failed", replayReq.Replay.ID().String())
	}

	if err := w.replayRepo.UpdateReplay(ctx, replayReq.Replay.ID(), state, updatedRuns, message); err != nil {
		w.l.Error("unable to update replay with replay_id [%s]: %s", replayReq.Replay.ID().String(), err)
		return err
	}
	return nil
}

func identifyUpdatedRunStatus(existingJobRuns, incomingJobRuns []*scheduler.JobRunStatus) map[time.Time]scheduler.State {
	incomingRunStatusMap := scheduler.JobRunStatusList(incomingJobRuns).ToRunStatusMap()

	updatedReplayMap := make(map[time.Time]scheduler.State)
	for _, run := range existingJobRuns {
		if run.State != scheduler.StateReplayed {
			continue
		}
		if incomingRunStatusMap[run.ScheduledAt.UTC()] == scheduler.StateSuccess || incomingRunStatusMap[run.ScheduledAt.UTC()] == scheduler.StateFailed {
			updatedReplayMap[run.ScheduledAt.UTC()] = incomingRunStatusMap[run.ScheduledAt.UTC()]
		}
	}
	return updatedReplayMap
}

func (w ReplayWorker) getJobCron(ctx context.Context, replayReq *scheduler.ReplayWithRun) (*cron.ScheduleSpec, error) {
	jobWithDetails, err := w.jobRepo.GetJobDetails(ctx, replayReq.Replay.Tenant().ProjectName(), replayReq.Replay.JobName())
	if err != nil || jobWithDetails == nil {
		return nil, errors.AddErrContext(err, scheduler.EntityReplay,
			fmt.Sprintf("unable to get job details for jobName: %s, project: %s", replayReq.Replay.JobName(), replayReq.Replay.Tenant().ProjectName()))
	}
	interval := jobWithDetails.Schedule.Interval
	if interval == "" {
		w.l.Error("job interval is empty")
		return nil, errors.InvalidArgument(scheduler.EntityReplay, "job schedule interval is empty")
	}
	jobCron, err := cron.ParseCronSchedule(interval)
	if err != nil {
		w.l.Error("error parsing cron interval: %s", err)
		return nil, errors.InternalError(scheduler.EntityReplay, "unable to parse job cron interval", err)
	}
	return jobCron, nil
}

func (w ReplayWorker) fetchRuns(ctx context.Context, replayReq *scheduler.ReplayWithRun, jobCron *cron.ScheduleSpec) ([]*scheduler.JobRunStatus, error) {
	jobRunCriteria := &scheduler.JobRunsCriteria{
		Name:      replayReq.Replay.JobName().String(),
		StartDate: replayReq.Replay.Config().StartTime,
		EndDate:   replayReq.Replay.Config().EndTime,
	}
	return w.scheduler.GetJobRuns(ctx, replayReq.Replay.Tenant(), jobRunCriteria, jobCron)
}

func (w ReplayWorker) updateReplayAsFailed(ctx context.Context, replayID uuid.UUID, message string) {
	if err := w.replayRepo.UpdateReplayStatus(ctx, replayID, scheduler.ReplayStateFailed, message); err != nil {
		w.l.Error("unable to update replay state to failed for replay_id [%s]: %s", replayID, err)
	}
}
