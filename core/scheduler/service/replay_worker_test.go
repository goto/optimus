package service_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/goto/salt/log"
	"github.com/stretchr/testify/mock"

	"github.com/goto/optimus/config"
	"github.com/goto/optimus/core/scheduler"
	"github.com/goto/optimus/core/scheduler/service"
	"github.com/goto/optimus/core/tenant"
	"github.com/goto/optimus/internal/lib/cron"
)

func TestReplayWorker(t *testing.T) {
	logger := log.NewNoop()
	now := time.Now()
	replayServerConfig := config.ReplayConfig{
		ExecutionIntervalInSeconds: 1,
		ReplayTimeoutInMinutes:     5,
	}

	projName := tenant.ProjectName("proj")
	namespaceName := tenant.ProjectName("ns1")
	tnnt, _ := tenant.NewTenant(projName.String(), namespaceName.String())

	startTimeStr := "2023-01-02T00:00:00Z"
	startTime, _ := time.Parse(scheduler.ISODateFormat, startTimeStr)
	endTime := startTime.Add(48 * time.Hour)

	scheduledTimeStr1 := "2023-01-02T12:00:00Z"
	scheduledTimeStr2 := "2023-01-03T12:00:00Z"
	scheduledTime1, _ := time.Parse(scheduler.ISODateFormat, scheduledTimeStr1)
	scheduledTime2, _ := time.Parse(scheduler.ISODateFormat, scheduledTimeStr2)

	jobCronStr := "0 12 * * *"
	jobCron, _ := cron.ParseCronSchedule(jobCronStr)

	jobAName, _ := scheduler.JobNameFrom("job-a")
	jobA := scheduler.Job{
		Name:   jobAName,
		Tenant: tnnt,
	}
	jobAWithDetails := &scheduler.JobWithDetails{
		Job: &jobA,
		JobMetadata: &scheduler.JobMetadata{
			Version: 1,
		},
		Schedule: &scheduler.Schedule{
			StartDate: startTime.Add(-time.Hour * 24),
			Interval:  jobCronStr,
		},
	}
	message := "sample message"

	replayJobConfig := map[string]string{"EXECUTION_PROJECT": "example_project"}
	replayDescription := "sample backfill"
	replayConfig := scheduler.NewReplayConfig(startTime, endTime, false, replayJobConfig, replayDescription)
	replayConfigParallel := scheduler.NewReplayConfig(startTime, endTime, true, replayJobConfig, replayDescription)

	t.Run("Execute", func(t *testing.T) {
		t.Run("should able to process sequential replay request with single run", func(t *testing.T) {
			replayRepository := new(ReplayRepository)
			defer replayRepository.AssertExpectations(t)

			sch := new(mockReplayScheduler)
			defer sch.AssertExpectations(t)

			jobRepository := new(JobRepository)
			defer jobRepository.AssertExpectations(t)

			runsPhaseOne := []*scheduler.JobRunStatus{{ScheduledAt: scheduledTime1, State: scheduler.StatePending}}
			runsPhaseTwo := []*scheduler.JobRunStatus{{ScheduledAt: scheduledTime1, State: scheduler.StateInProgress}}
			runsPhaseThree := []*scheduler.JobRunStatus{{ScheduledAt: scheduledTime1, State: scheduler.StateSuccess}}

			schedulerRunsPhaseOne := []*scheduler.JobRunStatus{{ScheduledAt: scheduledTime1, State: scheduler.StateFailed}}

			replayID := uuid.New()
			replay := scheduler.NewReplay(replayID, jobAName, tnnt, replayConfig, scheduler.ReplayStateCreated, time.Now(), now, message)
			replayReq := &scheduler.ReplayWithRun{
				Replay: replay,
				Runs:   runsPhaseOne,
			}
			replayPhaseTwo := &scheduler.ReplayWithRun{
				Replay: scheduler.NewReplay(replayID, jobAName, tnnt, replayConfig, scheduler.ReplayStateInProgress, time.Now(), now, message),
				Runs:   runsPhaseTwo,
			}

			// for cancelled check
			replayRepository.On("GetReplayRequestByID", mock.Anything, replayReq.Replay.ID()).Return(replay, nil)

			// loop 1
			replayRepository.On("UpdateReplayStatus", mock.Anything, replayReq.Replay.ID(), scheduler.ReplayStateInProgress, "started handling replay request").Return(nil).Once()

			jobRepository.On("GetJobDetails", mock.Anything, projName, jobAName).Return(jobAWithDetails, nil).Once()
			replayRepository.On("GetReplayByID", mock.Anything, replayReq.Replay.ID()).Return(replayReq, nil).Once()
			sch.On("GetJobRuns", mock.Anything, tnnt, mock.Anything, jobCron).Return(schedulerRunsPhaseOne, nil).Once()
			replayRepository.On("UpdateReplayRuns", mock.Anything, replayReq.Replay.ID(), runsPhaseOne).Return(nil).Once()
			sch.On("ClearBatch", mock.Anything, tnnt, jobAName, scheduledTime1.Add(-24*time.Hour), scheduledTime1.Add(-24*time.Hour)).Return(nil).Once()
			replayRepository.On("UpdateReplayRuns", mock.Anything, replayReq.Replay.ID(), runsPhaseTwo).Return(nil).Once()

			// loop 2
			replayRepository.On("GetReplayByID", mock.Anything, replayReq.Replay.ID()).Return(replayPhaseTwo, nil).Once()
			sch.On("GetJobRuns", mock.Anything, tnnt, mock.Anything, jobCron).Return(runsPhaseThree, nil).Once()
			replayRepository.On("UpdateReplayRuns", mock.Anything, replayReq.Replay.ID(), runsPhaseThree).Return(nil).Once()
			summaryMsg := "replay is finished with run status: success(1)"
			replayRepository.On("UpdateReplay", mock.Anything, replayReq.Replay.ID(), scheduler.ReplayStateSuccess, runsPhaseThree, summaryMsg).Return(nil).Once()
			replayRepository.On("UpdateReplayHeartbeat", mock.Anything, replayReq.Replay.ID()).Return(nil).Once()

			alertManager := new(mockAlertManager)
			alertManager.On("SendReplayEvent", mock.Anything).Return()
			defer alertManager.AssertExpectations(t)

			worker := service.NewReplayWorker(logger, replayRepository, jobRepository, sch, replayServerConfig, alertManager)
			worker.Execute(replayID, tnnt, jobAName)
		})
		t.Run("should able to process sequential replay request with multiple run", func(t *testing.T) {
			replayRepository := new(ReplayRepository)
			defer replayRepository.AssertExpectations(t)

			sch := new(mockReplayScheduler)
			defer sch.AssertExpectations(t)

			jobRepository := new(JobRepository)
			defer jobRepository.AssertExpectations(t)

			runsPhase1 := []*scheduler.JobRunStatus{
				{ScheduledAt: scheduledTime1, State: scheduler.StatePending},
				{ScheduledAt: scheduledTime2, State: scheduler.StatePending},
			}
			runsPhase2 := []*scheduler.JobRunStatus{
				{ScheduledAt: scheduledTime1, State: scheduler.StateInProgress},
				{ScheduledAt: scheduledTime2, State: scheduler.StatePending},
			}
			runsPhase3 := []*scheduler.JobRunStatus{
				{ScheduledAt: scheduledTime1, State: scheduler.StateSuccess},
				{ScheduledAt: scheduledTime2, State: scheduler.StateInProgress},
			}

			replayID := uuid.New()
			replay := scheduler.NewReplay(replayID, jobAName, tnnt, replayConfig, scheduler.ReplayStateCreated, time.Now(), now, message)
			replayReq := &scheduler.ReplayWithRun{
				Replay: replay,
				Runs:   runsPhase1,
			}
			replayPhase2 := &scheduler.ReplayWithRun{
				Replay: scheduler.NewReplay(replayID, jobAName, tnnt, replayConfig, scheduler.ReplayStateInProgress, time.Now(), now, message),
				Runs:   runsPhase2,
			}
			replayPhase3 := &scheduler.ReplayWithRun{
				Replay: scheduler.NewReplay(replayID, jobAName, tnnt, replayConfig, scheduler.ReplayStateInProgress, time.Now(), now, message),
				Runs:   runsPhase3,
			}

			schedulerRunsPhase1 := []*scheduler.JobRunStatus{
				{ScheduledAt: scheduledTime1, State: scheduler.StateFailed},
				{ScheduledAt: scheduledTime2, State: scheduler.StateFailed},
			}
			schedulerRunsPhase2 := []*scheduler.JobRunStatus{
				{ScheduledAt: scheduledTime1, State: scheduler.StateSuccess},
				{ScheduledAt: scheduledTime2, State: scheduler.StateFailed},
			}
			schedulerRunsPhase3 := []*scheduler.JobRunStatus{
				{ScheduledAt: scheduledTime1, State: scheduler.StateSuccess},
				{ScheduledAt: scheduledTime2, State: scheduler.StateSuccess},
			}

			// for cancelled check
			replayRepository.On("GetReplayRequestByID", mock.Anything, replayReq.Replay.ID()).Return(replay, nil)

			// loop 1
			replayRepository.On("UpdateReplayStatus", mock.Anything, replayReq.Replay.ID(), scheduler.ReplayStateInProgress, "started handling replay request").Return(nil).Once()

			jobRepository.On("GetJobDetails", mock.Anything, projName, jobAName).Return(jobAWithDetails, nil).Once()
			replayRepository.On("GetReplayByID", mock.Anything, replayReq.Replay.ID()).Return(replayReq, nil).Once()
			sch.On("GetJobRuns", mock.Anything, tnnt, mock.Anything, jobCron).Return(schedulerRunsPhase1, nil).Once()
			replayRepository.On("UpdateReplayRuns", mock.Anything, replayReq.Replay.ID(), mock.Anything).Return(nil).Once()
			sch.On("ClearBatch", mock.Anything, tnnt, jobAName, scheduledTime1.Add(-24*time.Hour), scheduledTime1.Add(-24*time.Hour)).Return(nil).Once()
			replayRepository.On("UpdateReplayRuns", mock.Anything, replayReq.Replay.ID(), []*scheduler.JobRunStatus{runsPhase2[0]}).Return(nil).Once()

			// loop 2
			replayRepository.On("GetReplayByID", mock.Anything, replayReq.Replay.ID()).Return(replayPhase2, nil).Once()
			sch.On("GetJobRuns", mock.Anything, tnnt, mock.Anything, jobCron).Return(schedulerRunsPhase2, nil).Once()
			replayRepository.On("UpdateReplayRuns", mock.Anything, replayReq.Replay.ID(), mock.Anything).Return(nil).Once()
			sch.On("ClearBatch", mock.Anything, tnnt, jobAName, scheduledTime2.Add(-24*time.Hour), scheduledTime2.Add(-24*time.Hour)).Return(nil).Once()
			replayRepository.On("UpdateReplayRuns", mock.Anything, replayReq.Replay.ID(), []*scheduler.JobRunStatus{runsPhase3[1]}).Return(nil).Once()
			replayRepository.On("UpdateReplayHeartbeat", mock.Anything, replayReq.Replay.ID()).Return(nil).Once()

			// loop 3
			replayRepository.On("GetReplayByID", mock.Anything, replayReq.Replay.ID()).Return(replayPhase3, nil).Once()
			sch.On("GetJobRuns", mock.Anything, tnnt, mock.Anything, jobCron).Return(schedulerRunsPhase3, nil).Once()
			replayRepository.On("UpdateReplayRuns", mock.Anything, replayReq.Replay.ID(), mock.Anything).Return(nil).Once()
			summaryMsg := "replay is finished with run status: success(2)"
			replayRepository.On("UpdateReplay", mock.Anything, replayReq.Replay.ID(), scheduler.ReplayStateSuccess, mock.Anything, summaryMsg).Return(nil).Once()
			replayRepository.On("UpdateReplayHeartbeat", mock.Anything, replayReq.Replay.ID()).Return(nil).Once()

			alertManager := new(mockAlertManager)
			alertManager.On("SendReplayEvent", mock.Anything).Return()
			defer alertManager.AssertExpectations(t)

			worker := service.NewReplayWorker(logger, replayRepository, jobRepository, sch, replayServerConfig, alertManager)
			worker.Execute(replayID, tnnt, jobAName)
		})
		t.Run("should able to process parallel replay request", func(t *testing.T) {
			replayRepository := new(ReplayRepository)
			defer replayRepository.AssertExpectations(t)

			sch := new(mockReplayScheduler)
			defer sch.AssertExpectations(t)

			jobRepository := new(JobRepository)
			defer jobRepository.AssertExpectations(t)

			runsPhase1 := []*scheduler.JobRunStatus{
				{ScheduledAt: scheduledTime1, State: scheduler.StatePending},
				{ScheduledAt: scheduledTime2, State: scheduler.StatePending},
			}
			runsPhase2 := []*scheduler.JobRunStatus{
				{ScheduledAt: scheduledTime1, State: scheduler.StateInProgress},
				{ScheduledAt: scheduledTime2, State: scheduler.StateInProgress},
			}
			runsPhase3 := []*scheduler.JobRunStatus{
				{ScheduledAt: scheduledTime1, State: scheduler.StateSuccess},
				{ScheduledAt: scheduledTime2, State: scheduler.StateInProgress},
			}

			replayID := uuid.New()
			replay := scheduler.NewReplay(replayID, jobAName, tnnt, replayConfigParallel, scheduler.ReplayStateCreated, time.Now(), now, message)
			replayReq := &scheduler.ReplayWithRun{
				Replay: replay,
				Runs:   runsPhase1,
			}
			replayPhase2 := &scheduler.ReplayWithRun{
				Replay: scheduler.NewReplay(replayID, jobAName, tnnt, replayConfigParallel, scheduler.ReplayStateInProgress, time.Now(), now, message),
				Runs:   runsPhase2,
			}
			replayPhase3 := &scheduler.ReplayWithRun{
				Replay: scheduler.NewReplay(replayID, jobAName, tnnt, replayConfigParallel, scheduler.ReplayStateInProgress, time.Now(), now, message),
				Runs:   runsPhase3,
			}

			schedulerRunsPhase1 := []*scheduler.JobRunStatus{
				{ScheduledAt: scheduledTime1, State: scheduler.StateFailed},
				{ScheduledAt: scheduledTime2, State: scheduler.StateFailed},
			}
			schedulerRunsPhase2 := []*scheduler.JobRunStatus{
				{ScheduledAt: scheduledTime1, State: scheduler.StateSuccess},
				{ScheduledAt: scheduledTime2, State: scheduler.StateRunning},
			}
			schedulerRunsPhase3 := []*scheduler.JobRunStatus{
				{ScheduledAt: scheduledTime1, State: scheduler.StateSuccess},
				{ScheduledAt: scheduledTime2, State: scheduler.StateSuccess},
			}

			// for cancelled check
			replayRepository.On("GetReplayRequestByID", mock.Anything, replayReq.Replay.ID()).Return(replay, nil)

			// loop 1
			replayRepository.On("UpdateReplayStatus", mock.Anything, replayReq.Replay.ID(), scheduler.ReplayStateInProgress, "started handling replay request").Return(nil).Once()

			jobRepository.On("GetJobDetails", mock.Anything, projName, jobAName).Return(jobAWithDetails, nil).Once()
			replayRepository.On("GetReplayByID", mock.Anything, replayReq.Replay.ID()).Return(replayReq, nil).Once()
			sch.On("GetJobRuns", mock.Anything, tnnt, mock.Anything, jobCron).Return(schedulerRunsPhase1, nil).Once()
			replayRepository.On("UpdateReplayRuns", mock.Anything, replayReq.Replay.ID(), mock.Anything).Return(nil).Once()
			sch.On("ClearBatch", mock.Anything, tnnt, jobAName, scheduledTime1.Add(-24*time.Hour), scheduledTime2.Add(-24*time.Hour)).Return(nil).Once()
			replayRepository.On("UpdateReplayRuns", mock.Anything, replayReq.Replay.ID(), mock.Anything).Return(nil).Once()

			// loop 2
			replayRepository.On("GetReplayByID", mock.Anything, replayReq.Replay.ID()).Return(replayPhase2, nil).Once()
			sch.On("GetJobRuns", mock.Anything, tnnt, mock.Anything, jobCron).Return(schedulerRunsPhase2, nil).Once()
			replayRepository.On("UpdateReplayRuns", mock.Anything, replayReq.Replay.ID(), mock.Anything).Return(nil).Once()
			replayRepository.On("UpdateReplayHeartbeat", mock.Anything, replayReq.Replay.ID()).Return(nil).Once()

			// loop 3
			replayRepository.On("GetReplayByID", mock.Anything, replayReq.Replay.ID()).Return(replayPhase3, nil).Once()
			sch.On("GetJobRuns", mock.Anything, tnnt, mock.Anything, jobCron).Return(schedulerRunsPhase3, nil).Once()
			replayRepository.On("UpdateReplayRuns", mock.Anything, replayReq.Replay.ID(), mock.Anything).Return(nil).Once()
			summaryMsg := "replay is finished with run status: success(2)"
			replayRepository.On("UpdateReplayHeartbeat", mock.Anything, replayReq.Replay.ID()).Return(nil).Once()
			replayRepository.On("UpdateReplay", mock.Anything, replayReq.Replay.ID(), scheduler.ReplayStateSuccess, mock.Anything, summaryMsg).Return(nil).Once()

			alertManager := new(mockAlertManager)
			alertManager.On("SendReplayEvent", mock.Anything).Return()
			defer alertManager.AssertExpectations(t)

			worker := service.NewReplayWorker(logger, replayRepository, jobRepository, sch, replayServerConfig, alertManager)
			worker.Execute(replayID, tnnt, jobAName)
		})

		t.Run("should able to process replay request with sequential mode and creating non existing runs", func(t *testing.T) {
			replayRepository := new(ReplayRepository)
			defer replayRepository.AssertExpectations(t)

			sch := new(mockReplayScheduler)
			defer sch.AssertExpectations(t)

			jobRepository := new(JobRepository)
			defer jobRepository.AssertExpectations(t)

			runsPhase1 := []*scheduler.JobRunStatus{
				{ScheduledAt: scheduledTime1, State: scheduler.StatePending},
				{ScheduledAt: scheduledTime2, State: scheduler.StatePending},
			}
			runsPhase2 := []*scheduler.JobRunStatus{
				{ScheduledAt: scheduledTime1, State: scheduler.StateInProgress},
				{ScheduledAt: scheduledTime2, State: scheduler.StatePending},
			}
			runsPhase3 := []*scheduler.JobRunStatus{
				{ScheduledAt: scheduledTime1, State: scheduler.StateSuccess},
				{ScheduledAt: scheduledTime2, State: scheduler.StateInProgress},
			}

			replayID := uuid.New()
			replay := scheduler.NewReplay(replayID, jobAName, tnnt, replayConfig, scheduler.ReplayStateCreated, time.Now(), now, message)
			replayReq := &scheduler.ReplayWithRun{
				Replay: replay,
				Runs:   runsPhase1,
			}
			replayPhase2 := &scheduler.ReplayWithRun{
				Replay: scheduler.NewReplay(replayID, jobAName, tnnt, replayConfig, scheduler.ReplayStateInProgress, time.Now(), now, message),
				Runs:   runsPhase2,
			}
			replayPhase3 := &scheduler.ReplayWithRun{
				Replay: scheduler.NewReplay(replayID, jobAName, tnnt, replayConfig, scheduler.ReplayStateInProgress, time.Now(), now, message),
				Runs:   runsPhase3,
			}

			schedulerRunsPhase1 := []*scheduler.JobRunStatus{
				{ScheduledAt: scheduledTime2, State: scheduler.StateFailed},
			}
			schedulerRunsPhase2 := []*scheduler.JobRunStatus{
				{ScheduledAt: scheduledTime1, State: scheduler.StateSuccess},
				{ScheduledAt: scheduledTime2, State: scheduler.StateFailed},
			}
			schedulerRunsPhase3 := []*scheduler.JobRunStatus{
				{ScheduledAt: scheduledTime1, State: scheduler.StateSuccess},
				{ScheduledAt: scheduledTime2, State: scheduler.StateSuccess},
			}

			// for cancelled check
			replayRepository.On("GetReplayRequestByID", mock.Anything, replayReq.Replay.ID()).Return(replay, nil)

			// loop 1
			replayRepository.On("UpdateReplayStatus", mock.Anything, replayReq.Replay.ID(), scheduler.ReplayStateInProgress, "started handling replay request").Return(nil).Once()

			jobRepository.On("GetJobDetails", mock.Anything, projName, jobAName).Return(jobAWithDetails, nil).Once()
			replayRepository.On("GetReplayByID", mock.Anything, replayReq.Replay.ID()).Return(replayReq, nil).Once()
			sch.On("GetJobRuns", mock.Anything, tnnt, mock.Anything, jobCron).Return(schedulerRunsPhase1, nil).Once()
			replayRepository.On("UpdateReplayRuns", mock.Anything, replayReq.Replay.ID(), mock.Anything).Return(nil).Once()
			sch.On("CreateRun", mock.Anything, tnnt, jobAName, scheduledTime1.Add(-24*time.Hour), "replayed").Return(nil).Once()
			replayRepository.On("UpdateReplayRuns", mock.Anything, replayReq.Replay.ID(), []*scheduler.JobRunStatus{runsPhase2[0]}).Return(nil).Once()

			// loop 2
			replayRepository.On("GetReplayByID", mock.Anything, replayReq.Replay.ID()).Return(replayPhase2, nil).Once()
			sch.On("GetJobRuns", mock.Anything, tnnt, mock.Anything, jobCron).Return(schedulerRunsPhase2, nil).Once()
			replayRepository.On("UpdateReplayRuns", mock.Anything, replayReq.Replay.ID(), mock.Anything).Return(nil).Once()
			sch.On("ClearBatch", mock.Anything, tnnt, jobAName, scheduledTime2.Add(-24*time.Hour), scheduledTime2.Add(-24*time.Hour)).Return(nil).Once()
			replayRepository.On("UpdateReplayRuns", mock.Anything, replayReq.Replay.ID(), []*scheduler.JobRunStatus{runsPhase3[1]}).Return(nil).Once()
			replayRepository.On("UpdateReplayHeartbeat", mock.Anything, replayReq.Replay.ID()).Return(nil).Once()

			// loop 3
			replayRepository.On("GetReplayByID", mock.Anything, replayReq.Replay.ID()).Return(replayPhase3, nil).Once()
			sch.On("GetJobRuns", mock.Anything, tnnt, mock.Anything, jobCron).Return(schedulerRunsPhase3, nil).Once()
			replayRepository.On("UpdateReplayRuns", mock.Anything, replayReq.Replay.ID(), mock.Anything).Return(nil).Once()
			summaryMsg := "replay is finished with run status: success(2)"
			replayRepository.On("UpdateReplay", mock.Anything, replayReq.Replay.ID(), scheduler.ReplayStateSuccess, mock.Anything, summaryMsg).Return(nil).Once()
			replayRepository.On("UpdateReplayHeartbeat", mock.Anything, replayReq.Replay.ID()).Return(nil).Once()
			alertManager := new(mockAlertManager)
			alertManager.On("SendReplayEvent", mock.Anything).Return()
			defer alertManager.AssertExpectations(t)
			worker := service.NewReplayWorker(logger, replayRepository, jobRepository, sch, replayServerConfig, alertManager)
			worker.Execute(replayID, tnnt, jobAName)
		})
		t.Run("should able to process replay request with parallel mode and creating non existing runs", func(t *testing.T) {
			replayRepository := new(ReplayRepository)
			defer replayRepository.AssertExpectations(t)

			sch := new(mockReplayScheduler)
			defer sch.AssertExpectations(t)

			jobRepository := new(JobRepository)
			defer jobRepository.AssertExpectations(t)

			runsPhase1 := []*scheduler.JobRunStatus{
				{ScheduledAt: scheduledTime1, State: scheduler.StatePending},
				{ScheduledAt: scheduledTime2, State: scheduler.StatePending},
			}
			runsPhase2 := []*scheduler.JobRunStatus{
				{ScheduledAt: scheduledTime1, State: scheduler.StateInProgress},
				{ScheduledAt: scheduledTime2, State: scheduler.StateInProgress},
			}
			runsPhase3 := []*scheduler.JobRunStatus{
				{ScheduledAt: scheduledTime1, State: scheduler.StateSuccess},
				{ScheduledAt: scheduledTime2, State: scheduler.StateInProgress},
			}

			replayID := uuid.New()
			replay := scheduler.NewReplay(replayID, jobAName, tnnt, replayConfigParallel, scheduler.ReplayStateCreated, time.Now(), now, message)
			replayReq := &scheduler.ReplayWithRun{
				Replay: replay,
				Runs:   runsPhase1,
			}
			replayPhase2 := &scheduler.ReplayWithRun{
				Replay: scheduler.NewReplay(replayID, jobAName, tnnt, replayConfigParallel, scheduler.ReplayStateInProgress, time.Now(), now, message),
				Runs:   runsPhase2,
			}
			replayPhase3 := &scheduler.ReplayWithRun{
				Replay: scheduler.NewReplay(replayID, jobAName, tnnt, replayConfigParallel, scheduler.ReplayStateInProgress, time.Now(), now, message),
				Runs:   runsPhase3,
			}

			schedulerRunsPhase1 := []*scheduler.JobRunStatus{
				{ScheduledAt: scheduledTime2, State: scheduler.StateFailed},
			}
			schedulerRunsPhase2 := []*scheduler.JobRunStatus{
				{ScheduledAt: scheduledTime1, State: scheduler.StateSuccess},
				{ScheduledAt: scheduledTime2, State: scheduler.StateRunning},
			}
			schedulerRunsPhase3 := []*scheduler.JobRunStatus{
				{ScheduledAt: scheduledTime1, State: scheduler.StateSuccess},
				{ScheduledAt: scheduledTime2, State: scheduler.StateSuccess},
			}

			// for cancelled check
			replayRepository.On("GetReplayRequestByID", mock.Anything, replayReq.Replay.ID()).Return(replay, nil)

			// loop 1
			replayRepository.On("UpdateReplayStatus", mock.Anything, replayReq.Replay.ID(), scheduler.ReplayStateInProgress, "started handling replay request").Return(nil).Once()

			jobRepository.On("GetJobDetails", mock.Anything, projName, jobAName).Return(jobAWithDetails, nil).Once()
			replayRepository.On("GetReplayByID", mock.Anything, replayReq.Replay.ID()).Return(replayReq, nil).Once()
			sch.On("GetJobRuns", mock.Anything, tnnt, mock.Anything, jobCron).Return(schedulerRunsPhase1, nil).Once()
			replayRepository.On("UpdateReplayRuns", mock.Anything, replayReq.Replay.ID(), mock.Anything).Return(nil).Once()
			sch.On("ClearBatch", mock.Anything, tnnt, jobAName, scheduledTime2.Add(-24*time.Hour), scheduledTime2.Add(-24*time.Hour)).Return(nil).Once()
			sch.On("CreateRun", mock.Anything, tnnt, jobAName, scheduledTime1.Add(-24*time.Hour), "replayed").Return(nil).Once()
			replayRepository.On("UpdateReplayRuns", mock.Anything, replayReq.Replay.ID(), mock.Anything).Return(nil).Once()

			// loop 2
			replayRepository.On("GetReplayByID", mock.Anything, replayReq.Replay.ID()).Return(replayPhase2, nil).Once()
			sch.On("GetJobRuns", mock.Anything, tnnt, mock.Anything, jobCron).Return(schedulerRunsPhase2, nil).Once()
			replayRepository.On("UpdateReplayRuns", mock.Anything, replayReq.Replay.ID(), mock.Anything).Return(nil).Once()
			replayRepository.On("UpdateReplayHeartbeat", mock.Anything, replayReq.Replay.ID()).Return(nil).Once()

			// loop 3
			replayRepository.On("GetReplayByID", mock.Anything, replayReq.Replay.ID()).Return(replayPhase3, nil).Once()
			sch.On("GetJobRuns", mock.Anything, tnnt, mock.Anything, jobCron).Return(schedulerRunsPhase3, nil).Once()
			replayRepository.On("UpdateReplayRuns", mock.Anything, replayReq.Replay.ID(), mock.Anything).Return(nil).Once()
			summaryMsg := "replay is finished with run status: success(2)"
			replayRepository.On("UpdateReplay", mock.Anything, replayReq.Replay.ID(), scheduler.ReplayStateSuccess, mock.Anything, summaryMsg).Return(nil).Once()
			replayRepository.On("UpdateReplayHeartbeat", mock.Anything, replayReq.Replay.ID()).Return(nil).Once()

			alertManager := new(mockAlertManager)
			alertManager.On("SendReplayEvent", mock.Anything).Return()
			defer alertManager.AssertExpectations(t)

			worker := service.NewReplayWorker(logger, replayRepository, jobRepository, sch, replayServerConfig, alertManager)
			worker.Execute(replayID, tnnt, jobAName)
		})

		t.Run("should able to update replay state as failed if unable to get job details", func(t *testing.T) {
			replayRepository := new(ReplayRepository)
			defer replayRepository.AssertExpectations(t)

			sch := new(mockReplayScheduler)
			defer sch.AssertExpectations(t)

			jobRepository := new(JobRepository)
			defer jobRepository.AssertExpectations(t)

			runsPhase1 := []*scheduler.JobRunStatus{
				{ScheduledAt: scheduledTime1, State: scheduler.StatePending},
				{ScheduledAt: scheduledTime2, State: scheduler.StatePending},
			}
			replayID := uuid.New()
			replayReq := &scheduler.ReplayWithRun{
				Replay: scheduler.NewReplay(replayID, jobAName, tnnt, replayConfigParallel, scheduler.ReplayStateCreated, time.Now(), now, message),
				Runs:   runsPhase1,
			}

			// loop 1
			errorMsg := "internal error"
			jobRepository.On("GetJobDetails", mock.Anything, projName, jobAName).Return(nil, errors.New(errorMsg)).Once()
			errorMsgToStore := "internal error for entity replay: unable to get job details for jobName: job-a, project: proj: internal error"
			replayRepository.On("UpdateReplayStatus", mock.Anything, replayReq.Replay.ID(), scheduler.ReplayStateFailed, errorMsgToStore).Return(nil).Once()
			worker := service.NewReplayWorker(logger, replayRepository, jobRepository, sch, replayServerConfig, nil)
			worker.Execute(replayID, tnnt, jobAName)
		})
		t.Run("should able to update replay state as failed if unable to get replay by id", func(t *testing.T) {
			replayRepository := new(ReplayRepository)
			defer replayRepository.AssertExpectations(t)

			sch := new(mockReplayScheduler)
			defer sch.AssertExpectations(t)

			jobRepository := new(JobRepository)
			defer jobRepository.AssertExpectations(t)

			runsPhase1 := []*scheduler.JobRunStatus{
				{ScheduledAt: scheduledTime1, State: scheduler.StatePending},
				{ScheduledAt: scheduledTime2, State: scheduler.StatePending},
			}

			replayID := uuid.New()
			replay := scheduler.NewReplay(replayID, jobAName, tnnt, replayConfigParallel, scheduler.ReplayStateCreated, time.Now(), now, message)
			replayReq := &scheduler.ReplayWithRun{
				Replay: replay,
				Runs:   runsPhase1,
			}

			// loop 1

			jobRepository.On("GetJobDetails", mock.Anything, projName, jobAName).Return(jobAWithDetails, nil).Once()
			errorMsg := "internal error"
			replayRepository.On("GetReplayByID", mock.Anything, replayReq.Replay.ID()).Return(nil, errors.New(errorMsg)).Once()
			replayRepository.On("UpdateReplayStatus", mock.Anything, replayReq.Replay.ID(), scheduler.ReplayStateFailed, errorMsg).Return(nil).Once()
			alertManager := new(mockAlertManager)
			alertManager.On("SendReplayEvent", mock.Anything).Return()
			defer alertManager.AssertExpectations(t)
			worker := service.NewReplayWorker(logger, replayRepository, jobRepository, sch, replayServerConfig, alertManager)
			worker.Execute(replayID, tnnt, jobAName)
		})
		t.Run("should able to update replay state as failed if unable to fetch job runs", func(t *testing.T) {
			replayRepository := new(ReplayRepository)
			defer replayRepository.AssertExpectations(t)

			sch := new(mockReplayScheduler)
			defer sch.AssertExpectations(t)

			jobRepository := new(JobRepository)
			defer jobRepository.AssertExpectations(t)

			runsPhase1 := []*scheduler.JobRunStatus{
				{ScheduledAt: scheduledTime1, State: scheduler.StatePending},
				{ScheduledAt: scheduledTime2, State: scheduler.StatePending},
			}

			replayID := uuid.New()
			replayReq := &scheduler.ReplayWithRun{
				Replay: scheduler.NewReplay(replayID, jobAName, tnnt, replayConfigParallel, scheduler.ReplayStateCreated, time.Now(), now, message),
				Runs:   runsPhase1,
			}

			// loop 1
			replayRepository.On("UpdateReplayStatus", mock.Anything, replayReq.Replay.ID(), scheduler.ReplayStateInProgress, "started handling replay request").Return(nil).Once()

			jobRepository.On("GetJobDetails", mock.Anything, projName, jobAName).Return(jobAWithDetails, nil).Once()
			replayRepository.On("GetReplayByID", mock.Anything, replayReq.Replay.ID()).Return(replayReq, nil).Once()
			errorMsg := "internal error"
			sch.On("GetJobRuns", mock.Anything, tnnt, mock.Anything, jobCron).Return(nil, errors.New(errorMsg)).Once()
			replayRepository.On("UpdateReplayStatus", mock.Anything, replayReq.Replay.ID(), scheduler.ReplayStateFailed, errorMsg).Return(nil).Once()
			alertManager := new(mockAlertManager)
			alertManager.On("SendReplayEvent", mock.Anything).Return()
			defer alertManager.AssertExpectations(t)
			worker := service.NewReplayWorker(logger, replayRepository, jobRepository, sch, replayServerConfig, alertManager)
			worker.Execute(replayID, tnnt, jobAName)
		})
		t.Run("should able to update replay state as failed if unable to update replay once it is synced", func(t *testing.T) {
			replayRepository := new(ReplayRepository)
			defer replayRepository.AssertExpectations(t)

			sch := new(mockReplayScheduler)
			defer sch.AssertExpectations(t)

			jobRepository := new(JobRepository)
			defer jobRepository.AssertExpectations(t)

			runsPhase1 := []*scheduler.JobRunStatus{
				{ScheduledAt: scheduledTime1, State: scheduler.StatePending},
				{ScheduledAt: scheduledTime2, State: scheduler.StatePending},
			}

			replayID := uuid.New()
			replayReq := &scheduler.ReplayWithRun{
				Replay: scheduler.NewReplay(replayID, jobAName, tnnt, replayConfigParallel, scheduler.ReplayStateCreated, time.Now(), now, message),
				Runs:   runsPhase1,
			}

			schedulerRunsPhase1 := []*scheduler.JobRunStatus{
				{ScheduledAt: scheduledTime2, State: scheduler.StateFailed},
			}

			// loop 1
			replayRepository.On("UpdateReplayStatus", mock.Anything, replayReq.Replay.ID(), scheduler.ReplayStateInProgress, "started handling replay request").Return(nil).Once()

			jobRepository.On("GetJobDetails", mock.Anything, projName, jobAName).Return(jobAWithDetails, nil).Once()
			replayRepository.On("GetReplayByID", mock.Anything, replayReq.Replay.ID()).Return(replayReq, nil).Once()
			sch.On("GetJobRuns", mock.Anything, tnnt, mock.Anything, jobCron).Return(schedulerRunsPhase1, nil).Once()
			errorMsg := "internal error"
			replayRepository.On("UpdateReplayRuns", mock.Anything, replayReq.Replay.ID(), mock.Anything).Return(errors.New(errorMsg)).Once()
			replayRepository.On("UpdateReplayStatus", mock.Anything, replayReq.Replay.ID(), scheduler.ReplayStateFailed, errorMsg).Return(nil).Once()

			alertManager := new(mockAlertManager)
			alertManager.On("SendReplayEvent", mock.Anything).Return()
			defer alertManager.AssertExpectations(t)

			worker := service.NewReplayWorker(logger, replayRepository, jobRepository, sch, replayServerConfig, alertManager)
			worker.Execute(replayID, tnnt, jobAName)
		})
		t.Run("should able to update replay state as failed if unable to do clear batch of runs", func(t *testing.T) {
			replayRepository := new(ReplayRepository)
			defer replayRepository.AssertExpectations(t)

			sch := new(mockReplayScheduler)
			defer sch.AssertExpectations(t)

			jobRepository := new(JobRepository)
			defer jobRepository.AssertExpectations(t)

			runsPhase1 := []*scheduler.JobRunStatus{
				{ScheduledAt: scheduledTime1, State: scheduler.StatePending},
				{ScheduledAt: scheduledTime2, State: scheduler.StatePending},
			}

			replayID := uuid.New()
			replay := scheduler.NewReplay(replayID, jobAName, tnnt, replayConfigParallel, scheduler.ReplayStateCreated, time.Now(), now, message)
			replayReq := &scheduler.ReplayWithRun{
				Replay: replay,
				Runs:   runsPhase1,
			}

			schedulerRunsPhase1 := []*scheduler.JobRunStatus{
				{ScheduledAt: scheduledTime2, State: scheduler.StateFailed},
			}
			// for cancelled check
			replayRepository.On("GetReplayRequestByID", mock.Anything, replayReq.Replay.ID()).Return(replay, nil)

			// loop 1
			replayRepository.On("UpdateReplayStatus", mock.Anything, replayReq.Replay.ID(), scheduler.ReplayStateInProgress, "started handling replay request").Return(nil).Once()

			jobRepository.On("GetJobDetails", mock.Anything, projName, jobAName).Return(jobAWithDetails, nil).Once()
			replayRepository.On("GetReplayByID", mock.Anything, replayReq.Replay.ID()).Return(replayReq, nil).Once()
			sch.On("GetJobRuns", mock.Anything, tnnt, mock.Anything, jobCron).Return(schedulerRunsPhase1, nil).Once()
			replayRepository.On("UpdateReplayRuns", mock.Anything, replayReq.Replay.ID(), mock.Anything).Return(nil).Once()
			errorMsg := "internal error"
			sch.On("ClearBatch", mock.Anything, tnnt, jobAName, scheduledTime2.Add(-24*time.Hour), scheduledTime2.Add(-24*time.Hour)).Return(errors.New(errorMsg)).Once()
			replayRepository.On("UpdateReplayStatus", mock.Anything, replayReq.Replay.ID(), scheduler.ReplayStateFailed, errorMsg).Return(nil).Once()
			alertManager := new(mockAlertManager)
			alertManager.On("SendReplayEvent", mock.Anything).Return()
			defer alertManager.AssertExpectations(t)
			worker := service.NewReplayWorker(logger, replayRepository, jobRepository, sch, replayServerConfig, alertManager)
			worker.Execute(replayID, tnnt, jobAName)
		})
		t.Run("should able to update replay state as failed if unable to create missing run", func(t *testing.T) {
			replayRepository := new(ReplayRepository)
			defer replayRepository.AssertExpectations(t)

			sch := new(mockReplayScheduler)
			defer sch.AssertExpectations(t)

			jobRepository := new(JobRepository)
			defer jobRepository.AssertExpectations(t)

			runsPhase1 := []*scheduler.JobRunStatus{
				{ScheduledAt: scheduledTime1, State: scheduler.StatePending},
				{ScheduledAt: scheduledTime2, State: scheduler.StatePending},
			}

			replayID := uuid.New()
			replay := scheduler.NewReplay(replayID, jobAName, tnnt, replayConfigParallel, scheduler.ReplayStateCreated, time.Now(), now, message)
			replayReq := &scheduler.ReplayWithRun{
				Replay: replay,
				Runs:   runsPhase1,
			}

			schedulerRunsPhase1 := []*scheduler.JobRunStatus{
				{ScheduledAt: scheduledTime2, State: scheduler.StateFailed},
			}
			// for cancelled check
			replayRepository.On("GetReplayRequestByID", mock.Anything, replayReq.Replay.ID()).Return(replay, nil)
			// loop 1
			replayRepository.On("UpdateReplayStatus", mock.Anything, replayReq.Replay.ID(), scheduler.ReplayStateInProgress, "started handling replay request").Return(nil).Once()

			jobRepository.On("GetJobDetails", mock.Anything, projName, jobAName).Return(jobAWithDetails, nil).Once()
			replayRepository.On("GetReplayByID", mock.Anything, replayReq.Replay.ID()).Return(replayReq, nil).Once()
			sch.On("GetJobRuns", mock.Anything, tnnt, mock.Anything, jobCron).Return(schedulerRunsPhase1, nil).Once()
			replayRepository.On("UpdateReplayRuns", mock.Anything, replayReq.Replay.ID(), mock.Anything).Return(nil).Once()
			sch.On("ClearBatch", mock.Anything, tnnt, jobAName, scheduledTime2.Add(-24*time.Hour), scheduledTime2.Add(-24*time.Hour)).Return(nil).Once()
			errorMsg := "internal error"
			sch.On("CreateRun", mock.Anything, tnnt, jobAName, scheduledTime1.Add(-24*time.Hour), "replayed").Return(errors.New(errorMsg)).Once()
			errorMsgToStore := "create runs:\n internal error"
			replayRepository.On("UpdateReplayStatus", mock.Anything, replayReq.Replay.ID(), scheduler.ReplayStateFailed, errorMsgToStore).Return(nil).Once()
			alertManager := new(mockAlertManager)
			alertManager.On("SendReplayEvent", mock.Anything).Return()
			defer alertManager.AssertExpectations(t)

			worker := service.NewReplayWorker(logger, replayRepository, jobRepository, sch, replayServerConfig, alertManager)
			worker.Execute(replayID, tnnt, jobAName)
		})

		t.Run("should able to still process replay if some of the runs are in failed state", func(t *testing.T) {
			replayRepository := new(ReplayRepository)
			defer replayRepository.AssertExpectations(t)

			sch := new(mockReplayScheduler)
			defer sch.AssertExpectations(t)

			jobRepository := new(JobRepository)
			defer jobRepository.AssertExpectations(t)

			runsPhase1 := []*scheduler.JobRunStatus{
				{ScheduledAt: scheduledTime1, State: scheduler.StatePending},
				{ScheduledAt: scheduledTime2, State: scheduler.StatePending},
			}
			runsPhase2 := []*scheduler.JobRunStatus{
				{ScheduledAt: scheduledTime1, State: scheduler.StateInProgress},
				{ScheduledAt: scheduledTime2, State: scheduler.StatePending},
			}
			runsPhase3 := []*scheduler.JobRunStatus{
				{ScheduledAt: scheduledTime1, State: scheduler.StateFailed},
				{ScheduledAt: scheduledTime2, State: scheduler.StateInProgress},
			}

			replayID := uuid.New()
			replay := scheduler.NewReplay(replayID, jobAName, tnnt, replayConfig, scheduler.ReplayStateCreated, time.Now(), now, message)
			replayReq := &scheduler.ReplayWithRun{
				Replay: replay,
				Runs:   runsPhase1,
			}
			replayPhase2 := &scheduler.ReplayWithRun{
				Replay: scheduler.NewReplay(replayID, jobAName, tnnt, replayConfig, scheduler.ReplayStateInProgress, time.Now(), now, message),
				Runs:   runsPhase2,
			}
			replayPhase3 := &scheduler.ReplayWithRun{
				Replay: scheduler.NewReplay(replayID, jobAName, tnnt, replayConfig, scheduler.ReplayStateInProgress, time.Now(), now, message),
				Runs:   runsPhase3,
			}

			schedulerRunsPhase1 := []*scheduler.JobRunStatus{
				{ScheduledAt: scheduledTime1, State: scheduler.StateFailed},
				{ScheduledAt: scheduledTime2, State: scheduler.StateFailed},
			}
			schedulerRunsPhase2 := []*scheduler.JobRunStatus{
				{ScheduledAt: scheduledTime1, State: scheduler.StateFailed},
				{ScheduledAt: scheduledTime2, State: scheduler.StateFailed},
			}
			schedulerRunsPhase3 := []*scheduler.JobRunStatus{
				{ScheduledAt: scheduledTime1, State: scheduler.StateFailed},
				{ScheduledAt: scheduledTime2, State: scheduler.StateSuccess},
			}

			// for cancelled check
			replayRepository.On("GetReplayRequestByID", mock.Anything, replayReq.Replay.ID()).Return(replay, nil)

			// loop 1
			replayRepository.On("UpdateReplayStatus", mock.Anything, replayReq.Replay.ID(), scheduler.ReplayStateInProgress, "started handling replay request").Return(nil).Once()

			jobRepository.On("GetJobDetails", mock.Anything, projName, jobAName).Return(jobAWithDetails, nil).Once()
			replayRepository.On("GetReplayByID", mock.Anything, replayReq.Replay.ID()).Return(replayReq, nil).Once()
			sch.On("GetJobRuns", mock.Anything, tnnt, mock.Anything, jobCron).Return(schedulerRunsPhase1, nil).Once()
			replayRepository.On("UpdateReplayRuns", mock.Anything, replayReq.Replay.ID(), mock.Anything).Return(nil).Once()
			sch.On("ClearBatch", mock.Anything, tnnt, jobAName, scheduledTime1.Add(-24*time.Hour), scheduledTime1.Add(-24*time.Hour)).Return(nil).Once()
			replayRepository.On("UpdateReplayRuns", mock.Anything, replayReq.Replay.ID(), []*scheduler.JobRunStatus{runsPhase2[0]}).Return(nil).Once()

			// loop 2
			replayRepository.On("GetReplayByID", mock.Anything, replayReq.Replay.ID()).Return(replayPhase2, nil).Once()
			sch.On("GetJobRuns", mock.Anything, tnnt, mock.Anything, jobCron).Return(schedulerRunsPhase2, nil).Once()
			replayRepository.On("UpdateReplayRuns", mock.Anything, replayReq.Replay.ID(), mock.Anything).Return(nil).Once()
			sch.On("ClearBatch", mock.Anything, tnnt, jobAName, scheduledTime2.Add(-24*time.Hour), scheduledTime2.Add(-24*time.Hour)).Return(nil).Once()
			replayRepository.On("UpdateReplayRuns", mock.Anything, replayReq.Replay.ID(), []*scheduler.JobRunStatus{runsPhase3[1]}).Return(nil).Once()
			replayRepository.On("UpdateReplayHeartbeat", mock.Anything, replayReq.Replay.ID()).Return(nil).Once()

			// loop 3
			replayRepository.On("GetReplayByID", mock.Anything, replayReq.Replay.ID()).Return(replayPhase3, nil).Once()
			sch.On("GetJobRuns", mock.Anything, tnnt, mock.Anything, jobCron).Return(schedulerRunsPhase3, nil).Once()
			replayRepository.On("UpdateReplayRuns", mock.Anything, replayReq.Replay.ID(), mock.Anything).Return(nil).Once()
			replayRepository.On("UpdateReplay", mock.Anything, replayReq.Replay.ID(), scheduler.ReplayStateFailed, mock.Anything, mock.Anything).Return(nil).Once()
			replayRepository.On("UpdateReplayHeartbeat", mock.Anything, replayReq.Replay.ID()).Return(nil).Once()

			alertManager := new(mockAlertManager)
			alertManager.On("SendReplayEvent", mock.Anything).Return()
			defer alertManager.AssertExpectations(t)
			worker := service.NewReplayWorker(logger, replayRepository, jobRepository, sch, replayServerConfig, alertManager)
			worker.Execute(replayID, tnnt, jobAName)
		})

		t.Run("should able to stop replay process if it is being terminated externally", func(t *testing.T) {
			replayRepository := new(ReplayRepository)
			defer replayRepository.AssertExpectations(t)

			sch := new(mockReplayScheduler)
			defer sch.AssertExpectations(t)

			jobRepository := new(JobRepository)
			defer jobRepository.AssertExpectations(t)

			runsPhase1 := []*scheduler.JobRunStatus{
				{ScheduledAt: scheduledTime1, State: scheduler.StatePending},
				{ScheduledAt: scheduledTime2, State: scheduler.StatePending},
			}

			replayID := uuid.New()
			replayWithRun := &scheduler.ReplayWithRun{
				Replay: scheduler.NewReplay(replayID, jobAName, tnnt, replayConfigParallel, scheduler.ReplayStateCancelled, time.Now(), now, message),
				Runs:   runsPhase1,
			}

			jobRepository.On("GetJobDetails", mock.Anything, projName, jobAName).Return(jobAWithDetails, nil).Once()
			replayRepository.On("GetReplayByID", mock.Anything, replayWithRun.Replay.ID()).Return(replayWithRun, nil).Once()

			alertManager := new(mockAlertManager)
			alertManager.On("SendReplayEvent", mock.Anything).Return()
			defer alertManager.AssertExpectations(t)

			worker := service.NewReplayWorker(logger, replayRepository, jobRepository, sch, replayServerConfig, alertManager)
			worker.Execute(replayID, tnnt, jobAName)
		})
	})
}

// mockReplayScheduler is an autogenerated mock type for the mockReplayScheduler type
type mockReplayScheduler struct {
	mock.Mock
}

// Clear provides a mock function with given fields: ctx, t, jobName, scheduledAt
func (_m *mockReplayScheduler) Clear(ctx context.Context, t tenant.Tenant, jobName scheduler.JobName, scheduledAt time.Time) error {
	ret := _m.Called(ctx, t, jobName, scheduledAt)

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, tenant.Tenant, scheduler.JobName, time.Time) error); ok {
		r0 = rf(ctx, t, jobName, scheduledAt)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// ClearBatch provides a mock function with given fields: ctx, t, jobName, startTime, endTime
func (_m *mockReplayScheduler) ClearBatch(ctx context.Context, t tenant.Tenant, jobName scheduler.JobName, startTime, endTime time.Time) error {
	ret := _m.Called(ctx, t, jobName, startTime, endTime)

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, tenant.Tenant, scheduler.JobName, time.Time, time.Time) error); ok {
		r0 = rf(ctx, t, jobName, startTime, endTime)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// CreateRun provides a mock function with given fields: ctx, tnnt, jobName, executionTime, dagRunID
func (_m *mockReplayScheduler) CreateRun(ctx context.Context, tnnt tenant.Tenant, jobName scheduler.JobName, executionTime time.Time, dagRunIDPrefix string) error {
	ret := _m.Called(ctx, tnnt, jobName, executionTime, dagRunIDPrefix)

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, tenant.Tenant, scheduler.JobName, time.Time, string) error); ok {
		r0 = rf(ctx, tnnt, jobName, executionTime, dagRunIDPrefix)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

func (_m *mockReplayScheduler) CancelRun(ctx context.Context, tnnt tenant.Tenant, jobName scheduler.JobName, dagRunID string) error {
	args := _m.Called(ctx, tnnt, jobName, dagRunID)
	return args.Error(0)
}

func (_m *mockReplayScheduler) GetJobRunsWithDetails(ctx context.Context, t tenant.Tenant, criteria *scheduler.JobRunsCriteria, jobCron *cron.ScheduleSpec) ([]*scheduler.JobRunWithDetails, error) {
	args := _m.Called(ctx, t, criteria, jobCron)
	return args.Get(0).([]*scheduler.JobRunWithDetails), args.Error(1)
}

// GetJobRuns provides a mock function with given fields: ctx, t, criteria, jobCron
func (_m *mockReplayScheduler) GetJobRuns(ctx context.Context, t tenant.Tenant, criteria *scheduler.JobRunsCriteria, jobCron *cron.ScheduleSpec) ([]*scheduler.JobRunStatus, error) {
	ret := _m.Called(ctx, t, criteria, jobCron)

	var r0 []*scheduler.JobRunStatus
	if rf, ok := ret.Get(0).(func(context.Context, tenant.Tenant, *scheduler.JobRunsCriteria, *cron.ScheduleSpec) []*scheduler.JobRunStatus); ok {
		r0 = rf(ctx, t, criteria, jobCron)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]*scheduler.JobRunStatus)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, tenant.Tenant, *scheduler.JobRunsCriteria, *cron.ScheduleSpec) error); ok {
		r1 = rf(ctx, t, criteria, jobCron)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}
