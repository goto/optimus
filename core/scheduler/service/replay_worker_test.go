package service_test

import (
	"context"
	"errors"
	"github.com/google/uuid"
	"github.com/goto/optimus/config"
	"github.com/goto/optimus/core/scheduler"
	"github.com/goto/optimus/core/scheduler/service"
	"github.com/goto/optimus/core/tenant"
	"github.com/goto/optimus/internal/lib/cron"
	"github.com/goto/salt/log"
	"github.com/stretchr/testify/mock"
	"testing"
	"time"
)

func TestReplayWorker(t *testing.T) {
	ctx := context.Background()

	logger := log.NewNoop()
	replayServerConfig := config.ReplayConfig{ExecutionInterval: time.Second}

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
			replayReq := &scheduler.ReplayWithRun{
				Replay: scheduler.NewReplay(replayID, jobAName, tnnt, replayConfig, scheduler.ReplayStateCreated, time.Now()),
				Runs:   runsPhaseOne,
			}
			replayPhaseTwo := &scheduler.ReplayWithRun{
				Replay: scheduler.NewReplay(replayID, jobAName, tnnt, replayConfig, scheduler.ReplayStateInProgress, time.Now()),
				Runs:   runsPhaseTwo,
			}

			// loop 1
			jobRepository.On("GetJobDetails", mock.Anything, projName, jobAName).Return(jobAWithDetails, nil).Once()
			replayRepository.On("GetReplayByID", mock.Anything, replayReq.Replay.ID()).Return(replayReq, nil).Once()
			sch.On("GetJobRuns", mock.Anything, tnnt, mock.Anything, jobCron).Return(schedulerRunsPhaseOne, nil).Once()
			replayRepository.On("UpdateReplay", mock.Anything, replayReq.Replay.ID(), scheduler.ReplayStateInProgress, runsPhaseOne, "").Return(nil).Once()
			sch.On("ClearBatch", mock.Anything, tnnt, jobAName, scheduledTime1.Add(-24*time.Hour), scheduledTime1.Add(-24*time.Hour)).Return(nil).Once()
			replayRepository.On("UpdateReplay", mock.Anything, replayReq.Replay.ID(), scheduler.ReplayStateInProgress, runsPhaseTwo, "").Return(nil).Once()

			// loop 2
			replayRepository.On("GetReplayByID", mock.Anything, replayReq.Replay.ID()).Return(replayPhaseTwo, nil).Once()
			sch.On("GetJobRuns", mock.Anything, tnnt, mock.Anything, jobCron).Return(runsPhaseThree, nil).Once()
			replayRepository.On("UpdateReplay", mock.Anything, replayReq.Replay.ID(), scheduler.ReplayStateInProgress, runsPhaseThree, "").Return(nil).Once()
			replayRepository.On("UpdateReplay", mock.Anything, replayReq.Replay.ID(), scheduler.ReplayStateSuccess, runsPhaseThree, "").Return(nil).Once()

			worker := service.NewReplayWorker(logger, replayRepository, jobRepository, sch, replayServerConfig)
			worker.Execute(ctx, replayReq)
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
			replayReq := &scheduler.ReplayWithRun{
				Replay: scheduler.NewReplay(replayID, jobAName, tnnt, replayConfig, scheduler.ReplayStateCreated, time.Now()),
				Runs:   runsPhase1,
			}
			replayPhase2 := &scheduler.ReplayWithRun{
				Replay: scheduler.NewReplay(replayID, jobAName, tnnt, replayConfig, scheduler.ReplayStateInProgress, time.Now()),
				Runs:   runsPhase2,
			}
			replayPhase3 := &scheduler.ReplayWithRun{
				Replay: scheduler.NewReplay(replayID, jobAName, tnnt, replayConfig, scheduler.ReplayStateInProgress, time.Now()),
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

			// loop 1
			jobRepository.On("GetJobDetails", mock.Anything, projName, jobAName).Return(jobAWithDetails, nil).Once()
			replayRepository.On("GetReplayByID", mock.Anything, replayReq.Replay.ID()).Return(replayReq, nil).Once()
			sch.On("GetJobRuns", mock.Anything, tnnt, mock.Anything, jobCron).Return(schedulerRunsPhase1, nil).Once()
			replayRepository.On("UpdateReplay", mock.Anything, replayReq.Replay.ID(), scheduler.ReplayStateInProgress, mock.Anything, "").Return(nil).Once()
			sch.On("ClearBatch", mock.Anything, tnnt, jobAName, scheduledTime1.Add(-24*time.Hour), scheduledTime1.Add(-24*time.Hour)).Return(nil).Once()
			replayRepository.On("UpdateReplay", mock.Anything, replayReq.Replay.ID(), scheduler.ReplayStateInProgress, []*scheduler.JobRunStatus{runsPhase2[0]}, "").Return(nil).Once()

			// loop 2
			replayRepository.On("GetReplayByID", mock.Anything, replayReq.Replay.ID()).Return(replayPhase2, nil).Once()
			sch.On("GetJobRuns", mock.Anything, tnnt, mock.Anything, jobCron).Return(schedulerRunsPhase2, nil).Once()
			replayRepository.On("UpdateReplay", mock.Anything, replayReq.Replay.ID(), scheduler.ReplayStateInProgress, mock.Anything, "").Return(nil).Once()
			sch.On("ClearBatch", mock.Anything, tnnt, jobAName, scheduledTime2.Add(-24*time.Hour), scheduledTime2.Add(-24*time.Hour)).Return(nil).Once()
			replayRepository.On("UpdateReplay", mock.Anything, replayReq.Replay.ID(), scheduler.ReplayStateInProgress, []*scheduler.JobRunStatus{runsPhase3[1]}, "").Return(nil).Once()

			// loop 3
			replayRepository.On("GetReplayByID", mock.Anything, replayReq.Replay.ID()).Return(replayPhase3, nil).Once()
			sch.On("GetJobRuns", mock.Anything, tnnt, mock.Anything, jobCron).Return(schedulerRunsPhase3, nil).Once()
			replayRepository.On("UpdateReplay", mock.Anything, replayReq.Replay.ID(), scheduler.ReplayStateInProgress, mock.Anything, "").Return(nil).Once()
			replayRepository.On("UpdateReplay", mock.Anything, replayReq.Replay.ID(), scheduler.ReplayStateSuccess, mock.Anything, "").Return(nil).Once()

			worker := service.NewReplayWorker(logger, replayRepository, jobRepository, sch, replayServerConfig)
			worker.Execute(ctx, replayReq)
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
			replayReq := &scheduler.ReplayWithRun{
				Replay: scheduler.NewReplay(replayID, jobAName, tnnt, replayConfigParallel, scheduler.ReplayStateCreated, time.Now()),
				Runs:   runsPhase1,
			}
			replayPhase2 := &scheduler.ReplayWithRun{
				Replay: scheduler.NewReplay(replayID, jobAName, tnnt, replayConfigParallel, scheduler.ReplayStateInProgress, time.Now()),
				Runs:   runsPhase2,
			}
			replayPhase3 := &scheduler.ReplayWithRun{
				Replay: scheduler.NewReplay(replayID, jobAName, tnnt, replayConfigParallel, scheduler.ReplayStateInProgress, time.Now()),
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

			// loop 1
			jobRepository.On("GetJobDetails", mock.Anything, projName, jobAName).Return(jobAWithDetails, nil).Once()
			replayRepository.On("GetReplayByID", mock.Anything, replayReq.Replay.ID()).Return(replayReq, nil).Once()
			sch.On("GetJobRuns", mock.Anything, tnnt, mock.Anything, jobCron).Return(schedulerRunsPhase1, nil).Once()
			replayRepository.On("UpdateReplay", mock.Anything, replayReq.Replay.ID(), scheduler.ReplayStateInProgress, mock.Anything, "").Return(nil).Once()
			sch.On("ClearBatch", mock.Anything, tnnt, jobAName, scheduledTime1.Add(-24*time.Hour), scheduledTime2.Add(-24*time.Hour)).Return(nil).Once()
			replayRepository.On("UpdateReplay", mock.Anything, replayReq.Replay.ID(), scheduler.ReplayStateInProgress, mock.Anything, "").Return(nil).Once()

			// loop 2
			replayRepository.On("GetReplayByID", mock.Anything, replayReq.Replay.ID()).Return(replayPhase2, nil).Once()
			sch.On("GetJobRuns", mock.Anything, tnnt, mock.Anything, jobCron).Return(schedulerRunsPhase2, nil).Once()
			replayRepository.On("UpdateReplay", mock.Anything, replayReq.Replay.ID(), scheduler.ReplayStateInProgress, mock.Anything, "").Return(nil).Once()

			// loop 3
			replayRepository.On("GetReplayByID", mock.Anything, replayReq.Replay.ID()).Return(replayPhase3, nil).Once()
			sch.On("GetJobRuns", mock.Anything, tnnt, mock.Anything, jobCron).Return(schedulerRunsPhase3, nil).Once()
			replayRepository.On("UpdateReplay", mock.Anything, replayReq.Replay.ID(), scheduler.ReplayStateInProgress, mock.Anything, "").Return(nil).Once()
			replayRepository.On("UpdateReplay", mock.Anything, replayReq.Replay.ID(), scheduler.ReplayStateSuccess, mock.Anything, "").Return(nil).Once()

			worker := service.NewReplayWorker(logger, replayRepository, jobRepository, sch, replayServerConfig)
			worker.Execute(ctx, replayReq)
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
			replayReq := &scheduler.ReplayWithRun{
				Replay: scheduler.NewReplay(replayID, jobAName, tnnt, replayConfig, scheduler.ReplayStateCreated, time.Now()),
				Runs:   runsPhase1,
			}
			replayPhase2 := &scheduler.ReplayWithRun{
				Replay: scheduler.NewReplay(replayID, jobAName, tnnt, replayConfig, scheduler.ReplayStateInProgress, time.Now()),
				Runs:   runsPhase2,
			}
			replayPhase3 := &scheduler.ReplayWithRun{
				Replay: scheduler.NewReplay(replayID, jobAName, tnnt, replayConfig, scheduler.ReplayStateInProgress, time.Now()),
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

			// loop 1
			jobRepository.On("GetJobDetails", mock.Anything, projName, jobAName).Return(jobAWithDetails, nil).Once()
			replayRepository.On("GetReplayByID", mock.Anything, replayReq.Replay.ID()).Return(replayReq, nil).Once()
			sch.On("GetJobRuns", mock.Anything, tnnt, mock.Anything, jobCron).Return(schedulerRunsPhase1, nil).Once()
			replayRepository.On("UpdateReplay", mock.Anything, replayReq.Replay.ID(), scheduler.ReplayStateInProgress, mock.Anything, "").Return(nil).Once()
			sch.On("CreateRun", mock.Anything, tnnt, jobAName, scheduledTime1.Add(-24*time.Hour), "replayed").Return(nil).Once()
			replayRepository.On("UpdateReplay", mock.Anything, replayReq.Replay.ID(), scheduler.ReplayStateInProgress, []*scheduler.JobRunStatus{runsPhase2[0]}, "").Return(nil).Once()

			// loop 2
			replayRepository.On("GetReplayByID", mock.Anything, replayReq.Replay.ID()).Return(replayPhase2, nil).Once()
			sch.On("GetJobRuns", mock.Anything, tnnt, mock.Anything, jobCron).Return(schedulerRunsPhase2, nil).Once()
			replayRepository.On("UpdateReplay", mock.Anything, replayReq.Replay.ID(), scheduler.ReplayStateInProgress, mock.Anything, "").Return(nil).Once()
			sch.On("ClearBatch", mock.Anything, tnnt, jobAName, scheduledTime2.Add(-24*time.Hour), scheduledTime2.Add(-24*time.Hour)).Return(nil).Once()
			replayRepository.On("UpdateReplay", mock.Anything, replayReq.Replay.ID(), scheduler.ReplayStateInProgress, []*scheduler.JobRunStatus{runsPhase3[1]}, "").Return(nil).Once()

			// loop 3
			replayRepository.On("GetReplayByID", mock.Anything, replayReq.Replay.ID()).Return(replayPhase3, nil).Once()
			sch.On("GetJobRuns", mock.Anything, tnnt, mock.Anything, jobCron).Return(schedulerRunsPhase3, nil).Once()
			replayRepository.On("UpdateReplay", mock.Anything, replayReq.Replay.ID(), scheduler.ReplayStateInProgress, mock.Anything, "").Return(nil).Once()
			replayRepository.On("UpdateReplay", mock.Anything, replayReq.Replay.ID(), scheduler.ReplayStateSuccess, mock.Anything, "").Return(nil).Once()

			worker := service.NewReplayWorker(logger, replayRepository, jobRepository, sch, replayServerConfig)
			worker.Execute(ctx, replayReq)
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
			replayReq := &scheduler.ReplayWithRun{
				Replay: scheduler.NewReplay(replayID, jobAName, tnnt, replayConfigParallel, scheduler.ReplayStateCreated, time.Now()),
				Runs:   runsPhase1,
			}
			replayPhase2 := &scheduler.ReplayWithRun{
				Replay: scheduler.NewReplay(replayID, jobAName, tnnt, replayConfigParallel, scheduler.ReplayStateInProgress, time.Now()),
				Runs:   runsPhase2,
			}
			replayPhase3 := &scheduler.ReplayWithRun{
				Replay: scheduler.NewReplay(replayID, jobAName, tnnt, replayConfigParallel, scheduler.ReplayStateInProgress, time.Now()),
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

			// loop 1
			jobRepository.On("GetJobDetails", mock.Anything, projName, jobAName).Return(jobAWithDetails, nil).Once()
			replayRepository.On("GetReplayByID", mock.Anything, replayReq.Replay.ID()).Return(replayReq, nil).Once()
			sch.On("GetJobRuns", mock.Anything, tnnt, mock.Anything, jobCron).Return(schedulerRunsPhase1, nil).Once()
			replayRepository.On("UpdateReplay", mock.Anything, replayReq.Replay.ID(), scheduler.ReplayStateInProgress, mock.Anything, "").Return(nil).Once()
			sch.On("ClearBatch", mock.Anything, tnnt, jobAName, scheduledTime2.Add(-24*time.Hour), scheduledTime2.Add(-24*time.Hour)).Return(nil).Once()
			sch.On("CreateRun", mock.Anything, tnnt, jobAName, scheduledTime1.Add(-24*time.Hour), "replayed").Return(nil).Once()
			replayRepository.On("UpdateReplay", mock.Anything, replayReq.Replay.ID(), scheduler.ReplayStateInProgress, mock.Anything, "").Return(nil).Once()

			// loop 2
			replayRepository.On("GetReplayByID", mock.Anything, replayReq.Replay.ID()).Return(replayPhase2, nil).Once()
			sch.On("GetJobRuns", mock.Anything, tnnt, mock.Anything, jobCron).Return(schedulerRunsPhase2, nil).Once()
			replayRepository.On("UpdateReplay", mock.Anything, replayReq.Replay.ID(), scheduler.ReplayStateInProgress, mock.Anything, "").Return(nil).Once()

			// loop 3
			replayRepository.On("GetReplayByID", mock.Anything, replayReq.Replay.ID()).Return(replayPhase3, nil).Once()
			sch.On("GetJobRuns", mock.Anything, tnnt, mock.Anything, jobCron).Return(schedulerRunsPhase3, nil).Once()
			replayRepository.On("UpdateReplay", mock.Anything, replayReq.Replay.ID(), scheduler.ReplayStateInProgress, mock.Anything, "").Return(nil).Once()
			replayRepository.On("UpdateReplay", mock.Anything, replayReq.Replay.ID(), scheduler.ReplayStateSuccess, mock.Anything, "").Return(nil).Once()

			worker := service.NewReplayWorker(logger, replayRepository, jobRepository, sch, replayServerConfig)
			worker.Execute(ctx, replayReq)
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
				Replay: scheduler.NewReplay(replayID, jobAName, tnnt, replayConfigParallel, scheduler.ReplayStateCreated, time.Now()),
				Runs:   runsPhase1,
			}

			// loop 1
			errorMsg := "internal error"
			jobRepository.On("GetJobDetails", mock.Anything, projName, jobAName).Return(nil, errors.New(errorMsg)).Once()
			errorMsgToStore := "internal error for entity replay: unable to get job details for jobName: job-a, project: proj: internal error"
			replayRepository.On("UpdateReplayStatus", mock.Anything, replayReq.Replay.ID(), scheduler.ReplayStateFailed, errorMsgToStore).Return(nil).Once()

			worker := service.NewReplayWorker(logger, replayRepository, jobRepository, sch, replayServerConfig)
			worker.Execute(ctx, replayReq)
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
			replayReq := &scheduler.ReplayWithRun{
				Replay: scheduler.NewReplay(replayID, jobAName, tnnt, replayConfigParallel, scheduler.ReplayStateCreated, time.Now()),
				Runs:   runsPhase1,
			}

			// loop 1
			jobRepository.On("GetJobDetails", mock.Anything, projName, jobAName).Return(jobAWithDetails, nil).Once()
			errorMsg := "internal error"
			replayRepository.On("GetReplayByID", mock.Anything, replayReq.Replay.ID()).Return(nil, errors.New(errorMsg)).Once()
			replayRepository.On("UpdateReplayStatus", mock.Anything, replayReq.Replay.ID(), scheduler.ReplayStateFailed, errorMsg).Return(nil).Once()
			worker := service.NewReplayWorker(logger, replayRepository, jobRepository, sch, replayServerConfig)
			worker.Execute(ctx, replayReq)
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
				Replay: scheduler.NewReplay(replayID, jobAName, tnnt, replayConfigParallel, scheduler.ReplayStateCreated, time.Now()),
				Runs:   runsPhase1,
			}

			// loop 1
			jobRepository.On("GetJobDetails", mock.Anything, projName, jobAName).Return(jobAWithDetails, nil).Once()
			replayRepository.On("GetReplayByID", mock.Anything, replayReq.Replay.ID()).Return(replayReq, nil).Once()
			errorMsg := "internal error"
			sch.On("GetJobRuns", mock.Anything, tnnt, mock.Anything, jobCron).Return(nil, errors.New(errorMsg)).Once()
			replayRepository.On("UpdateReplayStatus", mock.Anything, replayReq.Replay.ID(), scheduler.ReplayStateFailed, errorMsg).Return(nil).Once()

			worker := service.NewReplayWorker(logger, replayRepository, jobRepository, sch, replayServerConfig)
			worker.Execute(ctx, replayReq)
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
				Replay: scheduler.NewReplay(replayID, jobAName, tnnt, replayConfigParallel, scheduler.ReplayStateCreated, time.Now()),
				Runs:   runsPhase1,
			}

			schedulerRunsPhase1 := []*scheduler.JobRunStatus{
				{ScheduledAt: scheduledTime2, State: scheduler.StateFailed},
			}

			// loop 1
			jobRepository.On("GetJobDetails", mock.Anything, projName, jobAName).Return(jobAWithDetails, nil).Once()
			replayRepository.On("GetReplayByID", mock.Anything, replayReq.Replay.ID()).Return(replayReq, nil).Once()
			sch.On("GetJobRuns", mock.Anything, tnnt, mock.Anything, jobCron).Return(schedulerRunsPhase1, nil).Once()
			errorMsg := "internal error"
			replayRepository.On("UpdateReplay", mock.Anything, replayReq.Replay.ID(), scheduler.ReplayStateInProgress, mock.Anything, "").Return(errors.New(errorMsg)).Once()
			replayRepository.On("UpdateReplayStatus", mock.Anything, replayReq.Replay.ID(), scheduler.ReplayStateFailed, errorMsg).Return(nil).Once()

			worker := service.NewReplayWorker(logger, replayRepository, jobRepository, sch, replayServerConfig)
			worker.Execute(ctx, replayReq)
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
			replayReq := &scheduler.ReplayWithRun{
				Replay: scheduler.NewReplay(replayID, jobAName, tnnt, replayConfigParallel, scheduler.ReplayStateCreated, time.Now()),
				Runs:   runsPhase1,
			}

			schedulerRunsPhase1 := []*scheduler.JobRunStatus{
				{ScheduledAt: scheduledTime2, State: scheduler.StateFailed},
			}

			// loop 1
			jobRepository.On("GetJobDetails", mock.Anything, projName, jobAName).Return(jobAWithDetails, nil).Once()
			replayRepository.On("GetReplayByID", mock.Anything, replayReq.Replay.ID()).Return(replayReq, nil).Once()
			sch.On("GetJobRuns", mock.Anything, tnnt, mock.Anything, jobCron).Return(schedulerRunsPhase1, nil).Once()
			replayRepository.On("UpdateReplay", mock.Anything, replayReq.Replay.ID(), scheduler.ReplayStateInProgress, mock.Anything, "").Return(nil).Once()
			sch.On("ClearBatch", mock.Anything, tnnt, jobAName, scheduledTime2.Add(-24*time.Hour), scheduledTime2.Add(-24*time.Hour)).Return(nil).Once()
			errorMsg := "internal error"
			sch.On("CreateRun", mock.Anything, tnnt, jobAName, scheduledTime1.Add(-24*time.Hour), "replayed").Return(errors.New(errorMsg)).Once()
			replayRepository.On("UpdateReplayStatus", mock.Anything, replayReq.Replay.ID(), scheduler.ReplayStateFailed, errorMsg).Return(nil).Once()

			worker := service.NewReplayWorker(logger, replayRepository, jobRepository, sch, replayServerConfig)
			worker.Execute(ctx, replayReq)
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
			replayReq := &scheduler.ReplayWithRun{
				Replay: scheduler.NewReplay(replayID, jobAName, tnnt, replayConfigParallel, scheduler.ReplayStateCreated, time.Now()),
				Runs:   runsPhase1,
			}

			schedulerRunsPhase1 := []*scheduler.JobRunStatus{
				{ScheduledAt: scheduledTime2, State: scheduler.StateFailed},
			}

			// loop 1
			jobRepository.On("GetJobDetails", mock.Anything, projName, jobAName).Return(jobAWithDetails, nil).Once()
			replayRepository.On("GetReplayByID", mock.Anything, replayReq.Replay.ID()).Return(replayReq, nil).Once()
			sch.On("GetJobRuns", mock.Anything, tnnt, mock.Anything, jobCron).Return(schedulerRunsPhase1, nil).Once()
			replayRepository.On("UpdateReplay", mock.Anything, replayReq.Replay.ID(), scheduler.ReplayStateInProgress, mock.Anything, "").Return(nil).Once()
			sch.On("ClearBatch", mock.Anything, tnnt, jobAName, scheduledTime2.Add(-24*time.Hour), scheduledTime2.Add(-24*time.Hour)).Return(nil).Once()
			errorMsg := "internal error"
			sch.On("CreateRun", mock.Anything, tnnt, jobAName, scheduledTime1.Add(-24*time.Hour), "replayed").Return(errors.New(errorMsg)).Once()
			errorMsgToStore := "create runs:\n internal error"
			replayRepository.On("UpdateReplayStatus", mock.Anything, replayReq.Replay.ID(), scheduler.ReplayStateFailed, errorMsgToStore).Return(nil).Once()

			worker := service.NewReplayWorker(logger, replayRepository, jobRepository, sch, replayServerConfig)
			worker.Execute(ctx, replayReq)
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
			replayReq := &scheduler.ReplayWithRun{
				Replay: scheduler.NewReplay(replayID, jobAName, tnnt, replayConfig, scheduler.ReplayStateCreated, time.Now()),
				Runs:   runsPhase1,
			}
			replayPhase2 := &scheduler.ReplayWithRun{
				Replay: scheduler.NewReplay(replayID, jobAName, tnnt, replayConfig, scheduler.ReplayStateInProgress, time.Now()),
				Runs:   runsPhase2,
			}
			replayPhase3 := &scheduler.ReplayWithRun{
				Replay: scheduler.NewReplay(replayID, jobAName, tnnt, replayConfig, scheduler.ReplayStateInProgress, time.Now()),
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

			// loop 1
			jobRepository.On("GetJobDetails", mock.Anything, projName, jobAName).Return(jobAWithDetails, nil).Once()
			replayRepository.On("GetReplayByID", mock.Anything, replayReq.Replay.ID()).Return(replayReq, nil).Once()
			sch.On("GetJobRuns", mock.Anything, tnnt, mock.Anything, jobCron).Return(schedulerRunsPhase1, nil).Once()
			replayRepository.On("UpdateReplay", mock.Anything, replayReq.Replay.ID(), scheduler.ReplayStateInProgress, mock.Anything, "").Return(nil).Once()
			sch.On("ClearBatch", mock.Anything, tnnt, jobAName, scheduledTime1.Add(-24*time.Hour), scheduledTime1.Add(-24*time.Hour)).Return(nil).Once()
			replayRepository.On("UpdateReplay", mock.Anything, replayReq.Replay.ID(), scheduler.ReplayStateInProgress, []*scheduler.JobRunStatus{runsPhase2[0]}, "").Return(nil).Once()

			// loop 2
			replayRepository.On("GetReplayByID", mock.Anything, replayReq.Replay.ID()).Return(replayPhase2, nil).Once()
			sch.On("GetJobRuns", mock.Anything, tnnt, mock.Anything, jobCron).Return(schedulerRunsPhase2, nil).Once()
			replayRepository.On("UpdateReplay", mock.Anything, replayReq.Replay.ID(), scheduler.ReplayStateInProgress, mock.Anything, "").Return(nil).Once()
			sch.On("ClearBatch", mock.Anything, tnnt, jobAName, scheduledTime2.Add(-24*time.Hour), scheduledTime2.Add(-24*time.Hour)).Return(nil).Once()
			replayRepository.On("UpdateReplay", mock.Anything, replayReq.Replay.ID(), scheduler.ReplayStateInProgress, []*scheduler.JobRunStatus{runsPhase3[1]}, "").Return(nil).Once()

			// loop 3
			replayRepository.On("GetReplayByID", mock.Anything, replayReq.Replay.ID()).Return(replayPhase3, nil).Once()
			sch.On("GetJobRuns", mock.Anything, tnnt, mock.Anything, jobCron).Return(schedulerRunsPhase3, nil).Once()
			replayRepository.On("UpdateReplay", mock.Anything, replayReq.Replay.ID(), scheduler.ReplayStateInProgress, mock.Anything, "").Return(nil).Once()
			replayRepository.On("UpdateReplay", mock.Anything, replayReq.Replay.ID(), scheduler.ReplayStateFailed, mock.Anything, "replay is failed due to some of runs are in failed state").Return(nil).Once()

			worker := service.NewReplayWorker(logger, replayRepository, jobRepository, sch, replayServerConfig)
			worker.Execute(ctx, replayReq)
		})
	})
}
