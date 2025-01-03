package service_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/goto/salt/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/goto/optimus/core/event/moderator"
	"github.com/goto/optimus/core/job"
	"github.com/goto/optimus/core/scheduler"
	"github.com/goto/optimus/core/scheduler/service"
	"github.com/goto/optimus/core/tenant"
	"github.com/goto/optimus/internal/errors"
	"github.com/goto/optimus/internal/lib/cron"
	"github.com/goto/optimus/internal/lib/window"
	"github.com/goto/optimus/internal/utils/filter"
)

func TestJobRunService(t *testing.T) {
	ctx := context.Background()
	projName := tenant.ProjectName("proj")
	namespaceName := tenant.ProjectName("ns1")
	jobName := scheduler.JobName("sample_select")
	todayDate := time.Now()
	scheduledAtString := "2022-01-02T15:04:05Z"
	scheduledAtTimeStamp, _ := time.Parse(scheduler.ISODateFormat, scheduledAtString)
	logger := log.NewNoop()
	tnnt, _ := tenant.NewTenant(projName.String(), namespaceName.String())

	monitoring := map[string]any{
		"slot_millis":           float64(5000),
		"total_bytes_processed": float64(2500),
	}
	t.Run("UpdateJobState", func(t *testing.T) {
		t.Run("should reject unregistered events", func(t *testing.T) {
			runService := service.NewJobRunService(logger,
				nil, nil, nil, nil, nil, nil, nil, nil, nil)

			event := &scheduler.Event{
				JobName: jobName,
				Tenant:  tnnt,
				Type:    "UnregisteredEventTYpe",
				Values:  map[string]any{},
			}
			err := runService.UpdateJobState(ctx, event)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "invalid argument for entity event: invalid event type: UnregisteredEventTYpe")
		})
		t.Run("registerNewJobRun", func(t *testing.T) {
			t.Run("should return error on TaskStartEvent for creating a new job run, if GetJobDetails fails", func(t *testing.T) {
				jobRunRepository := new(mockJobRunRepository)
				jobRunRepository.On("GetByScheduledAt", ctx, tnnt, jobName, scheduledAtTimeStamp).Return(nil, errors.NotFound(scheduler.EntityJobRun, "job run not found in db for given schedule date"))
				defer jobRunRepository.AssertExpectations(t)

				jobRepo := new(JobRepository)
				jobRepo.On("GetJobDetails", ctx, projName, jobName).Return(nil, fmt.Errorf("some error"))
				defer jobRepo.AssertExpectations(t)

				runService := service.NewJobRunService(logger,
					jobRepo, jobRunRepository, nil, nil, nil, nil, nil, nil, nil)

				event := &scheduler.Event{
					JobName:        jobName,
					Tenant:         tnnt,
					Type:           scheduler.TaskStartEvent,
					EventTime:      todayDate,
					OperatorName:   "taskBq2bq",
					JobScheduledAt: scheduledAtTimeStamp,
					Values:         map[string]any{},
				}
				err := runService.UpdateJobState(ctx, event)
				assert.NotNil(t, err)
				assert.EqualError(t, err, "some error")
			})
			t.Run("should return error on TaskStartEvent if job.SLADuration fails while creating a new job run, due to wrong duration format", func(t *testing.T) {
				jobWithDetails := scheduler.JobWithDetails{
					Name: jobName,
					Job: &scheduler.Job{
						Name:   jobName,
						Tenant: tnnt,
					},
					Alerts: []scheduler.Alert{
						{
							On: scheduler.EventCategorySLAMiss,
							Channels: []string{
								"chanel1",
								"chanel2",
							},
							Config: map[string]string{
								"key":      "value",
								"duration": "wrong duration format",
							},
						},
					},
				}
				jobRepo := new(JobRepository)
				jobRepo.On("GetJobDetails", ctx, projName, jobName).Return(&jobWithDetails, nil)
				defer jobRepo.AssertExpectations(t)

				jobRunRepository := new(mockJobRunRepository)
				jobRunRepository.On("GetByScheduledAt", ctx, tnnt, jobName, scheduledAtTimeStamp).Return(nil, errors.NotFound(scheduler.EntityJobRun, "job run not found in db for given schedule date"))
				defer jobRunRepository.AssertExpectations(t)

				runService := service.NewJobRunService(logger,
					jobRepo, jobRunRepository, nil, nil, nil, nil, nil, nil, nil)

				event := &scheduler.Event{
					JobName:        jobName,
					Tenant:         tnnt,
					Type:           scheduler.TaskStartEvent,
					JobScheduledAt: scheduledAtTimeStamp,
					Values:         map[string]any{},
				}

				err := runService.UpdateJobState(ctx, event)
				assert.NotNil(t, err)
				assert.EqualError(t, err, "failed to parse sla_miss duration wrong duration format: time: invalid duration \"wrong duration format\"")
			})
			t.Run("should create job_run row on JobSuccessEvent if job run row does not already exist", func(t *testing.T) {
				jobWithDetails := scheduler.JobWithDetails{
					Name: jobName,
					Job: &scheduler.Job{
						Name:   jobName,
						Tenant: tnnt,
					},
					Alerts: []scheduler.Alert{
						{
							On: scheduler.EventCategorySLAMiss,
							Channels: []string{
								"chanel1",
								"chanel2",
							},
							Config: map[string]string{
								"key":      "value",
								"duration": "2h45m",
							},
						},
					},
				}
				slaDefinitionInSec, err := jobWithDetails.SLADuration()
				assert.Nil(t, err)

				event := &scheduler.Event{
					JobName:        jobName,
					Tenant:         tnnt,
					Type:           scheduler.JobSuccessEvent,
					EventTime:      time.Time{},
					Status:         scheduler.StateSuccess,
					OperatorName:   "some_dummy_name",
					JobScheduledAt: scheduledAtTimeStamp,
					Values: map[string]any{
						"status":     "success",
						"monitoring": monitoring,
					},
				}

				jobRepo := new(JobRepository)
				jobRepo.On("GetJobDetails", ctx, projName, jobName).Return(&jobWithDetails, nil)
				defer jobRepo.AssertExpectations(t)

				jobRun := &scheduler.JobRun{
					ID:        uuid.New(),
					JobName:   jobName,
					Tenant:    tnnt,
					StartTime: todayDate,
				}

				jobRunRepo := new(mockJobRunRepository)
				jobRunRepo.On("GetByScheduledAt", ctx, tnnt, jobName, scheduledAtTimeStamp).Return(nil, errors.NotFound(scheduler.EntityJobRun, "job run not found in db for given schedule date")).Once()
				jobRunRepo.On("Create", ctx, tnnt, jobName, scheduledAtTimeStamp, slaDefinitionInSec).Return(nil)
				jobRunRepo.On("GetByScheduledAt", ctx, tnnt, jobName, scheduledAtTimeStamp).Return(jobRun, nil).Once()
				jobRunRepo.On("Update", ctx, jobRun.ID, event.EventTime, scheduler.StateSuccess).Return(nil)
				jobRunRepo.On("UpdateMonitoring", ctx, jobRun.ID, monitoring).Return(nil)
				defer jobRunRepo.AssertExpectations(t)

				operatorRunRepo := new(mockOperatorRunRepository)
				defer operatorRunRepo.AssertExpectations(t)

				eventHandler := newEventHandler(t)
				eventHandler.On("HandleEvent", mock.Anything).Times(1)
				defer eventHandler.AssertExpectations(t)

				runService := service.NewJobRunService(logger,
					jobRepo, jobRunRepo, nil, operatorRunRepo, nil, nil, nil, eventHandler, nil)

				err = runService.UpdateJobState(ctx, event)
				assert.Nil(t, err)
			})
		})

		t.Run("updateJobRun", func(t *testing.T) {
			t.Run("should update job_run row on JobSuccessEvent, when no error in format etc", func(t *testing.T) {
				scheduledAtTimeStamp, _ := time.Parse(scheduler.ISODateFormat, "2022-01-02T15:04:05Z")
				eventTime := time.Unix(todayDate.Add(time.Hour).Unix(), 0)
				endTime := eventTime
				event := &scheduler.Event{
					JobName:        jobName,
					Tenant:         tnnt,
					Type:           scheduler.JobSuccessEvent,
					Status:         scheduler.StateSuccess,
					JobScheduledAt: scheduledAtTimeStamp,
					EventTime:      eventTime,
					Values: map[string]any{
						"status":     "success",
						"monitoring": monitoring,
					},
				}

				jobRun := scheduler.JobRun{
					ID:        uuid.New(),
					JobName:   jobName,
					Tenant:    tnnt,
					StartTime: todayDate,
				}

				jobRunRepo := new(mockJobRunRepository)
				jobRunRepo.On("GetByScheduledAt", ctx, tnnt, jobName, scheduledAtTimeStamp).Return(&jobRun, nil)
				jobRunRepo.On("Update", ctx, jobRun.ID, endTime, scheduler.StateSuccess).Return(nil)
				jobRunRepo.On("UpdateMonitoring", ctx, jobRun.ID, monitoring).Return(nil)
				defer jobRunRepo.AssertExpectations(t)

				eventHandler := newEventHandler(t)
				eventHandler.On("HandleEvent", mock.Anything).Times(1)
				defer eventHandler.AssertExpectations(t)

				runService := service.NewJobRunService(logger,
					nil, jobRunRepo, nil, nil, nil, nil, nil, eventHandler, nil)

				err := runService.UpdateJobState(ctx, event)
				assert.Nil(t, err)
			})
			t.Run("should create and update job_run row on JobSuccessEvent, when job_run row does not exist already", func(t *testing.T) {
				jobWithDetails := scheduler.JobWithDetails{
					Name: jobName,
					Job: &scheduler.Job{
						Name:   jobName,
						Tenant: tnnt,
					},
					Alerts: []scheduler.Alert{
						{
							On: scheduler.EventCategorySLAMiss,
							Channels: []string{
								"chanel1",
								"chanel2",
							},
							Config: map[string]string{
								"key":      "value",
								"duration": "2h45m",
							},
						},
					},
				}

				scheduledAtTimeStamp, _ := time.Parse(scheduler.ISODateFormat, "2022-01-02T15:04:05Z")
				eventTime := time.Unix(todayDate.Add(time.Hour).Unix(), 0)
				endTime := eventTime
				event := &scheduler.Event{
					JobName:        jobName,
					Tenant:         tnnt,
					Type:           scheduler.JobFailureEvent,
					Status:         scheduler.StateSuccess,
					JobScheduledAt: scheduledAtTimeStamp,
					EventTime:      eventTime,
					Values: map[string]any{
						"status":     "success",
						"monitoring": monitoring,
					},
				}

				jobRun := scheduler.JobRun{
					ID:        uuid.New(),
					JobName:   jobName,
					Tenant:    tnnt,
					StartTime: time.Now(),
				}
				slaDefinitionInSec, _ := jobWithDetails.SLADuration()

				t.Run("scenario, return error when, GetByScheduledAt return errors other than not found", func(t *testing.T) {
					jobRunRepo := new(mockJobRunRepository)
					jobRunRepo.On("GetByScheduledAt", ctx, tnnt, jobName, scheduledAtTimeStamp).Return(nil, fmt.Errorf("some random error")).Once()
					defer jobRunRepo.AssertExpectations(t)

					runService := service.NewJobRunService(logger,
						nil, jobRunRepo, nil, nil, nil, nil, nil, nil, nil)

					err := runService.UpdateJobState(ctx, event)
					assert.NotNil(t, err)
					assert.EqualError(t, err, "some random error")
				})
				t.Run("scenario, return error when, unable to create job run", func(t *testing.T) {
					jobRunRepo := new(mockJobRunRepository)
					jobRunRepo.On("GetByScheduledAt", ctx, tnnt, jobName, scheduledAtTimeStamp).Return(nil, errors.NotFound(scheduler.EntityJobRun, "job run not found")).Once()
					jobRunRepo.On("Create", ctx, tnnt, jobName, scheduledAtTimeStamp, slaDefinitionInSec).Return(fmt.Errorf("unable to create job run")).Once()
					defer jobRunRepo.AssertExpectations(t)

					jobRepo := new(JobRepository)
					jobRepo.On("GetJobDetails", ctx, projName, jobName).Return(&jobWithDetails, nil)
					defer jobRepo.AssertExpectations(t)

					runService := service.NewJobRunService(logger,
						jobRepo, jobRunRepo, nil, nil, nil, nil, nil, nil, nil)

					err := runService.UpdateJobState(ctx, event)
					assert.NotNil(t, err)
					assert.EqualError(t, err, "unable to create job run")
				})
				t.Run("scenario, return error when, despite successful creation getByScheduledAt still fails", func(t *testing.T) {
					jobRunRepo := new(mockJobRunRepository)
					jobRunRepo.On("GetByScheduledAt", ctx, tnnt, jobName, scheduledAtTimeStamp).Return(nil, errors.NotFound(scheduler.EntityJobRun, "job run not found"))
					jobRunRepo.On("Create", ctx, tnnt, jobName, scheduledAtTimeStamp, slaDefinitionInSec).Return(nil)
					defer jobRunRepo.AssertExpectations(t)

					jobRepo := new(JobRepository)
					jobRepo.On("GetJobDetails", ctx, projName, jobName).Return(&jobWithDetails, nil)
					defer jobRepo.AssertExpectations(t)

					runService := service.NewJobRunService(logger,
						jobRepo, jobRunRepo, nil, nil, nil, nil, nil, nil, nil)

					err := runService.UpdateJobState(ctx, event)
					assert.NotNil(t, err)
					assert.EqualError(t, err, "not found for entity jobRun: job run not found")
				})
				t.Run("scenario should successfully register new job run row", func(t *testing.T) {
					jobRunRepo := new(mockJobRunRepository)
					jobRunRepo.On("GetByScheduledAt", ctx, tnnt, jobName, scheduledAtTimeStamp).Return(nil, errors.NotFound(scheduler.EntityJobRun, "job run not found")).Once()
					jobRunRepo.On("Create", ctx, tnnt, jobName, scheduledAtTimeStamp, slaDefinitionInSec).Return(nil).Once()
					jobRunRepo.On("GetByScheduledAt", ctx, tnnt, jobName, scheduledAtTimeStamp).Return(&jobRun, nil).Once()
					jobRunRepo.On("Update", ctx, jobRun.ID, endTime, scheduler.StateSuccess).Return(nil)
					jobRunRepo.On("UpdateMonitoring", ctx, jobRun.ID, monitoring).Return(nil)
					defer jobRunRepo.AssertExpectations(t)

					eventHandler := newEventHandler(t)
					eventHandler.On("HandleEvent", mock.Anything).Times(1)
					defer eventHandler.AssertExpectations(t)

					jobRepo := new(JobRepository)
					jobRepo.On("GetJobDetails", ctx, projName, jobName).Return(&jobWithDetails, nil)
					defer jobRepo.AssertExpectations(t)

					runService := service.NewJobRunService(logger,
						jobRepo, jobRunRepo, nil, nil, nil, nil, nil, eventHandler, nil)

					err := runService.UpdateJobState(ctx, event)
					assert.Nil(t, err)
				})
			})
		})

		t.Run("createOperatorRun", func(t *testing.T) {
			t.Run("should return error on TaskStartEvent if GetJobDetails fails", func(t *testing.T) {
				scheduledAtTimeStamp, _ := time.Parse(scheduler.ISODateFormat, "2022-01-02T15:04:05Z")
				eventTime := time.Unix(todayDate.Add(time.Hour).Unix(), 0)
				event := &scheduler.Event{
					JobName:        jobName,
					Tenant:         tnnt,
					Type:           scheduler.TaskStartEvent,
					JobScheduledAt: scheduledAtTimeStamp,
					EventTime:      eventTime,
					Values:         map[string]any{},
				}

				jobRunRepo := new(mockJobRunRepository)
				jobRunRepo.On("GetByScheduledAt", ctx, tnnt, jobName, scheduledAtTimeStamp).Return(nil, fmt.Errorf("some error in GetByScheduledAt"))
				defer jobRunRepo.AssertExpectations(t)

				runService := service.NewJobRunService(logger,
					nil, jobRunRepo, nil, nil, nil, nil, nil, nil, nil)

				err := runService.UpdateJobState(ctx, event)
				assert.NotNil(t, err)
				assert.EqualError(t, err, "some error in GetByScheduledAt")
			})
			t.Run("should return error on SensorStartEvent if GetJobDetails fails", func(t *testing.T) {
				scheduledAtTimeStamp, _ := time.Parse(scheduler.ISODateFormat, "2022-01-02T15:04:05Z")
				eventTime := time.Unix(todayDate.Add(time.Hour).Unix(), 0)
				event := &scheduler.Event{
					JobName:        jobName,
					Tenant:         tnnt,
					Type:           scheduler.SensorStartEvent,
					JobScheduledAt: scheduledAtTimeStamp,
					EventTime:      eventTime,
					Values:         map[string]any{},
				}

				jobRunRepo := new(mockJobRunRepository)
				jobRunRepo.On("GetByScheduledAt", ctx, tnnt, jobName, scheduledAtTimeStamp).Return(nil, fmt.Errorf("some error in GetByScheduledAt"))
				defer jobRunRepo.AssertExpectations(t)

				runService := service.NewJobRunService(logger,
					nil, jobRunRepo, nil, nil, nil, nil, nil, nil, nil)

				err := runService.UpdateJobState(ctx, event)
				assert.NotNil(t, err)
				assert.EqualError(t, err, "some error in GetByScheduledAt")
			})
			t.Run("should return error on HookStartEvent if GetJobDetails fails", func(t *testing.T) {
				scheduledAtTimeStamp, _ := time.Parse(scheduler.ISODateFormat, "2022-01-02T15:04:05Z")
				eventTime := time.Unix(todayDate.Add(time.Hour).Unix(), 0)
				event := &scheduler.Event{
					JobName:        jobName,
					Tenant:         tnnt,
					Type:           scheduler.HookStartEvent,
					JobScheduledAt: scheduledAtTimeStamp,
					EventTime:      eventTime,
					Values:         map[string]any{},
				}

				jobRunRepo := new(mockJobRunRepository)
				jobRunRepo.On("GetByScheduledAt", ctx, tnnt, jobName, scheduledAtTimeStamp).Return(nil, fmt.Errorf("some error in GetByScheduledAt"))
				defer jobRunRepo.AssertExpectations(t)

				runService := service.NewJobRunService(logger,
					nil, jobRunRepo, nil, nil, nil, nil, nil, nil, nil)

				err := runService.UpdateJobState(ctx, event)
				assert.NotNil(t, err)
				assert.EqualError(t, err, "some error in GetByScheduledAt")
			})
			t.Run("on TaskStartEvent", func(t *testing.T) {
				scheduledAtTimeStamp, _ := time.Parse(scheduler.ISODateFormat, "2022-01-02T15:04:05Z")
				eventTime := time.Unix(todayDate.Add(time.Hour).Unix(), 0)
				event := &scheduler.Event{
					JobName:        jobName,
					Tenant:         tnnt,
					Type:           scheduler.TaskStartEvent,
					Status:         scheduler.StateRunning,
					EventTime:      eventTime,
					OperatorName:   "task_bq3bq",
					JobScheduledAt: scheduledAtTimeStamp,
					Values:         map[string]any{},
				}

				jobRun := scheduler.JobRun{
					ID:        uuid.New(),
					JobName:   jobName,
					Tenant:    tnnt,
					StartTime: time.Now(),
				}

				jobRunRepo := new(mockJobRunRepository)
				jobRunRepo.On("GetByScheduledAt", ctx, tnnt, jobName, scheduledAtTimeStamp).Return(&jobRun, nil)
				jobRunRepo.On("UpdateState", ctx, jobRun.ID, scheduler.StateInProgress).Return(nil)
				defer jobRunRepo.AssertExpectations(t)

				t.Run("should pass creating new operator run ", func(t *testing.T) {
					operatorRunRepository := new(mockOperatorRunRepository)
					operatorRunRepository.On("CreateOperatorRun", ctx, event.OperatorName, scheduler.OperatorTask, jobRun.ID, eventTime).Return(nil)
					defer operatorRunRepository.AssertExpectations(t)

					eventHandler := newEventHandler(t)
					eventHandler.On("HandleEvent", mock.Anything).Times(1)
					defer eventHandler.AssertExpectations(t)

					runService := service.NewJobRunService(logger,
						nil, jobRunRepo, nil, operatorRunRepository, nil, nil, nil, eventHandler, nil)

					err := runService.UpdateJobState(ctx, event)
					assert.Nil(t, err)
				})
			})
		})
		t.Run("updateOperatorRun", func(t *testing.T) {
			t.Run("on TaskSuccessEvent should create task_run row", func(t *testing.T) {
				scheduledAtTimeStamp, _ := time.Parse(scheduler.ISODateFormat, "2022-01-02T15:04:05Z")
				eventTime := time.Unix(todayDate.Add(time.Hour).Unix(), 0)
				event := &scheduler.Event{
					JobName:        jobName,
					Tenant:         tnnt,
					Type:           scheduler.TaskSuccessEvent,
					EventTime:      eventTime,
					Status:         scheduler.StateSuccess,
					OperatorName:   "task_bq2bq",
					JobScheduledAt: scheduledAtTimeStamp,
					Values: map[string]any{
						"status": "success",
					},
				}

				t.Run("scenario OperatorRun not found and new operator creation fails", func(t *testing.T) {
					jobRun := scheduler.JobRun{
						ID:        uuid.New(),
						JobName:   jobName,
						Tenant:    tnnt,
						StartTime: time.Now(),
					}
					operatorRunRepository := new(mockOperatorRunRepository)
					operatorRunRepository.On("GetOperatorRun", ctx, event.OperatorName, scheduler.OperatorTask, jobRun.ID).Return(nil, errors.NotFound(scheduler.EntityEvent, "operator not found in db")).Once()
					operatorRunRepository.On("CreateOperatorRun", ctx, event.OperatorName, scheduler.OperatorTask, jobRun.ID, eventTime).Return(fmt.Errorf("some error in creating operator run"))
					defer operatorRunRepository.AssertExpectations(t)

					jobRunRepo := new(mockJobRunRepository)
					jobRunRepo.On("GetByScheduledAt", ctx, tnnt, jobName, scheduledAtTimeStamp).Return(&jobRun, nil)
					jobRunRepo.On("UpdateState", ctx, jobRun.ID, scheduler.StateInProgress).Return(nil)
					defer jobRunRepo.AssertExpectations(t)

					eventHandler := newEventHandler(t)
					eventHandler.On("HandleEvent", mock.Anything).Times(1)
					defer eventHandler.AssertExpectations(t)

					runService := service.NewJobRunService(logger,
						nil, jobRunRepo, nil, operatorRunRepository, nil, nil, nil, eventHandler, nil)

					err := runService.UpdateJobState(ctx, event)
					assert.NotNil(t, err)
					assert.EqualError(t, err, "some error in creating operator run")
				})
				t.Run("scenario OperatorRun not found even after successful new operator creation", func(t *testing.T) {
					jobRun := scheduler.JobRun{
						ID:        uuid.New(),
						JobName:   jobName,
						Tenant:    tnnt,
						StartTime: time.Now(),
					}

					operatorRunRepository := new(mockOperatorRunRepository)
					operatorRunRepository.On("GetOperatorRun", ctx, event.OperatorName, scheduler.OperatorTask, jobRun.ID).Return(nil, errors.NotFound(scheduler.EntityEvent, "operator not found in db")).Once()
					operatorRunRepository.On("CreateOperatorRun", ctx, event.OperatorName, scheduler.OperatorTask, jobRun.ID, eventTime).Return(nil)
					operatorRunRepository.On("GetOperatorRun", ctx, event.OperatorName, scheduler.OperatorTask, jobRun.ID).Return(nil, fmt.Errorf("some error in getting operator run")).Once()
					defer operatorRunRepository.AssertExpectations(t)

					jobRunRepo := new(mockJobRunRepository)
					jobRunRepo.On("GetByScheduledAt", ctx, tnnt, jobName, scheduledAtTimeStamp).Return(&jobRun, nil)
					jobRunRepo.On("UpdateState", ctx, jobRun.ID, scheduler.StateInProgress).Return(nil)
					defer jobRunRepo.AssertExpectations(t)

					eventHandler := newEventHandler(t)
					eventHandler.On("HandleEvent", mock.Anything).Times(1)
					defer eventHandler.AssertExpectations(t)

					runService := service.NewJobRunService(logger,
						nil, jobRunRepo, nil, operatorRunRepository, nil, nil, nil, eventHandler, nil)

					err := runService.UpdateJobState(ctx, event)
					assert.NotNil(t, err)
					assert.EqualError(t, err, "some error in getting operator run")
				})
				t.Run("scenario OperatorRun found", func(t *testing.T) {
					jobRun := scheduler.JobRun{
						ID:        uuid.New(),
						JobName:   jobName,
						Tenant:    tnnt,
						StartTime: time.Now(),
					}

					operatorRun := scheduler.OperatorRun{
						ID:           uuid.New(),
						Name:         "task_bq2bq",
						JobRunID:     jobRun.ID,
						OperatorType: scheduler.OperatorTask,
						Status:       scheduler.StateRunning,
					}
					operatorRunRepository := new(mockOperatorRunRepository)
					operatorRunRepository.On("GetOperatorRun", ctx, event.OperatorName, scheduler.OperatorTask, jobRun.ID).Return(&operatorRun, nil)
					operatorRunRepository.On("UpdateOperatorRun", ctx, scheduler.OperatorTask, operatorRun.ID, eventTime, scheduler.StateSuccess).Return(nil)
					defer operatorRunRepository.AssertExpectations(t)

					jobRunRepo := new(mockJobRunRepository)
					jobRunRepo.On("GetByScheduledAt", ctx, tnnt, jobName, scheduledAtTimeStamp).Return(&jobRun, nil)
					defer jobRunRepo.AssertExpectations(t)

					runService := service.NewJobRunService(logger,
						nil, jobRunRepo, nil, operatorRunRepository, nil, nil, nil, nil, nil)

					err := runService.UpdateJobState(ctx, event)
					assert.Nil(t, err)
				})
			})
			t.Run("on SensorSuccessEvent should fail when unable to get job run due to errors other than not found error ", func(t *testing.T) {
				scheduledAtTimeStamp, _ := time.Parse(scheduler.ISODateFormat, "2022-01-02T15:04:05Z")
				eventTime := time.Unix(todayDate.Add(time.Hour).Unix(), 0)
				event := &scheduler.Event{
					JobName:        jobName,
					Tenant:         tnnt,
					Type:           scheduler.SensorSuccessEvent,
					EventTime:      eventTime,
					OperatorName:   "wait-sample_select",
					JobScheduledAt: scheduledAtTimeStamp,
					Values: map[string]any{
						"status": "success",
					},
				}

				jobRunRepo := new(mockJobRunRepository)
				jobRunRepo.On("GetByScheduledAt", ctx, tnnt, jobName, scheduledAtTimeStamp).Return(nil, fmt.Errorf("error in getting job run GetByScheduledAt"))
				defer jobRunRepo.AssertExpectations(t)

				runService := service.NewJobRunService(logger,
					nil, jobRunRepo, nil, nil, nil, nil, nil, nil, nil)

				err := runService.UpdateJobState(ctx, event)
				assert.NotNil(t, err)
				assert.EqualError(t, err, "error in getting job run GetByScheduledAt")
			})
			t.Run("on HookSuccessEvent should fail when unable to get operator run due to errors other than not found error ", func(t *testing.T) {
				scheduledAtTimeStamp, _ := time.Parse(scheduler.ISODateFormat, "2022-01-02T15:04:05Z")
				eventTime := time.Unix(todayDate.Add(time.Hour).Unix(), 0)
				event := &scheduler.Event{
					JobName:        jobName,
					Tenant:         tnnt,
					Type:           scheduler.HookSuccessEvent,
					EventTime:      eventTime,
					OperatorName:   "hook-sample_select",
					JobScheduledAt: scheduledAtTimeStamp,
					Values: map[string]any{
						"status": "success",
					},
				}

				jobRun := scheduler.JobRun{
					ID:        uuid.New(),
					JobName:   jobName,
					Tenant:    tnnt,
					StartTime: time.Now(),
				}

				jobRunRepo := new(mockJobRunRepository)
				jobRunRepo.On("GetByScheduledAt", ctx, tnnt, jobName, scheduledAtTimeStamp).Return(&jobRun, nil)
				defer jobRunRepo.AssertExpectations(t)

				operatorRunRepository := new(mockOperatorRunRepository)
				operatorRunRepository.On("GetOperatorRun", ctx, event.OperatorName, scheduler.OperatorHook, jobRun.ID).Return(nil, fmt.Errorf("error in getting operator run"))
				// operatorRunRepository.On("UpdateOperatorRun", ctx, scheduler.OperatorSensor, operatorRun.ID, eventTime, "success").Return(nil)
				defer operatorRunRepository.AssertExpectations(t)
				runService := service.NewJobRunService(logger,
					nil, jobRunRepo, nil, operatorRunRepository, nil, nil, nil, nil, nil)

				err := runService.UpdateJobState(ctx, event)
				assert.NotNil(t, err)
				assert.EqualError(t, err, "error in getting operator run")
			})
		})

		t.Run("updateJobRunSLA", func(t *testing.T) {
			t.Run("scenario false sla notification", func(t *testing.T) {
				scheduledAtTimeStamp, _ := time.Parse(scheduler.ISODateFormat, "2022-01-02T15:04:05Z")
				eventTime := time.Now()
				// example of an hourly job
				slaBreachedJobRunScheduleTimes := []time.Time{
					time.Now().Add(time.Hour * time.Duration(-3)),
					time.Now().Add(time.Hour * time.Duration(-2)),
					time.Now().Add(time.Hour * time.Duration(-1)),
				}
				var slaObjectList []*scheduler.SLAObject
				for _, scheduleTime := range slaBreachedJobRunScheduleTimes {
					slaObjectList = append(slaObjectList, &scheduler.SLAObject{
						JobName:        jobName,
						JobScheduledAt: scheduleTime,
					})
				}

				event := &scheduler.Event{
					JobName:        jobName,
					Tenant:         tnnt,
					Type:           scheduler.SLAMissEvent,
					EventTime:      eventTime,
					OperatorName:   "task_bq2bq",
					Status:         scheduler.StateSuccess,
					JobScheduledAt: scheduledAtTimeStamp,
					Values: map[string]any{
						"status": "success",
					},
					SLAObjectList: slaObjectList,
				}

				var jobRuns []*scheduler.JobRun
				for _, slaBreachedJobRunScheduleTime := range slaBreachedJobRunScheduleTimes {
					jobRuns = append(jobRuns, &scheduler.JobRun{
						JobName:       jobName,
						Tenant:        tnnt,
						ScheduledAt:   slaBreachedJobRunScheduleTime,
						SLAAlert:      false,
						StartTime:     slaBreachedJobRunScheduleTime.Add(time.Second * time.Duration(1)),
						SLADefinition: 100,
					})
				}

				endTime0 := slaBreachedJobRunScheduleTimes[0].Add(time.Second * time.Duration(40)) // duration 40-1 = 39 Sec (Not an SLA breach)
				jobRuns[0].EndTime = &endTime0
				endTime1 := slaBreachedJobRunScheduleTimes[1].Add(time.Second * time.Duration(120)) // duration 120-1 = 119 Sec
				jobRuns[1].EndTime = &endTime1
				endTime2 := slaBreachedJobRunScheduleTimes[2].Add(time.Second * time.Duration(200)) // duration 200-1 = 199 Sec
				jobRuns[2].EndTime = &endTime2

				jobRunRepo := new(mockJobRunRepository)
				jobRunRepo.On("GetByScheduledTimes", ctx, tnnt, jobName, slaBreachedJobRunScheduleTimes).Return(jobRuns, nil).Once()
				jobRunRepo.On("UpdateSLA", ctx, event.JobName, event.Tenant.ProjectName(), []time.Time{
					slaBreachedJobRunScheduleTimes[1], slaBreachedJobRunScheduleTimes[2],
				}).Return(nil).Once()
				defer jobRunRepo.AssertExpectations(t)

				runService := service.NewJobRunService(logger,
					nil, jobRunRepo, nil, nil, nil, nil, nil, nil, nil)

				err := runService.UpdateJobState(ctx, event)
				assert.Nil(t, err)

				t.Run("scenario false sla notification, filter the false sla alert", func(t *testing.T) {
					assert.Equal(t, 2, len(event.SLAObjectList))
					for _, slaObject := range event.SLAObjectList {
						// slaBreachedJobRunScheduleTimes [0] should not be in the list as that is a false alert
						assert.False(t, slaObject.JobScheduledAt.Equal(slaBreachedJobRunScheduleTimes[0]))
					}
				})
			})
		})
	})

	t.Run("JobRunInput", func(t *testing.T) {
		t.Run("should return error if getJob fails", func(t *testing.T) {
			jobRepo := new(JobRepository)
			jobRepo.On("GetJobDetails", ctx, projName, jobName).Return(&scheduler.JobWithDetails{}, fmt.Errorf("some error"))
			defer jobRepo.AssertExpectations(t)

			runService := service.NewJobRunService(logger,
				jobRepo, nil, nil, nil, nil, nil, nil, nil, nil)
			executorInput, err := runService.JobRunInput(ctx, projName, jobName, scheduler.RunConfig{})
			assert.Nil(t, executorInput)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "some error")
		})
		t.Run("should get jobRunByScheduledAt if job run id is not given", func(t *testing.T) {
			tnnt, _ := tenant.NewTenant(projName.String(), namespaceName.String())
			job := scheduler.Job{
				Name:   jobName,
				Tenant: tnnt,
				Task: &scheduler.Task{
					Config: map[string]string{},
				},
			}
			details := scheduler.JobWithDetails{Job: &job}

			someScheduleTime := todayDate.Add(time.Hour * 24 * -1)
			executedAt := todayDate.Add(time.Hour * 23 * -1)
			startTime := executedAt
			runConfig := scheduler.RunConfig{
				Executor:    scheduler.Executor{},
				ScheduledAt: someScheduleTime,
				JobRunID:    scheduler.JobRunID{},
			}

			jobRepo := new(JobRepository)
			jobRepo.On("GetJobDetails", ctx, projName, jobName).
				Return(&details, nil)
			defer jobRepo.AssertExpectations(t)

			jobRun := scheduler.JobRun{
				JobName:   jobName,
				Tenant:    tnnt,
				StartTime: startTime,
			}
			jobRunRepo := new(mockJobRunRepository)
			jobRunRepo.On("GetByScheduledAt", ctx, tnnt, jobName, someScheduleTime).
				Return(&jobRun, nil)
			defer jobRunRepo.AssertExpectations(t)

			dummyExecutorInput := scheduler.ExecutorInput{
				Configs: scheduler.ConfigMap{
					"someKey": "someValue",
				},
			}
			jobToCompile := job
			jobToCompile.Task.Config["EXECUTION_PROJECT"] = "example"
			jobToCompileDetails := scheduler.JobWithDetails{Job: &jobToCompile}

			jobReplayRepo := new(ReplayRepository)
			jobReplayRepo.On("GetReplayJobConfig", ctx, tnnt, jobName, someScheduleTime).Return(map[string]string{"EXECUTION_PROJECT": "example"}, nil)
			defer jobReplayRepo.AssertExpectations(t)

			jobInputCompiler := new(mockJobInputCompiler)
			jobInputCompiler.On("Compile", ctx, &jobToCompileDetails, runConfig, executedAt).
				Return(&dummyExecutorInput, nil)
			defer jobInputCompiler.AssertExpectations(t)

			runService := service.NewJobRunService(logger,
				jobRepo, jobRunRepo, jobReplayRepo, nil, nil, nil, jobInputCompiler, nil, nil)
			executorInput, err := runService.JobRunInput(ctx, projName, jobName, runConfig)

			assert.Equal(t, &dummyExecutorInput, executorInput)
			assert.Nil(t, err)
		})
		t.Run("should use GetByID if job run id is given", func(t *testing.T) {
			tnnt, _ := tenant.NewTenant(projName.String(), namespaceName.String())
			job := scheduler.Job{
				Name:   jobName,
				Tenant: tnnt,
				Task: &scheduler.Task{
					Config: map[string]string{},
				},
			}
			details := scheduler.JobWithDetails{Job: &job}

			someScheduleTime := todayDate.Add(time.Hour * 24 * -1)
			executedAt := todayDate.Add(time.Hour * 23 * -1)
			startTime := executedAt
			jobRunID := scheduler.JobRunID(uuid.New())
			runConfig := scheduler.RunConfig{
				Executor:    scheduler.Executor{},
				ScheduledAt: someScheduleTime,
				JobRunID:    jobRunID,
			}

			jobRepo := new(JobRepository)
			jobRepo.On("GetJobDetails", ctx, projName, jobName).
				Return(&details, nil)
			defer jobRepo.AssertExpectations(t)

			jobRun := scheduler.JobRun{
				JobName:   jobName,
				Tenant:    tnnt,
				StartTime: startTime,
			}
			jobRunRepo := new(mockJobRunRepository)
			jobRunRepo.On("GetByID", ctx, jobRunID).
				Return(&jobRun, nil)
			defer jobRunRepo.AssertExpectations(t)

			dummyExecutorInput := scheduler.ExecutorInput{
				Configs: scheduler.ConfigMap{
					"someKey": "someValue",
				},
			}

			jobToCompile := job
			jobToCompile.Task.Config["EXECUTION_PROJECT"] = "example"
			jobToCompileDetails := scheduler.JobWithDetails{Job: &jobToCompile}

			jobReplayRepo := new(ReplayRepository)
			jobReplayRepo.On("GetReplayJobConfig", ctx, tnnt, jobName, someScheduleTime).Return(map[string]string{"EXECUTION_PROJECT": "example"}, nil)
			defer jobReplayRepo.AssertExpectations(t)

			jobInputCompiler := new(mockJobInputCompiler)
			jobInputCompiler.On("Compile", ctx, &jobToCompileDetails, runConfig, executedAt).
				Return(&dummyExecutorInput, nil)
			defer jobInputCompiler.AssertExpectations(t)

			runService := service.NewJobRunService(logger,
				jobRepo, jobRunRepo, jobReplayRepo, nil, nil, nil, jobInputCompiler, nil, nil)
			executorInput, err := runService.JobRunInput(ctx, projName, jobName, runConfig)

			assert.Equal(t, &dummyExecutorInput, executorInput)
			assert.Nil(t, err)
		})
		t.Run("should handle if job run is not found , and fallback to execution time being schedule time", func(t *testing.T) {
			tnnt, _ := tenant.NewTenant(projName.String(), namespaceName.String())
			job := scheduler.Job{
				Name:   jobName,
				Tenant: tnnt,
				Task: &scheduler.Task{
					Config: map[string]string{},
				},
			}
			details := scheduler.JobWithDetails{Job: &job}

			someScheduleTime := todayDate.Add(time.Hour * 24 * -1)
			jobRunID := scheduler.JobRunID(uuid.New())
			runConfig := scheduler.RunConfig{
				Executor:    scheduler.Executor{},
				ScheduledAt: someScheduleTime,
				JobRunID:    jobRunID,
			}

			jobRepo := new(JobRepository)
			jobRepo.On("GetJobDetails", ctx, projName, jobName).
				Return(&details, nil)
			defer jobRepo.AssertExpectations(t)

			jobRunRepo := new(mockJobRunRepository)
			jobRunRepo.On("GetByID", ctx, jobRunID).
				Return(&scheduler.JobRun{}, errors.NotFound(scheduler.EntityJobRun, "no record for job:"+jobName.String()))
			defer jobRunRepo.AssertExpectations(t)

			dummyExecutorInput := scheduler.ExecutorInput{
				Configs: scheduler.ConfigMap{
					"someKey": "someValue",
				},
			}

			jobToCompile := job
			jobToCompile.Task.Config["EXECUTION_PROJECT"] = "example"
			jobToCompileDetails := scheduler.JobWithDetails{Job: &jobToCompile}

			jobReplayRepo := new(ReplayRepository)
			jobReplayRepo.On("GetReplayJobConfig", ctx, tnnt, jobName, someScheduleTime).Return(map[string]string{"EXECUTION_PROJECT": "example"}, nil)
			defer jobReplayRepo.AssertExpectations(t)

			jobInputCompiler := new(mockJobInputCompiler)
			jobInputCompiler.On("Compile", ctx, &jobToCompileDetails, runConfig, someScheduleTime).
				Return(&dummyExecutorInput, nil)
			defer jobInputCompiler.AssertExpectations(t)

			runService := service.NewJobRunService(logger,
				jobRepo, jobRunRepo, jobReplayRepo, nil, nil, nil, jobInputCompiler, nil, nil)
			executorInput, err := runService.JobRunInput(ctx, projName, jobName, runConfig)

			assert.Equal(t, &dummyExecutorInput, executorInput)
			assert.Nil(t, err)
		})
		t.Run("should not return error if get job run fails", func(t *testing.T) {
			tnnt, _ := tenant.NewTenant(projName.String(), namespaceName.String())
			job := scheduler.Job{
				Name:   jobName,
				Tenant: tnnt,
				Task: &scheduler.Task{
					Config: map[string]string{},
				},
			}
			details := scheduler.JobWithDetails{Job: &job}

			someScheduleTime := todayDate.Add(time.Hour * 24 * -1)
			jobRunID := scheduler.JobRunID(uuid.New())
			runConfig := scheduler.RunConfig{
				Executor:    scheduler.Executor{},
				ScheduledAt: someScheduleTime,
				JobRunID:    jobRunID,
			}

			jobRepo := new(JobRepository)
			jobRepo.On("GetJobDetails", ctx, projName, jobName).
				Return(&details, nil)
			defer jobRepo.AssertExpectations(t)

			jobRunRepo := new(mockJobRunRepository)
			jobRunRepo.On("GetByID", ctx, jobRunID).
				Return(&scheduler.JobRun{}, fmt.Errorf("some error other than not found error "))
			defer jobRunRepo.AssertExpectations(t)

			dummyExecutorInput := scheduler.ExecutorInput{
				Configs: scheduler.ConfigMap{
					"someKey": "someValue",
				},
			}
			jobToCompile := job
			jobToCompile.Task.Config["EXECUTION_PROJECT"] = "example"
			jobToCompileDetails := scheduler.JobWithDetails{Job: &jobToCompile}

			jobReplayRepo := new(ReplayRepository)
			jobReplayRepo.On("GetReplayJobConfig", ctx, tnnt, jobName, someScheduleTime).Return(map[string]string{"EXECUTION_PROJECT": "example"}, nil)
			defer jobReplayRepo.AssertExpectations(t)

			jobInputCompiler := new(mockJobInputCompiler)
			jobInputCompiler.On("Compile", ctx, &jobToCompileDetails, runConfig, someScheduleTime).
				Return(&dummyExecutorInput, nil)
			defer jobInputCompiler.AssertExpectations(t)

			runService := service.NewJobRunService(logger,
				jobRepo, jobRunRepo, jobReplayRepo, nil, nil, nil, jobInputCompiler, nil, nil)
			executorInput, err := runService.JobRunInput(ctx, projName, jobName, runConfig)

			assert.Nil(t, err)
			assert.Equal(t, &dummyExecutorInput, executorInput)
		})
	})

	t.Run("GetJobRunList", func(t *testing.T) {
		startDate, err := time.Parse(time.RFC3339, "2022-03-20T02:00:00+00:00")
		if err != nil {
			t.Errorf("unable to parse the time to test GetJobRuns %v", err)
		}
		endDate, err := time.Parse(time.RFC3339, "2022-03-25T02:00:00+00:00")
		if err != nil {
			t.Errorf("unable to parse the time to test GetJobRuns %v", err)
		}
		jobCron, err := cron.ParseCronSchedule("0 12 * * *")
		if err != nil {
			t.Errorf("unable to parse the interval to test GetJobRuns %v", err)
		}
		t.Run("should not able to get job runs when unable to get job details", func(t *testing.T) {
			criteria := &scheduler.JobRunsCriteria{
				Name:      "sample_select",
				StartDate: startDate,
				EndDate:   endDate,
				Filter:    []string{"success"},
			}

			jobRepo := new(JobRepository)
			jobRepo.On("GetJobDetails", ctx, projName, jobName).Return(nil, fmt.Errorf("some error in get job details"))
			defer jobRepo.AssertExpectations(t)

			runService := service.NewJobRunService(logger,
				jobRepo, nil, nil, nil, nil, nil, nil, nil, nil)
			returnedRuns, err := runService.GetJobRuns(ctx, projName, jobName, criteria)
			assert.Error(t, err)
			assert.ErrorContains(t, err, "unable to get job details for jobName: sample_select, project:proj")
			assert.Nil(t, returnedRuns)
		})
		t.Run("should not able to get job runs when scheduler returns empty response", func(t *testing.T) {
			tnnt, _ := tenant.NewTenant(projName.String(), namespaceName.String())
			job := scheduler.Job{
				Name:   jobName,
				Tenant: tnnt,
			}
			jobWithDetails := scheduler.JobWithDetails{
				Job: &job,
				JobMetadata: &scheduler.JobMetadata{
					Version: 1,
				},
				Schedule: &scheduler.Schedule{
					StartDate: startDate.Add(-time.Hour * 24),
					EndDate:   nil,
					Interval:  "0 12 * * *",
				},
			}

			criteria := &scheduler.JobRunsCriteria{
				Name:      "sample_select",
				StartDate: startDate,
				EndDate:   endDate,
				Filter:    []string{"success"},
			}
			sch := new(mockScheduler)
			sch.On("GetJobRuns", ctx, tnnt, criteria, jobCron).Return([]*scheduler.JobRunStatus{}, nil)
			defer sch.AssertExpectations(t)
			jobRepo := new(JobRepository)
			jobRepo.On("GetJobDetails", ctx, projName, jobName).Return(&jobWithDetails, nil)
			defer jobRepo.AssertExpectations(t)

			runService := service.NewJobRunService(logger,
				jobRepo, nil, nil, nil, sch, nil, nil, nil, nil)
			returnedRuns, err := runService.GetJobRuns(ctx, projName, jobName, criteria)
			assert.Nil(t, err)
			assert.Nil(t, returnedRuns)
		})
		t.Run("should able to get job runs when scheduler returns valid response", func(t *testing.T) {
			tnnt, _ := tenant.NewTenant(projName.String(), namespaceName.String())
			job := scheduler.Job{
				Name:   jobName,
				Tenant: tnnt,
			}
			jobWithDetails := scheduler.JobWithDetails{
				Job: &job,
				JobMetadata: &scheduler.JobMetadata{
					Version: 1,
				},
				Schedule: &scheduler.Schedule{
					StartDate: startDate.Add(-time.Hour * 24),
					EndDate:   nil,
					Interval:  "0 12 * * *",
				},
			}

			runsFromScheduler, err := mockGetJobRuns(5, startDate, jobWithDetails.Schedule.Interval, scheduler.StateSuccess)
			if err != nil {
				t.Errorf("unable to parse the interval to test GetJobRuns %v", err)
			}
			runsFromSchFor3days, err := mockGetJobRuns(3, startDate, jobWithDetails.Schedule.Interval, scheduler.StateSuccess)
			if err != nil {
				t.Errorf("unable to build mock job runs to test GetJobRunList for success state %v", err)
			}
			expPendingRuns, err := mockGetJobRuns(2, startDate.Add(time.Hour*24*3), jobWithDetails.Schedule.Interval, scheduler.StatePending)
			if err != nil {
				t.Errorf("unable to build mock job runs to test GetJobRunList for pending state %v", err)
			}
			type cases struct {
				description    string
				input          *scheduler.JobRunsCriteria
				job            scheduler.JobWithDetails
				runs           []*scheduler.JobRunStatus
				expectedResult []*scheduler.JobRunStatus
			}
			for _, scenario := range []cases{
				{
					description: "filtering based on success",
					input: &scheduler.JobRunsCriteria{
						Name:      "sample_select",
						StartDate: startDate,
						EndDate:   endDate,
						Filter:    []string{scheduler.StateSuccess.String()},
					},
					job:            jobWithDetails,
					runs:           runsFromScheduler,
					expectedResult: runsFromScheduler,
				},
				{
					description: "filtering based on failed",
					input: &scheduler.JobRunsCriteria{
						Name:      "sample_select",
						StartDate: startDate,
						EndDate:   endDate,
						Filter:    []string{scheduler.StateFailed.String()},
					},
					job:            jobWithDetails,
					expectedResult: nil,
				},
				{
					description: "no filterRuns applied",
					input: &scheduler.JobRunsCriteria{
						Name:      "sample_select",
						StartDate: startDate,
						EndDate:   endDate,
						Filter:    []string{},
					},
					job:            jobWithDetails,
					runs:           runsFromScheduler,
					expectedResult: runsFromScheduler,
				},
				{
					description: "filtering based on pending",
					input: &scheduler.JobRunsCriteria{
						Name:      "sample_select",
						StartDate: startDate,
						EndDate:   endDate,
						Filter:    []string{scheduler.StatePending.String()},
					},
					job:            jobWithDetails,
					runs:           runsFromScheduler,
					expectedResult: nil,
				},
				{
					description: "when some job instances are not started by scheduler and filtered based on pending status",
					input: &scheduler.JobRunsCriteria{
						Name:      "sample_select",
						StartDate: startDate,
						EndDate:   endDate,
						Filter:    []string{scheduler.StatePending.String()},
					},
					job:            jobWithDetails,
					runs:           runsFromSchFor3days,
					expectedResult: expPendingRuns,
				},
				{
					description: "when some job instances are not started by scheduler and filtered based on success status",
					input: &scheduler.JobRunsCriteria{
						Name:      "sample_select",
						StartDate: startDate,
						EndDate:   endDate,
						Filter:    []string{scheduler.StateSuccess.String()},
					},
					job:            jobWithDetails,
					runs:           runsFromSchFor3days,
					expectedResult: runsFromSchFor3days,
				},
				{
					description: "when some job instances are not started by scheduler and no filterRuns applied",
					input: &scheduler.JobRunsCriteria{
						Name:      "sample_select",
						StartDate: startDate,
						EndDate:   endDate,
						Filter:    []string{},
					},
					job:            jobWithDetails,
					runs:           runsFromSchFor3days,
					expectedResult: append(runsFromSchFor3days, expPendingRuns...),
				},
			} {
				t.Run(scenario.description, func(t *testing.T) {
					sch := new(mockScheduler)
					sch.On("GetJobRuns", ctx, tnnt, scenario.input, jobCron).Return(scenario.runs, nil)
					defer sch.AssertExpectations(t)
					jobRepo := new(JobRepository)
					jobRepo.On("GetJobDetails", ctx, projName, jobName).Return(&jobWithDetails, nil)
					defer jobRepo.AssertExpectations(t)
					runService := service.NewJobRunService(logger,
						jobRepo, nil, nil, nil, sch, nil, nil, nil, nil)
					returnedRuns, err := runService.GetJobRuns(ctx, projName, jobName, scenario.input)
					assert.Nil(t, err)
					assert.Equal(t, scenario.expectedResult, returnedRuns)
				})
			}
		})
		t.Run("should not able to get job runs when invalid date range is given", func(t *testing.T) {
			tnnt, _ := tenant.NewTenant(projName.String(), namespaceName.String())
			job := scheduler.Job{
				Name:   jobName,
				Tenant: tnnt,
			}
			jobWithDetails := scheduler.JobWithDetails{
				Job: &job,
				JobMetadata: &scheduler.JobMetadata{
					Version: 1,
				},
				Schedule: &scheduler.Schedule{
					StartDate: startDate.Add(-time.Hour * 24),
					EndDate:   nil,
					Interval:  "0 12 * * *",
				},
			}

			jobQuery := &scheduler.JobRunsCriteria{
				Name:      "sample_select",
				StartDate: startDate.Add(-time.Hour * 24 * 2),
				EndDate:   endDate,
				Filter:    []string{"success"},
			}

			jobRepo := new(JobRepository)
			jobRepo.On("GetJobDetails", ctx, projName, jobName).Return(&jobWithDetails, nil)
			defer jobRepo.AssertExpectations(t)

			runService := service.NewJobRunService(logger, jobRepo, nil, nil, nil, nil, nil, nil, nil, nil)
			returnedRuns, err := runService.GetJobRuns(ctx, projName, jobName, jobQuery)
			assert.Error(t, err)
			assert.ErrorContains(t, err, "invalid date range, interval contains dates before job start")
			assert.Nil(t, returnedRuns)
		})
		t.Run("should not able to get job runs when invalid cron interval present at DB", func(t *testing.T) {
			tnnt, _ := tenant.NewTenant(projName.String(), namespaceName.String())
			job := scheduler.Job{
				Name:   jobName,
				Tenant: tnnt,
			}
			jobWithDetails := scheduler.JobWithDetails{
				Job: &job,
				JobMetadata: &scheduler.JobMetadata{
					Version: 1,
				},
				Schedule: &scheduler.Schedule{
					StartDate: startDate.Add(-time.Hour * 24),
					EndDate:   nil,
					Interval:  "invalid interval",
				},
			}

			jobQuery := &scheduler.JobRunsCriteria{
				Name:      "sample_select",
				StartDate: startDate,
				EndDate:   endDate,
				Filter:    []string{"success"},
			}

			jobRepo := new(JobRepository)
			jobRepo.On("GetJobDetails", ctx, projName, jobName).Return(&jobWithDetails, nil)
			defer jobRepo.AssertExpectations(t)

			runService := service.NewJobRunService(logger, jobRepo, nil, nil, nil, nil, nil, nil, nil, nil)
			returnedRuns, err := runService.GetJobRuns(ctx, projName, jobName, jobQuery)
			assert.Error(t, err)
			assert.ErrorContains(t, err, "unable to parse job cron interval: expected exactly 5 fields, found 2: [invalid interval]")
			assert.Nil(t, returnedRuns)
		})
		t.Run("should not able to get job runs when no cron interval present at DB", func(t *testing.T) {
			tnnt, _ := tenant.NewTenant(projName.String(), namespaceName.String())
			job := scheduler.Job{
				Name:   jobName,
				Tenant: tnnt,
			}
			jobWithDetails := scheduler.JobWithDetails{
				Job: &job,
				JobMetadata: &scheduler.JobMetadata{
					Version: 1,
				},
				Schedule: &scheduler.Schedule{
					StartDate: startDate.Add(-time.Hour * 24),
					EndDate:   nil,
					Interval:  "",
				},
			}

			jobQuery := &scheduler.JobRunsCriteria{
				Name:      "sample_select",
				StartDate: startDate,
				EndDate:   endDate,
				Filter:    []string{"success"},
			}
			jobRepo := new(JobRepository)
			jobRepo.On("GetJobDetails", ctx, projName, jobName).Return(&jobWithDetails, nil)
			defer jobRepo.AssertExpectations(t)

			runService := service.NewJobRunService(logger, jobRepo, nil, nil, nil, nil, nil, nil, nil, nil)
			returnedRuns, err := runService.GetJobRuns(ctx, projName, jobName, jobQuery)
			assert.Error(t, err)
			assert.ErrorContains(t, err, "cannot get job runs, job interval is empty")
			assert.Nil(t, returnedRuns)
		})
		t.Run("should not able to get job runs when no start date present at DB", func(t *testing.T) {
			tnnt, _ := tenant.NewTenant(projName.String(), namespaceName.String())
			job := scheduler.Job{
				Name:   jobName,
				Tenant: tnnt,
			}
			jobWithDetails := scheduler.JobWithDetails{
				Job: &job,
				JobMetadata: &scheduler.JobMetadata{
					Version: 1,
				},
				Schedule: &scheduler.Schedule{
					EndDate:  nil,
					Interval: "0 12 * * *",
				},
			}

			jobQuery := &scheduler.JobRunsCriteria{
				Name:      "sample_select",
				StartDate: startDate,
				EndDate:   endDate,
				Filter:    []string{"success"},
			}
			jobRepo := new(JobRepository)
			jobRepo.On("GetJobDetails", ctx, projName, jobName).Return(&jobWithDetails, nil)
			defer jobRepo.AssertExpectations(t)
			runService := service.NewJobRunService(logger, jobRepo, nil, nil, nil, nil, nil, nil, nil, nil)
			returnedRuns, err := runService.GetJobRuns(ctx, projName, jobName, jobQuery)
			assert.Error(t, err)
			assert.ErrorContains(t, err, "job schedule startDate not found in job")
			assert.Nil(t, returnedRuns)
		})
		t.Run("should able to get job runs when only last run is required", func(t *testing.T) {
			tnnt, _ := tenant.NewTenant(projName.String(), namespaceName.String())
			job := scheduler.Job{
				Name:   jobName,
				Tenant: tnnt,
			}
			jobWithDetails := scheduler.JobWithDetails{
				Job: &job,
				JobMetadata: &scheduler.JobMetadata{
					Version: 1,
				},
				Schedule: &scheduler.Schedule{
					StartDate: startDate.Add(-time.Hour * 24),
					EndDate:   nil,
					Interval:  "0 12 * * *",
				},
			}

			criteria := &scheduler.JobRunsCriteria{
				Name:        "sample_select",
				OnlyLastRun: true,
			}

			runs := []*scheduler.JobRunStatus{
				{
					State:       scheduler.StateSuccess,
					ScheduledAt: endDate,
				},
			}

			sch := new(mockScheduler)
			sch.On("GetJobRuns", ctx, tnnt, criteria, jobCron).Return(runs, nil)
			defer sch.AssertExpectations(t)
			jobRepo := new(JobRepository)
			jobRepo.On("GetJobDetails", ctx, projName, jobName).Return(&jobWithDetails, nil)
			defer jobRepo.AssertExpectations(t)

			runService := service.NewJobRunService(logger, jobRepo, nil, nil, nil, sch, nil, nil, nil, nil)
			returnedRuns, err := runService.GetJobRuns(ctx, projName, jobName, criteria)
			assert.Nil(t, err)
			assert.Equal(t, runs, returnedRuns)
		})
	})

	t.Run("GetInterval", func(t *testing.T) {
		referenceTime := time.Now()

		conf := map[string]string{
			"STORAGE_PATH":   "file://",
			"SCHEDULER_HOST": "http://scheduler",
		}
		vars := map[string]string{}

		project, err := tenant.NewProject(projName.String(), conf, vars)
		assert.NotNil(t, project)
		assert.NoError(t, err)

		preset, err := tenant.NewPreset("yesterday", "preset for test", "1d", "1d", "", "")
		assert.NotZero(t, preset)
		assert.NoError(t, err)

		presets := map[string]tenant.Preset{
			"yesterday": preset,
		}
		project.SetPresets(presets)

		t.Run("returns zero and error if cannot get project by its name", func(t *testing.T) {
			projectGetter := new(mockProjectGetter)
			defer projectGetter.AssertExpectations(t)

			projectGetter.On("Get", ctx, projName).Return(nil, errors.NewError(errors.ErrInternalError, tenant.EntityProject, "unexpected error"))

			service := service.NewJobRunService(logger, nil, nil, nil, nil, nil, nil, nil, nil, projectGetter)

			actualInterval, actualError := service.GetInterval(ctx, projName, jobName, referenceTime)

			assert.Zero(t, actualInterval)
			assert.ErrorContains(t, actualError, "unexpected error")
		})

		t.Run("returns zero and error if cannot get project by its name", func(t *testing.T) {
			projectGetter := new(mockProjectGetter)
			defer projectGetter.AssertExpectations(t)

			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			projectGetter.On("Get", ctx, projName).Return(project, nil)

			jobRepo.On("GetJobDetails", ctx, projName, jobName).Return(nil, errors.NewError(errors.ErrInternalError, job.EntityJob, "unexpected error"))

			service := service.NewJobRunService(logger, jobRepo, nil, nil, nil, nil, nil, nil, nil, projectGetter)

			actualInterval, actualError := service.GetInterval(ctx, projName, jobName, referenceTime)

			assert.Zero(t, actualInterval)
			assert.ErrorContains(t, actualError, "unexpected error")
		})

		t.Run("returns interval and nil if custom window v3 is used", func(t *testing.T) {
			projectGetter := new(mockProjectGetter)
			defer projectGetter.AssertExpectations(t)

			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			projectGetter.On("Get", ctx, projName).Return(project, nil)

			windowConfig, err := window.NewConfig(preset.Config().Size, preset.Config().ShiftBy, preset.Config().Location, preset.Config().TruncateTo)
			assert.NotNil(t, windowConfig)
			assert.NoError(t, err)

			job := &scheduler.JobWithDetails{
				Name: jobName,
				Schedule: &scheduler.Schedule{
					Interval: "0 * * * *",
				},
				Job: &scheduler.Job{
					WindowConfig: windowConfig,
				},
				JobMetadata: &scheduler.JobMetadata{
					Version: window.NewWindowVersion,
				},
			}

			jobRepo.On("GetJobDetails", ctx, projName, jobName).Return(job, nil)
			service := service.NewJobRunService(logger, jobRepo, nil, nil, nil, nil, nil, nil, nil, projectGetter)

			actualInterval, actualError := service.GetInterval(ctx, projName, jobName, referenceTime)

			assert.NotZero(t, actualInterval)
			assert.NoError(t, actualError)
		})

		t.Run("returns interval and nil if no error is encountered", func(t *testing.T) {
			projectGetter := new(mockProjectGetter)
			defer projectGetter.AssertExpectations(t)

			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			projectGetter.On("Get", ctx, projName).Return(project, nil)

			windowConfig, err := window.NewPresetConfig("yesterday")
			assert.NotNil(t, windowConfig)
			assert.NoError(t, err)

			job := &scheduler.JobWithDetails{
				Name: jobName,
				Schedule: &scheduler.Schedule{
					Interval: "0 * * * *",
				},
				Job: &scheduler.Job{
					WindowConfig: windowConfig,
				},
			}

			jobRepo.On("GetJobDetails", ctx, projName, jobName).Return(job, nil)

			service := service.NewJobRunService(logger, jobRepo, nil, nil, nil, nil, nil, nil, nil, projectGetter)

			actualInterval, actualError := service.GetInterval(ctx, projName, jobName, referenceTime)

			assert.NotZero(t, actualInterval)
			assert.NoError(t, actualError)
		})
	})

	t.Run("GetJobRunsByFilter", func(t *testing.T) {
		t.Run("should return job runs by time range when start date and end date filters are provided", func(t *testing.T) {
			startDate := time.Now().Add(-24 * time.Hour)
			endDate := time.Now()
			runState := scheduler.StateSuccess

			jobRunRepo := new(mockJobRunRepository)
			jobRunRepo.On("GetRunsByTimeRange", ctx, projName, jobName, &runState, startDate, endDate).Return([]*scheduler.JobRun{
				{
					ID:        uuid.New(),
					JobName:   jobName,
					Tenant:    tnnt,
					StartTime: startDate,
					State:     runState,
				},
			}, nil)
			defer jobRunRepo.AssertExpectations(t)

			runService := service.NewJobRunService(logger, nil, jobRunRepo, nil, nil, nil, nil, nil, nil, nil)

			filters := []filter.FilterOpt{
				filter.WithTime(filter.StartDate, startDate),
				filter.WithTime(filter.EndDate, endDate),
				filter.WithString(filter.RunState, runState.String()),
			}
			jobRuns, err := runService.GetJobRunsByFilter(ctx, projName, jobName, filters...)

			assert.Nil(t, err)
			assert.Len(t, jobRuns, 1)
			assert.Equal(t, jobName, jobRuns[0].JobName)
			assert.Equal(t, runState, jobRuns[0].State)
		})

		t.Run("should return latest job run when no date filters are provided", func(t *testing.T) {
			runState := scheduler.StateSuccess
			jobRun := &scheduler.JobRun{
				ID:        uuid.New(),
				JobName:   jobName,
				Tenant:    tnnt,
				StartTime: time.Now(),
				State:     runState,
			}

			jobRunRepo := new(mockJobRunRepository)
			jobRunRepo.On("GetLatestRun", ctx, projName, jobName, &runState).Return(jobRun, nil)
			defer jobRunRepo.AssertExpectations(t)

			runService := service.NewJobRunService(logger, nil, jobRunRepo, nil, nil, nil, nil, nil, nil, nil)

			filters := []filter.FilterOpt{
				filter.WithString(filter.RunState, runState.String()),
			}
			jobRuns, err := runService.GetJobRunsByFilter(ctx, projName, jobName, filters...)

			assert.Nil(t, err)
			assert.Len(t, jobRuns, 1)
			assert.Equal(t, jobName, jobRuns[0].JobName)
			assert.Equal(t, runState, jobRuns[0].State)
		})

		t.Run("should return error when GetRunsByTimeRange fails", func(t *testing.T) {
			startDate := time.Now().Add(-24 * time.Hour)
			endDate := time.Now()
			runState := scheduler.StateSuccess

			jobRunRepo := new(mockJobRunRepository)
			var runsByTimeRange []*scheduler.JobRun
			jobRunRepo.On("GetRunsByTimeRange", ctx, projName, jobName, &runState, startDate, endDate).Return(runsByTimeRange, fmt.Errorf("some error"))
			defer jobRunRepo.AssertExpectations(t)

			runService := service.NewJobRunService(logger, nil, jobRunRepo, nil, nil, nil, nil, nil, nil, nil)

			filters := []filter.FilterOpt{
				filter.WithTime(filter.StartDate, startDate),
				filter.WithTime(filter.EndDate, endDate),
				filter.WithString(filter.RunState, runState.String()),
			}
			jobRuns, err := runService.GetJobRunsByFilter(ctx, projName, jobName, filters...)

			assert.NotNil(t, err)
			assert.Nil(t, jobRuns)
			assert.EqualError(t, err, "some error")
		})

		t.Run("should return error when GetLatestRun fails", func(t *testing.T) {
			runState := scheduler.StateSuccess

			jobRunRepo := new(mockJobRunRepository)
			var jobRun *scheduler.JobRun
			jobRunRepo.On("GetLatestRun", ctx, projName, jobName, &runState).Return(jobRun, fmt.Errorf("some error"))
			defer jobRunRepo.AssertExpectations(t)

			runService := service.NewJobRunService(logger, nil, jobRunRepo, nil, nil, nil, nil, nil, nil, nil)

			filters := []filter.FilterOpt{
				filter.WithString(filter.RunState, runState.String()),
			}
			jobRuns, err := runService.GetJobRunsByFilter(ctx, projName, jobName, filters...)

			assert.NotNil(t, err)
			assert.Nil(t, jobRuns)
			assert.EqualError(t, err, "some error")
		})
	})
}

func mockGetJobRuns(afterDays int, date time.Time, interval string, status scheduler.State) ([]*scheduler.JobRunStatus, error) {
	var expRuns []*scheduler.JobRunStatus
	schSpec, err := cron.ParseCronSchedule(interval)
	if err != nil {
		return expRuns, err
	}
	nextStart := schSpec.Next(date.Add(-time.Second * 1))
	for i := 0; i < afterDays; i++ {
		expRuns = append(expRuns, &scheduler.JobRunStatus{
			State:       status,
			ScheduledAt: nextStart,
		})
		nextStart = schSpec.Next(nextStart)
	}
	return expRuns, nil
}

type mockJobInputCompiler struct {
	mock.Mock
}

func (m *mockJobInputCompiler) Compile(ctx context.Context, job *scheduler.JobWithDetails, config scheduler.RunConfig, executedAt time.Time) (*scheduler.ExecutorInput, error) {
	args := m.Called(ctx, job, config, executedAt)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*scheduler.ExecutorInput), args.Error(1)
}

type mockJobRunRepository struct {
	mock.Mock
}

func (m *mockJobRunRepository) GetByID(ctx context.Context, id scheduler.JobRunID) (*scheduler.JobRun, error) {
	args := m.Called(ctx, id)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*scheduler.JobRun), args.Error(1)
}

func (m *mockJobRunRepository) GetByScheduledAt(ctx context.Context, tenant tenant.Tenant, name scheduler.JobName, scheduledAt time.Time) (*scheduler.JobRun, error) {
	args := m.Called(ctx, tenant, name, scheduledAt)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*scheduler.JobRun), args.Error(1)
}

func (m *mockJobRunRepository) Create(ctx context.Context, tenant tenant.Tenant, name scheduler.JobName, scheduledAt time.Time, slaDefinitionInSec int64) error {
	args := m.Called(ctx, tenant, name, scheduledAt, slaDefinitionInSec)
	return args.Error(0)
}

func (m *mockJobRunRepository) GetLatestRun(ctx context.Context, project tenant.ProjectName, jobName scheduler.JobName, runState *scheduler.State) (*scheduler.JobRun, error) {
	args := m.Called(ctx, project, jobName, runState)
	return args.Get(0).(*scheduler.JobRun), args.Error(1)
}

func (m *mockJobRunRepository) GetRunsByTimeRange(ctx context.Context, project tenant.ProjectName, jobName scheduler.JobName, runState *scheduler.State, since, until time.Time) ([]*scheduler.JobRun, error) {
	args := m.Called(ctx, project, jobName, runState, since, until)
	return args.Get(0).([]*scheduler.JobRun), args.Error(1)
}

func (m *mockJobRunRepository) Update(ctx context.Context, jobRunID uuid.UUID, endTime time.Time, jobRunStatus scheduler.State) error {
	args := m.Called(ctx, jobRunID, endTime, jobRunStatus)
	return args.Error(0)
}

func (m *mockJobRunRepository) UpdateState(ctx context.Context, jobRunID uuid.UUID, jobRunStatus scheduler.State) error {
	args := m.Called(ctx, jobRunID, jobRunStatus)
	return args.Error(0)
}

func (m *mockJobRunRepository) GetByScheduledTimes(ctx context.Context, tenant tenant.Tenant, jobName scheduler.JobName, scheduledTimes []time.Time) ([]*scheduler.JobRun, error) {
	args := m.Called(ctx, tenant, jobName, scheduledTimes)
	return args.Get(0).([]*scheduler.JobRun), args.Error(1)
}

func (m *mockJobRunRepository) UpdateSLA(ctx context.Context, jobName scheduler.JobName, project tenant.ProjectName, scheduledTimes []time.Time) error {
	args := m.Called(ctx, jobName, project, scheduledTimes)
	return args.Error(0)
}

func (m *mockJobRunRepository) UpdateMonitoring(ctx context.Context, jobRunID uuid.UUID, monitoring map[string]any) error {
	args := m.Called(ctx, jobRunID, monitoring)
	return args.Error(0)
}

type JobRepository struct {
	mock.Mock
}

func (j *JobRepository) GetJob(ctx context.Context, name tenant.ProjectName, jobName scheduler.JobName) (*scheduler.Job, error) {
	args := j.Called(ctx, name, jobName)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*scheduler.Job), args.Error(1)
}

func (j *JobRepository) GetJobDetails(ctx context.Context, projectName tenant.ProjectName, jobName scheduler.JobName) (*scheduler.JobWithDetails, error) {
	args := j.Called(ctx, projectName, jobName)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*scheduler.JobWithDetails), args.Error(1)
}

func (j *JobRepository) GetAll(ctx context.Context, projectName tenant.ProjectName) ([]*scheduler.JobWithDetails, error) {
	args := j.Called(ctx, projectName)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]*scheduler.JobWithDetails), args.Error(1)
}

func (j *JobRepository) GetJobs(ctx context.Context, projectName tenant.ProjectName, jobs []string) ([]*scheduler.JobWithDetails, error) {
	args := j.Called(ctx, projectName, jobs)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]*scheduler.JobWithDetails), args.Error(1)
}

type mockScheduler struct {
	mock.Mock
}

func (ms *mockScheduler) GetJobRuns(ctx context.Context, t tenant.Tenant, criteria *scheduler.JobRunsCriteria, jobCron *cron.ScheduleSpec) ([]*scheduler.JobRunStatus, error) {
	args := ms.Called(ctx, t, criteria, jobCron)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]*scheduler.JobRunStatus), args.Error(1)
}

func (ms *mockScheduler) DeployJobs(ctx context.Context, t tenant.Tenant, jobs []*scheduler.JobWithDetails) error {
	args := ms.Called(ctx, t, jobs)
	return args.Error(0)
}

func (ms *mockScheduler) ListJobs(ctx context.Context, t tenant.Tenant) ([]string, error) {
	args := ms.Called(ctx, t)
	return args.Get(0).([]string), args.Error(1)
}

func (ms *mockScheduler) DeleteJobs(ctx context.Context, t tenant.Tenant, jobsToDelete []string) error {
	args := ms.Called(ctx, t, jobsToDelete)
	return args.Error(0)
}

func (ms *mockScheduler) UpdateJobState(ctx context.Context, tnnt tenant.Tenant, jobNames []job.Name, state string) error {
	args := ms.Called(ctx, tnnt, jobNames, state)
	return args.Error(0)
}

type mockOperatorRunRepository struct {
	mock.Mock
}

func (m *mockOperatorRunRepository) GetOperatorRun(ctx context.Context, operatorName string, operator scheduler.OperatorType, jobRunID uuid.UUID) (*scheduler.OperatorRun, error) {
	args := m.Called(ctx, operatorName, operator, jobRunID)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*scheduler.OperatorRun), args.Error(1)
}

func (m *mockOperatorRunRepository) CreateOperatorRun(ctx context.Context, operatorName string, operator scheduler.OperatorType, jobRunID uuid.UUID, startTime time.Time) error {
	args := m.Called(ctx, operatorName, operator, jobRunID, startTime)
	return args.Error(0)
}

func (m *mockOperatorRunRepository) UpdateOperatorRun(ctx context.Context, operator scheduler.OperatorType, jobRunID uuid.UUID, eventTime time.Time, state scheduler.State) error {
	args := m.Called(ctx, operator, jobRunID, eventTime, state)
	return args.Error(0)
}

type mockEventHandler struct {
	mock.Mock
}

func (m *mockEventHandler) HandleEvent(e moderator.Event) {
	m.Called(e)
}

type mockConstructorEventHandler interface {
	mock.TestingT
	Cleanup(func())
}

func newEventHandler(t mockConstructorEventHandler) *mockEventHandler {
	mock := &mockEventHandler{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}

type mockProjectGetter struct {
	mock.Mock
}

func (m *mockProjectGetter) Get(ctx context.Context, projectName tenant.ProjectName) (*tenant.Project, error) {
	args := m.Called(ctx, projectName)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*tenant.Project), args.Error(1)
}
