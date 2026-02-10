package v1beta1_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/goto/salt/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/goto/optimus/config"
	"github.com/goto/optimus/core/resource"
	"github.com/goto/optimus/core/scheduler"
	"github.com/goto/optimus/core/scheduler/handler/v1beta1"
	"github.com/goto/optimus/core/scheduler/service"
	"github.com/goto/optimus/core/tenant"
	"github.com/goto/optimus/internal/lib/interval"
	"github.com/goto/optimus/internal/lib/window"
	"github.com/goto/optimus/internal/utils/filter"
	pb "github.com/goto/optimus/protos/gotocompany/optimus/core/v1beta1"
)

const (
	AirflowDateFormat = "2006-01-02T15:04:05+00:00"
)

func TestJobRunHandler(t *testing.T) {
	logger := log.NewNoop()
	ctx := context.Background()
	projectName := "a-data-proj"
	jobName := "a-job-name"

	t.Run("GetJobRun", func(t *testing.T) {
		t.Run("should return error if project name is invalid", func(t *testing.T) {
			jobRunHandler := v1beta1.NewJobRunHandler(logger, nil, nil, nil, nil, nil, nil, nil)
			req := &pb.GetJobRunsRequest{
				ProjectName: "",
				JobName:     "job1",
				Since:       timestamppb.Now(),
				Until:       timestamppb.Now(),
				State:       "success",
			}
			resp, err := jobRunHandler.GetJobRuns(ctx, req)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "rpc error: code = InvalidArgument desc = invalid argument for entity project: project name is empty: unable to get job run for job1")
			assert.Nil(t, resp)
		})

		t.Run("should return error if job name is invalid", func(t *testing.T) {
			jobRunHandler := v1beta1.NewJobRunHandler(logger, nil, nil, nil, nil, nil, nil, nil)
			req := &pb.GetJobRunsRequest{
				ProjectName: "proj",
				JobName:     "",
				Since:       timestamppb.Now(),
				Until:       timestamppb.Now(),
				State:       "success",
			}
			resp, err := jobRunHandler.GetJobRuns(ctx, req)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "rpc error: code = InvalidArgument desc = invalid argument for entity jobRun: job name is empty: unable to get job run for ")
			assert.Nil(t, resp)
		})

		t.Run("should return error if state is invalid", func(t *testing.T) {
			jobRunHandler := v1beta1.NewJobRunHandler(logger, nil, nil, nil, nil, nil, nil, nil)
			req := &pb.GetJobRunsRequest{
				ProjectName: "proj",
				JobName:     "job1",
				Since:       timestamppb.Now(),
				Until:       timestamppb.Now(),
				State:       "invalid_state",
			}
			resp, err := jobRunHandler.GetJobRuns(ctx, req)
			assert.NotNil(t, err)
			assert.ErrorContains(t, err, "invalid job run state: invalid_state")
			assert.Nil(t, resp)
		})

		t.Run("should return job runs successfully", func(t *testing.T) {
			jobRuns := []*scheduler.JobRun{
				{
					State:       scheduler.StateSuccess,
					ScheduledAt: time.Now(),
				},
			}
			jobRunService := new(mockJobRunService)
			jobRunService.On("GetJobRunsByFilter", ctx, tenant.ProjectName("proj"), scheduler.JobName("job1"), mock.Anything).
				Return(jobRuns, nil)
			defer jobRunService.AssertExpectations(t)

			jobRunHandler := v1beta1.NewJobRunHandler(logger, jobRunService, nil, nil, nil, nil, nil, nil)
			req := &pb.GetJobRunsRequest{
				ProjectName: "proj",
				JobName:     "job1",
				Since:       timestamppb.Now(),
				Until:       timestamppb.Now(),
				State:       "success",
			}
			resp, err := jobRunHandler.GetJobRuns(ctx, req)
			assert.Nil(t, err)
			assert.Equal(t, len(jobRuns), len(resp.JobRuns))
			for _, expectedRun := range jobRuns {
				var found bool
				for _, respRun := range resp.JobRuns {
					if expectedRun.ScheduledAt.Equal(respRun.ScheduledAt.AsTime()) &&
						expectedRun.State.String() == respRun.State {
						found = true
						break
					}
				}
				if !found {
					assert.Fail(t, fmt.Sprintf("failed to find expected job run %v", expectedRun))
				}
			}
		})

		t.Run("should return error if service returns error", func(t *testing.T) {
			jobRunService := new(mockJobRunService)
			var jobRuns []*scheduler.JobRun
			jobRunService.On("GetJobRunsByFilter", ctx, tenant.ProjectName("proj"), scheduler.JobName("job1"), mock.Anything).
				Return(jobRuns, fmt.Errorf("service error"))
			defer jobRunService.AssertExpectations(t)

			jobRunHandler := v1beta1.NewJobRunHandler(logger, jobRunService, nil, nil, nil, nil, nil, nil)
			req := &pb.GetJobRunsRequest{
				ProjectName: "proj",
				JobName:     "job1",
				Since:       timestamppb.Now(),
				Until:       timestamppb.Now(),
				State:       "success",
			}
			resp, err := jobRunHandler.GetJobRuns(ctx, req)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "rpc error: code = Internal desc = service error: unable to get job run for job1")
			assert.Nil(t, resp)
		})
	})

	t.Run("JobRunInput", func(t *testing.T) {
		t.Run("returns error when project name is invalid", func(t *testing.T) {
			service := new(mockJobRunService)
			handler := v1beta1.NewJobRunHandler(logger, service, nil, nil, nil, nil, nil, nil)

			inputRequest := pb.JobRunInputRequest{
				ProjectName:  "",
				JobName:      "job1",
				ScheduledAt:  timestamppb.Now(),
				InstanceName: "bq2bq",
				InstanceType: pb.InstanceSpec_TYPE_TASK,
				JobrunId:     "",
			}

			_, err := handler.JobRunInput(ctx, &inputRequest)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "rpc error: code = InvalidArgument desc = invalid argument for "+
				"entity project: project name is empty: unable to get job run input for job1")
		})
		t.Run("returns error when job name is invalid", func(t *testing.T) {
			service := new(mockJobRunService)
			handler := v1beta1.NewJobRunHandler(logger, service, nil, nil, nil, nil, nil, nil)

			inputRequest := pb.JobRunInputRequest{
				ProjectName:  "proj",
				JobName:      "",
				ScheduledAt:  timestamppb.Now(),
				InstanceName: "bq2bq",
				InstanceType: pb.InstanceSpec_TYPE_TASK,
				JobrunId:     "",
			}

			_, err := handler.JobRunInput(ctx, &inputRequest)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "rpc error: code = InvalidArgument desc = invalid argument for entity "+
				"jobRun: job name is empty: unable to get job run input for ")
		})
		t.Run("returns error when executor is invalid", func(t *testing.T) {
			service := new(mockJobRunService)
			handler := v1beta1.NewJobRunHandler(logger, service, nil, nil, nil, nil, nil, nil)

			inputRequest := pb.JobRunInputRequest{
				ProjectName:  "proj",
				JobName:      "job1",
				ScheduledAt:  timestamppb.Now(),
				InstanceName: "",
				InstanceType: pb.InstanceSpec_TYPE_TASK,
				JobrunId:     "",
			}

			_, err := handler.JobRunInput(ctx, &inputRequest)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "rpc error: code = InvalidArgument desc = invalid argument for entity "+
				"jobRun: executor name is invalid: unable to get job run input for job1")
		})
		t.Run("returns error when scheduled_at is invalid", func(t *testing.T) {
			service := new(mockJobRunService)
			handler := v1beta1.NewJobRunHandler(logger, service, nil, nil, nil, nil, nil, nil)

			inputRequest := pb.JobRunInputRequest{
				ProjectName:  "proj",
				JobName:      "job1",
				InstanceName: "bq2bq",
				InstanceType: pb.InstanceSpec_TYPE_TASK,
				JobrunId:     "",
			}

			_, err := handler.JobRunInput(ctx, &inputRequest)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "rpc error: code = InvalidArgument desc = invalid argument for entity "+
				"jobRun: invalid scheduled_at: unable to get job run input for job1")
		})
		t.Run("returns error when run config is invalid", func(t *testing.T) {
			service := new(mockJobRunService)
			handler := v1beta1.NewJobRunHandler(logger, service, nil, nil, nil, nil, nil, nil)

			inputRequest := pb.JobRunInputRequest{
				ProjectName:  "proj",
				JobName:      "job1",
				ScheduledAt:  timestamppb.Now(),
				InstanceName: "bq2bq",
				InstanceType: pb.InstanceSpec_TYPE_TASK,
				JobrunId:     "1234",
			}

			_, err := handler.JobRunInput(ctx, &inputRequest)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "rpc error: code = InvalidArgument desc = invalid argument for entity "+
				"jobRun: invalid job run ID 1234: unable to get job run input for job1")
		})
		t.Run("returns error when service returns error", func(t *testing.T) {
			service := new(mockJobRunService)
			service.On("JobRunInput", ctx, tenant.ProjectName("proj"), scheduler.JobName("job1"), mock.Anything).
				Return(&scheduler.ExecutorInput{}, fmt.Errorf("error in service"))
			defer service.AssertExpectations(t)

			handler := v1beta1.NewJobRunHandler(logger, service, nil, nil, nil, nil, nil, nil)

			inputRequest := pb.JobRunInputRequest{
				ProjectName:  "proj",
				JobName:      "job1",
				ScheduledAt:  timestamppb.Now(),
				InstanceName: "bq2bq",
				InstanceType: pb.InstanceSpec_TYPE_TASK,
				JobrunId:     "",
			}

			_, err := handler.JobRunInput(ctx, &inputRequest)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "rpc error: code = Internal desc = error in service: unable to get job "+
				"run input for job1")
		})
		t.Run("returns job run input successfully", func(t *testing.T) {
			service := new(mockJobRunService)
			service.On("JobRunInput", ctx, tenant.ProjectName("proj"), scheduler.JobName("job1"), mock.Anything).
				Return(&scheduler.ExecutorInput{
					Configs: map[string]string{"a": "b"},
					Secrets: map[string]string{"name": "secret_value"},
					Files:   nil,
				}, nil)
			defer service.AssertExpectations(t)

			handler := v1beta1.NewJobRunHandler(logger, service, nil, nil, nil, nil, nil, nil)

			inputRequest := pb.JobRunInputRequest{
				ProjectName:  "proj",
				JobName:      "job1",
				ScheduledAt:  timestamppb.Now(),
				InstanceName: "bq2bq",
				InstanceType: pb.InstanceSpec_TYPE_TASK,
				JobrunId:     "",
			}

			input, err := handler.JobRunInput(ctx, &inputRequest)
			assert.Nil(t, err)
			assert.Equal(t, "b", input.Envs["a"])
			assert.Equal(t, "secret_value", input.Secrets["name"])
		})
	})
	t.Run("GetThirdPartySensorStatus", func(t *testing.T) {
		jakartaLoc, err := time.LoadLocation("Asia/Jakarta")
		if err != nil {
			t.Errorf("unable to get timezone location %v", err)
		}

		t.Run("should return all date based logs of resources underlying dex", func(t *testing.T) {
			jobScheduleTime := time.Date(2025, time.April, 25, 8, 0, 0, 0, time.UTC)

			resourceURN, err := resource.NewURN("maxcompute", "project.dataset.table_name")
			assert.Nil(t, err)

			dexResp := &scheduler.DataCompletenessStatus{
				IsComplete: true,
				DataCompletenessByDate: []*scheduler.DataCompletenessByDate{
					{
						Date:       time.Date(2025, time.April, 24, 7, 0, 0, 0, time.UTC),
						IsComplete: true,
					},
					{
						Date:       time.Date(2025, time.April, 25, 7, 0, 0, 0, time.UTC),
						IsComplete: true,
					},
				},
			}

			dexIntervalStart := time.Date(2025, time.April, 23, 15, 0, 0, 0, jakartaLoc)
			dexIntervalEnd := time.Date(2025, time.April, 25, 15, 0, 0, 0, jakartaLoc)

			thirdPartyClient := new(mockThirdPartyClient)
			thirdPartyClient.On("IsComplete", ctx, resourceURN, dexIntervalStart, dexIntervalEnd).Return(dexResp.IsComplete, dexResp, nil)
			defer thirdPartyClient.AssertExpectations(t)

			upstreamResolverType := config.DexUpstreamResolver

			thirdPartySensorService := new(mockThirdPartySensorService)
			thirdPartySensorService.On("GetClient", upstreamResolverType).Return(thirdPartyClient, nil)
			defer thirdPartySensorService.AssertExpectations(t)

			jobRunService := new(mockJobRunService)
			jobIntervalStart := time.Date(2025, time.April, 23, 8, 0, 0, 0, time.UTC)
			jobIntervalEnd := time.Date(2025, time.April, 25, 8, 0, 0, 0, time.UTC)
			dataInterval := interval.NewInterval(jobIntervalStart, jobIntervalEnd)

			jobRunService.On("GetInterval", ctx, tenant.ProjectName(projectName), scheduler.JobName(jobName), jobScheduleTime).Return(dataInterval, nil)
			defer jobRunService.AssertExpectations(t)

			jobRunHandler := v1beta1.NewJobRunHandler(logger, jobRunService, nil, nil, nil, nil, thirdPartySensorService, nil)

			req := &pb.GetThirdPartySensorRequest{
				ThirdPartyType: upstreamResolverType.String(),
				ProjectName:    projectName,
				JobName:        jobName,
				ScheduledAt:    timestamppb.New(jobScheduleTime),
				Payload: &pb.GetThirdPartySensorRequest_DexSensorRequest{
					DexSensorRequest: &pb.DexSensorRequest{
						ResourceUrn: resourceURN.String(),
					},
				},
			}

			resp, err := jobRunHandler.GetThirdPartySensorStatus(ctx, req)
			assert.Nil(t, err)
			assert.Equal(t, dexResp.IsComplete, resp.GetDexSensorResponse().IsComplete)
			for _, completeness := range resp.GetDexSensorResponse().Log {
				assert.Equal(t, true, completeness.IsComplete)
			}
		})
		t.Run("should return all date based logs of resources underlying dex, except dates Greater than current date", func(t *testing.T) {
			jobScheduleTime := time.Now().UTC()

			resourceURN, err := resource.NewURN("maxcompute", "project.dataset.table_name")
			assert.Nil(t, err)

			todayInJakarta := jobScheduleTime.In(jakartaLoc)

			dexResp := &scheduler.DataCompletenessStatus{
				IsComplete: true,
				DataCompletenessByDate: []*scheduler.DataCompletenessByDate{
					{
						Date:       time.Date(todayInJakarta.Year(), todayInJakarta.Month(), todayInJakarta.Day()-2, 0, 0, 0, 0, jakartaLoc),
						IsComplete: true,
					},
					{
						Date:       time.Date(todayInJakarta.Year(), todayInJakarta.Month(), todayInJakarta.Day()-1, 0, 0, 0, 0, jakartaLoc),
						IsComplete: true,
					},
				},
			}
			dexIntervalStart := jobScheduleTime.Add(-2 * 24 * time.Hour).In(jakartaLoc)

			jakMidnight := time.Date(todayInJakarta.Year(), todayInJakarta.Month(), todayInJakarta.Day(), 0, 0, 0, 0, jakartaLoc)
			dexIntervalEnd := jakMidnight.Add(-1 * time.Minute)

			thirdPartyClient := new(mockThirdPartyClient)
			thirdPartyClient.On("IsComplete", ctx, resourceURN, dexIntervalStart, dexIntervalEnd).Return(dexResp.IsComplete, dexResp, nil)
			defer thirdPartyClient.AssertExpectations(t)

			upstreamResolverType := config.DexUpstreamResolver

			thirdPartySensorService := new(mockThirdPartySensorService)
			thirdPartySensorService.On("GetClient", upstreamResolverType).Return(thirdPartyClient, nil)
			defer thirdPartySensorService.AssertExpectations(t)

			jobRunService := new(mockJobRunService)
			// 2 days window
			jobIntervalStart := jobScheduleTime.Add(-2 * 24 * time.Hour)
			jobIntervalEnd := jobScheduleTime
			dataInterval := interval.NewInterval(jobIntervalStart, jobIntervalEnd)

			jobRunService.On("GetInterval", ctx, tenant.ProjectName(projectName), scheduler.JobName(jobName), jobScheduleTime).Return(dataInterval, nil)
			defer jobRunService.AssertExpectations(t)

			jobRunHandler := v1beta1.NewJobRunHandler(logger, jobRunService, nil, nil, nil, nil, thirdPartySensorService, nil)

			req := &pb.GetThirdPartySensorRequest{
				ThirdPartyType: upstreamResolverType.String(),
				ProjectName:    projectName,
				JobName:        jobName,
				ScheduledAt:    timestamppb.New(jobScheduleTime),
				Payload: &pb.GetThirdPartySensorRequest_DexSensorRequest{
					DexSensorRequest: &pb.DexSensorRequest{
						ResourceUrn: resourceURN.String(),
					},
				},
			}

			resp, err := jobRunHandler.GetThirdPartySensorStatus(ctx, req)
			assert.Nil(t, err)
			assert.NotNil(t, resp)
			assert.Equal(t, dexResp.IsComplete, resp.GetDexSensorResponse().IsComplete)
			for _, completeness := range resp.GetDexSensorResponse().Log {
				assert.Equal(t, true, completeness.IsComplete)
			}
		})
	})
	t.Run("JobRun", func(t *testing.T) {
		date, err := time.Parse(AirflowDateFormat, "2022-03-25T02:00:00+00:00")
		if err != nil {
			t.Errorf("unable to parse the time to test GetJobRuns %v", err)
		}
		t.Run("should return all job run via scheduler if valid inputs are given", func(t *testing.T) {
			job := scheduler.Job{
				Name: "transform-tables",
			}

			jobRuns := []*scheduler.JobRunStatus{{
				ScheduledAt: date,
				State:       scheduler.StateSuccess,
			}}
			query := &scheduler.JobRunsCriteria{
				Name:      job.Name.String(),
				StartDate: date,
				EndDate:   date.Add(time.Hour * 24),
				Filter:    []string{"success"},
			}
			jobRunService := new(mockJobRunService)
			jobRunService.On("GetJobRuns", ctx, tenant.ProjectName(projectName), job.Name, query).Return(jobRuns, "", nil)
			defer jobRunService.AssertExpectations(t)

			jobRunHandler := v1beta1.NewJobRunHandler(logger, jobRunService, nil, nil, nil, nil, nil, nil)

			req := &pb.JobRunRequest{
				ProjectName: projectName,
				JobName:     job.Name.String(),
				StartDate:   timestamppb.New(date),
				EndDate:     timestamppb.New(date.Add(time.Hour * 24)),
				Filter:      []string{"success"},
			}
			resp, err := jobRunHandler.JobRun(ctx, req)
			assert.Nil(t, err)
			assert.Equal(t, len(jobRuns), len(resp.JobRuns))
			for _, expectedStatus := range jobRuns {
				var found bool
				for _, respVal := range resp.JobRuns {
					if expectedStatus.ScheduledAt.Equal(respVal.ScheduledAt.AsTime()) &&
						expectedStatus.State.String() == respVal.State {
						found = true
						break
					}
				}
				if !found {
					assert.Fail(t, fmt.Sprintf("failed to find expected job Run status %v", expectedStatus))
				}
			}
		})
		t.Run("should return all job run via scheduler if valid inputs are given", func(t *testing.T) {
			job := scheduler.Job{
				Name: "transform-tables",
			}

			jobRuns := []*scheduler.JobRunStatus{{
				ScheduledAt: date,
				State:       scheduler.StateSuccess,
			}}
			query := &scheduler.JobRunsCriteria{
				Name:      job.Name.String(),
				StartDate: date,
				EndDate:   date.Add(time.Hour * 24),
				Filter:    []string{"success"},
			}
			jobRunService := new(mockJobRunService)
			jobRunService.On("GetJobRuns", ctx, tenant.ProjectName(projectName), job.Name, query).Return(jobRuns, "", nil)
			defer jobRunService.AssertExpectations(t)

			jobRunHandler := v1beta1.NewJobRunHandler(logger, jobRunService, nil, nil, nil, nil, nil, nil)

			req := &pb.JobRunRequest{
				ProjectName: projectName,
				JobName:     job.Name.String(),
				StartDate:   timestamppb.New(date),
				EndDate:     timestamppb.New(date.Add(time.Hour * 24)),
				Filter:      []string{"success"},
			}
			resp, err := jobRunHandler.JobRun(ctx, req)
			assert.Nil(t, err)
			assert.Equal(t, len(jobRuns), len(resp.JobRuns))
			for _, expectedStatus := range jobRuns {
				var found bool
				for _, respVal := range resp.JobRuns {
					if expectedStatus.ScheduledAt.Equal(respVal.ScheduledAt.AsTime()) &&
						expectedStatus.State.String() == respVal.State {
						found = true
						break
					}
				}
				if !found {
					assert.Fail(t, fmt.Sprintf("failed to find expected job Run status %v", expectedStatus))
				}
			}
		})
		t.Run("should return error if job run service raises error in GetJobRuns", func(t *testing.T) {
			job := scheduler.Job{
				Name: "transform-tables",
			}
			query := &scheduler.JobRunsCriteria{
				Name:        job.Name.String(),
				OnlyLastRun: true,
			}
			jobRunService := new(mockJobRunService)
			jobRunService.On("GetJobRuns", ctx, tenant.ProjectName(projectName), job.Name, query).Return(nil, "", fmt.Errorf("some random error"))
			defer jobRunService.AssertExpectations(t)

			jobRunHandler := v1beta1.NewJobRunHandler(logger, jobRunService, nil, nil, nil, nil, nil, nil)

			req := &pb.JobRunRequest{
				ProjectName: projectName,
				JobName:     job.Name.String(),
				Filter:      []string{"success"},
			}
			resp, err := jobRunHandler.JobRun(ctx, req)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "rpc error: code = Internal desc = some random error: unable to get job run for transform-tables")
			assert.Nil(t, resp)
		})

		t.Run("should not return job runs if project name is not valid", func(t *testing.T) {
			jobRunHandler := v1beta1.NewJobRunHandler(logger, nil, nil, nil, nil, nil, nil, nil)
			req := &pb.JobRunRequest{
				ProjectName: "",
				JobName:     "transform-tables",
				StartDate:   timestamppb.New(date),
				EndDate:     timestamppb.New(date.Add(time.Hour * 24)),
				Filter:      []string{"success"},
			}
			resp, err := jobRunHandler.JobRun(ctx, req)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "rpc error: code = InvalidArgument desc = invalid argument for entity project: project name is empty: unable to get job run for transform-tables")
			assert.Nil(t, resp)
		})

		t.Run("should not return job runs if job name is not valid", func(t *testing.T) {
			jobRunHandler := v1beta1.NewJobRunHandler(logger, nil, nil, nil, nil, nil, nil, nil)
			req := &pb.JobRunRequest{
				ProjectName: "some-project",
				JobName:     "",
				StartDate:   timestamppb.New(date),
				EndDate:     timestamppb.New(date.Add(time.Hour * 24)),
				Filter:      []string{"success"},
			}
			resp, err := jobRunHandler.JobRun(ctx, req)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "rpc error: code = InvalidArgument desc = invalid argument for entity jobRun: job name is empty: unable to get job run for ")
			assert.Nil(t, resp)
		})
		t.Run("should not return job runs if only start date is invalid", func(t *testing.T) {
			jobRunHandler := v1beta1.NewJobRunHandler(logger, nil, nil, nil, nil, nil, nil, nil)
			req := &pb.JobRunRequest{
				ProjectName: "some-project",
				JobName:     "jobname",
				EndDate:     timestamppb.New(date.Add(time.Hour * 24)),
				Filter:      []string{"success"},
			}
			resp, err := jobRunHandler.JobRun(ctx, req)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "rpc error: code = InvalidArgument desc = invalid argument for entity jobRun: empty start date is given: unable to get job run for jobname")
			assert.Nil(t, resp)
		})
		t.Run("should not return job runs if only end date is invalid", func(t *testing.T) {
			jobRunHandler := v1beta1.NewJobRunHandler(logger, nil, nil, nil, nil, nil, nil, nil)
			req := &pb.JobRunRequest{
				ProjectName: "some-project",
				JobName:     "jobname",
				StartDate:   timestamppb.New(date),
				Filter:      []string{"success"},
			}
			resp, err := jobRunHandler.JobRun(ctx, req)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "rpc error: code = InvalidArgument desc = invalid argument for entity jobRun: empty end date is given: unable to get job run for jobname")
			assert.Nil(t, resp)
		})
	})
	t.Run("UploadToScheduler", func(t *testing.T) {
		t.Run("should fail deployment if project name empty", func(t *testing.T) {
			jobRunHandler := v1beta1.NewJobRunHandler(logger, nil, nil, nil, nil, nil, nil, nil)
			namespaceName := "namespace-name"
			req := &pb.UploadToSchedulerRequest{
				ProjectName:   "",
				NamespaceName: &namespaceName,
			}
			resp, err := jobRunHandler.UploadToScheduler(ctx, req)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "rpc error: code = InvalidArgument desc = invalid argument for entity project: project name is empty: unable to get projectName")
			assert.Nil(t, resp)
		})
		t.Run("should return after triggering deploy to scheduler", func(t *testing.T) {
			namespaceName := "namespace-name"
			req := &pb.UploadToSchedulerRequest{
				ProjectName:   projectName,
				NamespaceName: &namespaceName,
			}
			jobRunService := new(mockJobRunService)
			jobRunService.On("UploadToScheduler", ctx, tenant.ProjectName(projectName)).Return(nil)
			jobRunHandler := v1beta1.NewJobRunHandler(logger, jobRunService, nil, nil, nil, nil, nil, nil)

			_, err := jobRunHandler.UploadToScheduler(ctx, req)
			assert.Nil(t, err)
		})
		t.Run("should return error if projectName is not valid", func(t *testing.T) {
			namespaceName := "namespace-name"
			eventValues, _ := structpb.NewStruct(
				map[string]interface{}{
					"url": "https://example.io",
				},
			)
			req := &pb.RegisterJobEventRequest{
				ProjectName:   "",
				JobName:       jobName,
				NamespaceName: namespaceName,
				Event: &pb.JobEvent{
					Type:  9,
					Value: eventValues,
				},
			}
			jobRunHandler := v1beta1.NewJobRunHandler(logger, nil, nil, nil, nil, nil, nil, nil)

			resp, err := jobRunHandler.RegisterJobEvent(ctx, req)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "rpc error: code = InvalidArgument desc = invalid argument for entity project: project name is empty: unable to get tenant")
			assert.Nil(t, resp)
		})

		t.Run("should return error if NamespaceName is not valid", func(t *testing.T) {
			namespaceName := ""
			eventValues, _ := structpb.NewStruct(
				map[string]interface{}{
					"url": "https://example.io",
				},
			)
			req := &pb.RegisterJobEventRequest{
				ProjectName:   projectName,
				JobName:       jobName,
				NamespaceName: namespaceName,
				Event: &pb.JobEvent{
					Type:  9,
					Value: eventValues,
				},
			}
			jobRunHandler := v1beta1.NewJobRunHandler(logger, nil, nil, nil, nil, nil, nil, nil)

			resp, err := jobRunHandler.RegisterJobEvent(ctx, req)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "rpc error: code = InvalidArgument desc = invalid argument for entity namespace: namespace name is empty: unable to get tenant")
			assert.Nil(t, resp)
		})
		t.Run("should return error if job name is not valid", func(t *testing.T) {
			namespaceName := "namespace-name"
			eventValues, _ := structpb.NewStruct(
				map[string]interface{}{
					"url": "https://example.io",
				},
			)
			req := &pb.RegisterJobEventRequest{
				ProjectName:   projectName,
				JobName:       "",
				NamespaceName: namespaceName,
				Event: &pb.JobEvent{
					Type:  9,
					Value: eventValues,
				},
			}
			jobRunHandler := v1beta1.NewJobRunHandler(logger, nil, nil, nil, nil, nil, nil, nil)

			resp, err := jobRunHandler.RegisterJobEvent(ctx, req)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "rpc error: code = InvalidArgument desc = invalid argument for entity jobRun: job name is empty: unable to get job name")
			assert.Nil(t, resp)
		})

		t.Run("should return error on unregistered event type", func(t *testing.T) {
			eventValues, _ := structpb.NewStruct(
				map[string]interface{}{
					"url": "https://example.io",
				},
			)
			namespaceName := "some-namespace"
			req := &pb.RegisterJobEventRequest{
				ProjectName:   projectName,
				JobName:       jobName,
				NamespaceName: namespaceName,
				Event: &pb.JobEvent{
					Type:  200,
					Value: eventValues,
				},
			}
			jobRunHandler := v1beta1.NewJobRunHandler(logger, nil, nil, nil, nil, nil, nil, nil)

			resp, err := jobRunHandler.RegisterJobEvent(ctx, req)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "rpc error: code = InvalidArgument desc = invalid argument for entity event: unknown event 200: unable to parse event")
			assert.Nil(t, resp)
		})
		t.Run("should return error if Update Job State fails", func(t *testing.T) {
			namespaceName := "some-namespace"
			tnnt, _ := tenant.NewTenant(projectName, namespaceName)
			eventValues, _ := structpb.NewStruct(
				map[string]interface{}{
					"url":          "https://example.io",
					"event_time":   1600361600,
					"task_id":      "wait_sample_select",
					"status":       "success",
					"scheduled_at": "2022-01-02T15:04:05Z",
					"event_context": map[string]any{
						"operator_type": "SENSOR",
						"event_type":    "operator_success",
					},
				},
			)
			req := &pb.RegisterJobEventRequest{
				ProjectName:   projectName,
				JobName:       jobName,
				NamespaceName: namespaceName,
				Event: &pb.JobEvent{
					Type:  pb.JobEvent_TYPE_TASK_SUCCESS,
					Value: eventValues,
				},
			}
			event, err := scheduler.EventFrom(
				req.GetEvent().Type.String(),
				req.GetEvent().Value.AsMap(),
				scheduler.JobName(jobName), tnnt,
			)
			assert.Nil(t, err)
			jobRunService := new(mockJobRunService)
			jobRunService.On("UpdateJobState", ctx, event).
				Return(fmt.Errorf("some error"))
			defer jobRunService.AssertExpectations(t)

			notifier := new(mockNotifier)
			notifier.On("Push", ctx, event).
				Return(nil)
			notifier.On("Webhook", ctx, event).
				Return(nil)
			notifier.On("Relay", ctx, event).Return(nil)
			defer jobRunService.AssertExpectations(t)

			jobRunHandler := v1beta1.NewJobRunHandler(logger, jobRunService, notifier, nil, nil, nil, nil, nil)

			resp, err := jobRunHandler.RegisterJobEvent(ctx, req)
			assert.NotNil(t, err)
			assert.ErrorContains(t, err, "scheduler could not update job run state")
			assert.Equal(t, &pb.RegisterJobEventResponse{}, resp)
		})
		t.Run("should return error if notify Push fails", func(t *testing.T) {
			namespaceName := "some-namespace"
			tnnt, _ := tenant.NewTenant(projectName, namespaceName)
			eventValues, _ := structpb.NewStruct(
				map[string]interface{}{
					"url":          "https://example.io",
					"event_time":   1600361600,
					"status":       "success",
					"scheduled_at": "2022-01-02T15:04:05Z",
					"task_id":      "wait_sample_select",
					"event_context": map[string]any{
						"operator_type": "SENSOR",
						"event_type":    "operator_success",
					},
				},
			)
			req := &pb.RegisterJobEventRequest{
				ProjectName:   projectName,
				JobName:       jobName,
				NamespaceName: namespaceName,
				Event: &pb.JobEvent{
					Type:  pb.JobEvent_TYPE_TASK_SUCCESS,
					Value: eventValues,
				},
			}
			event, err := scheduler.EventFrom(
				req.GetEvent().Type.String(),
				req.GetEvent().Value.AsMap(),
				scheduler.JobName(jobName), tnnt,
			)
			assert.Nil(t, err)
			jobRunService := new(mockJobRunService)
			jobRunService.On("UpdateJobState", ctx, event).
				Return(nil)
			defer jobRunService.AssertExpectations(t)

			notifier := new(mockNotifier)
			notifier.On("Push", ctx, event).
				Return(fmt.Errorf("some error"))
			notifier.On("Webhook", ctx, event).
				Return(nil)
			notifier.On("Relay", ctx, event).Return(nil)
			defer jobRunService.AssertExpectations(t)

			jobRunHandler := v1beta1.NewJobRunHandler(logger, jobRunService, notifier, nil, nil, nil, nil, nil)

			resp, err := jobRunHandler.RegisterJobEvent(ctx, req)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "rpc error: code = Internal desc = errors in RegisterJobEvent:\n some error: error in RegisterJobEvent handler")
			assert.Equal(t, &pb.RegisterJobEventResponse{}, resp)
		})
	})
	t.Run("GetInterval", func(t *testing.T) {
		referenceTime := timestamppb.Now()

		t.Run("should return nil and error if project name is invalid", func(t *testing.T) {
			service := new(mockJobRunService)
			defer service.AssertExpectations(t)

			handler := v1beta1.NewJobRunHandler(logger, service, nil, nil, nil, nil, nil, nil)
			request := &pb.GetIntervalRequest{
				ProjectName:   "",
				JobName:       "test_job",
				ReferenceTime: referenceTime,
			}

			actualResponse, actualError := handler.GetInterval(ctx, request)

			assert.Nil(t, actualResponse)
			assert.Error(t, actualError)
			assert.EqualError(t, actualError, "rpc error: code = InvalidArgument desc = invalid argument for entity project: project name is empty: unable to adapt project name")
		})

		t.Run("should return nil and error if job name is invalid", func(t *testing.T) {
			service := new(mockJobRunService)
			defer service.AssertExpectations(t)

			handler := v1beta1.NewJobRunHandler(logger, service, nil, nil, nil, nil, nil, nil)
			request := &pb.GetIntervalRequest{
				ProjectName:   "test_project",
				JobName:       "",
				ReferenceTime: referenceTime,
			}

			actualResponse, actualError := handler.GetInterval(ctx, request)

			assert.Nil(t, actualResponse)
			assert.Error(t, actualError)
			assert.EqualError(t, actualError, "rpc error: code = InvalidArgument desc = invalid argument for entity jobRun: job name is empty: unable to adapt job name")
		})

		t.Run("should return nil and error if reference time is invalid", func(t *testing.T) {
			service := new(mockJobRunService)
			defer service.AssertExpectations(t)

			handler := v1beta1.NewJobRunHandler(logger, service, nil, nil, nil, nil, nil, nil)
			request := &pb.GetIntervalRequest{
				ProjectName:   "test_project",
				JobName:       "test_job",
				ReferenceTime: nil,
			}

			actualResponse, actualError := handler.GetInterval(ctx, request)

			assert.Nil(t, actualResponse)
			assert.Error(t, actualError)
			assert.EqualError(t, actualError, "rpc error: code = InvalidArgument desc = invalid argument for entity jobRun: invalid reference time: unable to get interval for test_job")
		})

		t.Run("should return nil and error if error when getting interval", func(t *testing.T) {
			service := new(mockJobRunService)
			defer service.AssertExpectations(t)

			handler := v1beta1.NewJobRunHandler(logger, service, nil, nil, nil, nil, nil, nil)
			request := &pb.GetIntervalRequest{
				ProjectName:   "test_project",
				JobName:       "test_job",
				ReferenceTime: referenceTime,
			}

			service.On("GetInterval", ctx, mock.Anything, mock.Anything, referenceTime.AsTime()).Return(interval.Interval{}, errors.New("unexpected error"))

			actualResponse, actualError := handler.GetInterval(ctx, request)

			assert.Nil(t, actualResponse)
			assert.Error(t, actualError)
			assert.EqualError(t, actualError, "rpc error: code = Internal desc = unexpected error: error getting interval for job test_job")
		})

		t.Run("should return response and nil if no error is encountered", func(t *testing.T) {
			service := new(mockJobRunService)
			defer service.AssertExpectations(t)

			projectName := "test_project"
			projectConfig := map[string]string{
				"STORAGE_PATH":   "file://",
				"SCHEDULER_HOST": "http://scheduler",
			}
			projectVars := map[string]string{}

			project, err := tenant.NewProject(projectName, projectConfig, projectVars)
			assert.NotNil(t, project)
			assert.NoError(t, err)

			preset := tenant.NewPresetWithConfig("yesterday", "preset for test", window.SimpleConfig{
				Size:       "1d",
				ShiftBy:    "1d",
				Location:   "",
				TruncateTo: "",
			})
			assert.NotZero(t, preset)

			presets := map[string]tenant.Preset{
				"yesterday": preset,
			}
			project.SetPresets(presets)

			windowConfig, err := window.NewPresetConfig("yesterday")
			assert.NotNil(t, windowConfig)
			assert.NoError(t, err)

			window, err := window.From(windowConfig, "0 * * * *", project.GetPreset)
			assert.NotNil(t, window)
			assert.NoError(t, err)

			interval, err := window.GetInterval(referenceTime.AsTime())
			assert.NotNil(t, interval)
			assert.NoError(t, err)

			handler := v1beta1.NewJobRunHandler(logger, service, nil, nil, nil, nil, nil, nil)
			request := &pb.GetIntervalRequest{
				ProjectName:   "test_project",
				JobName:       "test_job",
				ReferenceTime: referenceTime,
			}

			service.On("GetInterval", ctx, mock.Anything, mock.Anything, referenceTime.AsTime()).Return(interval, nil)

			actualResponse, actualError := handler.GetInterval(ctx, request)

			assert.NotNil(t, actualResponse)
			assert.NoError(t, actualError)
		})
	})

	t.Run("GetJobRunLineageSummary", func(t *testing.T) {
		t.Run("should return error when fromJobRunLineageSummaryRequest fails", func(t *testing.T) {
			jobRunService := new(mockJobRunService)
			jobLineageService := new(mockJobLineageService)
			defer jobRunService.AssertExpectations(t)
			defer jobLineageService.AssertExpectations(t)

			handler := v1beta1.NewJobRunHandler(logger, jobRunService, nil, nil, jobLineageService, nil, nil, nil)

			req := &pb.GetJobRunLineageSummaryRequest{
				TargetJobs: []*pb.TargetJobRunIdentifier{
					{
						JobName:     "",
						ScheduledAt: timestamppb.Now(),
					},
				},
				NumberOfUpstreamPerLevel: 5,
			}

			resp, err := handler.GetJobRunLineageSummary(ctx, req)
			assert.NotNil(t, err)
			assert.Nil(t, resp)
			assert.ErrorContains(t, err, "unable to parse job schedules from request")
		})

		t.Run("should return error when job lineage service fails", func(t *testing.T) {
			jobRunService := new(mockJobRunService)
			jobLineageService := new(mockJobLineageService)
			defer jobRunService.AssertExpectations(t)
			defer jobLineageService.AssertExpectations(t)

			handler := v1beta1.NewJobRunHandler(logger, jobRunService, nil, nil, jobLineageService, nil, nil, nil)

			req := &pb.GetJobRunLineageSummaryRequest{
				TargetJobs: []*pb.TargetJobRunIdentifier{
					{
						JobName:     "test-job",
						ScheduledAt: timestamppb.Now(),
					},
				},
				NumberOfUpstreamPerLevel: 5,
			}

			jobLineageService.On("GetJobExecutionSummary", ctx, mock.Anything, 5).
				Return(nil, errors.New("service error"))

			resp, err := handler.GetJobRunLineageSummary(ctx, req)
			assert.NotNil(t, err)
			assert.Nil(t, resp)
			assert.ErrorContains(t, err, "unable to get job run lineage summary")
		})

		t.Run("should return job run lineage summary successfully", func(t *testing.T) {
			jobRunService := new(mockJobRunService)
			jobLineageService := new(mockJobLineageService)
			defer jobRunService.AssertExpectations(t)
			defer jobLineageService.AssertExpectations(t)

			handler := v1beta1.NewJobRunHandler(logger, jobRunService, nil, nil, jobLineageService, nil, nil, nil)

			scheduledAt := timestamppb.Now()
			req := &pb.GetJobRunLineageSummaryRequest{
				TargetJobs: []*pb.TargetJobRunIdentifier{
					{
						JobName:     "test-job",
						ScheduledAt: scheduledAt,
					},
				},
				NumberOfUpstreamPerLevel: 5,
			}

			mockLineages := []*scheduler.JobRunLineage{
				{
					JobName:     "test-job",
					ScheduledAt: scheduledAt.AsTime(),
					JobRuns: []*scheduler.JobExecutionSummary{
						{
							JobName: "test-job",
							JobRunSummary: &scheduler.JobRunSummary{
								JobName:     "test-job",
								ScheduledAt: scheduledAt.AsTime(),
							},
						},
					},
					ExecutionSummary: &scheduler.LineageExecutionSummary{},
				},
			}

			jobLineageService.On("GetJobExecutionSummary", ctx, mock.Anything, 5).
				Return(mockLineages, nil)

			resp, err := handler.GetJobRunLineageSummary(ctx, req)
			assert.Nil(t, err)
			assert.NotNil(t, resp)
			assert.Equal(t, len(mockLineages), len(resp.Jobs))
		})
	})

	t.Run("GenerateEstimatedFinishTime", func(t *testing.T) {
		t.Run("should return error when estimator service error", func(t *testing.T) {
			jobRunService := new(mockJobRunService)
			defer jobRunService.AssertExpectations(t)

			jobEstimatorService := NewJobEstimatorService(t)
			defer jobEstimatorService.AssertExpectations(t)

			handler := v1beta1.NewJobRunHandler(logger, jobRunService, nil, nil, nil, nil, nil, jobEstimatorService)

			req := &pb.GenerateEstimatedFinishTimeRequest{
				ProjectName:           projectName,
				JobNames:              []string{"job-A"},
				ScheduledRangeInHours: 12,
			}

			jobEstimatorService.On("GenerateEstimatedFinishTimes", ctx, tenant.ProjectName(projectName), []scheduler.JobName{"job-A"}, mock.Anything, mock.Anything, 12*time.Hour).Return(nil, errors.New("service error"))
			resp, err := handler.GenerateEstimatedFinishTime(ctx, req)
			assert.NotNil(t, err)
			assert.Nil(t, resp)
			assert.ErrorContains(t, err, "unable to generate estimated finish times")
		})
		t.Run("should return estimated finish time successfully", func(t *testing.T) {
			jobRunService := new(mockJobRunService)
			defer jobRunService.AssertExpectations(t)

			jobEstimatorService := NewJobEstimatorService(t)
			defer jobEstimatorService.AssertExpectations(t)

			handler := v1beta1.NewJobRunHandler(logger, jobRunService, nil, nil, nil, nil, nil, jobEstimatorService)

			req := &pb.GenerateEstimatedFinishTimeRequest{
				ProjectName:           projectName,
				JobNames:              []string{"job-A"},
				ScheduledRangeInHours: 12,
			}

			expectedFinishTime := timestamppb.New(time.Now().Add(30 * time.Minute))

			jobEstimatorService.On("GenerateEstimatedFinishTimes", ctx, tenant.ProjectName(projectName), []scheduler.JobName{"job-A"}, mock.Anything, mock.Anything, 12*time.Hour).Return(map[scheduler.JobSchedule]time.Time{
				{
					JobName:     "job-A",
					ScheduledAt: time.Now(),
				}: expectedFinishTime.AsTime(),
			}, nil)
			resp, err := handler.GenerateEstimatedFinishTime(ctx, req)
			assert.Nil(t, err)
			assert.NotNil(t, resp)
			assert.Equal(t, 1, len(resp.Jobs))
		})
	})
}

type mockThirdPartyClient struct {
	mock.Mock
}

func (m *mockThirdPartyClient) IsManaged(ctx context.Context, resourceURN resource.URN) (bool, error) {
	args := m.Called(ctx, resourceURN)
	if args.Get(0) == nil {
		return false, args.Error(1)
	}
	return args.Get(0).(bool), args.Error(1)
}

func (m *mockThirdPartyClient) IsComplete(ctx context.Context, resourceURN resource.URN, dateFrom, dateTo time.Time) (bool, interface{}, error) {
	args := m.Called(ctx, resourceURN, dateFrom, dateTo)
	if args.Get(0) == nil {
		return false, args.Get(1), args.Error(2)
	}
	return args.Get(0).(bool), args.Get(1), args.Error(2)
}

type mockThirdPartySensorService struct {
	mock.Mock
}

func (m *mockThirdPartySensorService) GetClient(upstreamResolverType config.UpstreamResolverType) (service.ThirdPartyClient, error) {
	args := m.Called(upstreamResolverType)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(service.ThirdPartyClient), args.Error(1)
}

type mockJobLineageService struct {
	mock.Mock
}

func (m *mockJobLineageService) GetJobExecutionSummary(ctx context.Context, jobSchedules []*scheduler.JobSchedule, numberOfUpstreamPerLevel int) ([]*scheduler.JobRunLineage, error) {
	args := m.Called(ctx, jobSchedules, numberOfUpstreamPerLevel)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]*scheduler.JobRunLineage), args.Error(1)
}

type mockJobRunService struct {
	mock.Mock
}

func (m *mockJobRunService) JobRunInput(ctx context.Context, projectName tenant.ProjectName, jobName scheduler.JobName, config scheduler.RunConfig) (*scheduler.ExecutorInput, error) {
	args := m.Called(ctx, projectName, jobName, config)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*scheduler.ExecutorInput), args.Error(1)
}

func (m *mockJobRunService) UpdateJobState(ctx context.Context, event *scheduler.Event) error {
	args := m.Called(ctx, event)
	return args.Error(0)
}

func (m *mockJobRunService) UploadToScheduler(ctx context.Context, projectName tenant.ProjectName) error {
	args := m.Called(ctx, projectName)
	return args.Error(0)
}

func (m *mockJobRunService) GetJobRunsByFilter(ctx context.Context, projectName tenant.ProjectName, jobName scheduler.JobName, filters ...filter.FilterOpt) ([]*scheduler.JobRun, error) {
	args := m.Called(ctx, projectName, jobName, filters)
	return args.Get(0).([]*scheduler.JobRun), args.Error(1)
}

func (m *mockJobRunService) GetJobRuns(ctx context.Context, projectName tenant.ProjectName, jobName scheduler.JobName, criteria *scheduler.JobRunsCriteria) ([]*scheduler.JobRunStatus, string, error) {
	args := m.Called(ctx, projectName, jobName, criteria)
	if args.Get(0) == nil {
		return nil, "", args.Error(2)
	}
	return args.Get(0).([]*scheduler.JobRunStatus), "", args.Error(2)
}

func (m *mockJobRunService) GetInterval(ctx context.Context, projectName tenant.ProjectName, jobName scheduler.JobName, referenceTime time.Time) (interval.Interval, error) {
	args := m.Called(ctx, projectName, jobName, referenceTime)
	if args.Get(0) == nil {
		return interval.Interval{}, args.Error(1)
	}
	return args.Get(0).(interval.Interval), args.Error(1)
}

type mockNotifier struct {
	mock.Mock
}

func (m *mockNotifier) Push(ctx context.Context, event *scheduler.Event) error {
	args := m.Called(ctx, event)
	return args.Error(0)
}

func (m *mockNotifier) Webhook(ctx context.Context, event *scheduler.Event) error {
	args := m.Called(ctx, event)
	return args.Error(0)
}

func (m *mockNotifier) Relay(ctx context.Context, event *scheduler.Event) error {
	args := m.Called(ctx, event)
	return args.Error(0)
}

// JobEstimatorService is an autogenerated mock type for the JobEstimatorService type
type JobEstimatorService struct {
	mock.Mock
}

// GenerateEstimatedFinishTimes provides a mock function with given fields: ctx, projectName, jobNames, labels, referenceTime, scheduleRangeInHours
func (_m *JobEstimatorService) GenerateEstimatedFinishTimes(ctx context.Context, projectName tenant.ProjectName, jobNames []scheduler.JobName, labels map[string]string, referenceTime time.Time, scheduleRangeInHours time.Duration) (map[scheduler.JobSchedule]time.Time, error) {
	ret := _m.Called(ctx, projectName, jobNames, labels, referenceTime, scheduleRangeInHours)

	if len(ret) == 0 {
		panic("no return value specified for GenerateEstimatedFinishTimes")
	}

	var r0 map[scheduler.JobSchedule]time.Time
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context, tenant.ProjectName, []scheduler.JobName, map[string]string, time.Time, time.Duration) (map[scheduler.JobSchedule]time.Time, error)); ok {
		return rf(ctx, projectName, jobNames, labels, referenceTime, scheduleRangeInHours)
	}
	if rf, ok := ret.Get(0).(func(context.Context, tenant.ProjectName, []scheduler.JobName, map[string]string, time.Time, time.Duration) map[scheduler.JobSchedule]time.Time); ok {
		r0 = rf(ctx, projectName, jobNames, labels, referenceTime, scheduleRangeInHours)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(map[scheduler.JobSchedule]time.Time)
		}
	}

	if rf, ok := ret.Get(1).(func(context.Context, tenant.ProjectName, []scheduler.JobName, map[string]string, time.Time, time.Duration) error); ok {
		r1 = rf(ctx, projectName, jobNames, labels, referenceTime, scheduleRangeInHours)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// NewJobEstimatorService creates a new instance of JobEstimatorService. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewJobEstimatorService(t interface {
	mock.TestingT
	Cleanup(func())
},
) *JobEstimatorService {
	mock := &JobEstimatorService{}
	mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
