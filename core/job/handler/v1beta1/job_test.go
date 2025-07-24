package v1beta1_test

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/goto/salt/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"google.golang.org/grpc/metadata"

	"github.com/goto/optimus/core/job"
	"github.com/goto/optimus/core/job/dto"
	"github.com/goto/optimus/core/job/handler/v1beta1"
	"github.com/goto/optimus/core/resource"
	"github.com/goto/optimus/core/tenant"
	"github.com/goto/optimus/internal/lib/window"
	"github.com/goto/optimus/internal/utils/filter"
	"github.com/goto/optimus/internal/writer"
	"github.com/goto/optimus/plugin"
	pb "github.com/goto/optimus/protos/gotocompany/optimus/core/v1beta1"
)

func TestNewJobHandler(t *testing.T) {
	ctx := context.Background()
	project, _ := tenant.NewProject("test-proj",
		map[string]string{
			"bucket":                     "gs://some_folder-2",
			tenant.ProjectSchedulerHost:  "host",
			tenant.ProjectStoragePathKey: "gs://location",
		}, map[string]string{}) // TODO: add test for presets
	namespace, _ := tenant.NewNamespace("test-ns", project.Name(),
		map[string]string{
			"bucket": "gs://ns_bucket",
		}, map[string]string{})
	sampleTenant, _ := tenant.NewTenant(project.Name().String(), namespace.Name().String())
	jobVersion := 1
	startDate, err := job.ScheduleDateFrom("2022-10-01")
	assert.NoError(t, err)
	jobSchedule, err := job.NewScheduleBuilder(startDate).Build()
	assert.NoError(t, err)
	w, err := window.NewConfig("1d", "1d", "", "")
	assert.NoError(t, err)
	jobConfig, err := job.ConfigFrom(map[string]string{"sample_key": "sample_value"})
	assert.NoError(t, err)
	jobTask := job.NewTask("bq2bq", jobConfig, "")
	jobBehavior := &pb.JobSpecification_Behavior{
		Retry: &pb.JobSpecification_Behavior_Retry{ExponentialBackoff: false},
		Notify: []*pb.JobSpecification_Behavior_Notifiers{
			{On: 0, Channels: []string{"sample"}},
		},
	}
	jobDependencies := []*pb.JobDependency{
		{Name: "job-B", Type: "static"},
	}
	jobMetadata := &pb.JobMetadata{
		Resource: &pb.JobSpecMetadataResource{
			Request: &pb.JobSpecMetadataResourceConfig{Cpu: "1", Memory: "8"},
			Limit:   &pb.JobSpecMetadataResourceConfig{Cpu: ".5", Memory: "4"},
		},
		Airflow: &pb.JobSpecMetadataAirflow{Pool: "100", Queue: "50"},
	}

	resourceRequestConfig := job.NewMetadataResourceConfig("1", "8")
	resourceLimitConfig := job.NewMetadataResourceConfig(".5", "4")
	resourceMetadata := job.NewResourceMetadata(resourceRequestConfig, resourceLimitConfig)
	metadataSpec, _ := job.NewMetadataBuilder().
		WithResource(resourceMetadata).
		WithScheduler(map[string]string{"pool": "100", "queue": "50"}).
		Build()

	log := log.NewNoop()
	sampleOwner := "sample-owner"

	resourceURNA, err := resource.ParseURN("store://table-A")
	assert.NoError(t, err)
	resourceURNB, err := resource.ParseURN("store://table-B")
	assert.NoError(t, err)
	resourceURNC, err := resource.ParseURN("store://table-C")
	assert.NoError(t, err)
	resourceURND, err := resource.ParseURN("store://table-D")
	assert.NoError(t, err)

	t.Run("AddJobSpecifications", func(t *testing.T) {
		t.Run("adds job", func(t *testing.T) {
			jobService := new(JobService)
			changeLogService := new(ChangeLogService)

			jobHandler := v1beta1.NewJobHandler(jobService, changeLogService, log)

			jobSpecProto := &pb.JobSpecification{
				Version:   int32(jobVersion),
				Name:      "job-A",
				Owner:     sampleOwner,
				StartDate: jobSchedule.StartDate().String(),
				EndDate:   jobSchedule.EndDate().String(),
				Interval:  jobSchedule.Interval(),
				Task: &pb.JobSpecTask{
					Name:  jobTask.Name().String(),Version: jobTask.Version(),
				},
				Window: &pb.JobSpecification_Window{
					Size:       w.GetSimpleConfig().Size,
					ShiftBy:    w.GetSimpleConfig().ShiftBy,
					TruncateTo: w.GetSimpleConfig().TruncateTo,
				},
			}
			jobProtos := []*pb.JobSpecification{jobSpecProto}
			request := pb.AddJobSpecificationsRequest{
				ProjectName:   project.Name().String(),
				NamespaceName: namespace.Name().String(),
				Specs:         jobProtos,
			}
			jobSuccesses := []job.Name{job.Name(jobSpecProto.Name)}

			jobService.On("Add", ctx, sampleTenant, mock.Anything).Return(jobSuccesses, nil)

			resp, err := jobHandler.AddJobSpecifications(ctx, &request)
			assert.Nil(t, err)
			assert.Equal(t, &pb.AddJobSpecificationsResponse{
				Log:                "jobs are successfully created",
				SuccessfulJobNames: []string{jobSpecProto.Name},
			}, resp)
		})
		t.Run("adds complete job", func(t *testing.T) {
			jobService := new(JobService)
			changeLogService := new(ChangeLogService)

			jobHandler := v1beta1.NewJobHandler(jobService, changeLogService, log)

			jobSpecProto := &pb.JobSpecification{
				Version:   int32(jobVersion),
				Name:      "job-A",
				Owner:     sampleOwner,
				StartDate: jobSchedule.StartDate().String(),
				EndDate:   jobSchedule.EndDate().String(),
				Interval:  jobSchedule.Interval(),
				Task: &pb.JobSpecTask{
					Name:  jobTask.Name().String(),Version: jobTask.Version(),
				},
				Window: &pb.JobSpecification_Window{
					Size:       w.GetSimpleConfig().Size,
					ShiftBy:    w.GetSimpleConfig().ShiftBy,
					TruncateTo: w.GetSimpleConfig().TruncateTo,
				},
				Behavior:     jobBehavior,
				Dependencies: jobDependencies,
				Metadata:     jobMetadata,
			}
			jobProtos := []*pb.JobSpecification{jobSpecProto}
			request := pb.AddJobSpecificationsRequest{
				ProjectName:   project.Name().String(),
				NamespaceName: namespace.Name().String(),
				Specs:         jobProtos,
			}
			jobSuccesses := []job.Name{job.Name(jobSpecProto.Name)}

			jobService.On("Add", ctx, sampleTenant, mock.Anything).Return(jobSuccesses, nil)

			resp, err := jobHandler.AddJobSpecifications(ctx, &request)
			assert.Nil(t, err)
			assert.Equal(t, &pb.AddJobSpecificationsResponse{
				Log:                "jobs are successfully created",
				SuccessfulJobNames: []string{jobSpecProto.Name},
			}, resp)
		})
		t.Run("returns error when unable to create tenant", func(t *testing.T) {
			jobService := new(JobService)
			changeLogService := new(ChangeLogService)

			jobHandler := v1beta1.NewJobHandler(jobService, changeLogService, log)

			request := pb.AddJobSpecificationsRequest{
				NamespaceName: namespace.Name().String(),
			}

			resp, err := jobHandler.AddJobSpecifications(ctx, &request)
			assert.NotNil(t, err)
			assert.Nil(t, resp)
		})
		t.Run("skips job if unable to parse from proto", func(t *testing.T) {
			t.Run("due to empty owner", func(t *testing.T) {
				jobService := new(JobService)
				changeLogService := new(ChangeLogService)

				jobHandler := v1beta1.NewJobHandler(jobService, changeLogService, log)

				jobSpecProtos := []*pb.JobSpecification{
					{
						Version:   int32(0),
						Name:      "job-A",
						StartDate: jobSchedule.StartDate().String(),
						EndDate:   jobSchedule.EndDate().String(),
						Interval:  jobSchedule.Interval(),
						Task: &pb.JobSpecTask{
							Name:  jobTask.Name().String(),Version: jobTask.Version(),
						},
						Window: &pb.JobSpecification_Window{
							Size:       w.GetSimpleConfig().Size,
							ShiftBy:    w.GetSimpleConfig().ShiftBy,
							TruncateTo: w.GetSimpleConfig().TruncateTo,
						},
					},
					{
						Version:   int32(jobVersion),
						Name:      "job-B",
						Owner:     sampleOwner,
						StartDate: jobSchedule.StartDate().String(),
						EndDate:   jobSchedule.EndDate().String(),
						Interval:  jobSchedule.Interval(),
						Task: &pb.JobSpecTask{
							Name:  jobTask.Name().String(),},
						Window: &pb.JobSpecification_Window{
							Size:       w.GetSimpleConfig().Size,
							ShiftBy:    w.GetSimpleConfig().ShiftBy,
							TruncateTo: w.GetSimpleConfig().TruncateTo,
						},
					},
				}
				request := pb.AddJobSpecificationsRequest{
					ProjectName:   project.Name().String(),
					NamespaceName: namespace.Name().String(),
					Specs:         jobSpecProtos,
				}

				jobSuccesses := []job.Name{
					job.Name(jobSpecProtos[0].Name),
					job.Name(jobSpecProtos[1].Name),
				}

				jobService.On("Add", ctx, sampleTenant, mock.Anything).Return(jobSuccesses, nil)

				resp, err := jobHandler.AddJobSpecifications(ctx, &request)
				assert.Nil(t, err)
				assert.Contains(t, resp.Log, "error")
			})
			t.Run("due to invalid start date", func(t *testing.T) {
				jobService := new(JobService)
				changeLogService := new(ChangeLogService)

				jobHandler := v1beta1.NewJobHandler(jobService, changeLogService, log)

				jobSpecProtos := []*pb.JobSpecification{
					{
						Version:   int32(jobVersion),
						Name:      "job-A",
						StartDate: "invalid",
						Owner:     sampleOwner,
						EndDate:   jobSchedule.EndDate().String(),
						Interval:  jobSchedule.Interval(),
						Task: &pb.JobSpecTask{
							Name:  jobTask.Name().String(),},
						Window: &pb.JobSpecification_Window{
							Size:       w.GetSimpleConfig().Size,
							ShiftBy:    w.GetSimpleConfig().ShiftBy,
							TruncateTo: w.GetSimpleConfig().TruncateTo,
						},
					},
					{
						Version:   int32(jobVersion),
						Name:      "job-B",
						Owner:     sampleOwner,
						StartDate: jobSchedule.StartDate().String(),
						EndDate:   jobSchedule.EndDate().String(),
						Interval:  jobSchedule.Interval(),
						Task: &pb.JobSpecTask{
							Name:  jobTask.Name().String(),},
						Window: &pb.JobSpecification_Window{
							Size:       w.GetSimpleConfig().Size,
							ShiftBy:    w.GetSimpleConfig().ShiftBy,
							TruncateTo: w.GetSimpleConfig().TruncateTo,
						},
					},
				}
				request := pb.AddJobSpecificationsRequest{
					ProjectName:   project.Name().String(),
					NamespaceName: namespace.Name().String(),
					Specs:         jobSpecProtos,
				}
				jobSuccesses := []job.Name{
					job.Name(jobSpecProtos[0].Name),
					job.Name(jobSpecProtos[1].Name),
				}

				jobService.On("Add", ctx, sampleTenant, mock.Anything).Return(jobSuccesses, nil)

				resp, err := jobHandler.AddJobSpecifications(ctx, &request)
				assert.Nil(t, err)
				assert.Contains(t, resp.Log, "error")
			})
			t.Run("due to invalid end date", func(t *testing.T) {
				jobService := new(JobService)
				changeLogService := new(ChangeLogService)

				jobHandler := v1beta1.NewJobHandler(jobService, changeLogService, log)

				jobSpecProtos := []*pb.JobSpecification{
					{
						Version:   int32(jobVersion),
						Name:      "job-A",
						StartDate: jobSchedule.StartDate().String(),
						EndDate:   "invalid",
						Owner:     sampleOwner,
						Interval:  jobSchedule.Interval(),
						Task: &pb.JobSpecTask{
							Name:  jobTask.Name().String(),},
						Window: &pb.JobSpecification_Window{
							Size:       w.GetSimpleConfig().Size,
							ShiftBy:    w.GetSimpleConfig().ShiftBy,
							TruncateTo: w.GetSimpleConfig().TruncateTo,
						},
					},
					{
						Version:   int32(jobVersion),
						Name:      "job-B",
						Owner:     sampleOwner,
						StartDate: jobSchedule.StartDate().String(),
						EndDate:   jobSchedule.EndDate().String(),
						Interval:  jobSchedule.Interval(),
						Task: &pb.JobSpecTask{
							Name:  jobTask.Name().String(),},
						Window: &pb.JobSpecification_Window{
							Size:       w.GetSimpleConfig().Size,
							ShiftBy:    w.GetSimpleConfig().ShiftBy,
							TruncateTo: w.GetSimpleConfig().TruncateTo,
						},
					},
				}
				request := pb.AddJobSpecificationsRequest{
					ProjectName:   project.Name().String(),
					NamespaceName: namespace.Name().String(),
					Specs:         jobSpecProtos,
				}
				jobSuccesses := []job.Name{
					job.Name(jobSpecProtos[0].Name),
					job.Name(jobSpecProtos[1].Name),
				}

				jobService.On("Add", ctx, sampleTenant, mock.Anything).Return(jobSuccesses, nil)

				resp, err := jobHandler.AddJobSpecifications(ctx, &request)
				assert.Nil(t, err)
				assert.Contains(t, resp.Log, "error")
			})
			t.Run("due to invalid alert configuration", func(t *testing.T) {
				jobService := new(JobService)
				changeLogService := new(ChangeLogService)

				jobHandler := v1beta1.NewJobHandler(jobService, changeLogService, log)

				behaviorWithInvalidAlertConf := &pb.JobSpecification_Behavior{
					Retry: &pb.JobSpecification_Behavior_Retry{ExponentialBackoff: false},
					Notify: []*pb.JobSpecification_Behavior_Notifiers{
						{On: 0, Channels: []string{"sample"}, Config: map[string]string{"": ""}},
					},
				}

				jobSpecProtos := []*pb.JobSpecification{
					{
						Version:   int32(jobVersion),
						Name:      "job-A",
						Owner:     sampleOwner,
						StartDate: jobSchedule.StartDate().String(),
						EndDate:   jobSchedule.EndDate().String(),
						Interval:  jobSchedule.Interval(),
						Task: &pb.JobSpecTask{
							Name:  jobTask.Name().String(),},
						Window: &pb.JobSpecification_Window{
							Size:       w.GetSimpleConfig().Size,
							ShiftBy:    w.GetSimpleConfig().ShiftBy,
							TruncateTo: w.GetSimpleConfig().TruncateTo,
						},
						Behavior: behaviorWithInvalidAlertConf,
					},
					{
						Version:   int32(jobVersion),
						Name:      "job-B",
						Owner:     sampleOwner,
						StartDate: jobSchedule.StartDate().String(),
						EndDate:   jobSchedule.EndDate().String(),
						Interval:  jobSchedule.Interval(),
						Task: &pb.JobSpecTask{
							Name:  jobTask.Name().String(),},
						Window: &pb.JobSpecification_Window{
							Size:       w.GetSimpleConfig().Size,
							ShiftBy:    w.GetSimpleConfig().ShiftBy,
							TruncateTo: w.GetSimpleConfig().TruncateTo,
						},
					},
				}
				request := pb.AddJobSpecificationsRequest{
					ProjectName:   project.Name().String(),
					NamespaceName: namespace.Name().String(),
					Specs:         jobSpecProtos,
				}
				jobSuccesses := []job.Name{
					job.Name(jobSpecProtos[0].Name),
					job.Name(jobSpecProtos[1].Name),
				}

				jobService.On("Add", ctx, sampleTenant, mock.Anything).Return(jobSuccesses, nil)

				resp, err := jobHandler.AddJobSpecifications(ctx, &request)
				assert.Nil(t, err)
				assert.Contains(t, resp.Log, "error")
			})
		})
		t.Run("returns error when all jobs failed to be added", func(t *testing.T) {
			jobService := new(JobService)
			changeLogService := new(ChangeLogService)

			jobHandler := v1beta1.NewJobHandler(jobService, changeLogService, log)

			jobSpecProtos := []*pb.JobSpecification{
				{
					Version:   int32(0),
					Name:      "job-A",
					StartDate: jobSchedule.StartDate().String(),
					EndDate:   jobSchedule.EndDate().String(),
					Interval:  jobSchedule.Interval(),
					Task: &pb.JobSpecTask{
						Name:  jobTask.Name().String(),},
					Window: &pb.JobSpecification_Window{
						Size:       w.GetSimpleConfig().Size,
						ShiftBy:    w.GetSimpleConfig().ShiftBy,
						TruncateTo: w.GetSimpleConfig().TruncateTo,
					},
				},
			}
			request := pb.AddJobSpecificationsRequest{
				ProjectName:   project.Name().String(),
				NamespaceName: namespace.Name().String(),
				Specs:         jobSpecProtos,
			}

			resp, err := jobHandler.AddJobSpecifications(ctx, &request)
			assert.ErrorContains(t, err, "no jobs to be processed")
			assert.Nil(t, resp)
		})
		t.Run("returns response with job errors log when some jobs failed to be added", func(t *testing.T) {
			jobService := new(JobService)
			changeLogService := new(ChangeLogService)

			jobHandler := v1beta1.NewJobHandler(jobService, changeLogService, log)

			jobSpecProtos := []*pb.JobSpecification{
				{
					Version:   int32(jobVersion),
					Name:      "job-A",
					Owner:     sampleOwner,
					StartDate: jobSchedule.StartDate().String(),
					EndDate:   jobSchedule.EndDate().String(),
					Interval:  jobSchedule.Interval(),
					Task: &pb.JobSpecTask{
						Name:  jobTask.Name().String(),},
					Window: &pb.JobSpecification_Window{
						Size:       w.GetSimpleConfig().Size,
						ShiftBy:    w.GetSimpleConfig().ShiftBy,
						TruncateTo: w.GetSimpleConfig().TruncateTo,
					},
				},
				{
					Version:   int32(jobVersion),
					Name:      "job-B",
					StartDate: jobSchedule.StartDate().String(),
					EndDate:   jobSchedule.EndDate().String(),
					Interval:  jobSchedule.Interval(),
					Task: &pb.JobSpecTask{
						Name:  jobTask.Name().String(),},
					Window: &pb.JobSpecification_Window{
						Size:       w.GetSimpleConfig().Size,
						ShiftBy:    w.GetSimpleConfig().ShiftBy,
						TruncateTo: w.GetSimpleConfig().TruncateTo,
					},
				},
			}
			request := pb.AddJobSpecificationsRequest{
				ProjectName:   project.Name().String(),
				NamespaceName: namespace.Name().String(),
				Specs:         jobSpecProtos,
			}

			jobService.On("Add", ctx, sampleTenant, mock.Anything).Return(nil, errors.New("internal error"))

			resp, err := jobHandler.AddJobSpecifications(ctx, &request)
			assert.Nil(t, err)
			assert.Contains(t, resp.Log, "error")
		})
	})
	t.Run("UpdateJobSpecifications", func(t *testing.T) {
		t.Run("update jobs", func(t *testing.T) {
			jobService := new(JobService)
			changeLogService := new(ChangeLogService)

			jobHandler := v1beta1.NewJobHandler(jobService, changeLogService, log)

			jobSpecProto := &pb.JobSpecification{
				Version:   int32(jobVersion),
				Name:      "job-A",
				Owner:     sampleOwner,
				StartDate: jobSchedule.StartDate().String(),
				EndDate:   jobSchedule.EndDate().String(),
				Interval:  jobSchedule.Interval(),
				Task: &pb.JobSpecTask{
					Name:  jobTask.Name().String(),},
				Window: &pb.JobSpecification_Window{
					Size:       w.GetSimpleConfig().Size,
					ShiftBy:    w.GetSimpleConfig().ShiftBy,
					TruncateTo: w.GetSimpleConfig().TruncateTo,
				},
			}
			jobProtos := []*pb.JobSpecification{jobSpecProto}
			request := pb.UpdateJobSpecificationsRequest{
				ProjectName:   project.Name().String(),
				NamespaceName: namespace.Name().String(),
				Specs:         jobProtos,
			}

			jobService.On("Update", ctx, sampleTenant, mock.Anything).Return([]job.Name{"job-A"}, nil)

			resp, err := jobHandler.UpdateJobSpecifications(ctx, &request)
			assert.Nil(t, err)
			assert.Equal(t, &pb.UpdateJobSpecificationsResponse{
				Log:                "jobs are successfully updated",
				SuccessfulJobNames: []string{"job-A"},
			}, resp)
		})
		t.Run("update complete jobs", func(t *testing.T) {
			jobService := new(JobService)
			changeLogService := new(ChangeLogService)

			jobHandler := v1beta1.NewJobHandler(jobService, changeLogService, log)

			jobSpecProto := &pb.JobSpecification{
				Version:      int32(jobVersion),
				Name:         "job-A",
				Owner:        sampleOwner,
				StartDate:    jobSchedule.StartDate().String(),
				EndDate:      jobSchedule.EndDate().String(),
				Interval:     jobSchedule.Interval(),
				Task: &pb.JobSpecTask{
					Name:     jobTask.Name().String(),},
				Behavior:     jobBehavior,
				Dependencies: jobDependencies,
				Metadata:     jobMetadata,
				Window: &pb.JobSpecification_Window{
					Size:       w.GetSimpleConfig().Size,
					ShiftBy:    w.GetSimpleConfig().ShiftBy,
					TruncateTo: w.GetSimpleConfig().TruncateTo,
				},
			}
			jobProtos := []*pb.JobSpecification{jobSpecProto}
			request := pb.UpdateJobSpecificationsRequest{
				ProjectName:   project.Name().String(),
				NamespaceName: namespace.Name().String(),
				Specs:         jobProtos,
			}

			jobService.On("Update", ctx, sampleTenant, mock.Anything).Return([]job.Name{"job-A"}, nil)

			resp, err := jobHandler.UpdateJobSpecifications(ctx, &request)
			assert.Nil(t, err)
			assert.Equal(t, &pb.UpdateJobSpecificationsResponse{
				Log:                "jobs are successfully updated",
				SuccessfulJobNames: []string{"job-A"},
			}, resp)
		})
		t.Run("returns error when unable to create tenant", func(t *testing.T) {
			jobService := new(JobService)
			changeLogService := new(ChangeLogService)

			jobHandler := v1beta1.NewJobHandler(jobService, changeLogService, log)

			request := pb.UpdateJobSpecificationsRequest{
				NamespaceName: namespace.Name().String(),
			}

			resp, err := jobHandler.UpdateJobSpecifications(ctx, &request)
			assert.NotNil(t, err)
			assert.Nil(t, resp)
		})
		t.Run("skips job if unable to parse from proto", func(t *testing.T) {
			jobService := new(JobService)
			changeLogService := new(ChangeLogService)

			jobHandler := v1beta1.NewJobHandler(jobService, changeLogService, log)

			jobSpecProtos := []*pb.JobSpecification{
				{
					Version:   int32(0),
					Name:      "job-A",
					StartDate: jobSchedule.StartDate().String(),
					EndDate:   jobSchedule.EndDate().String(),
					Interval:  jobSchedule.Interval(),
					Task: &pb.JobSpecTask{
						Name:  jobTask.Name().String(),},
					Window: &pb.JobSpecification_Window{
						Size:       w.GetSimpleConfig().Size,
						ShiftBy:    w.GetSimpleConfig().ShiftBy,
						TruncateTo: w.GetSimpleConfig().TruncateTo,
					},
				},
				{
					Version:   int32(jobVersion),
					Name:      "job-B",
					Owner:     sampleOwner,
					StartDate: jobSchedule.StartDate().String(),
					EndDate:   jobSchedule.EndDate().String(),
					Interval:  jobSchedule.Interval(),
					Task: &pb.JobSpecTask{
						Name:  jobTask.Name().String(),},
					Window: &pb.JobSpecification_Window{
						Size:       w.GetSimpleConfig().Size,
						ShiftBy:    w.GetSimpleConfig().ShiftBy,
						TruncateTo: w.GetSimpleConfig().TruncateTo,
					},
				},
			}
			request := pb.UpdateJobSpecificationsRequest{
				ProjectName:   project.Name().String(),
				NamespaceName: namespace.Name().String(),
				Specs:         jobSpecProtos,
			}

			jobService.On("Update", ctx, sampleTenant, mock.Anything).Return([]job.Name{"job-B"}, nil)

			resp, err := jobHandler.UpdateJobSpecifications(ctx, &request)
			assert.Nil(t, err)
			assert.Contains(t, resp.Log, "error")
		})
		t.Run("returns error when all jobs failed to be updated", func(t *testing.T) {
			jobService := new(JobService)
			changeLogService := new(ChangeLogService)

			jobHandler := v1beta1.NewJobHandler(jobService, changeLogService, log)

			jobSpecProtos := []*pb.JobSpecification{
				{
					Version:   int32(0),
					Name:      "job-A",
					StartDate: jobSchedule.StartDate().String(),
					EndDate:   jobSchedule.EndDate().String(),
					Interval:  jobSchedule.Interval(),
					Task: &pb.JobSpecTask{
						Name:  jobTask.Name().String(),},
					Window: &pb.JobSpecification_Window{
						Size:       w.GetSimpleConfig().Size,
						ShiftBy:    w.GetSimpleConfig().ShiftBy,
						TruncateTo: w.GetSimpleConfig().TruncateTo,
					},
				},
			}
			request := pb.UpdateJobSpecificationsRequest{
				ProjectName:   project.Name().String(),
				NamespaceName: namespace.Name().String(),
				Specs:         jobSpecProtos,
			}

			jobService.On("Update", ctx, sampleTenant, mock.Anything).Return([]job.Name{}, errors.New("internal error"))

			resp, err := jobHandler.UpdateJobSpecifications(ctx, &request)
			assert.ErrorContains(t, err, "no jobs to be processed")
			assert.Nil(t, resp)
		})
		t.Run("returns response with job errors log when some jobs failed to be updated", func(t *testing.T) {
			jobService := new(JobService)
			changeLogService := new(ChangeLogService)

			jobHandler := v1beta1.NewJobHandler(jobService, changeLogService, log)

			jobSpecProtos := []*pb.JobSpecification{
				{
					Version:   int32(jobVersion),
					Name:      "job-A",
					Owner:     sampleOwner,
					StartDate: jobSchedule.StartDate().String(),
					EndDate:   jobSchedule.EndDate().String(),
					Interval:  jobSchedule.Interval(),
					Task: &pb.JobSpecTask{
						Name:  jobTask.Name().String(),},
					Window: &pb.JobSpecification_Window{
						Size:       w.GetSimpleConfig().Size,
						ShiftBy:    w.GetSimpleConfig().ShiftBy,
						TruncateTo: w.GetSimpleConfig().TruncateTo,
					},
				},
				{
					Version:   int32(jobVersion),
					Name:      "job-B",
					StartDate: jobSchedule.StartDate().String(),
					EndDate:   jobSchedule.EndDate().String(),
					Interval:  jobSchedule.Interval(),
					Task: &pb.JobSpecTask{
						Name:  jobTask.Name().String(),},
					Window: &pb.JobSpecification_Window{
						Size:       w.GetSimpleConfig().Size,
						ShiftBy:    w.GetSimpleConfig().ShiftBy,
						TruncateTo: w.GetSimpleConfig().TruncateTo,
					},
				},
			}
			request := pb.UpdateJobSpecificationsRequest{
				ProjectName:   project.Name().String(),
				NamespaceName: namespace.Name().String(),
				Specs:         jobSpecProtos,
			}

			jobService.On("Update", ctx, sampleTenant, mock.Anything).Return([]job.Name{}, errors.New("internal error"))

			resp, err := jobHandler.UpdateJobSpecifications(ctx, &request)
			assert.Nil(t, err)
			assert.Contains(t, resp.Log, "error")
		})
	})
	t.Run("UpsertJobSpecifications", func(t *testing.T) {
		t.Run("upsert jobs", func(t *testing.T) {
			jobService := new(JobService)
			changeLogService := new(ChangeLogService)

			jobHandler := v1beta1.NewJobHandler(jobService, changeLogService, log)

			jobSpecProto := &pb.JobSpecification{
				Version:   int32(jobVersion),
				Name:      "job-A",
				Owner:     sampleOwner,
				StartDate: jobSchedule.StartDate().String(),
				EndDate:   jobSchedule.EndDate().String(),
				Interval:  jobSchedule.Interval(),
				Task: &pb.JobSpecTask{
					Name:  jobTask.Name().String(),},
				Window: &pb.JobSpecification_Window{
					Size:       w.GetSimpleConfig().Size,
					ShiftBy:    w.GetSimpleConfig().ShiftBy,
					TruncateTo: w.GetSimpleConfig().TruncateTo,
				},
			}
			jobProtos := []*pb.JobSpecification{jobSpecProto}
			request := pb.UpsertJobSpecificationsRequest{
				ProjectName:   project.Name().String(),
				NamespaceName: namespace.Name().String(),
				Specs:         jobProtos,
			}

			jobAName := job.Name("job-A")
			upsertResult := dto.UpsertResult{
				JobName: jobAName,
				Status:  job.DeployStateSuccess,
			}
			jobService.On("Upsert", ctx, sampleTenant, mock.Anything).Return([]dto.UpsertResult{upsertResult}, nil)

			resp, err := jobHandler.UpsertJobSpecifications(ctx, &request)
			assert.Nil(t, err)
			assert.Equal(t, &pb.UpsertJobSpecificationsResponse{
				SuccessfulJobNames: []string{jobAName.String()},
			}, resp)
		})
		t.Run("upsert a job with complete configuration", func(t *testing.T) {
			jobService := new(JobService)
			changeLogService := new(ChangeLogService)

			jobHandler := v1beta1.NewJobHandler(jobService, changeLogService, log)

			jobSpecProto := &pb.JobSpecification{
				Version:   int32(jobVersion),
				Name:      "job-A",
				Owner:     sampleOwner,
				StartDate: jobSchedule.StartDate().String(),
				EndDate:   jobSchedule.EndDate().String(),
				Interval:  jobSchedule.Interval(),
				Task: &pb.JobSpecTask{
					Name:  jobTask.Name().String(),},
				Window: &pb.JobSpecification_Window{
					Size:       w.GetSimpleConfig().Size,
					ShiftBy:    w.GetSimpleConfig().ShiftBy,
					TruncateTo: w.GetSimpleConfig().TruncateTo,
				},

				Behavior:     jobBehavior,
				Dependencies: jobDependencies,
				Metadata:     jobMetadata,
			}
			jobProtos := []*pb.JobSpecification{jobSpecProto}
			request := pb.UpsertJobSpecificationsRequest{
				ProjectName:   project.Name().String(),
				NamespaceName: namespace.Name().String(),
				Specs:         jobProtos,
			}

			jobAName := job.Name("job-A")
			upsertResult := dto.UpsertResult{
				JobName: jobAName,
				Status:  job.DeployStateSuccess,
			}

			jobService.On("Upsert", ctx, sampleTenant, mock.Anything).Return([]dto.UpsertResult{upsertResult}, nil)

			resp, err := jobHandler.UpsertJobSpecifications(ctx, &request)
			assert.Nil(t, err)
			assert.Equal(t, &pb.UpsertJobSpecificationsResponse{
				SuccessfulJobNames: []string{jobAName.String()},
			}, resp)
		})
		t.Run("returns error when unable to create tenant", func(t *testing.T) {
			jobService := new(JobService)
			changeLogService := new(ChangeLogService)

			jobHandler := v1beta1.NewJobHandler(jobService, changeLogService, log)

			request := pb.UpsertJobSpecificationsRequest{
				NamespaceName: namespace.Name().String(),
			}

			resp, err := jobHandler.UpsertJobSpecifications(ctx, &request)
			assert.NotNil(t, err)
			assert.Nil(t, resp)
		})
		t.Run("skips job if unable to parse from proto", func(t *testing.T) {
			jobService := new(JobService)
			changeLogService := new(ChangeLogService)

			jobHandler := v1beta1.NewJobHandler(jobService, changeLogService, log)

			jobAName := job.Name("job-A")
			jobBName := job.Name("job-B")

			jobSpecProtos := []*pb.JobSpecification{
				{
					Version:   int32(0),
					Name:      jobAName.String(),
					StartDate: jobSchedule.StartDate().String(),
					EndDate:   jobSchedule.EndDate().String(),
					Interval:  jobSchedule.Interval(),
					Task: &pb.JobSpecTask{
						Name:  jobTask.Name().String(),},
					Window: &pb.JobSpecification_Window{
						Size:       w.GetSimpleConfig().Size,
						ShiftBy:    w.GetSimpleConfig().ShiftBy,
						TruncateTo: w.GetSimpleConfig().TruncateTo,
					},
				},
				{
					Version:   int32(jobVersion),
					Name:      jobBName.String(),
					Owner:     sampleOwner,
					StartDate: jobSchedule.StartDate().String(),
					EndDate:   jobSchedule.EndDate().String(),
					Interval:  jobSchedule.Interval(),
					Task: &pb.JobSpecTask{
						Name:  jobTask.Name().String(),},
					Window: &pb.JobSpecification_Window{
						Size:       w.GetSimpleConfig().Size,
						ShiftBy:    w.GetSimpleConfig().ShiftBy,
						TruncateTo: w.GetSimpleConfig().TruncateTo,
					},
				},
			}
			request := pb.UpsertJobSpecificationsRequest{
				ProjectName:   project.Name().String(),
				NamespaceName: namespace.Name().String(),
				Specs:         jobSpecProtos,
			}

			upsertResult := dto.UpsertResult{
				JobName: jobBName,
				Status:  job.DeployStateSuccess,
			}
			jobService.On("Upsert", ctx, sampleTenant, mock.Anything).Return([]dto.UpsertResult{upsertResult}, nil)

			resp, err := jobHandler.UpsertJobSpecifications(ctx, &request)
			assert.Nil(t, err)
			assert.Contains(t, resp.Log, "error")
			assert.ElementsMatch(t, []string{jobAName.String()}, resp.FailedJobNames)
			assert.ElementsMatch(t, []string{jobBName.String()}, resp.SuccessfulJobNames)
		})
		t.Run("returns error when all jobs failed in upsert process", func(t *testing.T) {
			jobService := new(JobService)
			changeLogService := new(ChangeLogService)

			jobHandler := v1beta1.NewJobHandler(jobService, changeLogService, log)

			jobSpecProtos := []*pb.JobSpecification{
				{
					Version:   int32(0),
					Name:      "job-A",
					StartDate: jobSchedule.StartDate().String(),
					EndDate:   jobSchedule.EndDate().String(),
					Interval:  jobSchedule.Interval(),
					Task: &pb.JobSpecTask{
						Name:  jobTask.Name().String(),},
					Window: &pb.JobSpecification_Window{
						Size:       w.GetSimpleConfig().Size,
						ShiftBy:    w.GetSimpleConfig().ShiftBy,
						TruncateTo: w.GetSimpleConfig().TruncateTo,
					},
				},
			}
			request := pb.UpsertJobSpecificationsRequest{
				ProjectName:   project.Name().String(),
				NamespaceName: namespace.Name().String(),
				Specs:         jobSpecProtos,
			}

			jobService.On("Upsert", ctx, sampleTenant, mock.Anything).Return(nil, errors.New("internal error"))

			resp, err := jobHandler.UpsertJobSpecifications(ctx, &request)
			assert.ErrorContains(t, err, "no jobs to be processed")
			assert.Nil(t, resp)
		})
		t.Run("returns response with job errors log when some jobs failed in upsert process", func(t *testing.T) {
			jobService := new(JobService)
			changeLogService := new(ChangeLogService)

			jobHandler := v1beta1.NewJobHandler(jobService, changeLogService, log)

			jobAName := job.Name("job-A")
			jobBName := job.Name("job-B")
			jobSpecProtos := []*pb.JobSpecification{
				{
					Version:   int32(jobVersion),
					Name:      jobAName.String(),
					Owner:     sampleOwner,
					StartDate: jobSchedule.StartDate().String(),
					EndDate:   jobSchedule.EndDate().String(),
					Interval:  jobSchedule.Interval(),
					Task: &pb.JobSpecTask{
						Name:  jobTask.Name().String(),},
					Window: &pb.JobSpecification_Window{
						Size:       w.GetSimpleConfig().Size,
						ShiftBy:    w.GetSimpleConfig().ShiftBy,
						TruncateTo: w.GetSimpleConfig().TruncateTo,
					},
				},
				{
					Version:   int32(jobVersion),
					Name:      jobBName.String(),
					Owner:     sampleOwner,
					StartDate: jobSchedule.StartDate().String(),
					EndDate:   jobSchedule.EndDate().String(),
					Interval:  jobSchedule.Interval(),
					Task: &pb.JobSpecTask{
						Name:  jobTask.Name().String(),},
					Window: &pb.JobSpecification_Window{
						Size:       w.GetSimpleConfig().Size,
						ShiftBy:    w.GetSimpleConfig().ShiftBy,
						TruncateTo: w.GetSimpleConfig().TruncateTo,
					},
				},
			}
			request := pb.UpsertJobSpecificationsRequest{
				ProjectName:   project.Name().String(),
				NamespaceName: namespace.Name().String(),
				Specs:         jobSpecProtos,
			}

			upsertResults := []dto.UpsertResult{
				{
					JobName: jobAName,
					Status:  job.DeployStateSuccess,
				},
				{
					JobName: jobBName,
					Status:  job.DeployStateFailed,
				},
			}

			jobService.On("Upsert", ctx, sampleTenant, mock.Anything).Return(upsertResults, errors.New("internal error"))

			resp, err := jobHandler.UpsertJobSpecifications(ctx, &request)
			assert.Nil(t, err)
			assert.Contains(t, resp.Log, "error")
			assert.ElementsMatch(t, []string{jobAName.String()}, resp.SuccessfulJobNames)
			assert.ElementsMatch(t, []string{jobBName.String()}, resp.FailedJobNames)
		})
	})
	t.Run("ChangeJobNamespace", func(t *testing.T) {
		newNamespaceName := "newNamespace"

		t.Run("fail if invalid params", func(t *testing.T) {
			t.Run("invalid source namespace", func(t *testing.T) {
				jobService := new(JobService)
				changeLogService := new(ChangeLogService)
				defer jobService.AssertExpectations(t)
				jobAName, _ := job.NameFrom("job-A")
				request := &pb.ChangeJobNamespaceRequest{
					ProjectName:      project.Name().String(),
					NamespaceName:    "",
					JobName:          jobAName.String(),
					NewNamespaceName: newNamespaceName,
				}
				jobHandler := v1beta1.NewJobHandler(jobService, changeLogService, log)
				_, err := jobHandler.ChangeJobNamespace(ctx, request)
				assert.ErrorContains(t, err, "failed to adapt source tenant when changing job namespace")
			})
			t.Run("invalid new namespace", func(t *testing.T) {
				jobService := new(JobService)
				changeLogService := new(ChangeLogService)
				defer jobService.AssertExpectations(t)

				jobAName, _ := job.NameFrom("job-A")
				request := &pb.ChangeJobNamespaceRequest{
					ProjectName:      project.Name().String(),
					NamespaceName:    namespace.Name().String(),
					JobName:          jobAName.String(),
					NewNamespaceName: "",
				}
				jobHandler := v1beta1.NewJobHandler(jobService, changeLogService, log)
				_, err := jobHandler.ChangeJobNamespace(ctx, request)
				assert.ErrorContains(t, err, "failed to adapt new tenant when changing job namespace")
			})
			t.Run("invalid job name", func(t *testing.T) {
				jobService := new(JobService)
				changeLogService := new(ChangeLogService)
				defer jobService.AssertExpectations(t)

				request := &pb.ChangeJobNamespaceRequest{
					ProjectName:      project.Name().String(),
					NamespaceName:    namespace.Name().String(),
					JobName:          "",
					NewNamespaceName: newNamespaceName,
				}
				jobHandler := v1beta1.NewJobHandler(jobService, changeLogService, log)
				_, err := jobHandler.ChangeJobNamespace(ctx, request)
				assert.ErrorContains(t, err, "failed to adapt job name when changing job specification")
			})
		})

		t.Run("Change job namespace successfully", func(t *testing.T) {
			jobService := new(JobService)
			changeLogService := new(ChangeLogService)

			jobAName, _ := job.NameFrom("job-A")
			request := &pb.ChangeJobNamespaceRequest{
				ProjectName:      project.Name().String(),
				NamespaceName:    namespace.Name().String(),
				JobName:          jobAName.String(),
				NewNamespaceName: newNamespaceName,
			}
			newTenant, _ := tenant.NewTenant(project.Name().String(), newNamespaceName)
			jobService.On("ChangeNamespace", ctx, sampleTenant, newTenant, jobAName).Return(nil)

			jobHandler := v1beta1.NewJobHandler(jobService, changeLogService, log)
			_, err := jobHandler.ChangeJobNamespace(ctx, request)
			assert.NoError(t, err)
		})
		t.Run("fail to Change job namespace", func(t *testing.T) {
			jobService := new(JobService)
			changeLogService := new(ChangeLogService)

			jobAName, _ := job.NameFrom("job-A")
			request := &pb.ChangeJobNamespaceRequest{
				ProjectName:      project.Name().String(),
				NamespaceName:    namespace.Name().String(),
				JobName:          jobAName.String(),
				NewNamespaceName: newNamespaceName,
			}
			newTenant, _ := tenant.NewTenant(project.Name().String(), newNamespaceName)
			jobService.On("ChangeNamespace", ctx, sampleTenant, newTenant, jobAName).Return(errors.New("error in changing namespace"))

			jobHandler := v1beta1.NewJobHandler(jobService, changeLogService, log)
			_, err := jobHandler.ChangeJobNamespace(ctx, request)
			assert.ErrorContains(t, err, "error in changing namespace: failed to change job namespace")
		})
	})
	t.Run("UpdateJobState", func(t *testing.T) {
		updateRemark := "job state update remark"
		jobAName, _ := job.NameFrom("job-A")
		t.Run("fail if improper tenant info", func(t *testing.T) {
			request := &pb.UpdateJobsStateRequest{
				ProjectName:   project.Name().String(),
				NamespaceName: "",
				State:         pb.JobState_JOB_STATE_ENABLED,
				Remark:        updateRemark,
				JobNames:      []string{jobAName.String()},
			}

			jobHandler := v1beta1.NewJobHandler(nil, nil, log)
			_, err := jobHandler.UpdateJobsState(ctx, request)
			assert.ErrorContains(t, err, "namespace name is empty")
		})
		t.Run("fail if improper job name", func(t *testing.T) {
			request := &pb.UpdateJobsStateRequest{
				ProjectName:   project.Name().String(),
				NamespaceName: namespace.Name().String(),
				State:         pb.JobState_JOB_STATE_ENABLED,
				Remark:        updateRemark,
				JobNames:      []string{""},
			}

			jobHandler := v1beta1.NewJobHandler(nil, nil, log)
			_, err := jobHandler.UpdateJobsState(ctx, request)
			assert.ErrorContains(t, err, "name is empty")
		})
		t.Run("fail if State is improper", func(t *testing.T) {
			request := &pb.UpdateJobsStateRequest{
				ProjectName:   project.Name().String(),
				NamespaceName: namespace.Name().String(),
				State:         pb.JobState_JOB_STATE_UNSPECIFIED,
				Remark:        updateRemark,
				JobNames:      []string{jobAName.String()},
			}
			jobHandler := v1beta1.NewJobHandler(nil, nil, log)
			_, err := jobHandler.UpdateJobsState(ctx, request)
			assert.ErrorContains(t, err, "invalid state")
		})
		t.Run("fail if remark is empty", func(t *testing.T) {
			request := &pb.UpdateJobsStateRequest{
				ProjectName:   project.Name().String(),
				NamespaceName: namespace.Name().String(),
				JobNames:      []string{jobAName.String()},
				State:         pb.JobState_JOB_STATE_ENABLED,
				Remark:        "",
			}

			jobHandler := v1beta1.NewJobHandler(nil, nil, log)
			_, err := jobHandler.UpdateJobsState(ctx, request)
			assert.ErrorContains(t, err, "can not update job state without a valid remark")
		})
	})
	t.Run("DeleteJobSpecification", func(t *testing.T) {
		t.Run("deletes job successfully", func(t *testing.T) {
			jobService := new(JobService)
			changeLogService := new(ChangeLogService)

			jobAName, _ := job.NameFrom("job-A")
			request := &pb.DeleteJobSpecificationRequest{
				ProjectName:   project.Name().String(),
				NamespaceName: namespace.Name().String(),
				JobName:       jobAName.String(),
				CleanHistory:  false,
				Force:         false,
			}

			jobService.On("Delete", ctx, sampleTenant, jobAName, false, false).Return(nil, nil)

			jobHandler := v1beta1.NewJobHandler(jobService, changeLogService, log)
			resp, err := jobHandler.DeleteJobSpecification(ctx, request)
			assert.NoError(t, err)
			assert.NotContains(t, resp.Message, "these downstream will be affected")
		})
		t.Run("force deletes job with downstream successfully", func(t *testing.T) {
			jobService := new(JobService)
			changeLogService := new(ChangeLogService)

			jobAName, _ := job.NameFrom("job-A")
			request := &pb.DeleteJobSpecificationRequest{
				ProjectName:   project.Name().String(),
				NamespaceName: namespace.Name().String(),
				JobName:       jobAName.String(),
				CleanHistory:  false,
				Force:         true,
			}

			downstreamNames := []job.FullName{"job-B"}
			jobService.On("Delete", ctx, sampleTenant, jobAName, false, true).Return(downstreamNames, nil)

			jobHandler := v1beta1.NewJobHandler(jobService, changeLogService, log)
			resp, err := jobHandler.DeleteJobSpecification(ctx, request)
			assert.NoError(t, err)
			assert.Contains(t, resp.Message, "these downstream will be affected")
		})
		t.Run("returns error if unable to construct tenant", func(t *testing.T) {
			jobService := new(JobService)
			changeLogService := new(ChangeLogService)

			jobAName, _ := job.NameFrom("job-A")
			request := &pb.DeleteJobSpecificationRequest{
				NamespaceName: namespace.Name().String(),
				JobName:       jobAName.String(),
				CleanHistory:  false,
				Force:         true,
			}

			jobHandler := v1beta1.NewJobHandler(jobService, changeLogService, log)
			resp, err := jobHandler.DeleteJobSpecification(ctx, request)
			assert.Error(t, err)
			assert.Nil(t, resp)
		})
		t.Run("returns error if job name is not found", func(t *testing.T) {
			jobService := new(JobService)
			changeLogService := new(ChangeLogService)

			request := &pb.DeleteJobSpecificationRequest{
				ProjectName:   project.Name().String(),
				NamespaceName: namespace.Name().String(),
				CleanHistory:  false,
				Force:         true,
			}

			jobHandler := v1beta1.NewJobHandler(jobService, changeLogService, log)
			resp, err := jobHandler.DeleteJobSpecification(ctx, request)
			assert.Error(t, err)
			assert.Nil(t, resp)
		})
		t.Run("returns error if unable to delete job", func(t *testing.T) {
			jobService := new(JobService)
			changeLogService := new(ChangeLogService)

			jobAName, _ := job.NameFrom("job-A")
			request := &pb.DeleteJobSpecificationRequest{
				ProjectName:   project.Name().String(),
				NamespaceName: namespace.Name().String(),
				JobName:       jobAName.String(),
				CleanHistory:  false,
				Force:         true,
			}

			jobService.On("Delete", ctx, sampleTenant, jobAName, false, true).Return(nil, errors.New("internal error"))

			jobHandler := v1beta1.NewJobHandler(jobService, changeLogService, log)
			resp, err := jobHandler.DeleteJobSpecification(ctx, request)
			assert.Error(t, err)
			assert.Nil(t, resp)
		})
	})
	t.Run("ReplaceAllJobSpecifications", func(t *testing.T) {
		var jobNamesWithInvalidSpec []job.Name
		t.Run("replaces all job specifications of a tenant", func(t *testing.T) {
			jobService := new(JobService)
			changeLogService := new(ChangeLogService)

			jobHandler := v1beta1.NewJobHandler(jobService, changeLogService, log)

			jobProtos := []*pb.JobSpecification{
				{
					Version:   int32(jobVersion),
					Name:      "job-A",
					Owner:     sampleOwner,
					StartDate: jobSchedule.StartDate().String(),
					EndDate:   jobSchedule.EndDate().String(),
					Interval:  jobSchedule.Interval(),
					Task: &pb.JobSpecTask{
						Name:  jobTask.Name().String(),},
					Window: &pb.JobSpecification_Window{
						Size:       w.GetSimpleConfig().Size,
						ShiftBy:    w.GetSimpleConfig().ShiftBy,
						TruncateTo: w.GetSimpleConfig().TruncateTo,
					},
					Config:       []*pb.JobConfigItem{},
					Dependencies: []*pb.JobDependency{},
					Assets:       map[string]string{},
					Hooks:        []*pb.JobSpecHook{},
					Description:  "",
					Labels:       map[string]string{},
					Behavior:     &pb.JobSpecification_Behavior{Notify: []*pb.JobSpecification_Behavior_Notifiers{}},
					Metadata: &pb.JobMetadata{
						Resource: &pb.JobSpecMetadataResource{
							Request: nil,
							Limit:   nil,
						},
						Airflow: &pb.JobSpecMetadataAirflow{},
					},
					Destination: "",
					Sources:     []string{},
				},
				{
					Version:   int32(jobVersion),
					Name:      "job-B",
					Owner:     sampleOwner,
					StartDate: jobSchedule.StartDate().String(),
					EndDate:   jobSchedule.EndDate().String(),
					Interval:  jobSchedule.Interval(),
					Task: &pb.JobSpecTask{
						Name:  jobTask.Name().String(),},
					Window: &pb.JobSpecification_Window{
						Size:       w.GetSimpleConfig().Size,
						ShiftBy:    w.GetSimpleConfig().ShiftBy,
						TruncateTo: w.GetSimpleConfig().TruncateTo,
					},
				},
			}
			request := &pb.ReplaceAllJobSpecificationsRequest{
				ProjectName:   project.Name().String(),
				NamespaceName: namespace.Name().String(),
				Jobs:          jobProtos,
			}

			stream := new(ReplaceAllJobSpecificationsServer)
			stream.On("Context").Return(ctx)
			stream.On("Recv").Return(request, nil).Once()
			stream.On("Recv").Return(nil, io.EOF).Once()

			jobService.On("ReplaceAll", ctx, sampleTenant, mock.Anything, jobNamesWithInvalidSpec, mock.Anything).Return(nil)

			stream.On("Send", mock.AnythingOfType("*optimus.ReplaceAllJobSpecificationsResponse")).Return(nil).Twice()

			err := jobHandler.ReplaceAllJobSpecifications(stream)
			assert.Nil(t, err)
		})
		t.Run("replaces all job specifications given multiple tenant", func(t *testing.T) {
			jobService := new(JobService)
			changeLogService := new(ChangeLogService)

			jobHandler := v1beta1.NewJobHandler(jobService, changeLogService, log)

			jobProtos := []*pb.JobSpecification{
				{
					Version:   int32(jobVersion),
					Name:      "job-A",
					Owner:     sampleOwner,
					StartDate: jobSchedule.StartDate().String(),
					EndDate:   jobSchedule.EndDate().String(),
					Interval:  jobSchedule.Interval(),
					Task: &pb.JobSpecTask{
						Name:  jobTask.Name().String(),},
					Window: &pb.JobSpecification_Window{
						Size:       w.GetSimpleConfig().Size,
						ShiftBy:    w.GetSimpleConfig().ShiftBy,
						TruncateTo: w.GetSimpleConfig().TruncateTo,
					},
				},
			}
			request1 := &pb.ReplaceAllJobSpecificationsRequest{
				ProjectName:   project.Name().String(),
				NamespaceName: namespace.Name().String(),
				Jobs:          jobProtos,
			}

			otherTenant, _ := tenant.NewTenant(project.Name().String(), "other-namespace")
			request2 := &pb.ReplaceAllJobSpecificationsRequest{
				ProjectName:   project.Name().String(),
				NamespaceName: otherTenant.NamespaceName().String(),
				Jobs:          jobProtos,
			}

			stream := new(ReplaceAllJobSpecificationsServer)
			stream.On("Context").Return(ctx)
			stream.On("Recv").Return(request1, nil).Once()
			stream.On("Recv").Return(request2, nil).Once()
			stream.On("Recv").Return(nil, io.EOF).Once()

			jobService.On("ReplaceAll", ctx, sampleTenant, mock.Anything, jobNamesWithInvalidSpec, mock.Anything).Return(nil)
			jobService.On("ReplaceAll", ctx, otherTenant, mock.Anything, jobNamesWithInvalidSpec, mock.Anything).Return(nil)

			stream.On("Send", mock.AnythingOfType("*optimus.ReplaceAllJobSpecificationsResponse")).Return(nil).Twice()

			err := jobHandler.ReplaceAllJobSpecifications(stream)
			assert.Nil(t, err)
		})
		t.Run("skips a job if the proto is invalid", func(t *testing.T) {
			jobService := new(JobService)
			changeLogService := new(ChangeLogService)

			jobHandler := v1beta1.NewJobHandler(jobService, changeLogService, log)

			jobProtos := []*pb.JobSpecification{
				{
					Version: int32(jobVersion),
					Name:    "job-A",
					Owner:   sampleOwner,
				},
				{
					Version:   int32(jobVersion),
					Name:      "job-B",
					Owner:     sampleOwner,
					StartDate: jobSchedule.StartDate().String(),
					EndDate:   jobSchedule.EndDate().String(),
					Interval:  jobSchedule.Interval(),
					Task: &pb.JobSpecTask{
						Name:  jobTask.Name().String(),},
					Window: &pb.JobSpecification_Window{
						Size:       w.GetSimpleConfig().Size,
						ShiftBy:    w.GetSimpleConfig().ShiftBy,
						TruncateTo: w.GetSimpleConfig().TruncateTo,
					},
				},
			}
			request := &pb.ReplaceAllJobSpecificationsRequest{
				ProjectName:   project.Name().String(),
				NamespaceName: namespace.Name().String(),
				Jobs:          jobProtos,
			}

			stream := new(ReplaceAllJobSpecificationsServer)
			stream.On("Context").Return(ctx)
			stream.On("Recv").Return(request, nil).Once()
			stream.On("Recv").Return(nil, io.EOF).Once()

			jobService.On("ReplaceAll", ctx, sampleTenant, mock.Anything, []job.Name{"job-A"}, mock.Anything).Return(nil)

			stream.On("Send", mock.AnythingOfType("*optimus.ReplaceAllJobSpecificationsResponse")).Return(nil).Times(3)

			err := jobHandler.ReplaceAllJobSpecifications(stream)
			assert.ErrorContains(t, err, "error when replacing job specifications")
		})
		t.Run("skips operation for a namespace if the tenant is invalid", func(t *testing.T) {
			jobService := new(JobService)
			changeLogService := new(ChangeLogService)

			jobHandler := v1beta1.NewJobHandler(jobService, changeLogService, log)

			jobProtos := []*pb.JobSpecification{
				{
					Version:   int32(jobVersion),
					Name:      "job-A",
					Owner:     sampleOwner,
					StartDate: jobSchedule.StartDate().String(),
					EndDate:   jobSchedule.EndDate().String(),
					Interval:  jobSchedule.Interval(),
					Task: &pb.JobSpecTask{
						Name:  jobTask.Name().String(),},
					Window: &pb.JobSpecification_Window{
						Size:       w.GetSimpleConfig().Size,
						ShiftBy:    w.GetSimpleConfig().ShiftBy,
						TruncateTo: w.GetSimpleConfig().TruncateTo,
					},
				},
				{
					Version:   int32(jobVersion),
					Name:      "job-B",
					Owner:     sampleOwner,
					StartDate: jobSchedule.StartDate().String(),
					EndDate:   jobSchedule.EndDate().String(),
					Interval:  jobSchedule.Interval(),
					Task: &pb.JobSpecTask{
						Name:  jobTask.Name().String(),},
					Window: &pb.JobSpecification_Window{
						Size:       w.GetSimpleConfig().Size,
						ShiftBy:    w.GetSimpleConfig().ShiftBy,
						TruncateTo: w.GetSimpleConfig().TruncateTo,
					},
				},
			}
			request1 := &pb.ReplaceAllJobSpecificationsRequest{}
			request2 := &pb.ReplaceAllJobSpecificationsRequest{
				ProjectName:   project.Name().String(),
				NamespaceName: namespace.Name().String(),
				Jobs:          jobProtos,
			}

			stream := new(ReplaceAllJobSpecificationsServer)
			stream.On("Context").Return(ctx)
			stream.On("Recv").Return(request1, nil).Once()
			stream.On("Recv").Return(request2, nil).Once()
			stream.On("Recv").Return(nil, io.EOF).Once()

			jobService.On("ReplaceAll", ctx, sampleTenant, mock.Anything, jobNamesWithInvalidSpec, mock.Anything).Return(nil)

			stream.On("Send", mock.AnythingOfType("*optimus.ReplaceAllJobSpecificationsResponse")).Return(nil).Times(4)

			err := jobHandler.ReplaceAllJobSpecifications(stream)
			assert.Error(t, err)
		})
		t.Run("marks operation for this namespace to failed if unable to successfully do replace all", func(t *testing.T) {
			jobService := new(JobService)
			changeLogService := new(ChangeLogService)

			jobHandler := v1beta1.NewJobHandler(jobService, changeLogService, log)

			jobProtos := []*pb.JobSpecification{
				{
					Version:   int32(jobVersion),
					Name:      "job-A",
					Owner:     sampleOwner,
					StartDate: jobSchedule.StartDate().String(),
					EndDate:   jobSchedule.EndDate().String(),
					Interval:  jobSchedule.Interval(),
					Task: &pb.JobSpecTask{
						Name:  jobTask.Name().String(),},
					Window: &pb.JobSpecification_Window{
						Size:       w.GetSimpleConfig().Size,
						ShiftBy:    w.GetSimpleConfig().ShiftBy,
						TruncateTo: w.GetSimpleConfig().TruncateTo,
					},
				},
				{
					Version:   int32(jobVersion),
					Name:      "job-B",
					Owner:     sampleOwner,
					StartDate: jobSchedule.StartDate().String(),
					EndDate:   jobSchedule.EndDate().String(),
					Interval:  jobSchedule.Interval(),
					Task: &pb.JobSpecTask{
						Name:  jobTask.Name().String(),},
					Window: &pb.JobSpecification_Window{
						Size:       w.GetSimpleConfig().Size,
						ShiftBy:    w.GetSimpleConfig().ShiftBy,
						TruncateTo: w.GetSimpleConfig().TruncateTo,
					},
				},
			}
			request := &pb.ReplaceAllJobSpecificationsRequest{
				ProjectName:   project.Name().String(),
				NamespaceName: namespace.Name().String(),
				Jobs:          jobProtos,
			}

			stream := new(ReplaceAllJobSpecificationsServer)
			stream.On("Context").Return(ctx)
			stream.On("Recv").Return(request, nil).Once()
			stream.On("Recv").Return(nil, io.EOF).Once()

			jobService.On("ReplaceAll", ctx, sampleTenant, mock.Anything, jobNamesWithInvalidSpec, mock.Anything).Return(errors.New("internal error"))

			stream.On("Send", mock.AnythingOfType("*optimus.ReplaceAllJobSpecificationsResponse")).Return(nil).Times(3)

			err := jobHandler.ReplaceAllJobSpecifications(stream)
			assert.Error(t, err)
		})
	})
	t.Run("RefreshJobs", func(t *testing.T) {
		t.Run("do refresh for the requested jobs", func(t *testing.T) {
			jobService := new(JobService)
			changeLogService := new(ChangeLogService)

			jobHandler := v1beta1.NewJobHandler(jobService, changeLogService, log)

			request := &pb.RefreshJobsRequest{
				ProjectName:    project.Name().String(),
				NamespaceNames: []string{namespace.Name().String()},
			}

			stream := new(RefreshJobsServer)
			stream.On("Context").Return(ctx)

			jobService.On("Refresh", ctx, project.Name(), request.NamespaceNames, request.JobNames, mock.Anything).Return(nil)

			stream.On("Send", mock.AnythingOfType("*optimus.RefreshJobsResponse")).Return(nil)

			err := jobHandler.RefreshJobs(request, stream)
			assert.Nil(t, err)
		})
		t.Run("returns error if project name is invalid", func(t *testing.T) {
			jobService := new(JobService)
			changeLogService := new(ChangeLogService)

			jobHandler := v1beta1.NewJobHandler(jobService, changeLogService, log)

			request := &pb.RefreshJobsRequest{
				NamespaceNames: []string{namespace.Name().String()},
			}

			stream := new(RefreshJobsServer)
			stream.On("Context").Return(ctx)

			stream.On("Send", mock.AnythingOfType("*optimus.RefreshJobsResponse")).Return(nil)

			err := jobHandler.RefreshJobs(request, stream)
			assert.Error(t, err)
		})
		t.Run("returns error if unable to successfully run refresh", func(t *testing.T) {
			jobService := new(JobService)
			changeLogService := new(ChangeLogService)

			jobHandler := v1beta1.NewJobHandler(jobService, changeLogService, log)

			request := &pb.RefreshJobsRequest{
				ProjectName:    project.Name().String(),
				NamespaceNames: []string{namespace.Name().String()},
			}

			stream := new(RefreshJobsServer)
			stream.On("Context").Return(ctx)

			jobService.On("Refresh", ctx, project.Name(), request.NamespaceNames, request.JobNames, mock.Anything).Return(errors.New("internal error"))

			stream.On("Send", mock.AnythingOfType("*optimus.RefreshJobsResponse")).Return(nil)

			err := jobHandler.RefreshJobs(request, stream)
			assert.ErrorContains(t, err, "internal error")
		})
	})
	t.Run("GetJobSpecification", func(t *testing.T) {
		t.Run("return error when tenant creation failed", func(t *testing.T) {
			jobService := new(JobService)
			changeLogService := new(ChangeLogService)
			defer jobService.AssertExpectations(t)

			request := pb.GetJobSpecificationRequest{}

			jobHandler := v1beta1.NewJobHandler(jobService, changeLogService, log)
			resp, err := jobHandler.GetJobSpecification(ctx, &request)
			assert.Error(t, err)
			assert.Nil(t, resp)
		})
		t.Run("return error when job name is empty", func(t *testing.T) {
			jobService := new(JobService)
			changeLogService := new(ChangeLogService)
			defer jobService.AssertExpectations(t)

			request := pb.GetJobSpecificationRequest{
				ProjectName:   sampleTenant.ProjectName().String(),
				NamespaceName: sampleTenant.NamespaceName().String(),
			}

			jobHandler := v1beta1.NewJobHandler(jobService, changeLogService, log)
			resp, err := jobHandler.GetJobSpecification(ctx, &request)
			assert.Error(t, err)
			assert.Nil(t, resp)
		})
		t.Run("return error when service get failed", func(t *testing.T) {
			jobService := new(JobService)
			changeLogService := new(ChangeLogService)
			defer jobService.AssertExpectations(t)

			request := pb.GetJobSpecificationRequest{
				ProjectName:   sampleTenant.ProjectName().String(),
				NamespaceName: sampleTenant.NamespaceName().String(),
				JobName:       "job-A",
			}

			jobService.On("Get", ctx, sampleTenant, job.Name("job-A")).Return(nil, errors.New("error encountered"))
			jobHandler := v1beta1.NewJobHandler(jobService, changeLogService, log)
			resp, err := jobHandler.GetJobSpecification(ctx, &request)
			assert.Error(t, err)
			assert.Nil(t, resp)
		})
		t.Run("return success", func(t *testing.T) {
			jobService := new(JobService)
			changeLogService := new(ChangeLogService)
			defer jobService.AssertExpectations(t)

			specA, _ := job.NewSpecBuilder(jobVersion, "job-A", sampleOwner, jobSchedule, w, jobTask).Build()
			jobA := job.NewJob(sampleTenant, specA, resourceURNA, []resource.URN{resourceURNB}, false)

			request := pb.GetJobSpecificationRequest{
				ProjectName:   sampleTenant.ProjectName().String(),
				NamespaceName: sampleTenant.NamespaceName().String(),
				JobName:       jobA.Spec().Name().String(),
			}

			jobService.On("Get", ctx, sampleTenant, jobA.Spec().Name()).Return(jobA, nil)
			jobHandler := v1beta1.NewJobHandler(jobService, changeLogService, log)
			resp, err := jobHandler.GetJobSpecification(ctx, &request)
			assert.NoError(t, err)
			assert.NotNil(t, resp)
		})
	})
	t.Run("GetJobSpecifications", func(t *testing.T) {
		asset := map[string]string{
			"query.sql": "select * from something",
		}
		t.Run("return error when service get by filter is failed", func(t *testing.T) {
			jobService := new(JobService)
			changeLogService := new(ChangeLogService)
			defer jobService.AssertExpectations(t)

			request := pb.GetJobSpecificationsRequest{}

			jobService.On("GetByFilter", ctx, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("error encountered"))
			jobHandler := v1beta1.NewJobHandler(jobService, changeLogService, log)
			resp, err := jobHandler.GetJobSpecifications(ctx, &request)
			assert.Error(t, err)
			assert.NotNil(t, resp)
			assert.Empty(t, resp.JobSpecificationResponses)
		})
		t.Run("return success", func(t *testing.T) {
			jobService := new(JobService)
			changeLogService := new(ChangeLogService)
			defer jobService.AssertExpectations(t)

			request := pb.GetJobSpecificationsRequest{}

			specA, _ := job.NewSpecBuilder(jobVersion, "job-A", sampleOwner, jobSchedule, w, jobTask).WithAsset(asset).Build()
			jobA := job.NewJob(sampleTenant, specA, resourceURNA, []resource.URN{resourceURNB}, false)
			specB, _ := job.NewSpecBuilder(jobVersion, "job-B", sampleOwner, jobSchedule, w, jobTask).WithAsset(asset).Build()
			jobB := job.NewJob(sampleTenant, specB, resourceURNB, []resource.URN{resourceURNC}, false)

			jobService.On("GetByFilter", ctx, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*job.Job{jobA, jobB}, nil)
			jobHandler := v1beta1.NewJobHandler(jobService, changeLogService, log)
			resp, err := jobHandler.GetJobSpecifications(ctx, &request)
			assert.NoError(t, err)
			assert.NotNil(t, resp)
			assert.NotEmpty(t, resp.JobSpecificationResponses)
			assert.Len(t, resp.JobSpecificationResponses, 2)
			assert.NotEmpty(t, resp.JobSpecificationResponses[0].Job.Assets)
			assert.NotEmpty(t, resp.JobSpecificationResponses[1].Job.Assets)
		})
		t.Run("return success without asset", func(t *testing.T) {
			jobService := new(JobService)
			changeLogService := new(ChangeLogService)
			defer jobService.AssertExpectations(t)

			request := pb.GetJobSpecificationsRequest{
				IgnoreAssets: true,
			}

			specA, _ := job.NewSpecBuilder(jobVersion, "job-A", sampleOwner, jobSchedule, w, jobTask).WithAsset(asset).Build()
			jobA := job.NewJob(sampleTenant, specA, resourceURNA, []resource.URN{resourceURNB}, false)
			specB, _ := job.NewSpecBuilder(jobVersion, "job-B", sampleOwner, jobSchedule, w, jobTask).WithAsset(asset).Build()
			jobB := job.NewJob(sampleTenant, specB, resourceURNB, []resource.URN{resourceURNC}, false)

			jobService.On("GetByFilter", ctx, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*job.Job{jobA, jobB}, nil)
			jobHandler := v1beta1.NewJobHandler(jobService, changeLogService, log)
			resp, err := jobHandler.GetJobSpecifications(ctx, &request)
			assert.NoError(t, err)
			assert.NotNil(t, resp)
			assert.NotEmpty(t, resp.JobSpecificationResponses)
			assert.Len(t, resp.JobSpecificationResponses, 2)
			assert.Empty(t, resp.JobSpecificationResponses[0].Job.Assets)
			assert.Empty(t, resp.JobSpecificationResponses[1].Job.Assets)
		})
	})
	t.Run("ListJobSpecification", func(t *testing.T) {
		asset := map[string]string{
			"query.sql": "select * from something",
		}
		t.Run("return error when service get by filter failed", func(t *testing.T) {
			jobService := new(JobService)
			changeLogService := new(ChangeLogService)
			defer jobService.AssertExpectations(t)

			request := pb.ListJobSpecificationRequest{}

			jobService.On("GetByFilter", ctx, mock.Anything, mock.Anything).Return(nil, errors.New("error encountered"))
			jobHandler := v1beta1.NewJobHandler(jobService, changeLogService, log)
			resp, err := jobHandler.ListJobSpecification(ctx, &request)
			assert.Error(t, err)
			assert.NotNil(t, resp)
			assert.Empty(t, resp.Jobs)
		})
		t.Run("return success", func(t *testing.T) {
			jobService := new(JobService)
			changeLogService := new(ChangeLogService)
			defer jobService.AssertExpectations(t)

			request := pb.ListJobSpecificationRequest{
				ProjectName:   sampleTenant.ProjectName().String(),
				NamespaceName: sampleTenant.NamespaceName().String(),
			}

			specA, _ := job.NewSpecBuilder(jobVersion, "job-A", sampleOwner, jobSchedule, w, jobTask).WithAsset(asset).Build()
			jobA := job.NewJob(sampleTenant, specA, resourceURNA, []resource.URN{resourceURNB}, false)
			specB, _ := job.NewSpecBuilder(jobVersion, "job-B", sampleOwner, jobSchedule, w, jobTask).WithAsset(asset).Build()
			jobB := job.NewJob(sampleTenant, specB, resourceURNB, []resource.URN{resourceURNC}, false)

			jobService.On("GetByFilter", ctx, mock.Anything, mock.Anything).Return([]*job.Job{jobA, jobB}, nil)
			jobHandler := v1beta1.NewJobHandler(jobService, changeLogService, log)
			resp, err := jobHandler.ListJobSpecification(ctx, &request)
			assert.NoError(t, err)
			assert.NotNil(t, resp)
			assert.NotEmpty(t, resp.Jobs)
			assert.Len(t, resp.Jobs, 2)
			assert.Equal(t, asset, resp.Jobs[0].Assets)
			assert.Equal(t, asset, resp.Jobs[1].Assets)
		})
		t.Run("return success without asset", func(t *testing.T) {
			jobService := new(JobService)
			changeLogService := new(ChangeLogService)
			defer jobService.AssertExpectations(t)

			request := pb.ListJobSpecificationRequest{
				ProjectName:   sampleTenant.ProjectName().String(),
				NamespaceName: sampleTenant.NamespaceName().String(),
				IgnoreAssets:  true,
			}

			specA, _ := job.NewSpecBuilder(jobVersion, "job-A", sampleOwner, jobSchedule, w, jobTask).WithAsset(asset).Build()
			jobA := job.NewJob(sampleTenant, specA, resourceURNA, []resource.URN{resourceURNB}, false)
			specB, _ := job.NewSpecBuilder(jobVersion, "job-B", sampleOwner, jobSchedule, w, jobTask).WithAsset(asset).Build()
			jobB := job.NewJob(sampleTenant, specB, resourceURNB, []resource.URN{resourceURNC}, false)

			jobService.On("GetByFilter", ctx, mock.Anything, mock.Anything).Return([]*job.Job{jobA, jobB}, nil)
			jobHandler := v1beta1.NewJobHandler(jobService, changeLogService, log)
			resp, err := jobHandler.ListJobSpecification(ctx, &request)
			assert.NoError(t, err)
			assert.NotNil(t, resp)
			assert.NotEmpty(t, resp.Jobs)
			assert.Len(t, resp.Jobs, 2)
			assert.Empty(t, resp.Jobs[0].Assets)
			assert.Empty(t, resp.Jobs[1].Assets)
		})
	})
	t.Run("JobInspect", func(t *testing.T) {
		configs := []*pb.JobConfigItem{
			{
				Name:  "sample_key",
				Value: "sample_value",
			},
		}
		t.Run("should return basic info, upstream, downstream of an existing job", func(t *testing.T) {
			jobService := new(JobService)
			changeLogService := new(ChangeLogService)

			httpUpstream, _ := job.NewSpecHTTPUpstreamBuilder("sample-upstream", "sample-url").Build()
			upstreamSpec, _ := job.NewSpecUpstreamBuilder().WithSpecHTTPUpstream([]*job.SpecHTTPUpstream{httpUpstream}).Build()
			specA, _ := job.NewSpecBuilder(jobVersion, "job-A", sampleOwner, jobSchedule, w, jobTask).WithSpecUpstream(upstreamSpec).WithMetadata(metadataSpec).Build()
			jobA := job.NewJob(sampleTenant, specA, resourceURNA, nil, false)

			upstreamB := job.NewUpstreamResolved("job-B", "", resourceURNB, sampleTenant, "static", "bq2bq", false)
			upstreamC := job.NewUpstreamResolved("job-C", "other-host", resourceURNC, sampleTenant, "inferred", "bq2bq", true)

			jobAUpstream := []*job.Upstream{
				upstreamB,
				upstreamC,
			}
			jobADownstream := []*job.Downstream{
				job.NewDownstream("job-x", project.Name(), namespace.Name(), jobTask.Name()),
			}

			var basicInfoLogger writer.BufferedLogger
			jobService.On("GetJobBasicInfo", ctx, sampleTenant, specA.Name(), mock.Anything).Return(jobA, basicInfoLogger)
			jobService.On("GetUpstreamsToInspect", ctx, jobA, false).Return(jobAUpstream, nil)
			jobService.On("GetDownstream", ctx, jobA, false).Return(jobADownstream, nil)

			req := &pb.JobInspectRequest{
				ProjectName:   project.Name().String(),
				NamespaceName: namespace.Name().String(),
				JobName:       specA.Name().String(),
			}

			resp := &pb.JobInspectResponse{
				BasicInfo: &pb.JobInspectResponse_BasicInfoSection{
					Job: &pb.JobSpecification{
						Version:       int32(jobVersion),
						Name:          specA.Name().String(),
						Owner:         sampleOwner,
						StartDate:     specA.Schedule().StartDate().String(),
						EndDate:       specA.Schedule().EndDate().String(),
						Interval:      specA.Schedule().Interval(),
						DependsOnPast: specA.Schedule().DependsOnPast(),
						CatchUp:       specA.Schedule().CatchUp(),
						Task: &pb.JobSpecTask{
							Name: specA.Task().Name().String(),
							Config: []*pb.JobConfigItem{{
								Name:  "sample_key",
								Value: "sample_value",
							}},
						},
						TaskName:      specA.Task().Name().String(),
						Window: &pb.JobSpecification_Window{
							Size:       w.GetSimpleConfig().Size,
							ShiftBy:    w.GetSimpleConfig().ShiftBy,
							TruncateTo: w.GetSimpleConfig().TruncateTo,
						},
						Destination: "store://table-A",
						Config: []*pb.JobConfigItem{{
							Name:  "sample_key",
							Value: "sample_value",
						}},
						Dependencies: []*pb.JobDependency{
							{
								HttpDependency: &pb.HttpDependency{
									Name: httpUpstream.Name(),
									Url:  httpUpstream.URL(),
								},
							},
						},
						Metadata: jobMetadata,
					},
					Destination: "store://table-A",
				},
				Upstreams: &pb.JobInspectResponse_UpstreamSection{
					ExternalDependency: []*pb.JobInspectResponse_JobDependency{
						{
							Name:          upstreamC.Name().String(),
							Host:          upstreamC.Host(),
							ProjectName:   upstreamC.ProjectName().String(),
							NamespaceName: upstreamC.NamespaceName().String(),
							TaskName:      upstreamC.TaskName().String(),
						},
					},
					InternalDependency: []*pb.JobInspectResponse_JobDependency{
						{
							Name:          upstreamB.Name().String(),
							Host:          upstreamB.Host(),
							ProjectName:   upstreamB.ProjectName().String(),
							NamespaceName: upstreamB.NamespaceName().String(),
							TaskName:      upstreamB.TaskName().String(),
						},
					},
					HttpDependency: []*pb.HttpDependency{
						{
							Name: httpUpstream.Name(),
							Url:  httpUpstream.URL(),
						},
					},
				},
				Downstreams: &pb.JobInspectResponse_DownstreamSection{
					DownstreamJobs: []*pb.JobInspectResponse_JobDependency{
						{
							Name:          jobADownstream[0].Name().String(),
							ProjectName:   jobADownstream[0].ProjectName().String(),
							NamespaceName: jobADownstream[0].NamespaceName().String(),
							TaskName:      jobADownstream[0].TaskName().String(),
						},
					},
				},
			}

			handler := v1beta1.NewJobHandler(jobService, changeLogService, log)
			result, err := handler.JobInspect(ctx, req)
			assert.NoError(t, err)
			assert.Equal(t, resp, result)
		})
		t.Run("should return basic info, upstream, downstream of a user given job spec", func(t *testing.T) {
			jobService := new(JobService)
			changeLogService := new(ChangeLogService)

			httpUpstream, _ := job.NewSpecHTTPUpstreamBuilder("sample-upstream", "sample-url").Build()
			upstreamSpec, _ := job.NewSpecUpstreamBuilder().
				WithSpecHTTPUpstream([]*job.SpecHTTPUpstream{httpUpstream}).
				WithUpstreamNames([]job.SpecUpstreamName{"job-B"}).Build()

			hook1, _ := job.NewHook("hook-1", jobConfig, "")

			specA, _ := job.NewSpecBuilder(jobVersion, "job-A", sampleOwner, jobSchedule, w, jobTask).
				WithSpecUpstream(upstreamSpec).
				WithHooks([]*job.Hook{hook1}).Build()
			jobA := job.NewJob(sampleTenant, specA, resourceURNA, nil, false)

			upstreamB := job.NewUpstreamResolved("job-B", "", resourceURNB, sampleTenant, "static", "bq2bq", false)
			upstreamC := job.NewUpstreamResolved("job-C", "other-host", resourceURNC, sampleTenant, "inferred", "bq2bq", true)
			upstreamD := job.NewUpstreamUnresolvedInferred(resourceURND)
			upstreamE := job.NewUpstreamUnresolvedStatic("job-e", project.Name())

			jobAUpstream := []*job.Upstream{
				upstreamB,
				upstreamC,
				upstreamD,
				upstreamE,
			}
			jobADownstream := []*job.Downstream{
				job.NewDownstream("job-x", project.Name(), namespace.Name(), jobTask.Name()),
			}

			var basicInfoLogger writer.BufferedLogger
			jobService.On("GetJobBasicInfo", ctx, sampleTenant, mock.Anything, mock.Anything).Return(jobA, basicInfoLogger)
			jobService.On("GetUpstreamsToInspect", ctx, jobA, true).Return(jobAUpstream, nil)
			jobService.On("GetDownstream", ctx, jobA, true).Return(jobADownstream, nil)

			jobDependenciesWithHTTPProto := []*pb.JobDependency{
				{Name: "job-B"},
				{HttpDependency: &pb.HttpDependency{Name: "sample-upstream", Url: "sample-url"}},
			}

			jobHooksProto := []*pb.JobSpecHook{
				{Name: "hook-1", Config: configs},
			}

			jobSpecProto := &pb.JobSpecification{
				Version:   int32(jobVersion),
				Name:      "job-A",
				Owner:     sampleOwner,
				StartDate: jobSchedule.StartDate().String(),
				Interval:  jobSchedule.Interval(),
				Task: &pb.JobSpecTask{
					Name: jobTask.Name().String(),
				},
				TaskName:  jobTask.Name().String(),
				Window: &pb.JobSpecification_Window{
					Size:       w.GetSimpleConfig().Size,
					ShiftBy:    w.GetSimpleConfig().ShiftBy,
					TruncateTo: w.GetSimpleConfig().TruncateTo,
				},

				Behavior:     jobBehavior,
				Dependencies: jobDependenciesWithHTTPProto,
				Metadata:     jobMetadata,
				Hooks:        jobHooksProto,
			}
			req := &pb.JobInspectRequest{
				ProjectName:   project.Name().String(),
				NamespaceName: namespace.Name().String(),
				Spec:          jobSpecProto,
			}

			resp := &pb.JobInspectResponse{
				BasicInfo: &pb.JobInspectResponse_BasicInfoSection{
					Job: &pb.JobSpecification{
						Version:       int32(jobVersion),
						Name:          specA.Name().String(),
						Owner:         sampleOwner,
						StartDate:     specA.Schedule().StartDate().String(),
						EndDate:       specA.Schedule().EndDate().String(),
						Interval:      specA.Schedule().Interval(),
						DependsOnPast: specA.Schedule().DependsOnPast(),
						CatchUp:       specA.Schedule().CatchUp(),
						Task: &pb.JobSpecTask{
							Name:   specA.Task().Name().String(),
							Config: configs,
						},
						TaskName:      specA.Task().Name().String(),
						Window: &pb.JobSpecification_Window{
							Size:       w.GetSimpleConfig().Size,
							ShiftBy:    w.GetSimpleConfig().ShiftBy,
							TruncateTo: w.GetSimpleConfig().TruncateTo,
						},
						Destination:  "store://table-A",
						Config:       configs,
						Dependencies: jobDependenciesWithHTTPProto,
						Hooks:        jobHooksProto,
					},
					Destination: "store://table-A",
				},
				Upstreams: &pb.JobInspectResponse_UpstreamSection{
					ExternalDependency: []*pb.JobInspectResponse_JobDependency{
						{
							Name:          upstreamC.Name().String(),
							Host:          upstreamC.Host(),
							ProjectName:   upstreamC.ProjectName().String(),
							NamespaceName: upstreamC.NamespaceName().String(),
							TaskName:      upstreamC.TaskName().String(),
						},
					},
					InternalDependency: []*pb.JobInspectResponse_JobDependency{
						{
							Name:          upstreamB.Name().String(),
							Host:          upstreamB.Host(),
							ProjectName:   upstreamB.ProjectName().String(),
							NamespaceName: upstreamB.NamespaceName().String(),
							TaskName:      upstreamB.TaskName().String(),
						},
					},
					UnknownDependencies: []*pb.JobInspectResponse_UpstreamSection_UnknownDependencies{
						{
							ResourceDestination: upstreamD.Resource().String(),
						},
						{
							JobName:     upstreamE.Name().String(),
							ProjectName: upstreamE.ProjectName().String(),
						},
					},
					HttpDependency: []*pb.HttpDependency{
						{Name: "sample-upstream", Url: "sample-url"},
					},
				},
				Downstreams: &pb.JobInspectResponse_DownstreamSection{
					DownstreamJobs: []*pb.JobInspectResponse_JobDependency{
						{
							Name:          jobADownstream[0].Name().String(),
							ProjectName:   jobADownstream[0].ProjectName().String(),
							NamespaceName: jobADownstream[0].NamespaceName().String(),
							TaskName:      jobADownstream[0].TaskName().String(),
						},
					},
				},
			}

			handler := v1beta1.NewJobHandler(jobService, changeLogService, log)
			result, err := handler.JobInspect(ctx, req)
			assert.NoError(t, err)
			assert.Equal(t, resp, result)
		})
		t.Run("should return error if tenant is invalid", func(t *testing.T) {
			jobService := new(JobService)
			changeLogService := new(ChangeLogService)

			req := &pb.JobInspectRequest{
				ProjectName: project.Name().String(),
				JobName:     "job-A",
			}

			handler := v1beta1.NewJobHandler(jobService, changeLogService, log)
			result, err := handler.JobInspect(ctx, req)
			assert.Error(t, err)
			assert.Nil(t, result)
		})
		t.Run("should return error if job name and spec are not provided", func(t *testing.T) {
			jobService := new(JobService)
			changeLogService := new(ChangeLogService)

			req := &pb.JobInspectRequest{
				ProjectName:   project.Name().String(),
				NamespaceName: namespace.Name().String(),
			}

			handler := v1beta1.NewJobHandler(jobService, changeLogService, log)
			result, err := handler.JobInspect(ctx, req)
			assert.Error(t, err)
			assert.Nil(t, result)
		})
		t.Run("should return error if job spec proto is invalid", func(t *testing.T) {
			jobService := new(JobService)
			changeLogService := new(ChangeLogService)

			jobSpecProto := &pb.JobSpecification{
				Name: "job-A",
			}
			req := &pb.JobInspectRequest{
				ProjectName:   project.Name().String(),
				NamespaceName: namespace.Name().String(),
				Spec:          jobSpecProto,
			}

			handler := v1beta1.NewJobHandler(jobService, changeLogService, log)
			result, err := handler.JobInspect(ctx, req)
			assert.Error(t, err)
			assert.Nil(t, result)
		})
		t.Run("should return downstream and upstream error log messages if exist", func(t *testing.T) {
			jobService := new(JobService)
			changeLogService := new(ChangeLogService)

			specA, _ := job.NewSpecBuilder(jobVersion, "job-A", sampleOwner, jobSchedule, w, jobTask).Build()
			jobA := job.NewJob(sampleTenant, specA, resourceURNA, nil, false)

			upstreamB := job.NewUpstreamResolved("job-B", "", resourceURNB, sampleTenant, "static", "bq2bq", false)
			upstreamC := job.NewUpstreamResolved("job-C", "other-host", resourceURNC, sampleTenant, "inferred", "bq2bq", true)

			jobAUpstream := []*job.Upstream{
				upstreamB,
				upstreamC,
			}
			jobADownstream := []*job.Downstream{
				job.NewDownstream("job-x", project.Name(), namespace.Name(), jobTask.Name()),
			}

			var basicInfoLogger writer.BufferedLogger
			jobService.On("GetJobBasicInfo", ctx, sampleTenant, specA.Name(), mock.Anything).Return(jobA, basicInfoLogger)

			upstreamErrorMsg := "sample upstream error"
			jobService.On("GetUpstreamsToInspect", ctx, jobA, false).Return(jobAUpstream, errors.New(upstreamErrorMsg))

			downstreamErrorMsg := "sample downstream error"
			jobService.On("GetDownstream", ctx, jobA, false).Return(jobADownstream, errors.New(downstreamErrorMsg))

			req := &pb.JobInspectRequest{
				ProjectName:   project.Name().String(),
				NamespaceName: namespace.Name().String(),
				JobName:       specA.Name().String(),
			}

			resp := &pb.JobInspectResponse{
				BasicInfo: &pb.JobInspectResponse_BasicInfoSection{
					Job: &pb.JobSpecification{
						Version:       int32(jobVersion),
						Name:          specA.Name().String(),
						Owner:         sampleOwner,
						StartDate:     specA.Schedule().StartDate().String(),
						EndDate:       specA.Schedule().EndDate().String(),
						Interval:      specA.Schedule().Interval(),
						DependsOnPast: specA.Schedule().DependsOnPast(),
						CatchUp:       specA.Schedule().CatchUp(),
						Task: &pb.JobSpecTask{
							Name: specA.Task().Name().String(),
							Config: []*pb.JobConfigItem{{
								Name:  "sample_key",
								Value: "sample_value",
							}},
						},
						TaskName:      specA.Task().Name().String(),
						Window: &pb.JobSpecification_Window{
							Size:       w.GetSimpleConfig().Size,
							ShiftBy:    w.GetSimpleConfig().ShiftBy,
							TruncateTo: w.GetSimpleConfig().TruncateTo,
						},
						Destination: "store://table-A",
						Config: []*pb.JobConfigItem{{
							Name:  "sample_key",
							Value: "sample_value",
						}},
					},
					Destination: "store://table-A",
				},
				Upstreams: &pb.JobInspectResponse_UpstreamSection{
					ExternalDependency: []*pb.JobInspectResponse_JobDependency{
						{
							Name:          upstreamC.Name().String(),
							Host:          upstreamC.Host(),
							ProjectName:   upstreamC.ProjectName().String(),
							NamespaceName: upstreamC.NamespaceName().String(),
							TaskName:      upstreamC.TaskName().String(),
						},
					},
					InternalDependency: []*pb.JobInspectResponse_JobDependency{
						{
							Name:          upstreamB.Name().String(),
							Host:          upstreamB.Host(),
							ProjectName:   upstreamB.ProjectName().String(),
							NamespaceName: upstreamB.NamespaceName().String(),
							TaskName:      upstreamB.TaskName().String(),
						},
					},
					Notice: []*pb.Log{{Level: pb.Level_LEVEL_ERROR, Message: "unable to get upstream jobs: sample upstream error"}},
				},
				Downstreams: &pb.JobInspectResponse_DownstreamSection{
					DownstreamJobs: []*pb.JobInspectResponse_JobDependency{
						{
							Name:          jobADownstream[0].Name().String(),
							ProjectName:   jobADownstream[0].ProjectName().String(),
							NamespaceName: jobADownstream[0].NamespaceName().String(),
							TaskName:      jobADownstream[0].TaskName().String(),
						},
					},
					Notice: []*pb.Log{{Level: pb.Level_LEVEL_ERROR, Message: "unable to get downstream jobs: sample downstream error"}},
				},
			}

			handler := v1beta1.NewJobHandler(jobService, changeLogService, log)
			result, err := handler.JobInspect(ctx, req)
			assert.NoError(t, err)
			assert.Equal(t, resp, result)
		})
		t.Run("should return error if job basic info is not found", func(t *testing.T) {
			jobService := new(JobService)
			changeLogService := new(ChangeLogService)

			httpUpstream, _ := job.NewSpecHTTPUpstreamBuilder("sample-upstream", "sample-url").Build()
			upstreamSpec, _ := job.NewSpecUpstreamBuilder().WithSpecHTTPUpstream([]*job.SpecHTTPUpstream{httpUpstream}).Build()
			specA, _ := job.NewSpecBuilder(jobVersion, "job-A", sampleOwner, jobSchedule, w, jobTask).WithSpecUpstream(upstreamSpec).Build()

			basicInfoLogger := writer.BufferedLogger{Messages: []*pb.Log{
				{Message: "not found"},
			}}
			jobService.On("GetJobBasicInfo", ctx, sampleTenant, specA.Name(), mock.Anything).Return(nil, basicInfoLogger)

			req := &pb.JobInspectRequest{
				ProjectName:   project.Name().String(),
				NamespaceName: namespace.Name().String(),
				JobName:       specA.Name().String(),
			}

			handler := v1beta1.NewJobHandler(jobService, changeLogService, log)
			result, err := handler.JobInspect(ctx, req)
			assert.Nil(t, result)
			assert.ErrorContains(t, err, "not found")
		})
	})
	t.Run("GetJobTask", func(t *testing.T) {
		t.Run("return error when create tenant failed", func(t *testing.T) {
			jobService := new(JobService)
			changeLogService := new(ChangeLogService)
			defer jobService.AssertExpectations(t)

			req := &pb.GetJobTaskRequest{}

			handler := v1beta1.NewJobHandler(jobService, changeLogService, log)
			resp, err := handler.GetJobTask(ctx, req)
			assert.Error(t, err)
			assert.Nil(t, resp)
			assert.Equal(t, "invalid argument for entity project: project name is empty", err.Error())
		})
		t.Run("return error when job name is empty", func(t *testing.T) {
			jobService := new(JobService)
			changeLogService := new(ChangeLogService)
			defer jobService.AssertExpectations(t)

			req := &pb.GetJobTaskRequest{
				ProjectName:   sampleTenant.ProjectName().String(),
				NamespaceName: sampleTenant.NamespaceName().String(),
			}

			handler := v1beta1.NewJobHandler(jobService, changeLogService, log)
			resp, err := handler.GetJobTask(ctx, req)
			assert.Error(t, err)
			assert.Nil(t, resp)
			assert.Equal(t, "invalid argument for entity job: name is empty", err.Error())
		})
		t.Run("return error when service get job eror", func(t *testing.T) {
			jobService := new(JobService)
			changeLogService := new(ChangeLogService)
			defer jobService.AssertExpectations(t)

			req := &pb.GetJobTaskRequest{
				ProjectName:   sampleTenant.ProjectName().String(),
				NamespaceName: sampleTenant.NamespaceName().String(),
				JobName:       "job-A",
			}

			jobService.On("Get", ctx, sampleTenant, job.Name("job-A")).Return(nil, errors.New("error encountered"))
			handler := v1beta1.NewJobHandler(jobService, changeLogService, log)
			resp, err := handler.GetJobTask(ctx, req)
			assert.Error(t, err)
			assert.Nil(t, resp)
		})
		t.Run("return error when service get task info error", func(t *testing.T) {
			jobService := new(JobService)
			changeLogService := new(ChangeLogService)
			defer jobService.AssertExpectations(t)

			specA, _ := job.NewSpecBuilder(jobVersion, "job-A", sampleOwner, jobSchedule, w, jobTask).Build()
			jobA := job.NewJob(sampleTenant, specA, resourceURNA, []resource.URN{resourceURNB}, false)

			req := &pb.GetJobTaskRequest{
				ProjectName:   sampleTenant.ProjectName().String(),
				NamespaceName: sampleTenant.NamespaceName().String(),
				JobName:       jobA.Spec().Name().String(),
			}

			jobService.On("Get", ctx, sampleTenant, jobA.Spec().Name()).Return(jobA, nil)
			jobService.On("GetTaskInfo", ctx, jobA.Spec().Task()).Return(nil, errors.New("error encountered"))
			handler := v1beta1.NewJobHandler(jobService, changeLogService, log)
			resp, err := handler.GetJobTask(ctx, req)
			assert.Error(t, err)
			assert.Nil(t, resp)
		})
		t.Run("return success", func(t *testing.T) {
			jobService := new(JobService)
			changeLogService := new(ChangeLogService)
			defer jobService.AssertExpectations(t)

			specA, _ := job.NewSpecBuilder(jobVersion, "job-A", sampleOwner, jobSchedule, w, jobTask).Build()
			jobA := job.NewJob(sampleTenant, specA, resourceURNA, []resource.URN{resourceURNB}, false)

			req := &pb.GetJobTaskRequest{
				ProjectName:   sampleTenant.ProjectName().String(),
				NamespaceName: sampleTenant.NamespaceName().String(),
				JobName:       jobA.Spec().Name().String(),
			}

			taskInfo := &plugin.Spec{
				Name:        "bq2bq",
				Description: "task info desc",
				PluginVersion: map[string]plugin.VersionDetails{
					"default": {
						Image: "example.com/plugis/exec",
						Tag:   "1.0.0.",
					},
				},
			}
			jobService.On("Get", ctx, sampleTenant, jobA.Spec().Name()).Return(jobA, nil)
			jobService.On("GetTaskInfo", ctx, jobA.Spec().Task()).Return(taskInfo, nil)
			handler := v1beta1.NewJobHandler(jobService, changeLogService, log)
			resp, err := handler.GetJobTask(ctx, req)
			assert.NoError(t, err)
			assert.NotNil(t, resp)
			assert.NotEmpty(t, resp)
		})
	})
}

// ChangeLogService is an autogenerated mock type for the JobService type
type ChangeLogService struct {
	mock.Mock
}

func (_m *ChangeLogService) GetChangelog(ctx context.Context, projectName tenant.ProjectName, jobName job.Name) ([]*job.ChangeLog, error) {
	args := _m.Called(ctx, projectName, jobName)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]*job.ChangeLog), args.Error(1)
}

// JobService is an autogenerated mock type for the JobService type
type JobService struct {
	mock.Mock
}

// Add provides a mock function with given fields: ctx, jobTenant, jobs
func (_m *JobService) Add(ctx context.Context, jobTenant tenant.Tenant, jobs []*job.Spec) ([]job.Name, error) {
	ret := _m.Called(ctx, jobTenant, jobs)

	if len(ret) == 0 {
		panic("no return value specified for Add")
	}

	var r0 []job.Name
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context, tenant.Tenant, []*job.Spec) ([]job.Name, error)); ok {
		return rf(ctx, jobTenant, jobs)
	}
	if rf, ok := ret.Get(0).(func(context.Context, tenant.Tenant, []*job.Spec) []job.Name); ok {
		r0 = rf(ctx, jobTenant, jobs)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]job.Name)
		}
	}

	if rf, ok := ret.Get(1).(func(context.Context, tenant.Tenant, []*job.Spec) error); ok {
		r1 = rf(ctx, jobTenant, jobs)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// Delete provides a mock function with given fields: ctx, jobTenant, jobName, cleanFlag, forceFlag
func (_m *JobService) Delete(ctx context.Context, jobTenant tenant.Tenant, jobName job.Name, cleanFlag, forceFlag bool) ([]job.FullName, error) {
	ret := _m.Called(ctx, jobTenant, jobName, cleanFlag, forceFlag)

	var r0 []job.FullName
	if rf, ok := ret.Get(0).(func(context.Context, tenant.Tenant, job.Name, bool, bool) []job.FullName); ok {
		r0 = rf(ctx, jobTenant, jobName, cleanFlag, forceFlag)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]job.FullName)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, tenant.Tenant, job.Name, bool, bool) error); ok {
		r1 = rf(ctx, jobTenant, jobName, cleanFlag, forceFlag)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// ChangeNamespace provides a mock function with given fields: ctx, jobName, jobTenant, jobNewTenant
func (_m *JobService) ChangeNamespace(ctx context.Context, jobTenant, jobNewTenant tenant.Tenant, jobName job.Name) error {
	ret := _m.Called(ctx, jobTenant, jobNewTenant, jobName)
	return ret.Error(0)
}

// UpdateState provides a mock function with given fields: ctx, jobTenant, jobNames, jobState, remark
func (_m *JobService) UpdateState(ctx context.Context, jobTenant tenant.Tenant, jobNames []job.Name, jobState job.State, remark string) error {
	ret := _m.Called(ctx, jobTenant, jobNames, jobState, remark)
	return ret.Error(0)
}

// UpdateState provides a mock function with given fields: ctx, jobTenant, disabledJobNames, enabledJobNames
func (_m *JobService) SyncState(ctx context.Context, jobTenant tenant.Tenant, disabledJobNames, enabledJobNames []job.Name) error {
	ret := _m.Called(ctx, jobTenant, disabledJobNames, enabledJobNames)
	return ret.Error(0)
}

// Get provides a mock function with given fields: ctx, jobTenant, jobName
func (_m *JobService) Get(ctx context.Context, jobTenant tenant.Tenant, jobName job.Name) (*job.Job, error) {
	ret := _m.Called(ctx, jobTenant, jobName)

	var r0 *job.Job
	if rf, ok := ret.Get(0).(func(context.Context, tenant.Tenant, job.Name) *job.Job); ok {
		r0 = rf(ctx, jobTenant, jobName)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*job.Job)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, tenant.Tenant, job.Name) error); ok {
		r1 = rf(ctx, jobTenant, jobName)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// GetByFilter provides a mock function with given fields: ctx, filters
func (_m *JobService) GetByFilter(ctx context.Context, filters ...filter.FilterOpt) ([]*job.Job, error) {
	_va := make([]interface{}, len(filters))
	for _i := range filters {
		_va[_i] = filters[_i]
	}
	var _ca []interface{}
	_ca = append(_ca, ctx)
	_ca = append(_ca, _va...)
	ret := _m.Called(_ca...)

	var r0 []*job.Job
	if rf, ok := ret.Get(0).(func(context.Context, ...filter.FilterOpt) []*job.Job); ok {
		r0 = rf(ctx, filters...)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]*job.Job)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, ...filter.FilterOpt) error); ok {
		r1 = rf(ctx, filters...)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// GetDownstream provides a mock function with given fields: ctx, _a1, localJob
func (_m *JobService) GetDownstream(ctx context.Context, _a1 *job.Job, localJob bool) ([]*job.Downstream, error) {
	ret := _m.Called(ctx, _a1, localJob)

	var r0 []*job.Downstream
	if rf, ok := ret.Get(0).(func(context.Context, *job.Job, bool) []*job.Downstream); ok {
		r0 = rf(ctx, _a1, localJob)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]*job.Downstream)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, *job.Job, bool) error); ok {
		r1 = rf(ctx, _a1, localJob)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// GetJobBasicInfo provides a mock function with given fields: ctx, jobTenant, jobName, spec
func (_m *JobService) GetJobBasicInfo(ctx context.Context, jobTenant tenant.Tenant, jobName job.Name, spec *job.Spec) (*job.Job, writer.BufferedLogger) {
	ret := _m.Called(ctx, jobTenant, jobName, spec)

	var r0 *job.Job
	if rf, ok := ret.Get(0).(func(context.Context, tenant.Tenant, job.Name, *job.Spec) *job.Job); ok {
		r0 = rf(ctx, jobTenant, jobName, spec)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*job.Job)
		}
	}

	var r1 writer.BufferedLogger
	if rf, ok := ret.Get(1).(func(context.Context, tenant.Tenant, job.Name, *job.Spec) writer.BufferedLogger); ok {
		r1 = rf(ctx, jobTenant, jobName, spec)
	} else {
		r1 = ret.Get(1).(writer.BufferedLogger)
	}

	return r0, r1
}

func (_m *JobService) GetChangelog(ctx context.Context, projectName tenant.ProjectName, jobName job.Name) ([]*job.ChangeLog, error) {
	args := _m.Called(ctx, projectName, jobName)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]*job.ChangeLog), args.Error(1)
}

// GetTaskInfo provides a mock function with given fields: ctx, task
func (_m *JobService) GetTaskInfo(ctx context.Context, task job.Task) (*plugin.Spec, error) {
	ret := _m.Called(ctx, task)

	if ret.Get(0) == nil {
		return nil, ret.Error(1)
	}

	return ret.Get(0).(*plugin.Spec), ret.Error(1)
}

// GetUpstreamsToInspect provides a mock function with given fields: ctx, subjectJob, localJob
func (_m *JobService) GetUpstreamsToInspect(ctx context.Context, subjectJob *job.Job, localJob bool) ([]*job.Upstream, error) {
	ret := _m.Called(ctx, subjectJob, localJob)

	var r0 []*job.Upstream
	if rf, ok := ret.Get(0).(func(context.Context, *job.Job, bool) []*job.Upstream); ok {
		r0 = rf(ctx, subjectJob, localJob)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]*job.Upstream)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, *job.Job, bool) error); ok {
		r1 = rf(ctx, subjectJob, localJob)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// Refresh provides a mock function with given fields: ctx, projectName, namespaceNames, jobNames, logWriter
func (_m *JobService) Refresh(ctx context.Context, projectName tenant.ProjectName, namespaceNames, jobNames []string, logWriter writer.LogWriter) error {
	ret := _m.Called(ctx, projectName, namespaceNames, jobNames, logWriter)

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, tenant.ProjectName, []string, []string, writer.LogWriter) error); ok {
		r0 = rf(ctx, projectName, namespaceNames, jobNames, logWriter)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// ReplaceAll provides a mock function with given fields: ctx, jobTenant, jobs, jobNamesWithInvalidSpec, logWriter
func (_m *JobService) ReplaceAll(ctx context.Context, jobTenant tenant.Tenant, jobs []*job.Spec, jobNamesWithInvalidSpec []job.Name, logWriter writer.LogWriter) error {
	ret := _m.Called(ctx, jobTenant, jobs, jobNamesWithInvalidSpec, logWriter)

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, tenant.Tenant, []*job.Spec, []job.Name, writer.LogWriter) error); ok {
		r0 = rf(ctx, jobTenant, jobs, jobNamesWithInvalidSpec, logWriter)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// Update provides a mock function with given fields: ctx, jobTenant, jobs
func (_m *JobService) Update(ctx context.Context, jobTenant tenant.Tenant, jobs []*job.Spec) ([]job.Name, error) {
	ret := _m.Called(ctx, jobTenant, jobs)

	if len(ret) == 0 {
		panic("no return value specified for Update")
	}

	var r0 []job.Name
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context, tenant.Tenant, []*job.Spec) ([]job.Name, error)); ok {
		return rf(ctx, jobTenant, jobs)
	}
	if rf, ok := ret.Get(0).(func(context.Context, tenant.Tenant, []*job.Spec) []job.Name); ok {
		r0 = rf(ctx, jobTenant, jobs)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]job.Name)
		}
	}

	if rf, ok := ret.Get(1).(func(context.Context, tenant.Tenant, []*job.Spec) error); ok {
		r1 = rf(ctx, jobTenant, jobs)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// Upsert provides a mock function with given fields: ctx, jobTenant, jobs
func (_m *JobService) Upsert(ctx context.Context, jobTenant tenant.Tenant, jobs []*job.Spec) ([]dto.UpsertResult, error) {
	ret := _m.Called(ctx, jobTenant, jobs)

	var r0 []dto.UpsertResult
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context, tenant.Tenant, []*job.Spec) ([]dto.UpsertResult, error)); ok {
		return rf(ctx, jobTenant, jobs)
	}
	if rf, ok := ret.Get(0).(func(context.Context, tenant.Tenant, []*job.Spec) []dto.UpsertResult); ok {
		r0 = rf(ctx, jobTenant, jobs)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]dto.UpsertResult)
		}
	}

	if rf, ok := ret.Get(1).(func(context.Context, tenant.Tenant, []*job.Spec) error); ok {
		r1 = rf(ctx, jobTenant, jobs)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// Validate provides a mock function with given fields: ctx, request
func (_m *JobService) Validate(ctx context.Context, request dto.ValidateRequest) (map[job.Name][]dto.ValidateResult, error) {
	ret := _m.Called(ctx, request)

	var r0 map[job.Name][]dto.ValidateResult
	if rf, ok := ret.Get(0).(func(context.Context, dto.ValidateRequest) map[job.Name][]dto.ValidateResult); ok {
		r0 = rf(ctx, request)
	} else {
		r0 = ret.Get(0).(map[job.Name][]dto.ValidateResult)
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, dto.ValidateRequest) error); ok {
		r1 = rf(ctx, request)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// BulkDeleteJobs provides a mock function with given fields: ctx, projectName, jobsToDelete
func (_m *JobService) BulkDeleteJobs(ctx context.Context, projectName tenant.ProjectName, jobsToDelete []*dto.JobToDeleteRequest) (map[string]dto.BulkDeleteTracker, error) {
	ret := _m.Called(ctx, projectName, jobsToDelete)

	if len(ret) == 0 {
		panic("no return value specified for BulkDeleteJobs")
	}

	var r0 map[string]dto.BulkDeleteTracker
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context, tenant.ProjectName, []*dto.JobToDeleteRequest) (map[string]dto.BulkDeleteTracker, error)); ok {
		return rf(ctx, projectName, jobsToDelete)
	}
	if rf, ok := ret.Get(0).(func(context.Context, tenant.ProjectName, []*dto.JobToDeleteRequest) map[string]dto.BulkDeleteTracker); ok {
		r0 = rf(ctx, projectName, jobsToDelete)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(map[string]dto.BulkDeleteTracker)
		}
	}

	if rf, ok := ret.Get(1).(func(context.Context, tenant.ProjectName, []*dto.JobToDeleteRequest) error); ok {
		r1 = rf(ctx, projectName, jobsToDelete)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// ReplaceAllJobSpecificationsServer is an autogenerated mock type for the ReplaceAllJobSpecificationsServer type
type ReplaceAllJobSpecificationsServer struct {
	mock.Mock
}

// Context provides a mock function with given fields:
func (_m *ReplaceAllJobSpecificationsServer) Context() context.Context {
	ret := _m.Called()

	var r0 context.Context
	if rf, ok := ret.Get(0).(func() context.Context); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(context.Context)
		}
	}

	return r0
}

// Recv provides a mock function with given fields:
func (_m *ReplaceAllJobSpecificationsServer) Recv() (*pb.ReplaceAllJobSpecificationsRequest, error) {
	ret := _m.Called()

	var r0 *pb.ReplaceAllJobSpecificationsRequest
	if rf, ok := ret.Get(0).(func() *pb.ReplaceAllJobSpecificationsRequest); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*pb.ReplaceAllJobSpecificationsRequest)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func() error); ok {
		r1 = rf()
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// RecvMsg provides a mock function with given fields: m
func (_m *ReplaceAllJobSpecificationsServer) RecvMsg(m interface{}) error {
	ret := _m.Called(m)

	var r0 error
	if rf, ok := ret.Get(0).(func(interface{}) error); ok {
		r0 = rf(m)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// Send provides a mock function with given fields: _a0
func (_m *ReplaceAllJobSpecificationsServer) Send(_a0 *pb.ReplaceAllJobSpecificationsResponse) error {
	ret := _m.Called(_a0)

	var r0 error
	if rf, ok := ret.Get(0).(func(*pb.ReplaceAllJobSpecificationsResponse) error); ok {
		r0 = rf(_a0)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// SendHeader provides a mock function with given fields: _a0
func (_m *ReplaceAllJobSpecificationsServer) SendHeader(_a0 metadata.MD) error {
	ret := _m.Called(_a0)

	var r0 error
	if rf, ok := ret.Get(0).(func(metadata.MD) error); ok {
		r0 = rf(_a0)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// SendMsg provides a mock function with given fields: m
func (_m *ReplaceAllJobSpecificationsServer) SendMsg(m interface{}) error {
	ret := _m.Called(m)

	var r0 error
	if rf, ok := ret.Get(0).(func(interface{}) error); ok {
		r0 = rf(m)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// SetHeader provides a mock function with given fields: _a0
func (_m *ReplaceAllJobSpecificationsServer) SetHeader(_a0 metadata.MD) error {
	ret := _m.Called(_a0)

	var r0 error
	if rf, ok := ret.Get(0).(func(metadata.MD) error); ok {
		r0 = rf(_a0)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// SetTrailer provides a mock function with given fields: _a0
func (_m *ReplaceAllJobSpecificationsServer) SetTrailer(_a0 metadata.MD) {
	_m.Called(_a0)
}

// RefreshJobsServer is an autogenerated mock type for the RefreshJobsServer type
type RefreshJobsServer struct {
	mock.Mock
}

// Context provides a mock function with given fields:
func (_m *RefreshJobsServer) Context() context.Context {
	ret := _m.Called()

	var r0 context.Context
	if rf, ok := ret.Get(0).(func() context.Context); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(context.Context)
		}
	}

	return r0
}

// RecvMsg provides a mock function with given fields: m
func (_m *RefreshJobsServer) RecvMsg(m interface{}) error {
	ret := _m.Called(m)

	var r0 error
	if rf, ok := ret.Get(0).(func(interface{}) error); ok {
		r0 = rf(m)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// Send provides a mock function with given fields: _a0
func (_m *RefreshJobsServer) Send(_a0 *pb.RefreshJobsResponse) error {
	ret := _m.Called(_a0)

	var r0 error
	if rf, ok := ret.Get(0).(func(*pb.RefreshJobsResponse) error); ok {
		r0 = rf(_a0)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// SendHeader provides a mock function with given fields: _a0
func (_m *RefreshJobsServer) SendHeader(_a0 metadata.MD) error {
	ret := _m.Called(_a0)

	var r0 error
	if rf, ok := ret.Get(0).(func(metadata.MD) error); ok {
		r0 = rf(_a0)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// SendMsg provides a mock function with given fields: m
func (_m *RefreshJobsServer) SendMsg(m interface{}) error {
	ret := _m.Called(m)

	var r0 error
	if rf, ok := ret.Get(0).(func(interface{}) error); ok {
		r0 = rf(m)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// SetHeader provides a mock function with given fields: _a0
func (_m *RefreshJobsServer) SetHeader(_a0 metadata.MD) error {
	ret := _m.Called(_a0)

	var r0 error
	if rf, ok := ret.Get(0).(func(metadata.MD) error); ok {
		r0 = rf(_a0)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// SetTrailer provides a mock function with given fields: _a0
func (_m *RefreshJobsServer) SetTrailer(_a0 metadata.MD) {
	_m.Called(_a0)
}

// CheckJobSpecificationsServer is an autogenerated mock type for the CheckJobSpecificationsServer type
type CheckJobSpecificationsServer struct {
	mock.Mock
}

// Context provides a mock function with given fields:
func (_m *CheckJobSpecificationsServer) Context() context.Context {
	ret := _m.Called()

	var r0 context.Context
	if rf, ok := ret.Get(0).(func() context.Context); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(context.Context)
		}
	}

	return r0
}

// RecvMsg provides a mock function with given fields: m
func (_m *CheckJobSpecificationsServer) RecvMsg(m interface{}) error {
	ret := _m.Called(m)

	var r0 error
	if rf, ok := ret.Get(0).(func(interface{}) error); ok {
		r0 = rf(m)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// Send provides a mock function with given fields: _a0
func (_m *CheckJobSpecificationsServer) Send(_a0 *pb.CheckJobSpecificationsResponse) error {
	ret := _m.Called(_a0)

	var r0 error
	if rf, ok := ret.Get(0).(func(*pb.CheckJobSpecificationsResponse) error); ok {
		r0 = rf(_a0)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// SendHeader provides a mock function with given fields: _a0
func (_m *CheckJobSpecificationsServer) SendHeader(_a0 metadata.MD) error {
	ret := _m.Called(_a0)

	var r0 error
	if rf, ok := ret.Get(0).(func(metadata.MD) error); ok {
		r0 = rf(_a0)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// SendMsg provides a mock function with given fields: m
func (_m *CheckJobSpecificationsServer) SendMsg(m interface{}) error {
	ret := _m.Called(m)

	var r0 error
	if rf, ok := ret.Get(0).(func(interface{}) error); ok {
		r0 = rf(m)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// SetHeader provides a mock function with given fields: _a0
func (_m *CheckJobSpecificationsServer) SetHeader(_a0 metadata.MD) error {
	ret := _m.Called(_a0)

	var r0 error
	if rf, ok := ret.Get(0).(func(metadata.MD) error); ok {
		r0 = rf(_a0)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// SetTrailer provides a mock function with given fields: _a0
func (_m *CheckJobSpecificationsServer) SetTrailer(_a0 metadata.MD) {
	_m.Called(_a0)
}
