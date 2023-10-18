package service_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/goto/salt/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/goto/optimus/core/event/moderator"
	"github.com/goto/optimus/core/job"
	"github.com/goto/optimus/core/job/service"
	"github.com/goto/optimus/core/job/service/filter"
	"github.com/goto/optimus/core/tenant"
	"github.com/goto/optimus/internal/compiler"
	optErrors "github.com/goto/optimus/internal/errors"
	"github.com/goto/optimus/internal/lib/window"
	"github.com/goto/optimus/internal/models"
	"github.com/goto/optimus/internal/writer"
	"github.com/goto/optimus/sdk/plugin"
)

func TestJobService(t *testing.T) {
	ctx := context.Background()
	project, _ := tenant.NewProject("test-proj",
		map[string]string{
			"bucket":                     "gs://some_folder-2",
			tenant.ProjectSchedulerHost:  "host",
			tenant.ProjectStoragePathKey: "gs://location",
		})
	namespace, _ := tenant.NewNamespace("test-ns", project.Name(),
		map[string]string{
			"bucket": "gs://ns_bucket",
		})
	secret1, err := tenant.NewPlainTextSecret("table_name", "secret_table")
	assert.Nil(t, err)

	sampleTenant, _ := tenant.NewTenant(project.Name().String(), namespace.Name().String())
	detailedTenant, _ := tenant.NewTenantDetails(project, namespace, []*tenant.PlainTextSecret{secret1})

	otherNamespace, _ := tenant.NewNamespace("other-ns", project.Name(),
		map[string]string{
			"bucket": "gs://other_ns_bucket",
		})
	otherTenant, _ := tenant.NewTenant(project.Name().String(), otherNamespace.Name().String())
	secret2, err := tenant.NewPlainTextSecret("bucket", "gs://some_secret_bucket")
	assert.Nil(t, err)
	detailedOtherTenant, _ := tenant.NewTenantDetails(project, otherNamespace, []*tenant.PlainTextSecret{secret2})

	jobVersion := 1
	startDate, err := job.ScheduleDateFrom("2022-10-01")
	assert.NoError(t, err)
	jobSchedule, err := job.NewScheduleBuilder(startDate).Build()
	assert.NoError(t, err)
	w, _ := models.NewWindow(jobVersion, "d", "24h", "24h")
	jobWindow := window.NewCustomConfig(w)
	jobTaskConfig, err := job.ConfigFrom(map[string]string{
		"sample_task_key":    "sample_value",
		"BQ_SERVICE_ACCOUNT": "service_account",
	})
	assert.NoError(t, err)
	taskName, _ := job.TaskNameFrom("bq2bq")
	jobTask := job.NewTask(taskName, jobTaskConfig)
	jobAsset := job.Asset(map[string]string{
		"query.sql": "select * from `project.dataset.sample`",
	})

	log := log.NewNoop()

	var jobNamesWithInvalidSpec []job.Name
	var emptyJobNames []string

	t.Run("Add", func(t *testing.T) {
		t.Run("add jobs", func(t *testing.T) {
			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			upstreamRepo := new(UpstreamRepository)
			defer upstreamRepo.AssertExpectations(t)

			downstreamRepo := new(DownstreamRepository)
			defer downstreamRepo.AssertExpectations(t)

			pluginService := new(PluginService)
			defer pluginService.AssertExpectations(t)

			upstreamResolver := new(UpstreamResolver)
			defer upstreamResolver.AssertExpectations(t)

			tenantDetailsGetter := new(TenantDetailsGetter)
			defer tenantDetailsGetter.AssertExpectations(t)

			jobDeploymentService := new(JobDeploymentService)
			defer jobDeploymentService.AssertExpectations(t)

			eventHandler := newEventHandler(t)

			specA, _ := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()
			specs := []*job.Spec{specA}

			tenantDetailsGetter.On("GetDetails", ctx, sampleTenant).Return(detailedTenant, nil)

			jobADestination := job.ResourceURN("bigquery://project:dataset.tableA")
			pluginService.On("ConstructDestinationURN", ctx, specA.Task().Name().String(), mock.Anything).Return(jobADestination.String(), nil)

			jobAUpstreamName := []job.ResourceURN{"bigquery://project:dataset.tableB"}
			pluginService.On("IdentifyUpstreams", ctx, specA.Task().Name().String(), mock.Anything, mock.Anything).Return(jobResourceURNsToString(jobAUpstreamName), nil)

			jobA := job.NewJob(sampleTenant, specA, jobADestination, jobAUpstreamName)
			jobs := []*job.Job{jobA}
			jobRepo.On("Add", ctx, mock.Anything).Return(jobs, nil, nil)

			upstream := job.NewUpstreamResolved("job-B", "", "bigquery://project:dataset.tableB", sampleTenant, "static", taskName, false)
			jobWithUpstream := job.NewWithUpstream(jobA, []*job.Upstream{upstream})
			upstreamResolver.On("BulkResolve", ctx, project.Name(), jobs, mock.Anything).Return([]*job.WithUpstream{jobWithUpstream}, nil, nil)

			upstreamRepo.On("ReplaceUpstreams", ctx, []*job.WithUpstream{jobWithUpstream}).Return(nil)

			jobNamesToUpload := []string{jobA.GetName()}
			jobDeploymentService.On("UploadJobs", ctx, sampleTenant, jobNamesToUpload, emptyJobNames).Return(nil)

			eventHandler.On("HandleEvent", mock.Anything).Times(1)

			jobService := service.NewJobService(jobRepo, upstreamRepo, downstreamRepo, pluginService, upstreamResolver, tenantDetailsGetter, eventHandler, log, jobDeploymentService, compiler.NewEngine())
			err := jobService.Add(ctx, sampleTenant, specs)
			assert.NoError(t, err)
		})
		t.Run("return error if unable to get detailed tenant", func(t *testing.T) {
			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			upstreamRepo := new(UpstreamRepository)
			defer upstreamRepo.AssertExpectations(t)

			downstreamRepo := new(DownstreamRepository)
			defer downstreamRepo.AssertExpectations(t)

			pluginService := new(PluginService)
			defer pluginService.AssertExpectations(t)

			upstreamResolver := new(UpstreamResolver)
			defer upstreamResolver.AssertExpectations(t)

			tenantDetailsGetter := new(TenantDetailsGetter)
			defer tenantDetailsGetter.AssertExpectations(t)

			specA, _ := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()
			specs := []*job.Spec{specA}

			tenantDetailsGetter.On("GetDetails", ctx, sampleTenant).Return(&tenant.WithDetails{}, errors.New("internal error"))

			jobService := service.NewJobService(jobRepo, upstreamRepo, downstreamRepo, pluginService, upstreamResolver, tenantDetailsGetter, nil, log, nil, compiler.NewEngine())
			err := jobService.Add(ctx, sampleTenant, specs)
			assert.ErrorContains(t, err, "internal error")
		})
		t.Run("skip job that has issue when generating destination and return error", func(t *testing.T) {
			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			upstreamRepo := new(UpstreamRepository)
			defer upstreamRepo.AssertExpectations(t)

			downstreamRepo := new(DownstreamRepository)
			defer downstreamRepo.AssertExpectations(t)

			pluginService := new(PluginService)
			defer pluginService.AssertExpectations(t)

			upstreamResolver := new(UpstreamResolver)
			defer upstreamResolver.AssertExpectations(t)

			tenantDetailsGetter := new(TenantDetailsGetter)
			defer tenantDetailsGetter.AssertExpectations(t)

			jobDeploymentService := new(JobDeploymentService)
			defer jobDeploymentService.AssertExpectations(t)

			eventHandler := newEventHandler(t)

			specA, _ := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()
			specB, _ := job.NewSpecBuilder(jobVersion, "job-B", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()
			specC, _ := job.NewSpecBuilder(jobVersion, "job-C", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()
			specs := []*job.Spec{specB, specA, specC}

			tenantDetailsGetter.On("GetDetails", ctx, sampleTenant).Return(detailedTenant, nil)

			jobADestination := job.ResourceURN("bigquery://project:dataset.tableA")
			jobBDestination := job.ResourceURN("bigquery://project:dataset.tableB")
			var jobDestination job.ResourceURN
			pluginService.On("ConstructDestinationURN", ctx, specA.Task().Name().String(), mock.Anything).Return(jobADestination.String(), nil).Once()
			pluginService.On("ConstructDestinationURN", ctx, specB.Task().Name().String(), mock.Anything).Return(jobBDestination.String(), nil).Once()
			pluginService.On("ConstructDestinationURN", ctx, specC.Task().Name().String(), mock.Anything).Return(jobDestination.String(), errors.New("generate destination error")).Once()

			jobAUpstreamName := []job.ResourceURN{"bigquery://project:dataset.tableB"}
			pluginService.On("IdentifyUpstreams", ctx, specB.Task().Name().String(), mock.Anything, mock.Anything).Return([]string{}, errors.New("generate upstream error"))
			pluginService.On("IdentifyUpstreams", ctx, specA.Task().Name().String(), mock.Anything, mock.Anything).Return(jobResourceURNsToString(jobAUpstreamName), nil)

			jobA := job.NewJob(sampleTenant, specA, jobADestination, jobAUpstreamName)
			jobs := []*job.Job{jobA}
			jobRepo.On("Add", ctx, mock.Anything).Return(jobs, nil)

			upstream := job.NewUpstreamResolved("job-B", "", "bigquery://project:dataset.tableB", sampleTenant, "static", taskName, false)
			jobWithUpstream := job.NewWithUpstream(jobA, []*job.Upstream{upstream})
			upstreamResolver.On("BulkResolve", ctx, project.Name(), jobs, mock.Anything).Return([]*job.WithUpstream{jobWithUpstream}, nil, nil)

			upstreamRepo.On("ReplaceUpstreams", ctx, []*job.WithUpstream{jobWithUpstream}).Return(nil)

			jobNamesToUpload := []string{jobA.GetName()}
			jobDeploymentService.On("UploadJobs", ctx, sampleTenant, jobNamesToUpload, emptyJobNames).Return(nil)

			eventHandler.On("HandleEvent", mock.Anything).Times(1)

			jobService := service.NewJobService(jobRepo, upstreamRepo, downstreamRepo, pluginService, upstreamResolver, tenantDetailsGetter, eventHandler, log, jobDeploymentService, compiler.NewEngine())
			err := jobService.Add(ctx, sampleTenant, specs)
			assert.ErrorContains(t, err, "generate upstream error")
		})
		t.Run("return error when all jobs failed to have destination and upstream generated", func(t *testing.T) {
			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			upstreamRepo := new(UpstreamRepository)
			defer upstreamRepo.AssertExpectations(t)

			downstreamRepo := new(DownstreamRepository)
			defer downstreamRepo.AssertExpectations(t)

			pluginService := new(PluginService)
			defer pluginService.AssertExpectations(t)

			upstreamResolver := new(UpstreamResolver)
			defer upstreamResolver.AssertExpectations(t)

			tenantDetailsGetter := new(TenantDetailsGetter)
			defer tenantDetailsGetter.AssertExpectations(t)

			specA, _ := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()
			specB, _ := job.NewSpecBuilder(jobVersion, "job-B", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()
			specs := []*job.Spec{specB, specA}

			jobRepo.On("Add", ctx, mock.Anything).Return(nil, nil)

			upstreamResolver.On("BulkResolve", ctx, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)

			upstreamRepo.On("ReplaceUpstreams", ctx, mock.Anything).Return(nil)

			tenantDetailsGetter.On("GetDetails", ctx, sampleTenant).Return(detailedTenant, nil)

			var jobADestination job.ResourceURN
			jobBDestination := job.ResourceURN("bigquery://project:dataset.tableB")
			pluginService.On("ConstructDestinationURN", ctx, specB.Task().Name().String(), mock.Anything).Return(jobBDestination.String(), nil).Once()
			pluginService.On("ConstructDestinationURN", ctx, specA.Task().Name().String(), mock.Anything).Return(jobADestination.String(), errors.New("generate destination error")).Once()

			pluginService.On("IdentifyUpstreams", ctx, specB.Task().Name().String(), mock.Anything, mock.Anything).Return(nil, errors.New("generate upstream error"))

			jobService := service.NewJobService(jobRepo, upstreamRepo, downstreamRepo, pluginService, upstreamResolver, tenantDetailsGetter, nil, log, nil, compiler.NewEngine())
			err := jobService.Add(ctx, sampleTenant, specs)
			assert.ErrorContains(t, err, "generate upstream error")
		})
		t.Run("should not skip nor return error if jobs is not bq2bq", func(t *testing.T) {
			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			upstreamRepo := new(UpstreamRepository)
			defer upstreamRepo.AssertExpectations(t)

			downstreamRepo := new(DownstreamRepository)
			defer downstreamRepo.AssertExpectations(t)

			pluginService := new(PluginService)
			defer pluginService.AssertExpectations(t)

			upstreamResolver := new(UpstreamResolver)
			defer upstreamResolver.AssertExpectations(t)

			tenantDetailsGetter := new(TenantDetailsGetter)
			defer tenantDetailsGetter.AssertExpectations(t)

			jobDeploymentService := new(JobDeploymentService)
			defer jobDeploymentService.AssertExpectations(t)

			eventHandler := newEventHandler(t)

			nonBq2bqTask := job.NewTask("another", nil)
			specA, _ := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, jobWindow, nonBq2bqTask).Build()
			specs := []*job.Spec{specA}

			tenantDetailsGetter.On("GetDetails", ctx, sampleTenant).Return(detailedTenant, nil)

			jobA := job.NewJob(sampleTenant, specA, "", nil)
			jobs := []*job.Job{jobA}

			jobRepo.On("Add", ctx, mock.Anything).Return(jobs, nil)

			pluginService.On("ConstructDestinationURN", ctx, specA.Task().Name().String(), mock.Anything).Return("", nil)
			pluginService.On("IdentifyUpstreams", ctx, specA.Task().Name().String(), mock.Anything, mock.Anything).Return([]string{}, nil)
			jobWithUpstream := job.NewWithUpstream(jobA, nil)
			upstreamResolver.On("BulkResolve", ctx, project.Name(), jobs, mock.Anything).Return([]*job.WithUpstream{jobWithUpstream}, nil, nil)

			upstreamRepo.On("ReplaceUpstreams", ctx, []*job.WithUpstream{jobWithUpstream}).Return(nil)

			jobNamesToUpload := []string{jobA.GetName()}
			jobDeploymentService.On("UploadJobs", ctx, sampleTenant, jobNamesToUpload, emptyJobNames).Return(nil)

			eventHandler.On("HandleEvent", mock.Anything).Times(1)

			jobService := service.NewJobService(jobRepo, upstreamRepo, downstreamRepo, pluginService, upstreamResolver, tenantDetailsGetter, eventHandler, log, jobDeploymentService, compiler.NewEngine())
			err := jobService.Add(ctx, sampleTenant, specs)
			assert.NoError(t, err)
		})
		t.Run("should skip and not return error if one of the job is failed to be inserted to db", func(t *testing.T) {
			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			upstreamRepo := new(UpstreamRepository)
			defer upstreamRepo.AssertExpectations(t)

			downstreamRepo := new(DownstreamRepository)
			defer downstreamRepo.AssertExpectations(t)

			pluginService := new(PluginService)
			defer pluginService.AssertExpectations(t)

			upstreamResolver := new(UpstreamResolver)
			defer upstreamResolver.AssertExpectations(t)

			tenantDetailsGetter := new(TenantDetailsGetter)
			defer tenantDetailsGetter.AssertExpectations(t)

			jobDeploymentService := new(JobDeploymentService)
			defer jobDeploymentService.AssertExpectations(t)

			eventHandler := newEventHandler(t)

			specA, _ := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()
			specB, _ := job.NewSpecBuilder(jobVersion, "job-B", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()
			specs := []*job.Spec{specA, specB}

			tenantDetailsGetter.On("GetDetails", ctx, sampleTenant).Return(detailedTenant, nil)

			resourceA := job.ResourceURN("bigquery://project:dataset.tableA")
			var resourceB job.ResourceURN
			pluginService.On("ConstructDestinationURN", ctx, specA.Task().Name().String(), mock.Anything).Return(resourceA.String(), nil).Once()
			pluginService.On("ConstructDestinationURN", ctx, specB.Task().Name().String(), mock.Anything).Return(resourceB.String(), errors.New("some error")).Once()

			jobSourcesA := []job.ResourceURN{"bigquery://project:dataset.tableB"}
			pluginService.On("IdentifyUpstreams", ctx, specA.Task().Name().String(), mock.Anything, mock.Anything).Return(jobResourceURNsToString(jobSourcesA), nil)
			pluginService.On("IdentifyUpstreams", ctx, specB.Task().Name().String(), mock.Anything, mock.Anything).Return(nil, errors.New("another error"))

			jobB := job.NewJob(sampleTenant, specB, "", nil)
			savedJobs := []*job.Job{jobB}
			jobRepo.On("Add", ctx, mock.Anything).Return(savedJobs, errors.New("unable to save job A"))

			jobWithUpstreamB := job.NewWithUpstream(jobB, nil)
			upstreamResolver.On("BulkResolve", ctx, project.Name(), savedJobs, mock.Anything).Return([]*job.WithUpstream{jobWithUpstreamB}, nil, nil)

			upstreamRepo.On("ReplaceUpstreams", ctx, mock.Anything).Return(nil)

			jobNamesToUpload := []string{jobB.GetName()}
			jobDeploymentService.On("UploadJobs", ctx, sampleTenant, jobNamesToUpload, emptyJobNames).Return(nil)

			eventHandler.On("HandleEvent", mock.Anything).Times(1)

			jobService := service.NewJobService(jobRepo, upstreamRepo, downstreamRepo, pluginService, upstreamResolver, tenantDetailsGetter, eventHandler, log, jobDeploymentService, compiler.NewEngine())
			err := jobService.Add(ctx, sampleTenant, specs)
			assert.ErrorContains(t, err, "unable to save job A")
		})
		t.Run("return error when all jobs failed to be inserted to db", func(t *testing.T) {
			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			upstreamRepo := new(UpstreamRepository)
			defer upstreamRepo.AssertExpectations(t)

			downstreamRepo := new(DownstreamRepository)
			defer downstreamRepo.AssertExpectations(t)

			pluginService := new(PluginService)
			defer pluginService.AssertExpectations(t)

			upstreamResolver := new(UpstreamResolver)
			defer upstreamResolver.AssertExpectations(t)

			tenantDetailsGetter := new(TenantDetailsGetter)
			defer tenantDetailsGetter.AssertExpectations(t)

			specA, _ := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()
			specs := []*job.Spec{specA}

			tenantDetailsGetter.On("GetDetails", ctx, sampleTenant).Return(detailedTenant, nil)

			upstreamResolver.On("BulkResolve", ctx, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)

			upstreamRepo.On("ReplaceUpstreams", ctx, mock.Anything).Return(nil)

			resourceA := job.ResourceURN("bigquery://project:dataset.tableA")
			pluginService.On("ConstructDestinationURN", ctx, specA.Task().Name().String(), mock.Anything).Return(resourceA.String(), nil).Once()

			jobSourcesA := []job.ResourceURN{"bigquery://project:dataset.tableB"}
			pluginService.On("IdentifyUpstreams", ctx, specA.Task().Name().String(), mock.Anything, mock.Anything).Return(jobResourceURNsToString(jobSourcesA), nil)

			jobRepo.On("Add", ctx, mock.Anything).Return([]*job.Job{}, errors.New("unable to save job A"), errors.New("all jobs failed"))

			jobService := service.NewJobService(jobRepo, upstreamRepo, downstreamRepo, pluginService, upstreamResolver, tenantDetailsGetter, nil, log, nil, compiler.NewEngine())
			err := jobService.Add(ctx, sampleTenant, specs)
			assert.ErrorContains(t, err, "unable to save job A")
		})
		t.Run("should return error if failed to save upstream", func(t *testing.T) {
			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			upstreamRepo := new(UpstreamRepository)
			defer upstreamRepo.AssertExpectations(t)

			downstreamRepo := new(DownstreamRepository)
			defer downstreamRepo.AssertExpectations(t)

			pluginService := new(PluginService)
			defer pluginService.AssertExpectations(t)

			upstreamResolver := new(UpstreamResolver)
			defer upstreamResolver.AssertExpectations(t)

			tenantDetailsGetter := new(TenantDetailsGetter)
			defer tenantDetailsGetter.AssertExpectations(t)

			jobDeploymentService := new(JobDeploymentService)
			defer jobDeploymentService.AssertExpectations(t)

			eventHandler := newEventHandler(t)

			specA, _ := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()
			specs := []*job.Spec{specA}

			tenantDetailsGetter.On("GetDetails", ctx, sampleTenant).Return(detailedTenant, nil)

			resourceA := job.ResourceURN("bigquery://project:dataset.tableA")
			pluginService.On("ConstructDestinationURN", ctx, specA.Task().Name().String(), mock.Anything).Return(resourceA.String(), nil).Once()

			jobSourcesA := []job.ResourceURN{"bigquery://project:dataset.tableB"}
			pluginService.On("IdentifyUpstreams", ctx, specA.Task().Name().String(), mock.Anything, mock.Anything).Return(jobResourceURNsToString(jobSourcesA), nil)

			jobA := job.NewJob(sampleTenant, specA, resourceA, jobSourcesA)
			jobs := []*job.Job{jobA}

			jobRepo.On("Add", ctx, mock.Anything).Return(jobs, nil, nil)

			jobWithUpstreamA := job.NewWithUpstream(jobA, nil)
			upstreamResolver.On("BulkResolve", ctx, project.Name(), jobs, mock.Anything).Return([]*job.WithUpstream{jobWithUpstreamA}, nil, nil)

			upstreamRepo.On("ReplaceUpstreams", ctx, mock.Anything).Return(errors.New("internal error"))

			jobNamesToUpload := []string{jobA.GetName()}
			jobDeploymentService.On("UploadJobs", ctx, sampleTenant, jobNamesToUpload, emptyJobNames).Return(nil)

			eventHandler.On("HandleEvent", mock.Anything).Times(1)

			jobService := service.NewJobService(jobRepo, upstreamRepo, downstreamRepo, pluginService, upstreamResolver, tenantDetailsGetter, eventHandler, log, jobDeploymentService, compiler.NewEngine())
			err := jobService.Add(ctx, sampleTenant, specs)
			assert.Error(t, err)
		})
		t.Run("return error if encounter issue when uploading jobs", func(t *testing.T) {
			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			upstreamRepo := new(UpstreamRepository)
			defer upstreamRepo.AssertExpectations(t)

			downstreamRepo := new(DownstreamRepository)
			defer downstreamRepo.AssertExpectations(t)

			pluginService := new(PluginService)
			defer pluginService.AssertExpectations(t)

			upstreamResolver := new(UpstreamResolver)
			defer upstreamResolver.AssertExpectations(t)

			tenantDetailsGetter := new(TenantDetailsGetter)
			defer tenantDetailsGetter.AssertExpectations(t)

			jobDeploymentService := new(JobDeploymentService)
			defer jobDeploymentService.AssertExpectations(t)

			eventHandler := newEventHandler(t)

			specA, _ := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()
			specs := []*job.Spec{specA}

			tenantDetailsGetter.On("GetDetails", ctx, sampleTenant).Return(detailedTenant, nil)

			jobADestination := job.ResourceURN("bigquery://project:dataset.tableA")
			pluginService.On("ConstructDestinationURN", ctx, specA.Task().Name().String(), mock.Anything).Return(jobADestination.String(), nil)

			jobAUpstreamName := []job.ResourceURN{"bigquery://project:dataset.tableB"}
			pluginService.On("IdentifyUpstreams", ctx, specA.Task().Name().String(), mock.Anything, mock.Anything).Return(jobResourceURNsToString(jobAUpstreamName), nil)

			jobA := job.NewJob(sampleTenant, specA, jobADestination, jobAUpstreamName)
			jobs := []*job.Job{jobA}
			jobRepo.On("Add", ctx, mock.Anything).Return(jobs, nil, nil)

			upstream := job.NewUpstreamResolved("job-B", "", "bigquery://project:dataset.tableB", sampleTenant, "static", taskName, false)
			jobWithUpstream := job.NewWithUpstream(jobA, []*job.Upstream{upstream})
			upstreamResolver.On("BulkResolve", ctx, project.Name(), jobs, mock.Anything).Return([]*job.WithUpstream{jobWithUpstream}, nil, nil)

			upstreamRepo.On("ReplaceUpstreams", ctx, []*job.WithUpstream{jobWithUpstream}).Return(nil)

			errorMsg := "internal error"
			jobDeploymentService.On("UploadJobs", ctx, sampleTenant, mock.Anything, emptyJobNames).Return(errors.New(errorMsg))

			eventHandler.On("HandleEvent", mock.Anything).Times(1)

			jobService := service.NewJobService(jobRepo, upstreamRepo, downstreamRepo, pluginService, upstreamResolver, tenantDetailsGetter, eventHandler, log, jobDeploymentService, compiler.NewEngine())
			err := jobService.Add(ctx, sampleTenant, specs)
			assert.ErrorContains(t, err, errorMsg)
		})
	})
	t.Run("Update", func(t *testing.T) {
		t.Run("update jobs", func(t *testing.T) {
			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			upstreamRepo := new(UpstreamRepository)
			defer upstreamRepo.AssertExpectations(t)

			downstreamRepo := new(DownstreamRepository)
			defer downstreamRepo.AssertExpectations(t)

			pluginService := new(PluginService)
			defer pluginService.AssertExpectations(t)

			upstreamResolver := new(UpstreamResolver)
			defer upstreamResolver.AssertExpectations(t)

			tenantDetailsGetter := new(TenantDetailsGetter)
			defer tenantDetailsGetter.AssertExpectations(t)

			jobDeploymentService := new(JobDeploymentService)
			defer jobDeploymentService.AssertExpectations(t)

			eventHandler := newEventHandler(t)

			specA, _ := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()
			specs := []*job.Spec{specA}

			tenantDetailsGetter.On("GetDetails", ctx, sampleTenant).Return(detailedTenant, nil)

			jobADestination := job.ResourceURN("bigquery://project:dataset.tableA")
			pluginService.On("ConstructDestinationURN", ctx, specA.Task().Name().String(), mock.Anything).Return(jobADestination.String(), nil)

			jobAUpstreamName := []job.ResourceURN{"bigquery://project:dataset.tableB"}
			pluginService.On("IdentifyUpstreams", ctx, specA.Task().Name().String(), mock.Anything, mock.Anything).Return(jobResourceURNsToString(jobAUpstreamName), nil)

			jobA := job.NewJob(sampleTenant, specA, jobADestination, jobAUpstreamName)
			jobs := []*job.Job{jobA}
			jobRepo.On("Update", ctx, mock.Anything).Return(jobs, nil, nil)

			upstream := job.NewUpstreamResolved("job-B", "", "bigquery://project:dataset.tableB", sampleTenant, "static", taskName, false)
			jobWithUpstream := job.NewWithUpstream(jobA, []*job.Upstream{upstream})
			upstreamResolver.On("BulkResolve", ctx, project.Name(), jobs, mock.Anything).Return([]*job.WithUpstream{jobWithUpstream}, nil, nil)

			upstreamRepo.On("ReplaceUpstreams", ctx, []*job.WithUpstream{jobWithUpstream}).Return(nil)

			jobNamesToUpload := []string{jobA.GetName()}
			jobDeploymentService.On("UploadJobs", ctx, sampleTenant, jobNamesToUpload, emptyJobNames).Return(nil)

			eventHandler.On("HandleEvent", mock.Anything).Times(1)

			jobService := service.NewJobService(jobRepo, upstreamRepo, downstreamRepo, pluginService, upstreamResolver, tenantDetailsGetter, eventHandler, log, jobDeploymentService, compiler.NewEngine())
			err := jobService.Update(ctx, sampleTenant, specs)
			assert.NoError(t, err)
		})
		t.Run("return error if unable to get detailed tenant", func(t *testing.T) {
			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			upstreamRepo := new(UpstreamRepository)
			defer upstreamRepo.AssertExpectations(t)

			downstreamRepo := new(DownstreamRepository)
			defer downstreamRepo.AssertExpectations(t)

			pluginService := new(PluginService)
			defer pluginService.AssertExpectations(t)

			upstreamResolver := new(UpstreamResolver)
			defer upstreamResolver.AssertExpectations(t)

			tenantDetailsGetter := new(TenantDetailsGetter)
			defer tenantDetailsGetter.AssertExpectations(t)

			specA, _ := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()
			specs := []*job.Spec{specA}

			tenantDetailsGetter.On("GetDetails", ctx, sampleTenant).Return(&tenant.WithDetails{}, errors.New("internal error"))

			jobService := service.NewJobService(jobRepo, upstreamRepo, downstreamRepo, pluginService, upstreamResolver, tenantDetailsGetter, nil, log, nil, compiler.NewEngine())
			err := jobService.Update(ctx, sampleTenant, specs)
			assert.ErrorContains(t, err, "internal error")
		})
		t.Run("skip job that has issue when generating destination and return error", func(t *testing.T) {
			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			upstreamRepo := new(UpstreamRepository)
			defer upstreamRepo.AssertExpectations(t)

			downstreamRepo := new(DownstreamRepository)
			defer downstreamRepo.AssertExpectations(t)

			pluginService := new(PluginService)
			defer pluginService.AssertExpectations(t)

			upstreamResolver := new(UpstreamResolver)
			defer upstreamResolver.AssertExpectations(t)

			tenantDetailsGetter := new(TenantDetailsGetter)
			defer tenantDetailsGetter.AssertExpectations(t)

			jobDeploymentService := new(JobDeploymentService)
			defer jobDeploymentService.AssertExpectations(t)

			eventHandler := newEventHandler(t)

			specA, _ := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()
			specB, _ := job.NewSpecBuilder(jobVersion, "job-B", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()
			specC, _ := job.NewSpecBuilder(jobVersion, "job-C", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()
			specs := []*job.Spec{specB, specA, specC}

			tenantDetailsGetter.On("GetDetails", ctx, sampleTenant).Return(detailedTenant, nil)

			jobADestination := job.ResourceURN("bigquery://project:dataset.tableA")
			jobBDestination := job.ResourceURN("bigquery://project:dataset.tableB")
			var jobDestination job.ResourceURN
			pluginService.On("ConstructDestinationURN", ctx, specB.Task().Name().String(), mock.Anything).Return(jobBDestination.String(), nil).Once()
			pluginService.On("ConstructDestinationURN", ctx, specA.Task().Name().String(), mock.Anything).Return(jobADestination.String(), nil).Once()
			pluginService.On("ConstructDestinationURN", ctx, specC.Task().Name().String(), mock.Anything).Return(jobDestination.String(), errors.New("generate destination error")).Once()

			jobAUpstreamName := []job.ResourceURN{"bigquery://project:dataset.tableB"}
			pluginService.On("IdentifyUpstreams", ctx, specA.Task().Name().String(), mock.Anything, mock.Anything).Return([]string{}, errors.New("generate upstream error"))
			pluginService.On("IdentifyUpstreams", ctx, specB.Task().Name().String(), mock.Anything, mock.Anything).Return(jobResourceURNsToString(jobAUpstreamName), nil)

			jobA := job.NewJob(sampleTenant, specA, jobADestination, jobAUpstreamName)
			jobs := []*job.Job{jobA}
			jobRepo.On("Update", ctx, mock.Anything).Return(jobs, nil, nil)

			upstream := job.NewUpstreamResolved("job-B", "", "bigquery://project:dataset.tableB", sampleTenant, "static", taskName, false)
			jobWithUpstream := job.NewWithUpstream(jobA, []*job.Upstream{upstream})
			upstreamResolver.On("BulkResolve", ctx, project.Name(), jobs, mock.Anything).Return([]*job.WithUpstream{jobWithUpstream}, nil, nil)

			upstreamRepo.On("ReplaceUpstreams", ctx, []*job.WithUpstream{jobWithUpstream}).Return(nil)

			jobNamesToUpload := []string{jobA.GetName()}
			jobDeploymentService.On("UploadJobs", ctx, sampleTenant, jobNamesToUpload, emptyJobNames).Return(nil)

			eventHandler.On("HandleEvent", mock.Anything).Times(1)

			jobService := service.NewJobService(jobRepo, upstreamRepo, downstreamRepo, pluginService, upstreamResolver, tenantDetailsGetter, eventHandler, log, jobDeploymentService, compiler.NewEngine())
			err := jobService.Update(ctx, sampleTenant, specs)
			assert.ErrorContains(t, err, "generate upstream error")
		})
		t.Run("return error when all jobs failed to have destination and upstream generated", func(t *testing.T) {
			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			upstreamRepo := new(UpstreamRepository)
			defer upstreamRepo.AssertExpectations(t)

			downstreamRepo := new(DownstreamRepository)
			defer downstreamRepo.AssertExpectations(t)

			pluginService := new(PluginService)
			defer pluginService.AssertExpectations(t)

			upstreamResolver := new(UpstreamResolver)
			defer upstreamResolver.AssertExpectations(t)

			tenantDetailsGetter := new(TenantDetailsGetter)
			defer tenantDetailsGetter.AssertExpectations(t)

			specA, _ := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()
			specB, _ := job.NewSpecBuilder(jobVersion, "job-B", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()
			specs := []*job.Spec{specB, specA}

			jobRepo.On("Update", ctx, mock.Anything).Return(nil, nil)

			upstreamResolver.On("BulkResolve", ctx, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)

			upstreamRepo.On("ReplaceUpstreams", ctx, mock.Anything).Return(nil)

			tenantDetailsGetter.On("GetDetails", ctx, sampleTenant).Return(detailedTenant, nil)

			var jobADestination job.ResourceURN
			jobBDestination := job.ResourceURN("bigquery://project:dataset.tableB")
			pluginService.On("ConstructDestinationURN", ctx, specB.Task().Name().String(), mock.Anything).Return(jobBDestination.String(), nil).Once()
			pluginService.On("ConstructDestinationURN", ctx, specA.Task().Name().String(), mock.Anything).Return(jobADestination.String(), errors.New("generate destination error")).Once()

			pluginService.On("IdentifyUpstreams", ctx, specB.Task().Name().String(), mock.Anything, mock.Anything).Return(nil, errors.New("generate upstream error"))

			jobService := service.NewJobService(jobRepo, upstreamRepo, downstreamRepo, pluginService, upstreamResolver, tenantDetailsGetter, nil, log, nil, compiler.NewEngine())
			err := jobService.Update(ctx, sampleTenant, specs)
			assert.ErrorContains(t, err, "generate upstream error")
		})
		t.Run("should not skip nor return error if job is not bq2bq", func(t *testing.T) {
			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			upstreamRepo := new(UpstreamRepository)
			defer upstreamRepo.AssertExpectations(t)

			downstreamRepo := new(DownstreamRepository)
			defer downstreamRepo.AssertExpectations(t)

			pluginService := new(PluginService)
			defer pluginService.AssertExpectations(t)

			upstreamResolver := new(UpstreamResolver)
			defer upstreamResolver.AssertExpectations(t)

			tenantDetailsGetter := new(TenantDetailsGetter)
			defer tenantDetailsGetter.AssertExpectations(t)

			jobDeploymentService := new(JobDeploymentService)
			defer jobDeploymentService.AssertExpectations(t)

			eventHandler := newEventHandler(t)

			nonBq2bqTask := job.NewTask("another", nil)
			specA, _ := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, jobWindow, nonBq2bqTask).WithAsset(jobAsset).Build()
			specs := []*job.Spec{specA}

			tenantDetailsGetter.On("GetDetails", ctx, sampleTenant).Return(detailedTenant, nil)

			jobA := job.NewJob(sampleTenant, specA, "", nil)
			jobs := []*job.Job{jobA}

			jobRepo.On("Update", ctx, mock.Anything).Return(jobs, nil, nil)

			pluginService.On("ConstructDestinationURN", ctx, specA.Task().Name().String(), mock.Anything).Return("", nil)
			pluginService.On("IdentifyUpstreams", ctx, specA.Task().Name().String(), mock.Anything, mock.Anything).Return([]string{}, nil)
			jobWithUpstream := job.NewWithUpstream(jobA, nil)
			upstreamResolver.On("BulkResolve", ctx, project.Name(), jobs, mock.Anything).Return([]*job.WithUpstream{jobWithUpstream}, nil, nil)

			upstreamRepo.On("ReplaceUpstreams", ctx, []*job.WithUpstream{jobWithUpstream}).Return(nil)

			jobNamesToUpload := []string{jobA.GetName()}
			jobDeploymentService.On("UploadJobs", ctx, sampleTenant, jobNamesToUpload, emptyJobNames).Return(nil)

			eventHandler.On("HandleEvent", mock.Anything).Times(1)

			jobService := service.NewJobService(jobRepo, upstreamRepo, downstreamRepo, pluginService, upstreamResolver, tenantDetailsGetter, eventHandler, log, jobDeploymentService, compiler.NewEngine())
			err := jobService.Update(ctx, sampleTenant, specs)
			assert.NoError(t, err)
		})
		t.Run("should skip and not return error if one of the job is failed to be updated to db", func(t *testing.T) {
			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			upstreamRepo := new(UpstreamRepository)
			defer upstreamRepo.AssertExpectations(t)

			downstreamRepo := new(DownstreamRepository)
			defer downstreamRepo.AssertExpectations(t)

			pluginService := new(PluginService)
			defer pluginService.AssertExpectations(t)

			upstreamResolver := new(UpstreamResolver)
			defer upstreamResolver.AssertExpectations(t)

			tenantDetailsGetter := new(TenantDetailsGetter)
			defer tenantDetailsGetter.AssertExpectations(t)

			jobDeploymentService := new(JobDeploymentService)
			defer jobDeploymentService.AssertExpectations(t)

			eventHandler := newEventHandler(t)

			specA, _ := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()
			specB, _ := job.NewSpecBuilder(jobVersion, "job-B", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()
			specs := []*job.Spec{specA, specB}

			tenantDetailsGetter.On("GetDetails", ctx, sampleTenant).Return(detailedTenant, nil)

			resourceA := job.ResourceURN("bigquery://project:dataset.tableA")
			var jobDestination job.ResourceURN
			pluginService.On("ConstructDestinationURN", ctx, specA.Task().Name().String(), mock.Anything).Return(resourceA.String(), nil).Once()
			pluginService.On("ConstructDestinationURN", ctx, specB.Task().Name().String(), mock.Anything).Return(jobDestination.String(), errors.New("something error")).Once()

			jobSourcesA := []job.ResourceURN{"bigquery://project:dataset.tableB"}
			pluginService.On("IdentifyUpstreams", ctx, specA.Task().Name().String(), mock.Anything, mock.Anything).Return(jobResourceURNsToString(jobSourcesA), nil)

			jobB := job.NewJob(sampleTenant, specB, "", nil)
			savedJobs := []*job.Job{jobB}
			jobRepo.On("Update", ctx, mock.Anything).Return(savedJobs, errors.New("unable to save job A"), nil)

			jobWithUpstreamB := job.NewWithUpstream(jobB, nil)
			upstreamResolver.On("BulkResolve", ctx, project.Name(), savedJobs, mock.Anything).Return([]*job.WithUpstream{jobWithUpstreamB}, nil, nil)

			upstreamRepo.On("ReplaceUpstreams", ctx, mock.Anything).Return(nil)

			jobNamesToUpload := []string{jobB.GetName()}
			jobDeploymentService.On("UploadJobs", ctx, sampleTenant, jobNamesToUpload, emptyJobNames).Return(nil)

			eventHandler.On("HandleEvent", mock.Anything).Times(1)

			jobService := service.NewJobService(jobRepo, upstreamRepo, downstreamRepo, pluginService, upstreamResolver, tenantDetailsGetter, eventHandler, log, jobDeploymentService, compiler.NewEngine())
			err := jobService.Update(ctx, sampleTenant, specs)
			assert.ErrorContains(t, err, "unable to save job A")
		})
		t.Run("return error when all jobs failed to be updated to db", func(t *testing.T) {
			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			upstreamRepo := new(UpstreamRepository)
			defer upstreamRepo.AssertExpectations(t)

			downstreamRepo := new(DownstreamRepository)
			defer downstreamRepo.AssertExpectations(t)

			pluginService := new(PluginService)
			defer pluginService.AssertExpectations(t)

			upstreamResolver := new(UpstreamResolver)
			defer upstreamResolver.AssertExpectations(t)

			tenantDetailsGetter := new(TenantDetailsGetter)
			defer tenantDetailsGetter.AssertExpectations(t)

			specA, _ := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()
			specs := []*job.Spec{specA}

			tenantDetailsGetter.On("GetDetails", ctx, sampleTenant).Return(detailedTenant, nil)

			upstreamResolver.On("BulkResolve", ctx, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)

			upstreamRepo.On("ReplaceUpstreams", ctx, mock.Anything).Return(nil)

			resourceA := job.ResourceURN("bigquery://project:dataset.tableA")
			pluginService.On("ConstructDestinationURN", ctx, specA.Task().Name().String(), mock.Anything).Return(resourceA.String(), nil).Once()

			jobSourcesA := []job.ResourceURN{"bigquery://project:dataset.tableB"}
			pluginService.On("IdentifyUpstreams", ctx, specA.Task().Name().String(), mock.Anything, mock.Anything).Return(jobResourceURNsToString(jobSourcesA), nil)

			jobRepo.On("Update", ctx, mock.Anything).Return([]*job.Job{}, errors.New("unable to update job A"), errors.New("all jobs failed"))

			jobService := service.NewJobService(jobRepo, upstreamRepo, downstreamRepo, pluginService, upstreamResolver, tenantDetailsGetter, nil, log, nil, compiler.NewEngine())
			err := jobService.Update(ctx, sampleTenant, specs)
			assert.ErrorContains(t, err, "unable to update job A")
		})
		t.Run("should return error if failed to save upstream", func(t *testing.T) {
			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			upstreamRepo := new(UpstreamRepository)
			defer upstreamRepo.AssertExpectations(t)

			downstreamRepo := new(DownstreamRepository)
			defer downstreamRepo.AssertExpectations(t)

			pluginService := new(PluginService)
			defer pluginService.AssertExpectations(t)

			upstreamResolver := new(UpstreamResolver)
			defer upstreamResolver.AssertExpectations(t)

			tenantDetailsGetter := new(TenantDetailsGetter)
			defer tenantDetailsGetter.AssertExpectations(t)

			jobDeploymentService := new(JobDeploymentService)
			defer jobDeploymentService.AssertExpectations(t)

			eventHandler := newEventHandler(t)

			specA, _ := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()
			specs := []*job.Spec{specA}

			tenantDetailsGetter.On("GetDetails", ctx, sampleTenant).Return(detailedTenant, nil)

			resourceA := job.ResourceURN("bigquery://project:dataset.tableA")
			pluginService.On("ConstructDestinationURN", ctx, specA.Task().Name().String(), mock.Anything).Return(resourceA.String(), nil).Once()

			jobSourcesA := []job.ResourceURN{"bigquery://project:dataset.tableB"}
			pluginService.On("IdentifyUpstreams", ctx, specA.Task().Name().String(), mock.Anything, mock.Anything).Return(jobResourceURNsToString(jobSourcesA), nil)

			jobA := job.NewJob(sampleTenant, specA, resourceA, jobSourcesA)
			jobs := []*job.Job{jobA}
			jobRepo.On("Update", ctx, mock.Anything).Return(jobs, nil, nil)

			jobWithUpstreamA := job.NewWithUpstream(jobA, nil)
			upstreamResolver.On("BulkResolve", ctx, project.Name(), jobs, mock.Anything).Return([]*job.WithUpstream{jobWithUpstreamA}, nil, nil)

			upstreamRepo.On("ReplaceUpstreams", ctx, mock.Anything).Return(errors.New("internal error"))

			jobNamesToUpload := []string{jobA.GetName()}
			jobDeploymentService.On("UploadJobs", ctx, sampleTenant, jobNamesToUpload, emptyJobNames).Return(nil)

			eventHandler.On("HandleEvent", mock.Anything).Times(1)

			jobService := service.NewJobService(jobRepo, upstreamRepo, downstreamRepo, pluginService, upstreamResolver, tenantDetailsGetter, eventHandler, log, jobDeploymentService, compiler.NewEngine())
			err := jobService.Update(ctx, sampleTenant, specs)
			assert.Error(t, err)
		})
		t.Run("return error if encounter issue when uploading jobs", func(t *testing.T) {
			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			upstreamRepo := new(UpstreamRepository)
			defer upstreamRepo.AssertExpectations(t)

			downstreamRepo := new(DownstreamRepository)
			defer downstreamRepo.AssertExpectations(t)

			pluginService := new(PluginService)
			defer pluginService.AssertExpectations(t)

			upstreamResolver := new(UpstreamResolver)
			defer upstreamResolver.AssertExpectations(t)

			tenantDetailsGetter := new(TenantDetailsGetter)
			defer tenantDetailsGetter.AssertExpectations(t)

			jobDeploymentService := new(JobDeploymentService)
			defer jobDeploymentService.AssertExpectations(t)

			eventHandler := newEventHandler(t)

			specA, _ := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()
			specs := []*job.Spec{specA}

			tenantDetailsGetter.On("GetDetails", ctx, sampleTenant).Return(detailedTenant, nil)

			jobADestination := job.ResourceURN("bigquery://project:dataset.tableA")
			pluginService.On("ConstructDestinationURN", ctx, specA.Task().Name().String(), mock.Anything).Return(jobADestination.String(), nil)

			jobAUpstreamName := []job.ResourceURN{"bigquery://project:dataset.tableB"}
			pluginService.On("IdentifyUpstreams", ctx, specA.Task().Name().String(), mock.Anything, mock.Anything).Return(jobResourceURNsToString(jobAUpstreamName), nil)

			jobA := job.NewJob(sampleTenant, specA, jobADestination, jobAUpstreamName)
			jobs := []*job.Job{jobA}
			jobRepo.On("Update", ctx, mock.Anything).Return(jobs, nil, nil)

			upstream := job.NewUpstreamResolved("job-B", "", "bigquery://project:dataset.tableB", sampleTenant, "static", taskName, false)
			jobWithUpstream := job.NewWithUpstream(jobA, []*job.Upstream{upstream})
			upstreamResolver.On("BulkResolve", ctx, project.Name(), jobs, mock.Anything).Return([]*job.WithUpstream{jobWithUpstream}, nil, nil)

			upstreamRepo.On("ReplaceUpstreams", ctx, []*job.WithUpstream{jobWithUpstream}).Return(nil)

			errorMsg := "internal error"
			jobDeploymentService.On("UploadJobs", ctx, sampleTenant, mock.Anything, emptyJobNames).Return(errors.New(errorMsg))

			eventHandler.On("HandleEvent", mock.Anything).Times(1)

			jobService := service.NewJobService(jobRepo, upstreamRepo, downstreamRepo, pluginService, upstreamResolver, tenantDetailsGetter, eventHandler, log, jobDeploymentService, compiler.NewEngine())
			err := jobService.Update(ctx, sampleTenant, specs)
			assert.ErrorContains(t, err, errorMsg)
		})
	})

	t.Run("ChangeNamespace", func(t *testing.T) {
		newNamespaceName := "newNamespace"
		newTenant, _ := tenant.NewTenant(project.Name().String(), newNamespaceName)
		specA, _ := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()
		jobA := job.NewJob(newTenant, specA, "table-A", []job.ResourceURN{"bigquery://project:dataset.tableB"})
		t.Run("should fail if error in repo", func(t *testing.T) {
			jobRepo := new(JobRepository)
			jobRepo.On("ChangeJobNamespace", ctx, specA.Name(), sampleTenant, newTenant).Return(errors.New("error in transaction"))
			defer jobRepo.AssertExpectations(t)

			jobService := service.NewJobService(jobRepo, nil, nil, nil, nil, nil, nil, nil, nil, nil)
			err := jobService.ChangeNamespace(ctx, sampleTenant, newTenant, specA.Name())
			assert.ErrorContains(t, err, "error in transaction")
		})

		t.Run("should fail if error getting newly created job", func(t *testing.T) {
			jobRepo := new(JobRepository)
			jobRepo.On("ChangeJobNamespace", ctx, specA.Name(), sampleTenant, newTenant).Return(nil)
			jobRepo.On("GetByJobName", ctx, project.Name(), specA.Name()).Return(nil, errors.New("error in fetching job from DB"))
			defer jobRepo.AssertExpectations(t)

			jobService := service.NewJobService(jobRepo, nil, nil, nil, nil, nil, nil, nil, nil, nil)
			err := jobService.ChangeNamespace(ctx, sampleTenant, newTenant, specA.Name())
			assert.ErrorContains(t, err, "error in fetching job from DB")
		})

		t.Run("should fail if error in upload job", func(t *testing.T) {
			jobRepo := new(JobRepository)
			jobRepo.On("ChangeJobNamespace", ctx, specA.Name(), sampleTenant, newTenant).Return(nil)
			jobRepo.On("GetByJobName", ctx, project.Name(), specA.Name()).Return(jobA, nil)
			defer jobRepo.AssertExpectations(t)

			jobDeploymentService := new(JobDeploymentService)
			jobModified := []string{specA.Name().String()}
			jobDeploymentService.On("UploadJobs", ctx, sampleTenant, emptyJobNames, jobModified).Return(errors.New("error in upload jobs"))
			defer jobDeploymentService.AssertExpectations(t)

			jobService := service.NewJobService(jobRepo, nil, nil, nil, nil, nil, nil, nil, jobDeploymentService, nil)
			err := jobService.ChangeNamespace(ctx, sampleTenant, newTenant, specA.Name())
			assert.ErrorContains(t, err, "error in upload jobs")
		})

		t.Run("should fail if error in upload new job", func(t *testing.T) {
			jobRepo := new(JobRepository)
			jobRepo.On("ChangeJobNamespace", ctx, specA.Name(), sampleTenant, newTenant).Return(nil)
			jobRepo.On("GetByJobName", ctx, project.Name(), specA.Name()).Return(jobA, nil)
			defer jobRepo.AssertExpectations(t)

			jobDeploymentService := new(JobDeploymentService)
			jobModified := []string{specA.Name().String()}
			jobDeploymentService.On("UploadJobs", ctx, sampleTenant, emptyJobNames, jobModified).Return(nil)
			jobDeploymentService.On("UploadJobs", ctx, newTenant, jobModified, emptyJobNames).Return(errors.New("error in upload new job"))
			defer jobDeploymentService.AssertExpectations(t)

			jobService := service.NewJobService(jobRepo, nil, nil, nil, nil, nil, nil, nil, jobDeploymentService, nil)

			err := jobService.ChangeNamespace(ctx, sampleTenant, newTenant, specA.Name())
			assert.ErrorContains(t, err, "error in upload new job")
		})

		t.Run("successfully", func(t *testing.T) {
			jobRepo := new(JobRepository)
			jobRepo.On("ChangeJobNamespace", ctx, specA.Name(), sampleTenant, newTenant).Return(nil)
			jobRepo.On("GetByJobName", ctx, project.Name(), specA.Name()).Return(jobA, nil)
			defer jobRepo.AssertExpectations(t)

			jobDeploymentService := new(JobDeploymentService)
			jobModified := []string{specA.Name().String()}
			jobDeploymentService.On("UploadJobs", ctx, sampleTenant, emptyJobNames, jobModified).Return(nil)
			jobDeploymentService.On("UploadJobs", ctx, newTenant, jobModified, emptyJobNames).Return(nil)
			defer jobDeploymentService.AssertExpectations(t)

			eventHandler := newEventHandler(t)
			eventHandler.On("HandleEvent", mock.Anything).Times(1)
			defer eventHandler.AssertExpectations(t)

			jobService := service.NewJobService(jobRepo, nil, nil, nil, nil, nil, eventHandler, nil, jobDeploymentService, nil)
			err := jobService.ChangeNamespace(ctx, sampleTenant, newTenant, specA.Name())
			assert.NoError(t, err)
		})
	})

	t.Run("Delete", func(t *testing.T) {
		t.Run("deletes job without downstream", func(t *testing.T) {
			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			upstreamRepo := new(UpstreamRepository)
			defer upstreamRepo.AssertExpectations(t)

			downstreamRepo := new(DownstreamRepository)
			defer downstreamRepo.AssertExpectations(t)

			jobDeploymentService := new(JobDeploymentService)
			defer jobDeploymentService.AssertExpectations(t)

			eventHandler := newEventHandler(t)

			specA, _ := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()

			downstreamRepo.On("GetDownstreamByJobName", ctx, project.Name(), specA.Name()).Return(nil, nil)
			jobRepo.On("Delete", ctx, project.Name(), specA.Name(), false).Return(nil)

			jobNamesToRemove := []string{specA.Name().String()}
			jobDeploymentService.On("UploadJobs", ctx, sampleTenant, emptyJobNames, jobNamesToRemove).Return(nil)

			eventHandler.On("HandleEvent", mock.Anything).Times(1)

			jobService := service.NewJobService(jobRepo, upstreamRepo, downstreamRepo, nil, nil, nil, eventHandler, log, jobDeploymentService, nil)
			affectedDownstream, err := jobService.Delete(ctx, sampleTenant, specA.Name(), false, false)
			assert.NoError(t, err)
			assert.Empty(t, affectedDownstream)
		})

		t.Run("deletes job with downstream if it is a force delete", func(t *testing.T) {
			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			upstreamRepo := new(UpstreamRepository)
			defer upstreamRepo.AssertExpectations(t)

			downstreamRepo := new(DownstreamRepository)
			defer downstreamRepo.AssertExpectations(t)

			jobDeploymentService := new(JobDeploymentService)
			defer jobDeploymentService.AssertExpectations(t)

			eventHandler := newEventHandler(t)

			specA, _ := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()

			downstreamFullNames := []job.FullName{"test-proj/job-B", "test-proj/job-C"}
			downstreamList := []*job.Downstream{
				job.NewDownstream("job-B", project.Name(), namespace.Name(), taskName),
				job.NewDownstream("job-C", project.Name(), namespace.Name(), taskName),
			}

			downstreamRepo.On("GetDownstreamByJobName", ctx, project.Name(), specA.Name()).Return(downstreamList, nil)
			jobRepo.On("Delete", ctx, project.Name(), specA.Name(), false).Return(nil)

			jobNamesToRemove := []string{specA.Name().String()}
			jobDeploymentService.On("UploadJobs", ctx, sampleTenant, emptyJobNames, jobNamesToRemove).Return(nil)

			eventHandler.On("HandleEvent", mock.Anything).Times(1)
			jobService := service.NewJobService(jobRepo, upstreamRepo, downstreamRepo, nil, nil, nil, eventHandler, log, jobDeploymentService, nil)

			affectedDownstream, err := jobService.Delete(ctx, sampleTenant, specA.Name(), false, true)
			assert.NoError(t, err)
			assert.EqualValues(t, downstreamFullNames, affectedDownstream)
		})
		t.Run("not delete the job if it has downstream and not a force delete", func(t *testing.T) {
			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			upstreamRepo := new(UpstreamRepository)
			defer upstreamRepo.AssertExpectations(t)

			downstreamRepo := new(DownstreamRepository)
			defer downstreamRepo.AssertExpectations(t)

			specA, _ := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()

			downstreamList := []*job.Downstream{
				job.NewDownstream("job-B", project.Name(), namespace.Name(), taskName),
				job.NewDownstream("job-C", project.Name(), namespace.Name(), taskName),
			}
			downstreamRepo.On("GetDownstreamByJobName", ctx, project.Name(), specA.Name()).Return(downstreamList, nil)

			jobService := service.NewJobService(jobRepo, upstreamRepo, downstreamRepo, nil, nil, nil, nil, log, nil, nil)
			affectedDownstream, err := jobService.Delete(ctx, sampleTenant, specA.Name(), false, false)
			assert.Error(t, err)
			assert.Empty(t, affectedDownstream)
		})
		t.Run("returns error if unable to get downstream", func(t *testing.T) {
			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			upstreamRepo := new(UpstreamRepository)
			defer upstreamRepo.AssertExpectations(t)

			downstreamRepo := new(DownstreamRepository)
			defer downstreamRepo.AssertExpectations(t)

			specA, _ := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()

			downstreamRepo.On("GetDownstreamByJobName", ctx, project.Name(), specA.Name()).Return(nil, errors.New("internal error"))

			jobService := service.NewJobService(jobRepo, upstreamRepo, downstreamRepo, nil, nil, nil, nil, log, nil, nil)

			affectedDownstream, err := jobService.Delete(ctx, sampleTenant, specA.Name(), false, false)
			assert.Error(t, err)
			assert.Empty(t, affectedDownstream)
		})
		t.Run("returns error if unable to delete job", func(t *testing.T) {
			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			upstreamRepo := new(UpstreamRepository)
			defer upstreamRepo.AssertExpectations(t)

			downstreamRepo := new(DownstreamRepository)
			defer downstreamRepo.AssertExpectations(t)

			specA, _ := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()

			downstreamRepo.On("GetDownstreamByJobName", ctx, project.Name(), specA.Name()).Return(nil, nil)
			jobRepo.On("Delete", ctx, project.Name(), specA.Name(), false).Return(errors.New("internal error"))

			jobService := service.NewJobService(jobRepo, upstreamRepo, downstreamRepo, nil, nil, nil, nil, log, nil, nil)
			affectedDownstream, err := jobService.Delete(ctx, sampleTenant, specA.Name(), false, false)
			assert.Error(t, err)
			assert.Empty(t, affectedDownstream)
		})
		t.Run("return error if encounter issue when removing jobs from scheduler", func(t *testing.T) {
			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			upstreamRepo := new(UpstreamRepository)
			defer upstreamRepo.AssertExpectations(t)

			downstreamRepo := new(DownstreamRepository)
			defer downstreamRepo.AssertExpectations(t)

			jobDeploymentService := new(JobDeploymentService)
			defer jobDeploymentService.AssertExpectations(t)

			eventHandler := newEventHandler(t)

			specA, _ := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()

			downstreamRepo.On("GetDownstreamByJobName", ctx, project.Name(), specA.Name()).Return(nil, nil)
			jobRepo.On("Delete", ctx, project.Name(), specA.Name(), false).Return(nil)

			errorMsg := "internal error"
			jobDeploymentService.On("UploadJobs", ctx, sampleTenant, emptyJobNames, mock.Anything).Return(errors.New(errorMsg))

			jobService := service.NewJobService(jobRepo, upstreamRepo, downstreamRepo, nil, nil, nil, eventHandler, log, jobDeploymentService, nil)
			affectedDownstream, err := jobService.Delete(ctx, sampleTenant, specA.Name(), false, false)
			assert.ErrorContains(t, err, errorMsg)
			assert.Empty(t, affectedDownstream)
		})
	})
	t.Run("ReplaceAll", func(t *testing.T) {
		t.Run("adds new jobs that does not exist yet", func(t *testing.T) {
			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			upstreamRepo := new(UpstreamRepository)
			defer upstreamRepo.AssertExpectations(t)

			downstreamRepo := new(DownstreamRepository)
			defer downstreamRepo.AssertExpectations(t)

			pluginService := new(PluginService)
			defer pluginService.AssertExpectations(t)

			upstreamResolver := new(UpstreamResolver)
			defer upstreamResolver.AssertExpectations(t)

			tenantDetailsGetter := new(TenantDetailsGetter)
			defer tenantDetailsGetter.AssertExpectations(t)

			logWriter := new(mockWriter)
			defer logWriter.AssertExpectations(t)

			jobDeploymentService := new(JobDeploymentService)
			defer jobDeploymentService.AssertExpectations(t)

			eventHandler := newEventHandler(t)

			tenantDetailsGetter.On("GetDetails", ctx, sampleTenant).Return(detailedTenant, nil)

			specA, _ := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()
			jobADestination := job.ResourceURN("bigquery://project:dataset.tableA")
			jobAUpstreamName := []job.ResourceURN{"bigquery://project:dataset.tableB"}
			jobA := job.NewJob(sampleTenant, specA, jobADestination, jobAUpstreamName)

			specB, _ := job.NewSpecBuilder(jobVersion, "job-B", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()
			jobB := job.NewJob(sampleTenant, specB, "", nil)

			incomingSpecs := []*job.Spec{specA, specB}

			existingJobs := []*job.Job{jobB}

			jobRepo.On("GetAllByTenant", ctx, sampleTenant).Return(existingJobs, nil)

			pluginService.On("ConstructDestinationURN", ctx, specA.Task().Name().String(), mock.Anything).Return(jobADestination.String(), nil).Once()
			pluginService.On("IdentifyUpstreams", ctx, specA.Task().Name().String(), mock.Anything, mock.Anything).Return(jobResourceURNsToString(jobAUpstreamName), nil)

			jobRepo.On("Add", ctx, mock.Anything).Return([]*job.Job{jobA}, nil)

			upstream := job.NewUpstreamResolved("job-B", "", "bigquery://project:dataset.tableB", sampleTenant, "static", taskName, false)

			jobWithUpstream := job.NewWithUpstream(jobA, []*job.Upstream{upstream})
			upstreamResolver.On("BulkResolve", ctx, project.Name(), []*job.Job{jobA}, mock.Anything).Return([]*job.WithUpstream{jobWithUpstream}, nil, nil)

			upstreamRepo.On("ReplaceUpstreams", ctx, []*job.WithUpstream{jobWithUpstream}).Return(nil)

			logWriter.On("Write", mock.Anything, mock.Anything).Return(nil)
			eventHandler.On("HandleEvent", mock.Anything).Times(1)

			jobNamesToUpload := []string{jobA.GetName()}
			var jobNamesToRemove []string
			jobDeploymentService.On("UploadJobs", ctx, sampleTenant, jobNamesToUpload, jobNamesToRemove).Return(nil)

			jobService := service.NewJobService(jobRepo, upstreamRepo, downstreamRepo, pluginService, upstreamResolver, tenantDetailsGetter, eventHandler, log, jobDeploymentService, compiler.NewEngine())
			err := jobService.ReplaceAll(ctx, sampleTenant, incomingSpecs, jobNamesWithInvalidSpec, logWriter)
			assert.NoError(t, err)
		})
		t.Run("updates modified existing jobs", func(t *testing.T) {
			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			upstreamRepo := new(UpstreamRepository)
			defer upstreamRepo.AssertExpectations(t)

			downstreamRepo := new(DownstreamRepository)
			defer downstreamRepo.AssertExpectations(t)

			pluginService := new(PluginService)
			defer pluginService.AssertExpectations(t)

			upstreamResolver := new(UpstreamResolver)
			defer upstreamResolver.AssertExpectations(t)

			tenantDetailsGetter := new(TenantDetailsGetter)
			defer tenantDetailsGetter.AssertExpectations(t)

			logWriter := new(mockWriter)
			defer logWriter.AssertExpectations(t)

			jobDeploymentService := new(JobDeploymentService)
			defer jobDeploymentService.AssertExpectations(t)

			eventHandler := newEventHandler(t)

			tenantDetailsGetter.On("GetDetails", ctx, sampleTenant).Return(detailedTenant, nil)

			specA, _ := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()
			jobADestination := job.ResourceURN("bigquery://project:dataset.tableA")
			jobAUpstreamName := []job.ResourceURN{"bigquery://project:dataset.tableB"}
			jobA := job.NewJob(sampleTenant, specA, jobADestination, jobAUpstreamName)

			incomingSpecs := []*job.Spec{specA}

			w2, _ := models.NewWindow(jobVersion, "d", "0h", "24h")
			existingJobWindow := window.NewCustomConfig(w2)
			existingSpecA, _ := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, existingJobWindow, jobTask).WithAsset(jobAsset).Build()
			existingJobA := job.NewJob(sampleTenant, existingSpecA, jobADestination, jobAUpstreamName)
			existingSpecs := []*job.Job{existingJobA}

			jobRepo.On("GetAllByTenant", ctx, sampleTenant).Return(existingSpecs, nil)

			pluginService.On("ConstructDestinationURN", ctx, specA.Task().Name().String(), mock.Anything).Return(jobADestination.String(), nil).Once()
			pluginService.On("IdentifyUpstreams", ctx, specA.Task().Name().String(), mock.Anything, mock.Anything).Return(jobResourceURNsToString(jobAUpstreamName), nil)

			jobRepo.On("Update", ctx, mock.Anything).Return([]*job.Job{jobA}, nil)
			eventHandler.On("HandleEvent", mock.Anything).Times(1)

			upstream := job.NewUpstreamResolved("job-B", "", "bigquery://project:dataset.tableB", sampleTenant, "static", taskName, false)

			jobWithUpstream := job.NewWithUpstream(jobA, []*job.Upstream{upstream})
			upstreamResolver.On("BulkResolve", ctx, project.Name(), []*job.Job{jobA}, mock.Anything).Return([]*job.WithUpstream{jobWithUpstream}, nil, nil)

			upstreamRepo.On("ReplaceUpstreams", ctx, []*job.WithUpstream{jobWithUpstream}).Return(nil)

			logWriter.On("Write", mock.Anything, mock.Anything).Return(nil)

			jobNamesToUpload := []string{jobA.GetName()}
			var jobNamesToRemove []string
			jobDeploymentService.On("UploadJobs", ctx, sampleTenant, jobNamesToUpload, jobNamesToRemove).Return(nil)

			jobService := service.NewJobService(jobRepo, upstreamRepo, downstreamRepo, pluginService, upstreamResolver, tenantDetailsGetter, eventHandler, log, jobDeploymentService, compiler.NewEngine())
			err := jobService.ReplaceAll(ctx, sampleTenant, incomingSpecs, jobNamesWithInvalidSpec, logWriter)
			assert.NoError(t, err)
		})
		t.Run("deletes the removed jobs", func(t *testing.T) {
			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			upstreamRepo := new(UpstreamRepository)
			defer upstreamRepo.AssertExpectations(t)

			downstreamRepo := new(DownstreamRepository)
			defer downstreamRepo.AssertExpectations(t)

			pluginService := new(PluginService)
			defer pluginService.AssertExpectations(t)

			upstreamResolver := new(UpstreamResolver)
			defer upstreamResolver.AssertExpectations(t)

			tenantDetailsGetter := new(TenantDetailsGetter)
			defer tenantDetailsGetter.AssertExpectations(t)

			logWriter := new(mockWriter)
			defer logWriter.AssertExpectations(t)

			jobDeploymentService := new(JobDeploymentService)
			defer jobDeploymentService.AssertExpectations(t)

			eventHandler := newEventHandler(t)

			tenantDetailsGetter.On("GetDetails", ctx, sampleTenant).Return(detailedTenant, nil)

			specA, _ := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()
			jobA := job.NewJob(sampleTenant, specA, "", nil)

			specB, _ := job.NewSpecBuilder(jobVersion, "job-B", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()
			jobB := job.NewJob(sampleTenant, specB, "", nil)

			incomingSpecs := []*job.Spec{specA}

			existingSpecs := []*job.Job{jobA, jobB}

			jobRepo.On("GetAllByTenant", ctx, sampleTenant).Return(existingSpecs, nil)

			downstreamRepo.On("GetDownstreamByJobName", ctx, project.Name(), specB.Name()).Return(nil, nil)
			jobRepo.On("Delete", ctx, project.Name(), specB.Name(), false).Return(nil)
			eventHandler.On("HandleEvent", mock.Anything).Times(1)

			logWriter.On("Write", mock.Anything, mock.Anything).Return(nil)

			var jobNamesToUpload []string
			jobNamesToRemove := []string{specB.Name().String()}
			jobDeploymentService.On("UploadJobs", ctx, sampleTenant, jobNamesToUpload, jobNamesToRemove).Return(nil)

			jobService := service.NewJobService(jobRepo, upstreamRepo, downstreamRepo, pluginService, upstreamResolver, tenantDetailsGetter, eventHandler, log, jobDeploymentService, compiler.NewEngine())
			err := jobService.ReplaceAll(ctx, sampleTenant, incomingSpecs, jobNamesWithInvalidSpec, logWriter)
			assert.NoError(t, err)
		})
		t.Run("deletes the jobs which the downstreams are also be deleted", func(t *testing.T) {
			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			upstreamRepo := new(UpstreamRepository)
			defer upstreamRepo.AssertExpectations(t)

			downstreamRepo := new(DownstreamRepository)
			defer downstreamRepo.AssertExpectations(t)

			pluginService := new(PluginService)
			defer pluginService.AssertExpectations(t)

			upstreamResolver := new(UpstreamResolver)
			defer upstreamResolver.AssertExpectations(t)

			tenantDetailsGetter := new(TenantDetailsGetter)
			defer tenantDetailsGetter.AssertExpectations(t)

			logWriter := new(mockWriter)
			defer logWriter.AssertExpectations(t)

			jobDeploymentService := new(JobDeploymentService)
			defer jobDeploymentService.AssertExpectations(t)

			eventHandler := newEventHandler(t)

			tenantDetailsGetter.On("GetDetails", ctx, sampleTenant).Return(detailedTenant, nil)

			specA, _ := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()
			specB, _ := job.NewSpecBuilder(jobVersion, "job-B", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()
			specC, _ := job.NewSpecBuilder(jobVersion, "job-C", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()
			specD, _ := job.NewSpecBuilder(jobVersion, "job-D", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()
			jobA := job.NewJob(sampleTenant, specA, "", nil)
			jobB := job.NewJob(sampleTenant, specB, "", nil)
			jobC := job.NewJob(sampleTenant, specC, "", nil)
			jobD := job.NewJob(sampleTenant, specD, "", nil)

			incomingSpecs := []*job.Spec{}

			existingSpecs := []*job.Job{jobA, jobB, jobC, jobD}

			jobRepo.On("GetAllByTenant", ctx, sampleTenant).Return(existingSpecs, nil)

			downstreamA := []*job.Downstream{
				job.NewDownstream("job-B", project.Name(), namespace.Name(), taskName),
				job.NewDownstream("job-D", project.Name(), namespace.Name(), taskName),
			}
			downstreamB := []*job.Downstream{
				job.NewDownstream("job-C", project.Name(), namespace.Name(), taskName),
			}
			downstreamRepo.On("GetDownstreamByJobName", ctx, project.Name(), specA.Name()).Return(downstreamA, nil)
			downstreamRepo.On("GetDownstreamByJobName", ctx, project.Name(), specB.Name()).Return(downstreamB, nil)
			downstreamRepo.On("GetDownstreamByJobName", ctx, project.Name(), specC.Name()).Return(nil, nil)
			downstreamRepo.On("GetDownstreamByJobName", ctx, project.Name(), specD.Name()).Return(nil, nil)
			jobRepo.On("Delete", ctx, project.Name(), mock.Anything, false).Return(nil)
			eventHandler.On("HandleEvent", mock.Anything).Times(4)

			logWriter.On("Write", mock.Anything, mock.Anything).Return(nil)

			jobDeploymentService.On("UploadJobs", ctx, sampleTenant, mock.Anything, mock.Anything).Return(nil)

			jobService := service.NewJobService(jobRepo, upstreamRepo, downstreamRepo, pluginService, upstreamResolver, tenantDetailsGetter, eventHandler, log, jobDeploymentService, compiler.NewEngine())
			err := jobService.ReplaceAll(ctx, sampleTenant, incomingSpecs, jobNamesWithInvalidSpec, logWriter)
			assert.NoError(t, err)
		})
		t.Run("adds, updates, and deletes jobs in a request", func(t *testing.T) {
			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			upstreamRepo := new(UpstreamRepository)
			defer upstreamRepo.AssertExpectations(t)

			downstreamRepo := new(DownstreamRepository)
			defer downstreamRepo.AssertExpectations(t)

			pluginService := new(PluginService)
			defer pluginService.AssertExpectations(t)

			upstreamResolver := new(UpstreamResolver)
			defer upstreamResolver.AssertExpectations(t)

			tenantDetailsGetter := new(TenantDetailsGetter)
			defer tenantDetailsGetter.AssertExpectations(t)

			logWriter := new(mockWriter)
			defer logWriter.AssertExpectations(t)

			jobDeploymentService := new(JobDeploymentService)
			defer jobDeploymentService.AssertExpectations(t)

			eventHandler := newEventHandler(t)

			tenantDetailsGetter.On("GetDetails", ctx, sampleTenant).Return(detailedTenant, nil)

			specA, _ := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()
			specB, _ := job.NewSpecBuilder(jobVersion, "job-B", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()
			incomingSpecs := []*job.Spec{specA, specB}

			w2, _ := models.NewWindow(jobVersion, "d", "0h", "24h")
			existingJobWindow := window.NewCustomConfig(w2)
			existingSpecB, _ := job.NewSpecBuilder(jobVersion, "job-B", "sample-owner", jobSchedule, existingJobWindow, jobTask).WithAsset(jobAsset).Build()
			existingJobB := job.NewJob(sampleTenant, existingSpecB, "", nil)
			existingSpecC, _ := job.NewSpecBuilder(jobVersion, "job-C", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()
			existingJobC := job.NewJob(sampleTenant, existingSpecC, "", nil)

			existingSpecs := []*job.Job{existingJobB, existingJobC}

			jobRepo.On("GetAllByTenant", ctx, sampleTenant).Return(existingSpecs, nil)

			jobADestination := job.ResourceURN("bigquery://project:dataset.tableA")
			jobBDestination := job.ResourceURN("bigquery://project:dataset.tableB")
			pluginService.On("ConstructDestinationURN", ctx, specA.Task().Name().String(), mock.Anything).Return(jobADestination.String(), nil).Once()
			pluginService.On("ConstructDestinationURN", ctx, specB.Task().Name().String(), mock.Anything).Return(jobBDestination.String(), nil).Once()

			jobAUpstreamNames := []job.ResourceURN{"bigquery://project:dataset.tableB"}
			var jobBUpstreamNames []job.ResourceURN
			pluginService.On("IdentifyUpstreams", ctx, specA.Task().Name().String(), mock.Anything, mock.Anything).Return(jobResourceURNsToString(jobAUpstreamNames), nil)
			pluginService.On("IdentifyUpstreams", ctx, specB.Task().Name().String(), mock.Anything, mock.Anything).Return(jobResourceURNsToString(jobBUpstreamNames), nil)

			jobA := job.NewJob(sampleTenant, specA, jobADestination, jobAUpstreamNames)
			jobRepo.On("Add", ctx, mock.Anything).Return([]*job.Job{jobA}, nil)

			jobB := job.NewJob(sampleTenant, specB, "", nil)
			jobRepo.On("Update", ctx, mock.Anything).Return([]*job.Job{jobB}, nil)

			downstreamRepo.On("GetDownstreamByJobName", ctx, project.Name(), existingSpecC.Name()).Return(nil, nil)
			jobRepo.On("Delete", ctx, project.Name(), existingSpecC.Name(), false).Return(nil)

			upstream := job.NewUpstreamResolved("job-B", "", "bigquery://project:dataset.tableB", sampleTenant, "static", taskName, false)
			jobWithUpstream := job.NewWithUpstream(jobA, []*job.Upstream{upstream})
			upstreamResolver.On("BulkResolve", ctx, project.Name(), []*job.Job{jobA, jobB}, mock.Anything).Return([]*job.WithUpstream{jobWithUpstream}, nil, nil)

			upstreamRepo.On("ReplaceUpstreams", ctx, []*job.WithUpstream{jobWithUpstream}).Return(nil)

			logWriter.On("Write", mock.Anything, mock.Anything).Return(nil)
			eventHandler.On("HandleEvent", mock.Anything).Times(3)

			jobNamesToUpload := []string{jobA.GetName(), jobB.GetName()}
			jobNamesToRemove := []string{existingJobC.GetName()}
			jobDeploymentService.On("UploadJobs", ctx, sampleTenant, jobNamesToUpload, jobNamesToRemove).Return(nil)

			jobService := service.NewJobService(jobRepo, upstreamRepo, downstreamRepo, pluginService, upstreamResolver, tenantDetailsGetter, eventHandler, log, jobDeploymentService, compiler.NewEngine())
			err := jobService.ReplaceAll(ctx, sampleTenant, incomingSpecs, jobNamesWithInvalidSpec, logWriter)
			assert.NoError(t, err)
		})
		t.Run("skips invalid job when classifying specs as added, modified, or deleted", func(t *testing.T) {
			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			upstreamRepo := new(UpstreamRepository)
			defer upstreamRepo.AssertExpectations(t)

			downstreamRepo := new(DownstreamRepository)
			defer downstreamRepo.AssertExpectations(t)

			pluginService := new(PluginService)
			defer pluginService.AssertExpectations(t)

			upstreamResolver := new(UpstreamResolver)
			defer upstreamResolver.AssertExpectations(t)

			tenantDetailsGetter := new(TenantDetailsGetter)
			defer tenantDetailsGetter.AssertExpectations(t)

			logWriter := new(mockWriter)
			defer logWriter.AssertExpectations(t)

			jobDeploymentService := new(JobDeploymentService)
			defer jobDeploymentService.AssertExpectations(t)

			eventHandler := newEventHandler(t)

			tenantDetailsGetter.On("GetDetails", ctx, sampleTenant).Return(detailedTenant, nil)

			specA, _ := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()
			specB, _ := job.NewSpecBuilder(jobVersion, "job-B", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()
			incomingSpecs := []*job.Spec{specA, specB}

			w2, _ := models.NewWindow(jobVersion, "d", "0h", "24h")
			existingJobWindow := window.NewCustomConfig(w2)
			existingSpecB, _ := job.NewSpecBuilder(jobVersion, "job-B", "sample-owner", jobSchedule, existingJobWindow, jobTask).WithAsset(jobAsset).Build()
			existingJobB := job.NewJob(sampleTenant, existingSpecB, "", nil)
			existingSpecC, _ := job.NewSpecBuilder(jobVersion, "job-C", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()
			existingJobC := job.NewJob(sampleTenant, existingSpecC, "", nil)
			existingSpecD, _ := job.NewSpecBuilder(jobVersion, "job-D", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()
			existingJobD := job.NewJob(sampleTenant, existingSpecD, "", nil)

			existingSpecs := []*job.Job{existingJobB, existingJobC, existingJobD}

			jobRepo.On("GetAllByTenant", ctx, sampleTenant).Return(existingSpecs, nil)

			jobADestination := job.ResourceURN("bigquery://project:dataset.tableA")
			jobBDestination := job.ResourceURN("bigquery://project:dataset.tableB")
			pluginService.On("ConstructDestinationURN", ctx, specA.Task().Name().String(), mock.Anything).Return(jobADestination.String(), nil).Once()
			pluginService.On("ConstructDestinationURN", ctx, specB.Task().Name().String(), mock.Anything).Return(jobBDestination.String(), nil).Once()

			jobAUpstreamNames := []job.ResourceURN{"bigquery://project:dataset.tableB"}
			var jobBUpstreamNames []job.ResourceURN
			pluginService.On("IdentifyUpstreams", ctx, specA.Task().Name().String(), mock.Anything, mock.Anything).Return(jobResourceURNsToString(jobAUpstreamNames), nil)
			pluginService.On("IdentifyUpstreams", ctx, specB.Task().Name().String(), mock.Anything, mock.Anything).Return(jobResourceURNsToString(jobBUpstreamNames), nil)

			jobA := job.NewJob(sampleTenant, specA, jobADestination, jobAUpstreamNames)
			jobRepo.On("Add", ctx, mock.Anything).Return([]*job.Job{jobA}, nil)

			jobB := job.NewJob(sampleTenant, specB, "", nil)
			jobRepo.On("Update", ctx, mock.Anything).Return([]*job.Job{jobB}, nil)

			downstreamRepo.On("GetDownstreamByJobName", ctx, project.Name(), existingSpecC.Name()).Return(nil, nil)
			jobRepo.On("Delete", ctx, project.Name(), existingSpecC.Name(), false).Return(nil)

			upstream := job.NewUpstreamResolved("job-B", "", "bigquery://project:dataset.tableB", sampleTenant, "static", taskName, false)
			jobWithUpstream := job.NewWithUpstream(jobA, []*job.Upstream{upstream})
			upstreamResolver.On("BulkResolve", ctx, project.Name(), []*job.Job{jobA, jobB}, mock.Anything).Return([]*job.WithUpstream{jobWithUpstream}, nil, nil)

			upstreamRepo.On("ReplaceUpstreams", ctx, []*job.WithUpstream{jobWithUpstream}).Return(nil)

			logWriter.On("Write", mock.Anything, mock.Anything).Return(nil)
			eventHandler.On("HandleEvent", mock.Anything).Times(3)

			jobNamesToUpload := []string{jobA.GetName(), jobB.GetName()}
			jobNamesToRemove := []string{existingJobC.GetName()}
			jobDeploymentService.On("UploadJobs", ctx, sampleTenant, jobNamesToUpload, jobNamesToRemove).Return(nil)

			jobService := service.NewJobService(jobRepo, upstreamRepo, downstreamRepo, pluginService, upstreamResolver, tenantDetailsGetter, eventHandler, log, jobDeploymentService, compiler.NewEngine())
			err := jobService.ReplaceAll(ctx, sampleTenant, incomingSpecs, []job.Name{"job-D"}, logWriter)
			assert.NoError(t, err)
		})
		t.Run("skips adding new invalid jobs", func(t *testing.T) {
			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			upstreamRepo := new(UpstreamRepository)
			defer upstreamRepo.AssertExpectations(t)

			downstreamRepo := new(DownstreamRepository)
			defer downstreamRepo.AssertExpectations(t)

			pluginService := new(PluginService)
			defer pluginService.AssertExpectations(t)

			upstreamResolver := new(UpstreamResolver)
			defer upstreamResolver.AssertExpectations(t)

			tenantDetailsGetter := new(TenantDetailsGetter)
			defer tenantDetailsGetter.AssertExpectations(t)

			logWriter := new(mockWriter)
			defer logWriter.AssertExpectations(t)

			eventHandler := newEventHandler(t)

			jobDeploymentService := new(JobDeploymentService)
			defer jobDeploymentService.AssertExpectations(t)

			tenantDetailsGetter.On("GetDetails", ctx, sampleTenant).Return(detailedTenant, nil)

			specA, _ := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()
			incomingSpecs := []*job.Spec{specA}

			existingSpecC, _ := job.NewSpecBuilder(jobVersion, "job-C", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()
			existingJobC := job.NewJob(sampleTenant, existingSpecC, "", nil)
			existingSpecs := []*job.Job{existingJobC}

			jobRepo.On("GetAllByTenant", ctx, sampleTenant).Return(existingSpecs, nil)

			var specADestination job.ResourceURN
			pluginService.On("ConstructDestinationURN", ctx, specA.Task().Name().String(), mock.Anything).Return(specADestination.String(), errors.New("internal error")).Once()

			downstreamRepo.On("GetDownstreamByJobName", ctx, project.Name(), existingSpecC.Name()).Return(nil, nil)
			jobRepo.On("Delete", ctx, project.Name(), existingSpecC.Name(), false).Return(nil)
			eventHandler.On("HandleEvent", mock.Anything).Times(1)

			logWriter.On("Write", mock.Anything, mock.Anything).Return(nil)

			var jobNamesToUpload []string
			jobNamesToRemove := []string{existingJobC.GetName()}
			jobDeploymentService.On("UploadJobs", ctx, sampleTenant, jobNamesToUpload, jobNamesToRemove).Return(nil)

			jobService := service.NewJobService(jobRepo, upstreamRepo, downstreamRepo, pluginService, upstreamResolver, tenantDetailsGetter, eventHandler, log, jobDeploymentService, compiler.NewEngine())
			err := jobService.ReplaceAll(ctx, sampleTenant, incomingSpecs, jobNamesWithInvalidSpec, logWriter)
			assert.ErrorContains(t, err, "internal error")
		})
		t.Run("skips invalid modified jobs", func(t *testing.T) {
			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			upstreamRepo := new(UpstreamRepository)
			defer upstreamRepo.AssertExpectations(t)

			downstreamRepo := new(DownstreamRepository)
			defer downstreamRepo.AssertExpectations(t)

			pluginService := new(PluginService)
			defer pluginService.AssertExpectations(t)

			upstreamResolver := new(UpstreamResolver)
			defer upstreamResolver.AssertExpectations(t)

			tenantDetailsGetter := new(TenantDetailsGetter)
			defer tenantDetailsGetter.AssertExpectations(t)

			logWriter := new(mockWriter)
			defer logWriter.AssertExpectations(t)

			jobDeploymentService := new(JobDeploymentService)
			defer jobDeploymentService.AssertExpectations(t)

			eventHandler := newEventHandler(t)

			tenantDetailsGetter.On("GetDetails", ctx, sampleTenant).Return(detailedTenant, nil)

			specB, _ := job.NewSpecBuilder(jobVersion, "job-B", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()
			incomingSpecs := []*job.Spec{specB}

			w2, _ := models.NewWindow(jobVersion, "d", "0h", "24h")
			existingJobWindow := window.NewCustomConfig(w2)
			existingSpecB, _ := job.NewSpecBuilder(jobVersion, "job-B", "sample-owner", jobSchedule, existingJobWindow, jobTask).WithAsset(jobAsset).Build()
			existingJobB := job.NewJob(sampleTenant, existingSpecB, "", nil)
			existingSpecC, _ := job.NewSpecBuilder(jobVersion, "job-C", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()
			existingJobC := job.NewJob(sampleTenant, existingSpecC, "", nil)
			existingSpecs := []*job.Job{existingJobB, existingJobC}

			jobRepo.On("GetAllByTenant", ctx, sampleTenant).Return(existingSpecs, nil)

			var jobBDestination job.ResourceURN
			pluginService.On("ConstructDestinationURN", ctx, specB.Task().Name().String(), mock.Anything).Return(jobBDestination.String(), errors.New("internal error")).Once()

			downstreamRepo.On("GetDownstreamByJobName", ctx, project.Name(), existingSpecC.Name()).Return(nil, nil)
			jobRepo.On("Delete", ctx, project.Name(), existingSpecC.Name(), false).Return(nil)
			eventHandler.On("HandleEvent", mock.Anything).Times(1)

			logWriter.On("Write", mock.Anything, mock.Anything).Return(nil)

			var jobNamesToUpload []string
			jobNamesToRemove := []string{existingJobC.GetName()}
			jobDeploymentService.On("UploadJobs", ctx, sampleTenant, jobNamesToUpload, jobNamesToRemove).Return(nil)

			jobService := service.NewJobService(jobRepo, upstreamRepo, downstreamRepo, pluginService, upstreamResolver, tenantDetailsGetter, eventHandler, log, jobDeploymentService, compiler.NewEngine())
			err := jobService.ReplaceAll(ctx, sampleTenant, incomingSpecs, jobNamesWithInvalidSpec, logWriter)
			assert.ErrorContains(t, err, "internal error")
		})
		t.Run("skips to delete jobs if the downstream is not deleted", func(t *testing.T) {
			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			upstreamRepo := new(UpstreamRepository)
			defer upstreamRepo.AssertExpectations(t)

			downstreamRepo := new(DownstreamRepository)
			defer downstreamRepo.AssertExpectations(t)

			pluginService := new(PluginService)
			defer pluginService.AssertExpectations(t)

			upstreamResolver := new(UpstreamResolver)
			defer upstreamResolver.AssertExpectations(t)

			tenantDetailsGetter := new(TenantDetailsGetter)
			defer tenantDetailsGetter.AssertExpectations(t)

			logWriter := new(mockWriter)
			defer logWriter.AssertExpectations(t)

			jobDeploymentService := new(JobDeploymentService)
			defer jobDeploymentService.AssertExpectations(t)

			eventHandler := newEventHandler(t)

			tenantDetailsGetter.On("GetDetails", ctx, sampleTenant).Return(detailedTenant, nil)

			specA, _ := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()
			specE, _ := job.NewSpecBuilder(jobVersion, "job-E", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()
			incomingSpecs := []*job.Spec{specA, specE}

			existingSpecC, _ := job.NewSpecBuilder(jobVersion, "job-C", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()
			existingJobC := job.NewJob(sampleTenant, existingSpecC, "", nil)
			existingSpecD, _ := job.NewSpecBuilder(jobVersion, "job-D", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()
			existingJobD := job.NewJob(sampleTenant, existingSpecD, "", nil)
			existingSpecE, _ := job.NewSpecBuilder(jobVersion, "job-E", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()
			existingJobE := job.NewJob(sampleTenant, existingSpecE, "", nil)
			existingSpecs := []*job.Job{existingJobC, existingJobD, existingJobE}

			jobRepo.On("GetAllByTenant", ctx, sampleTenant).Return(existingSpecs, nil)

			jobADestination := job.ResourceURN("bigquery://project:dataset.tableA")
			pluginService.On("ConstructDestinationURN", ctx, specA.Task().Name().String(), mock.Anything).Return(jobADestination.String(), nil).Once()

			jobAUpstreamNames := []job.ResourceURN{"bigquery://project:dataset.tableB"}
			pluginService.On("IdentifyUpstreams", ctx, specA.Task().Name().String(), mock.Anything, mock.Anything).Return(jobResourceURNsToString(jobAUpstreamNames), nil)

			jobA := job.NewJob(sampleTenant, specA, jobADestination, jobAUpstreamNames)
			jobRepo.On("Add", ctx, mock.Anything).Return([]*job.Job{jobA}, nil)

			downstreamList := []*job.Downstream{
				job.NewDownstream("job-E", project.Name(), namespace.Name(), taskName),
			}

			downstreamRepo.On("GetDownstreamByJobName", ctx, project.Name(), existingSpecC.Name()).Return(downstreamList, nil)
			downstreamRepo.On("GetDownstreamByJobName", ctx, project.Name(), existingSpecD.Name()).Return(nil, nil)
			downstreamRepo.On("GetDownstreamByJobName", ctx, project.Name(), existingSpecE.Name()).Return(nil, nil)
			jobRepo.On("Delete", ctx, project.Name(), existingSpecD.Name(), false).Return(nil)

			upstream := job.NewUpstreamResolved("job-B", "", "bigquery://project:dataset.tableB", sampleTenant, "static", taskName, false)
			jobWithUpstream := job.NewWithUpstream(jobA, []*job.Upstream{upstream})
			upstreamResolver.On("BulkResolve", ctx, project.Name(), []*job.Job{jobA}, mock.Anything).Return([]*job.WithUpstream{jobWithUpstream}, nil, nil)

			upstreamRepo.On("ReplaceUpstreams", ctx, []*job.WithUpstream{jobWithUpstream}).Return(nil)

			logWriter.On("Write", mock.Anything, mock.Anything).Return(nil)
			eventHandler.On("HandleEvent", mock.Anything).Times(2)

			jobNamesToUpload := []string{jobA.GetName()}
			jobNamesToRemove := []string{existingJobD.GetName()}
			jobDeploymentService.On("UploadJobs", ctx, sampleTenant, jobNamesToUpload, jobNamesToRemove).Return(nil)

			jobService := service.NewJobService(jobRepo, upstreamRepo, downstreamRepo, pluginService, upstreamResolver, tenantDetailsGetter, eventHandler, log, jobDeploymentService, compiler.NewEngine())
			err := jobService.ReplaceAll(ctx, sampleTenant, incomingSpecs, jobNamesWithInvalidSpec, logWriter)
			assert.ErrorContains(t, err, "job is being used by")
		})
		t.Run("should not break process if one of job failed to be added", func(t *testing.T) {
			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			upstreamRepo := new(UpstreamRepository)
			defer upstreamRepo.AssertExpectations(t)

			downstreamRepo := new(DownstreamRepository)
			defer downstreamRepo.AssertExpectations(t)

			pluginService := new(PluginService)
			defer pluginService.AssertExpectations(t)

			upstreamResolver := new(UpstreamResolver)
			defer upstreamResolver.AssertExpectations(t)

			tenantDetailsGetter := new(TenantDetailsGetter)
			defer tenantDetailsGetter.AssertExpectations(t)

			logWriter := new(mockWriter)
			defer logWriter.AssertExpectations(t)

			jobDeploymentService := new(JobDeploymentService)
			defer jobDeploymentService.AssertExpectations(t)

			eventHandler := newEventHandler(t)

			tenantDetailsGetter.On("GetDetails", ctx, sampleTenant).Return(detailedTenant, nil)

			specA, _ := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()
			jobADestination := job.ResourceURN("bigquery://project:dataset.tableA")
			jobAUpstreamName := []job.ResourceURN{"bigquery://project:dataset.tableB"}
			jobA := job.NewJob(sampleTenant, specA, jobADestination, jobAUpstreamName)

			specB, _ := job.NewSpecBuilder(jobVersion, "job-B", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()
			jobBDestination := job.ResourceURN("bigquery://project:dataset.tableB")
			jobBUpstreamName := []job.ResourceURN{"bigquery://project:dataset.tableC"}

			incomingSpecs := []*job.Spec{specA, specB}

			jobRepo.On("GetAllByTenant", ctx, sampleTenant).Return([]*job.Job{}, nil)

			pluginService.On("ConstructDestinationURN", ctx, specA.Task().Name().String(), mock.Anything).Return(jobADestination.String(), nil).Once()
			pluginService.On("IdentifyUpstreams", ctx, specA.Task().Name().String(), mock.Anything, mock.Anything).Return(jobResourceURNsToString(jobAUpstreamName), nil).Once()

			pluginService.On("ConstructDestinationURN", ctx, specB.Task().Name().String(), mock.Anything).Return(jobBDestination.String(), nil).Once()
			pluginService.On("IdentifyUpstreams", ctx, specB.Task().Name().String(), mock.Anything, mock.Anything).Return(jobResourceURNsToString(jobBUpstreamName), nil).Once()

			jobRepo.On("Add", ctx, mock.Anything).Return([]*job.Job{jobA}, errors.New("internal error"))

			upstream := job.NewUpstreamResolved("job-B", "", "bigquery://project:dataset.tableB", sampleTenant, "static", taskName, false)

			jobWithUpstream := job.NewWithUpstream(jobA, []*job.Upstream{upstream})
			upstreamResolver.On("BulkResolve", ctx, project.Name(), []*job.Job{jobA}, mock.Anything).Return([]*job.WithUpstream{jobWithUpstream}, nil, nil)

			upstreamRepo.On("ReplaceUpstreams", ctx, []*job.WithUpstream{jobWithUpstream}).Return(nil)

			logWriter.On("Write", mock.Anything, mock.Anything).Return(nil)
			eventHandler.On("HandleEvent", mock.Anything).Times(1)

			jobNamesToUpload := []string{jobA.GetName()}
			var jobNamesToRemove []string
			jobDeploymentService.On("UploadJobs", ctx, sampleTenant, jobNamesToUpload, jobNamesToRemove).Return(nil)

			jobService := service.NewJobService(jobRepo, upstreamRepo, downstreamRepo, pluginService, upstreamResolver, tenantDetailsGetter, eventHandler, log, jobDeploymentService, compiler.NewEngine())
			err := jobService.ReplaceAll(ctx, sampleTenant, incomingSpecs, jobNamesWithInvalidSpec, logWriter)
			assert.ErrorContains(t, err, "internal error")
		})
		t.Run("should not break process if one of job failed to be updated", func(t *testing.T) {
			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			upstreamRepo := new(UpstreamRepository)
			defer upstreamRepo.AssertExpectations(t)

			downstreamRepo := new(DownstreamRepository)
			defer downstreamRepo.AssertExpectations(t)

			pluginService := new(PluginService)
			defer pluginService.AssertExpectations(t)

			upstreamResolver := new(UpstreamResolver)
			defer upstreamResolver.AssertExpectations(t)

			tenantDetailsGetter := new(TenantDetailsGetter)
			defer tenantDetailsGetter.AssertExpectations(t)

			logWriter := new(mockWriter)
			defer logWriter.AssertExpectations(t)

			jobDeploymentService := new(JobDeploymentService)
			defer jobDeploymentService.AssertExpectations(t)

			tenantDetailsGetter.On("GetDetails", ctx, sampleTenant).Return(detailedTenant, nil)

			specA, _ := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()
			jobADestination := job.ResourceURN("bigquery://project:dataset.tableA")
			jobAUpstreamName := []job.ResourceURN{"bigquery://project:dataset.tableB"}

			incomingSpecs := []*job.Spec{specA}

			w2, _ := models.NewWindow(jobVersion, "d", "0h", "24h")
			existingJobWindow := window.NewCustomConfig(w2)
			existingSpecA, _ := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, existingJobWindow, jobTask).WithAsset(jobAsset).Build()
			existingJobA := job.NewJob(sampleTenant, existingSpecA, jobADestination, jobAUpstreamName)
			existingSpecs := []*job.Job{existingJobA}

			jobRepo.On("GetAllByTenant", ctx, sampleTenant).Return(existingSpecs, nil)

			pluginService.On("ConstructDestinationURN", ctx, specA.Task().Name().String(), mock.Anything).Return(jobADestination.String(), nil).Once()
			pluginService.On("IdentifyUpstreams", ctx, specA.Task().Name().String(), mock.Anything, mock.Anything).Return(jobResourceURNsToString(jobAUpstreamName), nil)

			jobRepo.On("Update", ctx, mock.Anything).Return([]*job.Job{}, errors.New("internal error"))

			logWriter.On("Write", mock.Anything, mock.Anything).Return(nil)
			jobService := service.NewJobService(jobRepo, upstreamRepo, downstreamRepo, pluginService, upstreamResolver, tenantDetailsGetter, nil, log, jobDeploymentService, compiler.NewEngine())
			err := jobService.ReplaceAll(ctx, sampleTenant, incomingSpecs, jobNamesWithInvalidSpec, logWriter)
			assert.ErrorContains(t, err, "internal error")
		})
		t.Run("should not break process if one of job failed to be deleted", func(t *testing.T) {
			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			upstreamRepo := new(UpstreamRepository)
			defer upstreamRepo.AssertExpectations(t)

			downstreamRepo := new(DownstreamRepository)
			defer downstreamRepo.AssertExpectations(t)

			pluginService := new(PluginService)
			defer pluginService.AssertExpectations(t)

			upstreamResolver := new(UpstreamResolver)
			defer upstreamResolver.AssertExpectations(t)

			tenantDetailsGetter := new(TenantDetailsGetter)
			defer tenantDetailsGetter.AssertExpectations(t)

			logWriter := new(mockWriter)
			defer logWriter.AssertExpectations(t)

			jobDeploymentService := new(JobDeploymentService)
			defer jobDeploymentService.AssertExpectations(t)

			tenantDetailsGetter.On("GetDetails", ctx, sampleTenant).Return(detailedTenant, nil)

			specA, _ := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()
			jobA := job.NewJob(sampleTenant, specA, "", nil)

			specB, _ := job.NewSpecBuilder(jobVersion, "job-B", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()
			jobB := job.NewJob(sampleTenant, specB, "", nil)

			incomingSpecs := []*job.Spec{specA}

			existingSpecs := []*job.Job{jobA, jobB}

			jobRepo.On("GetAllByTenant", ctx, sampleTenant).Return(existingSpecs, nil)

			downstreamRepo.On("GetDownstreamByJobName", ctx, project.Name(), specB.Name()).Return(nil, nil)
			jobRepo.On("Delete", ctx, project.Name(), specB.Name(), false).Return(errors.New("internal error"))

			logWriter.On("Write", mock.Anything, mock.Anything).Return(nil).Times(3)

			jobService := service.NewJobService(jobRepo, upstreamRepo, downstreamRepo, pluginService, upstreamResolver, tenantDetailsGetter, nil, log, jobDeploymentService, compiler.NewEngine())
			err := jobService.ReplaceAll(ctx, sampleTenant, incomingSpecs, jobNamesWithInvalidSpec, logWriter)
			assert.ErrorContains(t, err, "internal error")
		})
		t.Run("should not delete job if unable to check downstream of the job", func(t *testing.T) {
			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			upstreamRepo := new(UpstreamRepository)
			defer upstreamRepo.AssertExpectations(t)

			downstreamRepo := new(DownstreamRepository)
			defer downstreamRepo.AssertExpectations(t)

			pluginService := new(PluginService)
			defer pluginService.AssertExpectations(t)

			upstreamResolver := new(UpstreamResolver)
			defer upstreamResolver.AssertExpectations(t)

			tenantDetailsGetter := new(TenantDetailsGetter)
			defer tenantDetailsGetter.AssertExpectations(t)

			logWriter := new(mockWriter)
			defer logWriter.AssertExpectations(t)

			jobDeploymentService := new(JobDeploymentService)
			defer jobDeploymentService.AssertExpectations(t)

			tenantDetailsGetter.On("GetDetails", ctx, sampleTenant).Return(detailedTenant, nil)

			specA, _ := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()
			jobA := job.NewJob(sampleTenant, specA, "", nil)

			specB, _ := job.NewSpecBuilder(jobVersion, "job-B", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()
			jobB := job.NewJob(sampleTenant, specB, "", nil)

			incomingSpecs := []*job.Spec{specA}

			existingSpecs := []*job.Job{jobA, jobB}

			jobRepo.On("GetAllByTenant", ctx, sampleTenant).Return(existingSpecs, nil)

			downstreamRepo.On("GetDownstreamByJobName", ctx, project.Name(), specB.Name()).Return(nil, errors.New("internal error"))

			logWriter.On("Write", mock.Anything, mock.Anything).Return(nil).Twice()

			jobService := service.NewJobService(jobRepo, upstreamRepo, downstreamRepo, pluginService, upstreamResolver, tenantDetailsGetter, nil, log, jobDeploymentService, compiler.NewEngine())
			err := jobService.ReplaceAll(ctx, sampleTenant, incomingSpecs, jobNamesWithInvalidSpec, logWriter)
			assert.ErrorContains(t, err, "internal error")
		})
		t.Run("should not delete job if one of its downstream is unable to delete", func(t *testing.T) {
			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			upstreamRepo := new(UpstreamRepository)
			defer upstreamRepo.AssertExpectations(t)

			downstreamRepo := new(DownstreamRepository)
			defer downstreamRepo.AssertExpectations(t)

			pluginService := new(PluginService)
			defer pluginService.AssertExpectations(t)

			upstreamResolver := new(UpstreamResolver)
			defer upstreamResolver.AssertExpectations(t)

			tenantDetailsGetter := new(TenantDetailsGetter)
			defer tenantDetailsGetter.AssertExpectations(t)

			logWriter := new(mockWriter)
			defer logWriter.AssertExpectations(t)

			jobDeploymentService := new(JobDeploymentService)
			defer jobDeploymentService.AssertExpectations(t)

			eventHandler := newEventHandler(t)

			tenantDetailsGetter.On("GetDetails", ctx, sampleTenant).Return(detailedTenant, nil)

			specA, _ := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()
			specB, _ := job.NewSpecBuilder(jobVersion, "job-B", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()
			specC, _ := job.NewSpecBuilder(jobVersion, "job-C", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()
			specD, _ := job.NewSpecBuilder(jobVersion, "job-D", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()
			jobA := job.NewJob(sampleTenant, specA, "", nil)
			jobB := job.NewJob(sampleTenant, specB, "", nil)
			jobC := job.NewJob(sampleTenant, specC, "", nil)
			jobD := job.NewJob(sampleTenant, specD, "", nil)

			incomingSpecs := []*job.Spec{}

			existingSpecs := []*job.Job{jobA, jobB, jobC, jobD}

			jobRepo.On("GetAllByTenant", ctx, sampleTenant).Return(existingSpecs, nil)

			downstreamA := []*job.Downstream{
				job.NewDownstream("job-B", project.Name(), namespace.Name(), taskName),
				job.NewDownstream("job-D", project.Name(), namespace.Name(), taskName),
			}
			downstreamB := []*job.Downstream{
				job.NewDownstream("job-C", project.Name(), namespace.Name(), taskName),
			}
			downstreamRepo.On("GetDownstreamByJobName", ctx, project.Name(), specA.Name()).Return(downstreamA, nil)
			downstreamRepo.On("GetDownstreamByJobName", ctx, project.Name(), specB.Name()).Return(downstreamB, nil)
			downstreamRepo.On("GetDownstreamByJobName", ctx, project.Name(), specC.Name()).Return(nil, nil)
			downstreamRepo.On("GetDownstreamByJobName", ctx, project.Name(), specD.Name()).Return(nil, nil)
			jobRepo.On("Delete", ctx, project.Name(), specC.Name(), false).Return(errors.New("db error"))
			jobRepo.On("Delete", ctx, project.Name(), specD.Name(), false).Return(nil)
			eventHandler.On("HandleEvent", mock.Anything).Times(1)

			logWriter.On("Write", mock.Anything, mock.Anything).Return(nil)

			var jobNamesToUpload []string
			jobNamesToRemove := []string{specD.Name().String()}
			jobDeploymentService.On("UploadJobs", ctx, sampleTenant, jobNamesToUpload, jobNamesToRemove).Return(nil)

			jobService := service.NewJobService(jobRepo, upstreamRepo, downstreamRepo, pluginService, upstreamResolver, tenantDetailsGetter, eventHandler, log, jobDeploymentService, compiler.NewEngine())
			err := jobService.ReplaceAll(ctx, sampleTenant, incomingSpecs, jobNamesWithInvalidSpec, logWriter)
			assert.Error(t, err)
		})
		t.Run("returns error if unable to get tenant details", func(t *testing.T) {
			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			upstreamRepo := new(UpstreamRepository)
			defer upstreamRepo.AssertExpectations(t)

			downstreamRepo := new(DownstreamRepository)
			defer downstreamRepo.AssertExpectations(t)

			pluginService := new(PluginService)
			defer pluginService.AssertExpectations(t)

			upstreamResolver := new(UpstreamResolver)
			defer upstreamResolver.AssertExpectations(t)

			tenantDetailsGetter := new(TenantDetailsGetter)
			defer tenantDetailsGetter.AssertExpectations(t)

			logWriter := new(mockWriter)
			defer logWriter.AssertExpectations(t)

			jobDeploymentService := new(JobDeploymentService)
			defer jobDeploymentService.AssertExpectations(t)

			specA, _ := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()
			specB, _ := job.NewSpecBuilder(jobVersion, "job-B", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()
			jobB := job.NewJob(sampleTenant, specB, "", nil)

			incomingSpecs := []*job.Spec{specA, specB}
			existingJobs := []*job.Job{jobB}

			jobRepo.On("GetAllByTenant", ctx, sampleTenant).Return(existingJobs, nil)

			logWriter.On("Write", mock.Anything, mock.Anything).Return(nil)

			errorMsg := "project/namespace error"
			tenantDetailsGetter.On("GetDetails", ctx, sampleTenant).Return(nil, errors.New(errorMsg))

			jobService := service.NewJobService(jobRepo, upstreamRepo, downstreamRepo, pluginService, upstreamResolver, tenantDetailsGetter, nil, log, jobDeploymentService, compiler.NewEngine())

			err := jobService.ReplaceAll(ctx, sampleTenant, incomingSpecs, jobNamesWithInvalidSpec, logWriter)
			assert.ErrorContains(t, err, errorMsg)
		})
		t.Run("returns error if encounter error when uploading/removing jobs", func(t *testing.T) {
			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			upstreamRepo := new(UpstreamRepository)
			defer upstreamRepo.AssertExpectations(t)

			downstreamRepo := new(DownstreamRepository)
			defer downstreamRepo.AssertExpectations(t)

			pluginService := new(PluginService)
			defer pluginService.AssertExpectations(t)

			upstreamResolver := new(UpstreamResolver)
			defer upstreamResolver.AssertExpectations(t)

			tenantDetailsGetter := new(TenantDetailsGetter)
			defer tenantDetailsGetter.AssertExpectations(t)

			logWriter := new(mockWriter)
			defer logWriter.AssertExpectations(t)

			jobDeploymentService := new(JobDeploymentService)
			defer jobDeploymentService.AssertExpectations(t)

			eventHandler := newEventHandler(t)

			tenantDetailsGetter.On("GetDetails", ctx, sampleTenant).Return(detailedTenant, nil)

			specA, _ := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()
			jobADestination := job.ResourceURN("bigquery://project:dataset.tableA")
			jobAUpstreamName := []job.ResourceURN{"bigquery://project:dataset.tableB"}
			jobA := job.NewJob(sampleTenant, specA, jobADestination, jobAUpstreamName)

			specB, _ := job.NewSpecBuilder(jobVersion, "job-B", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()
			jobB := job.NewJob(sampleTenant, specB, "", nil)

			incomingSpecs := []*job.Spec{specA, specB}

			existingJobs := []*job.Job{jobB}

			jobRepo.On("GetAllByTenant", ctx, sampleTenant).Return(existingJobs, nil)

			pluginService.On("ConstructDestinationURN", ctx, specA.Task().Name().String(), mock.Anything).Return(jobADestination.String(), nil).Once()
			pluginService.On("IdentifyUpstreams", ctx, specA.Task().Name().String(), mock.Anything, mock.Anything).Return(jobResourceURNsToString(jobAUpstreamName), nil)

			jobRepo.On("Add", ctx, mock.Anything).Return([]*job.Job{jobA}, nil)

			upstream := job.NewUpstreamResolved("job-B", "", "bigquery://project:dataset.tableB", sampleTenant, "static", taskName, false)

			jobWithUpstream := job.NewWithUpstream(jobA, []*job.Upstream{upstream})
			upstreamResolver.On("BulkResolve", ctx, project.Name(), []*job.Job{jobA}, mock.Anything).Return([]*job.WithUpstream{jobWithUpstream}, nil, nil)

			upstreamRepo.On("ReplaceUpstreams", ctx, []*job.WithUpstream{jobWithUpstream}).Return(nil)

			logWriter.On("Write", mock.Anything, mock.Anything).Return(nil)
			eventHandler.On("HandleEvent", mock.Anything).Times(1)

			errorMsg := "internal error"
			jobDeploymentService.On("UploadJobs", ctx, sampleTenant, mock.Anything, mock.Anything).Return(errors.New(errorMsg))

			jobService := service.NewJobService(jobRepo, upstreamRepo, downstreamRepo, pluginService, upstreamResolver, tenantDetailsGetter, eventHandler, log, jobDeploymentService, compiler.NewEngine())
			err := jobService.ReplaceAll(ctx, sampleTenant, incomingSpecs, jobNamesWithInvalidSpec, logWriter)
			assert.ErrorContains(t, err, errorMsg)
		})
	})

	t.Run("Refresh", func(t *testing.T) {
		t.Run("resolves and saves upstream for all existing jobs in the given tenant", func(t *testing.T) {
			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			upstreamRepo := new(UpstreamRepository)
			defer upstreamRepo.AssertExpectations(t)

			downstreamRepo := new(DownstreamRepository)
			defer downstreamRepo.AssertExpectations(t)

			pluginService := new(PluginService)
			defer pluginService.AssertExpectations(t)

			upstreamResolver := new(UpstreamResolver)
			defer upstreamResolver.AssertExpectations(t)

			tenantDetailsGetter := new(TenantDetailsGetter)
			defer tenantDetailsGetter.AssertExpectations(t)

			logWriter := new(mockWriter)
			defer logWriter.AssertExpectations(t)

			jobDeploymentService := new(JobDeploymentService)
			defer jobDeploymentService.AssertExpectations(t)

			eventHandler := newEventHandler(t)

			specA, _ := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()
			jobADestination := job.ResourceURN("bigquery://project:dataset.tableA")
			jobAUpstreamName := []job.ResourceURN{"bigquery://project:dataset.tableB"}
			jobA := job.NewJob(sampleTenant, specA, jobADestination, jobAUpstreamName)

			var jobBDestination job.ResourceURN
			jobBUpstreamName := []job.ResourceURN{"bigquery://project:dataset.tableC"}
			specB, _ := job.NewSpecBuilder(jobVersion, "job-B", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()
			jobB := job.NewJob(sampleTenant, specB, jobBDestination, jobBUpstreamName)

			jobRepo.On("GetAllByTenant", ctx, sampleTenant).Return([]*job.Job{jobA, jobB}, nil)

			tenantDetailsGetter.On("GetDetails", ctx, sampleTenant).Return(detailedTenant, nil)

			pluginService.On("ConstructDestinationURN", ctx, specA.Task().Name().String(), mock.Anything).Return(jobADestination.String(), nil).Once()
			pluginService.On("IdentifyUpstreams", ctx, specA.Task().Name().String(), mock.Anything, mock.Anything).Return(jobResourceURNsToString(jobAUpstreamName), nil)

			pluginService.On("ConstructDestinationURN", ctx, specB.Task().Name().String(), mock.Anything).Return(jobBDestination.String(), nil).Once()
			pluginService.On("IdentifyUpstreams", ctx, specB.Task().Name().String(), mock.Anything, mock.Anything).Return(jobResourceURNsToString(jobBUpstreamName), nil)

			jobRepo.On("Update", ctx, mock.Anything).Return([]*job.Job{jobA, jobB}, nil)

			upstreamB := job.NewUpstreamResolved("job-B", "", "bigquery://project:dataset.tableB", sampleTenant, "static", taskName, false)
			jobAWithUpstream := job.NewWithUpstream(jobA, []*job.Upstream{upstreamB})
			upstreamC := job.NewUpstreamResolved("job-C", "", "bigquery://project:dataset.tableC", sampleTenant, "static", taskName, false)
			jobBWithUpstream := job.NewWithUpstream(jobB, []*job.Upstream{upstreamC})
			upstreamResolver.On("BulkResolve", ctx, project.Name(), []*job.Job{jobA, jobB}, mock.Anything).Return([]*job.WithUpstream{jobAWithUpstream, jobBWithUpstream}, nil)

			upstreamRepo.On("ReplaceUpstreams", ctx, []*job.WithUpstream{jobAWithUpstream, jobBWithUpstream}).Return(nil)

			logWriter.On("Write", mock.Anything, mock.Anything).Return(nil).Times(3)
			eventHandler.On("HandleEvent", mock.Anything).Times(2)

			var jobNamesToRemove []string
			jobNamesToUpload := []string{jobA.GetName(), jobB.GetName()}
			jobDeploymentService.On("UploadJobs", ctx, sampleTenant, jobNamesToUpload, jobNamesToRemove).Return(nil)

			jobService := service.NewJobService(jobRepo, upstreamRepo, downstreamRepo, pluginService, upstreamResolver, tenantDetailsGetter, eventHandler, log, jobDeploymentService, compiler.NewEngine())
			err := jobService.Refresh(ctx, project.Name(), []string{namespace.Name().String()}, nil, logWriter)
			assert.NoError(t, err)
		})
		t.Run("resolves and saves upstream for all existing jobs for multiple tenant", func(t *testing.T) {
			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			upstreamRepo := new(UpstreamRepository)
			defer upstreamRepo.AssertExpectations(t)

			downstreamRepo := new(DownstreamRepository)
			defer downstreamRepo.AssertExpectations(t)

			pluginService := new(PluginService)
			defer pluginService.AssertExpectations(t)

			upstreamResolver := new(UpstreamResolver)
			defer upstreamResolver.AssertExpectations(t)

			tenantDetailsGetter := new(TenantDetailsGetter)
			defer tenantDetailsGetter.AssertExpectations(t)

			logWriter := new(mockWriter)
			defer logWriter.AssertExpectations(t)

			jobDeploymentService := new(JobDeploymentService)
			defer jobDeploymentService.AssertExpectations(t)

			eventHandler := newEventHandler(t)

			specA, _ := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()
			jobADestination := job.ResourceURN("bigquery://project:dataset.tableA")
			jobAUpstreamName := []job.ResourceURN{"bigquery://project:dataset.tableB"}
			jobA := job.NewJob(sampleTenant, specA, jobADestination, jobAUpstreamName)
			jobsTenant1 := []*job.Job{jobA}

			var jobBDestination job.ResourceURN
			var jobBUpstreamName []job.ResourceURN
			specB, _ := job.NewSpecBuilder(jobVersion, "job-B", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()
			jobB := job.NewJob(otherTenant, specB, jobBDestination, jobBUpstreamName)
			jobsTenant2 := []*job.Job{jobB}

			jobRepo.On("GetAllByTenant", ctx, sampleTenant).Return(jobsTenant1, nil).Once()
			jobRepo.On("GetAllByTenant", ctx, otherTenant).Return(jobsTenant2, nil).Once()

			tenantDetailsGetter.On("GetDetails", ctx, sampleTenant).Return(detailedTenant, nil).Once()
			tenantDetailsGetter.On("GetDetails", ctx, otherTenant).Return(detailedOtherTenant, nil).Once()

			pluginService.On("ConstructDestinationURN", ctx, specA.Task().Name().String(), mock.Anything).Return(jobADestination.String(), nil).Once()
			pluginService.On("IdentifyUpstreams", ctx, specA.Task().Name().String(), mock.Anything, mock.Anything).Return(jobResourceURNsToString(jobAUpstreamName), nil).Once()
			pluginService.On("ConstructDestinationURN", ctx, specB.Task().Name().String(), mock.Anything).Return(jobBDestination.String(), nil).Once()
			pluginService.On("IdentifyUpstreams", ctx, specB.Task().Name().String(), mock.Anything, mock.Anything).Return(jobResourceURNsToString(jobBUpstreamName), nil).Once()

			jobRepo.On("Update", ctx, mock.Anything).Return([]*job.Job{jobA}, nil).Once()
			jobRepo.On("Update", ctx, mock.Anything).Return([]*job.Job{jobB}, nil).Once()

			upstreamB := job.NewUpstreamResolved("job-B", "", "bigquery://project:dataset.tableB", sampleTenant, "static", taskName, false)
			jobAWithUpstream := job.NewWithUpstream(jobA, []*job.Upstream{upstreamB})
			upstreamC := job.NewUpstreamResolved("job-C", "", "bigquery://project:dataset.tableC", otherTenant, "static", taskName, false)
			jobBWithUpstream := job.NewWithUpstream(jobB, []*job.Upstream{upstreamC})

			upstreamResolver.On("BulkResolve", ctx, project.Name(), []*job.Job{jobA}, mock.Anything).Return([]*job.WithUpstream{jobAWithUpstream}, nil)
			upstreamResolver.On("BulkResolve", ctx, project.Name(), []*job.Job{jobB}, mock.Anything).Return([]*job.WithUpstream{jobBWithUpstream}, nil)

			upstreamRepo.On("ReplaceUpstreams", ctx, []*job.WithUpstream{jobAWithUpstream}).Return(nil)
			upstreamRepo.On("ReplaceUpstreams", ctx, []*job.WithUpstream{jobBWithUpstream}).Return(nil)

			logWriter.On("Write", mock.Anything, mock.Anything).Return(nil).Times(4)
			eventHandler.On("HandleEvent", mock.Anything).Times(2)

			var jobNamesToRemove []string
			jobDeploymentService.On("UploadJobs", ctx, sampleTenant, []string{jobA.GetName()}, jobNamesToRemove).Return(nil)
			jobDeploymentService.On("UploadJobs", ctx, otherTenant, []string{jobB.GetName()}, jobNamesToRemove).Return(nil)

			jobService := service.NewJobService(jobRepo, upstreamRepo, downstreamRepo, pluginService, upstreamResolver, tenantDetailsGetter, eventHandler, log, jobDeploymentService, compiler.NewEngine())
			err := jobService.Refresh(ctx, project.Name(), []string{namespace.Name().String(), otherNamespace.Name().String()}, nil, logWriter)
			assert.NoError(t, err)
		})
		t.Run("returns error if unable to get existing jobs", func(t *testing.T) {
			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			upstreamRepo := new(UpstreamRepository)
			defer upstreamRepo.AssertExpectations(t)

			downstreamRepo := new(DownstreamRepository)
			defer downstreamRepo.AssertExpectations(t)

			pluginService := new(PluginService)
			defer pluginService.AssertExpectations(t)

			upstreamResolver := new(UpstreamResolver)
			defer upstreamResolver.AssertExpectations(t)

			tenantDetailsGetter := new(TenantDetailsGetter)
			defer tenantDetailsGetter.AssertExpectations(t)

			jobRepo.On("GetAllByTenant", ctx, sampleTenant).Return(nil, errors.New("internal error"))

			jobService := service.NewJobService(jobRepo, upstreamRepo, downstreamRepo, pluginService, upstreamResolver, tenantDetailsGetter, nil, log, nil, compiler.NewEngine())
			err := jobService.Refresh(ctx, project.Name(), []string{namespace.Name().String()}, nil, nil)
			assert.ErrorContains(t, err, "internal error")
		})
	})

	t.Run("RefreshResourceDownstream", func(t *testing.T) {
		t.Run("returns error if encountered error when identifying downstreams", func(t *testing.T) {
			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			upstreamRepo := new(UpstreamRepository)
			defer upstreamRepo.AssertExpectations(t)

			downstreamRepo := new(DownstreamRepository)
			defer downstreamRepo.AssertExpectations(t)

			pluginService := new(PluginService)
			defer pluginService.AssertExpectations(t)

			upstreamResolver := new(UpstreamResolver)
			defer upstreamResolver.AssertExpectations(t)

			tenantDetailsGetter := new(TenantDetailsGetter)
			defer tenantDetailsGetter.AssertExpectations(t)

			logWriter := new(mockWriter)
			defer logWriter.AssertExpectations(t)

			jobDeploymentService := new(JobDeploymentService)
			defer jobDeploymentService.AssertExpectations(t)

			eventHandler := newEventHandler(t)

			resourceURNs := []job.ResourceURN{"bigquery://project:dataset.table"}

			jobService := service.NewJobService(jobRepo, upstreamRepo, downstreamRepo, pluginService, upstreamResolver, tenantDetailsGetter, eventHandler, log, jobDeploymentService, compiler.NewEngine())

			downstreamRepo.On("GetDownstreamBySources", ctx, resourceURNs).Return(nil, errors.New("internal error"))

			err := jobService.RefreshResourceDownstream(ctx, resourceURNs, logWriter)
			assert.ErrorContains(t, err, "internal error")
		})

		t.Run("should refresh and return nil if no error is encountered when refreshing downstreams", func(t *testing.T) {
			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			upstreamRepo := new(UpstreamRepository)
			defer upstreamRepo.AssertExpectations(t)

			downstreamRepo := new(DownstreamRepository)
			defer downstreamRepo.AssertExpectations(t)

			pluginService := new(PluginService)
			defer pluginService.AssertExpectations(t)

			upstreamResolver := new(UpstreamResolver)
			defer upstreamResolver.AssertExpectations(t)

			tenantDetailsGetter := new(TenantDetailsGetter)
			defer tenantDetailsGetter.AssertExpectations(t)

			logWriter := new(mockWriter)
			defer logWriter.AssertExpectations(t)

			jobDeploymentService := new(JobDeploymentService)
			defer jobDeploymentService.AssertExpectations(t)

			eventHandler := newEventHandler(t)

			specA, _ := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()
			jobADestination := job.ResourceURN("bigquery://project:dataset.tableA")
			jobAUpstreamName := job.ResourceURN("bigquery://project:dataset.tableB")
			jobA := job.NewJob(sampleTenant, specA, jobADestination, []job.ResourceURN{jobAUpstreamName})

			specB, _ := job.NewSpecBuilder(jobVersion, "job-B", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()
			jobBDestination := job.ResourceURN("bigquery://project:dataset.tableB")
			jobBUpstreamName := job.ResourceURN("bigquery://project:dataset.tableC")
			jobB := job.NewJob(sampleTenant, specB, jobBDestination, []job.ResourceURN{jobBUpstreamName})

			resourceURNs := []job.ResourceURN{jobAUpstreamName, jobBUpstreamName}

			jobDownstreams := []*job.Downstream{
				job.NewDownstream(jobA.Spec().Name(), sampleTenant.ProjectName(), sampleTenant.NamespaceName(), jobTask.Name()),
				job.NewDownstream(jobB.Spec().Name(), sampleTenant.ProjectName(), sampleTenant.NamespaceName(), jobTask.Name()),
			}

			downstreamRepo.On("GetDownstreamBySources", ctx, resourceURNs).Return(jobDownstreams, nil)
			jobRepo.On("GetByJobName", ctx, sampleTenant.ProjectName(), jobA.Spec().Name()).Return(jobA, nil)
			jobRepo.On("GetByJobName", ctx, sampleTenant.ProjectName(), jobB.Spec().Name()).Return(jobB, nil)

			tenantDetailsGetter.On("GetDetails", ctx, sampleTenant).Return(detailedTenant, nil)

			pluginService.On("ConstructDestinationURN", ctx, specA.Task().Name().String(), mock.Anything).Return(jobADestination.String(), nil).Once()
			pluginService.On("IdentifyUpstreams", ctx, specA.Task().Name().String(), mock.Anything, mock.Anything).Return(jobResourceURNsToString([]job.ResourceURN{jobAUpstreamName}), nil)

			pluginService.On("ConstructDestinationURN", ctx, specB.Task().Name().String(), mock.Anything).Return(jobBDestination.String(), nil).Once()
			pluginService.On("IdentifyUpstreams", ctx, specB.Task().Name().String(), mock.Anything, mock.Anything).Return(jobResourceURNsToString([]job.ResourceURN{jobBUpstreamName}), nil)

			jobRepo.On("Update", ctx, mock.Anything).Return([]*job.Job{jobA, jobB}, nil)

			upstreamB := job.NewUpstreamResolved("job-B", "", "bigquery://project:dataset.tableB", sampleTenant, "static", taskName, false)
			jobAWithUpstream := job.NewWithUpstream(jobA, []*job.Upstream{upstreamB})
			upstreamC := job.NewUpstreamResolved("job-C", "", "bigquery://project:dataset.tableC", sampleTenant, "static", taskName, false)
			jobBWithUpstream := job.NewWithUpstream(jobB, []*job.Upstream{upstreamC})
			upstreamResolver.On("BulkResolve", ctx, project.Name(), []*job.Job{jobA, jobB}, mock.Anything).Return([]*job.WithUpstream{jobAWithUpstream, jobBWithUpstream}, nil)

			upstreamRepo.On("ReplaceUpstreams", ctx, []*job.WithUpstream{jobAWithUpstream, jobBWithUpstream}).Return(nil)

			logWriter.On("Write", mock.Anything, mock.Anything).Return(nil).Times(3)
			eventHandler.On("HandleEvent", mock.Anything).Times(2)

			var jobNamesToRemove []string
			jobNamesToUpload := []string{jobA.GetName(), jobB.GetName()}
			jobDeploymentService.On("UploadJobs", ctx, sampleTenant, jobNamesToUpload, jobNamesToRemove).Return(nil)

			jobService := service.NewJobService(jobRepo, upstreamRepo, downstreamRepo, pluginService, upstreamResolver, tenantDetailsGetter, eventHandler, log, jobDeploymentService, compiler.NewEngine())

			err := jobService.RefreshResourceDownstream(ctx, resourceURNs, logWriter)
			assert.NoError(t, err)
		})
	})

	t.Run("Get", func(t *testing.T) {
		t.Run("return error when repo get by job name error", func(t *testing.T) {
			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			upstreamRepo := new(UpstreamRepository)
			defer upstreamRepo.AssertExpectations(t)

			downstreamRepo := new(DownstreamRepository)
			defer downstreamRepo.AssertExpectations(t)

			jobName, _ := job.NameFrom("job-A")
			jobRepo.On("GetByJobName", ctx, sampleTenant.ProjectName(), jobName).Return(nil, errors.New("error when fetch job"))

			jobService := service.NewJobService(jobRepo, upstreamRepo, downstreamRepo, nil, nil, nil, nil, log, nil, nil)

			actual, err := jobService.Get(ctx, sampleTenant, jobName)
			assert.Error(t, err, "error when fetch job")
			assert.Nil(t, actual)
		})
		t.Run("return job when success fetch the job", func(t *testing.T) {
			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			upstreamRepo := new(UpstreamRepository)
			defer upstreamRepo.AssertExpectations(t)

			downstreamRepo := new(DownstreamRepository)
			defer downstreamRepo.AssertExpectations(t)

			specA, _ := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()
			jobA := job.NewJob(sampleTenant, specA, "table-A", []job.ResourceURN{"bigquery://project:dataset.tableB"})
			jobRepo.On("GetByJobName", ctx, sampleTenant.ProjectName(), specA.Name()).Return(jobA, nil)

			jobService := service.NewJobService(jobRepo, upstreamRepo, downstreamRepo, nil, nil, nil, nil, log, nil, nil)

			actual, err := jobService.Get(ctx, sampleTenant, specA.Name())
			assert.NoError(t, err, "error when fetch job")
			assert.NotNil(t, actual)
			assert.Equal(t, jobA, actual)
		})
	})

	t.Run("GetByFilter", func(t *testing.T) {
		t.Run("filter by resource destination", func(t *testing.T) {
			t.Run("return error when repo error", func(t *testing.T) {
				jobRepo := new(JobRepository)
				defer jobRepo.AssertExpectations(t)

				jobRepo.On("GetAllByResourceDestination", ctx, job.ResourceURN("bigquery://project:dataset.example")).Return(nil, errors.New("error encountered"))

				jobService := service.NewJobService(jobRepo, nil, nil, nil, nil, nil, nil, log, nil, nil)
				actual, err := jobService.GetByFilter(ctx, filter.WithString(filter.ResourceDestination, "bigquery://project:dataset.example"))
				assert.Error(t, err, "error encountered")
				assert.Nil(t, actual)
			})
			t.Run("return success", func(t *testing.T) {
				jobRepo := new(JobRepository)
				defer jobRepo.AssertExpectations(t)

				specA, _ := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()
				jobA := job.NewJob(sampleTenant, specA, "table-A", []job.ResourceURN{"bigquery://project:dataset.tableB"})
				jobRepo.On("GetAllByResourceDestination", ctx, job.ResourceURN("bigquery://project:dataset.tableA")).Return([]*job.Job{jobA}, nil)

				jobService := service.NewJobService(jobRepo, nil, nil, nil, nil, nil, nil, log, nil, nil)
				actual, err := jobService.GetByFilter(ctx, filter.WithString(filter.ResourceDestination, "bigquery://project:dataset.tableA"))
				assert.NoError(t, err)
				assert.NotNil(t, actual)
				assert.NotEmpty(t, actual)
				assert.Len(t, actual, 1)
			})
		})
		t.Run("filter by project name and job names", func(t *testing.T) {
			t.Run("return error when repo error", func(t *testing.T) {
				jobRepo := new(JobRepository)
				defer jobRepo.AssertExpectations(t)

				jobName, _ := job.NameFrom("job-A")
				jobRepo.On("GetByJobName", ctx, sampleTenant.ProjectName(), jobName).Return(nil, errors.New("error encountered"))

				jobService := service.NewJobService(jobRepo, nil, nil, nil, nil, nil, nil, log, nil, nil)
				actual, err := jobService.GetByFilter(ctx,
					filter.WithString(filter.ProjectName, sampleTenant.ProjectName().String()),
					filter.WithStringArray(filter.JobNames, []string{jobName.String()}),
				)
				assert.Error(t, err, "error encountered")
				assert.Nil(t, actual)
			})
			t.Run("return success and some error when some of job is failed to retrieved", func(t *testing.T) {
				jobRepo := new(JobRepository)
				defer jobRepo.AssertExpectations(t)

				specA, _ := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()
				jobA := job.NewJob(sampleTenant, specA, "table-A", []job.ResourceURN{"bigquery://project:dataset.tableB"})
				specB, _ := job.NewSpecBuilder(jobVersion, "job-B", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()
				jobRepo.On("GetByJobName", ctx, sampleTenant.ProjectName(), specA.Name()).Return(jobA, nil)
				jobRepo.On("GetByJobName", ctx, sampleTenant.ProjectName(), specB.Name()).Return(nil, errors.New("error encountered"))

				jobService := service.NewJobService(jobRepo, nil, nil, nil, nil, nil, nil, log, nil, nil)
				actual, err := jobService.GetByFilter(ctx,
					filter.WithString(filter.ProjectName, sampleTenant.ProjectName().String()),
					filter.WithStringArray(filter.JobNames, []string{specA.Name().String(), specB.Name().String()}),
				)
				assert.Error(t, err)
				assert.NotNil(t, actual)
				assert.NotEmpty(t, actual)
				assert.Len(t, actual, 1)
			})
			t.Run("not return error if the record is not found", func(t *testing.T) {
				jobRepo := new(JobRepository)
				defer jobRepo.AssertExpectations(t)

				specA, _ := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()
				jobRepo.On("GetByJobName", ctx, sampleTenant.ProjectName(), specA.Name()).Return(nil, optErrors.NotFound(job.EntityJob, "job not found"))

				jobService := service.NewJobService(jobRepo, nil, nil, nil, nil, nil, nil, log, nil, nil)
				actual, err := jobService.GetByFilter(ctx,
					filter.WithString(filter.ProjectName, sampleTenant.ProjectName().String()),
					filter.WithStringArray(filter.JobNames, []string{specA.Name().String()}),
				)
				assert.NoError(t, err)
				assert.Nil(t, actual)
			})
			t.Run("return success", func(t *testing.T) {
				jobRepo := new(JobRepository)
				defer jobRepo.AssertExpectations(t)

				specA, _ := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()
				jobA := job.NewJob(sampleTenant, specA, "table-A", []job.ResourceURN{"bigquery://project:dataset.tableB"})
				jobRepo.On("GetByJobName", ctx, sampleTenant.ProjectName(), specA.Name()).Return(jobA, nil)

				jobService := service.NewJobService(jobRepo, nil, nil, nil, nil, nil, nil, log, nil, nil)
				actual, err := jobService.GetByFilter(ctx,
					filter.WithString(filter.ProjectName, sampleTenant.ProjectName().String()),
					filter.WithStringArray(filter.JobNames, []string{specA.Name().String()}),
				)
				assert.NoError(t, err)
				assert.NotNil(t, actual)
				assert.NotEmpty(t, actual)
				assert.Len(t, actual, 1)
			})
		})
		t.Run("filter by project name and job name", func(t *testing.T) {
			t.Run("return error when repo error", func(t *testing.T) {
				jobRepo := new(JobRepository)
				defer jobRepo.AssertExpectations(t)

				jobName, _ := job.NameFrom("job-A")
				jobRepo.On("GetByJobName", ctx, sampleTenant.ProjectName(), jobName).Return(nil, errors.New("error encountered"))

				jobService := service.NewJobService(jobRepo, nil, nil, nil, nil, nil, nil, log, nil, nil)
				actual, err := jobService.GetByFilter(ctx,
					filter.WithString(filter.ProjectName, sampleTenant.ProjectName().String()),
					filter.WithString(filter.JobName, jobName.String()),
				)
				assert.Error(t, err, "error encountered")
				assert.Nil(t, actual)
			})
			t.Run("not return error if the record is not found", func(t *testing.T) {
				jobRepo := new(JobRepository)
				defer jobRepo.AssertExpectations(t)

				specA, _ := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()
				jobRepo.On("GetByJobName", ctx, sampleTenant.ProjectName(), specA.Name()).Return(nil, optErrors.NotFound(job.EntityJob, "job not found"))

				jobService := service.NewJobService(jobRepo, nil, nil, nil, nil, nil, nil, log, nil, nil)
				actual, err := jobService.GetByFilter(ctx,
					filter.WithString(filter.ProjectName, sampleTenant.ProjectName().String()),
					filter.WithString(filter.JobName, specA.Name().String()),
				)
				assert.NoError(t, err)
				assert.Empty(t, actual)
			})
			t.Run("return success", func(t *testing.T) {
				jobRepo := new(JobRepository)
				defer jobRepo.AssertExpectations(t)

				specA, _ := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()
				jobA := job.NewJob(sampleTenant, specA, "table-A", []job.ResourceURN{"bigquery://project:dataset.tableB"})
				jobRepo.On("GetByJobName", ctx, sampleTenant.ProjectName(), specA.Name()).Return(jobA, nil)

				jobService := service.NewJobService(jobRepo, nil, nil, nil, nil, nil, nil, log, nil, nil)
				actual, err := jobService.GetByFilter(ctx,
					filter.WithString(filter.ProjectName, sampleTenant.ProjectName().String()),
					filter.WithString(filter.JobName, specA.Name().String()),
				)
				assert.NoError(t, err)
				assert.NotNil(t, actual)
				assert.NotEmpty(t, actual)
				assert.Equal(t, []*job.Job{jobA}, actual)
			})
		})
		t.Run("filter by project name and namespace names", func(t *testing.T) {
			t.Run("return error when repo error", func(t *testing.T) {
				jobRepo := new(JobRepository)
				defer jobRepo.AssertExpectations(t)

				jobRepo.On("GetAllByTenant", ctx, sampleTenant).Return(nil, errors.New("error encountered"))

				jobService := service.NewJobService(jobRepo, nil, nil, nil, nil, nil, nil, log, nil, nil)
				actual, err := jobService.GetByFilter(ctx,
					filter.WithString(filter.ProjectName, sampleTenant.ProjectName().String()),
					filter.WithStringArray(filter.NamespaceNames, []string{sampleTenant.NamespaceName().String()}),
				)
				assert.Error(t, err, "error encountered")
				assert.Nil(t, actual)
			})
			t.Run("return error when namespace empty", func(t *testing.T) {
				jobRepo := new(JobRepository)
				defer jobRepo.AssertExpectations(t)

				jobService := service.NewJobService(jobRepo, nil, nil, nil, nil, nil, nil, log, nil, nil)
				actual, err := jobService.GetByFilter(ctx,
					filter.WithString(filter.ProjectName, sampleTenant.ProjectName().String()),
					filter.WithStringArray(filter.NamespaceNames, []string{""}),
				)
				assert.Error(t, err)
				assert.Nil(t, actual)
			})
			t.Run("return success", func(t *testing.T) {
				jobRepo := new(JobRepository)
				defer jobRepo.AssertExpectations(t)

				specA, _ := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()
				jobA := job.NewJob(sampleTenant, specA, "table-A", []job.ResourceURN{"bigquery://project:dataset.tableB"})
				jobRepo.On("GetAllByTenant", ctx, sampleTenant).Return([]*job.Job{jobA}, nil)

				jobService := service.NewJobService(jobRepo, nil, nil, nil, nil, nil, nil, log, nil, nil)
				actual, err := jobService.GetByFilter(ctx,
					filter.WithString(filter.ProjectName, sampleTenant.ProjectName().String()),
					filter.WithStringArray(filter.NamespaceNames, []string{sampleTenant.NamespaceName().String()}),
				)
				assert.NoError(t, err)
				assert.NotNil(t, actual)
				assert.NotEmpty(t, actual)
				assert.Len(t, actual, 1)
			})
		})
		t.Run("filter by project name and namespace name", func(t *testing.T) {
			t.Run("return error when repo error", func(t *testing.T) {
				jobRepo := new(JobRepository)
				defer jobRepo.AssertExpectations(t)

				jobRepo.On("GetAllByTenant", ctx, sampleTenant).Return(nil, errors.New("error encountered"))

				jobService := service.NewJobService(jobRepo, nil, nil, nil, nil, nil, nil, log, nil, nil)
				actual, err := jobService.GetByFilter(ctx,
					filter.WithString(filter.ProjectName, sampleTenant.ProjectName().String()),
					filter.WithString(filter.NamespaceName, sampleTenant.NamespaceName().String()),
				)
				assert.Error(t, err, "error encountered")
				assert.Nil(t, actual)
			})
			t.Run("return success", func(t *testing.T) {
				jobRepo := new(JobRepository)
				defer jobRepo.AssertExpectations(t)

				specA, _ := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()
				jobA := job.NewJob(sampleTenant, specA, "table-A", []job.ResourceURN{"bigquery://project:dataset.tableB"})
				jobRepo.On("GetAllByTenant", ctx, sampleTenant).Return([]*job.Job{jobA}, nil)

				jobService := service.NewJobService(jobRepo, nil, nil, nil, nil, nil, nil, log, nil, nil)
				actual, err := jobService.GetByFilter(ctx,
					filter.WithString(filter.ProjectName, sampleTenant.ProjectName().String()),
					filter.WithString(filter.NamespaceName, sampleTenant.NamespaceName().String()),
				)
				assert.NoError(t, err)
				assert.NotNil(t, actual)
				assert.NotEmpty(t, actual)
				assert.Len(t, actual, 1)
			})
		})
		t.Run("filter by project name", func(t *testing.T) {
			t.Run("return error when repo error", func(t *testing.T) {
				jobRepo := new(JobRepository)
				defer jobRepo.AssertExpectations(t)

				jobRepo.On("GetAllByProjectName", ctx, sampleTenant.ProjectName()).Return(nil, errors.New("error encountered"))

				jobService := service.NewJobService(jobRepo, nil, nil, nil, nil, nil, nil, log, nil, nil)
				actual, err := jobService.GetByFilter(ctx,
					filter.WithString(filter.ProjectName, sampleTenant.ProjectName().String()),
				)
				assert.Error(t, err, "error encountered")
				assert.Nil(t, actual)
			})
			t.Run("return success", func(t *testing.T) {
				jobRepo := new(JobRepository)
				defer jobRepo.AssertExpectations(t)

				specA, _ := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()
				jobA := job.NewJob(sampleTenant, specA, "table-A", []job.ResourceURN{"bigquery://project:dataset.tableB"})
				jobRepo.On("GetAllByProjectName", ctx, sampleTenant.ProjectName()).Return([]*job.Job{jobA}, nil)

				jobService := service.NewJobService(jobRepo, nil, nil, nil, nil, nil, nil, log, nil, nil)
				actual, err := jobService.GetByFilter(ctx,
					filter.WithString(filter.ProjectName, sampleTenant.ProjectName().String()),
				)
				assert.NoError(t, err)
				assert.NotNil(t, actual)
				assert.NotEmpty(t, actual)
				assert.Len(t, actual, 1)
			})
		})
		t.Run("return error when there's no filter", func(t *testing.T) {
			jobService := service.NewJobService(nil, nil, nil, nil, nil, nil, nil, log, nil, nil)
			actual, err := jobService.GetByFilter(ctx)
			assert.Error(t, err, "no filter matched")
			assert.Nil(t, actual)
		})
	})

	t.Run("GetTaskInfo", func(t *testing.T) {
		t.Run("return error when plugin could not retrieve info", func(t *testing.T) {
			pluginService := new(PluginService)
			defer pluginService.AssertExpectations(t)

			pluginService.On("Info", ctx, jobTask.Name().String()).Return(nil, errors.New("error encountered"))

			jobService := service.NewJobService(nil, nil, nil, pluginService, nil, nil, nil, nil, nil, nil)

			actual, err := jobService.GetTaskInfo(ctx, jobTask)
			assert.Error(t, err, "error encountered")
			assert.Nil(t, actual)
		})
		t.Run("return task with information included when success", func(t *testing.T) {
			pluginService := new(PluginService)
			defer pluginService.AssertExpectations(t)

			pluginInfoResp := &plugin.Info{
				Name:        "bq2bq",
				Description: "plugin desc",
				Image:       "goto/bq2bq:latest",
			}
			pluginService.On("Info", ctx, jobTask.Name().String()).Return(pluginInfoResp, nil)

			jobService := service.NewJobService(nil, nil, nil, pluginService, nil, nil, nil, nil, nil, nil)

			actual, err := jobService.GetTaskInfo(ctx, jobTask)
			assert.NoError(t, err)
			assert.NotNil(t, actual)
			assert.Equal(t, pluginInfoResp, actual)
		})
	})

	t.Run("Validate", func(t *testing.T) {
		t.Run("returns error when get tenant details if failed", func(t *testing.T) {
			tenantDetailsGetter := new(TenantDetailsGetter)
			tenantDetailsGetter.On("GetDetails", ctx, mock.Anything).Return(nil, errors.New("get tenant details fail"))
			defer tenantDetailsGetter.AssertExpectations(t)

			jobService := service.NewJobService(nil, nil, nil, nil, nil, tenantDetailsGetter, nil, log, nil, compiler.NewEngine())
			err := jobService.Validate(ctx, sampleTenant, []*job.Spec{}, jobNamesWithInvalidSpec, nil)
			assert.Error(t, err)
			assert.Equal(t, "get tenant details fail", err.Error())
		})
		t.Run("returns error when validate duplicate jobs detected", func(t *testing.T) {
			tenantDetailsGetter := new(TenantDetailsGetter)
			tenantDetailsGetter.On("GetDetails", ctx, mock.Anything).Return(detailedTenant, nil)
			defer tenantDetailsGetter.AssertExpectations(t)

			pluginService := new(PluginService)
			defer pluginService.AssertExpectations(t)

			upstreamResolver := new(UpstreamResolver)
			defer upstreamResolver.AssertExpectations(t)

			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			upstreamRepo := new(UpstreamRepository)
			defer upstreamRepo.AssertExpectations(t)

			downstreamRepo := new(DownstreamRepository)
			defer downstreamRepo.AssertExpectations(t)

			logWriter := new(mockWriter)
			defer logWriter.AssertExpectations(t)

			specA1, _ := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()
			specA2, _ := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()

			jobRepo.On("GetAllByTenant", ctx, sampleTenant).Return(nil, nil)

			jobService := service.NewJobService(jobRepo, upstreamRepo, downstreamRepo, pluginService, upstreamResolver, tenantDetailsGetter, nil, log, nil, compiler.NewEngine())

			logWriter.On("Write", mock.Anything, mock.Anything).Return(nil)

			err := jobService.Validate(ctx, sampleTenant, []*job.Spec{specA1, specA2}, jobNamesWithInvalidSpec, logWriter)
			assert.Error(t, err)
			assert.Equal(t, "validate specs errors:\n duplicate job-A", err.Error())
		})
		t.Run("returns error when generate jobs", func(t *testing.T) {
			tenantDetailsGetter := new(TenantDetailsGetter)
			tenantDetailsGetter.On("GetDetails", ctx, mock.Anything).Return(detailedTenant, nil)
			defer tenantDetailsGetter.AssertExpectations(t)

			pluginService := new(PluginService)
			defer pluginService.AssertExpectations(t)

			upstreamResolver := new(UpstreamResolver)
			defer upstreamResolver.AssertExpectations(t)

			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			upstreamRepo := new(UpstreamRepository)
			defer upstreamRepo.AssertExpectations(t)

			downstreamRepo := new(DownstreamRepository)
			defer downstreamRepo.AssertExpectations(t)

			logWriter := new(mockWriter)
			defer logWriter.AssertExpectations(t)

			specA, _ := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()

			jobRepo.On("GetAllByTenant", ctx, sampleTenant).Return(nil, nil)

			pluginService.On("ConstructDestinationURN", ctx, specA.Task().Name().String(), mock.Anything).Return("", errors.New("some error on generate destination"))

			jobService := service.NewJobService(jobRepo, upstreamRepo, downstreamRepo, pluginService, upstreamResolver, tenantDetailsGetter, nil, log, nil, compiler.NewEngine())

			logWriter.On("Write", mock.Anything, mock.Anything).Return(nil)

			err := jobService.Validate(ctx, sampleTenant, []*job.Spec{specA}, jobNamesWithInvalidSpec, logWriter)
			assert.Error(t, err)
			assert.Equal(t, "validate specs errors:\n some error on generate destination", err.Error())
		})
		t.Run("returns error when a job fail a deletion check", func(t *testing.T) {
			tenantDetailsGetter := new(TenantDetailsGetter)
			tenantDetailsGetter.On("GetDetails", ctx, mock.Anything).Return(detailedTenant, nil)
			defer tenantDetailsGetter.AssertExpectations(t)

			pluginService := new(PluginService)
			defer pluginService.AssertExpectations(t)

			upstreamResolver := new(UpstreamResolver)
			defer upstreamResolver.AssertExpectations(t)

			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			upstreamRepo := new(UpstreamRepository)
			defer upstreamRepo.AssertExpectations(t)

			downstreamRepo := new(DownstreamRepository)
			defer downstreamRepo.AssertExpectations(t)

			logWriter := new(mockWriter)
			defer logWriter.AssertExpectations(t)

			jobTaskConfigA, _ := job.ConfigFrom(map[string]string{"table": "table-A", "BQ_SERVICE_ACCOUNT": "secret_account"})
			jobTaskConfigB, _ := job.ConfigFrom(map[string]string{"table": "table-B", "BQ_SERVICE_ACCOUNT": "secret_account"})
			jobTaskConfigC, _ := job.ConfigFrom(map[string]string{"table": "table-C", "BQ_SERVICE_ACCOUNT": "secret_account"})
			jobTaskConfigD, _ := job.ConfigFrom(map[string]string{"table": "table-D", "BQ_SERVICE_ACCOUNT": "secret_account"})
			jobTaskA := job.NewTask(taskName, jobTaskConfigA)
			jobTaskB := job.NewTask(taskName, jobTaskConfigB)
			jobTaskC := job.NewTask(taskName, jobTaskConfigC)
			jobTaskD := job.NewTask(taskName, jobTaskConfigD)

			specA, _ := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, jobWindow, jobTaskA).WithAsset(jobAsset).Build()
			specAUpdated, _ := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner-updated", jobSchedule, jobWindow, jobTaskA).WithAsset(jobAsset).Build()
			specB, _ := job.NewSpecBuilder(jobVersion, "job-B", "sample-owner", jobSchedule, jobWindow, jobTaskB).WithAsset(jobAsset).Build()
			specC, _ := job.NewSpecBuilder(jobVersion, "job-C", "sample-owner", jobSchedule, jobWindow, jobTaskC).WithAsset(jobAsset).Build()
			specD, _ := job.NewSpecBuilder(jobVersion, "job-D", "sample-owner", jobSchedule, jobWindow, jobTaskD).WithAsset(jobAsset).Build()
			specs := []*job.Spec{specAUpdated, specB, specD}

			jobA := job.NewJob(sampleTenant, specA, "table-A", []job.ResourceURN{"bigquery://project:dataset.tableZ"})
			jobB := job.NewJob(sampleTenant, specB, "table-B", []job.ResourceURN{"bigquery://project:dataset.tableA", "bigquery://project:dataset.tableD"})
			jobC := job.NewJob(sampleTenant, specC, "table-C", []job.ResourceURN{"bigquery://project:dataset.tableB"})
			jobD := job.NewJob(sampleTenant, specD, "table-D", nil)

			jobRepo.On("GetAllByTenant", ctx, sampleTenant).Return([]*job.Job{jobA, jobB, jobC, jobD}, nil)

			pluginService.On("ConstructDestinationURN", ctx, specA.Task().Name().String(), mock.Anything).Return(job.ResourceURN("bigquery://project:dataset.tableA").String(), nil)
			pluginService.On("IdentifyUpstreams", ctx, specA.Task().Name().String(), mock.Anything, mock.Anything).Return(jobResourceURNsToString([]job.ResourceURN{"bigquery://project:dataset.tableZ"}), nil)

			jobCDownstream := []*job.Downstream{
				job.NewDownstream("job-B", project.Name(), namespace.Name(), taskName),
			}
			jobBDownstream := []*job.Downstream{
				job.NewDownstream("job-D", project.Name(), namespace.Name(), taskName),
			}
			downstreamRepo.On("GetDownstreamByJobName", ctx, project.Name(), specC.Name()).Return(jobCDownstream, nil)
			downstreamRepo.On("GetDownstreamByJobName", ctx, project.Name(), specB.Name()).Return(jobBDownstream, nil)
			downstreamRepo.On("GetDownstreamByJobName", ctx, project.Name(), specD.Name()).Return([]*job.Downstream{}, nil)

			logWriter.On("Write", mock.Anything, mock.Anything).Return(nil)

			jobService := service.NewJobService(jobRepo, upstreamRepo, downstreamRepo, pluginService, upstreamResolver, tenantDetailsGetter, nil, log, nil, compiler.NewEngine())
			err := jobService.Validate(ctx, sampleTenant, specs, jobNamesWithInvalidSpec, logWriter)
			assert.Error(t, err)
			assert.ErrorContains(t, err, "deletion of job job-C will fail. job is being used by test-proj/job-B, test-proj/job-D")
		})

		t.Run("returns no error when delete the job and its downstreams", func(t *testing.T) {
			tenantDetailsGetter := new(TenantDetailsGetter)
			tenantDetailsGetter.On("GetDetails", ctx, mock.Anything).Return(detailedTenant, nil)
			defer tenantDetailsGetter.AssertExpectations(t)

			pluginService := new(PluginService)
			defer pluginService.AssertExpectations(t)

			upstreamResolver := new(UpstreamResolver)
			defer upstreamResolver.AssertExpectations(t)

			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			upstreamRepo := new(UpstreamRepository)
			defer upstreamRepo.AssertExpectations(t)

			downstreamRepo := new(DownstreamRepository)
			defer downstreamRepo.AssertExpectations(t)

			logWriter := new(mockWriter)
			defer logWriter.AssertExpectations(t)

			jobTaskConfigA, _ := job.ConfigFrom(map[string]string{"table": "table-A", "BQ_SERVICE_ACCOUNT": "service_account"})
			jobTaskConfigB, _ := job.ConfigFrom(map[string]string{"table": "table-B", "BQ_SERVICE_ACCOUNT": "service_account"})
			jobTaskConfigC, _ := job.ConfigFrom(map[string]string{"table": "table-C", "BQ_SERVICE_ACCOUNT": "service_account"})
			jobTaskA := job.NewTask(taskName, jobTaskConfigA)
			jobTaskB := job.NewTask(taskName, jobTaskConfigB)
			jobTaskC := job.NewTask(taskName, jobTaskConfigC)

			specA, _ := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, jobWindow, jobTaskA).WithAsset(jobAsset).Build()
			specAUpdated, _ := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner-updated", jobSchedule, jobWindow, jobTaskA).WithAsset(jobAsset).Build()
			specB, _ := job.NewSpecBuilder(jobVersion, "job-B", "sample-owner", jobSchedule, jobWindow, jobTaskB).WithAsset(jobAsset).Build()
			specC, _ := job.NewSpecBuilder(jobVersion, "job-C", "sample-owner", jobSchedule, jobWindow, jobTaskC).WithAsset(jobAsset).Build()
			specs := []*job.Spec{specAUpdated}

			jobA := job.NewJob(sampleTenant, specA, "table-A", []job.ResourceURN{"bigquery://project:dataset.tableZ"})
			jobB := job.NewJob(sampleTenant, specB, "table-B", nil)
			jobC := job.NewJob(sampleTenant, specC, "table-C", []job.ResourceURN{"bigquery://project:dataset.tableB"})

			jobRepo.On("GetAllByTenant", ctx, sampleTenant).Return([]*job.Job{jobA, jobB, jobC}, nil)

			pluginService.On("ConstructDestinationURN", ctx, specA.Task().Name().String(), mock.Anything).Return(job.ResourceURN("bigquery://project:dataset.tableA").String(), nil)
			pluginService.On("IdentifyUpstreams", ctx, specA.Task().Name().String(), mock.Anything, mock.Anything).Return(jobResourceURNsToString([]job.ResourceURN{"bigquery://project:dataset.tableZ"}), nil)

			jobCDownstream := []*job.Downstream{
				job.NewDownstream("job-B", project.Name(), namespace.Name(), taskName),
			}
			downstreamRepo.On("GetDownstreamByJobName", ctx, project.Name(), specC.Name()).Return(jobCDownstream, nil)
			downstreamRepo.On("GetDownstreamByJobName", ctx, project.Name(), specB.Name()).Return([]*job.Downstream{}, nil)

			logWriter.On("Write", mock.Anything, mock.Anything).Return(nil)

			jobService := service.NewJobService(jobRepo, upstreamRepo, downstreamRepo, pluginService, upstreamResolver, tenantDetailsGetter, nil, log, nil, compiler.NewEngine())
			err := jobService.Validate(ctx, sampleTenant, specs, jobNamesWithInvalidSpec, logWriter)
			assert.NoError(t, err)
		})

		t.Run("returns error when there's a cyclic", func(t *testing.T) {
			tenantDetailsGetter := new(TenantDetailsGetter)
			tenantDetailsGetter.On("GetDetails", ctx, mock.Anything).Return(detailedTenant, nil)
			defer tenantDetailsGetter.AssertExpectations(t)

			pluginService := new(PluginService)
			defer pluginService.AssertExpectations(t)

			upstreamResolver := new(UpstreamResolver)
			defer upstreamResolver.AssertExpectations(t)

			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			upstreamRepo := new(UpstreamRepository)
			defer upstreamRepo.AssertExpectations(t)

			downstreamRepo := new(DownstreamRepository)
			defer downstreamRepo.AssertExpectations(t)

			logWriter := new(mockWriter)
			defer logWriter.AssertExpectations(t)

			specA, _ := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()
			specB, _ := job.NewSpecBuilder(jobVersion, "job-B", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()
			specC, _ := job.NewSpecBuilder(jobVersion, "job-C", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()

			jobB := job.NewJob(sampleTenant, specB, "bigquery://project:dataset.tableB", []job.ResourceURN{"bigquery://project:dataset.tableA"})
			jobC := job.NewJob(sampleTenant, specC, "bigquery://project:dataset.tableC", []job.ResourceURN{"bigquery://project:dataset.tableB"})

			jobRepo.On("GetAllByTenant", ctx, sampleTenant).Return([]*job.Job{jobB, jobC}, nil)

			pluginService.On("ConstructDestinationURN", ctx, specA.Task().Name().String(), mock.Anything).Return("bigquery://project:dataset.tableA", nil)
			pluginService.On("IdentifyUpstreams", ctx, specA.Task().Name().String(), mock.Anything, mock.Anything).Return(jobResourceURNsToString([]job.ResourceURN{"bigquery://project:dataset.tableC"}), nil)

			logWriter.On("Write", mock.Anything, mock.Anything).Return(nil)

			jobService := service.NewJobService(jobRepo, upstreamRepo, downstreamRepo, pluginService, upstreamResolver, tenantDetailsGetter, nil, log, nil, compiler.NewEngine())
			err := jobService.Validate(ctx, sampleTenant, []*job.Spec{specA, specB, specC}, jobNamesWithInvalidSpec, logWriter)
			assert.Error(t, err)
			assert.ErrorContains(t, err, "a cycle dependency encountered in the tree:")
		})
		t.Run("returns error when there's a cyclic on the incoming job request", func(t *testing.T) {
			tenantDetailsGetter := new(TenantDetailsGetter)
			tenantDetailsGetter.On("GetDetails", ctx, mock.Anything).Return(detailedTenant, nil)
			defer tenantDetailsGetter.AssertExpectations(t)

			pluginService := new(PluginService)
			defer pluginService.AssertExpectations(t)

			upstreamResolver := new(UpstreamResolver)
			defer upstreamResolver.AssertExpectations(t)

			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			upstreamRepo := new(UpstreamRepository)
			defer upstreamRepo.AssertExpectations(t)

			downstreamRepo := new(DownstreamRepository)
			defer downstreamRepo.AssertExpectations(t)

			logWriter := new(mockWriter)
			defer logWriter.AssertExpectations(t)

			jobTaskConfigA, _ := job.ConfigFrom(map[string]string{"table": "table-A", "BQ_SERVICE_ACCOUNT": "service_account"})
			jobTaskConfigB, _ := job.ConfigFrom(map[string]string{"table": "table-B", "BQ_SERVICE_ACCOUNT": "service_account"})
			jobTaskConfigC, _ := job.ConfigFrom(map[string]string{"table": "table-C", "BQ_SERVICE_ACCOUNT": "service_account"})
			jobTaskA := job.NewTask(taskName, jobTaskConfigA)
			jobTaskB := job.NewTask(taskName, jobTaskConfigB)
			jobTaskC := job.NewTask(taskName, jobTaskConfigC)

			specA, _ := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, jobWindow, jobTaskA).WithAsset(jobAsset).Build()
			specAUpdated, _ := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner-updated", jobSchedule, jobWindow, jobTaskA).WithAsset(jobAsset).Build()
			specB, _ := job.NewSpecBuilder(jobVersion, "job-B", "sample-owner", jobSchedule, jobWindow, jobTaskB).WithAsset(jobAsset).Build()
			specC, _ := job.NewSpecBuilder(jobVersion, "job-C", "sample-owner", jobSchedule, jobWindow, jobTaskC).WithAsset(jobAsset).Build()

			jobA := job.NewJob(sampleTenant, specA, "bigquery://project:dataset.tableA", []job.ResourceURN{"bigquery://project:dataset.tableZ"})
			jobB := job.NewJob(sampleTenant, specB, "bigquery://project:dataset.tableB", []job.ResourceURN{"bigquery://project:dataset.tableA"})
			jobC := job.NewJob(sampleTenant, specC, "bigquery://project:dataset.tableC", []job.ResourceURN{"bigquery://project:dataset.tableB"})

			jobRepo.On("GetAllByTenant", ctx, sampleTenant).Return([]*job.Job{jobA, jobB, jobC}, nil)

			pluginService.On("ConstructDestinationURN", ctx, specA.Task().Name().String(), mock.Anything).Return(job.ResourceURN("bigquery://project:dataset.tableA").String(), nil)
			pluginService.On("IdentifyUpstreams", ctx, specA.Task().Name().String(), mock.Anything, mock.Anything).Return(jobResourceURNsToString([]job.ResourceURN{"bigquery://project:dataset.tableC", "bigquery://project:dataset.tableZ"}), nil)

			logWriter.On("Write", mock.Anything, mock.Anything).Return(nil)

			jobService := service.NewJobService(jobRepo, upstreamRepo, downstreamRepo, pluginService, upstreamResolver, tenantDetailsGetter, nil, log, nil, compiler.NewEngine())
			err := jobService.Validate(ctx, sampleTenant, []*job.Spec{specAUpdated, specB, specC}, jobNamesWithInvalidSpec, logWriter)
			assert.Error(t, err)
			assert.ErrorContains(t, err, "a cycle dependency encountered in the tree:")
		})
		t.Run("returns error when there's a cyclic without resource destination", func(t *testing.T) {
			tenantDetailsGetter := new(TenantDetailsGetter)
			tenantDetailsGetter.On("GetDetails", ctx, mock.Anything).Return(detailedTenant, nil)
			defer tenantDetailsGetter.AssertExpectations(t)

			pluginService := new(PluginService)
			defer pluginService.AssertExpectations(t)

			upstreamResolver := new(UpstreamResolver)
			defer upstreamResolver.AssertExpectations(t)

			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			upstreamRepo := new(UpstreamRepository)
			defer upstreamRepo.AssertExpectations(t)

			downstreamRepo := new(DownstreamRepository)
			defer downstreamRepo.AssertExpectations(t)

			logWriter := new(mockWriter)
			defer logWriter.AssertExpectations(t)

			jobTaskConfigA, _ := job.ConfigFrom(map[string]string{"table": "table-A", "BQ_SERVICE_ACCOUNT": "service_account"})
			jobTaskConfigB, _ := job.ConfigFrom(map[string]string{"table": "table-B", "BQ_SERVICE_ACCOUNT": "service_account"})
			jobTaskConfigC, _ := job.ConfigFrom(map[string]string{"example": "value"})
			jobTaskA := job.NewTask(taskName, jobTaskConfigA)
			jobTaskB := job.NewTask(taskName, jobTaskConfigB)
			jobTaskC := job.NewTask("python", jobTaskConfigC)

			upstreamsSpecA, _ := job.NewSpecUpstreamBuilder().WithUpstreamNames([]job.SpecUpstreamName{"test-proj/job-C"}).Build()
			upstreamsSpecC, _ := job.NewSpecUpstreamBuilder().WithUpstreamNames([]job.SpecUpstreamName{"test-proj/job-B"}).Build()
			upstreamsSpecB, _ := job.NewSpecUpstreamBuilder().WithUpstreamNames([]job.SpecUpstreamName{"test-proj/job-A"}).Build()
			specA, _ := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, jobWindow, jobTaskA).WithAsset(jobAsset).WithSpecUpstream(upstreamsSpecA).Build()
			specAUpdated, _ := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner-updated", jobSchedule, jobWindow, jobTaskA).WithAsset(jobAsset).WithSpecUpstream(upstreamsSpecA).Build()
			specB, _ := job.NewSpecBuilder(jobVersion, "job-B", "sample-owner", jobSchedule, jobWindow, jobTaskB).WithAsset(jobAsset).Build()
			specBUpdated, _ := job.NewSpecBuilder(jobVersion, "job-B", "sample-owner-updated", jobSchedule, jobWindow, jobTaskB).WithAsset(jobAsset).WithSpecUpstream(upstreamsSpecB).Build()
			specC, _ := job.NewSpecBuilder(jobVersion, "job-C", "sample-owner", jobSchedule, jobWindow, jobTaskC).WithSpecUpstream(upstreamsSpecC).Build()

			jobA := job.NewJob(sampleTenant, specA, "bigquery://project:dataset.tableA", []job.ResourceURN{"bigquery://project:dataset.tableZ"})
			jobB := job.NewJob(sampleTenant, specB, "bigquery://project:dataset.tableB", []job.ResourceURN{"bigquery://project:dataset.tableA"})

			jobRepo.On("GetAllByTenant", ctx, sampleTenant).Return([]*job.Job{jobA, jobB}, nil)

			pluginService.On("ConstructDestinationURN", ctx, specA.Task().Name().String(), mock.Anything).Return(job.ResourceURN("bigquery://project:dataset.tableA").String(), nil)
			pluginService.On("ConstructDestinationURN", ctx, specB.Task().Name().String(), mock.Anything).Return(job.ResourceURN("bigquery://project:dataset.tableB").String(), nil)
			pluginService.On("ConstructDestinationURN", ctx, specC.Task().Name().String(), mock.Anything).Return("", nil)
			pluginService.On("IdentifyUpstreams", ctx, specA.Task().Name().String(), mock.Anything, mock.Anything).Return(jobResourceURNsToString([]job.ResourceURN{"bigquery://project:dataset.tableZ"}), nil)
			pluginService.On("IdentifyUpstreams", ctx, specBUpdated.Task().Name().String(), mock.Anything, mock.Anything).Return(jobResourceURNsToString([]job.ResourceURN{"bigquery://project:dataset.tableA"}), nil)
			pluginService.On("IdentifyUpstreams", ctx, specC.Task().Name().String(), mock.Anything, mock.Anything).Return([]string{}, nil)

			logWriter.On("Write", mock.Anything, mock.Anything).Return(nil)

			jobService := service.NewJobService(jobRepo, upstreamRepo, downstreamRepo, pluginService, upstreamResolver, tenantDetailsGetter, nil, log, nil, compiler.NewEngine())
			err := jobService.Validate(ctx, sampleTenant, []*job.Spec{specAUpdated, specBUpdated, specC}, jobNamesWithInvalidSpec, logWriter)
			assert.Error(t, err)
			assert.ErrorContains(t, err, "a cycle dependency encountered in the tree:")
		})
		t.Run("returns error when there's a cyclic for static upstreams", func(t *testing.T) {
			tenantDetailsGetter := new(TenantDetailsGetter)
			tenantDetailsGetter.On("GetDetails", ctx, mock.Anything).Return(detailedTenant, nil)
			defer tenantDetailsGetter.AssertExpectations(t)

			pluginService := new(PluginService)
			defer pluginService.AssertExpectations(t)

			upstreamResolver := new(UpstreamResolver)
			defer upstreamResolver.AssertExpectations(t)

			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			upstreamRepo := new(UpstreamRepository)
			defer upstreamRepo.AssertExpectations(t)

			downstreamRepo := new(DownstreamRepository)
			defer downstreamRepo.AssertExpectations(t)

			logWriter := new(mockWriter)
			defer logWriter.AssertExpectations(t)

			jobTaskPython := job.NewTask("python", jobTaskConfig)
			upstreamsSpecA, _ := job.NewSpecUpstreamBuilder().WithUpstreamNames([]job.SpecUpstreamName{"test-proj/job-C"}).Build()
			upstreamsSpecB, _ := job.NewSpecUpstreamBuilder().WithUpstreamNames([]job.SpecUpstreamName{"test-proj/job-A"}).Build()
			upstreamsSpecC, _ := job.NewSpecUpstreamBuilder().WithUpstreamNames([]job.SpecUpstreamName{"test-proj/job-B"}).Build()
			specA, _ := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, jobWindow, jobTaskPython).WithSpecUpstream(upstreamsSpecA).Build()
			specB, _ := job.NewSpecBuilder(jobVersion, "job-B", "sample-owner", jobSchedule, jobWindow, jobTaskPython).WithSpecUpstream(upstreamsSpecB).Build()
			specC, _ := job.NewSpecBuilder(jobVersion, "job-C", "sample-owner", jobSchedule, jobWindow, jobTaskPython).WithSpecUpstream(upstreamsSpecC).Build()
			specs := []*job.Spec{specA, specB, specC}

			jobRepo.On("GetAllByTenant", ctx, sampleTenant).Return([]*job.Job{}, nil)

			logWriter.On("Write", mock.Anything, mock.Anything).Return(nil)

			pluginService.On("ConstructDestinationURN", ctx, specA.Task().Name().String(), mock.Anything).Return("", nil)
			pluginService.On("IdentifyUpstreams", ctx, specA.Task().Name().String(), mock.Anything, mock.Anything).Return([]string{}, nil)

			jobService := service.NewJobService(jobRepo, upstreamRepo, downstreamRepo, pluginService, upstreamResolver, tenantDetailsGetter, nil, log, nil, compiler.NewEngine())
			err := jobService.Validate(ctx, sampleTenant, specs, jobNamesWithInvalidSpec, logWriter)
			assert.Error(t, err)
			assert.ErrorContains(t, err, "a cycle dependency encountered in the tree:")
		})
		t.Run("returns no error when success", func(t *testing.T) {
			tenantDetailsGetter := new(TenantDetailsGetter)
			tenantDetailsGetter.On("GetDetails", ctx, mock.Anything).Return(detailedTenant, nil)
			defer tenantDetailsGetter.AssertExpectations(t)

			pluginService := new(PluginService)
			defer pluginService.AssertExpectations(t)

			upstreamResolver := new(UpstreamResolver)
			defer upstreamResolver.AssertExpectations(t)

			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			upstreamRepo := new(UpstreamRepository)
			defer upstreamRepo.AssertExpectations(t)

			downstreamRepo := new(DownstreamRepository)
			defer downstreamRepo.AssertExpectations(t)

			logWriter := new(mockWriter)
			defer logWriter.AssertExpectations(t)

			jobTaskConfigA, _ := job.ConfigFrom(map[string]string{"table": "table-A", "BQ_SERVICE_ACCOUNT": "service_account"})
			jobTaskConfigB, _ := job.ConfigFrom(map[string]string{"table": "table-B", "BQ_SERVICE_ACCOUNT": "service_account"})
			jobTaskConfigC, _ := job.ConfigFrom(map[string]string{"table": "table-C", "BQ_SERVICE_ACCOUNT": "service_account"})
			jobTaskA := job.NewTask(taskName, jobTaskConfigA)
			jobTaskB := job.NewTask(taskName, jobTaskConfigB)
			jobTaskC := job.NewTask(taskName, jobTaskConfigC)

			specA, _ := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, jobWindow, jobTaskA).WithAsset(jobAsset).Build()
			specAUpdated, _ := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner-updated", jobSchedule, jobWindow, jobTaskA).WithAsset(jobAsset).Build()
			specB, _ := job.NewSpecBuilder(jobVersion, "job-B", "sample-owner", jobSchedule, jobWindow, jobTaskB).WithAsset(jobAsset).Build()
			specC, _ := job.NewSpecBuilder(jobVersion, "job-C", "sample-owner", jobSchedule, jobWindow, jobTaskC).WithAsset(jobAsset).Build()
			specs := []*job.Spec{specAUpdated, specB}

			jobA := job.NewJob(sampleTenant, specA, "bigquery://project:dataset.tableA", []job.ResourceURN{"bigquery://project:dataset.tableZ"})
			jobB := job.NewJob(sampleTenant, specB, "bigquery://project:dataset.tableB", []job.ResourceURN{"bigquery://project:dataset.tableA"})
			jobC := job.NewJob(sampleTenant, specC, "bigquery://project:dataset.tableC", []job.ResourceURN{"bigquery://project:dataset.tableB"})

			jobRepo.On("GetAllByTenant", ctx, sampleTenant).Return([]*job.Job{jobA, jobB, jobC}, nil)

			pluginService.On("ConstructDestinationURN", ctx, specA.Task().Name().String(), mock.Anything).Return(job.ResourceURN("bigquery://project:dataset.tableA").String(), nil)
			pluginService.On("IdentifyUpstreams", ctx, specA.Task().Name().String(), mock.Anything, mock.Anything).Return(jobResourceURNsToString([]job.ResourceURN{"bigquery://project:dataset.tableZ"}), nil)

			downstreamRepo.On("GetDownstreamByJobName", ctx, project.Name(), specC.Name()).Return([]*job.Downstream{}, nil)

			logWriter.On("Write", mock.Anything, mock.Anything).Return(nil)
			jobService := service.NewJobService(jobRepo, upstreamRepo, downstreamRepo, pluginService, upstreamResolver, tenantDetailsGetter, nil, log, nil, compiler.NewEngine())
			err := jobService.Validate(ctx, sampleTenant, specs, jobNamesWithInvalidSpec, logWriter)
			assert.NoError(t, err)
		})
	})

	t.Run("GetUpstreamsToInspect", func(t *testing.T) {
		t.Run("should return upstream for an existing job", func(t *testing.T) {
			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			upstreamRepo := new(UpstreamRepository)
			defer upstreamRepo.AssertExpectations(t)

			downstreamRepo := new(DownstreamRepository)
			defer downstreamRepo.AssertExpectations(t)

			pluginService := new(PluginService)
			defer pluginService.AssertExpectations(t)

			upstreamResolver := new(UpstreamResolver)
			defer upstreamResolver.AssertExpectations(t)

			tenantDetailsGetter := new(TenantDetailsGetter)
			defer tenantDetailsGetter.AssertExpectations(t)

			specA, _ := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()
			jobA := job.NewJob(sampleTenant, specA, "bigquery://project:dataset.tableA", []job.ResourceURN{"bigquery://project:dataset.tableB"})

			upstreamB := job.NewUpstreamResolved("job-B", "", "bigquery://project:dataset.tableB", sampleTenant, "inferred", taskName, false)

			upstreamRepo.On("GetUpstreams", ctx, project.Name(), jobA.Spec().Name()).Return([]*job.Upstream{upstreamB}, nil)

			jobService := service.NewJobService(jobRepo, upstreamRepo, downstreamRepo, pluginService, upstreamResolver, tenantDetailsGetter, nil, log, nil, nil)
			result, err := jobService.GetUpstreamsToInspect(ctx, jobA, false)
			assert.NoError(t, err)
			assert.EqualValues(t, []*job.Upstream{upstreamB}, result)
		})
		t.Run("should return upstream for client's local job", func(t *testing.T) {
			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			upstreamRepo := new(UpstreamRepository)
			defer upstreamRepo.AssertExpectations(t)

			downstreamRepo := new(DownstreamRepository)
			defer downstreamRepo.AssertExpectations(t)

			pluginService := new(PluginService)
			defer pluginService.AssertExpectations(t)

			upstreamResolver := new(UpstreamResolver)
			defer upstreamResolver.AssertExpectations(t)

			tenantDetailsGetter := new(TenantDetailsGetter)
			defer tenantDetailsGetter.AssertExpectations(t)

			logWriter := new(mockWriter)
			defer logWriter.AssertExpectations(t)

			specA, _ := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()
			jobA := job.NewJob(sampleTenant, specA, "bigquery://project:dataset.tableA", []job.ResourceURN{"bigquery://project:dataset.tableB"})

			upstreamB := job.NewUpstreamResolved("job-B", "", "bigquery://project:dataset.tableB", sampleTenant, "inferred", taskName, false)

			upstreamResolver.On("Resolve", ctx, jobA, mock.Anything).Return([]*job.Upstream{upstreamB}, nil)

			jobService := service.NewJobService(jobRepo, upstreamRepo, downstreamRepo, pluginService, upstreamResolver, tenantDetailsGetter, nil, log, nil, nil)
			result, err := jobService.GetUpstreamsToInspect(ctx, jobA, true)
			assert.NoError(t, err)
			assert.EqualValues(t, []*job.Upstream{upstreamB}, result)
		})
	})

	t.Run("GetJobBasicInfo", func(t *testing.T) {
		t.Run("should return job basic info and its logger for user given job spec", func(t *testing.T) {
			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			upstreamRepo := new(UpstreamRepository)
			defer upstreamRepo.AssertExpectations(t)

			downstreamRepo := new(DownstreamRepository)
			defer downstreamRepo.AssertExpectations(t)

			pluginService := new(PluginService)
			defer pluginService.AssertExpectations(t)

			upstreamResolver := new(UpstreamResolver)
			defer upstreamResolver.AssertExpectations(t)

			tenantDetailsGetter := new(TenantDetailsGetter)
			defer tenantDetailsGetter.AssertExpectations(t)

			specA, _ := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()

			tenantDetailsGetter.On("GetDetails", ctx, sampleTenant).Return(detailedTenant, nil)

			jobADestination := job.ResourceURN("bigquery://project:dataset.tableA")
			pluginService.On("ConstructDestinationURN", ctx, specA.Task().Name().String(), mock.Anything).Return(jobADestination.String(), nil)

			jobASources := []job.ResourceURN{"bigquery://project:dataset.tableB"}
			pluginService.On("IdentifyUpstreams", ctx, specA.Task().Name().String(), mock.Anything, mock.Anything).Return(jobResourceURNsToString(jobASources), nil)

			jobRepo.On("GetAllByResourceDestination", ctx, jobADestination).Return([]*job.Job{}, nil)

			jobA := job.NewJob(sampleTenant, specA, jobADestination, jobASources)

			jobService := service.NewJobService(jobRepo, upstreamRepo, downstreamRepo, pluginService, upstreamResolver, tenantDetailsGetter, nil, log, nil, compiler.NewEngine())
			result, logger := jobService.GetJobBasicInfo(ctx, sampleTenant, "", specA)
			assert.Nil(t, logger.Messages)
			assert.Equal(t, jobA, result)
		})
		t.Run("should return job basic info and its logger for existing job spec", func(t *testing.T) {
			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			upstreamRepo := new(UpstreamRepository)
			defer upstreamRepo.AssertExpectations(t)

			downstreamRepo := new(DownstreamRepository)
			defer downstreamRepo.AssertExpectations(t)

			pluginService := new(PluginService)
			defer pluginService.AssertExpectations(t)

			upstreamResolver := new(UpstreamResolver)
			defer upstreamResolver.AssertExpectations(t)

			tenantDetailsGetter := new(TenantDetailsGetter)
			defer tenantDetailsGetter.AssertExpectations(t)

			specA, _ := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()
			jobADestination := job.ResourceURN("bigquery://project:dataset.tableA")
			jobASources := []job.ResourceURN{"bigquery://project:dataset.tableB"}
			jobA := job.NewJob(sampleTenant, specA, jobADestination, jobASources)

			jobRepo.On("GetByJobName", ctx, project.Name(), specA.Name()).Return(jobA, nil)
			jobRepo.On("GetAllByResourceDestination", ctx, jobADestination).Return([]*job.Job{}, nil)

			jobService := service.NewJobService(jobRepo, upstreamRepo, downstreamRepo, pluginService, upstreamResolver, tenantDetailsGetter, nil, log, nil, compiler.NewEngine())
			result, logger := jobService.GetJobBasicInfo(ctx, sampleTenant, specA.Name(), nil)
			assert.Nil(t, logger.Messages)
			assert.Equal(t, jobA, result)
		})
		t.Run("should return error if unable to get tenant details", func(t *testing.T) {
			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			upstreamRepo := new(UpstreamRepository)
			defer upstreamRepo.AssertExpectations(t)

			downstreamRepo := new(DownstreamRepository)
			defer downstreamRepo.AssertExpectations(t)

			pluginService := new(PluginService)
			defer pluginService.AssertExpectations(t)

			upstreamResolver := new(UpstreamResolver)
			defer upstreamResolver.AssertExpectations(t)

			tenantDetailsGetter := new(TenantDetailsGetter)
			defer tenantDetailsGetter.AssertExpectations(t)

			specA, _ := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()

			tenantDetailsGetter.On("GetDetails", ctx, sampleTenant).Return(detailedTenant, errors.New("sample error"))

			jobService := service.NewJobService(jobRepo, upstreamRepo, downstreamRepo, pluginService, upstreamResolver, tenantDetailsGetter, nil, log, nil, compiler.NewEngine())
			result, logger := jobService.GetJobBasicInfo(ctx, sampleTenant, "", specA)
			assert.Contains(t, logger.Messages[0].Message, "sample error")
			assert.Nil(t, result)
		})
		t.Run("should return error if unable to generate job based on spec", func(t *testing.T) {
			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			upstreamRepo := new(UpstreamRepository)
			defer upstreamRepo.AssertExpectations(t)

			downstreamRepo := new(DownstreamRepository)
			defer downstreamRepo.AssertExpectations(t)

			pluginService := new(PluginService)
			defer pluginService.AssertExpectations(t)

			upstreamResolver := new(UpstreamResolver)
			defer upstreamResolver.AssertExpectations(t)

			tenantDetailsGetter := new(TenantDetailsGetter)
			defer tenantDetailsGetter.AssertExpectations(t)

			specA, _ := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()

			tenantDetailsGetter.On("GetDetails", ctx, sampleTenant).Return(detailedTenant, nil)

			jobADestination := job.ResourceURN("bigquery://project:dataset.tableA")
			pluginService.On("ConstructDestinationURN", ctx, specA.Task().Name().String(), mock.Anything).Return(jobADestination.String(), nil).Once()

			jobAUpstreamName := []job.ResourceURN{"bigquery://project:dataset.tableB"}
			pluginService.On("IdentifyUpstreams", ctx, specA.Task().Name().String(), mock.Anything, mock.Anything).Return(jobResourceURNsToString(jobAUpstreamName), errors.New("sample error"))

			jobService := service.NewJobService(jobRepo, upstreamRepo, downstreamRepo, pluginService, upstreamResolver, tenantDetailsGetter, nil, log, nil, compiler.NewEngine())
			result, logger := jobService.GetJobBasicInfo(ctx, sampleTenant, "", specA)
			assert.Contains(t, logger.Messages[0].Message, "sample error")
			assert.Nil(t, result)
		})
		t.Run("should return job information with warning and errors", func(t *testing.T) {
			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			upstreamRepo := new(UpstreamRepository)
			defer upstreamRepo.AssertExpectations(t)

			downstreamRepo := new(DownstreamRepository)
			defer downstreamRepo.AssertExpectations(t)

			pluginService := new(PluginService)
			defer pluginService.AssertExpectations(t)

			upstreamResolver := new(UpstreamResolver)
			defer upstreamResolver.AssertExpectations(t)

			tenantDetailsGetter := new(TenantDetailsGetter)
			defer tenantDetailsGetter.AssertExpectations(t)

			specASchedule, err := job.NewScheduleBuilder(startDate).Build()
			assert.NoError(t, err)

			specA, _ := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", specASchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()
			jobADestination := job.ResourceURN("bigquery://project:dataset.tableA")
			jobA := job.NewJob(sampleTenant, specA, jobADestination, nil)

			jobRepo.On("GetByJobName", ctx, project.Name(), specA.Name()).Return(jobA, nil)
			jobRepo.On("GetAllByResourceDestination", ctx, jobADestination).Return([]*job.Job{}, errors.New("sample-error"))

			jobService := service.NewJobService(jobRepo, upstreamRepo, downstreamRepo, pluginService, upstreamResolver, tenantDetailsGetter, nil, log, nil, compiler.NewEngine())
			result, logger := jobService.GetJobBasicInfo(ctx, sampleTenant, specA.Name(), nil)
			assert.Contains(t, logger.Messages[0].Message, "no job sources detected")
			assert.Contains(t, logger.Messages[1].Message, "could not perform duplicate job destination check")
			assert.Equal(t, jobA, result)
		})
		t.Run("should return error if unable to get existing job", func(t *testing.T) {
			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			upstreamRepo := new(UpstreamRepository)
			defer upstreamRepo.AssertExpectations(t)

			downstreamRepo := new(DownstreamRepository)
			defer downstreamRepo.AssertExpectations(t)

			pluginService := new(PluginService)
			defer pluginService.AssertExpectations(t)

			upstreamResolver := new(UpstreamResolver)
			defer upstreamResolver.AssertExpectations(t)

			tenantDetailsGetter := new(TenantDetailsGetter)
			defer tenantDetailsGetter.AssertExpectations(t)

			specA, _ := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()

			jobRepo.On("GetByJobName", ctx, project.Name(), specA.Name()).Return(nil, errors.New("internal error"))

			jobService := service.NewJobService(jobRepo, upstreamRepo, downstreamRepo, pluginService, upstreamResolver, tenantDetailsGetter, nil, log, nil, compiler.NewEngine())
			result, logger := jobService.GetJobBasicInfo(ctx, sampleTenant, specA.Name(), nil)
			assert.Contains(t, logger.Messages[0].Message, "internal error")
			assert.Nil(t, result)
		})
		t.Run("should return error if requested job is not exist yet and spec is not provided", func(t *testing.T) {
			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			upstreamRepo := new(UpstreamRepository)
			defer upstreamRepo.AssertExpectations(t)

			downstreamRepo := new(DownstreamRepository)
			defer downstreamRepo.AssertExpectations(t)

			pluginService := new(PluginService)
			defer pluginService.AssertExpectations(t)

			upstreamResolver := new(UpstreamResolver)
			defer upstreamResolver.AssertExpectations(t)

			tenantDetailsGetter := new(TenantDetailsGetter)
			defer tenantDetailsGetter.AssertExpectations(t)

			specA, _ := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()

			jobRepo.On("GetByJobName", ctx, project.Name(), specA.Name()).Return(nil, errors.New("job not found"))

			jobService := service.NewJobService(jobRepo, upstreamRepo, downstreamRepo, pluginService, upstreamResolver, tenantDetailsGetter, nil, log, nil, compiler.NewEngine())
			result, logger := jobService.GetJobBasicInfo(ctx, sampleTenant, specA.Name(), nil)
			assert.Contains(t, logger.Messages[0].Message, "job not found")
			assert.Nil(t, result)
		})
		t.Run("should write log if found existing job with same resource destination", func(t *testing.T) {
			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			upstreamRepo := new(UpstreamRepository)
			defer upstreamRepo.AssertExpectations(t)

			downstreamRepo := new(DownstreamRepository)
			defer downstreamRepo.AssertExpectations(t)

			pluginService := new(PluginService)
			defer pluginService.AssertExpectations(t)

			upstreamResolver := new(UpstreamResolver)
			defer upstreamResolver.AssertExpectations(t)

			tenantDetailsGetter := new(TenantDetailsGetter)
			defer tenantDetailsGetter.AssertExpectations(t)

			specA, _ := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()
			jobADestination := job.ResourceURN("bigquery://project:dataset.tableA")
			jobASources := []job.ResourceURN{"bigquery://project:dataset.tableB"}
			jobA := job.NewJob(sampleTenant, specA, jobADestination, jobASources)

			specB, _ := job.NewSpecBuilder(jobVersion, "job-B", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()
			jobBDestination := job.ResourceURN("bigquery://project:dataset.tableB")
			jobBSources := []job.ResourceURN{"bigquery://project:dataset.tableC"}
			jobB := job.NewJob(sampleTenant, specB, jobBDestination, jobBSources)

			jobRepo.On("GetByJobName", ctx, project.Name(), specA.Name()).Return(jobA, nil)
			jobRepo.On("GetAllByResourceDestination", ctx, jobADestination).Return([]*job.Job{jobB, jobA}, nil)

			jobService := service.NewJobService(jobRepo, upstreamRepo, downstreamRepo, pluginService, upstreamResolver, tenantDetailsGetter, nil, log, nil, compiler.NewEngine())
			result, logger := jobService.GetJobBasicInfo(ctx, sampleTenant, specA.Name(), nil)
			assert.Contains(t, logger.Messages[0].Message, "job already exists with same Destination")
			assert.Equal(t, jobA, result)
		})
		t.Run("should not warn if the job with same resource destination is the same job as requested", func(t *testing.T) {
			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			upstreamRepo := new(UpstreamRepository)
			defer upstreamRepo.AssertExpectations(t)

			downstreamRepo := new(DownstreamRepository)
			defer downstreamRepo.AssertExpectations(t)

			pluginService := new(PluginService)
			defer pluginService.AssertExpectations(t)

			upstreamResolver := new(UpstreamResolver)
			defer upstreamResolver.AssertExpectations(t)

			tenantDetailsGetter := new(TenantDetailsGetter)
			defer tenantDetailsGetter.AssertExpectations(t)

			specA, _ := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()
			jobADestination := job.ResourceURN("bigquery://project:dataset.tableA")
			jobASources := []job.ResourceURN{"bigquery://project:dataset.tableB"}
			jobA := job.NewJob(sampleTenant, specA, jobADestination, jobASources)

			jobRepo.On("GetByJobName", ctx, project.Name(), specA.Name()).Return(jobA, nil)
			jobRepo.On("GetAllByResourceDestination", ctx, jobADestination).Return([]*job.Job{jobA}, nil)

			jobService := service.NewJobService(jobRepo, upstreamRepo, downstreamRepo, pluginService, upstreamResolver, tenantDetailsGetter, nil, log, nil, compiler.NewEngine())
			result, logger := jobService.GetJobBasicInfo(ctx, sampleTenant, specA.Name(), nil)
			assert.Nil(t, logger.Messages)
			assert.Equal(t, jobA, result)
		})
	})

	t.Run("GetDownstream", func(t *testing.T) {
		t.Run("should return downstream for client's local job", func(t *testing.T) {
			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			upstreamRepo := new(UpstreamRepository)
			defer upstreamRepo.AssertExpectations(t)

			downstreamRepo := new(DownstreamRepository)
			defer downstreamRepo.AssertExpectations(t)

			pluginService := new(PluginService)
			defer pluginService.AssertExpectations(t)

			upstreamResolver := new(UpstreamResolver)
			defer upstreamResolver.AssertExpectations(t)

			tenantDetailsGetter := new(TenantDetailsGetter)
			defer tenantDetailsGetter.AssertExpectations(t)

			specA, _ := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()
			jobA := job.NewJob(sampleTenant, specA, "bigquery://project:dataset.tableA", nil)

			jobADownstream := []*job.Downstream{
				job.NewDownstream("job-B", project.Name(), namespace.Name(), taskName),
			}
			downstreamRepo.On("GetDownstreamByDestination", ctx, project.Name(), jobA.Destination()).Return(jobADownstream, nil)

			jobService := service.NewJobService(jobRepo, upstreamRepo, downstreamRepo, pluginService, upstreamResolver, tenantDetailsGetter, nil, log, nil, nil)
			result, err := jobService.GetDownstream(ctx, jobA, true)
			assert.NoError(t, err)
			assert.Equal(t, jobADownstream, result)
		})
		t.Run("should return downstream of an existing job", func(t *testing.T) {
			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			upstreamRepo := new(UpstreamRepository)
			defer upstreamRepo.AssertExpectations(t)

			downstreamRepo := new(DownstreamRepository)
			defer downstreamRepo.AssertExpectations(t)

			pluginService := new(PluginService)
			defer pluginService.AssertExpectations(t)

			upstreamResolver := new(UpstreamResolver)
			defer upstreamResolver.AssertExpectations(t)

			tenantDetailsGetter := new(TenantDetailsGetter)
			defer tenantDetailsGetter.AssertExpectations(t)

			specA, _ := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()
			jobA := job.NewJob(sampleTenant, specA, "bigquery://project:dataset.tableA", nil)

			jobADownstream := []*job.Downstream{
				job.NewDownstream("job-B", project.Name(), namespace.Name(), taskName),
			}
			downstreamRepo.On("GetDownstreamByJobName", ctx, project.Name(), specA.Name()).Return(jobADownstream, nil)

			jobService := service.NewJobService(jobRepo, upstreamRepo, downstreamRepo, pluginService, upstreamResolver, tenantDetailsGetter, nil, log, nil, nil)
			result, err := jobService.GetDownstream(ctx, jobA, false)
			assert.NoError(t, err)
			assert.Equal(t, jobADownstream, result)
		})
	})
	t.Run("UpdateState", func(t *testing.T) {
		jobName, _ := job.NameFrom("job-A")
		jobsToUpdateState := []job.Name{jobName}
		state := job.DISABLED
		updateRemark := "job disable remark"
		t.Run("should fail if scheduler state change request fails", func(t *testing.T) {
			jobDeploymentService := new(JobDeploymentService)
			jobDeploymentService.On("UpdateJobScheduleState", ctx, sampleTenant, jobsToUpdateState, state.String()).Return(fmt.Errorf("some error in update Job State"))
			defer jobDeploymentService.AssertExpectations(t)

			jobService := service.NewJobService(nil, nil, nil, nil, nil, nil, nil, nil, jobDeploymentService, nil)
			err := jobService.UpdateState(ctx, sampleTenant, jobsToUpdateState, state, "job disable remark")
			assert.ErrorContains(t, err, "some error in update Job State")
		})

		t.Run("should fail if update state in job table fails", func(t *testing.T) {
			jobDeploymentService := new(JobDeploymentService)
			jobDeploymentService.On("UpdateJobScheduleState", ctx, sampleTenant, jobsToUpdateState, state.String()).Return(nil)
			defer jobDeploymentService.AssertExpectations(t)

			jobRepo := new(JobRepository)
			jobRepo.On("UpdateState", ctx, sampleTenant, jobsToUpdateState, state, updateRemark).Return(fmt.Errorf("some error in update Job State repo"))
			defer jobRepo.AssertExpectations(t)

			jobService := service.NewJobService(jobRepo, nil, nil, nil, nil, nil, nil, nil, jobDeploymentService, nil)
			err := jobService.UpdateState(ctx, sampleTenant, jobsToUpdateState, state, updateRemark)
			assert.ErrorContains(t, err, "some error in update Job State repo")
		})

		t.Run("should pass when no error in scheduler update and repo update", func(t *testing.T) {
			jobDeploymentService := new(JobDeploymentService)
			jobDeploymentService.On("UpdateJobScheduleState", ctx, sampleTenant, jobsToUpdateState, state.String()).Return(nil)
			defer jobDeploymentService.AssertExpectations(t)

			jobRepo := new(JobRepository)
			jobRepo.On("UpdateState", ctx, sampleTenant, jobsToUpdateState, state, updateRemark).Return(nil)
			defer jobRepo.AssertExpectations(t)

			eventHandler := newEventHandler(t)
			eventHandler.On("HandleEvent", mock.Anything).Times(1)

			jobService := service.NewJobService(jobRepo, nil, nil, nil, nil, nil, eventHandler, nil, jobDeploymentService, nil)
			err := jobService.UpdateState(ctx, sampleTenant, jobsToUpdateState, state, updateRemark)
			assert.Nil(t, err)
		})
	})
}

// JobRepository is an autogenerated mock type for the JobRepository type
type JobRepository struct {
	mock.Mock
}

// Add provides a mock function with given fields: _a0, _a1
func (_m *JobRepository) Add(_a0 context.Context, _a1 []*job.Job) ([]*job.Job, error) {
	ret := _m.Called(_a0, _a1)

	var r0 []*job.Job
	if rf, ok := ret.Get(0).(func(context.Context, []*job.Job) []*job.Job); ok {
		r0 = rf(_a0, _a1)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]*job.Job)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, []*job.Job) error); ok {
		r1 = rf(_a0, _a1)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// Delete provides a mock function with given fields: ctx, projectName, jobName, cleanHistory
func (_m *JobRepository) Delete(ctx context.Context, projectName tenant.ProjectName, jobName job.Name, cleanHistory bool) error {
	ret := _m.Called(ctx, projectName, jobName, cleanHistory)

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, tenant.ProjectName, job.Name, bool) error); ok {
		r0 = rf(ctx, projectName, jobName, cleanHistory)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// ChangeJobNamespace provides a mock function with given fields: ctx, jobName, jobTenant, jobNewTenant
func (_m *JobRepository) ChangeJobNamespace(ctx context.Context, jobName job.Name, jobTenant, jobNewTenant tenant.Tenant) error {
	ret := _m.Called(ctx, jobName, jobTenant, jobNewTenant)
	return ret.Error(0)
}

// UpdateState provides a mock function with given fields: ctx, jobName, jobTenant, jobNewTenant
func (_m *JobRepository) UpdateState(ctx context.Context, jobTenant tenant.Tenant, jobNames []job.Name, jobState job.State, remark string) error {
	ret := _m.Called(ctx, jobTenant, jobNames, jobState, remark)
	return ret.Error(0)
}

// SyncState provides a mock function with given fields: ctx, jobTenant, disabledJobs, enabledJobs
func (_m *JobRepository) SyncState(ctx context.Context, jobTenant tenant.Tenant, disabledJobNames, enabledJobNames []job.Name) error {
	ret := _m.Called(ctx, jobTenant, disabledJobNames, enabledJobNames)
	return ret.Error(0)
}

// GetAllByProjectName provides a mock function with given fields: ctx, projectName
func (_m *JobRepository) GetAllByProjectName(ctx context.Context, projectName tenant.ProjectName) ([]*job.Job, error) {
	ret := _m.Called(ctx, projectName)

	var r0 []*job.Job
	if rf, ok := ret.Get(0).(func(context.Context, tenant.ProjectName) []*job.Job); ok {
		r0 = rf(ctx, projectName)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]*job.Job)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, tenant.ProjectName) error); ok {
		r1 = rf(ctx, projectName)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// GetAllByResourceDestination provides a mock function with given fields: ctx, resourceDestination
func (_m *JobRepository) GetAllByResourceDestination(ctx context.Context, resourceDestination job.ResourceURN) ([]*job.Job, error) {
	ret := _m.Called(ctx, resourceDestination)

	var r0 []*job.Job
	if rf, ok := ret.Get(0).(func(context.Context, job.ResourceURN) []*job.Job); ok {
		r0 = rf(ctx, resourceDestination)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]*job.Job)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, job.ResourceURN) error); ok {
		r1 = rf(ctx, resourceDestination)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// GetAllByTenant provides a mock function with given fields: ctx, jobTenant
func (_m *JobRepository) GetAllByTenant(ctx context.Context, jobTenant tenant.Tenant) ([]*job.Job, error) {
	ret := _m.Called(ctx, jobTenant)

	var r0 []*job.Job
	if rf, ok := ret.Get(0).(func(context.Context, tenant.Tenant) []*job.Job); ok {
		r0 = rf(ctx, jobTenant)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]*job.Job)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, tenant.Tenant) error); ok {
		r1 = rf(ctx, jobTenant)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// GetByJobName provides a mock function with given fields: ctx, projectName, jobName
func (_m *JobRepository) GetByJobName(ctx context.Context, projectName tenant.ProjectName, jobName job.Name) (*job.Job, error) {
	ret := _m.Called(ctx, projectName, jobName)

	var r0 *job.Job
	if rf, ok := ret.Get(0).(func(context.Context, tenant.ProjectName, job.Name) *job.Job); ok {
		r0 = rf(ctx, projectName, jobName)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*job.Job)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, tenant.ProjectName, job.Name) error); ok {
		r1 = rf(ctx, projectName, jobName)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// Update provides a mock function with given fields: _a0, _a1
func (_m *JobRepository) Update(_a0 context.Context, _a1 []*job.Job) ([]*job.Job, error) {
	ret := _m.Called(_a0, _a1)

	var r0 []*job.Job
	if rf, ok := ret.Get(0).(func(context.Context, []*job.Job) []*job.Job); ok {
		r0 = rf(_a0, _a1)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]*job.Job)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, []*job.Job) error); ok {
		r1 = rf(_a0, _a1)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

type DownstreamRepository struct {
	mock.Mock
}

// GetDownstreamByDestination provides a mock function with given fields: ctx, projectName, destination
func (_d *DownstreamRepository) GetDownstreamByDestination(ctx context.Context, projectName tenant.ProjectName, destination job.ResourceURN) ([]*job.Downstream, error) {
	ret := _d.Called(ctx, projectName, destination)

	var r0 []*job.Downstream
	if rf, ok := ret.Get(0).(func(context.Context, tenant.ProjectName, job.ResourceURN) []*job.Downstream); ok {
		r0 = rf(ctx, projectName, destination)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]*job.Downstream)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, tenant.ProjectName, job.ResourceURN) error); ok {
		r1 = rf(ctx, projectName, destination)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// GetDownstreamByJobName provides a mock function with given fields: ctx, projectName, jobName
func (_d *DownstreamRepository) GetDownstreamByJobName(ctx context.Context, projectName tenant.ProjectName, jobName job.Name) ([]*job.Downstream, error) {
	ret := _d.Called(ctx, projectName, jobName)

	var r0 []*job.Downstream
	if rf, ok := ret.Get(0).(func(context.Context, tenant.ProjectName, job.Name) []*job.Downstream); ok {
		r0 = rf(ctx, projectName, jobName)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]*job.Downstream)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, tenant.ProjectName, job.Name) error); ok {
		r1 = rf(ctx, projectName, jobName)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// GetDownstreamBySources provides a mock function with given fields: ctx, sources
func (_d *DownstreamRepository) GetDownstreamBySources(ctx context.Context, sources []job.ResourceURN) ([]*job.Downstream, error) {
	ret := _d.Called(ctx, sources)

	var r0 []*job.Downstream
	if rf, ok := ret.Get(0).(func(context.Context, []job.ResourceURN) []*job.Downstream); ok {
		r0 = rf(ctx, sources)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]*job.Downstream)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, []job.ResourceURN) error); ok {
		r1 = rf(ctx, sources)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

type UpstreamRepository struct {
	mock.Mock
}

// GetJobNameWithInternalUpstreams provides a mock function with given fields: _a0, _a1, _a2
func (_u *UpstreamRepository) ResolveUpstreams(_a0 context.Context, _a1 tenant.ProjectName, _a2 []job.Name) (map[job.Name][]*job.Upstream, error) {
	ret := _u.Called(_a0, _a1, _a2)

	var r0 map[job.Name][]*job.Upstream
	if rf, ok := ret.Get(0).(func(context.Context, tenant.ProjectName, []job.Name) map[job.Name][]*job.Upstream); ok {
		r0 = rf(_a0, _a1, _a2)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(map[job.Name][]*job.Upstream)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, tenant.ProjectName, []job.Name) error); ok {
		r1 = rf(_a0, _a1, _a2)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// GetUpstreams provides a mock function with given fields: ctx, projectName, jobName
func (_u *UpstreamRepository) GetUpstreams(ctx context.Context, projectName tenant.ProjectName, jobName job.Name) ([]*job.Upstream, error) {
	ret := _u.Called(ctx, projectName, jobName)

	var r0 []*job.Upstream
	if rf, ok := ret.Get(0).(func(context.Context, tenant.ProjectName, job.Name) []*job.Upstream); ok {
		r0 = rf(ctx, projectName, jobName)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]*job.Upstream)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, tenant.ProjectName, job.Name) error); ok {
		r1 = rf(ctx, projectName, jobName)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// ReplaceUpstreams provides a mock function with given fields: _a0, _a1
func (_u *UpstreamRepository) ReplaceUpstreams(_a0 context.Context, _a1 []*job.WithUpstream) error {
	ret := _u.Called(_a0, _a1)

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, []*job.WithUpstream) error); ok {
		r0 = rf(_a0, _a1)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// PluginService is an autogenerated mock type for the PluginService type
type PluginService struct {
	mock.Mock
}

// ConstructDestinationURN provides a mock function with given fields: ctx, taskName, config
func (_m *PluginService) ConstructDestinationURN(ctx context.Context, taskName string, config map[string]string) (string, error) {
	ret := _m.Called(ctx, taskName, config)

	var r0 string
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context, string, map[string]string) (string, error)); ok {
		return rf(ctx, taskName, config)
	}
	if rf, ok := ret.Get(0).(func(context.Context, string, map[string]string) string); ok {
		r0 = rf(ctx, taskName, config)
	} else {
		r0 = ret.Get(0).(string)
	}

	if rf, ok := ret.Get(1).(func(context.Context, string, map[string]string) error); ok {
		r1 = rf(ctx, taskName, config)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// IdentifyUpstreams provides a mock function with given fields: ctx, taskName, config, assets
func (_m *PluginService) IdentifyUpstreams(ctx context.Context, taskName string, config, assets map[string]string) ([]string, error) {
	ret := _m.Called(ctx, taskName, config, assets)

	var r0 []string
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context, string, map[string]string, map[string]string) ([]string, error)); ok {
		return rf(ctx, taskName, config, assets)
	}
	if rf, ok := ret.Get(0).(func(context.Context, string, map[string]string, map[string]string) []string); ok {
		r0 = rf(ctx, taskName, config, assets)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]string)
		}
	}

	if rf, ok := ret.Get(1).(func(context.Context, string, map[string]string, map[string]string) error); ok {
		r1 = rf(ctx, taskName, config, assets)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// Info provides a mock function with given fields: ctx, taskName
func (_m *PluginService) Info(ctx context.Context, taskName string) (*plugin.Info, error) {
	ret := _m.Called(ctx, taskName)

	var r0 *plugin.Info
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context, string) (*plugin.Info, error)); ok {
		return rf(ctx, taskName)
	}
	if rf, ok := ret.Get(0).(func(context.Context, string) *plugin.Info); ok {
		r0 = rf(ctx, taskName)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*plugin.Info)
		}
	}

	if rf, ok := ret.Get(1).(func(context.Context, string) error); ok {
		r1 = rf(ctx, taskName)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// UpstreamResolver is an autogenerated mock type for the UpstreamResolver type
type UpstreamResolver struct {
	mock.Mock
}

// BulkResolve provides a mock function with given fields: ctx, projectName, jobs, logWriter
func (_m *UpstreamResolver) BulkResolve(ctx context.Context, projectName tenant.ProjectName, jobs []*job.Job, logWriter writer.LogWriter) ([]*job.WithUpstream, error) {
	ret := _m.Called(ctx, projectName, jobs, logWriter)

	var r0 []*job.WithUpstream
	if rf, ok := ret.Get(0).(func(context.Context, tenant.ProjectName, []*job.Job, writer.LogWriter) []*job.WithUpstream); ok {
		r0 = rf(ctx, projectName, jobs, logWriter)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]*job.WithUpstream)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, tenant.ProjectName, []*job.Job, writer.LogWriter) error); ok {
		r1 = rf(ctx, projectName, jobs, logWriter)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// Resolve provides a mock function with given fields: ctx, subjectJob, logWriter
func (_m *UpstreamResolver) Resolve(ctx context.Context, subjectJob *job.Job, logWriter writer.LogWriter) ([]*job.Upstream, error) {
	ret := _m.Called(ctx, subjectJob, logWriter)

	var r0 []*job.Upstream
	if rf, ok := ret.Get(0).(func(context.Context, *job.Job, writer.LogWriter) []*job.Upstream); ok {
		r0 = rf(ctx, subjectJob, logWriter)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]*job.Upstream)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, *job.Job, writer.LogWriter) error); ok {
		r1 = rf(ctx, subjectJob, logWriter)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// TenantDetailsGetter is an autogenerated mock type for the TenantDetailsGetter type
type TenantDetailsGetter struct {
	mock.Mock
}

// GetDetails provides a mock function with given fields: ctx, jobTenant
func (_m *TenantDetailsGetter) GetDetails(ctx context.Context, jobTenant tenant.Tenant) (*tenant.WithDetails, error) {
	ret := _m.Called(ctx, jobTenant)

	var r0 *tenant.WithDetails
	if rf, ok := ret.Get(0).(func(context.Context, tenant.Tenant) *tenant.WithDetails); ok {
		r0 = rf(ctx, jobTenant)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*tenant.WithDetails)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, tenant.Tenant) error); ok {
		r1 = rf(ctx, jobTenant)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

type mockWriter struct {
	mock.Mock
}

func (m *mockWriter) Write(level writer.LogLevel, s string) error {
	return m.Called(level, s).Error(0)
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

// JobDeploymentService is an autogenerated mock type for the JobDeploymentService type
type JobDeploymentService struct {
	mock.Mock
}

// UploadJobs provides a mock function with given fields: ctx, jobTenant, toUpdate, toDelete
func (_m *JobDeploymentService) UploadJobs(ctx context.Context, jobTenant tenant.Tenant, toUpdate, toDelete []string) error {
	ret := _m.Called(ctx, jobTenant, toUpdate, toDelete)

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, tenant.Tenant, []string, []string) error); ok {
		r0 = rf(ctx, jobTenant, toUpdate, toDelete)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

func (_m *JobDeploymentService) UpdateJobScheduleState(ctx context.Context, tnnt tenant.Tenant, jobNames []job.Name, state string) error {
	args := _m.Called(ctx, tnnt, jobNames, state)
	return args.Error(0)
}

func jobResourceURNsToString(resourceURNs []job.ResourceURN) []string {
	output := make([]string, len(resourceURNs))
	for i, urn := range resourceURNs {
		output[i] = urn.String()
	}
	return output
}
