package service_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/goto/salt/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/goto/optimus/config"
	"github.com/goto/optimus/core/event/moderator"
	"github.com/goto/optimus/core/job"
	"github.com/goto/optimus/core/job/dto"
	"github.com/goto/optimus/core/job/service"
	"github.com/goto/optimus/core/resource"
	"github.com/goto/optimus/core/scheduler"
	"github.com/goto/optimus/core/tenant"
	"github.com/goto/optimus/internal/compiler"
	optErrors "github.com/goto/optimus/internal/errors"
	"github.com/goto/optimus/internal/lib/window"
	"github.com/goto/optimus/internal/models"
	"github.com/goto/optimus/internal/utils/filter"
	"github.com/goto/optimus/internal/writer"
	"github.com/goto/optimus/plugin"
)

func TestJobService(t *testing.T) {
	ctx := context.Background()
	project, _ := tenant.NewProject("test-proj",
		map[string]string{
			"bucket":                     "gs://some_folder-2",
			tenant.ProjectSchedulerHost:  "host",
			tenant.ProjectStoragePathKey: "gs://location",
		}, map[string]string{})
	namespace, _ := tenant.NewNamespace("test-ns", project.Name(),
		map[string]string{
			"bucket": "gs://ns_bucket",
		}, map[string]string{})
	secret1, err := tenant.NewPlainTextSecret("table_name", "secret_table")
	assert.Nil(t, err)

	sampleTenant, _ := tenant.NewTenant(project.Name().String(), namespace.Name().String())
	detailedTenant, _ := tenant.NewTenantDetails(project, namespace, []*tenant.PlainTextSecret{secret1})

	otherNamespace, _ := tenant.NewNamespace("other-ns", project.Name(),
		map[string]string{
			"bucket": "gs://other_ns_bucket",
		}, map[string]string{})
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
	pluginSpec := plugin.Spec{
		SpecVersion: 2,
		Name:        "bq2bq",
		Description: "",
		PluginVersion: map[string]plugin.VersionDetails{
			"default": {
				Image: "example.io/bq2bq",
				Tag:   "1.0.5",
			},
		},
	}
	taskName, _ := job.TaskNameFrom("bq2bq")
	jobTask := job.NewTask(taskName, jobTaskConfig, "", nil)
	jobAsset := job.Asset(map[string]string{
		"query.sql": "select * from `project.dataset.sample`",
	})

	log := log.NewNoop()

	var jobNamesWithInvalidSpec []job.Name
	var emptyJobNames []string

	resourceURNA, err := resource.ParseURN("bigquery://project:dataset.tableA")
	assert.NoError(t, err)
	resourceURNB, err := resource.ParseURN("bigquery://project:dataset.tableB")
	assert.NoError(t, err)
	resourceURNC, err := resource.ParseURN("bigquery://project:dataset.tableC")
	assert.NoError(t, err)
	resourceURND, err := resource.ParseURN("bigquery://project:dataset.tableD")
	assert.NoError(t, err)

	t.Run("Add", func(t *testing.T) {
		t.Run("add jobs", func(t *testing.T) {
			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			upstreamRepo := new(UpstreamRepository)
			defer upstreamRepo.AssertExpectations(t)

			downstreamRepo := new(DownstreamRepository)
			defer downstreamRepo.AssertExpectations(t)

			pluginService := NewPluginService(t)

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

			jobADestination := resourceURNA
			pluginService.On("ConstructDestinationURN", ctx, specA.Task().Name().String(), mock.Anything).Return(jobADestination, nil)

			jobAUpstreamName := []resource.URN{resourceURNB}
			pluginService.On("IdentifyUpstreams", ctx, specA.Task().Name().String(), mock.Anything, mock.Anything).Return(jobAUpstreamName, nil)

			jobA := job.NewJob(sampleTenant, specA, jobADestination, jobAUpstreamName, false)
			jobs := []*job.Job{jobA}
			jobRepo.On("Add", ctx, mock.Anything).Return(jobs, nil, nil)

			downstreamRepo.On("GetDownstreamByDestination", ctx, mock.Anything, jobADestination).Return([]*job.Downstream{}, nil)

			upstream := job.NewUpstreamResolved("job-B", "", resourceURNB, sampleTenant, "static", taskName, false)
			jobWithUpstream := job.NewWithUpstream(jobA, []*job.Upstream{upstream})
			upstreamResolver.On("BulkResolve", ctx, project.Name(), jobs, mock.Anything).Return([]*job.WithUpstream{jobWithUpstream}, nil, nil)

			upstreamRepo.On("ReplaceUpstreams", ctx, []*job.WithUpstream{jobWithUpstream}).Return(nil)

			jobNamesToUpload := []string{jobA.GetName()}
			jobDeploymentService.On("UploadJobs", ctx, sampleTenant, jobNamesToUpload, emptyJobNames).Return(nil)

			eventHandler.On("HandleEvent", mock.Anything).Times(1)

			jobService := service.NewJobService(jobRepo, upstreamRepo, downstreamRepo, pluginService, upstreamResolver, tenantDetailsGetter, eventHandler, log, jobDeploymentService, compiler.NewEngine(), nil, nil, nil, config.JobValidationConfig{})
			addedJobs, err := jobService.Add(ctx, sampleTenant, specs)

			assert.NoError(t, err)
			assert.Len(t, addedJobs, len(specs))
		})
		t.Run("add jobs as a new upstream for existing job", func(t *testing.T) {
			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			upstreamRepo := new(UpstreamRepository)
			defer upstreamRepo.AssertExpectations(t)

			downstreamRepo := new(DownstreamRepository)
			defer downstreamRepo.AssertExpectations(t)

			pluginService := NewPluginService(t)

			upstreamResolver := new(UpstreamResolver)
			defer upstreamResolver.AssertExpectations(t)

			tenantDetailsGetter := new(TenantDetailsGetter)
			defer tenantDetailsGetter.AssertExpectations(t)

			jobDeploymentService := new(JobDeploymentService)
			defer jobDeploymentService.AssertExpectations(t)

			eventHandler := newEventHandler(t)

			specA, _ := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()
			specB, _ := job.NewSpecBuilder(jobVersion, "job-B", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()
			specs := []*job.Spec{specA}

			tenantDetailsGetter.On("GetDetails", ctx, sampleTenant).Return(detailedTenant, nil)

			jobADestination := resourceURNA
			jobBDestination := resourceURNB
			pluginService.On("ConstructDestinationURN", ctx, specA.Task().Name().String(), mock.Anything).Return(jobADestination, nil)

			jobAUpstreamName := []resource.URN{}
			jobBUpstreamName := []resource.URN{resourceURNA}
			pluginService.On("IdentifyUpstreams", ctx, specA.Task().Name().String(), mock.Anything, mock.Anything).Return(jobAUpstreamName, nil)

			jobA := job.NewJob(sampleTenant, specA, jobADestination, jobAUpstreamName, false)
			jobB := job.NewJob(sampleTenant, specB, jobBDestination, jobBUpstreamName, false)
			jobs := []*job.Job{jobA}
			jobRepo.On("Add", ctx, mock.Anything).Return(jobs, nil, nil)

			jobBDownstream := job.NewDownstream("job-B", sampleTenant.ProjectName(), sampleTenant.NamespaceName(), jobTask.Name())
			downstreamRepo.On("GetDownstreamByDestination", ctx, mock.Anything, jobADestination).Return([]*job.Downstream{jobBDownstream}, nil)
			jobRepo.On("GetByJobName", ctx, mock.Anything, jobBDownstream.Name()).Return(jobB, nil)

			upstream := job.NewUpstreamResolved("job-A", "", resourceURNA, sampleTenant, "static", taskName, false)
			jobAWithUpstream := job.NewWithUpstream(jobA, []*job.Upstream{})
			jobBWithUpstream := job.NewWithUpstream(jobB, []*job.Upstream{upstream})
			upstreamResolver.On("BulkResolve", ctx, project.Name(), mock.MatchedBy(func(elems []*job.Job) bool {
				return assert.ElementsMatch(t, elems, append(jobs, jobB))
			}), mock.Anything).Return([]*job.WithUpstream{jobAWithUpstream, jobBWithUpstream}, nil, nil)

			upstreamRepo.On("ReplaceUpstreams", ctx, []*job.WithUpstream{jobAWithUpstream, jobBWithUpstream}).Return(nil)

			jobNamesToUpload := []string{jobA.GetName(), jobB.GetName()}
			jobDeploymentService.On("UploadJobs", ctx, sampleTenant, mock.MatchedBy(func(elems []string) bool {
				return assert.ElementsMatch(t, elems, jobNamesToUpload)
			}), emptyJobNames).Return(nil)

			eventHandler.On("HandleEvent", mock.Anything).Times(1)

			jobService := service.NewJobService(jobRepo, upstreamRepo, downstreamRepo, pluginService, upstreamResolver, tenantDetailsGetter, eventHandler, log, jobDeploymentService, compiler.NewEngine(), nil, nil, nil, config.JobValidationConfig{})
			addedJobs, err := jobService.Add(ctx, sampleTenant, specs)
			assert.NoError(t, err)
			assert.Len(t, addedJobs, len(specs))
		})
		t.Run("return error if unable to get detailed tenant", func(t *testing.T) {
			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			upstreamRepo := new(UpstreamRepository)
			defer upstreamRepo.AssertExpectations(t)

			downstreamRepo := new(DownstreamRepository)
			defer downstreamRepo.AssertExpectations(t)

			pluginService := NewPluginService(t)

			upstreamResolver := new(UpstreamResolver)
			defer upstreamResolver.AssertExpectations(t)

			tenantDetailsGetter := new(TenantDetailsGetter)
			defer tenantDetailsGetter.AssertExpectations(t)

			specA, _ := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()
			specs := []*job.Spec{specA}

			tenantDetailsGetter.On("GetDetails", ctx, sampleTenant).Return(&tenant.WithDetails{}, errors.New("internal error"))

			jobService := service.NewJobService(jobRepo, upstreamRepo, downstreamRepo, pluginService, upstreamResolver, tenantDetailsGetter, nil, log, nil, compiler.NewEngine(), nil, nil, nil, config.JobValidationConfig{})
			_, err := jobService.Add(ctx, sampleTenant, specs)
			assert.ErrorContains(t, err, "internal error")
		})
		t.Run("skip job that has issue when generating destination and return error", func(t *testing.T) {
			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			upstreamRepo := new(UpstreamRepository)
			defer upstreamRepo.AssertExpectations(t)

			downstreamRepo := new(DownstreamRepository)
			defer downstreamRepo.AssertExpectations(t)

			pluginService := NewPluginService(t)

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

			jobADestination := resourceURNA
			jobBDestination := resourceURNB
			var jobDestination resource.URN
			pluginService.On("ConstructDestinationURN", ctx, specA.Task().Name().String(), mock.Anything).Return(jobADestination, nil).Once()
			pluginService.On("ConstructDestinationURN", ctx, specB.Task().Name().String(), mock.Anything).Return(jobBDestination, nil).Once()
			pluginService.On("ConstructDestinationURN", ctx, specC.Task().Name().String(), mock.Anything).Return(jobDestination, errors.New("generate destination error")).Once()

			jobAUpstreamName := []resource.URN{resourceURNB}
			pluginService.On("IdentifyUpstreams", ctx, specB.Task().Name().String(), mock.Anything, mock.Anything).Return([]resource.URN{}, errors.New("generate upstream error"))
			pluginService.On("IdentifyUpstreams", ctx, specA.Task().Name().String(), mock.Anything, mock.Anything).Return(jobAUpstreamName, nil)

			jobA := job.NewJob(sampleTenant, specA, jobADestination, jobAUpstreamName, false)
			jobs := []*job.Job{jobA}
			jobRepo.On("Add", ctx, mock.Anything).Return(jobs, nil)

			upstream := job.NewUpstreamResolved("job-B", "", resourceURNB, sampleTenant, "static", taskName, false)
			jobWithUpstream := job.NewWithUpstream(jobA, []*job.Upstream{upstream})
			upstreamResolver.On("BulkResolve", ctx, project.Name(), jobs, mock.Anything).Return([]*job.WithUpstream{jobWithUpstream}, nil, nil)

			upstreamRepo.On("ReplaceUpstreams", ctx, []*job.WithUpstream{jobWithUpstream}).Return(nil)

			jobNamesToUpload := []string{jobA.GetName()}
			jobDeploymentService.On("UploadJobs", ctx, sampleTenant, jobNamesToUpload, emptyJobNames).Return(nil)

			eventHandler.On("HandleEvent", mock.Anything).Times(1)

			jobService := service.NewJobService(jobRepo, upstreamRepo, downstreamRepo, pluginService, upstreamResolver, tenantDetailsGetter, eventHandler, log, jobDeploymentService, compiler.NewEngine(), nil, nil, nil, config.JobValidationConfig{})
			_, err := jobService.Add(ctx, sampleTenant, specs)
			assert.ErrorContains(t, err, "generate upstream error")
		})
		t.Run("return error when all jobs failed to have destination and upstream generated", func(t *testing.T) {
			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			upstreamRepo := new(UpstreamRepository)
			defer upstreamRepo.AssertExpectations(t)

			downstreamRepo := new(DownstreamRepository)
			defer downstreamRepo.AssertExpectations(t)

			pluginService := NewPluginService(t)

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

			var jobADestination resource.URN
			jobBDestination := resourceURNB
			pluginService.On("ConstructDestinationURN", ctx, specB.Task().Name().String(), mock.Anything).Return(jobBDestination, nil).Once()
			pluginService.On("ConstructDestinationURN", ctx, specA.Task().Name().String(), mock.Anything).Return(jobADestination, errors.New("generate destination error")).Once()

			pluginService.On("IdentifyUpstreams", ctx, specB.Task().Name().String(), mock.Anything, mock.Anything).Return(nil, errors.New("generate upstream error"))

			jobService := service.NewJobService(jobRepo, upstreamRepo, downstreamRepo, pluginService, upstreamResolver, tenantDetailsGetter, nil, log, nil, compiler.NewEngine(), nil, nil, nil, config.JobValidationConfig{})
			_, err := jobService.Add(ctx, sampleTenant, specs)
			assert.ErrorContains(t, err, "generate upstream error")
		})
		t.Run("should not skip nor return error if jobs is not bq2bq", func(t *testing.T) {
			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			upstreamRepo := new(UpstreamRepository)
			defer upstreamRepo.AssertExpectations(t)

			downstreamRepo := new(DownstreamRepository)
			defer downstreamRepo.AssertExpectations(t)

			pluginService := NewPluginService(t)

			upstreamResolver := new(UpstreamResolver)
			defer upstreamResolver.AssertExpectations(t)

			tenantDetailsGetter := new(TenantDetailsGetter)
			defer tenantDetailsGetter.AssertExpectations(t)

			jobDeploymentService := new(JobDeploymentService)
			defer jobDeploymentService.AssertExpectations(t)

			eventHandler := newEventHandler(t)

			nonBq2bqTask := job.NewTask("another", nil, "", nil)
			specA, _ := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, jobWindow, nonBq2bqTask).Build()
			specs := []*job.Spec{specA}

			tenantDetailsGetter.On("GetDetails", ctx, sampleTenant).Return(detailedTenant, nil)

			jobA := job.NewJob(sampleTenant, specA, resource.ZeroURN(), nil, false)
			jobs := []*job.Job{jobA}

			jobRepo.On("Add", ctx, mock.Anything).Return(jobs, nil)

			pluginService.On("ConstructDestinationURN", ctx, specA.Task().Name().String(), mock.Anything).Return(resource.ZeroURN(), nil)
			pluginService.On("IdentifyUpstreams", ctx, specA.Task().Name().String(), mock.Anything, mock.Anything).Return([]resource.URN{}, nil)
			jobWithUpstream := job.NewWithUpstream(jobA, nil)
			upstreamResolver.On("BulkResolve", ctx, project.Name(), jobs, mock.Anything).Return([]*job.WithUpstream{jobWithUpstream}, nil, nil)

			upstreamRepo.On("ReplaceUpstreams", ctx, []*job.WithUpstream{jobWithUpstream}).Return(nil)

			jobNamesToUpload := []string{jobA.GetName()}
			jobDeploymentService.On("UploadJobs", ctx, sampleTenant, jobNamesToUpload, emptyJobNames).Return(nil)

			eventHandler.On("HandleEvent", mock.Anything).Times(1)

			jobService := service.NewJobService(jobRepo, upstreamRepo, downstreamRepo, pluginService, upstreamResolver, tenantDetailsGetter, eventHandler, log, jobDeploymentService, compiler.NewEngine(), nil, nil, nil, config.JobValidationConfig{})
			addedJobs, err := jobService.Add(ctx, sampleTenant, specs)
			assert.NoError(t, err)
			assert.Len(t, addedJobs, len(specs))
		})
		t.Run("should skip and not return error if one of the job is failed to be inserted to db", func(t *testing.T) {
			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			upstreamRepo := new(UpstreamRepository)
			defer upstreamRepo.AssertExpectations(t)

			downstreamRepo := new(DownstreamRepository)
			defer downstreamRepo.AssertExpectations(t)

			pluginService := NewPluginService(t)

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

			resourceA := resourceURNA
			var resourceB resource.URN
			pluginService.On("ConstructDestinationURN", ctx, specA.Task().Name().String(), mock.Anything).Return(resourceA, nil).Once()
			pluginService.On("ConstructDestinationURN", ctx, specB.Task().Name().String(), mock.Anything).Return(resourceB, errors.New("some error")).Once()

			jobSourcesA := []resource.URN{resourceURNB}
			pluginService.On("IdentifyUpstreams", ctx, specA.Task().Name().String(), mock.Anything, mock.Anything).Return(jobSourcesA, nil)
			pluginService.On("IdentifyUpstreams", ctx, specB.Task().Name().String(), mock.Anything, mock.Anything).Return(nil, errors.New("another error"))

			jobB := job.NewJob(sampleTenant, specB, resource.ZeroURN(), nil, false)
			savedJobs := []*job.Job{jobB}
			jobRepo.On("Add", ctx, mock.Anything).Return(savedJobs, errors.New("unable to save job A"))

			downstreamRepo.On("GetDownstreamByDestination", ctx, mock.Anything, mock.Anything).Return([]*job.Downstream{}, nil)

			jobWithUpstreamB := job.NewWithUpstream(jobB, nil)
			upstreamResolver.On("BulkResolve", ctx, project.Name(), savedJobs, mock.Anything).Return([]*job.WithUpstream{jobWithUpstreamB}, nil, nil)

			upstreamRepo.On("ReplaceUpstreams", ctx, mock.Anything).Return(nil)

			jobNamesToUpload := []string{jobB.GetName()}
			jobDeploymentService.On("UploadJobs", ctx, sampleTenant, jobNamesToUpload, emptyJobNames).Return(nil)

			eventHandler.On("HandleEvent", mock.Anything).Times(1)

			jobService := service.NewJobService(jobRepo, upstreamRepo, downstreamRepo, pluginService, upstreamResolver, tenantDetailsGetter, eventHandler, log, jobDeploymentService, compiler.NewEngine(), nil, nil, nil, config.JobValidationConfig{})
			_, err := jobService.Add(ctx, sampleTenant, specs)
			assert.ErrorContains(t, err, "unable to save job A")
		})
		t.Run("return error when all jobs failed to be inserted to db", func(t *testing.T) {
			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			upstreamRepo := new(UpstreamRepository)
			defer upstreamRepo.AssertExpectations(t)

			downstreamRepo := new(DownstreamRepository)
			defer downstreamRepo.AssertExpectations(t)

			pluginService := NewPluginService(t)

			upstreamResolver := new(UpstreamResolver)
			defer upstreamResolver.AssertExpectations(t)

			tenantDetailsGetter := new(TenantDetailsGetter)
			defer tenantDetailsGetter.AssertExpectations(t)

			specA, _ := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()
			specs := []*job.Spec{specA}

			tenantDetailsGetter.On("GetDetails", ctx, sampleTenant).Return(detailedTenant, nil)

			downstreamRepo.On("GetDownstreamByDestination", ctx, mock.Anything, mock.Anything).Return([]*job.Downstream{}, nil)

			upstreamResolver.On("BulkResolve", ctx, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)

			upstreamRepo.On("ReplaceUpstreams", ctx, mock.Anything).Return(nil)

			resourceA := resourceURNA
			pluginService.On("ConstructDestinationURN", ctx, specA.Task().Name().String(), mock.Anything).Return(resourceA, nil).Once()

			jobSourcesA := []resource.URN{resourceURNB}
			pluginService.On("IdentifyUpstreams", ctx, specA.Task().Name().String(), mock.Anything, mock.Anything).Return(jobSourcesA, nil)

			jobRepo.On("Add", ctx, mock.Anything).Return([]*job.Job{}, errors.New("unable to save job A"), errors.New("all jobs failed"))

			jobService := service.NewJobService(jobRepo, upstreamRepo, downstreamRepo, pluginService, upstreamResolver, tenantDetailsGetter, nil, log, nil, compiler.NewEngine(), nil, nil, nil, config.JobValidationConfig{})
			_, err := jobService.Add(ctx, sampleTenant, specs)
			assert.ErrorContains(t, err, "unable to save job A")
		})
		t.Run("should return error if failed to save upstream", func(t *testing.T) {
			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			upstreamRepo := new(UpstreamRepository)
			defer upstreamRepo.AssertExpectations(t)

			downstreamRepo := new(DownstreamRepository)
			defer downstreamRepo.AssertExpectations(t)

			pluginService := NewPluginService(t)

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

			resourceA := resourceURNA
			pluginService.On("ConstructDestinationURN", ctx, specA.Task().Name().String(), mock.Anything).Return(resourceA, nil).Once()

			jobSourcesA := []resource.URN{resourceURNB}
			pluginService.On("IdentifyUpstreams", ctx, specA.Task().Name().String(), mock.Anything, mock.Anything).Return(jobSourcesA, nil)

			jobA := job.NewJob(sampleTenant, specA, resourceA, jobSourcesA, false)
			jobs := []*job.Job{jobA}

			jobRepo.On("Add", ctx, mock.Anything).Return(jobs, nil, nil)

			downstreamRepo.On("GetDownstreamByDestination", ctx, mock.Anything, mock.Anything).Return([]*job.Downstream{}, nil)

			jobWithUpstreamA := job.NewWithUpstream(jobA, nil)
			upstreamResolver.On("BulkResolve", ctx, project.Name(), jobs, mock.Anything).Return([]*job.WithUpstream{jobWithUpstreamA}, nil, nil)

			upstreamRepo.On("ReplaceUpstreams", ctx, mock.Anything).Return(errors.New("internal error"))

			jobNamesToUpload := []string{jobA.GetName()}
			jobDeploymentService.On("UploadJobs", ctx, sampleTenant, jobNamesToUpload, emptyJobNames).Return(nil)

			eventHandler.On("HandleEvent", mock.Anything).Times(1)

			jobService := service.NewJobService(jobRepo, upstreamRepo, downstreamRepo, pluginService, upstreamResolver, tenantDetailsGetter, eventHandler, log, jobDeploymentService, compiler.NewEngine(), nil, nil, nil, config.JobValidationConfig{})
			_, err := jobService.Add(ctx, sampleTenant, specs)
			assert.Error(t, err)
		})
		t.Run("should not return error if adding a job with its downstream", func(t *testing.T) {
			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			upstreamRepo := new(UpstreamRepository)
			defer upstreamRepo.AssertExpectations(t)

			downstreamRepo := new(DownstreamRepository)
			defer downstreamRepo.AssertExpectations(t)

			pluginService := NewPluginService(t)

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

			jobADestination := resourceURNA
			jobBDestination := resourceURNB
			pluginService.On("ConstructDestinationURN", ctx, specA.Task().Name().String(), mock.Anything).Return(jobADestination, nil)

			jobAUpstreamName := []resource.URN{}
			jobBUpstreamName := []resource.URN{resourceURNA}
			pluginService.On("IdentifyUpstreams", ctx, specA.Task().Name().String(), mock.Anything, mock.Anything).Return(jobAUpstreamName, nil)

			jobA := job.NewJob(sampleTenant, specA, jobADestination, jobAUpstreamName, false)
			jobB := job.NewJob(sampleTenant, specB, jobBDestination, jobBUpstreamName, false)
			jobs := []*job.Job{jobA, jobB}
			jobRepo.On("Add", ctx, mock.Anything).Return(jobs, nil, nil)

			jobBDownstream := job.NewDownstream("job-B", sampleTenant.ProjectName(), sampleTenant.NamespaceName(), jobTask.Name())
			downstreamRepo.On("GetDownstreamByDestination", ctx, mock.Anything, jobADestination).Return([]*job.Downstream{jobBDownstream}, nil)
			jobRepo.On("GetByJobName", ctx, mock.Anything, jobBDownstream.Name()).Return(jobB, nil)

			upstream := job.NewUpstreamResolved("job-A", "", resourceURNA, sampleTenant, "static", taskName, false)
			jobAWithUpstream := job.NewWithUpstream(jobA, []*job.Upstream{})
			jobBWithUpstream := job.NewWithUpstream(jobB, []*job.Upstream{upstream})
			upstreamResolver.On("BulkResolve", ctx, project.Name(), mock.MatchedBy(func(elems []*job.Job) bool {
				return assert.ElementsMatch(t, elems, jobs)
			}), mock.Anything).Return([]*job.WithUpstream{jobAWithUpstream, jobBWithUpstream}, nil, nil)

			upstreamRepo.On("ReplaceUpstreams", ctx, []*job.WithUpstream{jobAWithUpstream, jobBWithUpstream}).Return(nil)

			jobNamesToUpload := []string{jobA.GetName(), jobB.GetName()}
			jobDeploymentService.On("UploadJobs", ctx, sampleTenant, mock.MatchedBy(func(elems []string) bool {
				return assert.ElementsMatch(t, elems, jobNamesToUpload)
			}), emptyJobNames).Return(nil)

			eventHandler.On("HandleEvent", mock.Anything).Times(2)

			jobService := service.NewJobService(jobRepo, upstreamRepo, downstreamRepo, pluginService, upstreamResolver, tenantDetailsGetter, eventHandler, log, jobDeploymentService, compiler.NewEngine(), nil, nil, nil, config.JobValidationConfig{})
			addedJobs, err := jobService.Add(ctx, sampleTenant, specs)
			assert.NoError(t, err)
			assert.Len(t, addedJobs, len(specs))
		})
		t.Run("return error if encounter issue when uploading jobs", func(t *testing.T) {
			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			upstreamRepo := new(UpstreamRepository)
			defer upstreamRepo.AssertExpectations(t)

			downstreamRepo := new(DownstreamRepository)
			defer downstreamRepo.AssertExpectations(t)

			pluginService := NewPluginService(t)

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

			jobADestination := resourceURNA
			pluginService.On("ConstructDestinationURN", ctx, specA.Task().Name().String(), mock.Anything).Return(jobADestination, nil)

			jobAUpstreamName := []resource.URN{resourceURNB}
			pluginService.On("IdentifyUpstreams", ctx, specA.Task().Name().String(), mock.Anything, mock.Anything).Return(jobAUpstreamName, nil)

			jobA := job.NewJob(sampleTenant, specA, jobADestination, jobAUpstreamName, false)
			jobs := []*job.Job{jobA}
			jobRepo.On("Add", ctx, mock.Anything).Return(jobs, nil, nil)

			downstreamRepo.On("GetDownstreamByDestination", ctx, mock.Anything, mock.Anything).Return([]*job.Downstream{}, nil)

			upstream := job.NewUpstreamResolved("job-B", "", resourceURNB, sampleTenant, "static", taskName, false)
			jobWithUpstream := job.NewWithUpstream(jobA, []*job.Upstream{upstream})
			upstreamResolver.On("BulkResolve", ctx, project.Name(), jobs, mock.Anything).Return([]*job.WithUpstream{jobWithUpstream}, nil, nil)

			upstreamRepo.On("ReplaceUpstreams", ctx, []*job.WithUpstream{jobWithUpstream}).Return(nil)

			errorMsg := "internal error"
			jobDeploymentService.On("UploadJobs", ctx, sampleTenant, mock.Anything, emptyJobNames).Return(errors.New(errorMsg))

			eventHandler.On("HandleEvent", mock.Anything).Times(1)

			jobService := service.NewJobService(jobRepo, upstreamRepo, downstreamRepo, pluginService, upstreamResolver, tenantDetailsGetter, eventHandler, log, jobDeploymentService, compiler.NewEngine(), nil, nil, nil, config.JobValidationConfig{})
			_, err := jobService.Add(ctx, sampleTenant, specs)
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

			pluginService := NewPluginService(t)

			upstreamResolver := new(UpstreamResolver)
			defer upstreamResolver.AssertExpectations(t)

			tenantDetailsGetter := new(TenantDetailsGetter)
			defer tenantDetailsGetter.AssertExpectations(t)

			jobDeploymentService := new(JobDeploymentService)
			defer jobDeploymentService.AssertExpectations(t)

			alertManager := new(AlertManager)
			alertManager.On("SendJobEvent", mock.Anything).Return()
			defer alertManager.AssertExpectations(t)

			eventHandler := newEventHandler(t)

			specA, _ := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()
			specs := []*job.Spec{specA}

			tenantDetailsGetter.On("GetDetails", ctx, sampleTenant).Return(detailedTenant, nil)

			jobADestination := resourceURNA
			pluginService.On("ConstructDestinationURN", ctx, specA.Task().Name().String(), mock.Anything).Return(jobADestination, nil)

			jobAUpstreamName := []resource.URN{resourceURNB}
			pluginService.On("IdentifyUpstreams", ctx, specA.Task().Name().String(), mock.Anything, mock.Anything).Return(jobAUpstreamName, nil)

			downstreamRepo.On("GetDownstreamByDestination", ctx, mock.Anything, mock.Anything).Return([]*job.Downstream{}, nil)

			jobA := job.NewJob(sampleTenant, specA, jobADestination, jobAUpstreamName, false)
			jobs := []*job.Job{jobA}
			jobRepo.On("Update", ctx, mock.Anything).Return(jobs, nil, nil)
			jobRepo.On("GetByJobName", ctx, project.Name(), specA.Name()).Return(jobA, nil)

			upstream := job.NewUpstreamResolved("job-B", "", resourceURNB, sampleTenant, "static", taskName, false)
			jobWithUpstream := job.NewWithUpstream(jobA, []*job.Upstream{upstream})
			upstreamResolver.On("BulkResolve", ctx, project.Name(), jobs, mock.Anything).Return([]*job.WithUpstream{jobWithUpstream}, nil, nil)

			upstreamRepo.On("ReplaceUpstreams", ctx, []*job.WithUpstream{jobWithUpstream}).Return(nil)

			jobNamesToUpload := []string{jobA.GetName()}
			jobDeploymentService.On("UploadJobs", ctx, sampleTenant, jobNamesToUpload, emptyJobNames).Return(nil)

			eventHandler.On("HandleEvent", mock.Anything).Times(1)

			jobService := service.NewJobService(jobRepo, upstreamRepo, downstreamRepo, pluginService, upstreamResolver, tenantDetailsGetter, eventHandler, log, jobDeploymentService, compiler.NewEngine(), nil, nil, alertManager, config.JobValidationConfig{})
			updateJobs, err := jobService.Update(ctx, sampleTenant, specs)
			assert.NoError(t, err)
			assert.Len(t, updateJobs, 1)
		})

		t.Run("update jobs as a new upstream for another job", func(t *testing.T) {
			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			upstreamRepo := new(UpstreamRepository)
			defer upstreamRepo.AssertExpectations(t)

			downstreamRepo := new(DownstreamRepository)
			defer downstreamRepo.AssertExpectations(t)

			pluginService := NewPluginService(t)

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
			specs := []*job.Spec{specA}

			tenantDetailsGetter.On("GetDetails", ctx, sampleTenant).Return(detailedTenant, nil)

			jobADestination := resourceURNA
			jobBDestination := resourceURNB
			pluginService.On("ConstructDestinationURN", ctx, specA.Task().Name().String(), mock.Anything).Return(jobADestination, nil)

			jobAUpstreamName := []resource.URN{}
			jobBUpstreamName := []resource.URN{resourceURNA}
			jobCUpstreamName := []resource.URN{resourceURNA}
			pluginService.On("IdentifyUpstreams", ctx, specA.Task().Name().String(), mock.Anything, mock.Anything).Return(jobAUpstreamName, nil)

			jobA := job.NewJob(sampleTenant, specA, jobADestination, jobAUpstreamName, false)
			jobB := job.NewJob(sampleTenant, specB, jobBDestination, jobBUpstreamName, false)
			jobC := job.NewJob(sampleTenant, specC, jobBDestination, jobCUpstreamName, false)
			jobs := []*job.Job{jobA}
			jobRepo.On("GetByJobName", ctx, project.Name(), specA.Name()).Return(jobA, nil)

			jobBDownstream := job.NewDownstream("job-B", sampleTenant.ProjectName(), sampleTenant.NamespaceName(), jobTask.Name())
			downstreamRepo.On("GetDownstreamByDestination", ctx, mock.Anything, jobADestination).Return([]*job.Downstream{jobBDownstream}, nil).Once()
			jobRepo.On("GetByJobName", ctx, mock.Anything, jobBDownstream.Name()).Return(jobB, nil)

			jobRepo.On("Update", ctx, mock.Anything).Return(jobs, nil, nil)

			jobCDownstream := job.NewDownstream("job-C", sampleTenant.ProjectName(), sampleTenant.NamespaceName(), jobTask.Name())
			downstreamRepo.On("GetDownstreamByDestination", ctx, mock.Anything, jobADestination).Return([]*job.Downstream{jobCDownstream}, nil).Once()
			jobRepo.On("GetByJobName", ctx, mock.Anything, jobCDownstream.Name()).Return(jobC, nil)

			upstream := job.NewUpstreamResolved("job-A", "", resourceURNA, sampleTenant, "static", taskName, false)
			jobAWithUpstream := job.NewWithUpstream(jobA, []*job.Upstream{})
			jobBWithUpstream := job.NewWithUpstream(jobB, []*job.Upstream{})
			jobCWithUpstream := job.NewWithUpstream(jobC, []*job.Upstream{upstream})
			upstreamResolver.On("BulkResolve", ctx, project.Name(), mock.MatchedBy(func(elems []*job.Job) bool {
				return assert.ElementsMatch(t, elems, append(jobs, jobB, jobC))
			}), mock.Anything).Return([]*job.WithUpstream{jobAWithUpstream, jobBWithUpstream, jobCWithUpstream}, nil, nil)

			upstreamRepo.On("ReplaceUpstreams", ctx, []*job.WithUpstream{jobAWithUpstream, jobBWithUpstream, jobCWithUpstream}).Return(nil)

			jobNamesToUpload := []string{jobA.GetName(), jobB.GetName(), jobC.GetName()}
			jobDeploymentService.On("UploadJobs", ctx, sampleTenant, mock.MatchedBy(func(elems []string) bool {
				return assert.ElementsMatch(t, elems, jobNamesToUpload)
			}), emptyJobNames).Return(nil)

			eventHandler.On("HandleEvent", mock.Anything).Times(1)

			alertManager := new(AlertManager)
			alertManager.On("SendJobEvent", mock.Anything).Return()
			defer alertManager.AssertExpectations(t)

			jobService := service.NewJobService(jobRepo, upstreamRepo, downstreamRepo, pluginService, upstreamResolver, tenantDetailsGetter, eventHandler, log, jobDeploymentService, compiler.NewEngine(), nil, nil, alertManager, config.JobValidationConfig{})
			updateJobs, err := jobService.Update(ctx, sampleTenant, specs)
			assert.NoError(t, err)
			assert.Len(t, updateJobs, 1)
		})

		t.Run("return error if unable to get detailed tenant", func(t *testing.T) {
			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			upstreamRepo := new(UpstreamRepository)
			defer upstreamRepo.AssertExpectations(t)

			downstreamRepo := new(DownstreamRepository)
			defer downstreamRepo.AssertExpectations(t)

			pluginService := NewPluginService(t)

			upstreamResolver := new(UpstreamResolver)
			defer upstreamResolver.AssertExpectations(t)

			tenantDetailsGetter := new(TenantDetailsGetter)
			defer tenantDetailsGetter.AssertExpectations(t)

			specA, _ := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()
			specs := []*job.Spec{specA}

			tenantDetailsGetter.On("GetDetails", ctx, sampleTenant).Return(&tenant.WithDetails{}, errors.New("internal error"))

			jobService := service.NewJobService(jobRepo, upstreamRepo, downstreamRepo, pluginService, upstreamResolver, tenantDetailsGetter, nil, log, nil, compiler.NewEngine(), nil, nil, nil, config.JobValidationConfig{})
			updatedJobs, err := jobService.Update(ctx, sampleTenant, specs)
			assert.ErrorContains(t, err, "internal error")
			assert.Empty(t, updatedJobs)
		})
		t.Run("skip job that has issue when generating destination and return error", func(t *testing.T) {
			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			upstreamRepo := new(UpstreamRepository)
			defer upstreamRepo.AssertExpectations(t)

			downstreamRepo := new(DownstreamRepository)
			defer downstreamRepo.AssertExpectations(t)

			pluginService := NewPluginService(t)

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

			jobADestination := resourceURNA
			jobBDestination := resourceURNB
			jobCDestination := resourceURNC
			var jobDestination resource.URN
			pluginService.On("ConstructDestinationURN", ctx, specB.Task().Name().String(), mock.Anything).Return(jobBDestination, nil).Once()
			pluginService.On("ConstructDestinationURN", ctx, specA.Task().Name().String(), mock.Anything).Return(jobADestination, nil).Once()
			pluginService.On("ConstructDestinationURN", ctx, specC.Task().Name().String(), mock.Anything).Return(jobDestination, errors.New("generate destination error")).Once()

			jobAUpstreamName := []resource.URN{resourceURNB}
			jobBUpstreamName := []resource.URN{resourceURNC}
			jobCUpstreamName := []resource.URN{resourceURND}
			pluginService.On("IdentifyUpstreams", ctx, specA.Task().Name().String(), mock.Anything, mock.Anything).Return([]resource.URN{}, errors.New("generate upstream error"))
			pluginService.On("IdentifyUpstreams", ctx, specB.Task().Name().String(), mock.Anything, mock.Anything).Return(jobAUpstreamName, nil)

			jobA := job.NewJob(sampleTenant, specA, jobADestination, jobAUpstreamName, false)
			jobB := job.NewJob(sampleTenant, specA, jobBDestination, jobBUpstreamName, false)
			jobC := job.NewJob(sampleTenant, specA, jobCDestination, jobCUpstreamName, false)
			jobs := []*job.Job{jobA}
			jobRepo.On("Update", ctx, mock.Anything).Return(jobs, nil, nil)
			jobRepo.On("GetByJobName", ctx, project.Name(), specA.Name()).Return(jobA, nil)
			jobRepo.On("GetByJobName", ctx, project.Name(), specB.Name()).Return(jobB, nil)
			jobRepo.On("GetByJobName", ctx, project.Name(), specC.Name()).Return(jobC, nil)

			downstreamRepo.On("GetDownstreamByDestination", ctx, mock.Anything, mock.Anything).Return([]*job.Downstream{}, nil)

			upstream := job.NewUpstreamResolved("job-B", "", resourceURNB, sampleTenant, "static", taskName, false)
			jobWithUpstream := job.NewWithUpstream(jobA, []*job.Upstream{upstream})
			upstreamResolver.On("BulkResolve", ctx, project.Name(), jobs, mock.Anything).Return([]*job.WithUpstream{jobWithUpstream}, nil, nil)

			upstreamRepo.On("ReplaceUpstreams", ctx, []*job.WithUpstream{jobWithUpstream}).Return(nil)

			jobNamesToUpload := []string{jobA.GetName()}
			jobDeploymentService.On("UploadJobs", ctx, sampleTenant, jobNamesToUpload, emptyJobNames).Return(nil)

			eventHandler.On("HandleEvent", mock.Anything).Times(1)

			alertManager := new(AlertManager)
			alertManager.On("SendJobEvent", mock.Anything).Return()
			defer alertManager.AssertExpectations(t)

			jobService := service.NewJobService(jobRepo, upstreamRepo, downstreamRepo, pluginService, upstreamResolver, tenantDetailsGetter, eventHandler, log, jobDeploymentService, compiler.NewEngine(), nil, nil, alertManager, config.JobValidationConfig{})
			updatedJobs, err := jobService.Update(ctx, sampleTenant, specs)
			assert.ErrorContains(t, err, "generate upstream error")
			assert.Len(t, updatedJobs, 1)
		})
		t.Run("return error when all jobs failed to have destination and upstream generated", func(t *testing.T) {
			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			upstreamRepo := new(UpstreamRepository)
			defer upstreamRepo.AssertExpectations(t)

			downstreamRepo := new(DownstreamRepository)
			defer downstreamRepo.AssertExpectations(t)

			pluginService := NewPluginService(t)

			upstreamResolver := new(UpstreamResolver)
			defer upstreamResolver.AssertExpectations(t)

			tenantDetailsGetter := new(TenantDetailsGetter)
			defer tenantDetailsGetter.AssertExpectations(t)

			specA, _ := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()
			specB, _ := job.NewSpecBuilder(jobVersion, "job-B", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()
			jobA := job.NewJob(sampleTenant, specA, resourceURNA, []resource.URN{resourceURNB}, false)
			jobB := job.NewJob(sampleTenant, specA, resourceURNB, []resource.URN{resourceURNA}, false)
			specs := []*job.Spec{specB, specA}

			jobRepo.On("Update", ctx, mock.Anything).Return(nil, nil)
			jobRepo.On("GetByJobName", ctx, project.Name(), specA.Name()).Return(jobA, nil)
			jobRepo.On("GetByJobName", ctx, project.Name(), specB.Name()).Return(jobB, nil)

			downstreamRepo.On("GetDownstreamByDestination", ctx, mock.Anything, mock.Anything).Return([]*job.Downstream{}, nil)

			upstreamResolver.On("BulkResolve", ctx, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)

			upstreamRepo.On("ReplaceUpstreams", ctx, mock.Anything).Return(nil)

			tenantDetailsGetter.On("GetDetails", ctx, sampleTenant).Return(detailedTenant, nil)

			var jobADestination resource.URN
			jobBDestination := resourceURNB
			pluginService.On("ConstructDestinationURN", ctx, specB.Task().Name().String(), mock.Anything).Return(jobBDestination, nil).Once()
			pluginService.On("ConstructDestinationURN", ctx, specA.Task().Name().String(), mock.Anything).Return(jobADestination, errors.New("generate destination error")).Once()

			pluginService.On("IdentifyUpstreams", ctx, specB.Task().Name().String(), mock.Anything, mock.Anything).Return(nil, errors.New("generate upstream error"))

			jobService := service.NewJobService(jobRepo, upstreamRepo, downstreamRepo, pluginService, upstreamResolver, tenantDetailsGetter, nil, log, nil, compiler.NewEngine(), nil, nil, nil, config.JobValidationConfig{})
			updatedJobs, err := jobService.Update(ctx, sampleTenant, specs)
			assert.ErrorContains(t, err, "generate upstream error")
			assert.Empty(t, updatedJobs, 0)
		})
		t.Run("should not skip nor return error if job is not bq2bq", func(t *testing.T) {
			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			upstreamRepo := new(UpstreamRepository)
			defer upstreamRepo.AssertExpectations(t)

			downstreamRepo := new(DownstreamRepository)
			defer downstreamRepo.AssertExpectations(t)

			pluginService := NewPluginService(t)

			upstreamResolver := new(UpstreamResolver)
			defer upstreamResolver.AssertExpectations(t)

			tenantDetailsGetter := new(TenantDetailsGetter)
			defer tenantDetailsGetter.AssertExpectations(t)

			jobDeploymentService := new(JobDeploymentService)
			defer jobDeploymentService.AssertExpectations(t)

			eventHandler := newEventHandler(t)

			nonBq2bqTask := job.NewTask("another", nil, "", nil)
			specA, _ := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, jobWindow, nonBq2bqTask).WithAsset(jobAsset).Build()
			specs := []*job.Spec{specA}

			tenantDetailsGetter.On("GetDetails", ctx, sampleTenant).Return(detailedTenant, nil)

			jobA := job.NewJob(sampleTenant, specA, resource.ZeroURN(), nil, false)
			jobs := []*job.Job{jobA}

			jobRepo.On("Update", ctx, mock.Anything).Return(jobs, nil, nil)
			jobRepo.On("GetByJobName", ctx, project.Name(), specA.Name()).Return(jobA, nil)

			pluginService.On("ConstructDestinationURN", ctx, specA.Task().Name().String(), mock.Anything).Return(resource.ZeroURN(), nil)
			pluginService.On("IdentifyUpstreams", ctx, specA.Task().Name().String(), mock.Anything, mock.Anything).Return([]resource.URN{}, nil)
			jobWithUpstream := job.NewWithUpstream(jobA, nil)
			upstreamResolver.On("BulkResolve", ctx, project.Name(), jobs, mock.Anything).Return([]*job.WithUpstream{jobWithUpstream}, nil, nil)

			upstreamRepo.On("ReplaceUpstreams", ctx, []*job.WithUpstream{jobWithUpstream}).Return(nil)

			jobNamesToUpload := []string{jobA.GetName()}
			jobDeploymentService.On("UploadJobs", ctx, sampleTenant, jobNamesToUpload, emptyJobNames).Return(nil)

			alertManager := new(AlertManager)
			defer alertManager.AssertExpectations(t)
			alertManager.On("SendJobEvent", mock.Anything).Return()

			eventHandler.On("HandleEvent", mock.Anything).Times(1)

			jobService := service.NewJobService(jobRepo, upstreamRepo, downstreamRepo, pluginService, upstreamResolver, tenantDetailsGetter, eventHandler, log, jobDeploymentService, compiler.NewEngine(), nil, nil, alertManager, config.JobValidationConfig{})
			updatedJobs, err := jobService.Update(ctx, sampleTenant, specs)
			assert.NoError(t, err)
			assert.Len(t, updatedJobs, 1)
		})
		t.Run("should skip and not return error if one of the job is failed to be updated to db", func(t *testing.T) {
			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			upstreamRepo := new(UpstreamRepository)
			defer upstreamRepo.AssertExpectations(t)

			downstreamRepo := new(DownstreamRepository)
			defer downstreamRepo.AssertExpectations(t)

			pluginService := NewPluginService(t)

			upstreamResolver := new(UpstreamResolver)
			defer upstreamResolver.AssertExpectations(t)

			tenantDetailsGetter := new(TenantDetailsGetter)
			defer tenantDetailsGetter.AssertExpectations(t)

			jobDeploymentService := new(JobDeploymentService)
			defer jobDeploymentService.AssertExpectations(t)

			eventHandler := newEventHandler(t)
			eventHandler.On("HandleEvent", mock.Anything).Times(1)

			specA, _ := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()
			specB, _ := job.NewSpecBuilder(jobVersion, "job-B", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()
			specs := []*job.Spec{specA, specB}

			tenantDetailsGetter.On("GetDetails", ctx, sampleTenant).Return(detailedTenant, nil)

			resourceA := resourceURNA
			var jobDestination resource.URN
			pluginService.On("ConstructDestinationURN", ctx, specA.Task().Name().String(), mock.Anything).Return(resourceA, nil).Once()
			pluginService.On("ConstructDestinationURN", ctx, specB.Task().Name().String(), mock.Anything).Return(jobDestination, errors.New("something error")).Once()

			jobSourcesA := []resource.URN{resourceURNB}
			pluginService.On("IdentifyUpstreams", ctx, specA.Task().Name().String(), mock.Anything, mock.Anything).Return(jobSourcesA, nil)

			jobADestination := resourceURNA
			jobAUpstreamName := []resource.URN{resourceURNB}
			jobA := job.NewJob(sampleTenant, specA, jobADestination, jobAUpstreamName, false)
			jobB := job.NewJob(sampleTenant, specB, resource.ZeroURN(), nil, false)
			savedJobs := []*job.Job{jobB}
			jobRepo.On("Update", ctx, mock.Anything).Return(savedJobs, errors.New("unable to save job A"), nil)
			jobRepo.On("GetByJobName", ctx, project.Name(), specA.Name()).Return(jobA, nil)
			jobRepo.On("GetByJobName", ctx, project.Name(), specB.Name()).Return(jobB, nil)

			downstreamRepo.On("GetDownstreamByDestination", ctx, mock.Anything, mock.Anything).Return([]*job.Downstream{}, nil)

			jobWithUpstreamB := job.NewWithUpstream(jobB, nil)
			upstreamResolver.On("BulkResolve", ctx, project.Name(), savedJobs, mock.Anything).Return([]*job.WithUpstream{jobWithUpstreamB}, nil, nil)

			upstreamRepo.On("ReplaceUpstreams", ctx, mock.Anything).Return(nil)

			jobNamesToUpload := []string{jobB.GetName()}
			jobDeploymentService.On("UploadJobs", ctx, sampleTenant, jobNamesToUpload, emptyJobNames).Return(nil)

			alertManager := new(AlertManager)
			alertManager.On("SendJobEvent", mock.Anything)

			jobService := service.NewJobService(jobRepo, upstreamRepo, downstreamRepo, pluginService, upstreamResolver, tenantDetailsGetter, eventHandler, log, jobDeploymentService, compiler.NewEngine(), nil, nil, alertManager, config.JobValidationConfig{})
			updatedJobs, err := jobService.Update(ctx, sampleTenant, specs)
			assert.ErrorContains(t, err, "unable to save job A")
			assert.Len(t, updatedJobs, 1)
		})
		t.Run("return error when all jobs failed to be updated to db", func(t *testing.T) {
			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			upstreamRepo := new(UpstreamRepository)
			defer upstreamRepo.AssertExpectations(t)

			downstreamRepo := new(DownstreamRepository)
			defer downstreamRepo.AssertExpectations(t)

			pluginService := NewPluginService(t)

			upstreamResolver := new(UpstreamResolver)
			defer upstreamResolver.AssertExpectations(t)

			tenantDetailsGetter := new(TenantDetailsGetter)
			defer tenantDetailsGetter.AssertExpectations(t)

			specA, _ := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()
			specs := []*job.Spec{specA}

			tenantDetailsGetter.On("GetDetails", ctx, sampleTenant).Return(detailedTenant, nil)

			downstreamRepo.On("GetDownstreamByDestination", ctx, mock.Anything, mock.Anything).Return([]*job.Downstream{}, nil)

			upstreamResolver.On("BulkResolve", ctx, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)

			upstreamRepo.On("ReplaceUpstreams", ctx, mock.Anything).Return(nil)

			resourceA := resourceURNA
			pluginService.On("ConstructDestinationURN", ctx, specA.Task().Name().String(), mock.Anything).Return(resourceA, nil).Once()

			jobSourcesA := []resource.URN{resourceURNB}
			pluginService.On("IdentifyUpstreams", ctx, specA.Task().Name().String(), mock.Anything, mock.Anything).Return(jobSourcesA, nil)

			jobADestination := resourceURNA
			jobAUpstreamName := []resource.URN{resourceURNB}
			jobA := job.NewJob(sampleTenant, specA, jobADestination, jobAUpstreamName, false)
			jobRepo.On("Update", ctx, mock.Anything).Return([]*job.Job{}, errors.New("unable to update job A"), errors.New("all jobs failed"))
			jobRepo.On("GetByJobName", ctx, project.Name(), specA.Name()).Return(jobA, nil)

			jobService := service.NewJobService(jobRepo, upstreamRepo, downstreamRepo, pluginService, upstreamResolver, tenantDetailsGetter, nil, log, nil, compiler.NewEngine(), nil, nil, nil, config.JobValidationConfig{})
			updatedJobs, err := jobService.Update(ctx, sampleTenant, specs)
			assert.ErrorContains(t, err, "unable to update job A")
			assert.Empty(t, updatedJobs)
		})
		t.Run("should not return error if updating a job with its downstream in 1 time", func(t *testing.T) {
			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			upstreamRepo := new(UpstreamRepository)
			defer upstreamRepo.AssertExpectations(t)

			downstreamRepo := new(DownstreamRepository)
			defer downstreamRepo.AssertExpectations(t)

			pluginService := NewPluginService(t)

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
			specs := []*job.Spec{specA, specB}

			tenantDetailsGetter.On("GetDetails", ctx, sampleTenant).Return(detailedTenant, nil)

			jobADestination := resourceURNA
			jobBDestination := resourceURNB
			pluginService.On("ConstructDestinationURN", ctx, specA.Task().Name().String(), mock.Anything).Return(jobADestination, nil)

			jobAUpstreamName := []resource.URN{}
			jobBUpstreamName := []resource.URN{resourceURNA}
			jobCUpstreamName := []resource.URN{resourceURNA}
			pluginService.On("IdentifyUpstreams", ctx, specA.Task().Name().String(), mock.Anything, mock.Anything).Return(jobAUpstreamName, nil)

			jobA := job.NewJob(sampleTenant, specA, jobADestination, jobAUpstreamName, false)
			jobB := job.NewJob(sampleTenant, specB, jobBDestination, jobBUpstreamName, false)
			jobC := job.NewJob(sampleTenant, specC, jobBDestination, jobCUpstreamName, false)
			jobs := []*job.Job{jobA, jobB}
			jobRepo.On("GetByJobName", ctx, project.Name(), specA.Name()).Return(jobA, nil)
			jobRepo.On("GetByJobName", ctx, project.Name(), specB.Name()).Return(jobB, nil)

			jobBDownstream := job.NewDownstream("job-B", sampleTenant.ProjectName(), sampleTenant.NamespaceName(), jobTask.Name())
			downstreamRepo.On("GetDownstreamByDestination", ctx, mock.Anything, jobADestination).Return([]*job.Downstream{jobBDownstream}, nil).Once()
			jobRepo.On("GetByJobName", ctx, mock.Anything, jobBDownstream.Name()).Return(jobB, nil)

			jobRepo.On("Update", ctx, mock.Anything).Return(jobs, nil, nil)

			jobCDownstream := job.NewDownstream("job-C", sampleTenant.ProjectName(), sampleTenant.NamespaceName(), jobTask.Name())
			downstreamRepo.On("GetDownstreamByDestination", ctx, mock.Anything, jobADestination).Return([]*job.Downstream{jobCDownstream}, nil).Once()
			jobRepo.On("GetByJobName", ctx, mock.Anything, jobCDownstream.Name()).Return(jobC, nil)

			downstreamRepo.On("GetDownstreamByDestination", ctx, mock.Anything, jobBDestination).Return([]*job.Downstream{}, nil).Once()
			downstreamRepo.On("GetDownstreamByDestination", ctx, mock.Anything, jobBDestination).Return([]*job.Downstream{}, nil).Once()

			upstream := job.NewUpstreamResolved("job-A", "", resourceURNA, sampleTenant, "static", taskName, false)
			jobAWithUpstream := job.NewWithUpstream(jobA, []*job.Upstream{})
			jobBWithUpstream := job.NewWithUpstream(jobB, []*job.Upstream{})
			jobCWithUpstream := job.NewWithUpstream(jobC, []*job.Upstream{upstream})
			upstreamResolver.On("BulkResolve", ctx, project.Name(), mock.MatchedBy(func(elems []*job.Job) bool {
				return assert.ElementsMatch(t, elems, append(jobs, jobC))
			}), mock.Anything).Return([]*job.WithUpstream{jobAWithUpstream, jobBWithUpstream, jobCWithUpstream}, nil, nil)

			upstreamRepo.On("ReplaceUpstreams", ctx, []*job.WithUpstream{jobAWithUpstream, jobBWithUpstream, jobCWithUpstream}).Return(nil)

			jobNamesToUpload := []string{jobA.GetName(), jobB.GetName(), jobC.GetName()}
			jobDeploymentService.On("UploadJobs", ctx, sampleTenant, mock.MatchedBy(func(elems []string) bool {
				return assert.ElementsMatch(t, elems, jobNamesToUpload)
			}), emptyJobNames).Return(nil)

			eventHandler.On("HandleEvent", mock.Anything).Times(2)
			alertManager := new(AlertManager)
			alertManager.On("SendJobEvent", mock.Anything).Return()
			defer alertManager.AssertExpectations(t)

			jobService := service.NewJobService(jobRepo, upstreamRepo, downstreamRepo, pluginService, upstreamResolver, tenantDetailsGetter, eventHandler, log, jobDeploymentService, compiler.NewEngine(), nil, nil, alertManager, config.JobValidationConfig{})
			updateJobs, err := jobService.Update(ctx, sampleTenant, specs)
			assert.NoError(t, err)
			assert.Len(t, updateJobs, 2)
		})
		t.Run("should return error if failed to save upstream", func(t *testing.T) {
			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			upstreamRepo := new(UpstreamRepository)
			defer upstreamRepo.AssertExpectations(t)

			downstreamRepo := new(DownstreamRepository)
			defer downstreamRepo.AssertExpectations(t)

			pluginService := NewPluginService(t)

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

			resourceA := resourceURNA
			pluginService.On("ConstructDestinationURN", ctx, specA.Task().Name().String(), mock.Anything).Return(resourceA, nil).Once()

			jobSourcesA := []resource.URN{resourceURNB}
			pluginService.On("IdentifyUpstreams", ctx, specA.Task().Name().String(), mock.Anything, mock.Anything).Return(jobSourcesA, nil)

			jobA := job.NewJob(sampleTenant, specA, resourceA, jobSourcesA, false)
			jobs := []*job.Job{jobA}
			jobRepo.On("Update", ctx, mock.Anything).Return(jobs, nil, nil)
			jobRepo.On("GetByJobName", ctx, project.Name(), specA.Name()).Return(jobA, nil)

			downstreamRepo.On("GetDownstreamByDestination", ctx, mock.Anything, mock.Anything).Return([]*job.Downstream{}, nil)

			jobWithUpstreamA := job.NewWithUpstream(jobA, nil)
			upstreamResolver.On("BulkResolve", ctx, project.Name(), jobs, mock.Anything).Return([]*job.WithUpstream{jobWithUpstreamA}, nil, nil)

			upstreamRepo.On("ReplaceUpstreams", ctx, mock.Anything).Return(errors.New("internal error"))

			jobNamesToUpload := []string{jobA.GetName()}
			jobDeploymentService.On("UploadJobs", ctx, sampleTenant, jobNamesToUpload, emptyJobNames).Return(nil)

			eventHandler.On("HandleEvent", mock.Anything).Times(1)
			alertManager := new(AlertManager)
			alertManager.On("SendJobEvent", mock.Anything)
			jobService := service.NewJobService(jobRepo, upstreamRepo, downstreamRepo, pluginService, upstreamResolver, tenantDetailsGetter, eventHandler, log, jobDeploymentService, compiler.NewEngine(), nil, nil, alertManager, config.JobValidationConfig{})
			updatedJobs, err := jobService.Update(ctx, sampleTenant, specs)
			assert.Error(t, err)
			assert.Len(t, updatedJobs, 1)
		})
		t.Run("return error if encounter issue when uploading jobs", func(t *testing.T) {
			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			upstreamRepo := new(UpstreamRepository)
			defer upstreamRepo.AssertExpectations(t)

			downstreamRepo := new(DownstreamRepository)
			defer downstreamRepo.AssertExpectations(t)

			pluginService := NewPluginService(t)

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

			jobADestination := resourceURNA
			pluginService.On("ConstructDestinationURN", ctx, specA.Task().Name().String(), mock.Anything).Return(jobADestination, nil)

			jobAUpstreamName := []resource.URN{resourceURNB}
			pluginService.On("IdentifyUpstreams", ctx, specA.Task().Name().String(), mock.Anything, mock.Anything).Return(jobAUpstreamName, nil)

			jobA := job.NewJob(sampleTenant, specA, jobADestination, jobAUpstreamName, false)
			jobs := []*job.Job{jobA}
			jobRepo.On("Update", ctx, mock.Anything).Return(jobs, nil, nil)
			jobRepo.On("GetByJobName", ctx, project.Name(), specA.Name()).Return(jobA, nil)

			downstreamRepo.On("GetDownstreamByDestination", ctx, mock.Anything, mock.Anything).Return([]*job.Downstream{}, nil)

			upstream := job.NewUpstreamResolved("job-B", "", resourceURNB, sampleTenant, "static", taskName, false)
			jobWithUpstream := job.NewWithUpstream(jobA, []*job.Upstream{upstream})
			upstreamResolver.On("BulkResolve", ctx, project.Name(), jobs, mock.Anything).Return([]*job.WithUpstream{jobWithUpstream}, nil, nil)

			upstreamRepo.On("ReplaceUpstreams", ctx, []*job.WithUpstream{jobWithUpstream}).Return(nil)

			errorMsg := "internal error"
			jobDeploymentService.On("UploadJobs", ctx, sampleTenant, mock.Anything, emptyJobNames).Return(errors.New(errorMsg))

			eventHandler.On("HandleEvent", mock.Anything).Times(1)
			alertManager := new(AlertManager)
			alertManager.On("SendJobEvent", mock.Anything)
			jobService := service.NewJobService(jobRepo, upstreamRepo, downstreamRepo, pluginService, upstreamResolver, tenantDetailsGetter, eventHandler, log, jobDeploymentService, compiler.NewEngine(), nil, nil, alertManager, config.JobValidationConfig{})
			updatedJobs, err := jobService.Update(ctx, sampleTenant, specs)
			assert.ErrorContains(t, err, errorMsg)
			assert.Len(t, updatedJobs, 1)
		})
	})

	t.Run("Upsert", func(t *testing.T) {
		notFoundErr := optErrors.NewError(optErrors.ErrNotFound, job.EntityJob, "not found")
		t.Run("update and insert jobs", func(t *testing.T) {
			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			upstreamRepo := new(UpstreamRepository)
			defer upstreamRepo.AssertExpectations(t)

			downstreamRepo := new(DownstreamRepository)
			defer downstreamRepo.AssertExpectations(t)

			pluginService := NewPluginService(t)

			upstreamResolver := new(UpstreamResolver)
			defer upstreamResolver.AssertExpectations(t)

			tenantDetailsGetter := new(TenantDetailsGetter)
			defer tenantDetailsGetter.AssertExpectations(t)

			jobDeploymentService := new(JobDeploymentService)
			defer jobDeploymentService.AssertExpectations(t)

			eventHandler := newEventHandler(t)

			specAToUpdate, _ := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()
			specAExisting, _ := job.NewSpecBuilder(jobVersion, "job-A", "legacy-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()
			specBToAdd, _ := job.NewSpecBuilder(jobVersion, "job-B", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()
			specs := []*job.Spec{specAToUpdate, specBToAdd}

			tenantDetailsGetter.On("GetDetails", ctx, sampleTenant).Return(detailedTenant, nil)

			jobADestination := resourceURNA
			pluginService.On("ConstructDestinationURN", ctx, specAToUpdate.Task().Name().String(), mock.Anything).Return(jobADestination, nil)

			jobAUpstreamName := []resource.URN{resourceURNB}
			pluginService.On("IdentifyUpstreams", ctx, specAToUpdate.Task().Name().String(), mock.Anything, mock.Anything).Return(jobAUpstreamName, nil)

			jobAToUpdate := job.NewJob(sampleTenant, specAToUpdate, jobADestination, jobAUpstreamName, false)
			jobAExisting := job.NewJob(sampleTenant, specAExisting, jobADestination, jobAUpstreamName, false)

			jobBDestination := resourceURNB
			pluginService.On("ConstructDestinationURN", ctx, specBToAdd.Task().Name().String(), mock.Anything).Return(jobBDestination, nil)

			jobBUpstreamName := []resource.URN{resourceURNC}
			pluginService.On("IdentifyUpstreams", ctx, specAToUpdate.Task().Name().String(), mock.Anything, mock.Anything).Return(jobBUpstreamName, nil)

			jobBToadd := job.NewJob(sampleTenant, specBToAdd, jobBDestination, jobBUpstreamName, false)

			jobsToUpdate := []*job.Job{jobAToUpdate}
			jobRepo.On("Update", ctx, mock.Anything).Return(jobsToUpdate, nil, nil)

			jobsToAdd := []*job.Job{jobBToadd}
			jobRepo.On("Add", ctx, mock.Anything).Return(jobsToAdd, nil, nil)

			jobRepo.On("GetByJobName", ctx, project.Name(), specAToUpdate.Name()).Return(jobAExisting, nil).Once()
			jobRepo.On("GetByJobName", ctx, project.Name(), specBToAdd.Name()).Return(nil, notFoundErr).Once()

			upstreamB := job.NewUpstreamResolved("job-B", "", resourceURNB, sampleTenant, "static", taskName, false)
			jobAWithUpstream := job.NewWithUpstream(jobAToUpdate, []*job.Upstream{upstreamB})

			upstreamC := job.NewUpstreamResolved("job-C", "", resourceURNC, sampleTenant, "static", taskName, false)
			jobBWithUpstream := job.NewWithUpstream(jobBToadd, []*job.Upstream{upstreamC})

			downstreamRepo.On("GetDownstreamByDestination", ctx, mock.Anything, mock.Anything).Return([]*job.Downstream{}, nil)

			jobsToUpsert := []*job.Job{jobBToadd, jobAToUpdate}
			upstreamResolver.On("BulkResolve", ctx, project.Name(), mock.MatchedBy(func(elems []*job.Job) bool {
				return assert.ElementsMatch(t, elems, jobsToUpsert)
			}), mock.Anything).Return([]*job.WithUpstream{jobAWithUpstream, jobBWithUpstream}, nil, nil)

			upstreamRepo.On("ReplaceUpstreams", ctx, []*job.WithUpstream{jobAWithUpstream, jobBWithUpstream}).Return(nil)

			jobNamesToUpload := []string{jobBToadd.GetName(), jobAToUpdate.GetName()}
			jobDeploymentService.On("UploadJobs", ctx, sampleTenant, mock.MatchedBy(func(elems []string) bool {
				return assert.ElementsMatch(t, elems, jobNamesToUpload)
			}), emptyJobNames).Return(nil)

			eventHandler.On("HandleEvent", mock.Anything).Times(2)

			alertManager := new(AlertManager)
			alertManager.On("SendJobEvent", mock.Anything).Return()
			defer alertManager.AssertExpectations(t)

			expected := []dto.UpsertResult{
				{JobName: jobBToadd.Spec().Name(), Status: job.DeployStateSuccess},
				{JobName: jobAToUpdate.Spec().Name(), Status: job.DeployStateSuccess},
			}
			jobService := service.NewJobService(jobRepo, upstreamRepo, downstreamRepo, pluginService, upstreamResolver, tenantDetailsGetter, eventHandler, log, jobDeploymentService, compiler.NewEngine(), nil, nil, alertManager, config.JobValidationConfig{})
			actual, err := jobService.Upsert(ctx, sampleTenant, specs)
			assert.NoError(t, err)
			assert.ElementsMatch(t, expected, actual)
		})
		t.Run("return error if unable to get detailed tenant", func(t *testing.T) {
			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			upstreamRepo := new(UpstreamRepository)
			defer upstreamRepo.AssertExpectations(t)

			downstreamRepo := new(DownstreamRepository)
			defer downstreamRepo.AssertExpectations(t)

			pluginService := NewPluginService(t)

			upstreamResolver := new(UpstreamResolver)
			defer upstreamResolver.AssertExpectations(t)

			tenantDetailsGetter := new(TenantDetailsGetter)
			defer tenantDetailsGetter.AssertExpectations(t)

			specA, _ := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()
			specs := []*job.Spec{specA}

			tenantDetailsGetter.On("GetDetails", ctx, sampleTenant).Return(&tenant.WithDetails{}, errors.New("internal error"))

			jobService := service.NewJobService(jobRepo, upstreamRepo, downstreamRepo, pluginService, upstreamResolver, tenantDetailsGetter, nil, log, nil, compiler.NewEngine(), nil, nil, nil, config.JobValidationConfig{})
			actual, err := jobService.Upsert(ctx, sampleTenant, specs)
			assert.ErrorContains(t, err, "internal error")
			assert.Equal(t, 0, len(actual))
		})
		t.Run("return error if unable to get generate the upstream", func(t *testing.T) {
			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			upstreamRepo := new(UpstreamRepository)
			defer upstreamRepo.AssertExpectations(t)

			downstreamRepo := new(DownstreamRepository)
			defer downstreamRepo.AssertExpectations(t)

			pluginService := NewPluginService(t)

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

			jobRepo.On("GetByJobName", ctx, project.Name(), specA.Name()).Return(nil, notFoundErr).Once()

			var specADestination resource.URN
			pluginService.On("ConstructDestinationURN", ctx, specA.Task().Name().String(), mock.Anything).Return(specADestination, errors.New("unable to generate destination")).Once()

			expected := []dto.UpsertResult{
				{JobName: specA.Name(), Status: job.DeployStateFailed},
			}

			jobService := service.NewJobService(jobRepo, upstreamRepo, downstreamRepo, pluginService, upstreamResolver, tenantDetailsGetter, eventHandler, log, jobDeploymentService, compiler.NewEngine(), nil, nil, nil, config.JobValidationConfig{})
			actual, err := jobService.Upsert(ctx, sampleTenant, specs)
			assert.ErrorContains(t, err, "unable to generate destination")
			assert.ElementsMatch(t, expected, actual)
		})
		t.Run("should not skip nor return error if job is not bq2bq", func(t *testing.T) {
			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			upstreamRepo := new(UpstreamRepository)
			defer upstreamRepo.AssertExpectations(t)

			downstreamRepo := new(DownstreamRepository)
			defer downstreamRepo.AssertExpectations(t)

			pluginService := NewPluginService(t)

			upstreamResolver := new(UpstreamResolver)
			defer upstreamResolver.AssertExpectations(t)

			tenantDetailsGetter := new(TenantDetailsGetter)
			defer tenantDetailsGetter.AssertExpectations(t)

			jobDeploymentService := new(JobDeploymentService)
			defer jobDeploymentService.AssertExpectations(t)

			eventHandler := newEventHandler(t)

			nonBq2bqTask := job.NewTask("another", nil, "", nil)
			specA, _ := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, jobWindow, nonBq2bqTask).WithAsset(jobAsset).Build()
			specs := []*job.Spec{specA}

			tenantDetailsGetter.On("GetDetails", ctx, sampleTenant).Return(detailedTenant, nil)

			jobA := job.NewJob(sampleTenant, specA, resource.ZeroURN(), nil, false)
			jobs := []*job.Job{jobA}

			jobRepo.On("Add", ctx, mock.Anything).Return(jobs, nil)
			jobRepo.On("GetByJobName", ctx, project.Name(), specA.Name()).Return(nil, notFoundErr)

			pluginService.On("ConstructDestinationURN", ctx, specA.Task().Name().String(), mock.Anything).Return(resource.ZeroURN(), nil)
			pluginService.On("IdentifyUpstreams", ctx, specA.Task().Name().String(), mock.Anything, mock.Anything).Return([]resource.URN{}, nil)
			jobWithUpstream := job.NewWithUpstream(jobA, nil)
			upstreamResolver.On("BulkResolve", ctx, project.Name(), jobs, mock.Anything).Return([]*job.WithUpstream{jobWithUpstream}, nil, nil)

			upstreamRepo.On("ReplaceUpstreams", ctx, []*job.WithUpstream{jobWithUpstream}).Return(nil)

			jobNamesToUpload := []string{jobA.GetName()}
			jobDeploymentService.On("UploadJobs", ctx, sampleTenant, jobNamesToUpload, emptyJobNames).Return(nil)

			eventHandler.On("HandleEvent", mock.Anything).Times(1)

			expected := []dto.UpsertResult{
				{JobName: specA.Name(), Status: job.DeployStateSuccess},
			}

			jobService := service.NewJobService(jobRepo, upstreamRepo, downstreamRepo, pluginService, upstreamResolver, tenantDetailsGetter, eventHandler, log, jobDeploymentService, compiler.NewEngine(), nil, nil, nil, config.JobValidationConfig{})
			actual, err := jobService.Upsert(ctx, sampleTenant, specs)
			assert.NoError(t, err)
			assert.ElementsMatch(t, expected, actual)
		})
		t.Run("skip job that has issue when checking its existence and return error", func(t *testing.T) {
			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			upstreamRepo := new(UpstreamRepository)
			defer upstreamRepo.AssertExpectations(t)

			downstreamRepo := new(DownstreamRepository)
			defer downstreamRepo.AssertExpectations(t)

			pluginService := NewPluginService(t)

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

			jobADestination := resourceURNA
			jobBDestination := resourceURNB
			jobCDestination := resourceURNC
			pluginService.On("ConstructDestinationURN", ctx, specA.Task().Name().String(), mock.Anything).Return(jobADestination, nil).Once()
			pluginService.On("ConstructDestinationURN", ctx, specB.Task().Name().String(), mock.Anything).Return(jobBDestination, nil).Once()
			pluginService.On("ConstructDestinationURN", ctx, specC.Task().Name().String(), mock.Anything).Return(jobCDestination, nil).Once()

			jobAUpstreamName := []resource.URN{resourceURNB}
			pluginService.On("IdentifyUpstreams", ctx, specC.Task().Name().String(), mock.Anything, mock.Anything).Return([]resource.URN{}, nil)
			pluginService.On("IdentifyUpstreams", ctx, specB.Task().Name().String(), mock.Anything, mock.Anything).Return([]resource.URN{}, nil)
			pluginService.On("IdentifyUpstreams", ctx, specA.Task().Name().String(), mock.Anything, mock.Anything).Return(jobAUpstreamName, nil)

			jobA := job.NewJob(sampleTenant, specA, jobADestination, jobAUpstreamName, false)

			jobRepo.On("GetByJobName", ctx, project.Name(), specA.Name()).Return(nil, notFoundErr)
			jobRepo.On("GetByJobName", ctx, project.Name(), specB.Name()).Return(nil, errors.New("internal error"))
			jobRepo.On("GetByJobName", ctx, project.Name(), specC.Name()).Return(nil, errors.New("internal error"))

			downstreamRepo.On("GetDownstreamByDestination", ctx, mock.Anything, mock.Anything).Return([]*job.Downstream{}, nil)

			jobs := []*job.Job{jobA}
			jobRepo.On("Add", ctx, mock.Anything).Return(jobs, nil)

			upstream := job.NewUpstreamResolved("job-B", "", resourceURNB, sampleTenant, "static", taskName, false)
			jobWithUpstream := job.NewWithUpstream(jobA, []*job.Upstream{upstream})
			upstreamResolver.On("BulkResolve", ctx, project.Name(), jobs, mock.Anything).Return([]*job.WithUpstream{jobWithUpstream}, nil, nil)

			upstreamRepo.On("ReplaceUpstreams", ctx, []*job.WithUpstream{jobWithUpstream}).Return(nil)

			jobNamesToUpload := []string{jobA.GetName()}
			jobDeploymentService.On("UploadJobs", ctx, sampleTenant, jobNamesToUpload, emptyJobNames).Return(nil)

			eventHandler.On("HandleEvent", mock.Anything).Times(1)

			expected := []dto.UpsertResult{
				{JobName: specA.Name(), Status: job.DeployStateSuccess},
				{JobName: specB.Name(), Status: job.DeployStateFailed},
				{JobName: specC.Name(), Status: job.DeployStateFailed},
			}

			jobService := service.NewJobService(jobRepo, upstreamRepo, downstreamRepo, pluginService, upstreamResolver, tenantDetailsGetter, eventHandler, log, jobDeploymentService, compiler.NewEngine(), nil, nil, nil, config.JobValidationConfig{})
			actual, err := jobService.Upsert(ctx, sampleTenant, specs)
			assert.ErrorContains(t, err, "internal error")
			assert.ElementsMatch(t, expected, actual)
		})
		t.Run("update only modified jobs", func(t *testing.T) {
			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			upstreamRepo := new(UpstreamRepository)
			defer upstreamRepo.AssertExpectations(t)

			downstreamRepo := new(DownstreamRepository)
			defer downstreamRepo.AssertExpectations(t)

			pluginService := NewPluginService(t)

			upstreamResolver := new(UpstreamResolver)
			defer upstreamResolver.AssertExpectations(t)

			tenantDetailsGetter := new(TenantDetailsGetter)
			defer tenantDetailsGetter.AssertExpectations(t)

			jobDeploymentService := new(JobDeploymentService)
			defer jobDeploymentService.AssertExpectations(t)

			eventHandler := newEventHandler(t)

			specAToUpdate, _ := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()
			specAExisting, _ := job.NewSpecBuilder(jobVersion, "job-A", "legacy-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()
			specBToUpdate, _ := job.NewSpecBuilder(jobVersion, "job-B", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()
			specs := []*job.Spec{specAToUpdate, specBToUpdate}

			tenantDetailsGetter.On("GetDetails", ctx, sampleTenant).Return(detailedTenant, nil)

			jobADestination := resourceURNA
			pluginService.On("ConstructDestinationURN", ctx, specAToUpdate.Task().Name().String(), mock.Anything).Return(jobADestination, nil)

			jobAUpstreamName := []resource.URN{resourceURNB}
			pluginService.On("IdentifyUpstreams", ctx, specAToUpdate.Task().Name().String(), mock.Anything, mock.Anything).Return(jobAUpstreamName, nil)

			jobAToUpdate := job.NewJob(sampleTenant, specAToUpdate, jobADestination, jobAUpstreamName, false)
			jobAExisting := job.NewJob(sampleTenant, specAExisting, jobADestination, jobAUpstreamName, false)

			jobBDestination := resourceURNB
			pluginService.On("ConstructDestinationURN", ctx, specBToUpdate.Task().Name().String(), mock.Anything).Return(jobBDestination, nil)

			jobBUpstreamName := []resource.URN{resourceURNC}
			pluginService.On("IdentifyUpstreams", ctx, specAToUpdate.Task().Name().String(), mock.Anything, mock.Anything).Return(jobBUpstreamName, nil)

			jobBToUpdate := job.NewJob(sampleTenant, specBToUpdate, jobBDestination, jobBUpstreamName, false)

			jobsToUpdate := []*job.Job{jobAToUpdate}
			jobRepo.On("Update", ctx, mock.Anything).Return(jobsToUpdate, nil, nil)

			jobRepo.On("GetByJobName", ctx, project.Name(), specAToUpdate.Name()).Return(jobAExisting, nil).Once()
			jobRepo.On("GetByJobName", ctx, project.Name(), specBToUpdate.Name()).Return(jobBToUpdate, nil).Once()

			downstreamRepo.On("GetDownstreamByDestination", ctx, mock.Anything, mock.Anything).Return([]*job.Downstream{}, nil)

			upstreamB := job.NewUpstreamResolved("job-B", "", resourceURNB, sampleTenant, "static", taskName, false)
			jobAWithUpstream := job.NewWithUpstream(jobAToUpdate, []*job.Upstream{upstreamB})

			jobsToUpsert := []*job.Job{jobAToUpdate}
			upstreamResolver.On("BulkResolve", ctx, project.Name(), jobsToUpsert, mock.Anything).Return([]*job.WithUpstream{jobAWithUpstream}, nil, nil)

			upstreamRepo.On("ReplaceUpstreams", ctx, []*job.WithUpstream{jobAWithUpstream}).Return(nil)

			jobNamesToUpload := []string{jobAToUpdate.GetName()}
			jobDeploymentService.On("UploadJobs", ctx, sampleTenant, jobNamesToUpload, emptyJobNames).Return(nil)

			eventHandler.On("HandleEvent", mock.Anything).Times(1)
			alertManager := new(AlertManager)
			alertManager.On("SendJobEvent", mock.Anything).Return()
			defer alertManager.AssertExpectations(t)

			expected := []dto.UpsertResult{
				{JobName: specAToUpdate.Name(), Status: job.DeployStateSuccess},
				{JobName: specBToUpdate.Name(), Status: job.DeployStateSkipped},
			}

			jobService := service.NewJobService(jobRepo, upstreamRepo, downstreamRepo, pluginService, upstreamResolver, tenantDetailsGetter, eventHandler, log, jobDeploymentService, compiler.NewEngine(), nil, nil, alertManager, config.JobValidationConfig{})
			actual, err := jobService.Upsert(ctx, sampleTenant, specs)
			assert.NoError(t, err)
			assert.ElementsMatch(t, expected, actual)
		})
		t.Run("should not return error if updating the job and its downstream at 1 time", func(t *testing.T) {
			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			upstreamRepo := new(UpstreamRepository)
			defer upstreamRepo.AssertExpectations(t)

			downstreamRepo := new(DownstreamRepository)
			defer downstreamRepo.AssertExpectations(t)

			pluginService := NewPluginService(t)

			upstreamResolver := new(UpstreamResolver)
			defer upstreamResolver.AssertExpectations(t)

			tenantDetailsGetter := new(TenantDetailsGetter)
			defer tenantDetailsGetter.AssertExpectations(t)

			jobDeploymentService := new(JobDeploymentService)
			defer jobDeploymentService.AssertExpectations(t)

			eventHandler := newEventHandler(t)

			specAToUpdate, _ := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()
			specAExisting, _ := job.NewSpecBuilder(jobVersion, "job-A", "legacy-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()

			specBToUpdate, _ := job.NewSpecBuilder(jobVersion, "job-B", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()
			specBExisting, _ := job.NewSpecBuilder(jobVersion, "job-B", "legacy-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()

			specs := []*job.Spec{specAToUpdate, specBToUpdate}

			tenantDetailsGetter.On("GetDetails", ctx, sampleTenant).Return(detailedTenant, nil)

			jobADestination := resourceURNA
			pluginService.On("ConstructDestinationURN", ctx, specAToUpdate.Task().Name().String(), mock.Anything).Return(jobADestination, nil)
			jobBDestination := resourceURNB
			pluginService.On("ConstructDestinationURN", ctx, specBToUpdate.Task().Name().String(), mock.Anything).Return(jobBDestination, nil)

			jobAUpstreamName := []resource.URN{resourceURNB}
			pluginService.On("IdentifyUpstreams", ctx, specAToUpdate.Task().Name().String(), mock.Anything, mock.Anything).Return(jobAUpstreamName, nil)
			jobBUpstreamName := []resource.URN{}
			pluginService.On("IdentifyUpstreams", ctx, specBToUpdate.Task().Name().String(), mock.Anything, mock.Anything).Return(jobBUpstreamName, nil)

			jobAToUpdate := job.NewJob(sampleTenant, specAToUpdate, jobADestination, jobAUpstreamName, false)
			jobAExisting := job.NewJob(sampleTenant, specAExisting, jobADestination, jobAUpstreamName, false)

			jobBToUpdate := job.NewJob(sampleTenant, specBToUpdate, jobBDestination, jobBUpstreamName, false)
			jobBExisting := job.NewJob(sampleTenant, specBExisting, jobBDestination, jobBUpstreamName, false)

			jobsToUpdate := []*job.Job{jobAToUpdate, jobBToUpdate}
			jobRepo.On("Update", ctx, mock.Anything).Return(jobsToUpdate, nil, nil)

			jobRepo.On("GetByJobName", ctx, project.Name(), specAToUpdate.Name()).Return(jobAExisting, nil).Once()
			jobRepo.On("GetByJobName", ctx, project.Name(), specBToUpdate.Name()).Return(jobBExisting, nil).Once()

			upstreamB := job.NewUpstreamResolved("job-B", "", resourceURNB, sampleTenant, "static", taskName, false)
			jobAWithUpstream := job.NewWithUpstream(jobAToUpdate, []*job.Upstream{upstreamB})

			jobBWithUpstream := job.NewWithUpstream(jobBToUpdate, []*job.Upstream{})

			downstreamRepo.On("GetDownstreamByDestination", ctx, mock.Anything, mock.Anything).Return([]*job.Downstream{}, nil)

			jobsToUpsert := []*job.Job{jobBToUpdate, jobAToUpdate}
			upstreamResolver.On("BulkResolve", ctx, project.Name(), mock.MatchedBy(func(elems []*job.Job) bool {
				return assert.ElementsMatch(t, elems, jobsToUpsert)
			}), mock.Anything).Return([]*job.WithUpstream{jobAWithUpstream, jobBWithUpstream}, nil, nil)

			upstreamRepo.On("ReplaceUpstreams", ctx, []*job.WithUpstream{jobAWithUpstream, jobBWithUpstream}).Return(nil)

			jobNamesToUpload := []string{jobBToUpdate.GetName(), jobAToUpdate.GetName()}
			jobDeploymentService.On("UploadJobs", ctx, sampleTenant, mock.MatchedBy(func(elems []string) bool {
				return assert.ElementsMatch(t, elems, jobNamesToUpload)
			}), emptyJobNames).Return(nil)

			eventHandler.On("HandleEvent", mock.Anything).Times(2)
			alertManager := new(AlertManager)
			alertManager.On("SendJobEvent", mock.Anything).Return()
			defer alertManager.AssertExpectations(t)

			expected := []dto.UpsertResult{
				{JobName: jobBToUpdate.Spec().Name(), Status: job.DeployStateSuccess},
				{JobName: jobAToUpdate.Spec().Name(), Status: job.DeployStateSuccess},
			}
			jobService := service.NewJobService(jobRepo, upstreamRepo, downstreamRepo, pluginService, upstreamResolver, tenantDetailsGetter, eventHandler, log, jobDeploymentService, compiler.NewEngine(), nil, nil, alertManager, config.JobValidationConfig{})
			actual, err := jobService.Upsert(ctx, sampleTenant, specs)
			assert.NoError(t, err)
			assert.ElementsMatch(t, expected, actual)
		})
	})

	t.Run("ChangeNamespace", func(t *testing.T) {
		newNamespaceName := "newNamespace"
		newTenant, _ := tenant.NewTenant(project.Name().String(), newNamespaceName)
		specA, _ := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()
		jobA := job.NewJob(newTenant, specA, resourceURNA, []resource.URN{resourceURNB}, false)

		t.Run("should fail if error in repo", func(t *testing.T) {
			jobRepo := new(JobRepository)
			jobRepo.On("ChangeJobNamespace", ctx, specA.Name(), sampleTenant, newTenant).Return(errors.New("error in transaction"))
			defer jobRepo.AssertExpectations(t)

			jobService := service.NewJobService(jobRepo, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, config.JobValidationConfig{})
			err := jobService.ChangeNamespace(ctx, sampleTenant, newTenant, specA.Name())
			assert.ErrorContains(t, err, "error in transaction")
		})

		t.Run("should fail if error getting newly created job", func(t *testing.T) {
			jobRepo := new(JobRepository)
			jobRepo.On("ChangeJobNamespace", ctx, specA.Name(), sampleTenant, newTenant).Return(nil)
			jobRepo.On("GetByJobName", ctx, project.Name(), specA.Name()).Return(nil, errors.New("error in fetching job from DB"))
			defer jobRepo.AssertExpectations(t)

			jobService := service.NewJobService(jobRepo, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, config.JobValidationConfig{})
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

			jobService := service.NewJobService(jobRepo, nil, nil, nil, nil, nil, nil, nil, jobDeploymentService, nil, nil, nil, nil, config.JobValidationConfig{})
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

			jobService := service.NewJobService(jobRepo, nil, nil, nil, nil, nil, nil, nil, jobDeploymentService, nil, nil, nil, nil, config.JobValidationConfig{})

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

			alertManager := new(AlertManager)
			alertManager.On("SendJobEvent", mock.Anything)
			jobService := service.NewJobService(jobRepo, nil, nil, nil, nil, nil, eventHandler, nil, jobDeploymentService, nil, nil, nil, alertManager, config.JobValidationConfig{})
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

			alertManager := new(AlertManager)
			alertManager.On("SendJobEvent", mock.Anything)

			jobService := service.NewJobService(jobRepo, upstreamRepo, downstreamRepo, nil, nil, nil, eventHandler, log, jobDeploymentService, nil, nil, nil, alertManager, config.JobValidationConfig{})
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

			upstreamResolver := new(UpstreamResolver)
			defer upstreamResolver.AssertExpectations(t)

			eventHandler := newEventHandler(t)

			specA, _ := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()
			specB, _ := job.NewSpecBuilder(jobVersion, "job-B", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()
			specC, _ := job.NewSpecBuilder(jobVersion, "job-C", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()

			jobBDestination := resourceURNB
			jobBUpstreamName := []resource.URN{resourceURNA}
			jobB := job.NewJob(sampleTenant, specB, jobBDestination, jobBUpstreamName, false)

			jobCDestination := resourceURNB
			jobCUpstreamName := []resource.URN{resourceURNA}
			jobC := job.NewJob(otherTenant, specC, jobCDestination, jobCUpstreamName, false)

			downstreamFullNames := []job.FullName{"test-proj/job-B", "test-proj/job-C"}
			downstreamList := []*job.Downstream{
				job.NewDownstream("job-B", project.Name(), namespace.Name(), taskName),
				job.NewDownstream("job-C", project.Name(), otherNamespace.Name(), taskName),
			}

			downstreamRepo.On("GetDownstreamByJobName", ctx, project.Name(), specA.Name()).Return(downstreamList, nil)
			jobRepo.On("Delete", ctx, project.Name(), specA.Name(), false).Return(nil)

			// if there are downstreams
			jobRepo.On("GetByJobName", ctx, project.Name(), jobB.Spec().Name()).Return(jobB, nil)
			jobRepo.On("GetByJobName", ctx, project.Name(), jobC.Spec().Name()).Return(jobC, nil)

			jobBWithUpstream := job.NewWithUpstream(jobB, []*job.Upstream{})
			jobCWithUpstream := job.NewWithUpstream(jobC, []*job.Upstream{})

			upstreamResolver.On("BulkResolve", ctx, project.Name(), []*job.Job{jobB, jobC}, mock.Anything).Return([]*job.WithUpstream{jobBWithUpstream, jobCWithUpstream}, nil, nil)
			upstreamRepo.On("ReplaceUpstreams", ctx, []*job.WithUpstream{jobBWithUpstream, jobCWithUpstream}).Return(nil)

			// simulate deletion on other tenant as well
			jobNamesToRemove := []string{specA.Name().String()}
			downstreamBUpdatedJobNames := []string{"job-B"}
			downstreamCUpdatedJobNames := []string{"job-C"}
			jobDeploymentService.On("UploadJobs", ctx, sampleTenant, downstreamBUpdatedJobNames, emptyJobNames).Return(nil)
			jobDeploymentService.On("UploadJobs", ctx, otherTenant, downstreamCUpdatedJobNames, emptyJobNames).Return(nil)
			jobDeploymentService.On("UploadJobs", ctx, sampleTenant, emptyJobNames, jobNamesToRemove).Return(nil)

			eventHandler.On("HandleEvent", mock.Anything).Times(1)
			alertManager := new(AlertManager)
			alertManager.On("SendJobEvent", mock.Anything)
			jobService := service.NewJobService(jobRepo, upstreamRepo, downstreamRepo, nil, upstreamResolver, nil, eventHandler, log, jobDeploymentService, nil, nil, nil, alertManager, config.JobValidationConfig{})

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

			jobService := service.NewJobService(jobRepo, upstreamRepo, downstreamRepo, nil, nil, nil, nil, log, nil, nil, nil, nil, nil, config.JobValidationConfig{})
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

			jobService := service.NewJobService(jobRepo, upstreamRepo, downstreamRepo, nil, nil, nil, nil, log, nil, nil, nil, nil, nil, config.JobValidationConfig{})

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

			jobService := service.NewJobService(jobRepo, upstreamRepo, downstreamRepo, nil, nil, nil, nil, log, nil, nil, nil, nil, nil, config.JobValidationConfig{})
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

			jobService := service.NewJobService(jobRepo, upstreamRepo, downstreamRepo, nil, nil, nil, eventHandler, log, jobDeploymentService, nil, nil, nil, nil, config.JobValidationConfig{})
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

			pluginService := NewPluginService(t)

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
			jobADestination := resourceURNA
			jobAUpstreamName := []resource.URN{resourceURNB}
			jobA := job.NewJob(sampleTenant, specA, jobADestination, jobAUpstreamName, false)

			specB, _ := job.NewSpecBuilder(jobVersion, "job-B", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()
			jobB := job.NewJob(sampleTenant, specB, resource.ZeroURN(), nil, false)

			incomingSpecs := []*job.Spec{specA, specB}

			existingJobs := []*job.Job{jobB}

			jobRepo.On("GetAllByTenant", ctx, sampleTenant).Return(existingJobs, nil)

			pluginService.On("ConstructDestinationURN", ctx, specA.Task().Name().String(), mock.Anything).Return(jobADestination, nil).Once()
			pluginService.On("IdentifyUpstreams", ctx, specA.Task().Name().String(), mock.Anything, mock.Anything).Return(jobAUpstreamName, nil)

			jobRepo.On("Add", ctx, mock.Anything).Return([]*job.Job{jobA}, nil)
			jobRepo.On("SetDirty", ctx, sampleTenant, []job.Name{jobA.Spec().Name()}, true).Return(nil)
			jobRepo.On("SetDirty", ctx, sampleTenant, []job.Name{jobA.Spec().Name()}, false).Return(nil)

			upstream := job.NewUpstreamResolved("job-B", "", resourceURNB, sampleTenant, "static", taskName, false)

			downstreamRepo.On("GetDownstreamByDestination", ctx, mock.Anything, mock.Anything).Return([]*job.Downstream{}, nil)

			jobWithUpstream := job.NewWithUpstream(jobA, []*job.Upstream{upstream})
			upstreamResolver.On("BulkResolve", ctx, project.Name(), []*job.Job{jobA}, mock.Anything).Return([]*job.WithUpstream{jobWithUpstream}, nil, nil)

			upstreamRepo.On("ReplaceUpstreams", ctx, []*job.WithUpstream{jobWithUpstream}).Return(nil)

			logWriter.On("Write", mock.Anything, mock.Anything).Return(nil)
			eventHandler.On("HandleEvent", mock.Anything).Times(1)

			jobNamesToUpload := []string{jobA.GetName()}
			var jobNamesToRemove []string
			jobDeploymentService.On("UploadJobs", ctx, sampleTenant, jobNamesToUpload, jobNamesToRemove).Return(nil)

			jobService := service.NewJobService(jobRepo, upstreamRepo, downstreamRepo, pluginService, upstreamResolver, tenantDetailsGetter, eventHandler, log, jobDeploymentService, compiler.NewEngine(), nil, nil, nil, config.JobValidationConfig{})
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

			pluginService := NewPluginService(t)

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
			jobADestination := resourceURNA
			jobAUpstreamName := []resource.URN{resourceURNB}
			jobA := job.NewJob(sampleTenant, specA, jobADestination, jobAUpstreamName, false)

			incomingSpecs := []*job.Spec{specA}

			w2, _ := models.NewWindow(jobVersion, "d", "0h", "24h")
			existingJobWindow := window.NewCustomConfig(w2)
			existingSpecA, _ := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, existingJobWindow, jobTask).WithAsset(jobAsset).Build()
			existingJobA := job.NewJob(sampleTenant, existingSpecA, jobADestination, jobAUpstreamName, false)
			existingSpecs := []*job.Job{existingJobA}

			pluginService.On("ConstructDestinationURN", ctx, specA.Task().Name().String(), mock.Anything).Return(jobADestination, nil).Once()
			pluginService.On("IdentifyUpstreams", ctx, specA.Task().Name().String(), mock.Anything, mock.Anything).Return(jobAUpstreamName, nil)

			jobRepo.On("GetAllByTenant", ctx, sampleTenant).Return(existingSpecs, nil)
			jobRepo.On("Update", ctx, mock.Anything).Return([]*job.Job{jobA}, nil)
			jobRepo.On("SetDirty", ctx, sampleTenant, []job.Name{jobA.Spec().Name()}, true).Return(nil)
			jobRepo.On("SetDirty", ctx, sampleTenant, []job.Name{jobA.Spec().Name()}, false).Return(nil)

			eventHandler.On("HandleEvent", mock.Anything).Times(1)

			upstream := job.NewUpstreamResolved("job-B", "", resourceURNB, sampleTenant, "static", taskName, false)

			downstreamRepo.On("GetDownstreamByDestination", ctx, mock.Anything, mock.Anything).Return([]*job.Downstream{}, nil)

			jobWithUpstream := job.NewWithUpstream(jobA, []*job.Upstream{upstream})
			upstreamResolver.On("BulkResolve", ctx, project.Name(), []*job.Job{jobA}, mock.Anything).Return([]*job.WithUpstream{jobWithUpstream}, nil, nil)

			upstreamRepo.On("ReplaceUpstreams", ctx, []*job.WithUpstream{jobWithUpstream}).Return(nil)

			logWriter.On("Write", mock.Anything, mock.Anything).Return(nil)

			jobNamesToUpload := []string{jobA.GetName()}
			var jobNamesToRemove []string
			jobDeploymentService.On("UploadJobs", ctx, sampleTenant, jobNamesToUpload, jobNamesToRemove).Return(nil)
			alertManager := new(AlertManager)
			alertManager.On("SendJobEvent", mock.Anything).Return()
			defer alertManager.AssertExpectations(t)

			jobService := service.NewJobService(jobRepo, upstreamRepo, downstreamRepo, pluginService, upstreamResolver, tenantDetailsGetter, eventHandler, log, jobDeploymentService, compiler.NewEngine(), nil, nil, alertManager, config.JobValidationConfig{})

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

			pluginService := NewPluginService(t)

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
			jobA := job.NewJob(sampleTenant, specA, resource.ZeroURN(), nil, false)

			specB, _ := job.NewSpecBuilder(jobVersion, "job-B", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()
			jobB := job.NewJob(sampleTenant, specB, resource.ZeroURN(), nil, false)

			incomingSpecs := []*job.Spec{specA}

			existingSpecs := []*job.Job{jobA, jobB}

			downstreamRepo.On("GetDownstreamByJobName", ctx, project.Name(), specB.Name()).Return(nil, nil)

			jobRepo.On("GetAllByTenant", ctx, sampleTenant).Return(existingSpecs, nil)
			jobRepo.On("Delete", ctx, project.Name(), specB.Name(), false).Return(nil)
			jobRepo.On("Delete", ctx, project.Name(), specB.Name(), false).Return(nil)
			eventHandler.On("HandleEvent", mock.Anything).Times(1)

			logWriter.On("Write", mock.Anything, mock.Anything).Return(nil)

			var jobNamesToUpload []string
			jobNamesToRemove := []string{specB.Name().String()}
			jobDeploymentService.On("UploadJobs", ctx, sampleTenant, jobNamesToUpload, jobNamesToRemove).Return(nil)

			alertManager := new(AlertManager)
			alertManager.On("SendJobEvent", mock.Anything).Return()
			defer alertManager.AssertExpectations(t)

			jobService := service.NewJobService(jobRepo, upstreamRepo, downstreamRepo, pluginService, upstreamResolver, tenantDetailsGetter, eventHandler, log, jobDeploymentService, compiler.NewEngine(), nil, nil, alertManager, config.JobValidationConfig{})
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

			pluginService := NewPluginService(t)

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
			jobA := job.NewJob(sampleTenant, specA, resource.ZeroURN(), nil, false)
			jobB := job.NewJob(sampleTenant, specB, resource.ZeroURN(), nil, false)
			jobC := job.NewJob(sampleTenant, specC, resource.ZeroURN(), nil, false)
			jobD := job.NewJob(sampleTenant, specD, resource.ZeroURN(), nil, false)

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

			alertManager := new(AlertManager)
			alertManager.On("SendJobEvent", mock.Anything).Return()
			defer alertManager.AssertExpectations(t)

			jobService := service.NewJobService(jobRepo, upstreamRepo, downstreamRepo, pluginService, upstreamResolver, tenantDetailsGetter, eventHandler, log, jobDeploymentService, compiler.NewEngine(), nil, nil, alertManager, config.JobValidationConfig{})
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

			pluginService := NewPluginService(t)

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
			existingJobB := job.NewJob(sampleTenant, existingSpecB, resource.ZeroURN(), nil, false)
			existingSpecC, _ := job.NewSpecBuilder(jobVersion, "job-C", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()
			existingJobC := job.NewJob(sampleTenant, existingSpecC, resource.ZeroURN(), nil, false)

			existingSpecs := []*job.Job{existingJobB, existingJobC}

			jobRepo.On("GetAllByTenant", ctx, sampleTenant).Return(existingSpecs, nil)

			jobADestination := resourceURNA
			jobBDestination := resourceURNB
			pluginService.On("ConstructDestinationURN", ctx, specA.Task().Name().String(), mock.Anything).Return(jobADestination, nil).Once()
			pluginService.On("ConstructDestinationURN", ctx, specB.Task().Name().String(), mock.Anything).Return(jobBDestination, nil).Once()

			jobAUpstreamNames := []resource.URN{resourceURNB}
			var jobBUpstreamNames []resource.URN
			pluginService.On("IdentifyUpstreams", ctx, specA.Task().Name().String(), mock.Anything, mock.Anything).Return(jobAUpstreamNames, nil)
			pluginService.On("IdentifyUpstreams", ctx, specB.Task().Name().String(), mock.Anything, mock.Anything).Return(jobBUpstreamNames, nil)

			jobA := job.NewJob(sampleTenant, specA, jobADestination, jobAUpstreamNames, false)
			jobRepo.On("Add", ctx, mock.Anything).Return([]*job.Job{jobA}, nil)

			jobB := job.NewJob(sampleTenant, specB, resource.ZeroURN(), nil, false)
			jobRepo.On("Update", ctx, mock.Anything).Return([]*job.Job{jobB}, nil)

			jobRepo.On("SetDirty", ctx, sampleTenant, []job.Name{jobA.Spec().Name(), jobB.Spec().Name()}, true).Return(nil)
			jobRepo.On("SetDirty", ctx, sampleTenant, []job.Name{jobA.Spec().Name(), jobB.Spec().Name()}, false).Return(nil)

			downstreamRepo.On("GetDownstreamByJobName", ctx, project.Name(), existingSpecC.Name()).Return(nil, nil)
			jobRepo.On("Delete", ctx, project.Name(), existingSpecC.Name(), false).Return(nil)

			downstreamRepo.On("GetDownstreamByDestination", ctx, mock.Anything, mock.Anything).Return([]*job.Downstream{}, nil)

			upstream := job.NewUpstreamResolved("job-B", "", resourceURNB, sampleTenant, "static", taskName, false)
			jobWithUpstream := job.NewWithUpstream(jobA, []*job.Upstream{upstream})
			jobsToResolve := []*job.Job{jobA, jobB}
			upstreamResolver.On("BulkResolve", ctx, project.Name(), mock.MatchedBy(func(elems []*job.Job) bool {
				return assert.ElementsMatch(t, elems, jobsToResolve)
			}), mock.Anything).Return([]*job.WithUpstream{jobWithUpstream}, nil, nil)

			upstreamRepo.On("ReplaceUpstreams", ctx, []*job.WithUpstream{jobWithUpstream}).Return(nil)

			logWriter.On("Write", mock.Anything, mock.Anything).Return(nil)
			eventHandler.On("HandleEvent", mock.Anything).Times(3)
			alertManager := new(AlertManager)
			alertManager.On("SendJobEvent", mock.Anything).Return()
			defer alertManager.AssertExpectations(t)

			jobNamesToRemove := []string{existingJobC.GetName()}
			var emptyStringArr []string

			jobDeploymentService.On("UploadJobs", ctx, sampleTenant, emptyStringArr, jobNamesToRemove).Return(nil).Once()
			jobDeploymentService.On("UploadJobs", ctx, sampleTenant, mock.Anything, emptyStringArr).Return(nil).Once()

			jobService := service.NewJobService(jobRepo, upstreamRepo, downstreamRepo, pluginService, upstreamResolver, tenantDetailsGetter, eventHandler, log, jobDeploymentService, compiler.NewEngine(), nil, nil, alertManager, config.JobValidationConfig{})
			err := jobService.ReplaceAll(ctx, sampleTenant, incomingSpecs, jobNamesWithInvalidSpec, logWriter)
			assert.NoError(t, err)
		})
		t.Run("should not return error if modified the job and the downstream in one time", func(t *testing.T) {
			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			upstreamRepo := new(UpstreamRepository)
			defer upstreamRepo.AssertExpectations(t)

			downstreamRepo := new(DownstreamRepository)
			defer downstreamRepo.AssertExpectations(t)

			pluginService := NewPluginService(t)

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
			jobADestination := resourceURNA
			jobAUpstreamName := []resource.URN{resourceURNB}
			jobA := job.NewJob(sampleTenant, specA, jobADestination, jobAUpstreamName, false)

			specB, _ := job.NewSpecBuilder(jobVersion, "job-B", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()
			jobBDestination := resourceURNB
			var jobBUpstreamName []resource.URN
			jobB := job.NewJob(sampleTenant, specB, jobBDestination, jobBUpstreamName, false)

			incomingSpecs := []*job.Spec{specA, specB}

			w2, _ := models.NewWindow(jobVersion, "d", "0h", "24h")
			existingJobWindow := window.NewCustomConfig(w2)
			existingSpecA, _ := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, existingJobWindow, jobTask).WithAsset(jobAsset).Build()
			existingJobA := job.NewJob(sampleTenant, existingSpecA, jobADestination, jobAUpstreamName, false)
			existingSpecB, _ := job.NewSpecBuilder(jobVersion, "job-B", "sample-owner", jobSchedule, existingJobWindow, jobTask).WithAsset(jobAsset).Build()
			existingJobB := job.NewJob(sampleTenant, existingSpecB, jobBDestination, jobBUpstreamName, false)
			existingSpecs := []*job.Job{existingJobA, existingJobB}

			pluginService.On("ConstructDestinationURN", ctx, specA.Task().Name().String(), mock.Anything).Return(jobADestination, nil).Once()
			pluginService.On("IdentifyUpstreams", ctx, specA.Task().Name().String(), mock.Anything, mock.Anything).Return(jobAUpstreamName, nil).Once()

			pluginService.On("ConstructDestinationURN", ctx, specB.Task().Name().String(), mock.Anything).Return(jobBDestination, nil).Once()
			pluginService.On("IdentifyUpstreams", ctx, specB.Task().Name().String(), mock.Anything, mock.Anything).Return(nil, nil)

			jobRepo.On("GetAllByTenant", ctx, sampleTenant).Return(existingSpecs, nil)
			jobRepo.On("Update", ctx, mock.Anything).Return([]*job.Job{jobA, jobB}, nil)
			jobRepo.On("SetDirty", ctx, sampleTenant, []job.Name{jobA.Spec().Name(), jobB.Spec().Name()}, true).Return(nil)
			jobRepo.On("SetDirty", ctx, sampleTenant, []job.Name{jobA.Spec().Name(), jobB.Spec().Name()}, false).Return(nil)

			eventHandler.On("HandleEvent", mock.Anything).Times(2)
			alertManager := new(AlertManager)
			alertManager.On("SendJobEvent", mock.Anything).Return()
			defer alertManager.AssertExpectations(t)

			upstream := job.NewUpstreamResolved("job-B", "", resourceURNB, sampleTenant, "static", taskName, false)

			downstreamRepo.On("GetDownstreamByDestination", ctx, mock.Anything, mock.Anything).Return([]*job.Downstream{}, nil)

			jobAWithUpstream := job.NewWithUpstream(jobA, []*job.Upstream{upstream})
			jobBWithUpstream := job.NewWithUpstream(jobB, []*job.Upstream{})
			upstreamResolver.On("BulkResolve", ctx, project.Name(), mock.MatchedBy(func(elems []*job.Job) bool {
				return assert.ElementsMatch(t, elems, []*job.Job{jobA, jobB})
			}), mock.Anything).Return([]*job.WithUpstream{jobAWithUpstream, jobBWithUpstream}, nil, nil)

			upstreamRepo.On("ReplaceUpstreams", ctx, []*job.WithUpstream{jobAWithUpstream, jobBWithUpstream}).Return(nil)

			logWriter.On("Write", mock.Anything, mock.Anything).Return(nil)

			jobNamesToUpload := []string{jobA.GetName(), jobB.GetName()}
			var jobNamesToRemove []string
			jobDeploymentService.On("UploadJobs", ctx, sampleTenant, mock.MatchedBy(func(elems []string) bool {
				return assert.ElementsMatch(t, elems, jobNamesToUpload)
			}), jobNamesToRemove).Return(nil)

			jobService := service.NewJobService(jobRepo, upstreamRepo, downstreamRepo, pluginService, upstreamResolver, tenantDetailsGetter, eventHandler, log, jobDeploymentService, compiler.NewEngine(), nil, nil, alertManager, config.JobValidationConfig{})
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

			pluginService := NewPluginService(t)

			upstreamResolver := new(UpstreamResolver)
			defer upstreamResolver.AssertExpectations(t)

			tenantDetailsGetter := new(TenantDetailsGetter)
			defer tenantDetailsGetter.AssertExpectations(t)

			logWriter := new(mockWriter)
			defer logWriter.AssertExpectations(t)

			jobDeploymentService := new(JobDeploymentService)
			defer jobDeploymentService.AssertExpectations(t)

			eventHandler := newEventHandler(t)
			alertManager := new(AlertManager)
			alertManager.On("SendJobEvent", mock.Anything).Return()
			defer alertManager.AssertExpectations(t)

			tenantDetailsGetter.On("GetDetails", ctx, sampleTenant).Return(detailedTenant, nil)

			specA, _ := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()
			specB, _ := job.NewSpecBuilder(jobVersion, "job-B", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()
			incomingSpecs := []*job.Spec{specA, specB}

			w2, _ := models.NewWindow(jobVersion, "d", "0h", "24h")
			existingJobWindow := window.NewCustomConfig(w2)
			existingSpecB, _ := job.NewSpecBuilder(jobVersion, "job-B", "sample-owner", jobSchedule, existingJobWindow, jobTask).WithAsset(jobAsset).Build()
			existingJobB := job.NewJob(sampleTenant, existingSpecB, resource.ZeroURN(), nil, false)
			existingSpecC, _ := job.NewSpecBuilder(jobVersion, "job-C", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()
			existingJobC := job.NewJob(sampleTenant, existingSpecC, resource.ZeroURN(), nil, false)
			existingSpecD, _ := job.NewSpecBuilder(jobVersion, "job-D", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()
			existingJobD := job.NewJob(sampleTenant, existingSpecD, resource.ZeroURN(), nil, false)

			existingSpecs := []*job.Job{existingJobB, existingJobC, existingJobD}

			jobRepo.On("GetAllByTenant", ctx, sampleTenant).Return(existingSpecs, nil)

			jobADestination := resourceURNA
			jobBDestination := resourceURNB
			pluginService.On("ConstructDestinationURN", ctx, specA.Task().Name().String(), mock.Anything).Return(jobADestination, nil).Once()
			pluginService.On("ConstructDestinationURN", ctx, specB.Task().Name().String(), mock.Anything).Return(jobBDestination, nil).Once()

			jobAUpstreamNames := []resource.URN{resourceURNB}
			var jobBUpstreamNames []resource.URN
			pluginService.On("IdentifyUpstreams", ctx, specA.Task().Name().String(), mock.Anything, mock.Anything).Return(jobAUpstreamNames, nil)
			pluginService.On("IdentifyUpstreams", ctx, specB.Task().Name().String(), mock.Anything, mock.Anything).Return(jobBUpstreamNames, nil)

			jobA := job.NewJob(sampleTenant, specA, jobADestination, jobAUpstreamNames, false)
			jobRepo.On("Add", ctx, mock.Anything).Return([]*job.Job{jobA}, nil)
			jobB := job.NewJob(sampleTenant, specB, resource.ZeroURN(), nil, false)
			jobRepo.On("Update", ctx, mock.Anything).Return([]*job.Job{jobB}, nil)
			jobRepo.On("SetDirty", ctx, sampleTenant, []job.Name{jobA.Spec().Name(), jobB.Spec().Name()}, true).Return(nil)
			jobRepo.On("SetDirty", ctx, sampleTenant, []job.Name{jobA.Spec().Name(), jobB.Spec().Name()}, false).Return(nil)

			downstreamRepo.On("GetDownstreamByJobName", ctx, project.Name(), existingSpecC.Name()).Return(nil, nil)
			jobRepo.On("Delete", ctx, project.Name(), existingSpecC.Name(), false).Return(nil)

			downstreamRepo.On("GetDownstreamByDestination", ctx, mock.Anything, mock.Anything).Return([]*job.Downstream{}, nil)

			upstream := job.NewUpstreamResolved("job-B", "", resourceURNB, sampleTenant, "static", taskName, false)
			jobWithUpstream := job.NewWithUpstream(jobA, []*job.Upstream{upstream})
			upstreamResolver.On("BulkResolve", ctx, project.Name(), mock.MatchedBy(func(elems []*job.Job) bool {
				return assert.ElementsMatch(t, elems, []*job.Job{jobA, jobB})
			}), mock.Anything).Return([]*job.WithUpstream{jobWithUpstream}, nil, nil)

			upstreamRepo.On("ReplaceUpstreams", ctx, []*job.WithUpstream{jobWithUpstream}).Return(nil)

			logWriter.On("Write", mock.Anything, mock.Anything).Return(nil)
			eventHandler.On("HandleEvent", mock.Anything).Times(3)

			jobNamesToRemove := []string{existingJobC.GetName()}
			var emptyStringArr []string
			jobDeploymentService.On("UploadJobs", ctx, sampleTenant, mock.Anything, emptyStringArr).Return(nil)
			jobDeploymentService.On("UploadJobs", ctx, sampleTenant, emptyStringArr, jobNamesToRemove).Return(nil)

			jobService := service.NewJobService(jobRepo, upstreamRepo, downstreamRepo, pluginService, upstreamResolver, tenantDetailsGetter, eventHandler, log, jobDeploymentService, compiler.NewEngine(), nil, nil, alertManager, config.JobValidationConfig{})
			err := jobService.ReplaceAll(ctx, sampleTenant, incomingSpecs, []job.Name{"job-D"}, logWriter)
			assert.NoError(t, err)
		})
		t.Run("skips adding new invalid jobs, such jobs should be marked as dirty, and left like that, also dont process other jobs to be deleted and early return", func(t *testing.T) {
			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			upstreamRepo := new(UpstreamRepository)
			defer upstreamRepo.AssertExpectations(t)

			downstreamRepo := new(DownstreamRepository)
			defer downstreamRepo.AssertExpectations(t)

			pluginService := NewPluginService(t)

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
			existingJobC := job.NewJob(sampleTenant, existingSpecC, resource.ZeroURN(), nil, false)
			existingSpecs := []*job.Job{existingJobC}

			jobRepo.On("GetAllByTenant", ctx, sampleTenant).Return(existingSpecs, nil)

			var specADestination resource.URN
			pluginService.On("ConstructDestinationURN", ctx, specA.Task().Name().String(), mock.Anything).Return(specADestination, errors.New("internal error")).Once()

			logWriter.On("Write", mock.Anything, mock.Anything).Return(nil)
			jobService := service.NewJobService(jobRepo, upstreamRepo, downstreamRepo, pluginService, upstreamResolver, tenantDetailsGetter, eventHandler, log, jobDeploymentService, compiler.NewEngine(), nil, nil, nil, config.JobValidationConfig{})
			err := jobService.ReplaceAll(ctx, sampleTenant, incomingSpecs, jobNamesWithInvalidSpec, logWriter)
			assert.ErrorContains(t, err, "internal error")
		})
		t.Run("if incoming modified jobs has errors in generating then dont persist it, dont mark dirty, and return early", func(t *testing.T) {
			// such scenarios will be retried until fixed
			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			upstreamRepo := new(UpstreamRepository)
			defer upstreamRepo.AssertExpectations(t)

			downstreamRepo := new(DownstreamRepository)
			defer downstreamRepo.AssertExpectations(t)

			pluginService := NewPluginService(t)

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
			existingJobB := job.NewJob(sampleTenant, existingSpecB, resource.ZeroURN(), nil, false)
			existingSpecC, _ := job.NewSpecBuilder(jobVersion, "job-C", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()
			existingJobC := job.NewJob(sampleTenant, existingSpecC, resource.ZeroURN(), nil, false)
			existingSpecs := []*job.Job{existingJobB, existingJobC}
			jobRepo.On("GetAllByTenant", ctx, sampleTenant).Return(existingSpecs, nil)

			var jobBDestination resource.URN
			pluginService.On("ConstructDestinationURN", ctx, specB.Task().Name().String(), mock.Anything).Return(jobBDestination, errors.New("internal error")).Once()

			logWriter.On("Write", mock.Anything, mock.Anything).Return(nil)
			alertManager := new(AlertManager)

			jobService := service.NewJobService(jobRepo, upstreamRepo, downstreamRepo, pluginService, upstreamResolver, tenantDetailsGetter, eventHandler, log, jobDeploymentService, compiler.NewEngine(), nil, nil, alertManager, config.JobValidationConfig{})
			err := jobService.ReplaceAll(ctx, sampleTenant, incomingSpecs, jobNamesWithInvalidSpec, logWriter)
			assert.ErrorContains(t, err, "internal error")
		})
		t.Run("skips to delete jobs if the downstream is not deleted", func(t *testing.T) {
			/*
				incoming : A, E
				Existing : C, D, E
				Relation : C -> E

				Note: Job E is unmodified

				Expectation :
					Add job A
					Delete Job D
					Delete Job D from GCS
					Skip Job C Deletion as it has a Downstream still existing (this will be logged to the client)
						Job C shall not be marked as Dirty and Also the DAG will not be removed from GCS and DB
					mark job A as Dirty
					Don't upload it on GCS
			*/
			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			upstreamRepo := new(UpstreamRepository)
			defer upstreamRepo.AssertExpectations(t)

			downstreamRepo := new(DownstreamRepository)
			defer downstreamRepo.AssertExpectations(t)

			pluginService := NewPluginService(t)

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
			existingJobC := job.NewJob(sampleTenant, existingSpecC, resource.ZeroURN(), nil, false)
			existingSpecD, _ := job.NewSpecBuilder(jobVersion, "job-D", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()
			existingJobD := job.NewJob(sampleTenant, existingSpecD, resource.ZeroURN(), nil, false)
			existingSpecE, _ := job.NewSpecBuilder(jobVersion, "job-E", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()
			existingJobE := job.NewJob(sampleTenant, existingSpecE, resource.ZeroURN(), nil, false)
			existingSpecs := []*job.Job{existingJobC, existingJobD, existingJobE}

			jobRepo.On("GetAllByTenant", ctx, sampleTenant).Return(existingSpecs, nil)

			jobADestination := resourceURNA
			pluginService.On("ConstructDestinationURN", ctx, specA.Task().Name().String(), mock.Anything).Return(jobADestination, nil).Once()

			jobAUpstreamNames := []resource.URN{resourceURNB}
			pluginService.On("IdentifyUpstreams", ctx, specA.Task().Name().String(), mock.Anything, mock.Anything).Return(jobAUpstreamNames, nil)

			jobA := job.NewJob(sampleTenant, specA, jobADestination, jobAUpstreamNames, false)
			jobRepo.On("Add", ctx, mock.Anything).Return([]*job.Job{jobA}, nil)

			downstreamList := []*job.Downstream{
				job.NewDownstream("job-E", project.Name(), namespace.Name(), taskName),
			}

			downstreamRepo.On("GetDownstreamByJobName", ctx, project.Name(), existingSpecC.Name()).Return(downstreamList, nil)
			downstreamRepo.On("GetDownstreamByJobName", ctx, project.Name(), existingSpecD.Name()).Return(nil, nil)
			downstreamRepo.On("GetDownstreamByJobName", ctx, project.Name(), existingSpecE.Name()).Return(nil, nil)
			jobRepo.On("Delete", ctx, project.Name(), existingSpecD.Name(), false).Return(nil)
			jobRepo.On("SetDirty", ctx, sampleTenant, []job.Name{jobA.Spec().Name()}, true).Return(nil)

			logWriter.On("Write", mock.Anything, mock.Anything).Return(nil)
			eventHandler.On("HandleEvent", mock.Anything).Times(2)

			jobNamesToRemove := []string{existingJobD.GetName()}
			var emptyStringArr []string

			jobDeploymentService.On("UploadJobs", ctx, sampleTenant, emptyStringArr, jobNamesToRemove).Return(nil)
			alertManager := new(AlertManager)
			alertManager.On("SendJobEvent", mock.Anything).Return()
			defer alertManager.AssertExpectations(t)
			jobService := service.NewJobService(jobRepo, upstreamRepo, downstreamRepo, pluginService, upstreamResolver, tenantDetailsGetter, eventHandler, log, jobDeploymentService, compiler.NewEngine(), nil, nil, alertManager, config.JobValidationConfig{})
			err := jobService.ReplaceAll(ctx, sampleTenant, incomingSpecs, jobNamesWithInvalidSpec, logWriter)
			assert.ErrorContains(t, err, "job is being used by")
		})
		t.Run("should break process if one of job failed to be added", func(t *testing.T) {
			/*
				incoming : A, B

				Expectation :
					Add job A to DB
					Error in Adding Job B to DB
					Mark Job A as Dirty
			*/
			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			upstreamRepo := new(UpstreamRepository)
			defer upstreamRepo.AssertExpectations(t)

			downstreamRepo := new(DownstreamRepository)
			defer downstreamRepo.AssertExpectations(t)

			pluginService := NewPluginService(t)

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
			jobADestination := resourceURNA
			jobAUpstreamName := []resource.URN{resourceURNB}
			jobA := job.NewJob(sampleTenant, specA, jobADestination, jobAUpstreamName, false)

			specB, _ := job.NewSpecBuilder(jobVersion, "job-B", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()
			jobBDestination := resourceURNB
			jobBUpstreamName := []resource.URN{resourceURNC}

			incomingSpecs := []*job.Spec{specA, specB}

			jobRepo.On("GetAllByTenant", ctx, sampleTenant).Return([]*job.Job{}, nil)

			pluginService.On("ConstructDestinationURN", ctx, specA.Task().Name().String(), mock.Anything).Return(jobADestination, nil).Once()
			pluginService.On("IdentifyUpstreams", ctx, specA.Task().Name().String(), mock.Anything, mock.Anything).Return(jobAUpstreamName, nil).Once()

			pluginService.On("ConstructDestinationURN", ctx, specB.Task().Name().String(), mock.Anything).Return(jobBDestination, nil).Once()
			pluginService.On("IdentifyUpstreams", ctx, specB.Task().Name().String(), mock.Anything, mock.Anything).Return(jobBUpstreamName, nil).Once()

			jobRepo.On("Add", ctx, mock.Anything).Return([]*job.Job{jobA}, errors.New("internal error"))
			jobRepo.On("SetDirty", ctx, sampleTenant, []job.Name{jobA.Spec().Name()}, true).Return(nil)

			logWriter.On("Write", mock.Anything, mock.Anything).Return(nil)
			eventHandler.On("HandleEvent", mock.Anything).Times(1)

			jobService := service.NewJobService(jobRepo, upstreamRepo, downstreamRepo, pluginService, upstreamResolver, tenantDetailsGetter, eventHandler, log, jobDeploymentService, compiler.NewEngine(), nil, nil, nil, config.JobValidationConfig{})
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

			pluginService := NewPluginService(t)

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
			jobADestination := resourceURNA
			jobAUpstreamName := []resource.URN{resourceURNB}

			incomingSpecs := []*job.Spec{specA}

			w2, _ := models.NewWindow(jobVersion, "d", "0h", "24h")
			existingJobWindow := window.NewCustomConfig(w2)
			existingSpecA, _ := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, existingJobWindow, jobTask).WithAsset(jobAsset).Build()
			existingJobA := job.NewJob(sampleTenant, existingSpecA, jobADestination, jobAUpstreamName, false)
			existingSpecs := []*job.Job{existingJobA}

			jobRepo.On("GetAllByTenant", ctx, sampleTenant).Return(existingSpecs, nil)

			pluginService.On("ConstructDestinationURN", ctx, specA.Task().Name().String(), mock.Anything).Return(jobADestination, nil).Once()
			pluginService.On("IdentifyUpstreams", ctx, specA.Task().Name().String(), mock.Anything, mock.Anything).Return(jobAUpstreamName, nil)

			downstreamRepo.On("GetDownstreamByDestination", ctx, mock.Anything, mock.Anything).Return([]*job.Downstream{}, nil)

			jobRepo.On("Update", ctx, mock.Anything).Return([]*job.Job{}, errors.New("internal error"))

			logWriter.On("Write", mock.Anything, mock.Anything).Return(nil)

			jobService := service.NewJobService(jobRepo, upstreamRepo, downstreamRepo, pluginService, upstreamResolver, tenantDetailsGetter, nil, log, jobDeploymentService, compiler.NewEngine(), nil, nil, nil, config.JobValidationConfig{})
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

			pluginService := NewPluginService(t)

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
			jobA := job.NewJob(sampleTenant, specA, resource.ZeroURN(), nil, false)

			specB, _ := job.NewSpecBuilder(jobVersion, "job-B", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()
			jobB := job.NewJob(sampleTenant, specB, resource.ZeroURN(), nil, false)

			incomingSpecs := []*job.Spec{specA}

			existingSpecs := []*job.Job{jobA, jobB}

			jobRepo.On("GetAllByTenant", ctx, sampleTenant).Return(existingSpecs, nil)

			downstreamRepo.On("GetDownstreamByJobName", ctx, project.Name(), specB.Name()).Return(nil, nil)
			jobRepo.On("Delete", ctx, project.Name(), specB.Name(), false).Return(errors.New("internal error"))

			logWriter.On("Write", mock.Anything, mock.Anything).Return(nil).Times(3)

			jobService := service.NewJobService(jobRepo, upstreamRepo, downstreamRepo, pluginService, upstreamResolver, tenantDetailsGetter, nil, log, jobDeploymentService, compiler.NewEngine(), nil, nil, nil, config.JobValidationConfig{})
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

			pluginService := NewPluginService(t)

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
			jobA := job.NewJob(sampleTenant, specA, resource.ZeroURN(), nil, false)

			specB, _ := job.NewSpecBuilder(jobVersion, "job-B", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()
			jobB := job.NewJob(sampleTenant, specB, resource.ZeroURN(), nil, false)

			incomingSpecs := []*job.Spec{specA}

			existingSpecs := []*job.Job{jobA, jobB}

			jobRepo.On("GetAllByTenant", ctx, sampleTenant).Return(existingSpecs, nil)

			downstreamRepo.On("GetDownstreamByJobName", ctx, project.Name(), specB.Name()).Return(nil, errors.New("internal error"))

			logWriter.On("Write", mock.Anything, mock.Anything).Return(nil).Twice()

			jobService := service.NewJobService(jobRepo, upstreamRepo, downstreamRepo, pluginService, upstreamResolver, tenantDetailsGetter, nil, log, jobDeploymentService, compiler.NewEngine(), nil, nil, nil, config.JobValidationConfig{})
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

			pluginService := NewPluginService(t)

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
			jobA := job.NewJob(sampleTenant, specA, resource.ZeroURN(), nil, false)
			jobB := job.NewJob(sampleTenant, specB, resource.ZeroURN(), nil, false)
			jobC := job.NewJob(sampleTenant, specC, resource.ZeroURN(), nil, false)
			jobD := job.NewJob(sampleTenant, specD, resource.ZeroURN(), nil, false)

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
			alertManager := new(AlertManager)
			alertManager.On("SendJobEvent", mock.Anything).Return()
			defer alertManager.AssertExpectations(t)
			jobService := service.NewJobService(jobRepo, upstreamRepo, downstreamRepo, pluginService, upstreamResolver, tenantDetailsGetter, eventHandler, log, jobDeploymentService, compiler.NewEngine(), nil, nil, alertManager, config.JobValidationConfig{})
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

			pluginService := NewPluginService(t)

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

			incomingSpecs := []*job.Spec{specA, specB}

			errorMsg := "project/namespace error"
			tenantDetailsGetter.On("GetDetails", ctx, sampleTenant).Return(nil, errors.New(errorMsg))

			jobService := service.NewJobService(jobRepo, upstreamRepo, downstreamRepo, pluginService, upstreamResolver, tenantDetailsGetter, nil, log, jobDeploymentService, compiler.NewEngine(), nil, nil, nil, config.JobValidationConfig{})

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

			pluginService := NewPluginService(t)

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
			jobADestination := resourceURNA
			jobAUpstreamName := []resource.URN{resourceURNB}
			jobA := job.NewJob(sampleTenant, specA, jobADestination, jobAUpstreamName, false)

			specB, _ := job.NewSpecBuilder(jobVersion, "job-B", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()
			jobB := job.NewJob(sampleTenant, specB, resource.ZeroURN(), nil, false)

			incomingSpecs := []*job.Spec{specA, specB}

			existingJobs := []*job.Job{jobB}

			jobRepo.On("GetAllByTenant", ctx, sampleTenant).Return(existingJobs, nil)

			pluginService.On("ConstructDestinationURN", ctx, specA.Task().Name().String(), mock.Anything).Return(jobADestination, nil).Once()
			pluginService.On("IdentifyUpstreams", ctx, specA.Task().Name().String(), mock.Anything, mock.Anything).Return(jobAUpstreamName, nil)

			jobRepo.On("Add", ctx, mock.Anything).Return([]*job.Job{jobA}, nil)
			jobRepo.On("SetDirty", ctx, sampleTenant, []job.Name{jobA.Spec().Name()}, true).Return(nil)

			upstream := job.NewUpstreamResolved("job-B", "", resourceURNB, sampleTenant, "static", taskName, false)

			downstreamRepo.On("GetDownstreamByDestination", ctx, mock.Anything, mock.Anything).Return([]*job.Downstream{}, nil)

			jobWithUpstream := job.NewWithUpstream(jobA, []*job.Upstream{upstream})
			upstreamResolver.On("BulkResolve", ctx, project.Name(), []*job.Job{jobA}, mock.Anything).Return([]*job.WithUpstream{jobWithUpstream}, nil, nil)

			upstreamRepo.On("ReplaceUpstreams", ctx, []*job.WithUpstream{jobWithUpstream}).Return(nil)

			logWriter.On("Write", mock.Anything, mock.Anything).Return(nil)
			eventHandler.On("HandleEvent", mock.Anything).Times(1)

			errorMsg := "internal error"
			jobNamesToUpload := []string{specA.Name().String()}
			var emptyNmaesList []string
			jobDeploymentService.On("UploadJobs", ctx, sampleTenant, jobNamesToUpload, emptyNmaesList).Return(errors.New(errorMsg))

			jobService := service.NewJobService(jobRepo, upstreamRepo, downstreamRepo, pluginService, upstreamResolver, tenantDetailsGetter, eventHandler, log, jobDeploymentService, compiler.NewEngine(), nil, nil, nil, config.JobValidationConfig{})
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

			pluginService := NewPluginService(t)

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
			jobADestination := resourceURNA
			jobAUpstreamName := []resource.URN{resourceURNB}
			jobA := job.NewJob(sampleTenant, specA, jobADestination, jobAUpstreamName, false)

			var jobBDestination resource.URN
			jobBUpstreamName := []resource.URN{resourceURNC}
			specB, _ := job.NewSpecBuilder(jobVersion, "job-B", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()
			jobB := job.NewJob(sampleTenant, specB, jobBDestination, jobBUpstreamName, false)

			jobRepo.On("GetAllByTenant", ctx, sampleTenant).Return([]*job.Job{jobA, jobB}, nil)

			tenantDetailsGetter.On("GetDetails", ctx, sampleTenant).Return(detailedTenant, nil)

			pluginService.On("ConstructDestinationURN", ctx, specA.Task().Name().String(), mock.Anything).Return(jobADestination, nil).Once()
			pluginService.On("IdentifyUpstreams", ctx, specA.Task().Name().String(), mock.Anything, mock.Anything).Return(jobAUpstreamName, nil)

			pluginService.On("ConstructDestinationURN", ctx, specB.Task().Name().String(), mock.Anything).Return(jobBDestination, nil).Once()
			pluginService.On("IdentifyUpstreams", ctx, specB.Task().Name().String(), mock.Anything, mock.Anything).Return(jobBUpstreamName, nil)

			jobRepo.On("Update", ctx, mock.Anything).Return([]*job.Job{jobA, jobB}, nil)

			upstreamB := job.NewUpstreamResolved("job-B", "", resourceURNB, sampleTenant, "static", taskName, false)
			jobAWithUpstream := job.NewWithUpstream(jobA, []*job.Upstream{upstreamB})
			upstreamC := job.NewUpstreamResolved("job-C", "", resourceURNC, sampleTenant, "static", taskName, false)
			jobBWithUpstream := job.NewWithUpstream(jobB, []*job.Upstream{upstreamC})
			upstreamResolver.On("BulkResolve", ctx, project.Name(), []*job.Job{jobA, jobB}, mock.Anything).Return([]*job.WithUpstream{jobAWithUpstream, jobBWithUpstream}, nil)

			upstreamRepo.On("ReplaceUpstreams", ctx, []*job.WithUpstream{jobAWithUpstream, jobBWithUpstream}).Return(nil)

			logWriter.On("Write", mock.Anything, mock.Anything).Return(nil).Times(3)
			eventHandler.On("HandleEvent", mock.Anything).Times(2)

			var jobNamesToRemove []string
			jobNamesToUpload := []string{jobA.GetName(), jobB.GetName()}
			jobDeploymentService.On("UploadJobs", ctx, sampleTenant, jobNamesToUpload, jobNamesToRemove).Return(nil)
			alertManager := new(AlertManager)
			alertManager.On("SendJobEvent", mock.Anything).Return()
			defer alertManager.AssertExpectations(t)
			jobService := service.NewJobService(jobRepo, upstreamRepo, downstreamRepo, pluginService, upstreamResolver, tenantDetailsGetter, eventHandler, log, jobDeploymentService, compiler.NewEngine(), nil, nil, alertManager, config.JobValidationConfig{})
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

			pluginService := NewPluginService(t)

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
			jobADestination := resourceURNA
			jobAUpstreamName := []resource.URN{resourceURNB}
			jobA := job.NewJob(sampleTenant, specA, jobADestination, jobAUpstreamName, false)
			jobsTenant1 := []*job.Job{jobA}

			var jobBDestination resource.URN
			var jobBUpstreamName []resource.URN
			specB, _ := job.NewSpecBuilder(jobVersion, "job-B", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()
			jobB := job.NewJob(otherTenant, specB, jobBDestination, jobBUpstreamName, false)
			jobsTenant2 := []*job.Job{jobB}

			jobRepo.On("GetAllByTenant", ctx, sampleTenant).Return(jobsTenant1, nil).Once()
			jobRepo.On("GetAllByTenant", ctx, otherTenant).Return(jobsTenant2, nil).Once()

			tenantDetailsGetter.On("GetDetails", ctx, sampleTenant).Return(detailedTenant, nil).Once()
			tenantDetailsGetter.On("GetDetails", ctx, otherTenant).Return(detailedOtherTenant, nil).Once()

			pluginService.On("ConstructDestinationURN", ctx, specA.Task().Name().String(), mock.Anything).Return(jobADestination, nil).Once()
			pluginService.On("IdentifyUpstreams", ctx, specA.Task().Name().String(), mock.Anything, mock.Anything).Return(jobAUpstreamName, nil).Once()
			pluginService.On("ConstructDestinationURN", ctx, specB.Task().Name().String(), mock.Anything).Return(jobBDestination, nil).Once()
			pluginService.On("IdentifyUpstreams", ctx, specB.Task().Name().String(), mock.Anything, mock.Anything).Return(jobBUpstreamName, nil).Once()

			jobRepo.On("Update", ctx, mock.Anything).Return([]*job.Job{jobA}, nil).Once()
			jobRepo.On("Update", ctx, mock.Anything).Return([]*job.Job{jobB}, nil).Once()

			upstreamB := job.NewUpstreamResolved("job-B", "", resourceURNB, sampleTenant, "static", taskName, false)
			jobAWithUpstream := job.NewWithUpstream(jobA, []*job.Upstream{upstreamB})
			upstreamC := job.NewUpstreamResolved("job-C", "", resourceURNC, otherTenant, "static", taskName, false)
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
			alertManager := new(AlertManager)
			alertManager.On("SendJobEvent", mock.Anything).Return()
			defer alertManager.AssertExpectations(t)
			jobService := service.NewJobService(jobRepo, upstreamRepo, downstreamRepo, pluginService, upstreamResolver, tenantDetailsGetter, eventHandler, log, jobDeploymentService, compiler.NewEngine(), nil, nil, alertManager, config.JobValidationConfig{})
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

			pluginService := NewPluginService(t)

			upstreamResolver := new(UpstreamResolver)
			defer upstreamResolver.AssertExpectations(t)

			tenantDetailsGetter := new(TenantDetailsGetter)
			defer tenantDetailsGetter.AssertExpectations(t)

			jobRepo.On("GetAllByTenant", ctx, sampleTenant).Return(nil, errors.New("internal error"))

			jobService := service.NewJobService(jobRepo, upstreamRepo, downstreamRepo, pluginService, upstreamResolver, tenantDetailsGetter, nil, log, nil, compiler.NewEngine(), nil, nil, nil, config.JobValidationConfig{})
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

			pluginService := NewPluginService(t)

			upstreamResolver := new(UpstreamResolver)
			defer upstreamResolver.AssertExpectations(t)

			tenantDetailsGetter := new(TenantDetailsGetter)
			defer tenantDetailsGetter.AssertExpectations(t)

			logWriter := new(mockWriter)
			defer logWriter.AssertExpectations(t)

			jobDeploymentService := new(JobDeploymentService)
			defer jobDeploymentService.AssertExpectations(t)

			eventHandler := newEventHandler(t)

			resourceURNs := []resource.URN{resourceURNA}

			jobService := service.NewJobService(jobRepo, upstreamRepo, downstreamRepo, pluginService, upstreamResolver, tenantDetailsGetter, eventHandler, log, jobDeploymentService, compiler.NewEngine(), nil, nil, nil, config.JobValidationConfig{})

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

			pluginService := NewPluginService(t)

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
			jobADestination := resourceURNA
			jobAUpstreamName := resourceURNB
			jobA := job.NewJob(sampleTenant, specA, jobADestination, []resource.URN{jobAUpstreamName}, false)

			specB, _ := job.NewSpecBuilder(jobVersion, "job-B", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()
			jobBDestination := resourceURNB
			jobB := job.NewJob(sampleTenant, specB, jobBDestination, []resource.URN{resourceURNC}, false)

			resourceURNs := []resource.URN{jobAUpstreamName, resourceURNC}

			jobDownstreams := []*job.Downstream{
				job.NewDownstream(jobA.Spec().Name(), sampleTenant.ProjectName(), sampleTenant.NamespaceName(), jobTask.Name()),
				job.NewDownstream(jobB.Spec().Name(), sampleTenant.ProjectName(), sampleTenant.NamespaceName(), jobTask.Name()),
			}

			downstreamRepo.On("GetDownstreamBySources", ctx, resourceURNs).Return(jobDownstreams, nil)
			jobRepo.On("GetByJobName", ctx, sampleTenant.ProjectName(), jobA.Spec().Name()).Return(jobA, nil)
			jobRepo.On("GetByJobName", ctx, sampleTenant.ProjectName(), jobB.Spec().Name()).Return(jobB, nil)

			tenantDetailsGetter.On("GetDetails", ctx, sampleTenant).Return(detailedTenant, nil)

			pluginService.On("ConstructDestinationURN", ctx, specA.Task().Name().String(), mock.Anything).Return(jobADestination, nil).Once()
			pluginService.On("IdentifyUpstreams", ctx, specA.Task().Name().String(), mock.Anything, mock.Anything).Return([]resource.URN{jobAUpstreamName}, nil)

			pluginService.On("ConstructDestinationURN", ctx, specB.Task().Name().String(), mock.Anything).Return(jobBDestination, nil).Once()
			pluginService.On("IdentifyUpstreams", ctx, specB.Task().Name().String(), mock.Anything, mock.Anything).Return([]resource.URN{resourceURNC}, nil)

			jobRepo.On("Update", ctx, mock.Anything).Return([]*job.Job{jobA, jobB}, nil)

			upstreamB := job.NewUpstreamResolved("job-B", "", resourceURNB, sampleTenant, "static", taskName, false)
			jobAWithUpstream := job.NewWithUpstream(jobA, []*job.Upstream{upstreamB})
			upstreamC := job.NewUpstreamResolved("job-C", "", resourceURNC, sampleTenant, "static", taskName, false)
			jobBWithUpstream := job.NewWithUpstream(jobB, []*job.Upstream{upstreamC})
			upstreamResolver.On("BulkResolve", ctx, project.Name(), []*job.Job{jobA, jobB}, mock.Anything).Return([]*job.WithUpstream{jobAWithUpstream, jobBWithUpstream}, nil)

			upstreamRepo.On("ReplaceUpstreams", ctx, []*job.WithUpstream{jobAWithUpstream, jobBWithUpstream}).Return(nil)

			logWriter.On("Write", mock.Anything, mock.Anything).Return(nil).Times(3)
			eventHandler.On("HandleEvent", mock.Anything).Times(2)

			var jobNamesToRemove []string
			jobNamesToUpload := []string{jobA.GetName(), jobB.GetName()}
			jobDeploymentService.On("UploadJobs", ctx, sampleTenant, jobNamesToUpload, jobNamesToRemove).Return(nil)
			alertManager := new(AlertManager)
			alertManager.On("SendJobEvent", mock.Anything).Return()
			defer alertManager.AssertExpectations(t)
			jobService := service.NewJobService(jobRepo, upstreamRepo, downstreamRepo, pluginService, upstreamResolver, tenantDetailsGetter, eventHandler, log, jobDeploymentService, compiler.NewEngine(), nil, nil, alertManager, config.JobValidationConfig{})

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

			jobService := service.NewJobService(jobRepo, upstreamRepo, downstreamRepo, nil, nil, nil, nil, log, nil, nil, nil, nil, nil, config.JobValidationConfig{})

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
			jobA := job.NewJob(sampleTenant, specA, resourceURNA, []resource.URN{resourceURNB}, false)
			jobRepo.On("GetByJobName", ctx, sampleTenant.ProjectName(), specA.Name()).Return(jobA, nil)

			jobService := service.NewJobService(jobRepo, upstreamRepo, downstreamRepo, nil, nil, nil, nil, log, nil, nil, nil, nil, nil, config.JobValidationConfig{})

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

				jobRepo.On("GetAllByResourceDestination", ctx, resourceURNA).Return(nil, errors.New("error encountered"))

				jobService := service.NewJobService(jobRepo, nil, nil, nil, nil, nil, nil, log, nil, nil, nil, nil, nil, config.JobValidationConfig{})
				actual, err := jobService.GetByFilter(ctx, filter.WithString(filter.ResourceDestination, "bigquery://project:dataset.tableA"))
				assert.Error(t, err, "error encountered")
				assert.Nil(t, actual)
			})
			t.Run("return success", func(t *testing.T) {
				jobRepo := new(JobRepository)
				defer jobRepo.AssertExpectations(t)

				specA, _ := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()
				jobA := job.NewJob(sampleTenant, specA, resourceURNA, []resource.URN{resourceURNB}, false)
				jobRepo.On("GetAllByResourceDestination", ctx, resourceURNA).Return([]*job.Job{jobA}, nil)

				jobService := service.NewJobService(jobRepo, nil, nil, nil, nil, nil, nil, log, nil, nil, nil, nil, nil, config.JobValidationConfig{})
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

				jobService := service.NewJobService(jobRepo, nil, nil, nil, nil, nil, nil, log, nil, nil, nil, nil, nil, config.JobValidationConfig{})
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
				jobA := job.NewJob(sampleTenant, specA, resourceURNA, []resource.URN{resourceURNB}, false)
				specB, _ := job.NewSpecBuilder(jobVersion, "job-B", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()
				jobRepo.On("GetByJobName", ctx, sampleTenant.ProjectName(), specA.Name()).Return(jobA, nil)
				jobRepo.On("GetByJobName", ctx, sampleTenant.ProjectName(), specB.Name()).Return(nil, errors.New("error encountered"))

				jobService := service.NewJobService(jobRepo, nil, nil, nil, nil, nil, nil, log, nil, nil, nil, nil, nil, config.JobValidationConfig{})
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

				jobService := service.NewJobService(jobRepo, nil, nil, nil, nil, nil, nil, log, nil, nil, nil, nil, nil, config.JobValidationConfig{})
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
				jobA := job.NewJob(sampleTenant, specA, resourceURNA, []resource.URN{resourceURNB}, false)
				jobRepo.On("GetByJobName", ctx, sampleTenant.ProjectName(), specA.Name()).Return(jobA, nil)

				jobService := service.NewJobService(jobRepo, nil, nil, nil, nil, nil, nil, log, nil, nil, nil, nil, nil, config.JobValidationConfig{})
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

				jobService := service.NewJobService(jobRepo, nil, nil, nil, nil, nil, nil, log, nil, nil, nil, nil, nil, config.JobValidationConfig{})
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

				jobService := service.NewJobService(jobRepo, nil, nil, nil, nil, nil, nil, log, nil, nil, nil, nil, nil, config.JobValidationConfig{})
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
				jobA := job.NewJob(sampleTenant, specA, resourceURNA, []resource.URN{resourceURNB}, false)
				jobRepo.On("GetByJobName", ctx, sampleTenant.ProjectName(), specA.Name()).Return(jobA, nil)

				jobService := service.NewJobService(jobRepo, nil, nil, nil, nil, nil, nil, log, nil, nil, nil, nil, nil, config.JobValidationConfig{})
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

				jobService := service.NewJobService(jobRepo, nil, nil, nil, nil, nil, nil, log, nil, nil, nil, nil, nil, config.JobValidationConfig{})
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

				jobService := service.NewJobService(jobRepo, nil, nil, nil, nil, nil, nil, log, nil, nil, nil, nil, nil, config.JobValidationConfig{})
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
				jobA := job.NewJob(sampleTenant, specA, resourceURNA, []resource.URN{resourceURNB}, false)
				jobRepo.On("GetAllByTenant", ctx, sampleTenant).Return([]*job.Job{jobA}, nil)

				jobService := service.NewJobService(jobRepo, nil, nil, nil, nil, nil, nil, log, nil, nil, nil, nil, nil, config.JobValidationConfig{})
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

				jobService := service.NewJobService(jobRepo, nil, nil, nil, nil, nil, nil, log, nil, nil, nil, nil, nil, config.JobValidationConfig{})
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
				jobA := job.NewJob(sampleTenant, specA, resourceURNA, []resource.URN{resourceURNB}, false)
				jobRepo.On("GetAllByTenant", ctx, sampleTenant).Return([]*job.Job{jobA}, nil)

				jobService := service.NewJobService(jobRepo, nil, nil, nil, nil, nil, nil, log, nil, nil, nil, nil, nil, config.JobValidationConfig{})
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

				jobService := service.NewJobService(jobRepo, nil, nil, nil, nil, nil, nil, log, nil, nil, nil, nil, nil, config.JobValidationConfig{})
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
				jobA := job.NewJob(sampleTenant, specA, resourceURNA, []resource.URN{resourceURNB}, false)
				jobRepo.On("GetAllByProjectName", ctx, sampleTenant.ProjectName()).Return([]*job.Job{jobA}, nil)

				jobService := service.NewJobService(jobRepo, nil, nil, nil, nil, nil, nil, log, nil, nil, nil, nil, nil, config.JobValidationConfig{})
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
			jobService := service.NewJobService(nil, nil, nil, nil, nil, nil, nil, log, nil, nil, nil, nil, nil, config.JobValidationConfig{})
			actual, err := jobService.GetByFilter(ctx)
			assert.Error(t, err, "no filter matched")
			assert.Nil(t, actual)
		})
	})

	t.Run("GetTaskInfo", func(t *testing.T) {
		t.Run("return error when plugin could not retrieve info", func(t *testing.T) {
			pluginService := NewPluginService(t)

			pluginService.On("Info", ctx, jobTask.Name().String()).Return(nil, errors.New("error encountered"))

			jobService := service.NewJobService(nil, nil, nil, pluginService, nil, nil, nil, nil, nil, nil, nil, nil, nil, config.JobValidationConfig{})

			actual, err := jobService.GetTaskInfo(ctx, jobTask)
			assert.Error(t, err, "error encountered")
			assert.Nil(t, actual)
		})
		t.Run("return task with information included when success", func(t *testing.T) {
			pluginService := NewPluginService(t)

			pluginInfoResp := &plugin.Spec{
				Name:        "bq2bq",
				Description: "plugin desc",
			}
			pluginService.On("Info", ctx, jobTask.Name().String()).Return(pluginInfoResp, nil)

			jobService := service.NewJobService(nil, nil, nil, pluginService, nil, nil, nil, nil, nil, nil, nil, nil, nil, config.JobValidationConfig{})

			actual, err := jobService.GetTaskInfo(ctx, jobTask)
			assert.NoError(t, err)
			assert.NotNil(t, actual)
			assert.Equal(t, pluginInfoResp, actual)
		})
	})

	t.Run("Validate", func(t *testing.T) {
		t.Run("job preparation", func(t *testing.T) {
			t.Run("validate request", func(t *testing.T) {
				t.Run("returns nil and error if job names length is more than zero while job specs length is also more than zero", func(t *testing.T) {
					tenantDetailsGetter := new(TenantDetailsGetter)
					defer tenantDetailsGetter.AssertExpectations(t)

					jobRunInputCompiler := NewJobRunInputCompiler(t)
					resourceExistenceChecker := NewResourceExistenceChecker(t)

					jobService := service.NewJobService(nil, nil, nil, nil, nil, tenantDetailsGetter, nil, log, nil, compiler.NewEngine(), jobRunInputCompiler, resourceExistenceChecker, nil, config.JobValidationConfig{})

					request := dto.ValidateRequest{
						Tenant:       sampleTenant,
						JobSpecs:     []*job.Spec{{}, {}},
						JobNames:     []string{"", ""},
						DeletionMode: false,
					}

					actualResult, actualError := jobService.Validate(ctx, request)

					assert.Nil(t, actualResult)
					assert.ErrorContains(t, actualError, "job names and specs can not be specified together")
				})

				t.Run("returns nil and error if job names and job specs are both empty", func(t *testing.T) {
					tenantDetailsGetter := new(TenantDetailsGetter)
					defer tenantDetailsGetter.AssertExpectations(t)

					jobRunInputCompiler := NewJobRunInputCompiler(t)
					resourceExistenceChecker := NewResourceExistenceChecker(t)

					jobService := service.NewJobService(nil, nil, nil, nil, nil, tenantDetailsGetter, nil, log, nil, compiler.NewEngine(), jobRunInputCompiler, resourceExistenceChecker, nil, config.JobValidationConfig{})

					request := dto.ValidateRequest{
						Tenant:       sampleTenant,
						JobSpecs:     nil,
						JobNames:     nil,
						DeletionMode: false,
					}

					actualResult, actualError := jobService.Validate(ctx, request)

					assert.Nil(t, actualResult)
					assert.ErrorContains(t, actualError, "job names and job specs are both empty")
				})

				t.Run("returns nil and error if using deletion mode while job names length is zero", func(t *testing.T) {
					tenantDetailsGetter := new(TenantDetailsGetter)
					defer tenantDetailsGetter.AssertExpectations(t)

					jobRunInputCompiler := NewJobRunInputCompiler(t)
					resourceExistenceChecker := NewResourceExistenceChecker(t)

					jobService := service.NewJobService(nil, nil, nil, nil, nil, tenantDetailsGetter, nil, log, nil, compiler.NewEngine(), jobRunInputCompiler, resourceExistenceChecker, nil, config.JobValidationConfig{})

					request := dto.ValidateRequest{
						Tenant:       sampleTenant,
						JobSpecs:     []*job.Spec{{}, {}},
						JobNames:     nil,
						DeletionMode: true,
					}

					actualResult, actualError := jobService.Validate(ctx, request)

					assert.Nil(t, actualResult)
					assert.ErrorContains(t, actualError, "deletion job only accepts job names")
				})
			})

			t.Run("validate duplication", func(t *testing.T) {
				t.Run("returns nil and error if job names are duplicated", func(t *testing.T) {
					tenantDetailsGetter := new(TenantDetailsGetter)
					defer tenantDetailsGetter.AssertExpectations(t)

					jobRunInputCompiler := NewJobRunInputCompiler(t)
					resourceExistenceChecker := NewResourceExistenceChecker(t)

					jobService := service.NewJobService(nil, nil, nil, nil, nil, tenantDetailsGetter, nil, log, nil, compiler.NewEngine(), jobRunInputCompiler, resourceExistenceChecker, nil, config.JobValidationConfig{})

					request := dto.ValidateRequest{
						Tenant:       sampleTenant,
						JobSpecs:     nil,
						JobNames:     []string{"job1", "job2", "job1"},
						DeletionMode: false,
					}

					actualResult, actualError := jobService.Validate(ctx, request)

					assert.Nil(t, actualResult)
					assert.ErrorContains(t, actualError, "the following jobs are duplicated: [job1]")
				})

				t.Run("returns nil and error if job specs are duplicated based on name", func(t *testing.T) {
					tenantDetailsGetter := new(TenantDetailsGetter)
					defer tenantDetailsGetter.AssertExpectations(t)

					jobRunInputCompiler := NewJobRunInputCompiler(t)
					resourceExistenceChecker := NewResourceExistenceChecker(t)

					jobService := service.NewJobService(nil, nil, nil, nil, nil, tenantDetailsGetter, nil, log, nil, compiler.NewEngine(), jobRunInputCompiler, resourceExistenceChecker, nil, config.JobValidationConfig{})

					jobSpec1, err := job.NewSpecBuilder(1, "job1", "optimus@goto", jobSchedule, jobWindow, jobTask).Build()
					assert.NoError(t, err)
					jobSpec2, err := job.NewSpecBuilder(1, "job2", "optimus@goto", jobSchedule, jobWindow, jobTask).Build()
					assert.NoError(t, err)
					jobSpec3, err := job.NewSpecBuilder(1, "job1", "optimus@goto", jobSchedule, jobWindow, jobTask).Build() // intentional duplication
					assert.NoError(t, err)

					request := dto.ValidateRequest{
						Tenant:       sampleTenant,
						JobSpecs:     []*job.Spec{jobSpec1, jobSpec2, jobSpec3},
						JobNames:     nil,
						DeletionMode: false,
					}

					actualResult, actualError := jobService.Validate(ctx, request)

					assert.Nil(t, actualResult)
					assert.ErrorContains(t, actualError, "the following jobs are duplicated: [job1]")
				})
			})

			t.Run("returns result and nil if error when getting tenant details", func(t *testing.T) {
				tenantDetailsGetter := new(TenantDetailsGetter)
				defer tenantDetailsGetter.AssertExpectations(t)

				jobRepo := new(JobRepository)
				defer jobRepo.AssertExpectations(t)

				jobRunInputCompiler := NewJobRunInputCompiler(t)
				resourceExistenceChecker := NewResourceExistenceChecker(t)

				jobService := service.NewJobService(jobRepo, nil, nil, nil, nil, tenantDetailsGetter, nil, log, nil, compiler.NewEngine(), jobRunInputCompiler, resourceExistenceChecker, nil, config.JobValidationConfig{})

				tenantDetailsGetter.On("GetDetails", ctx, sampleTenant).Return(nil, errors.New("unexpected error in tenant"))

				request := dto.ValidateRequest{
					Tenant:       sampleTenant,
					JobSpecs:     nil,
					JobNames:     []string{"job1", "job2"},
					DeletionMode: false,
				}

				actualResult, actualError := jobService.Validate(ctx, request)

				assert.Nil(t, actualResult)
				assert.ErrorContains(t, actualError, "unexpected error in tenant")
			})

			t.Run("returns nil and error if one or more of job names are invalid", func(t *testing.T) {
				tenantDetailsGetter := new(TenantDetailsGetter)
				defer tenantDetailsGetter.AssertExpectations(t)

				jobRunInputCompiler := NewJobRunInputCompiler(t)
				resourceExistenceChecker := NewResourceExistenceChecker(t)

				jobService := service.NewJobService(nil, nil, nil, nil, nil, tenantDetailsGetter, nil, log, nil, compiler.NewEngine(), jobRunInputCompiler, resourceExistenceChecker, nil, config.JobValidationConfig{})

				request := dto.ValidateRequest{
					Tenant:       sampleTenant,
					JobSpecs:     nil,
					JobNames:     []string{""}, // intentional empty
					DeletionMode: false,
				}

				tenantDetailsGetter.On("GetDetails", ctx, sampleTenant).Return(detailedTenant, nil)

				actualResult, actualError := jobService.Validate(ctx, request)

				assert.Nil(t, actualResult)
				assert.ErrorContains(t, actualError, "name is empty")
			})

			t.Run("returns nil and error if one or more of job names are error when being fetched from repository", func(t *testing.T) {
				tenantDetailsGetter := new(TenantDetailsGetter)
				defer tenantDetailsGetter.AssertExpectations(t)

				jobRepo := new(JobRepository)
				defer jobRepo.AssertExpectations(t)

				jobRunInputCompiler := NewJobRunInputCompiler(t)
				resourceExistenceChecker := NewResourceExistenceChecker(t)

				jobService := service.NewJobService(jobRepo, nil, nil, nil, nil, tenantDetailsGetter, nil, log, nil, compiler.NewEngine(), jobRunInputCompiler, resourceExistenceChecker, nil, config.JobValidationConfig{})

				request := dto.ValidateRequest{
					Tenant:       sampleTenant,
					JobSpecs:     nil,
					JobNames:     []string{"job1"},
					DeletionMode: false,
				}

				tenantDetailsGetter.On("GetDetails", ctx, sampleTenant).Return(detailedTenant, nil)

				jobRepo.On("GetByJobName", ctx, sampleTenant.ProjectName(), job.Name("job1")).Return(nil, errors.New("unknown repository error"))

				actualResult, actualError := jobService.Validate(ctx, request)

				assert.Nil(t, actualResult)
				assert.ErrorContains(t, actualError, "unknown repository error")
			})

			t.Run("returns result and nil if one or more jobs do not have the same tenant as the root", func(t *testing.T) {
				tenantDetailsGetter := new(TenantDetailsGetter)
				defer tenantDetailsGetter.AssertExpectations(t)

				jobRepo := new(JobRepository)
				defer jobRepo.AssertExpectations(t)

				jobRunInputCompiler := NewJobRunInputCompiler(t)
				resourceExistenceChecker := NewResourceExistenceChecker(t)

				jobService := service.NewJobService(jobRepo, nil, nil, nil, nil, tenantDetailsGetter, nil, log, nil, compiler.NewEngine(), jobRunInputCompiler, resourceExistenceChecker, nil, config.JobValidationConfig{})

				jobSpec1, err := job.NewSpecBuilder(1, "job1", "optimus@goto", jobSchedule, jobWindow, jobTask).Build()
				assert.NoError(t, err)
				jobSpec2, err := job.NewSpecBuilder(1, "job2", "optimus@goto", jobSchedule, jobWindow, jobTask).Build()
				assert.NoError(t, err)

				job1 := job.NewJob(sampleTenant, jobSpec1, resource.ZeroURN(), nil, false)
				job2 := job.NewJob(otherTenant, jobSpec2, resource.ZeroURN(), nil, false) // intentional wrong tenant

				jobRepo.On("GetByJobName", ctx, sampleTenant.ProjectName(), job.Name("job1")).Return(job1, nil)
				jobRepo.On("GetByJobName", ctx, sampleTenant.ProjectName(), job.Name("job2")).Return(job2, nil)

				tenantDetailsGetter.On("GetDetails", ctx, sampleTenant).Return(detailedTenant, nil)

				request := dto.ValidateRequest{
					Tenant:       sampleTenant,
					JobSpecs:     nil,
					JobNames:     []string{"job1", "job2"},
					DeletionMode: false,
				}

				expectedResult := map[job.Name][]dto.ValidateResult{
					"job2": {
						{
							Stage: "tenant validation",
							Messages: []string{
								fmt.Sprintf("current tenant is [%s.%s]", otherTenant.ProjectName(), otherTenant.NamespaceName()),
								fmt.Sprintf("expected tenant is [%s.%s]", sampleTenant.ProjectName(), sampleTenant.NamespaceName()),
							},
							Success: false,
						},
					},
				}

				actualResult, actualError := jobService.Validate(ctx, request)

				assert.EqualValues(t, expectedResult, actualResult)
				assert.NoError(t, actualError)
			})
		})

		t.Run("deletion mode validation", func(t *testing.T) {
			t.Run("returns result and nil if error is encountered when getting downstream jobs", func(t *testing.T) {
				tenantDetailsGetter := new(TenantDetailsGetter)
				defer tenantDetailsGetter.AssertExpectations(t)

				jobRepo := new(JobRepository)
				defer jobRepo.AssertExpectations(t)

				downstreamRepo := new(DownstreamRepository)
				defer downstreamRepo.AssertExpectations(t)

				jobRunInputCompiler := NewJobRunInputCompiler(t)
				resourceExistenceChecker := NewResourceExistenceChecker(t)

				jobService := service.NewJobService(jobRepo, nil, downstreamRepo, nil, nil, tenantDetailsGetter, nil, log, nil, compiler.NewEngine(), jobRunInputCompiler, resourceExistenceChecker, nil, config.JobValidationConfig{})

				jobSpec1, err := job.NewSpecBuilder(1, "job1", "optimus@goto", jobSchedule, jobWindow, jobTask).Build()
				assert.NoError(t, err)
				jobSpec2, err := job.NewSpecBuilder(1, "job2", "optimus@goto", jobSchedule, jobWindow, jobTask).Build()
				assert.NoError(t, err)

				job1 := job.NewJob(sampleTenant, jobSpec1, resource.ZeroURN(), nil, false)
				job2 := job.NewJob(sampleTenant, jobSpec2, resource.ZeroURN(), nil, false)

				jobRepo.On("GetByJobName", ctx, sampleTenant.ProjectName(), job.Name("job1")).Return(job1, nil)
				jobRepo.On("GetByJobName", ctx, sampleTenant.ProjectName(), job.Name("job2")).Return(job2, nil)

				tenantDetailsGetter.On("GetDetails", ctx, sampleTenant).Return(detailedTenant, nil)

				downstreamRepo.On("GetDownstreamByJobName", ctx, sampleTenant.ProjectName(), jobSpec1.Name()).Return([]*job.Downstream{}, nil)
				downstreamRepo.On("GetDownstreamByJobName", ctx, sampleTenant.ProjectName(), jobSpec2.Name()).Return(nil, errors.New("unknown error"))

				request := dto.ValidateRequest{
					Tenant:       sampleTenant,
					JobSpecs:     nil,
					JobNames:     []string{"job1", "job2"},
					DeletionMode: true,
				}

				expectedResult := map[job.Name][]dto.ValidateResult{
					"job1": {
						{
							Stage:    "validation for deletion",
							Messages: []string{"job is safe for deletion"},
							Success:  true,
						},
					},
					"job2": {
						{
							Stage: "validation for deletion",
							Messages: []string{
								"downstreams can not be fetched",
								"unknown error",
							},
							Success: false,
						},
					},
				}

				actualResult, actualError := jobService.Validate(ctx, request)

				assert.EqualValues(t, expectedResult["job1"], actualResult["job1"])
				assert.EqualValues(t, expectedResult["job2"], actualResult["job2"])
				assert.NoError(t, actualError)
			})

			t.Run("returns result and nil if job is safe for deletion", func(t *testing.T) {
				tenantDetailsGetter := new(TenantDetailsGetter)
				defer tenantDetailsGetter.AssertExpectations(t)

				jobRepo := new(JobRepository)
				defer jobRepo.AssertExpectations(t)

				downstreamRepo := new(DownstreamRepository)
				defer downstreamRepo.AssertExpectations(t)

				jobRunInputCompiler := NewJobRunInputCompiler(t)
				resourceExistenceChecker := NewResourceExistenceChecker(t)

				jobService := service.NewJobService(jobRepo, nil, downstreamRepo, nil, nil, tenantDetailsGetter, nil, log, nil, compiler.NewEngine(), jobRunInputCompiler, resourceExistenceChecker, nil, config.JobValidationConfig{})

				jobSpec1, err := job.NewSpecBuilder(1, "job1", "optimus@goto", jobSchedule, jobWindow, jobTask).Build()
				assert.NoError(t, err)
				jobSpec2, err := job.NewSpecBuilder(1, "job2", "optimus@goto", jobSchedule, jobWindow, jobTask).Build()
				assert.NoError(t, err)

				job1 := job.NewJob(sampleTenant, jobSpec1, resource.ZeroURN(), nil, false)
				job2 := job.NewJob(sampleTenant, jobSpec2, resource.ZeroURN(), nil, false)

				jobRepo.On("GetByJobName", ctx, sampleTenant.ProjectName(), job.Name("job1")).Return(job1, nil)
				jobRepo.On("GetByJobName", ctx, sampleTenant.ProjectName(), job.Name("job2")).Return(job2, nil)

				tenantDetailsGetter.On("GetDetails", ctx, sampleTenant).Return(detailedTenant, nil)

				downstreamRepo.On("GetDownstreamByJobName", ctx, sampleTenant.ProjectName(), jobSpec1.Name()).Return([]*job.Downstream{}, nil)
				downstreamRepo.On("GetDownstreamByJobName", ctx, sampleTenant.ProjectName(), jobSpec2.Name()).Return([]*job.Downstream{}, nil)

				request := dto.ValidateRequest{
					Tenant:       sampleTenant,
					JobSpecs:     nil,
					JobNames:     []string{"job1", "job2"},
					DeletionMode: true,
				}

				expectedResult := map[job.Name][]dto.ValidateResult{
					"job1": {
						{
							Stage:    "validation for deletion",
							Messages: []string{"job is safe for deletion"},
							Success:  true,
						},
					},
					"job2": {
						{
							Stage:    "validation for deletion",
							Messages: []string{"job is safe for deletion"},
							Success:  true,
						},
					},
				}

				actualResult, actualError := jobService.Validate(ctx, request)

				assert.EqualValues(t, expectedResult["job1"], actualResult["job1"])
				assert.EqualValues(t, expectedResult["job2"], actualResult["job2"])
				assert.NoError(t, actualError)
			})

			t.Run("returns result and nil if job is not safe for deletion", func(t *testing.T) {
				tenantDetailsGetter := new(TenantDetailsGetter)
				defer tenantDetailsGetter.AssertExpectations(t)

				jobRepo := new(JobRepository)
				defer jobRepo.AssertExpectations(t)

				downstreamRepo := new(DownstreamRepository)
				defer downstreamRepo.AssertExpectations(t)

				jobRunInputCompiler := NewJobRunInputCompiler(t)
				resourceExistenceChecker := NewResourceExistenceChecker(t)

				jobService := service.NewJobService(jobRepo, nil, downstreamRepo, nil, nil, tenantDetailsGetter, nil, log, nil, compiler.NewEngine(), jobRunInputCompiler, resourceExistenceChecker, nil, config.JobValidationConfig{})

				jobSpec1, err := job.NewSpecBuilder(1, "job1", "optimus@goto", jobSchedule, jobWindow, jobTask).Build()
				assert.NoError(t, err)
				jobSpec2, err := job.NewSpecBuilder(1, "job2", "optimus@goto", jobSchedule, jobWindow, jobTask).Build()
				assert.NoError(t, err)

				job1 := job.NewJob(sampleTenant, jobSpec1, resource.ZeroURN(), nil, false)
				job2 := job.NewJob(sampleTenant, jobSpec2, resource.ZeroURN(), nil, false)
				job2Downstream := job.NewDownstream("job3", sampleTenant.ProjectName(), sampleTenant.NamespaceName(), taskName)

				jobRepo.On("GetByJobName", ctx, sampleTenant.ProjectName(), job.Name("job1")).Return(job1, nil)
				jobRepo.On("GetByJobName", ctx, sampleTenant.ProjectName(), job.Name("job2")).Return(job2, nil)

				tenantDetailsGetter.On("GetDetails", ctx, sampleTenant).Return(detailedTenant, nil)

				downstreamRepo.On("GetDownstreamByJobName", ctx, sampleTenant.ProjectName(), jobSpec1.Name()).Return([]*job.Downstream{}, nil)
				downstreamRepo.On("GetDownstreamByJobName", ctx, sampleTenant.ProjectName(), jobSpec2.Name()).Return([]*job.Downstream{job2Downstream}, nil)
				downstreamRepo.On("GetDownstreamByJobName", ctx, sampleTenant.ProjectName(), job2Downstream.Name()).Return([]*job.Downstream{}, nil)

				request := dto.ValidateRequest{
					Tenant:       sampleTenant,
					JobSpecs:     nil,
					JobNames:     []string{"job1", "job2"},
					DeletionMode: true,
				}

				expectedResult := map[job.Name][]dto.ValidateResult{
					"job1": {
						{
							Stage:    "validation for deletion",
							Messages: []string{"job is safe for deletion"},
							Success:  true,
						},
					},
					"job2": {
						{
							Stage:    "validation for deletion",
							Messages: []string{"job is not safe for deletion", "failed precondition for entity job: job is being used by test-proj/job3"},
							Success:  false,
						},
					},
				}

				actualResult, actualError := jobService.Validate(ctx, request)

				assert.EqualValues(t, expectedResult["job1"], actualResult["job1"])
				assert.EqualValues(t, expectedResult["job2"], actualResult["job2"])
				assert.NoError(t, actualError)
			})

			t.Run("show only job's direct dependencies when the job has multiple-level dependencies", func(t *testing.T) {
				// testcase for case A -> B -> C, and A & B are the jobs to be deleted
				// when showing direct downstream,
				tenantDetailsGetter := new(TenantDetailsGetter)
				defer tenantDetailsGetter.AssertExpectations(t)

				jobRepo := new(JobRepository)
				defer jobRepo.AssertExpectations(t)

				downstreamRepo := new(DownstreamRepository)
				defer downstreamRepo.AssertExpectations(t)

				jobRunInputCompiler := NewJobRunInputCompiler(t)
				resourceExistenceChecker := NewResourceExistenceChecker(t)

				jobService := service.NewJobService(jobRepo, nil, downstreamRepo, nil, nil, tenantDetailsGetter, nil, log, nil, compiler.NewEngine(), jobRunInputCompiler, resourceExistenceChecker, nil, config.JobValidationConfig{})

				jobSpec1, err := job.NewSpecBuilder(1, "job1", "optimus@goto", jobSchedule, jobWindow, jobTask).Build()
				assert.NoError(t, err)
				jobSpec2, err := job.NewSpecBuilder(1, "job2", "optimus@goto", jobSchedule, jobWindow, jobTask).Build()
				assert.NoError(t, err)
				jobSpec3, err := job.NewSpecBuilder(1, "job3", "optimus@goto", jobSchedule, jobWindow, jobTask).Build()
				assert.NoError(t, err)

				job1 := job.NewJob(sampleTenant, jobSpec1, resource.ZeroURN(), nil, false)
				job1Downstream := job.NewDownstream(jobSpec2.Name(), sampleTenant.ProjectName(), sampleTenant.NamespaceName(), taskName)
				job2Downstream := job.NewDownstream(jobSpec3.Name(), sampleTenant.ProjectName(), sampleTenant.NamespaceName(), taskName)

				jobRepo.On("GetByJobName", ctx, sampleTenant.ProjectName(), job.Name("job1")).Return(job1, nil)

				tenantDetailsGetter.On("GetDetails", ctx, sampleTenant).Return(detailedTenant, nil)

				downstreamRepo.On("GetDownstreamByJobName", ctx, sampleTenant.ProjectName(), jobSpec1.Name()).Return([]*job.Downstream{job1Downstream}, nil)
				downstreamRepo.On("GetDownstreamByJobName", ctx, sampleTenant.ProjectName(), jobSpec2.Name()).Return([]*job.Downstream{job2Downstream}, nil)
				downstreamRepo.On("GetDownstreamByJobName", ctx, sampleTenant.ProjectName(), jobSpec3.Name()).Return([]*job.Downstream{}, nil)

				request := dto.ValidateRequest{
					Tenant:       sampleTenant,
					JobSpecs:     nil,
					JobNames:     []string{"job1"},
					DeletionMode: true,
				}

				expectedResult := map[job.Name][]dto.ValidateResult{
					"job1": {
						{
							Stage:    "validation for deletion",
							Messages: []string{"job is not safe for deletion", "failed precondition for entity job: job is being used by test-proj/job2"},
							Success:  false,
						},
					},
				}

				actualResult, actualError := jobService.Validate(ctx, request)

				assert.EqualValues(t, expectedResult["job1"], actualResult["job1"])
				assert.NoError(t, actualError)
			})

			t.Run("show each deleted jobs' direct downstream if 2 jobs to be deleted has a common remaining dependency", func(t *testing.T) {
				// testcase for case A -> B -> C, and A & B are the jobs to be deleted
				// when showing direct downstream,
				tenantDetailsGetter := new(TenantDetailsGetter)
				defer tenantDetailsGetter.AssertExpectations(t)

				jobRepo := new(JobRepository)
				defer jobRepo.AssertExpectations(t)

				downstreamRepo := new(DownstreamRepository)
				defer downstreamRepo.AssertExpectations(t)

				jobRunInputCompiler := NewJobRunInputCompiler(t)
				resourceExistenceChecker := NewResourceExistenceChecker(t)

				jobService := service.NewJobService(jobRepo, nil, downstreamRepo, nil, nil, tenantDetailsGetter, nil, log, nil, compiler.NewEngine(), jobRunInputCompiler, resourceExistenceChecker, nil, config.JobValidationConfig{})

				jobSpec1, err := job.NewSpecBuilder(1, "job1", "optimus@goto", jobSchedule, jobWindow, jobTask).Build()
				assert.NoError(t, err)
				jobSpec2, err := job.NewSpecBuilder(1, "job2", "optimus@goto", jobSchedule, jobWindow, jobTask).Build()
				assert.NoError(t, err)
				jobSpec3, err := job.NewSpecBuilder(1, "job3", "optimus@goto", jobSchedule, jobWindow, jobTask).Build()
				assert.NoError(t, err)

				job1 := job.NewJob(sampleTenant, jobSpec1, resource.ZeroURN(), nil, false)
				job2 := job.NewJob(sampleTenant, jobSpec2, resource.ZeroURN(), nil, false)
				job1Downstream := job.NewDownstream(jobSpec2.Name(), sampleTenant.ProjectName(), sampleTenant.NamespaceName(), taskName)
				job2Downstream := job.NewDownstream(jobSpec3.Name(), sampleTenant.ProjectName(), sampleTenant.NamespaceName(), taskName)

				jobRepo.On("GetByJobName", ctx, sampleTenant.ProjectName(), job.Name("job1")).Return(job1, nil)
				jobRepo.On("GetByJobName", ctx, sampleTenant.ProjectName(), job.Name("job2")).Return(job2, nil)

				tenantDetailsGetter.On("GetDetails", ctx, sampleTenant).Return(detailedTenant, nil)

				downstreamRepo.On("GetDownstreamByJobName", ctx, sampleTenant.ProjectName(), jobSpec1.Name()).Return([]*job.Downstream{job1Downstream}, nil)
				downstreamRepo.On("GetDownstreamByJobName", ctx, sampleTenant.ProjectName(), jobSpec2.Name()).Return([]*job.Downstream{job2Downstream}, nil)
				downstreamRepo.On("GetDownstreamByJobName", ctx, sampleTenant.ProjectName(), jobSpec3.Name()).Return([]*job.Downstream{}, nil)

				request := dto.ValidateRequest{
					Tenant:       sampleTenant,
					JobSpecs:     nil,
					JobNames:     []string{"job1", "job2"},
					DeletionMode: true,
				}

				expectedResult := map[job.Name][]dto.ValidateResult{
					"job1": {
						{
							Stage:    "validation for deletion",
							Messages: []string{"job is not safe for deletion", "failed precondition for entity job: job is being used by test-proj/job2"},
							Success:  false,
						},
					},
					"job2": {
						{
							Stage:    "validation for deletion",
							Messages: []string{"job is not safe for deletion", "failed precondition for entity job: job is being used by test-proj/job3"},
							Success:  false,
						},
					},
				}

				actualResult, actualError := jobService.Validate(ctx, request)

				assert.EqualValues(t, expectedResult["job1"], actualResult["job1"])
				assert.EqualValues(t, expectedResult["job2"], actualResult["job2"])
				assert.NoError(t, actualError)
			})
		})

		t.Run("non-deletion mode validation", func(t *testing.T) {
			t.Run("validate cyclic", func(t *testing.T) {
				t.Run("returns result and nil if cyclic dependency is detected", func(t *testing.T) {
					tenantDetailsGetter := new(TenantDetailsGetter)
					defer tenantDetailsGetter.AssertExpectations(t)

					jobRepo := new(JobRepository)
					defer jobRepo.AssertExpectations(t)

					upstreamResolver := new(UpstreamResolver)
					defer upstreamResolver.AssertExpectations(t)

					downstreamRepo := new(DownstreamRepository)
					defer downstreamRepo.AssertExpectations(t)

					jobRunInputCompiler := NewJobRunInputCompiler(t)
					resourceExistenceChecker := NewResourceExistenceChecker(t)

					jobService := service.NewJobService(jobRepo, nil, downstreamRepo, nil, upstreamResolver, tenantDetailsGetter, nil, log, nil, compiler.NewEngine(), jobRunInputCompiler, resourceExistenceChecker, nil, config.JobValidationConfig{})

					jobSpecA, err := job.NewSpecBuilder(1, "jobA", "optimus@goto", jobSchedule, jobWindow, jobTask).Build()
					assert.NoError(t, err)
					jobSpecB, err := job.NewSpecBuilder(1, "jobB", "optimus@goto", jobSchedule, jobWindow, jobTask).Build()
					assert.NoError(t, err)
					jobSpecC, err := job.NewSpecBuilder(1, "jobC", "optimus@goto", jobSchedule, jobWindow, jobTask).Build()
					assert.NoError(t, err)

					jobA := job.NewJob(sampleTenant, jobSpecA, resourceURNA, []resource.URN{resourceURNC}, false)
					jobB := job.NewJob(sampleTenant, jobSpecB, resourceURNB, []resource.URN{resourceURNA}, false)
					jobC := job.NewJob(sampleTenant, jobSpecC, resourceURNC, []resource.URN{resourceURNB}, false)

					upstreamB := job.NewUpstreamResolved(jobSpecB.Name(), "", resourceURNB, sampleTenant, "static", taskName, false)
					jobAWithUpstream := job.NewWithUpstream(jobA, []*job.Upstream{upstreamB})
					upstreamC := job.NewUpstreamResolved(jobSpecC.Name(), "", resourceURNC, sampleTenant, "static", taskName, false)
					jobBWithUpstream := job.NewWithUpstream(jobB, []*job.Upstream{upstreamC})
					upstreamA := job.NewUpstreamResolved(jobSpecA.Name(), "", resourceURNA, sampleTenant, "static", taskName, false)
					jobCWithUpstream := job.NewWithUpstream(jobC, []*job.Upstream{upstreamA})

					jobRepo.On("GetAllByTenant", ctx, sampleTenant).Return([]*job.Job{jobA, jobB, jobC}, nil)

					jobRepo.On("GetByJobName", ctx, sampleTenant.ProjectName(), job.Name("jobA")).Return(jobA, nil)
					jobRepo.On("GetByJobName", ctx, sampleTenant.ProjectName(), job.Name("jobB")).Return(jobB, nil)
					jobRepo.On("GetByJobName", ctx, sampleTenant.ProjectName(), job.Name("jobC")).Return(jobC, nil)

					upstreamResolver.On("CheckStaticResolvable", ctx, sampleTenant, mock.Anything, mock.Anything).Return(nil)
					upstreamResolver.On("BulkResolve", ctx, sampleTenant.ProjectName(), mock.Anything, mock.Anything).Return([]*job.WithUpstream{jobAWithUpstream, jobBWithUpstream, jobCWithUpstream}, nil)

					tenantDetailsGetter.On("GetDetails", ctx, sampleTenant).Return(detailedTenant, nil)

					request := dto.ValidateRequest{
						Tenant:       sampleTenant,
						JobSpecs:     nil,
						JobNames:     []string{"jobA", "jobB", "jobC"},
						DeletionMode: false,
					}

					expectedResult := map[job.Name][]dto.ValidateResult{
						"jobA": {
							{
								Stage:    "cyclic validation",
								Messages: []string{"cyclic dependency is detected", "jobA", "jobC", "jobB", "jobA"},
								Success:  false,
							},
						},
						"jobB": {
							{
								Stage:    "cyclic validation",
								Messages: []string{"cyclic dependency is detected", "jobA", "jobC", "jobB", "jobA"},
								Success:  false,
							},
						},
						"jobC": {
							{
								Stage:    "cyclic validation",
								Messages: []string{"cyclic dependency is detected", "jobA", "jobC", "jobB", "jobA"},
								Success:  false,
							},
						},
					}

					actualResult, actualError := jobService.Validate(ctx, request)

					assert.EqualValues(t, expectedResult["jobA"], actualResult["jobA"])
					assert.EqualValues(t, expectedResult["jobB"], actualResult["jobB"])
					assert.EqualValues(t, expectedResult["jobC"], actualResult["jobC"])
					assert.NoError(t, actualError)
				})
			})

			t.Run("validate jobs", func(t *testing.T) {
				t.Run("returns result and nil if error is encountered when generating destination urn", func(t *testing.T) {
					tenantDetailsGetter := new(TenantDetailsGetter)
					defer tenantDetailsGetter.AssertExpectations(t)

					jobRepo := new(JobRepository)
					defer jobRepo.AssertExpectations(t)

					upstreamResolver := new(UpstreamResolver)
					defer upstreamResolver.AssertExpectations(t)

					downstreamRepo := new(DownstreamRepository)
					defer downstreamRepo.AssertExpectations(t)

					pluginService := NewPluginService(t)

					jobRunInputCompiler := NewJobRunInputCompiler(t)
					resourceExistenceChecker := NewResourceExistenceChecker(t)

					jobService := service.NewJobService(jobRepo, nil, downstreamRepo, pluginService, upstreamResolver, tenantDetailsGetter, nil, log, nil, compiler.NewEngine(), jobRunInputCompiler, resourceExistenceChecker, nil, config.JobValidationConfig{})

					jobSpecA, err := job.NewSpecBuilder(1, "jobA", "optimus@goto", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()
					assert.NoError(t, err)
					jobSpecB, err := job.NewSpecBuilder(1, "jobB", "optimus@goto", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()
					assert.NoError(t, err)

					jobA := job.NewJob(sampleTenant, jobSpecA, resourceURNA, []resource.URN{resourceURNC}, false)
					jobB := job.NewJob(sampleTenant, jobSpecB, resourceURNB, []resource.URN{resourceURNA}, false)

					upstreamB := job.NewUpstreamResolved(jobSpecB.Name(), "", resourceURNB, sampleTenant, "static", taskName, false)
					jobAWithUpstream := job.NewWithUpstream(jobA, []*job.Upstream{upstreamB})
					jobBWithUpstream := job.NewWithUpstream(jobB, []*job.Upstream{})

					jobRepo.On("GetByJobName", ctx, sampleTenant.ProjectName(), job.Name("jobA")).Return(jobA, nil)
					jobRepo.On("GetByJobName", ctx, sampleTenant.ProjectName(), job.Name("jobB")).Return(jobB, nil)
					jobRepo.On("GetAllByTenant", ctx, sampleTenant).Return([]*job.Job{jobA, jobB}, nil)

					tenantDetailsGetter.On("GetDetails", ctx, sampleTenant).Return(detailedTenant, nil)

					upstreamResolver.On("CheckStaticResolvable", ctx, sampleTenant, mock.Anything, mock.Anything).Return(nil)
					upstreamResolver.On("BulkResolve", ctx, sampleTenant.ProjectName(), mock.Anything, mock.Anything).Return([]*job.WithUpstream{jobAWithUpstream, jobBWithUpstream}, nil)

					pluginService.On("ConstructDestinationURN", ctx, jobTask.Name().String(), mock.Anything).Return(resource.ZeroURN(), errors.New("unknown error")).Twice()

					request := dto.ValidateRequest{
						Tenant:       sampleTenant,
						JobSpecs:     nil,
						JobNames:     []string{"jobA", "jobB"},
						DeletionMode: false,
					}

					expectedResult := map[job.Name][]dto.ValidateResult{
						"jobA": {
							{
								Stage:    "destination validation",
								Messages: []string{"can not generate destination resource", "unknown error"},
								Success:  false,
							},
						},
						"jobB": {
							{
								Stage:    "destination validation",
								Messages: []string{"can not generate destination resource", "unknown error"},
								Success:  false,
							},
						},
					}

					actualResult, actualError := jobService.Validate(ctx, request)

					assert.EqualValues(t, expectedResult["jobA"], actualResult["jobA"])
					assert.EqualValues(t, expectedResult["jobB"], actualResult["jobB"])
					assert.NoError(t, actualError)
				})

				t.Run("returns unsuccessful result and nil if one or more validations failed", func(t *testing.T) {
					tenantDetailsGetter := new(TenantDetailsGetter)
					defer tenantDetailsGetter.AssertExpectations(t)

					jobRepo := new(JobRepository)
					defer jobRepo.AssertExpectations(t)

					downstreamRepo := new(DownstreamRepository)
					defer downstreamRepo.AssertExpectations(t)

					upstreamResolver := new(UpstreamResolver)
					defer upstreamResolver.AssertExpectations(t)

					pluginService := NewPluginService(t)

					jobRunInputCompiler := NewJobRunInputCompiler(t)
					resourceExistenceChecker := NewResourceExistenceChecker(t)

					jobService := service.NewJobService(jobRepo, nil, downstreamRepo,
						pluginService, upstreamResolver, tenantDetailsGetter, nil,
						log, nil, compiler.NewEngine(),
						jobRunInputCompiler, resourceExistenceChecker, nil, config.JobValidationConfig{},
					)

					resourceURND, err := resource.ParseURN("bigquery://project:dataset.tableD")
					assert.NoError(t, err)

					jobSpecA, err := job.NewSpecBuilder(1, "jobA", "optimus@goto", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()
					assert.NoError(t, err)
					jobSpecB, err := job.NewSpecBuilder(1, "jobB", "optimus@goto", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()
					assert.NoError(t, err)

					jobA := job.NewJob(sampleTenant, jobSpecA, resourceURNA, []resource.URN{resourceURNC}, false)
					jobB := job.NewJob(sampleTenant, jobSpecB, resourceURNB, []resource.URN{resourceURNA}, false)

					upstreamB := job.NewUpstreamResolved(jobSpecB.Name(), "", resourceURNB, sampleTenant, "static", taskName, false)
					jobAWithUpstream := job.NewWithUpstream(jobA, []*job.Upstream{upstreamB})
					jobBWithUpstream := job.NewWithUpstream(jobB, []*job.Upstream{})

					tenantDetailsGetter.On("GetDetails", ctx, sampleTenant).Return(detailedTenant, nil)

					pluginService.On("Info", ctx, jobTask.Name().String()).Return(&pluginSpec, nil)
					pluginService.On("ConstructDestinationURN", ctx, jobTask.Name().String(), mock.Anything).Return(resource.ZeroURN(), nil)
					sourcesToValidate := []resource.URN{resourceURNA, resourceURNB, resourceURNC, resourceURND}
					pluginService.On("IdentifyUpstreams", ctx, jobTask.Name().String(), mock.Anything, mock.Anything).Return(sourcesToValidate, nil)

					jobRunInputCompiler.On("Compile", ctx, mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("unexpected compile error"))

					resourceExistenceChecker.On("GetByURN", ctx, sampleTenant, resourceURNA).Return(nil, errors.New("unexpected get by urn error"))
					resourceExistenceChecker.On("ExistInStore", ctx, sampleTenant, resourceURNA).Return(false, errors.New("unexpected exist in store error"))

					rsc, err := resource.NewResource("resource_1", "table", resource.Bigquery, sampleTenant, &resource.Metadata{Description: "table for test"}, map[string]any{"version": 1})
					assert.NoError(t, err)
					deletedRsc := resource.FromExisting(rsc, resource.ReplaceStatus(resource.StatusDeleted))

					jobRepo.On("GetAllByTenant", ctx, sampleTenant).Return([]*job.Job{jobA, jobB}, nil)

					resourceExistenceChecker.On("GetByURN", ctx, sampleTenant, resourceURNB).Return(deletedRsc, nil)
					resourceExistenceChecker.On("ExistInStore", ctx, sampleTenant, resourceURNB).Return(false, nil)

					resourceExistenceChecker.On("GetByURN", ctx, sampleTenant, resourceURNC).Return(rsc, nil)
					resourceExistenceChecker.On("ExistInStore", ctx, sampleTenant, resourceURNC).Return(false, nil)

					resourceExistenceChecker.On("GetByURN", ctx, sampleTenant, resourceURND).Return(rsc, nil)
					resourceExistenceChecker.On("ExistInStore", ctx, sampleTenant, resourceURND).Return(true, nil)

					upstreamResolver.On("CheckStaticResolvable", ctx, sampleTenant, mock.Anything, mock.Anything).Return(nil)
					upstreamResolver.On("BulkResolve", ctx, sampleTenant.ProjectName(), mock.Anything, mock.Anything).Return([]*job.WithUpstream{jobAWithUpstream, jobBWithUpstream}, nil)

					upstreamResolver.On("Resolve", ctx, mock.Anything, mock.Anything).Return(nil, errors.New("unexpected errors in upstream resolve")).Once()
					upstreamResolver.On("Resolve", ctx, mock.Anything, mock.Anything).Return(nil, nil).Once()

					request := dto.ValidateRequest{
						Tenant:       sampleTenant,
						JobSpecs:     []*job.Spec{jobSpecA, jobSpecB},
						JobNames:     nil,
						DeletionMode: false,
					}

					expectedResult := map[job.Name][]dto.ValidateResult{
						"jobA": {
							{
								Stage:    "destination validation",
								Messages: []string{"no issue"},
								Success:  true,
							},
							{
								Stage: "source validation",
								Messages: []string{
									"bigquery://project:dataset.tableA: unexpected exist in store error",
									"bigquery://project:dataset.tableB: resource does not exist in both db and store",
									"bigquery://project:dataset.tableC: resource exists in db but not in store",
									"bigquery://project:dataset.tableD: no issue",
								},
								Success: false,
							},
							{
								Stage:    "window validation",
								Messages: []string{"no issue"},
								Success:  true,
							},
							{
								Stage:    "plugin validation",
								Messages: []string{"no issue"},
								Success:  true,
							},
							{
								Stage: "compile validation for run",
								Messages: []string{
									"compiling [bq2bq] with type [task] failed with error: unexpected compile error",
								},
								Success: false,
							},
							{
								Stage: "upstream validation",
								Messages: []string{
									"can not resolve upstream",
									"unexpected errors in upstream resolve",
								},
								Success: false,
							},
						},
						"jobB": {
							{
								Stage:    "destination validation",
								Messages: []string{"no issue"},
								Success:  true,
							},
							{
								Stage: "source validation",
								Messages: []string{
									"bigquery://project:dataset.tableA: unexpected exist in store error",
									"bigquery://project:dataset.tableB: resource does not exist in both db and store",
									"bigquery://project:dataset.tableC: resource exists in db but not in store",
									"bigquery://project:dataset.tableD: no issue",
								},
								Success: false,
							},
							{
								Stage:    "window validation",
								Messages: []string{"no issue"},
								Success:  true,
							},
							{
								Stage:    "plugin validation",
								Messages: []string{"no issue"},
								Success:  true,
							},
							{
								Stage: "compile validation for run",
								Messages: []string{
									"compiling [bq2bq] with type [task] failed with error: unexpected compile error",
								},
								Success: false,
							},
							{
								Stage:    "upstream validation",
								Messages: []string{"no issue"},
								Success:  true,
							},
						},
					}

					actualResult, actualError := jobService.Validate(ctx, request)

					assert.EqualValues(t, expectedResult["jobA"], actualResult["jobA"])
					assert.EqualValues(t, expectedResult["jobB"], actualResult["jobB"])
					assert.NoError(t, actualError)
				})

				t.Run("returns successful result and nil if no validation failed", func(t *testing.T) {
					tenantDetailsGetter := new(TenantDetailsGetter)
					defer tenantDetailsGetter.AssertExpectations(t)

					jobRepo := new(JobRepository)
					defer jobRepo.AssertExpectations(t)

					downstreamRepo := new(DownstreamRepository)
					defer downstreamRepo.AssertExpectations(t)

					upstreamResolver := new(UpstreamResolver)
					defer upstreamResolver.AssertExpectations(t)

					pluginService := NewPluginService(t)

					jobRunInputCompiler := NewJobRunInputCompiler(t)
					resourceExistenceChecker := NewResourceExistenceChecker(t)

					jobService := service.NewJobService(jobRepo, nil, downstreamRepo,
						pluginService, upstreamResolver, tenantDetailsGetter, nil,
						log, nil, compiler.NewEngine(),
						jobRunInputCompiler, resourceExistenceChecker, nil, config.JobValidationConfig{},
					)

					resourceURND, err := resource.ParseURN("bigquery://project:dataset.tableD")
					assert.NoError(t, err)

					jobSpecA, err := job.NewSpecBuilder(1, "jobA", "optimus@goto", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()
					assert.NoError(t, err)
					jobSpecB, err := job.NewSpecBuilder(1, "jobB", "optimus@goto", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()
					assert.NoError(t, err)

					jobA := job.NewJob(sampleTenant, jobSpecA, resourceURNA, []resource.URN{resourceURNC}, false)
					jobB := job.NewJob(sampleTenant, jobSpecB, resourceURNB, []resource.URN{resourceURNA}, false)

					upstreamB := job.NewUpstreamResolved(jobSpecB.Name(), "", resourceURNB, sampleTenant, "static", taskName, false)
					jobAWithUpstream := job.NewWithUpstream(jobA, []*job.Upstream{upstreamB})
					jobBWithUpstream := job.NewWithUpstream(jobB, []*job.Upstream{})

					jobRepo.On("GetAllByTenant", ctx, sampleTenant).Return([]*job.Job{jobA, jobB}, nil)

					tenantDetailsGetter.On("GetDetails", ctx, sampleTenant).Return(detailedTenant, nil)

					pluginService.On("Info", ctx, jobTask.Name().String()).Return(&pluginSpec, nil)
					pluginService.On("ConstructDestinationURN", ctx, jobTask.Name().String(), mock.Anything).Return(resource.ZeroURN(), nil)
					sourcesToValidate := []resource.URN{resourceURNA, resourceURNB, resourceURNC, resourceURND}
					pluginService.On("IdentifyUpstreams", ctx, jobTask.Name().String(), mock.Anything, mock.Anything).Return(sourcesToValidate, nil)

					jobRunInputCompiler.On("Compile", ctx, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)

					rsc, err := resource.NewResource("resource_1", "table", resource.Bigquery, sampleTenant, &resource.Metadata{Description: "table for test"}, map[string]any{"version": 1})
					assert.NoError(t, err)

					resourceExistenceChecker.On("GetByURN", ctx, sampleTenant, resourceURNA).Return(rsc, nil)
					resourceExistenceChecker.On("ExistInStore", ctx, sampleTenant, resourceURNA).Return(true, nil)

					resourceExistenceChecker.On("GetByURN", ctx, sampleTenant, resourceURNB).Return(rsc, nil)
					resourceExistenceChecker.On("ExistInStore", ctx, sampleTenant, resourceURNB).Return(true, nil)

					resourceExistenceChecker.On("GetByURN", ctx, sampleTenant, resourceURNC).Return(rsc, nil)
					resourceExistenceChecker.On("ExistInStore", ctx, sampleTenant, resourceURNC).Return(true, nil)

					upstreamResolver.On("CheckStaticResolvable", ctx, sampleTenant, mock.Anything, mock.Anything).Return(nil)
					upstreamResolver.On("BulkResolve", ctx, sampleTenant.ProjectName(), mock.Anything, mock.Anything).Return([]*job.WithUpstream{jobAWithUpstream, jobBWithUpstream}, nil)

					resourceExistenceChecker.On("GetByURN", ctx, sampleTenant, resourceURND).Return(rsc, nil)
					resourceExistenceChecker.On("ExistInStore", ctx, sampleTenant, resourceURND).Return(true, nil)

					upstreamResolver.On("Resolve", ctx, mock.Anything, mock.Anything).Return(nil, nil)

					request := dto.ValidateRequest{
						Tenant:       sampleTenant,
						JobSpecs:     []*job.Spec{jobSpecA, jobSpecB},
						JobNames:     nil,
						DeletionMode: false,
					}

					expectedResult := map[job.Name][]dto.ValidateResult{
						"jobA": {
							{
								Stage:    "destination validation",
								Messages: []string{"no issue"},
								Success:  true,
							},
							{
								Stage: "source validation",
								Messages: []string{
									"bigquery://project:dataset.tableA: no issue",
									"bigquery://project:dataset.tableB: no issue",
									"bigquery://project:dataset.tableC: no issue",
									"bigquery://project:dataset.tableD: no issue",
								},
								Success: true,
							},
							{
								Stage:    "window validation",
								Messages: []string{"no issue"},
								Success:  true,
							},
							{
								Stage:    "plugin validation",
								Messages: []string{"no issue"},
								Success:  true,
							},
							{
								Stage:    "compile validation for run",
								Messages: []string{"compiling [bq2bq] with type [task] contains no issue"},
								Success:  true,
							},
							{
								Stage:    "upstream validation",
								Messages: []string{"no issue"},
								Success:  true,
							},
						},
						"jobB": {
							{
								Stage:    "destination validation",
								Messages: []string{"no issue"},
								Success:  true,
							},
							{
								Stage: "source validation",
								Messages: []string{
									"bigquery://project:dataset.tableA: no issue",
									"bigquery://project:dataset.tableB: no issue",
									"bigquery://project:dataset.tableC: no issue",
									"bigquery://project:dataset.tableD: no issue",
								},
								Success: true,
							},
							{
								Stage:    "window validation",
								Messages: []string{"no issue"},
								Success:  true,
							},
							{
								Stage:    "plugin validation",
								Messages: []string{"no issue"},
								Success:  true,
							},
							{
								Stage:    "compile validation for run",
								Messages: []string{"compiling [bq2bq] with type [task] contains no issue"},
								Success:  true,
							},
							{
								Stage:    "upstream validation",
								Messages: []string{"no issue"},
								Success:  true,
							},
						},
					}

					actualResult, actualError := jobService.Validate(ctx, request)

					assert.EqualValues(t, expectedResult["jobA"], actualResult["jobA"])
					assert.EqualValues(t, expectedResult["jobB"], actualResult["jobB"])
					assert.NoError(t, actualError)
				})
			})

			t.Run("validate job & upstream schedules", func(t *testing.T) {
				t.Run("returns unsuccessful result and nil if upstream job is after subject job", func(t *testing.T) {
					tenantDetailsGetter := new(TenantDetailsGetter)
					defer tenantDetailsGetter.AssertExpectations(t)

					jobRepo := new(JobRepository)
					defer jobRepo.AssertExpectations(t)

					downstreamRepo := new(DownstreamRepository)
					defer downstreamRepo.AssertExpectations(t)

					upstreamResolver := new(UpstreamResolver)
					defer upstreamResolver.AssertExpectations(t)

					pluginService := NewPluginService(t)

					jobRunInputCompiler := NewJobRunInputCompiler(t)
					resourceExistenceChecker := NewResourceExistenceChecker(t)

					jobService := service.NewJobService(jobRepo, nil, downstreamRepo,
						pluginService, upstreamResolver, tenantDetailsGetter, nil,
						log, nil, compiler.NewEngine(),
						jobRunInputCompiler, resourceExistenceChecker, nil, config.JobValidationConfig{
							ValidateSchedule: config.ValidateScheduleConfig{
								ReferenceTimezone: "UTC",
							},
						},
					)

					jobASchedule, _ := job.NewScheduleBuilder(startDate).WithInterval("0 2 * * *").Build()
					jobBSchedule, _ := job.NewScheduleBuilder(startDate).WithInterval("0 3 * * *").Build()

					jobSpecA, err := job.NewSpecBuilder(1, "jobA", "optimus@goto", jobASchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()
					assert.NoError(t, err)
					jobSpecB, err := job.NewSpecBuilder(1, "jobB", "optimus@goto", jobBSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()
					assert.NoError(t, err)

					jobA := job.NewJob(sampleTenant, jobSpecA, resourceURNA, []resource.URN{resourceURNB}, false)
					jobB := job.NewJob(sampleTenant, jobSpecB, resourceURNB, []resource.URN{resourceURNC}, false)

					upstreamB := job.NewUpstreamResolved(jobSpecB.Name(), "", resourceURNB, sampleTenant, "static", taskName, false)
					jobAWithUpstream := job.NewWithUpstream(jobA, []*job.Upstream{upstreamB})

					jobRepo.On("GetAllByTenant", ctx, sampleTenant).Return([]*job.Job{jobA}, nil)

					tenantDetailsGetter.On("GetDetails", ctx, sampleTenant).Return(detailedTenant, nil)

					pluginService.On("Info", ctx, jobTask.Name().String()).Return(&pluginSpec, nil)
					pluginService.On("ConstructDestinationURN", ctx, jobTask.Name().String(), mock.Anything).Return(resource.ZeroURN(), nil)
					sourcesToValidate := []resource.URN{resourceURNA, resourceURNB, resourceURNC}
					pluginService.On("IdentifyUpstreams", ctx, jobTask.Name().String(), mock.Anything, mock.Anything).Return(sourcesToValidate, nil)

					jobRunInputCompiler.On("Compile", ctx, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)

					rsc, err := resource.NewResource("resource_1", "table", resource.Bigquery, sampleTenant, &resource.Metadata{Description: "table for test"}, map[string]any{"version": 1})
					assert.NoError(t, err)

					resourceExistenceChecker.On("GetByURN", ctx, sampleTenant, resourceURNA).Return(rsc, nil)
					resourceExistenceChecker.On("ExistInStore", ctx, sampleTenant, resourceURNA).Return(true, nil)

					resourceExistenceChecker.On("GetByURN", ctx, sampleTenant, resourceURNB).Return(rsc, nil)
					resourceExistenceChecker.On("ExistInStore", ctx, sampleTenant, resourceURNB).Return(true, nil)

					resourceExistenceChecker.On("GetByURN", ctx, sampleTenant, resourceURNC).Return(rsc, nil)
					resourceExistenceChecker.On("ExistInStore", ctx, sampleTenant, resourceURNC).Return(true, nil)

					upstreamResolver.On("CheckStaticResolvable", ctx, sampleTenant, mock.Anything, mock.Anything).Return(nil)
					upstreamResolver.On("BulkResolve", ctx, sampleTenant.ProjectName(), mock.Anything, mock.Anything).Return([]*job.WithUpstream{jobAWithUpstream}, nil)

					upstreamResolver.On("Resolve", ctx, mock.Anything, mock.Anything).Return([]*job.Upstream{upstreamB}, nil)
					jobRepo.On("GetByJobName", ctx, sampleTenant.ProjectName(), job.Name("jobB")).Return(jobB, nil)

					request := dto.ValidateRequest{
						Tenant:       sampleTenant,
						JobSpecs:     []*job.Spec{jobSpecA},
						JobNames:     nil,
						DeletionMode: false,
					}

					expectedResult := map[job.Name][]dto.ValidateResult{
						"jobA": {
							{
								Stage:    "destination validation",
								Messages: []string{"no issue"},
								Success:  true,
							},
							{
								Stage: "source validation",
								Messages: []string{
									"bigquery://project:dataset.tableA: no issue",
									"bigquery://project:dataset.tableB: no issue",
									"bigquery://project:dataset.tableC: no issue",
								},
								Success: true,
							},
							{
								Stage:    "window validation",
								Messages: []string{"no issue"},
								Success:  true,
							},
							{
								Stage:    "plugin validation",
								Messages: []string{"no issue"},
								Success:  true,
							},
							{
								Stage:    "compile validation for run",
								Messages: []string{"compiling [bq2bq] with type [task] contains no issue"},
								Success:  true,
							},
							{
								Stage: "upstream validation",
								Messages: []string{
									"upstream job [test-proj/jobB] is scheduled with [0 3 * * *] which was before subject job schedule [0 2 * * *]",
								},
								Success: false,
							},
						},
					}

					actualResult, actualError := jobService.Validate(ctx, request)

					assert.EqualValues(t, expectedResult["jobA"], actualResult["jobA"])
					assert.NoError(t, actualError)
				})
			})
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

			pluginService := NewPluginService(t)

			upstreamResolver := new(UpstreamResolver)
			defer upstreamResolver.AssertExpectations(t)

			tenantDetailsGetter := new(TenantDetailsGetter)
			defer tenantDetailsGetter.AssertExpectations(t)

			specA, _ := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()
			jobA := job.NewJob(sampleTenant, specA, resourceURNA, []resource.URN{resourceURNB}, false)

			upstreamB := job.NewUpstreamResolved("job-B", "", resourceURNB, sampleTenant, "inferred", taskName, false)

			upstreamRepo.On("GetUpstreams", ctx, project.Name(), jobA.Spec().Name()).Return([]*job.Upstream{upstreamB}, nil)

			jobService := service.NewJobService(jobRepo, upstreamRepo, downstreamRepo, pluginService, upstreamResolver, tenantDetailsGetter, nil, log, nil, nil, nil, nil, nil, config.JobValidationConfig{})
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

			pluginService := NewPluginService(t)

			upstreamResolver := new(UpstreamResolver)
			defer upstreamResolver.AssertExpectations(t)

			tenantDetailsGetter := new(TenantDetailsGetter)
			defer tenantDetailsGetter.AssertExpectations(t)

			logWriter := new(mockWriter)
			defer logWriter.AssertExpectations(t)

			specA, _ := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()
			jobA := job.NewJob(sampleTenant, specA, resourceURNA, []resource.URN{resourceURNB}, false)

			upstreamB := job.NewUpstreamResolved("job-B", "", resourceURNB, sampleTenant, "inferred", taskName, false)

			upstreamResolver.On("Resolve", ctx, jobA, mock.Anything).Return([]*job.Upstream{upstreamB}, nil)

			jobService := service.NewJobService(jobRepo, upstreamRepo, downstreamRepo, pluginService, upstreamResolver, tenantDetailsGetter, nil, log, nil, nil, nil, nil, nil, config.JobValidationConfig{})
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

			pluginService := NewPluginService(t)

			upstreamResolver := new(UpstreamResolver)
			defer upstreamResolver.AssertExpectations(t)

			tenantDetailsGetter := new(TenantDetailsGetter)
			defer tenantDetailsGetter.AssertExpectations(t)

			specA, _ := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()

			tenantDetailsGetter.On("GetDetails", ctx, sampleTenant).Return(detailedTenant, nil)

			jobADestination := resourceURNA
			pluginService.On("ConstructDestinationURN", ctx, specA.Task().Name().String(), mock.Anything).Return(jobADestination, nil)

			jobASources := []resource.URN{resourceURNB}
			pluginService.On("IdentifyUpstreams", ctx, specA.Task().Name().String(), mock.Anything, mock.Anything).Return(jobASources, nil)

			jobRepo.On("GetAllByResourceDestination", ctx, jobADestination).Return([]*job.Job{}, nil)

			jobA := job.NewJob(sampleTenant, specA, jobADestination, jobASources, false)

			jobService := service.NewJobService(jobRepo, upstreamRepo, downstreamRepo, pluginService, upstreamResolver, tenantDetailsGetter, nil, log, nil, compiler.NewEngine(), nil, nil, nil, config.JobValidationConfig{})
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

			pluginService := NewPluginService(t)

			upstreamResolver := new(UpstreamResolver)
			defer upstreamResolver.AssertExpectations(t)

			tenantDetailsGetter := new(TenantDetailsGetter)
			defer tenantDetailsGetter.AssertExpectations(t)

			specA, _ := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()
			jobADestination := resourceURNA
			jobASources := []resource.URN{resourceURNB}
			jobA := job.NewJob(sampleTenant, specA, jobADestination, jobASources, false)

			jobRepo.On("GetByJobName", ctx, project.Name(), specA.Name()).Return(jobA, nil)
			jobRepo.On("GetAllByResourceDestination", ctx, jobADestination).Return([]*job.Job{}, nil)

			jobService := service.NewJobService(jobRepo, upstreamRepo, downstreamRepo, pluginService, upstreamResolver, tenantDetailsGetter, nil, log, nil, compiler.NewEngine(), nil, nil, nil, config.JobValidationConfig{})
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

			pluginService := NewPluginService(t)

			upstreamResolver := new(UpstreamResolver)
			defer upstreamResolver.AssertExpectations(t)

			tenantDetailsGetter := new(TenantDetailsGetter)
			defer tenantDetailsGetter.AssertExpectations(t)

			specA, _ := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()

			tenantDetailsGetter.On("GetDetails", ctx, sampleTenant).Return(detailedTenant, errors.New("sample error"))

			jobService := service.NewJobService(jobRepo, upstreamRepo, downstreamRepo, pluginService, upstreamResolver, tenantDetailsGetter, nil, log, nil, compiler.NewEngine(), nil, nil, nil, config.JobValidationConfig{})
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

			pluginService := NewPluginService(t)

			upstreamResolver := new(UpstreamResolver)
			defer upstreamResolver.AssertExpectations(t)

			tenantDetailsGetter := new(TenantDetailsGetter)
			defer tenantDetailsGetter.AssertExpectations(t)

			specA, _ := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()

			tenantDetailsGetter.On("GetDetails", ctx, sampleTenant).Return(detailedTenant, nil)

			jobADestination := resourceURNA
			pluginService.On("ConstructDestinationURN", ctx, specA.Task().Name().String(), mock.Anything).Return(jobADestination, nil).Once()

			jobAUpstreamName := []resource.URN{resourceURNB}
			pluginService.On("IdentifyUpstreams", ctx, specA.Task().Name().String(), mock.Anything, mock.Anything).Return(jobAUpstreamName, errors.New("sample error"))

			jobService := service.NewJobService(jobRepo, upstreamRepo, downstreamRepo, pluginService, upstreamResolver, tenantDetailsGetter, nil, log, nil, compiler.NewEngine(), nil, nil, nil, config.JobValidationConfig{})
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

			pluginService := NewPluginService(t)

			upstreamResolver := new(UpstreamResolver)
			defer upstreamResolver.AssertExpectations(t)

			tenantDetailsGetter := new(TenantDetailsGetter)
			defer tenantDetailsGetter.AssertExpectations(t)

			specASchedule, err := job.NewScheduleBuilder(startDate).WithCatchUp(true).Build()
			assert.NoError(t, err)

			specA, _ := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", specASchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()
			jobADestination := resourceURNA
			jobA := job.NewJob(sampleTenant, specA, jobADestination, nil, false)

			jobRepo.On("GetByJobName", ctx, project.Name(), specA.Name()).Return(jobA, nil)
			jobRepo.On("GetAllByResourceDestination", ctx, jobADestination).Return([]*job.Job{}, errors.New("sample-error"))

			jobService := service.NewJobService(jobRepo, upstreamRepo, downstreamRepo, pluginService, upstreamResolver, tenantDetailsGetter, nil, log, nil, compiler.NewEngine(), nil, nil, nil, config.JobValidationConfig{})
			result, logger := jobService.GetJobBasicInfo(ctx, sampleTenant, specA.Name(), nil)
			assert.Contains(t, logger.Messages[0].Message, "no job sources detected")
			assert.Contains(t, logger.Messages[1].Message, "catchup is enabled")
			assert.Contains(t, logger.Messages[2].Message, "could not perform duplicate job destination check")
			assert.Equal(t, jobA, result)
		})
		t.Run("should return error if unable to get existing job", func(t *testing.T) {
			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			upstreamRepo := new(UpstreamRepository)
			defer upstreamRepo.AssertExpectations(t)

			downstreamRepo := new(DownstreamRepository)
			defer downstreamRepo.AssertExpectations(t)

			pluginService := NewPluginService(t)

			upstreamResolver := new(UpstreamResolver)
			defer upstreamResolver.AssertExpectations(t)

			tenantDetailsGetter := new(TenantDetailsGetter)
			defer tenantDetailsGetter.AssertExpectations(t)

			specA, _ := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()

			jobRepo.On("GetByJobName", ctx, project.Name(), specA.Name()).Return(nil, errors.New("internal error"))

			jobService := service.NewJobService(jobRepo, upstreamRepo, downstreamRepo, pluginService, upstreamResolver, tenantDetailsGetter, nil, log, nil, compiler.NewEngine(), nil, nil, nil, config.JobValidationConfig{})
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

			pluginService := NewPluginService(t)

			upstreamResolver := new(UpstreamResolver)
			defer upstreamResolver.AssertExpectations(t)

			tenantDetailsGetter := new(TenantDetailsGetter)
			defer tenantDetailsGetter.AssertExpectations(t)

			specA, _ := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()

			jobRepo.On("GetByJobName", ctx, project.Name(), specA.Name()).Return(nil, errors.New("job not found"))

			jobService := service.NewJobService(jobRepo, upstreamRepo, downstreamRepo, pluginService, upstreamResolver, tenantDetailsGetter, nil, log, nil, compiler.NewEngine(), nil, nil, nil, config.JobValidationConfig{})
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

			pluginService := NewPluginService(t)

			upstreamResolver := new(UpstreamResolver)
			defer upstreamResolver.AssertExpectations(t)

			tenantDetailsGetter := new(TenantDetailsGetter)
			defer tenantDetailsGetter.AssertExpectations(t)

			specA, _ := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()
			jobADestination := resourceURNA
			jobASources := []resource.URN{resourceURNB}
			jobA := job.NewJob(sampleTenant, specA, jobADestination, jobASources, false)

			specB, _ := job.NewSpecBuilder(jobVersion, "job-B", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()
			jobBDestination := resourceURNB
			jobBSources := []resource.URN{resourceURNC}
			jobB := job.NewJob(sampleTenant, specB, jobBDestination, jobBSources, false)

			jobRepo.On("GetByJobName", ctx, project.Name(), specA.Name()).Return(jobA, nil)
			jobRepo.On("GetAllByResourceDestination", ctx, jobADestination).Return([]*job.Job{jobB, jobA}, nil)

			jobService := service.NewJobService(jobRepo, upstreamRepo, downstreamRepo, pluginService, upstreamResolver, tenantDetailsGetter, nil, log, nil, compiler.NewEngine(), nil, nil, nil, config.JobValidationConfig{})
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

			pluginService := NewPluginService(t)

			upstreamResolver := new(UpstreamResolver)
			defer upstreamResolver.AssertExpectations(t)

			tenantDetailsGetter := new(TenantDetailsGetter)
			defer tenantDetailsGetter.AssertExpectations(t)

			specA, _ := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()
			jobADestination := resourceURNA
			jobASources := []resource.URN{resourceURNB}
			jobA := job.NewJob(sampleTenant, specA, jobADestination, jobASources, false)

			jobRepo.On("GetByJobName", ctx, project.Name(), specA.Name()).Return(jobA, nil)
			jobRepo.On("GetAllByResourceDestination", ctx, jobADestination).Return([]*job.Job{jobA}, nil)

			jobService := service.NewJobService(jobRepo, upstreamRepo, downstreamRepo, pluginService, upstreamResolver, tenantDetailsGetter, nil, log, nil, compiler.NewEngine(), nil, nil, nil, config.JobValidationConfig{})
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

			pluginService := NewPluginService(t)

			upstreamResolver := new(UpstreamResolver)
			defer upstreamResolver.AssertExpectations(t)

			tenantDetailsGetter := new(TenantDetailsGetter)
			defer tenantDetailsGetter.AssertExpectations(t)

			specA, _ := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()
			jobA := job.NewJob(sampleTenant, specA, resourceURNA, nil, false)

			jobADownstream := []*job.Downstream{
				job.NewDownstream("job-B", project.Name(), namespace.Name(), taskName),
			}
			downstreamRepo.On("GetDownstreamByDestination", ctx, project.Name(), jobA.Destination()).Return(jobADownstream, nil)

			jobService := service.NewJobService(jobRepo, upstreamRepo, downstreamRepo, pluginService, upstreamResolver, tenantDetailsGetter, nil, log, nil, nil, nil, nil, nil, config.JobValidationConfig{})
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

			pluginService := NewPluginService(t)

			upstreamResolver := new(UpstreamResolver)
			defer upstreamResolver.AssertExpectations(t)

			tenantDetailsGetter := new(TenantDetailsGetter)
			defer tenantDetailsGetter.AssertExpectations(t)

			specA, _ := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()
			jobA := job.NewJob(sampleTenant, specA, resourceURNA, nil, false)

			jobADownstream := []*job.Downstream{
				job.NewDownstream("job-B", project.Name(), namespace.Name(), taskName),
			}
			downstreamRepo.On("GetDownstreamByJobName", ctx, project.Name(), specA.Name()).Return(jobADownstream, nil)

			jobService := service.NewJobService(jobRepo, upstreamRepo, downstreamRepo, pluginService, upstreamResolver, tenantDetailsGetter, nil, log, nil, nil, nil, nil, nil, config.JobValidationConfig{})
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
			jobDeploymentService.On("UpdateJobScheduleState", ctx, sampleTenant.ProjectName(), jobsToUpdateState, state).Return(fmt.Errorf("some error in update Job State"))
			defer jobDeploymentService.AssertExpectations(t)

			jobService := service.NewJobService(nil, nil, nil, nil, nil, nil, nil, nil, jobDeploymentService, nil, nil, nil, nil, config.JobValidationConfig{})
			err := jobService.UpdateState(ctx, sampleTenant, jobsToUpdateState, state, "job disable remark")
			assert.ErrorContains(t, err, "some error in update Job State")
		})

		t.Run("should fail if update state in job table fails", func(t *testing.T) {
			jobDeploymentService := new(JobDeploymentService)
			jobDeploymentService.On("UpdateJobScheduleState", ctx, sampleTenant.ProjectName(), jobsToUpdateState, state).Return(nil)
			defer jobDeploymentService.AssertExpectations(t)

			jobRepo := new(JobRepository)
			jobRepo.On("UpdateState", ctx, sampleTenant, jobsToUpdateState, state, updateRemark).Return(fmt.Errorf("some error in update Job State repo"))
			defer jobRepo.AssertExpectations(t)

			jobService := service.NewJobService(jobRepo, nil, nil, nil, nil, nil, nil, nil, jobDeploymentService, nil, nil, nil, nil, config.JobValidationConfig{})
			err := jobService.UpdateState(ctx, sampleTenant, jobsToUpdateState, state, updateRemark)
			assert.ErrorContains(t, err, "some error in update Job State repo")
		})

		t.Run("should pass when no error in scheduler update and repo update", func(t *testing.T) {
			jobDeploymentService := new(JobDeploymentService)
			jobDeploymentService.On("UpdateJobScheduleState", ctx, sampleTenant.ProjectName(), jobsToUpdateState, state).Return(nil)
			defer jobDeploymentService.AssertExpectations(t)

			jobRepo := new(JobRepository)
			jobRepo.On("UpdateState", ctx, sampleTenant, jobsToUpdateState, state, updateRemark).Return(nil)
			defer jobRepo.AssertExpectations(t)

			eventHandler := newEventHandler(t)
			eventHandler.On("HandleEvent", mock.Anything).Times(1)

			jobService := service.NewJobService(jobRepo, nil, nil, nil, nil, nil, eventHandler, nil, jobDeploymentService, nil, nil, nil, nil, config.JobValidationConfig{})
			err := jobService.UpdateState(ctx, sampleTenant, jobsToUpdateState, state, updateRemark)
			assert.Nil(t, err)
		})
	})

	t.Run("GetDownstreamByResourceURN", func(t *testing.T) {
		tnnt, _ := tenant.NewTenant("proj123", "name123")
		urn, err := resource.ParseURN("bigquery://dataset.table_test")
		assert.NoError(t, err)

		downstreamJobs := []*job.Downstream{
			job.NewDownstream("jobA", tnnt.ProjectName(), tnnt.NamespaceName(), "jobA"),
			job.NewDownstream("jobA", "proj501", "name4012", "jobA"),
		}

		t.Run("success found dependent job", func(t *testing.T) {
			var (
				downstreamRepo = new(DownstreamRepository)
				jobService     = service.NewJobService(nil, nil, downstreamRepo, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, config.JobValidationConfig{})
			)

			downstreamRepo.On("GetDownstreamBySources", ctx, []resource.URN{urn}).Return(downstreamJobs, nil)

			actual, err := jobService.GetDownstreamByResourceURN(ctx, tnnt, urn)
			assert.NoError(t, err)
			assert.NotNil(t, actual)
			assert.Len(t, actual.GetDownstreamFullNames(), 1)
		})

		t.Run("return error when GetDownstreamBySources", func(t *testing.T) {
			var (
				downstreamRepo = new(DownstreamRepository)
				jobService     = service.NewJobService(nil, nil, downstreamRepo, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, config.JobValidationConfig{})
			)

			downstreamRepo.On("GetDownstreamBySources", ctx, []resource.URN{urn}).Return(nil, context.DeadlineExceeded)

			actual, err := jobService.GetDownstreamByResourceURN(ctx, tnnt, urn)
			assert.Error(t, err)
			assert.Nil(t, actual)
		})
	})

	t.Run("BulkDeleteJobs", func(t *testing.T) {
		alertManager := new(AlertManager)
		alertManager.On("SendJobEvent", mock.Anything).Return()
		defer alertManager.AssertExpectations(t)

		t.Run("successfully deletes a job & its downstream", func(t *testing.T) {
			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			upstreamRepo := new(UpstreamRepository)
			defer upstreamRepo.AssertExpectations(t)

			downstreamRepo := new(DownstreamRepository)
			defer downstreamRepo.AssertExpectations(t)

			pluginService := NewPluginService(t)

			upstreamResolver := new(UpstreamResolver)
			defer upstreamResolver.AssertExpectations(t)

			tenantDetailsGetter := new(TenantDetailsGetter)
			defer tenantDetailsGetter.AssertExpectations(t)

			jobDeploymentService := new(JobDeploymentService)
			defer jobDeploymentService.AssertExpectations(t)

			eventHandler := newEventHandler(t)

			specA, _ := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()
			specB, _ := job.NewSpecBuilder(jobVersion, "job-B", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()
			jobA := job.NewJob(sampleTenant, specA, resource.ZeroURN(), nil, false)
			jobB := job.NewJob(sampleTenant, specB, resource.ZeroURN(), nil, false)

			downstreamA := []*job.Downstream{
				job.NewDownstream("job-B", project.Name(), namespace.Name(), taskName),
			}

			jobRepo.On("GetByJobName", ctx, project.Name(), job.Name("job-A")).Return(jobA, nil).Once()
			jobRepo.On("GetByJobName", ctx, project.Name(), job.Name("job-B")).Return(jobB, nil).Once()
			downstreamRepo.On("GetDownstreamByJobName", ctx, project.Name(), specA.Name()).Return(downstreamA, nil)
			downstreamRepo.On("GetDownstreamByJobName", ctx, project.Name(), specB.Name()).Return(nil, nil)
			jobRepo.On("Delete", ctx, project.Name(), jobB.Spec().Name(), false).Return(nil).Once()
			jobRepo.On("Delete", ctx, project.Name(), jobA.Spec().Name(), false).Return(nil).Once()
			eventHandler.On("HandleEvent", mock.Anything).Times(2)

			jobDeploymentService.On("UploadJobs", ctx, sampleTenant, mock.Anything, []string{specB.Name().String(), specA.Name().String()}).Return(nil)

			jobService := service.NewJobService(jobRepo, upstreamRepo, downstreamRepo, pluginService, upstreamResolver, tenantDetailsGetter, eventHandler, log, jobDeploymentService, compiler.NewEngine(), nil, nil, alertManager, config.JobValidationConfig{})
			deletionTracker, err := jobService.BulkDeleteJobs(ctx, project.Name(), []*dto.JobToDeleteRequest{
				{
					Namespace: sampleTenant.NamespaceName(),
					JobName:   "job-A",
				},
				{
					Namespace: sampleTenant.NamespaceName(),
					JobName:   "job-B",
				},
			})

			assert.NoError(t, err)
			// assess each job's deletion status: all deletion should be successful
			jobADeletion := deletionTracker["job-A"]
			assert.NotNil(t, jobADeletion)
			assert.True(t, jobADeletion.Success)
			assert.Empty(t, jobADeletion.Message)
			jobBDeletion := deletionTracker["job-B"]
			assert.NotNil(t, jobBDeletion)
			assert.True(t, jobBDeletion.Success)
			assert.Empty(t, jobBDeletion.Message)
		})

		t.Run("error when fetching one of job details", func(t *testing.T) {
			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			upstreamRepo := new(UpstreamRepository)
			defer upstreamRepo.AssertExpectations(t)

			downstreamRepo := new(DownstreamRepository)
			defer downstreamRepo.AssertExpectations(t)

			pluginService := NewPluginService(t)

			upstreamResolver := new(UpstreamResolver)
			defer upstreamResolver.AssertExpectations(t)

			tenantDetailsGetter := new(TenantDetailsGetter)
			defer tenantDetailsGetter.AssertExpectations(t)

			jobDeploymentService := new(JobDeploymentService)
			defer jobDeploymentService.AssertExpectations(t)

			eventHandler := newEventHandler(t)

			jobRepo.On("GetByJobName", ctx, project.Name(), job.Name("job-A")).Return(nil, errors.New("error")).Once()

			jobService := service.NewJobService(jobRepo, upstreamRepo, downstreamRepo, pluginService, upstreamResolver, tenantDetailsGetter, eventHandler, log, jobDeploymentService, compiler.NewEngine(), nil, nil, nil, config.JobValidationConfig{})
			deletionTracker, err := jobService.BulkDeleteJobs(ctx, project.Name(), []*dto.JobToDeleteRequest{
				{
					Namespace: sampleTenant.NamespaceName(),
					JobName:   "job-A",
				},
			})

			assert.Error(t, err)
			assert.Empty(t, deletionTracker)
		})

		t.Run("fail to delete a job because its downstream is failed to be deleted", func(t *testing.T) {
			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			upstreamRepo := new(UpstreamRepository)
			defer upstreamRepo.AssertExpectations(t)

			downstreamRepo := new(DownstreamRepository)
			defer downstreamRepo.AssertExpectations(t)

			pluginService := NewPluginService(t)

			upstreamResolver := new(UpstreamResolver)
			defer upstreamResolver.AssertExpectations(t)

			tenantDetailsGetter := new(TenantDetailsGetter)
			defer tenantDetailsGetter.AssertExpectations(t)

			jobDeploymentService := new(JobDeploymentService)
			defer jobDeploymentService.AssertExpectations(t)

			eventHandler := newEventHandler(t)

			specA, _ := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()
			specB, _ := job.NewSpecBuilder(jobVersion, "job-B", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(jobAsset).Build()
			jobA := job.NewJob(sampleTenant, specA, resource.ZeroURN(), nil, false)
			jobB := job.NewJob(sampleTenant, specB, resource.ZeroURN(), nil, false)

			downstreamA := []*job.Downstream{
				job.NewDownstream("job-B", project.Name(), namespace.Name(), taskName),
			}

			jobRepo.On("GetByJobName", ctx, project.Name(), job.Name("job-A")).Return(jobA, nil).Once()
			jobRepo.On("GetByJobName", ctx, project.Name(), job.Name("job-B")).Return(jobB, nil).Once()
			downstreamRepo.On("GetDownstreamByJobName", ctx, project.Name(), specA.Name()).Return(downstreamA, nil)
			downstreamRepo.On("GetDownstreamByJobName", ctx, project.Name(), specB.Name()).Return(nil, nil)
			jobRepo.On("Delete", ctx, project.Name(), jobB.Spec().Name(), false).Return(errors.New("error deleting job-B"))

			jobService := service.NewJobService(jobRepo, upstreamRepo, downstreamRepo, pluginService, upstreamResolver, tenantDetailsGetter, eventHandler, log, jobDeploymentService, compiler.NewEngine(), nil, nil, nil, config.JobValidationConfig{})
			deletionTracker, err := jobService.BulkDeleteJobs(ctx, project.Name(), []*dto.JobToDeleteRequest{
				{
					Namespace: sampleTenant.NamespaceName(),
					JobName:   "job-A",
				},
				{
					Namespace: sampleTenant.NamespaceName(),
					JobName:   "job-B",
				},
			})

			assert.NoError(t, err)
			jobADeletion := deletionTracker["job-A"]
			assert.NotNil(t, jobADeletion)
			assert.False(t, jobADeletion.Success)
			assert.Equal(t, "one or more job downstreams cannot be deleted", jobADeletion.Message)
			jobBDeletion := deletionTracker["job-B"]
			assert.NotNil(t, jobBDeletion)
			assert.False(t, jobBDeletion.Success)
			assert.Equal(t, "error deleting job-B", jobBDeletion.Message)
		})

		t.Run("fail to delete one or more jobs because there are existing downstream", func(t *testing.T) {
			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			upstreamRepo := new(UpstreamRepository)
			defer upstreamRepo.AssertExpectations(t)

			downstreamRepo := new(DownstreamRepository)
			defer downstreamRepo.AssertExpectations(t)

			pluginService := NewPluginService(t)

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
			jobA := job.NewJob(sampleTenant, specA, resource.ZeroURN(), nil, false)
			jobB := job.NewJob(sampleTenant, specB, resource.ZeroURN(), nil, false)
			_ = job.NewJob(sampleTenant, specC, resource.ZeroURN(), nil, false)

			downstreamA := []*job.Downstream{
				job.NewDownstream("job-B", project.Name(), namespace.Name(), taskName),
			}
			downstreamB := []*job.Downstream{
				job.NewDownstream("job-C", project.Name(), namespace.Name(), taskName),
			}

			jobRepo.On("GetByJobName", ctx, project.Name(), job.Name("job-A")).Return(jobA, nil).Once()
			jobRepo.On("GetByJobName", ctx, project.Name(), job.Name("job-B")).Return(jobB, nil).Once()
			downstreamRepo.On("GetDownstreamByJobName", ctx, project.Name(), specA.Name()).Return(downstreamA, nil)
			downstreamRepo.On("GetDownstreamByJobName", ctx, project.Name(), specB.Name()).Return(downstreamB, nil)
			downstreamRepo.On("GetDownstreamByJobName", ctx, project.Name(), specC.Name()).Return(nil, nil)

			jobService := service.NewJobService(jobRepo, upstreamRepo, downstreamRepo, pluginService, upstreamResolver, tenantDetailsGetter, eventHandler, log, jobDeploymentService, compiler.NewEngine(), nil, nil, nil, config.JobValidationConfig{})
			deletionTracker, err := jobService.BulkDeleteJobs(ctx, project.Name(), []*dto.JobToDeleteRequest{
				{
					Namespace: sampleTenant.NamespaceName(),
					JobName:   "job-A",
				},
				{
					Namespace: sampleTenant.NamespaceName(),
					JobName:   "job-B",
				},
			})

			assert.NoError(t, err)
			jobADeletion := deletionTracker["job-A"]
			assert.NotNil(t, jobADeletion)
			assert.False(t, jobADeletion.Success)
			assert.Equal(t, "failed precondition for entity job: job is being used by test-proj/job-B", jobADeletion.Message)
			jobBDeletion := deletionTracker["job-B"]
			assert.NotNil(t, jobBDeletion)
			assert.False(t, jobBDeletion.Success)
			assert.Equal(t, "failed precondition for entity job: job is being used by test-proj/job-C", jobBDeletion.Message)
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

func (_m *JobRepository) GetChangelog(ctx context.Context, projectName tenant.ProjectName, jobName job.Name) ([]*job.ChangeLog, error) {
	args := _m.Called(ctx, projectName, jobName)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]*job.ChangeLog), args.Error(1)
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

// SetDirty provides a mock function with given fields: ctx, jobTenant, jobNames
func (_m *JobRepository) SetDirty(ctx context.Context, jobsTenant tenant.Tenant, jobNames []job.Name, isDirty bool) error {
	ret := _m.Called(ctx, jobsTenant, jobNames, isDirty)
	return ret.Error(0)
}

// SetDirty provides a mock function with given fields: ctx, jobTenant, jobNames
func (_m *JobRepository) GetAll(ctx context.Context) ([]*job.Job, error) {
	ret := _m.Called(ctx)
	return ret.Get(1).([]*job.Job), ret.Error(1)
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
func (_m *JobRepository) GetAllByResourceDestination(ctx context.Context, resourceDestination resource.URN) ([]*job.Job, error) {
	ret := _m.Called(ctx, resourceDestination)

	var r0 []*job.Job
	if rf, ok := ret.Get(0).(func(context.Context, resource.URN) []*job.Job); ok {
		r0 = rf(ctx, resourceDestination)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]*job.Job)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, resource.URN) error); ok {
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
func (_d *DownstreamRepository) GetDownstreamByDestination(ctx context.Context, projectName tenant.ProjectName, destination resource.URN) ([]*job.Downstream, error) {
	ret := _d.Called(ctx, projectName, destination)

	var r0 []*job.Downstream
	if rf, ok := ret.Get(0).(func(context.Context, tenant.ProjectName, resource.URN) []*job.Downstream); ok {
		r0 = rf(ctx, projectName, destination)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]*job.Downstream)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, tenant.ProjectName, resource.URN) error); ok {
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
func (_d *DownstreamRepository) GetDownstreamBySources(ctx context.Context, sources []resource.URN) ([]*job.Downstream, error) {
	ret := _d.Called(ctx, sources)

	var r0 []*job.Downstream
	if rf, ok := ret.Get(0).(func(context.Context, []resource.URN) []*job.Downstream); ok {
		r0 = rf(ctx, sources)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]*job.Downstream)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, []resource.URN) error); ok {
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

// ConstructDestinationURN provides a mock function with given fields: ctx, taskName, compiledConfig
func (_m *PluginService) ConstructDestinationURN(ctx context.Context, taskName string, compiledConfig map[string]string) (resource.URN, error) {
	ret := _m.Called(ctx, taskName, compiledConfig)

	if len(ret) == 0 {
		panic("no return value specified for ConstructDestinationURN")
	}

	var r0 resource.URN
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context, string, map[string]string) (resource.URN, error)); ok {
		return rf(ctx, taskName, compiledConfig)
	}
	if rf, ok := ret.Get(0).(func(context.Context, string, map[string]string) resource.URN); ok {
		r0 = rf(ctx, taskName, compiledConfig)
	} else {
		r0 = ret.Get(0).(resource.URN)
	}

	if rf, ok := ret.Get(1).(func(context.Context, string, map[string]string) error); ok {
		r1 = rf(ctx, taskName, compiledConfig)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// IdentifyUpstreams provides a mock function with given fields: ctx, taskName, compiledConfig, assets
func (_m *PluginService) IdentifyUpstreams(ctx context.Context, taskName string, compiledConfig, assets map[string]string) ([]resource.URN, error) {
	ret := _m.Called(ctx, taskName, compiledConfig, assets)

	if len(ret) == 0 {
		panic("no return value specified for IdentifyUpstreams")
	}

	var r0 []resource.URN
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context, string, map[string]string, map[string]string) ([]resource.URN, error)); ok {
		return rf(ctx, taskName, compiledConfig, assets)
	}
	if rf, ok := ret.Get(0).(func(context.Context, string, map[string]string, map[string]string) []resource.URN); ok {
		r0 = rf(ctx, taskName, compiledConfig, assets)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]resource.URN)
		}
	}

	if rf, ok := ret.Get(1).(func(context.Context, string, map[string]string, map[string]string) error); ok {
		r1 = rf(ctx, taskName, compiledConfig, assets)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// Info provides a mock function with given fields: ctx, taskName
func (_m *PluginService) Info(ctx context.Context, taskName string) (*plugin.Spec, error) {
	ret := _m.Called(ctx, taskName)

	if ret.Get(0) == nil {
		return nil, ret.Error(1)
	}

	return ret.Get(0).(*plugin.Spec), ret.Error(1)
}

// NewPluginService creates a new instance of PluginService. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewPluginService(t interface {
	mock.TestingT
	Cleanup(func())
},
) *PluginService {
	mock := &PluginService{}
	mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}

// UpstreamResolver is an autogenerated mock type for the UpstreamResolver type
type UpstreamResolver struct {
	mock.Mock
}

func (_m *UpstreamResolver) CheckStaticResolvable(ctx context.Context, tnnt tenant.Tenant, jobs []*job.Job, logWriter writer.LogWriter) error {
	args := _m.Called(ctx, tnnt, jobs, logWriter)
	return args.Error(0)
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
	mock.Test(t)

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

func (_m *JobDeploymentService) UpdateJobScheduleState(ctx context.Context, projectName tenant.ProjectName, jobNames []job.Name, state job.State) error {
	args := _m.Called(ctx, projectName, jobNames, state)
	return args.Error(0)
}

func (_m *JobDeploymentService) GetJobSchedulerState(ctx context.Context, projectName tenant.ProjectName) (map[string]bool, error) {
	args := _m.Called(ctx, projectName)
	return args.Get(0).(map[string]bool), args.Error(1)
}

// JobRunInputCompiler is an autogenerated mock type for the JobRunInputCompiler type
type JobRunInputCompiler struct {
	mock.Mock
}

// Compile provides a mock function with given fields: ctx, job, config, executedAt
func (_m *JobRunInputCompiler) Compile(ctx context.Context, job *scheduler.JobWithDetails, config scheduler.RunConfig, executedAt time.Time) (*scheduler.ExecutorInput, error) {
	ret := _m.Called(ctx, job, config, executedAt)

	if len(ret) == 0 {
		panic("no return value specified for Compile")
	}

	var r0 *scheduler.ExecutorInput
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context, *scheduler.JobWithDetails, scheduler.RunConfig, time.Time) (*scheduler.ExecutorInput, error)); ok {
		return rf(ctx, job, config, executedAt)
	}
	if rf, ok := ret.Get(0).(func(context.Context, *scheduler.JobWithDetails, scheduler.RunConfig, time.Time) *scheduler.ExecutorInput); ok {
		r0 = rf(ctx, job, config, executedAt)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*scheduler.ExecutorInput)
		}
	}

	if rf, ok := ret.Get(1).(func(context.Context, *scheduler.JobWithDetails, scheduler.RunConfig, time.Time) error); ok {
		r1 = rf(ctx, job, config, executedAt)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// AlertManager is an autogenerated mock type for the AlertManager type
type AlertManager struct {
	mock.Mock
}

func (_m *AlertManager) SendJobEvent(attr *job.AlertAttrs) {
	_m.Called(attr)
}

// NewJobRunInputCompiler creates a new instance of JobRunInputCompiler. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewJobRunInputCompiler(t interface {
	mock.TestingT
	Cleanup(func())
},
) *JobRunInputCompiler {
	mock := &JobRunInputCompiler{}
	mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}

// ResourceExistenceChecker is an autogenerated mock type for the ResourceExistenceChecker type
type ResourceExistenceChecker struct {
	mock.Mock
}

// ExistInStore provides a mock function with given fields: ctx, tnnt, urn
func (_m *ResourceExistenceChecker) ExistInStore(ctx context.Context, tnnt tenant.Tenant, urn resource.URN) (bool, error) {
	ret := _m.Called(ctx, tnnt, urn)

	if len(ret) == 0 {
		panic("no return value specified for ExistInStore")
	}

	var r0 bool
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context, tenant.Tenant, resource.URN) (bool, error)); ok {
		return rf(ctx, tnnt, urn)
	}
	if rf, ok := ret.Get(0).(func(context.Context, tenant.Tenant, resource.URN) bool); ok {
		r0 = rf(ctx, tnnt, urn)
	} else {
		r0 = ret.Get(0).(bool)
	}

	if rf, ok := ret.Get(1).(func(context.Context, tenant.Tenant, resource.URN) error); ok {
		r1 = rf(ctx, tnnt, urn)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// GetByURN provides a mock function with given fields: ctx, tnnt, urn
func (_m *ResourceExistenceChecker) GetByURN(ctx context.Context, tnnt tenant.Tenant, urn resource.URN) (*resource.Resource, error) {
	ret := _m.Called(ctx, tnnt, urn)

	if len(ret) == 0 {
		panic("no return value specified for GetByURN")
	}

	var r0 *resource.Resource
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context, tenant.Tenant, resource.URN) (*resource.Resource, error)); ok {
		return rf(ctx, tnnt, urn)
	}
	if rf, ok := ret.Get(0).(func(context.Context, tenant.Tenant, resource.URN) *resource.Resource); ok {
		r0 = rf(ctx, tnnt, urn)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*resource.Resource)
		}
	}

	if rf, ok := ret.Get(1).(func(context.Context, tenant.Tenant, resource.URN) error); ok {
		r1 = rf(ctx, tnnt, urn)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// NewResourceExistenceChecker creates a new instance of ResourceExistenceChecker. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewResourceExistenceChecker(t interface {
	mock.TestingT
	Cleanup(func())
},
) *ResourceExistenceChecker {
	mock := &ResourceExistenceChecker{}
	mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
