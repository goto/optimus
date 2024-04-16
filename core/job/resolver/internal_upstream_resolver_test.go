package resolver_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"golang.org/x/net/context"

	"github.com/goto/optimus/core/job"
	"github.com/goto/optimus/core/job/resolver"
	"github.com/goto/optimus/core/tenant"
	"github.com/goto/optimus/internal/lib/window"
	"github.com/goto/optimus/internal/models"
)

func TestInternalUpstreamResolver(t *testing.T) {
	ctx := context.Background()
	sampleTenant, _ := tenant.NewTenant("project", "namespace")

	jobVersion := 1
	startDate, _ := job.ScheduleDateFrom("2022-10-01")
	jobSchedule, _ := job.NewScheduleBuilder(startDate).Build()
	w, _ := models.NewWindow(jobVersion, "d", "24h", "24h")
	jobWindow := window.NewCustomConfig(w)
	taskName, _ := job.TaskNameFrom("sample-task")
	jobTaskConfig := map[string]string{"sample_task_key": "sample_value"}
	jobTask := job.NewTask(taskName, jobTaskConfig)
	upstreamSpec, _ := job.NewSpecUpstreamBuilder().WithUpstreamNames([]job.SpecUpstreamName{"job-C"}).Build()
	specA, _ := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, jobWindow, jobTask).WithSpecUpstream(upstreamSpec).Build()
	jobADestination := job.ResourceURN("resource-A")
	jobASources := []job.ResourceURN{"resource-B", "resource-D"}
	jobA := job.NewJob(sampleTenant, specA, jobADestination, jobASources, false)

	specB, _ := job.NewSpecBuilder(jobVersion, "job-B", "sample-owner", jobSchedule, jobWindow, jobTask).Build()
	jobBDestination := job.ResourceURN("resource-B")
	jobB := job.NewJob(sampleTenant, specB, jobBDestination, nil, false)

	specC, _ := job.NewSpecBuilder(jobVersion, "job-C", "sample-owner", jobSchedule, jobWindow, jobTask).Build()
	jobCDestination := job.ResourceURN("resource-C")
	jobC := job.NewJob(sampleTenant, specC, jobCDestination, nil, false)

	internalUpstreamB := job.NewUpstreamResolved("job-B", "", "resource-B", sampleTenant, "inferred", taskName, false)
	internalUpstreamC := job.NewUpstreamResolved("job-C", "", "resource-C", sampleTenant, "static", taskName, false)

	unresolvedUpstreamB := job.NewUpstreamUnresolvedInferred("resource-B")
	unresolvedUpstreamC := job.NewUpstreamUnresolvedStatic("job-C", "project")
	unresolvedUpstreamD := job.NewUpstreamUnresolvedInferred("resource-D")

	t.Run("Resolve", func(t *testing.T) {
		t.Run("resolves inferred and static upstream internally", func(t *testing.T) {
			jobRepo := new(JobRepository)
			logWriter := new(mockWriter)
			resourceResolver := new(resourceResolverMock)

			defer logWriter.AssertExpectations(t)

			jobRepo.On("GetAllByResourceDestination", ctx, jobASources[0]).Return([]*job.Job{jobB}, nil)
			jobRepo.On("GetAllByResourceDestination", ctx, jobASources[1]).Return([]*job.Job{}, nil)
			jobRepo.On("GetByJobName", ctx, sampleTenant.ProjectName(), specC.Name()).Return(jobC, nil)

			jobWithUnresolvedUpstream := job.NewWithUpstream(jobA, []*job.Upstream{unresolvedUpstreamB, unresolvedUpstreamC, unresolvedUpstreamD})
			expectedJobWithUpstream := job.NewWithUpstream(jobA, []*job.Upstream{internalUpstreamB, internalUpstreamC, unresolvedUpstreamD})

			resourceResolver.On("CheckIsDeleted", ctx, []*job.WithUpstream{expectedJobWithUpstream}).Return(nil)

			internalUpstreamResolver := resolver.NewInternalUpstreamResolver(jobRepo, resourceResolver)
			result, err := internalUpstreamResolver.Resolve(ctx, jobWithUnresolvedUpstream)
			assert.NoError(t, err)
			assert.Equal(t, expectedJobWithUpstream.Job(), result.Job())
			assert.ElementsMatch(t, expectedJobWithUpstream.Upstreams(), result.Upstreams())
		})
		t.Run("resolves inferred and static upstream internally and prioritize static upstream when duplication found", func(t *testing.T) {
			jobRepo := new(JobRepository)
			logWriter := new(mockWriter)
			resourceResolver := new(resourceResolverMock)
			defer logWriter.AssertExpectations(t)

			specD, _ := job.NewSpecBuilder(jobVersion, "job-D", "sample-owner", jobSchedule, jobWindow, jobTask).WithSpecUpstream(upstreamSpec).Build()
			jobDDestination := job.ResourceURN("resource-D")
			jobDSources := []job.ResourceURN{"resource-C"}
			jobD := job.NewJob(sampleTenant, specD, jobDDestination, jobDSources, false)

			unresolvedUpstreamCInferred := job.NewUpstreamUnresolvedInferred("resource-C")
			unresolvedUpstreamCStatic := job.NewUpstreamUnresolvedStatic("job-C", sampleTenant.ProjectName())
			internalUpstreamCStatic := job.NewUpstreamResolved("job-C", "", "resource-C", sampleTenant, "static", taskName, false)

			jobRepo.On("GetAllByResourceDestination", ctx, jobDSources[0]).Return([]*job.Job{jobC}, nil)
			jobRepo.On("GetByJobName", ctx, sampleTenant.ProjectName(), specC.Name()).Return(jobC, nil)

			jobWithUnresolvedUpstream := job.NewWithUpstream(jobD, []*job.Upstream{unresolvedUpstreamCStatic, unresolvedUpstreamCInferred})
			expectedJobWithUpstream := job.NewWithUpstream(jobD, []*job.Upstream{internalUpstreamCStatic})

			resourceResolver.On("CheckIsDeleted", ctx, []*job.WithUpstream{expectedJobWithUpstream}).Return(nil)

			internalUpstreamResolver := resolver.NewInternalUpstreamResolver(jobRepo, resourceResolver)
			result, err := internalUpstreamResolver.Resolve(ctx, jobWithUnresolvedUpstream)
			assert.NoError(t, err)
			assert.EqualValues(t, expectedJobWithUpstream, result)
		})
		t.Run("resolves inferred upstream internally", func(t *testing.T) {
			jobRepo := new(JobRepository)
			logWriter := new(mockWriter)
			resourceResolver := new(resourceResolverMock)
			defer logWriter.AssertExpectations(t)

			specX, _ := job.NewSpecBuilder(jobVersion, "job-X", "sample-owner", jobSchedule, jobWindow, jobTask).Build()
			jobXDestination := job.ResourceURN("resource-X")
			jobX := job.NewJob(sampleTenant, specX, jobXDestination, []job.ResourceURN{"resource-B"}, false)

			jobRepo.On("GetAllByResourceDestination", ctx, jobX.Sources()[0]).Return([]*job.Job{jobB}, nil)

			jobWithUnresolvedUpstream := job.NewWithUpstream(jobX, []*job.Upstream{unresolvedUpstreamB})
			expectedJobWithUpstream := job.NewWithUpstream(jobX, []*job.Upstream{internalUpstreamB})

			resourceResolver.On("CheckIsDeleted", ctx, []*job.WithUpstream{expectedJobWithUpstream}).Return(nil)

			internalUpstreamResolver := resolver.NewInternalUpstreamResolver(jobRepo, resourceResolver)
			result, err := internalUpstreamResolver.Resolve(ctx, jobWithUnresolvedUpstream)
			assert.NoError(t, err)
			assert.ElementsMatch(t, expectedJobWithUpstream.Upstreams(), result.Upstreams())
		})
		t.Run("resolves static upstream internally", func(t *testing.T) {
			jobRepo := new(JobRepository)
			logWriter := new(mockWriter)
			resourceResolver := new(resourceResolverMock)
			defer logWriter.AssertExpectations(t)

			specX, _ := job.NewSpecBuilder(jobVersion, "job-X", "sample-owner", jobSchedule, jobWindow, jobTask).WithSpecUpstream(upstreamSpec).Build()
			jobXDestination := job.ResourceURN("resource-X")
			jobX := job.NewJob(sampleTenant, specX, jobXDestination, nil, false)

			jobRepo.On("GetByJobName", ctx, sampleTenant.ProjectName(), specC.Name()).Return(jobC, nil)

			jobWithUnresolvedUpstream := job.NewWithUpstream(jobX, []*job.Upstream{unresolvedUpstreamC})
			expectedJobWithUpstream := job.NewWithUpstream(jobX, []*job.Upstream{internalUpstreamC})

			resourceResolver.On("CheckIsDeleted", ctx, []*job.WithUpstream{expectedJobWithUpstream}).Return(nil)

			internalUpstreamResolver := resolver.NewInternalUpstreamResolver(jobRepo, resourceResolver)
			result, err := internalUpstreamResolver.Resolve(ctx, jobWithUnresolvedUpstream)
			assert.NoError(t, err)
			assert.ElementsMatch(t, expectedJobWithUpstream.Upstreams(), result.Upstreams())
		})
		t.Run("should not stop the process but keep appending error when unable to resolve inferred upstream", func(t *testing.T) {
			jobRepo := new(JobRepository)
			logWriter := new(mockWriter)
			resourceResolver := new(resourceResolverMock)
			defer logWriter.AssertExpectations(t)

			jobRepo.On("GetAllByResourceDestination", ctx, jobASources[0]).Return([]*job.Job{}, errors.New("internal error"))
			jobRepo.On("GetAllByResourceDestination", ctx, jobASources[1]).Return([]*job.Job{}, nil)

			jobRepo.On("GetByJobName", ctx, sampleTenant.ProjectName(), specC.Name()).Return(jobC, nil)

			jobWithUnresolvedUpstream := job.NewWithUpstream(jobA, []*job.Upstream{unresolvedUpstreamB, unresolvedUpstreamC, unresolvedUpstreamD})

			expectedJobWithUpstream := job.NewWithUpstream(jobA, []*job.Upstream{internalUpstreamC, unresolvedUpstreamB, unresolvedUpstreamD})

			resourceResolver.On("CheckIsDeleted", ctx, []*job.WithUpstream{expectedJobWithUpstream}).Return(nil)

			internalUpstreamResolver := resolver.NewInternalUpstreamResolver(jobRepo, resourceResolver)
			result, err := internalUpstreamResolver.Resolve(ctx, jobWithUnresolvedUpstream)
			assert.ErrorContains(t, err, "internal error")
			assert.ElementsMatch(t, expectedJobWithUpstream.Upstreams(), result.Upstreams())
		})
		t.Run("should not stop the process but keep appending error when unable to resolve static upstream", func(t *testing.T) {
			jobRepo := new(JobRepository)
			logWriter := new(mockWriter)
			resourceResolver := new(resourceResolverMock)
			defer logWriter.AssertExpectations(t)

			specEUpstreamSpec, _ := job.NewSpecUpstreamBuilder().WithUpstreamNames([]job.SpecUpstreamName{"job-unknown", "job-C"}).Build()
			specE, err := job.NewSpecBuilder(jobVersion, "job-E", "sample-owner", jobSchedule, jobWindow, jobTask).WithSpecUpstream(specEUpstreamSpec).Build()
			assert.NoError(t, err)
			jobEDestination := job.ResourceURN("resource-E")
			jobE := job.NewJob(sampleTenant, specE, jobEDestination, nil, false)

			jobRepo.On("GetByJobName", ctx, sampleTenant.ProjectName(), job.Name("job-unknown")).Return(nil, errors.New("not found"))
			jobRepo.On("GetByJobName", ctx, sampleTenant.ProjectName(), specC.Name()).Return(jobC, nil)

			unresolvedUpstreamUnknown := job.NewUpstreamUnresolvedStatic("job-unknown", sampleTenant.ProjectName())
			jobWithUnresolvedUpstream := job.NewWithUpstream(jobE, []*job.Upstream{unresolvedUpstreamUnknown, unresolvedUpstreamC})

			expectedJobWithUpstream := job.NewWithUpstream(jobE, []*job.Upstream{internalUpstreamC, unresolvedUpstreamUnknown})

			resourceResolver.On("CheckIsDeleted", ctx, []*job.WithUpstream{expectedJobWithUpstream}).Return(nil)

			internalUpstreamResolver := resolver.NewInternalUpstreamResolver(jobRepo, resourceResolver)
			result, err := internalUpstreamResolver.Resolve(ctx, jobWithUnresolvedUpstream)
			assert.ErrorContains(t, err, "not found")
			assert.ElementsMatch(t, expectedJobWithUpstream.Upstreams(), result.Upstreams())
		})
		t.Run("should not stop the process but keep appending error when static upstream name is invalid", func(t *testing.T) {
			jobRepo := new(JobRepository)

			logWriter := new(mockWriter)
			resourceResolver := new(resourceResolverMock)
			defer logWriter.AssertExpectations(t)

			specEUpstreamSpec, err := job.NewSpecUpstreamBuilder().WithUpstreamNames([]job.SpecUpstreamName{"/", "job-C"}).Build()
			assert.NoError(t, err)

			specE, err := job.NewSpecBuilder(jobVersion, "job-E", "sample-owner", jobSchedule, jobWindow, jobTask).WithSpecUpstream(specEUpstreamSpec).Build()
			assert.NoError(t, err)

			jobEDestination := job.ResourceURN("resource-E")
			jobE := job.NewJob(sampleTenant, specE, jobEDestination, nil, false)

			jobRepo.On("GetByJobName", ctx, sampleTenant.ProjectName(), specC.Name()).Return(jobC, nil)

			unresolvedUpstreamUnknown := job.NewUpstreamUnresolvedStatic("job-unknown", sampleTenant.ProjectName())
			jobWithUnresolvedUpstream := job.NewWithUpstream(jobE, []*job.Upstream{unresolvedUpstreamUnknown, unresolvedUpstreamC})

			expectedJobWithUpstream := job.NewWithUpstream(jobE, []*job.Upstream{internalUpstreamC, unresolvedUpstreamUnknown})

			resourceResolver.On("CheckIsDeleted", ctx, []*job.WithUpstream{expectedJobWithUpstream}).Return(nil)

			internalUpstreamResolver := resolver.NewInternalUpstreamResolver(jobRepo, resourceResolver)
			result, err := internalUpstreamResolver.Resolve(ctx, jobWithUnresolvedUpstream)
			assert.ErrorContains(t, err, "name is empty")
			assert.ElementsMatch(t, expectedJobWithUpstream.Upstreams(), result.Upstreams())
		})
	})
	t.Run("BulkResolve", func(t *testing.T) {
		specX, err := job.NewSpecBuilder(jobVersion, "job-X", "sample-owner", jobSchedule, jobWindow, jobTask).WithSpecUpstream(upstreamSpec).Build()
		assert.NoError(t, err)

		jobXDestination := job.ResourceURN("resource-X")
		jobX := job.NewJob(sampleTenant, specX, jobXDestination, []job.ResourceURN{"resource-B"}, false)

		t.Run("resolves upstream internally in bulk", func(t *testing.T) {
			jobRepo := new(JobRepository)
			logWriter := new(mockWriter)
			resourceResolver := new(resourceResolverMock)
			defer logWriter.AssertExpectations(t)

			internalUpstreamMap := map[job.Name][]*job.Upstream{
				"job-A": {internalUpstreamB, internalUpstreamC},
				"job-X": {internalUpstreamB, internalUpstreamC},
			}
			jobRepo.On("ResolveUpstreams", ctx, sampleTenant.ProjectName(), []job.Name{"job-A", "job-X"}).Return(internalUpstreamMap, nil)

			jobsWithUnresolvedUpstream := []*job.WithUpstream{
				job.NewWithUpstream(jobA, []*job.Upstream{unresolvedUpstreamB, unresolvedUpstreamC, unresolvedUpstreamD}),
				job.NewWithUpstream(jobX, []*job.Upstream{unresolvedUpstreamB, unresolvedUpstreamC}),
			}

			expectedJobsWithUpstream := []*job.WithUpstream{
				job.NewWithUpstream(jobA, []*job.Upstream{internalUpstreamB, internalUpstreamC, unresolvedUpstreamD}),
				job.NewWithUpstream(jobX, []*job.Upstream{internalUpstreamB, internalUpstreamC}),
			}

			resourceResolver.On("CheckIsDeleted", ctx, expectedJobsWithUpstream).Return(nil)

			internalUpstreamResolver := resolver.NewInternalUpstreamResolver(jobRepo, resourceResolver)
			result, err := internalUpstreamResolver.BulkResolve(ctx, sampleTenant.ProjectName(), jobsWithUnresolvedUpstream)
			assert.NoError(t, err)
			assert.Equal(t, expectedJobsWithUpstream[0].Job(), result[0].Job())
			assert.Equal(t, expectedJobsWithUpstream[1].Job(), result[1].Job())
			assert.ElementsMatch(t, expectedJobsWithUpstream[0].Upstreams(), result[0].Upstreams())
			assert.ElementsMatch(t, expectedJobsWithUpstream[1].Upstreams(), result[1].Upstreams())
		})

		t.Run("return error on deleted resources", func(t *testing.T) {
			jobRepo := new(JobRepository)
			logWriter := new(mockWriter)
			resourceResolver := new(resourceResolverMock)
			defer logWriter.AssertExpectations(t)

			internalUpstreamMap := map[job.Name][]*job.Upstream{
				"job-A": {internalUpstreamB, internalUpstreamC},
				"job-X": {internalUpstreamB, internalUpstreamC},
			}
			jobRepo.On("ResolveUpstreams", ctx, sampleTenant.ProjectName(), []job.Name{"job-A", "job-X"}).Return(internalUpstreamMap, nil)

			jobsWithUnresolvedUpstream := []*job.WithUpstream{
				job.NewWithUpstream(jobA, []*job.Upstream{unresolvedUpstreamB, unresolvedUpstreamC, unresolvedUpstreamD}),
				job.NewWithUpstream(jobX, []*job.Upstream{unresolvedUpstreamB, unresolvedUpstreamC}),
			}

			expectedJobsWithUpstream := []*job.WithUpstream{
				job.NewWithUpstream(jobA, []*job.Upstream{internalUpstreamB, internalUpstreamC, unresolvedUpstreamD}),
				job.NewWithUpstream(jobX, []*job.Upstream{internalUpstreamB, internalUpstreamC}),
			}

			resourceResolver.On("CheckIsDeleted", ctx, expectedJobsWithUpstream).Return(errors.New("failed precondition"))

			internalUpstreamResolver := resolver.NewInternalUpstreamResolver(jobRepo, resourceResolver)
			result, err := internalUpstreamResolver.BulkResolve(ctx, sampleTenant.ProjectName(), jobsWithUnresolvedUpstream)
			assert.Error(t, err)
			assert.Nil(t, result)
		})

		t.Run("returns error if unable to resolve upstream internally", func(t *testing.T) {
			jobRepo := new(JobRepository)
			logWriter := new(mockWriter)
			resourceResolver := new(resourceResolverMock)
			defer logWriter.AssertExpectations(t)

			jobRepo.On("ResolveUpstreams", ctx, sampleTenant.ProjectName(), []job.Name{"job-A", "job-X"}).Return(nil, errors.New("internal error"))

			jobsWithUnresolvedUpstream := []*job.WithUpstream{
				job.NewWithUpstream(jobA, []*job.Upstream{unresolvedUpstreamB, unresolvedUpstreamC, unresolvedUpstreamD}),
				job.NewWithUpstream(jobX, []*job.Upstream{unresolvedUpstreamB, unresolvedUpstreamC}),
			}

			internalUpstreamResolver := resolver.NewInternalUpstreamResolver(jobRepo, resourceResolver)
			result, err := internalUpstreamResolver.BulkResolve(ctx, sampleTenant.ProjectName(), jobsWithUnresolvedUpstream)
			assert.ErrorContains(t, err, "internal error")
			assert.Nil(t, result)
		})
	})
}
