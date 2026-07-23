package service_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/goto/optimus/core/job"
	"github.com/goto/optimus/core/job/service"
	"github.com/goto/optimus/core/resource"
	"github.com/goto/optimus/core/tenant"
	"github.com/goto/optimus/internal/lib/window"
	"github.com/goto/optimus/internal/models"
)

func TestBuildGlobalUpstreamGraph(t *testing.T) {
	ctx := context.Background()

	sampleTenant, err := tenant.NewTenant("test-proj", "test-ns")
	assert.NoError(t, err)
	otherProjectTenant, err := tenant.NewTenant("other-proj", "other-ns")
	assert.NoError(t, err)

	resourceURNA, err := resource.ParseURN("store://resource-A")
	assert.NoError(t, err)
	resourceURNB, err := resource.ParseURN("store://resource-B")
	assert.NoError(t, err)
	resourceURNC, err := resource.ParseURN("store://resource-C")
	assert.NoError(t, err)
	resourceURND, err := resource.ParseURN("store://resource-D")
	assert.NoError(t, err)

	jobVersion := 1
	startDate, err := job.ScheduleDateFrom("2022-10-01")
	assert.NoError(t, err)
	jobSchedule, err := job.NewScheduleBuilder(startDate).Build()
	assert.NoError(t, err)
	w, err := models.NewWindow(jobVersion, "d", "24h", "24h")
	assert.NoError(t, err)
	jobWindow := window.NewCustomConfig(w)
	taskName, err := job.TaskNameFrom("sample-task")
	assert.NoError(t, err)
	jobTask := job.NewTask(taskName, map[string]string{"key": "value"}, "", nil)

	newJobA := func(tnnt tenant.Tenant, destination resource.URN) *job.Job {
		spec, buildErr := job.NewSpecBuilder(jobVersion, "jobA", "owner", jobSchedule, jobWindow, jobTask).Build()
		assert.NoError(t, buildErr)
		return job.NewJob(tnnt, spec, destination, nil, false)
	}

	fullNameA := job.FullNameFrom(sampleTenant.ProjectName(), "jobA")

	t.Run("full-replaces an incoming job's edges with its freshly-resolved upstreams, keeping unrelated persisted edges", func(t *testing.T) {
		jobRepo := new(JobRepository)
		defer jobRepo.AssertExpectations(t)
		upstreamRepo := new(UpstreamRepository)
		defer upstreamRepo.AssertExpectations(t)
		downstreamRepo := new(DownstreamRepository)
		defer downstreamRepo.AssertExpectations(t)

		jobA := newJobA(sampleTenant, resourceURNA)
		resolvedUpstreamB := job.NewUpstreamResolved("jobB", "", resourceURNB, sampleTenant, "static", taskName, false)
		jobAWithUpstream := job.NewWithUpstream(jobA, []*job.Upstream{resolvedUpstreamB})

		unrelatedFullName := job.FullNameFrom(sampleTenant.ProjectName(), "unrelatedJob")
		fullNameB := job.FullNameFrom(sampleTenant.ProjectName(), "jobB")

		// the persisted graph already has a stale edge for jobA (pointing somewhere else) plus an
		// entirely unrelated edge - the stale jobA edge must be replaced, the unrelated one kept.
		upstreamRepo.On("GetAllResolvedUpstreamEdges", ctx).Return(map[job.FullName][]job.FullName{
			fullNameA:         {job.FullNameFrom(sampleTenant.ProjectName(), "staleUpstream")},
			unrelatedFullName: {fullNameB},
		}, nil)
		downstreamRepo.On("GetDownstreamByDestination", ctx, resourceURNA).Return(nil, nil)

		svc := service.NewJobService(jobRepo, upstreamRepo, downstreamRepo, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, service.JobValidateConfig{})

		graph, err := svc.BuildGlobalUpstreamGraph(ctx, []*job.Job{jobA}, []*job.WithUpstream{jobAWithUpstream})

		assert.NoError(t, err)
		assert.Equal(t, job.UpstreamGraph{
			fullNameA:         {fullNameB},
			unrelatedFullName: {fullNameB},
		}, graph)
	})

	t.Run("matches an unresolved inferred upstream against a sibling incoming job's destination", func(t *testing.T) {
		jobRepo := new(JobRepository)
		defer jobRepo.AssertExpectations(t)
		upstreamRepo := new(UpstreamRepository)
		defer upstreamRepo.AssertExpectations(t)
		downstreamRepo := new(DownstreamRepository)
		defer downstreamRepo.AssertExpectations(t)

		jobA := newJobA(sampleTenant, resourceURNA)
		specB, err := job.NewSpecBuilder(jobVersion, "jobB", "owner", jobSchedule, jobWindow, jobTask).Build()
		assert.NoError(t, err)
		jobB := job.NewJob(sampleTenant, specB, resourceURNB, nil, false)

		// jobA's inferred upstream isn't resolved yet (no owning job found via BulkResolve), but
		// its destination URN matches jobB, which is also part of this same incoming batch.
		unresolvedUpstream := job.NewUpstreamUnresolvedInferred(resourceURNB)
		jobAWithUpstream := job.NewWithUpstream(jobA, []*job.Upstream{unresolvedUpstream})
		jobBWithUpstream := job.NewWithUpstream(jobB, []*job.Upstream{})

		fullNameB := job.FullNameFrom(sampleTenant.ProjectName(), "jobB")

		upstreamRepo.On("GetAllResolvedUpstreamEdges", ctx).Return(map[job.FullName][]job.FullName{}, nil)
		downstreamRepo.On("GetDownstreamByDestination", ctx, resourceURNA).Return(nil, nil)
		downstreamRepo.On("GetDownstreamByDestination", ctx, resourceURNB).Return(nil, nil)

		svc := service.NewJobService(jobRepo, upstreamRepo, downstreamRepo, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, service.JobValidateConfig{})

		graph, err := svc.BuildGlobalUpstreamGraph(ctx, []*job.Job{jobA, jobB}, []*job.WithUpstream{jobAWithUpstream, jobBWithUpstream})

		assert.NoError(t, err)
		assert.Equal(t, []job.FullName{fullNameB}, graph[fullNameA])
		// jobB itself has no upstreams, but must still appear (with an empty/no edge list) since
		// it's part of the incoming batch and overlay 1 always sets its entry.
		assert.Empty(t, graph[fullNameB])
	})

	t.Run("falls back to an existing job producing the destination when no sibling incoming job matches", func(t *testing.T) {
		jobRepo := new(JobRepository)
		defer jobRepo.AssertExpectations(t)
		upstreamRepo := new(UpstreamRepository)
		defer upstreamRepo.AssertExpectations(t)
		downstreamRepo := new(DownstreamRepository)
		defer downstreamRepo.AssertExpectations(t)

		jobA := newJobA(sampleTenant, resourceURNA)
		unresolvedUpstream := job.NewUpstreamUnresolvedInferred(resourceURNC)
		jobAWithUpstream := job.NewWithUpstream(jobA, []*job.Upstream{unresolvedUpstream})

		specC, err := job.NewSpecBuilder(jobVersion, "jobC", "owner", jobSchedule, jobWindow, jobTask).Build()
		assert.NoError(t, err)
		jobC := job.NewJob(sampleTenant, specC, resourceURNC, nil, false)

		upstreamRepo.On("GetAllResolvedUpstreamEdges", ctx).Return(map[job.FullName][]job.FullName{}, nil)
		downstreamRepo.On("GetDownstreamByDestination", ctx, resourceURNA).Return(nil, nil)
		jobRepo.On("GetAllByResourceDestination", ctx, resourceURNC).Return([]*job.Job{jobC}, nil)

		svc := service.NewJobService(jobRepo, upstreamRepo, downstreamRepo, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, service.JobValidateConfig{})

		graph, err := svc.BuildGlobalUpstreamGraph(ctx, []*job.Job{jobA}, []*job.WithUpstream{jobAWithUpstream})

		assert.NoError(t, err)
		assert.Equal(t, []job.FullName{job.FullNameFrom(sampleTenant.ProjectName(), "jobC")}, graph[fullNameA])
	})

	t.Run("drops an unresolved inferred upstream whose destination no job anywhere currently produces", func(t *testing.T) {
		jobRepo := new(JobRepository)
		defer jobRepo.AssertExpectations(t)
		upstreamRepo := new(UpstreamRepository)
		defer upstreamRepo.AssertExpectations(t)
		downstreamRepo := new(DownstreamRepository)
		defer downstreamRepo.AssertExpectations(t)

		jobA := newJobA(sampleTenant, resourceURNA)
		unresolvedUpstream := job.NewUpstreamUnresolvedInferred(resourceURND)
		jobAWithUpstream := job.NewWithUpstream(jobA, []*job.Upstream{unresolvedUpstream})

		upstreamRepo.On("GetAllResolvedUpstreamEdges", ctx).Return(map[job.FullName][]job.FullName{}, nil)
		downstreamRepo.On("GetDownstreamByDestination", ctx, resourceURNA).Return(nil, nil)
		jobRepo.On("GetAllByResourceDestination", ctx, resourceURND).Return([]*job.Job{}, nil)

		svc := service.NewJobService(jobRepo, upstreamRepo, downstreamRepo, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, service.JobValidateConfig{})

		graph, err := svc.BuildGlobalUpstreamGraph(ctx, []*job.Job{jobA}, []*job.WithUpstream{jobAWithUpstream})

		assert.NoError(t, err)
		assert.Empty(t, graph[fullNameA])
	})

	t.Run("skips external upstreams entirely", func(t *testing.T) {
		jobRepo := new(JobRepository)
		defer jobRepo.AssertExpectations(t)
		upstreamRepo := new(UpstreamRepository)
		defer upstreamRepo.AssertExpectations(t)
		downstreamRepo := new(DownstreamRepository)
		defer downstreamRepo.AssertExpectations(t)

		jobA := newJobA(sampleTenant, resourceURNA)
		externalUpstream := job.NewUpstreamResolved("externalJob", "some-host", resourceURNB, otherProjectTenant, "static", taskName, true)
		jobAWithUpstream := job.NewWithUpstream(jobA, []*job.Upstream{externalUpstream})

		upstreamRepo.On("GetAllResolvedUpstreamEdges", ctx).Return(map[job.FullName][]job.FullName{}, nil)
		downstreamRepo.On("GetDownstreamByDestination", ctx, resourceURNA).Return(nil, nil)

		svc := service.NewJobService(jobRepo, upstreamRepo, downstreamRepo, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, service.JobValidateConfig{})

		graph, err := svc.BuildGlobalUpstreamGraph(ctx, []*job.Job{jobA}, []*job.WithUpstream{jobAWithUpstream})

		assert.NoError(t, err)
		assert.Empty(t, graph[fullNameA])
	})

	t.Run("adds a reverse-impact edge from an existing downstream job that is not part of the incoming batch", func(t *testing.T) {
		jobRepo := new(JobRepository)
		defer jobRepo.AssertExpectations(t)
		upstreamRepo := new(UpstreamRepository)
		defer upstreamRepo.AssertExpectations(t)
		downstreamRepo := new(DownstreamRepository)
		defer downstreamRepo.AssertExpectations(t)

		jobA := newJobA(sampleTenant, resourceURNA)
		jobAWithUpstream := job.NewWithUpstream(jobA, []*job.Upstream{})

		existingDownstreamC := job.NewDownstream("jobC", sampleTenant.ProjectName(), sampleTenant.NamespaceName(), taskName)

		upstreamRepo.On("GetAllResolvedUpstreamEdges", ctx).Return(map[job.FullName][]job.FullName{}, nil)
		downstreamRepo.On("GetDownstreamByDestination", ctx, resourceURNA).Return([]*job.Downstream{existingDownstreamC}, nil)

		svc := service.NewJobService(jobRepo, upstreamRepo, downstreamRepo, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, service.JobValidateConfig{})

		graph, err := svc.BuildGlobalUpstreamGraph(ctx, []*job.Job{jobA}, []*job.WithUpstream{jobAWithUpstream})

		assert.NoError(t, err)
		fullNameC := job.FullNameFrom(sampleTenant.ProjectName(), "jobC")
		assert.Equal(t, []job.FullName{fullNameA}, graph[fullNameC])
	})

	t.Run("does not apply a reverse-impact edge onto a job that is itself part of the incoming batch", func(t *testing.T) {
		jobRepo := new(JobRepository)
		defer jobRepo.AssertExpectations(t)
		upstreamRepo := new(UpstreamRepository)
		defer upstreamRepo.AssertExpectations(t)
		downstreamRepo := new(DownstreamRepository)
		defer downstreamRepo.AssertExpectations(t)

		jobA := newJobA(sampleTenant, resourceURNA)
		specB, err := job.NewSpecBuilder(jobVersion, "jobB", "owner", jobSchedule, jobWindow, jobTask).Build()
		assert.NoError(t, err)
		jobB := job.NewJob(sampleTenant, specB, resourceURNB, nil, false)

		jobAWithUpstream := job.NewWithUpstream(jobA, []*job.Upstream{})
		jobBWithUpstream := job.NewWithUpstream(jobB, []*job.Upstream{})

		// jobB "gains" a downstream edge to jobA per GetDownstreamByDestination, but jobB is also
		// part of the incoming batch - overlay 1 already gave it a complete edge list (empty), so
		// overlay 2 must not additively touch it.
		jobBAsDownstream := job.NewDownstream("jobB", sampleTenant.ProjectName(), sampleTenant.NamespaceName(), taskName)

		upstreamRepo.On("GetAllResolvedUpstreamEdges", ctx).Return(map[job.FullName][]job.FullName{}, nil)
		downstreamRepo.On("GetDownstreamByDestination", ctx, resourceURNA).Return([]*job.Downstream{jobBAsDownstream}, nil)
		downstreamRepo.On("GetDownstreamByDestination", ctx, resourceURNB).Return(nil, nil)

		svc := service.NewJobService(jobRepo, upstreamRepo, downstreamRepo, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, service.JobValidateConfig{})

		graph, err := svc.BuildGlobalUpstreamGraph(ctx, []*job.Job{jobA, jobB}, []*job.WithUpstream{jobAWithUpstream, jobBWithUpstream})

		assert.NoError(t, err)
		fullNameB := job.FullNameFrom(sampleTenant.ProjectName(), "jobB")
		assert.Empty(t, graph[fullNameB])
	})

	t.Run("skips the reverse-impact lookup entirely for an incoming job with no destination", func(t *testing.T) {
		jobRepo := new(JobRepository)
		defer jobRepo.AssertExpectations(t)
		upstreamRepo := new(UpstreamRepository)
		defer upstreamRepo.AssertExpectations(t)
		downstreamRepo := new(DownstreamRepository)
		defer downstreamRepo.AssertExpectations(t)

		jobA := newJobA(sampleTenant, resource.ZeroURN())
		jobAWithUpstream := job.NewWithUpstream(jobA, []*job.Upstream{})

		upstreamRepo.On("GetAllResolvedUpstreamEdges", ctx).Return(map[job.FullName][]job.FullName{}, nil)

		svc := service.NewJobService(jobRepo, upstreamRepo, downstreamRepo, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, service.JobValidateConfig{})

		graph, err := svc.BuildGlobalUpstreamGraph(ctx, []*job.Job{jobA}, []*job.WithUpstream{jobAWithUpstream})

		assert.NoError(t, err)
		assert.Empty(t, graph[fullNameA])
	})

	t.Run("propagates an error from the global persisted graph fetch", func(t *testing.T) {
		jobRepo := new(JobRepository)
		defer jobRepo.AssertExpectations(t)
		upstreamRepo := new(UpstreamRepository)
		defer upstreamRepo.AssertExpectations(t)
		downstreamRepo := new(DownstreamRepository)
		defer downstreamRepo.AssertExpectations(t)

		jobA := newJobA(sampleTenant, resourceURNA)
		jobAWithUpstream := job.NewWithUpstream(jobA, []*job.Upstream{})

		upstreamRepo.On("GetAllResolvedUpstreamEdges", ctx).Return(nil, errors.New("db error"))

		svc := service.NewJobService(jobRepo, upstreamRepo, downstreamRepo, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, service.JobValidateConfig{})

		graph, err := svc.BuildGlobalUpstreamGraph(ctx, []*job.Job{jobA}, []*job.WithUpstream{jobAWithUpstream})

		assert.Error(t, err)
		assert.Nil(t, graph)
	})
}
