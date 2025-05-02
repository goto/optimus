package resolver_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/goto/optimus/core/job"
	"github.com/goto/optimus/core/job/resolver"
	"github.com/goto/optimus/core/resource"
	"github.com/goto/optimus/core/tenant"
	"github.com/goto/optimus/internal/lib/window"
	"github.com/goto/optimus/internal/writer"
)

func TestUpstreamResolver(t *testing.T) {
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
	sampleTenant, _ := tenant.NewTenant(project.Name().String(), namespace.Name().String())
	externalTenant, _ := tenant.NewTenant("external-proj", "external-namespace")
	jobVersion := 1
	startDate, err := job.ScheduleDateFrom("2022-10-01")
	assert.NoError(t, err)
	jobSchedule, err := job.NewScheduleBuilder(startDate).Build()
	assert.NoError(t, err)
	jobWindow, err := window.NewConfig("1d", "1d", "", "")
	assert.NoError(t, err)
	jobTaskConfig, err := job.ConfigFrom(map[string]string{"sample_task_key": "sample_value"})
	assert.NoError(t, err)
	taskName, _ := job.TaskNameFrom("sample-task")
	jobTask := job.NewTask(taskName, jobTaskConfig)
	sampleOwner := "sample-owner"

	resourceURNA, err := resource.ParseURN("store://resource-A")
	assert.NoError(t, err)
	resourceURNB, err := resource.ParseURN("store://resource-B")
	assert.NoError(t, err)
	resourceURNC, err := resource.ParseURN("store://resource-C")
	assert.NoError(t, err)
	resourceURND, err := resource.ParseURN("store://resource-D")
	assert.NoError(t, err)
	resourceURNF, err := resource.ParseURN("store://resource-F")
	assert.NoError(t, err)
	resourceURNG, err := resource.ParseURN("store://resource-G")
	assert.NoError(t, err)

	t.Run("CheckStaticResolvable", func(t *testing.T) {
		specA, err := job.NewSpecBuilder(jobVersion, "job-A", sampleOwner, jobSchedule, jobWindow, jobTask).Build()
		assert.NoError(t, err)

		upstreamAName := job.SpecUpstreamName("job-A")
		upstreamASpec, err := job.NewSpecUpstreamBuilder().WithUpstreamNames([]job.SpecUpstreamName{upstreamAName}).Build()
		assert.NoError(t, err)
		specB, err := job.NewSpecBuilder(jobVersion, "job-B", sampleOwner, jobSchedule, jobWindow, jobTask).WithSpecUpstream(upstreamASpec).Build()
		assert.NoError(t, err)

		t.Run("ignore unresolved inferred upstream", func(t *testing.T) {
			externalUpstreamResolver := new(ExternalUpstreamResolver)
			internalUpstreamResolver := new(InternalUpstreamResolver)

			logWriter := new(mockWriter)
			defer logWriter.AssertExpectations(t)

			jobBDestination := resourceURNB
			jobBUpstreams := []resource.URN{resourceURNG} // unresolved inferred dependency
			jobB := job.NewJob(sampleTenant, specB, jobBDestination, jobBUpstreams, true)

			jobs := []*job.Job{jobB}

			upstreamB1 := job.NewUpstreamUnresolvedInferred(jobBUpstreams[0])
			upstreamsB := []*job.Upstream{upstreamB1}
			jobBWithUpstream := job.NewWithUpstream(jobB, upstreamsB)

			internalUpstreamResolver.On("BulkResolve", ctx, project.Name(), mock.Anything).Return([]*job.WithUpstream{jobBWithUpstream}, nil)

			externalUpstreamResolver.On("BulkResolve", ctx, mock.Anything, mock.Anything).Return([]*job.WithUpstream{jobBWithUpstream}, nil, nil)

			upstreamResolver := resolver.NewUpstreamResolver(nil, externalUpstreamResolver, internalUpstreamResolver)
			err = upstreamResolver.CheckStaticResolvable(ctx, sampleTenant, jobs, logWriter)
			assert.NoError(t, err)
		})
		t.Run("raise error if unresolved static upstream", func(t *testing.T) {
			externalUpstreamResolver := new(ExternalUpstreamResolver)
			internalUpstreamResolver := new(InternalUpstreamResolver)

			logWriter := new(mockWriter)
			defer logWriter.AssertExpectations(t)

			jobBDestination := resourceURNB
			jobBUpstreams := []resource.URN{resourceURNG} // unresolved inferred dependency
			jobB := job.NewJob(sampleTenant, specB, jobBDestination, jobBUpstreams, true)

			jobs := []*job.Job{jobB}

			upstreamB0 := job.NewUpstreamUnresolvedStatic(specA.Name(), sampleTenant.ProjectName())
			upstreamB1 := job.NewUpstreamUnresolvedInferred(jobBUpstreams[0])
			upstreamsB := []*job.Upstream{upstreamB0, upstreamB1}
			jobBWithUpstream := job.NewWithUpstream(jobB, upstreamsB)

			internalUpstreamResolver.On("BulkResolve", ctx, project.Name(), mock.Anything).Return([]*job.WithUpstream{jobBWithUpstream}, nil)

			externalUpstreamResolver.On("BulkResolve", ctx, mock.Anything, mock.Anything).Return([]*job.WithUpstream{jobBWithUpstream}, nil, nil)

			upstreamResolver := resolver.NewUpstreamResolver(nil, externalUpstreamResolver, internalUpstreamResolver)
			err = upstreamResolver.CheckStaticResolvable(ctx, sampleTenant, jobs, logWriter)
			assert.ErrorContains(t, err, "invalid state for entity job: could not resolve for static upstream: test-proj/job-A, for job: job-B")
		})
		t.Run("report no error if all upstreams are resolved", func(t *testing.T) {
			externalUpstreamResolver := new(ExternalUpstreamResolver)
			internalUpstreamResolver := new(InternalUpstreamResolver)

			logWriter := new(mockWriter)
			defer logWriter.AssertExpectations(t)

			jobBDestination := resourceURNB
			jobBUpstreams := []resource.URN{resourceURNG} // unresolved inferred dependency
			jobB := job.NewJob(sampleTenant, specB, jobBDestination, jobBUpstreams, true)

			jobs := []*job.Job{jobB}

			upstreamB0 := job.NewUpstreamResolved(specA.Name(), "", resourceURNB, sampleTenant, job.UpstreamTypeStatic, taskName, false)
			upstreamsB := []*job.Upstream{upstreamB0}
			jobBWithUpstream := job.NewWithUpstream(jobB, upstreamsB)

			internalUpstreamResolver.On("BulkResolve", ctx, project.Name(), mock.Anything).Return([]*job.WithUpstream{jobBWithUpstream}, nil)

			externalUpstreamResolver.On("BulkResolve", ctx, mock.Anything, mock.Anything).Return([]*job.WithUpstream{jobBWithUpstream}, nil, nil)

			upstreamResolver := resolver.NewUpstreamResolver(nil, externalUpstreamResolver, internalUpstreamResolver)
			err = upstreamResolver.CheckStaticResolvable(ctx, sampleTenant, jobs, logWriter)
			assert.NoError(t, err)
		})
		t.Run("handle incoming static upstream if static upstream does not already exist in DB, but is incoming in specs", func(t *testing.T) {
			externalUpstreamResolver := new(ExternalUpstreamResolver)
			internalUpstreamResolver := new(InternalUpstreamResolver)

			logWriter := new(mockWriter)
			defer logWriter.AssertExpectations(t)

			jobADestination := resourceURNA
			jobAUpstreams := []resource.URN{resourceURNF} // unresolved inferred dependency
			jobA := job.NewJob(sampleTenant, specA, jobADestination, jobAUpstreams, false)

			jobBDestination := resourceURNB
			jobBUpstreams := []resource.URN{resourceURNG} // unresolved inferred dependency
			jobB := job.NewJob(sampleTenant, specB, jobBDestination, jobBUpstreams, true)

			jobs := []*job.Job{jobA, jobB}

			upstreamA := job.NewUpstreamUnresolvedInferred(jobAUpstreams[0])
			upstreams := []*job.Upstream{upstreamA}
			jobAWithUpstream := job.NewWithUpstream(jobA, upstreams)

			upstreamB0 := job.NewUpstreamUnresolvedStatic(jobA.Spec().Name(), sampleTenant.ProjectName())
			upstreamB1 := job.NewUpstreamUnresolvedInferred(jobBUpstreams[0])
			upstreamsB := []*job.Upstream{upstreamB0, upstreamB1}
			jobBWithUpstream := job.NewWithUpstream(jobB, upstreamsB)

			internalUpstreamResolver.On("BulkResolve", ctx, project.Name(), mock.Anything).Return([]*job.WithUpstream{jobAWithUpstream, jobBWithUpstream}, nil)

			externalUpstreamResolver.On("BulkResolve", ctx, mock.Anything, mock.Anything).Return([]*job.WithUpstream{jobAWithUpstream, jobBWithUpstream}, nil, nil)

			logWriter.On("Write", mock.Anything, mock.Anything).Return(nil)

			upstreamResolver := resolver.NewUpstreamResolver(nil, externalUpstreamResolver, internalUpstreamResolver)
			err = upstreamResolver.CheckStaticResolvable(ctx, sampleTenant, jobs, logWriter)
			assert.NoError(t, err)
		})
		t.Run("return errors coming from internal and external Bulk resolve", func(t *testing.T) {
			externalUpstreamResolver := new(ExternalUpstreamResolver)
			internalUpstreamResolver := new(InternalUpstreamResolver)

			logWriter := new(mockWriter)
			defer logWriter.AssertExpectations(t)

			jobADestination := resourceURNA
			jobAUpstreams := []resource.URN{resourceURNF} // unresolved inferred dependency
			jobA := job.NewJob(sampleTenant, specA, jobADestination, jobAUpstreams, false)

			jobBDestination := resourceURNB
			jobBUpstreams := []resource.URN{resourceURNG} // unresolved inferred dependency
			jobB := job.NewJob(sampleTenant, specB, jobBDestination, jobBUpstreams, true)

			jobs := []*job.Job{jobA, jobB}

			upstreamA := job.NewUpstreamUnresolvedInferred(jobAUpstreams[0])
			upstreams := []*job.Upstream{upstreamA}
			jobAWithUpstream := job.NewWithUpstream(jobA, upstreams)

			upstreamB0 := job.NewUpstreamUnresolvedStatic(jobA.Spec().Name(), sampleTenant.ProjectName())
			upstreamB1 := job.NewUpstreamUnresolvedInferred(jobBUpstreams[0])
			upstreamsB := []*job.Upstream{upstreamB0, upstreamB1}
			jobBWithUpstream := job.NewWithUpstream(jobB, upstreamsB)

			internalUpstreamResolver.On("BulkResolve", ctx, project.Name(), mock.Anything).Return([]*job.WithUpstream{jobAWithUpstream, jobBWithUpstream}, errors.New("error in Internal bulkResolve"))

			externalUpstreamResolver.On("BulkResolve", ctx, mock.Anything, mock.Anything).Return([]*job.WithUpstream{jobAWithUpstream, jobBWithUpstream}, errors.New("error in External bulkResolve"))

			logWriter.On("Write", mock.Anything, mock.Anything).Return(nil)

			upstreamResolver := resolver.NewUpstreamResolver(nil, externalUpstreamResolver, internalUpstreamResolver)
			err = upstreamResolver.CheckStaticResolvable(ctx, sampleTenant, jobs, logWriter)
			assert.ErrorContains(t, err, "error in Internal bulkResolve")
			assert.ErrorContains(t, err, "error in External bulkResolve")
		})
	})

	t.Run("BulkResolve", func(t *testing.T) {
		t.Run("resolve upstream internally", func(t *testing.T) {
			jobRepo := new(JobRepository)
			externalUpstreamResolver := new(ExternalUpstreamResolver)
			internalUpstreamResolver := new(InternalUpstreamResolver)

			logWriter := new(mockWriter)
			defer logWriter.AssertExpectations(t)

			upstreamName := job.SpecUpstreamName("test-proj/job-c")
			upstreamSpec, err := job.NewSpecUpstreamBuilder().WithUpstreamNames([]job.SpecUpstreamName{upstreamName}).Build()
			assert.NoError(t, err)

			specA, err := job.NewSpecBuilder(jobVersion, "job-A", sampleOwner, jobSchedule, jobWindow, jobTask).WithSpecUpstream(upstreamSpec).Build()
			assert.NoError(t, err)

			jobADestination := resourceURNA
			jobAUpstreams := []resource.URN{resourceURNB}
			jobA := job.NewJob(sampleTenant, specA, jobADestination, jobAUpstreams, false)
			jobs := []*job.Job{jobA}

			upstreamB := job.NewUpstreamResolved("job-B", "", resourceURNB, sampleTenant, "inferred", taskName, false)
			upstreamC := job.NewUpstreamResolved("job-C", "", resourceURNC, sampleTenant, "static", taskName, false)
			upstreams := []*job.Upstream{upstreamB, upstreamC}
			jobAWithUpstream := job.NewWithUpstream(jobA, upstreams)

			internalUpstreamResolver.On("BulkResolve", ctx, project.Name(), mock.Anything).Return([]*job.WithUpstream{jobAWithUpstream}, nil)

			externalUpstreamResolver.On("BulkResolve", ctx, mock.Anything, mock.Anything).Return([]*job.WithUpstream{jobAWithUpstream}, nil, nil)

			expectedJobWitUpstreams := []*job.WithUpstream{job.NewWithUpstream(jobA, []*job.Upstream{upstreamB, upstreamC})}

			upstreamResolver := resolver.NewUpstreamResolver(jobRepo, externalUpstreamResolver, internalUpstreamResolver)
			result, err := upstreamResolver.BulkResolve(ctx, project.Name(), jobs, logWriter)
			assert.NoError(t, err)
			assert.EqualValues(t, expectedJobWitUpstreams, result)
		})
		t.Run("resolve upstream internally and externally", func(t *testing.T) {
			jobRepo := new(JobRepository)
			externalUpstreamResolver := new(ExternalUpstreamResolver)
			internalUpstreamResolver := new(InternalUpstreamResolver)

			logWriter := new(mockWriter)
			defer logWriter.AssertExpectations(t)

			upstreamName := job.SpecUpstreamNameFrom("job-c")
			upstreamSpec, err := job.NewSpecUpstreamBuilder().WithUpstreamNames([]job.SpecUpstreamName{upstreamName}).Build()
			assert.NoError(t, err)

			specA, err := job.NewSpecBuilder(jobVersion, "job-A", sampleOwner, jobSchedule, jobWindow, jobTask).WithSpecUpstream(upstreamSpec).Build()
			assert.NoError(t, err)

			jobADestination := resourceURNA
			jobAUpstreams := []resource.URN{resourceURNB, resourceURND}
			jobA := job.NewJob(sampleTenant, specA, jobADestination, jobAUpstreams, false)
			jobs := []*job.Job{jobA}

			unresolvedUpstreams := []*job.Upstream{
				job.NewUpstreamUnresolvedInferred(resourceURNB),
				job.NewUpstreamUnresolvedStatic("job-c", project.Name()),
				job.NewUpstreamUnresolvedInferred(resourceURND),
			}

			internalUpstream := job.NewUpstreamResolved("job-B", "", resourceURNB, sampleTenant, "static", taskName, false)
			jobWithInternalUpstreams := job.NewWithUpstream(jobA, []*job.Upstream{internalUpstream, unresolvedUpstreams[1], unresolvedUpstreams[2]})
			internalUpstreamResolver.On("BulkResolve", ctx, project.Name(), mock.Anything).Return([]*job.WithUpstream{jobWithInternalUpstreams}, nil)

			externalUpstreamC := job.NewUpstreamResolved("job-C", "external-host", resourceURNC, externalTenant, "static", taskName, true)
			externalUpstreamD := job.NewUpstreamResolved("job-D", "external-host", resourceURND, externalTenant, "inferred", taskName, true)
			jobWithExternalUpstreams := job.NewWithUpstream(jobA, []*job.Upstream{internalUpstream, externalUpstreamC, externalUpstreamD})
			externalUpstreamResolver.On("BulkResolve", ctx, mock.Anything, mock.Anything).Return([]*job.WithUpstream{jobWithExternalUpstreams}, nil)

			expectedJobWitUpstreams := []*job.WithUpstream{
				job.NewWithUpstream(jobA, []*job.Upstream{internalUpstream, externalUpstreamC, externalUpstreamD}),
			}

			upstreamResolver := resolver.NewUpstreamResolver(jobRepo, externalUpstreamResolver, internalUpstreamResolver)
			result, err := upstreamResolver.BulkResolve(ctx, project.Name(), jobs, logWriter)
			assert.NoError(t, err)
			assert.EqualValues(t, expectedJobWitUpstreams, result)
		})
		t.Run("returns error when unable to get internal upstreams", func(t *testing.T) {
			jobRepo := new(JobRepository)
			externalUpstreamResolver := new(ExternalUpstreamResolver)
			internalUpstreamResolver := new(InternalUpstreamResolver)

			logWriter := new(mockWriter)
			defer logWriter.AssertExpectations(t)

			specA, err := job.NewSpecBuilder(jobVersion, "job-A", sampleOwner, jobSchedule, jobWindow, jobTask).Build()
			assert.NoError(t, err)

			jobADestination := resourceURNA
			jobAUpstreams := []resource.URN{resourceURNB}
			jobA := job.NewJob(sampleTenant, specA, jobADestination, jobAUpstreams, false)
			jobs := []*job.Job{jobA}

			internalUpstreamResolver.On("BulkResolve", ctx, project.Name(), mock.Anything).Return(nil, errors.New("internal error"))

			logWriter.On("Write", mock.Anything, mock.Anything).Return(nil)

			upstreamResolver := resolver.NewUpstreamResolver(jobRepo, externalUpstreamResolver, internalUpstreamResolver)
			result, err := upstreamResolver.BulkResolve(ctx, project.Name(), jobs, logWriter)
			assert.ErrorContains(t, err, "internal error")
			assert.Nil(t, result)
		})
		t.Run("returns upstream error when there is unresolved static upstream", func(t *testing.T) {
			jobRepo := new(JobRepository)
			externalUpstreamResolver := new(ExternalUpstreamResolver)
			internalUpstreamResolver := new(InternalUpstreamResolver)

			logWriter := new(mockWriter)
			defer logWriter.AssertExpectations(t)

			upstreamName := job.SpecUpstreamNameFrom("job-c")
			upstreamSpec, err := job.NewSpecUpstreamBuilder().WithUpstreamNames([]job.SpecUpstreamName{upstreamName}).Build()
			assert.NoError(t, err)

			specA, err := job.NewSpecBuilder(jobVersion, "job-A", sampleOwner, jobSchedule, jobWindow, jobTask).WithSpecUpstream(upstreamSpec).Build()
			assert.NoError(t, err)

			jobADestination := resourceURNA
			jobAUpstreams := []resource.URN{resourceURNB, resourceURND}

			jobA := job.NewJob(sampleTenant, specA, jobADestination, jobAUpstreams, false)
			jobs := []*job.Job{jobA}

			unresolvedUpstreams := []*job.Upstream{
				job.NewUpstreamUnresolvedInferred(resourceURNB),
				job.NewUpstreamUnresolvedStatic("job-c", project.Name()),
				job.NewUpstreamUnresolvedInferred(resourceURND),
			}

			internalUpstream := job.NewUpstreamResolved("job-B", "", resourceURNB, sampleTenant, "static", taskName, false)
			jobWithInternalUpstreams := job.NewWithUpstream(jobA, []*job.Upstream{internalUpstream, unresolvedUpstreams[1], unresolvedUpstreams[2]})
			internalUpstreamResolver.On("BulkResolve", ctx, project.Name(), mock.Anything).Return([]*job.WithUpstream{jobWithInternalUpstreams}, nil)

			externalUpstreamD := job.NewUpstreamResolved("job-D", "external-host", resourceURND, externalTenant, "inferred", taskName, true)
			jobWithExternalUpstreams := job.NewWithUpstream(jobA, []*job.Upstream{internalUpstream, unresolvedUpstreams[1], externalUpstreamD})
			externalUpstreamResolver.On("BulkResolve", ctx, mock.Anything, mock.Anything).Return([]*job.WithUpstream{jobWithExternalUpstreams}, nil)

			logWriter.On("Write", mock.Anything, mock.Anything).Return(nil)

			upstreamResolver := resolver.NewUpstreamResolver(jobRepo, externalUpstreamResolver, internalUpstreamResolver)
			result, err := upstreamResolver.BulkResolve(ctx, project.Name(), jobs, logWriter)
			assert.Error(t, err)
			assert.EqualValues(t, []*job.WithUpstream{jobWithExternalUpstreams}, result)
		})
		t.Run("returns upstream error when encounter error on fetching external upstreams", func(t *testing.T) {
			jobRepo := new(JobRepository)
			externalUpstreamResolver := new(ExternalUpstreamResolver)
			internalUpstreamResolver := new(InternalUpstreamResolver)

			logWriter := new(mockWriter)
			defer logWriter.AssertExpectations(t)

			upstreamName := job.SpecUpstreamNameFrom("job-c")
			upstreamSpec, err := job.NewSpecUpstreamBuilder().WithUpstreamNames([]job.SpecUpstreamName{upstreamName}).Build()
			assert.NoError(t, err)

			specA, err := job.NewSpecBuilder(jobVersion, "job-A", sampleOwner, jobSchedule, jobWindow, jobTask).WithSpecUpstream(upstreamSpec).Build()
			assert.NoError(t, err)

			jobADestination := resourceURNA
			jobAUpstreams := []resource.URN{resourceURNB, resourceURND}

			jobA := job.NewJob(sampleTenant, specA, jobADestination, jobAUpstreams, false)
			jobs := []*job.Job{jobA}

			unresolvedUpstreams := []*job.Upstream{
				job.NewUpstreamUnresolvedInferred(resourceURNB),
				job.NewUpstreamUnresolvedStatic("job-c", project.Name()),
				job.NewUpstreamUnresolvedInferred(resourceURND),
			}

			internalUpstream := job.NewUpstreamResolved("job-B", "", resourceURNB, sampleTenant, "static", taskName, false)
			jobWithInternalUpstreams := job.NewWithUpstream(jobA, []*job.Upstream{internalUpstream, unresolvedUpstreams[1], unresolvedUpstreams[2]})
			internalUpstreamResolver.On("BulkResolve", ctx, project.Name(), mock.Anything).Return([]*job.WithUpstream{jobWithInternalUpstreams}, nil)

			externalUpstreamD := job.NewUpstreamResolved("job-D", "external-host", resourceURND, externalTenant, "inferred", taskName, true)
			jobWithExternalUpstreams := job.NewWithUpstream(jobA, []*job.Upstream{internalUpstream, unresolvedUpstreams[1], externalUpstreamD})
			externalUpstreamResolver.On("BulkResolve", ctx, mock.Anything, mock.Anything).Return([]*job.WithUpstream{jobWithExternalUpstreams}, errors.New("internal error"))

			logWriter.On("Write", mock.Anything, mock.Anything).Return(nil)

			upstreamResolver := resolver.NewUpstreamResolver(jobRepo, externalUpstreamResolver, internalUpstreamResolver)
			result, err := upstreamResolver.BulkResolve(ctx, project.Name(), jobs, logWriter)
			assert.ErrorContains(t, err, "internal error")
			assert.EqualValues(t, []*job.WithUpstream{jobWithExternalUpstreams}, result)
		})
	})
	t.Run("Resolve", func(t *testing.T) {
		t.Run("resolve upstream internally and externally", func(t *testing.T) {
			jobRepo := new(JobRepository)
			externalUpstreamResolver := new(ExternalUpstreamResolver)
			internalUpstreamResolver := new(InternalUpstreamResolver)

			logWriter := new(mockWriter)
			defer logWriter.AssertExpectations(t)

			jobAUpstreamCName := job.SpecUpstreamNameFrom("job-C")
			jobAUpstreamSpec, err := job.NewSpecUpstreamBuilder().WithUpstreamNames([]job.SpecUpstreamName{jobAUpstreamCName}).Build()
			assert.NoError(t, err)

			specA, err := job.NewSpecBuilder(jobVersion, "job-A", sampleOwner, jobSchedule, jobWindow, jobTask).WithSpecUpstream(jobAUpstreamSpec).Build()
			assert.NoError(t, err)

			jobADestination := resourceURNA
			jobASources := []resource.URN{resourceURNB, resourceURND}
			jobA := job.NewJob(sampleTenant, specA, jobADestination, jobASources, false)

			unresolvedUpstreams := []*job.Upstream{
				job.NewUpstreamUnresolvedStatic("job-C", project.Name()),
				job.NewUpstreamUnresolvedInferred(resourceURNB),
				job.NewUpstreamUnresolvedInferred(resourceURND),
			}

			internalUpstreamB := job.NewUpstreamResolved("job-B", "", resourceURNB, sampleTenant, "inferred", taskName, false)
			internalUpstreamC := job.NewUpstreamResolved("job-C", "", resourceURNC, sampleTenant, "static", taskName, false)
			externalUpstreamD := job.NewUpstreamResolved("job-D", "external-host", resourceURND, externalTenant, "inferred", taskName, true)

			jobWithInternalUpstream := job.NewWithUpstream(jobA, []*job.Upstream{internalUpstreamC, internalUpstreamB, unresolvedUpstreams[2]})
			jobWithUnresolvedUpstream := job.NewWithUpstream(jobA, unresolvedUpstreams)
			internalUpstreamResolver.On("Resolve", ctx, jobWithUnresolvedUpstream).Return(jobWithInternalUpstream, nil)

			jobWithExternalUpstream := job.NewWithUpstream(jobA, []*job.Upstream{internalUpstreamC, internalUpstreamB, externalUpstreamD})
			externalUpstreamResolver.On("Resolve", ctx, jobWithInternalUpstream, logWriter).Return(jobWithExternalUpstream, nil)

			upstreamResolver := resolver.NewUpstreamResolver(jobRepo, externalUpstreamResolver, internalUpstreamResolver)
			result, err := upstreamResolver.Resolve(ctx, jobA, logWriter)
			assert.NoError(t, err)
			assert.EqualValues(t, jobWithExternalUpstream.Upstreams(), result)
		})
		t.Run("should skip resolving upstream if the static upstream name is invalid", func(t *testing.T) {
			jobRepo := new(JobRepository)
			externalUpstreamResolver := new(ExternalUpstreamResolver)
			internalUpstreamResolver := new(InternalUpstreamResolver)

			logWriter := new(mockWriter)
			defer logWriter.AssertExpectations(t)

			jobAUpstreamCName := job.SpecUpstreamNameFrom("")
			jobAUpstreamSpec, err := job.NewSpecUpstreamBuilder().WithUpstreamNames([]job.SpecUpstreamName{jobAUpstreamCName}).Build()
			assert.NoError(t, err)

			specA, err := job.NewSpecBuilder(jobVersion, "job-A", sampleOwner, jobSchedule, jobWindow, jobTask).WithSpecUpstream(jobAUpstreamSpec).Build()
			assert.NoError(t, err)

			jobADestination := resourceURNA
			jobASources := []resource.URN{resourceURNB, resourceURND}
			jobA := job.NewJob(sampleTenant, specA, jobADestination, jobASources, false)

			unresolvedUpstreams := []*job.Upstream{
				job.NewUpstreamUnresolvedInferred(resourceURNB),
				job.NewUpstreamUnresolvedInferred(resourceURND),
			}

			internalUpstreamB := job.NewUpstreamResolved("job-B", "", resourceURNB, sampleTenant, "inferred", taskName, false)
			externalUpstreamD := job.NewUpstreamResolved("job-D", "external-host", resourceURND, externalTenant, "inferred", taskName, true)

			jobWithInternalUpstream := job.NewWithUpstream(jobA, []*job.Upstream{internalUpstreamB, unresolvedUpstreams[1]})
			jobWithUnresolvedUpstream := job.NewWithUpstream(jobA, []*job.Upstream{unresolvedUpstreams[0], unresolvedUpstreams[1]})
			internalUpstreamResolver.On("Resolve", ctx, jobWithUnresolvedUpstream).Return(jobWithInternalUpstream, nil)

			jobWithExternalUpstream := job.NewWithUpstream(jobA, []*job.Upstream{internalUpstreamB, externalUpstreamD})
			externalUpstreamResolver.On("Resolve", ctx, jobWithInternalUpstream, logWriter).Return(jobWithExternalUpstream, nil)

			expectedUpstream := []*job.Upstream{internalUpstreamB, externalUpstreamD}

			upstreamResolver := resolver.NewUpstreamResolver(jobRepo, externalUpstreamResolver, internalUpstreamResolver)
			result, err := upstreamResolver.Resolve(ctx, jobA, logWriter)
			assert.ErrorContains(t, err, "failed to get static upstreams to resolve")
			assert.EqualValues(t, expectedUpstream, result)
		})
		t.Run("should not break process but still return error if failed to resolve some static upstream internally", func(t *testing.T) {
			jobRepo := new(JobRepository)
			externalUpstreamResolver := new(ExternalUpstreamResolver)
			internalUpstreamResolver := new(InternalUpstreamResolver)

			logWriter := new(mockWriter)
			defer logWriter.AssertExpectations(t)

			jobAUpstreamCName := job.SpecUpstreamNameFrom("job-C")
			jobAUpstreamSpec, err := job.NewSpecUpstreamBuilder().WithUpstreamNames([]job.SpecUpstreamName{jobAUpstreamCName}).Build()
			assert.NoError(t, err)

			specA, err := job.NewSpecBuilder(jobVersion, "job-A", sampleOwner, jobSchedule, jobWindow, jobTask).WithSpecUpstream(jobAUpstreamSpec).Build()
			assert.NoError(t, err)

			jobADestination := resourceURNA
			jobASources := []resource.URN{resourceURNB, resourceURND}
			jobA := job.NewJob(sampleTenant, specA, jobADestination, jobASources, false)

			unresolvedUpstreams := []*job.Upstream{
				job.NewUpstreamUnresolvedStatic("job-C", project.Name()),
				job.NewUpstreamUnresolvedInferred(resourceURNB),
				job.NewUpstreamUnresolvedInferred(resourceURND),
			}

			internalUpstreamC := job.NewUpstreamResolved("job-C", "", resourceURNC, sampleTenant, "static", taskName, false)
			externalUpstreamD := job.NewUpstreamResolved("job-D", "external-host", resourceURND, externalTenant, "inferred", taskName, true)

			jobWithUnresolvedUpstream := job.NewWithUpstream(jobA, unresolvedUpstreams)
			jobWithInternalUpstream := job.NewWithUpstream(jobA, unresolvedUpstreams)
			errorMsg := "resolve upstream failed partially"
			internalUpstreamResolver.On("Resolve", ctx, jobWithUnresolvedUpstream).Return(jobWithInternalUpstream, errors.New(errorMsg))

			jobWithExternalUpstream := job.NewWithUpstream(jobA, []*job.Upstream{internalUpstreamC, unresolvedUpstreams[1], externalUpstreamD})
			externalUpstreamResolver.On("Resolve", ctx, jobWithInternalUpstream, logWriter).Return(jobWithExternalUpstream, nil)

			expectedUpstream := []*job.Upstream{internalUpstreamC, unresolvedUpstreams[1], externalUpstreamD}

			upstreamResolver := resolver.NewUpstreamResolver(jobRepo, externalUpstreamResolver, internalUpstreamResolver)
			result, err := upstreamResolver.Resolve(ctx, jobA, logWriter)
			assert.ErrorContains(t, err, errorMsg)
			assert.EqualValues(t, expectedUpstream, result)
		})
	})
}

// ExternalUpstreamResolver is an autogenerated mock type for the ExternalUpstreamResolver type
type ExternalUpstreamResolver struct {
	mock.Mock
}

// BulkResolve provides a mock function with given fields: _a0, _a1, _a2
func (_m *ExternalUpstreamResolver) BulkResolve(_a0 context.Context, _a1 []*job.WithUpstream, _a2 writer.LogWriter) ([]*job.WithUpstream, error) {
	ret := _m.Called(_a0, _a1, _a2)

	var r0 []*job.WithUpstream
	if rf, ok := ret.Get(0).(func(context.Context, []*job.WithUpstream, writer.LogWriter) []*job.WithUpstream); ok {
		r0 = rf(_a0, _a1, _a2)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]*job.WithUpstream)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, []*job.WithUpstream, writer.LogWriter) error); ok {
		r1 = rf(_a0, _a1, _a2)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// Resolve provides a mock function with given fields: ctx, jobWithUpstream, lw
func (_m *ExternalUpstreamResolver) Resolve(ctx context.Context, jobWithUpstream *job.WithUpstream, lw writer.LogWriter) (*job.WithUpstream, error) {
	ret := _m.Called(ctx, jobWithUpstream, lw)

	var r0 *job.WithUpstream
	if rf, ok := ret.Get(0).(func(context.Context, *job.WithUpstream, writer.LogWriter) *job.WithUpstream); ok {
		r0 = rf(ctx, jobWithUpstream, lw)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*job.WithUpstream)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, *job.WithUpstream, writer.LogWriter) error); ok {
		r1 = rf(ctx, jobWithUpstream, lw)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// InternalUpstreamResolver is an autogenerated mock type for the InternalUpstreamResolver type
type InternalUpstreamResolver struct {
	mock.Mock
}

// BulkResolve provides a mock function with given fields: _a0, _a1, _a2
func (_m *InternalUpstreamResolver) BulkResolve(_a0 context.Context, _a1 tenant.ProjectName, _a2 []*job.WithUpstream) ([]*job.WithUpstream, error) {
	ret := _m.Called(_a0, _a1, _a2)

	var r0 []*job.WithUpstream
	if rf, ok := ret.Get(0).(func(context.Context, tenant.ProjectName, []*job.WithUpstream) []*job.WithUpstream); ok {
		r0 = rf(_a0, _a1, _a2)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]*job.WithUpstream)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, tenant.ProjectName, []*job.WithUpstream) error); ok {
		r1 = rf(_a0, _a1, _a2)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// Resolve provides a mock function with given fields: _a0, _a1
func (_m *InternalUpstreamResolver) Resolve(_a0 context.Context, _a1 *job.WithUpstream) (*job.WithUpstream, error) {
	ret := _m.Called(_a0, _a1)

	var r0 *job.WithUpstream
	if rf, ok := ret.Get(0).(func(context.Context, *job.WithUpstream) *job.WithUpstream); ok {
		r0 = rf(_a0, _a1)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*job.WithUpstream)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, *job.WithUpstream) error); ok {
		r1 = rf(_a0, _a1)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// JobRepository is an autogenerated mock type for the JobRepository type
type JobRepository struct {
	mock.Mock
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

// GetJobNameWithInternalUpstreams provides a mock function with given fields: ctx, projectName, jobNames
func (_m *JobRepository) ResolveUpstreams(ctx context.Context, projectName tenant.ProjectName, jobNames []job.Name) (map[job.Name][]*job.Upstream, error) {
	ret := _m.Called(ctx, projectName, jobNames)

	var r0 map[job.Name][]*job.Upstream
	if rf, ok := ret.Get(0).(func(context.Context, tenant.ProjectName, []job.Name) map[job.Name][]*job.Upstream); ok {
		r0 = rf(ctx, projectName, jobNames)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(map[job.Name][]*job.Upstream)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, tenant.ProjectName, []job.Name) error); ok {
		r1 = rf(ctx, projectName, jobNames)
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
