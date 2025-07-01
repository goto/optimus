package resolver_test

import (
	"context"
	"errors"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/goto/optimus/config"
	"github.com/goto/optimus/core/job"
	"github.com/goto/optimus/core/job/resolver"
	"github.com/goto/optimus/core/resource"
	"github.com/goto/optimus/core/tenant"
	"github.com/goto/optimus/ext/resourcemanager"
	"github.com/goto/optimus/internal/lib/window"
	"github.com/goto/optimus/internal/models"
)

func TestExternalUpstreamResolver(t *testing.T) {
	ctx := context.Background()
	sampleTenant, _ := tenant.NewTenant("project", "namespace")
	externalTenant, _ := tenant.NewTenant("external-project", "external-namespace")
	resourceManager := new(ResourceManager)
	optimusResourceManagers := []resourcemanager.ResourceManager{resourceManager}

	jobVersion := 1
	startDate, _ := job.ScheduleDateFrom("2022-10-01")
	jobSchedule, _ := job.NewScheduleBuilder(startDate).Build()
	w, _ := models.NewWindow(jobVersion, "d", "24h", "24h")
	jobWindow := window.NewCustomConfig(w)
	taskName, _ := job.TaskNameFrom("sample-task")
	jobTaskConfig, _ := job.ConfigFrom(map[string]string{"sample_task_key": "sample_value"})
	jobTask := job.NewTask(taskName, jobTaskConfig, "")
	upstreamSpec, _ := job.NewSpecUpstreamBuilder().WithUpstreamNames([]job.SpecUpstreamName{"external-project/job-B"}).Build()
	specA, _ := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, jobWindow, jobTask).WithSpecUpstream(upstreamSpec).Build()

	resourceURNB, err := resource.ParseURN("store://resource-B")
	assert.NoError(t, err)
	resourceURNC, err := resource.ParseURN("store://resource-C")
	assert.NoError(t, err)
	resourceURND, err := resource.ParseURN("store://resource-D")
	assert.NoError(t, err)

	jobA := job.NewJob(sampleTenant, specA, resource.ZeroURN(), []resource.URN{resourceURNC}, false)

	t.Run("BulkResolve", func(t *testing.T) {
		t.Run("resolves upstream externally", func(t *testing.T) {
			logWriter := new(mockWriter)
			defer logWriter.AssertExpectations(t)

			unresolvedUpstreamB := job.NewUpstreamUnresolvedStatic("job-B", externalTenant.ProjectName())
			unresolvedUpstreamC := job.NewUpstreamUnresolvedInferred(resourceURNC)
			jobWithUnresolvedUpstream := job.NewWithUpstream(jobA, []*job.Upstream{unresolvedUpstreamB, unresolvedUpstreamC})

			upstreamB := job.NewUpstreamResolved("job-B", "external-host", resourceURNB, externalTenant, "static", taskName, true)
			upstreamC := job.NewUpstreamResolved("job-C", "external-host", resourceURNC, externalTenant, "inferred", taskName, true)
			resourceManager.On("GetOptimusUpstreams", ctx, unresolvedUpstreamB).Return([]*job.Upstream{upstreamB}, nil).Once()
			resourceManager.On("GetOptimusUpstreams", ctx, unresolvedUpstreamC).Return([]*job.Upstream{upstreamC}, nil).Once()

			logWriter.On("Write", mock.Anything, mock.Anything).Return(nil)

			extUpstreamResolver := resolver.NewTestExternalUpstreamResolver(optimusResourceManagers)
			result, err := extUpstreamResolver.BulkResolve(ctx, []*job.WithUpstream{jobWithUnresolvedUpstream}, logWriter)
			assert.Nil(t, result[0].GetUnresolvedUpstreams())
			assert.Nil(t, err)
			expected, actual := []*job.Upstream{upstreamB, upstreamC}, result[0].Upstreams()
			sort.SliceStable(expected, func(i, j int) bool { return expected[i].FullName() < expected[j].FullName() })
			sort.SliceStable(actual, func(i, j int) bool { return actual[i].FullName() < actual[j].FullName() })
			assert.EqualValues(t, expected, actual)
		})
		t.Run("returns the merged of previous resolved and external resolved upstreams", func(t *testing.T) {
			logWriter := new(mockWriter)
			defer logWriter.AssertExpectations(t)

			unresolvedUpstreamB := job.NewUpstreamUnresolvedStatic("job-B", externalTenant.ProjectName())
			unresolvedUpstreamC := job.NewUpstreamUnresolvedInferred(resourceURNC)
			upstreamD := job.NewUpstreamResolved("job-D", "internal-host", resourceURND, sampleTenant, "inferred", taskName, false)
			jobWithUnresolvedUpstream := job.NewWithUpstream(jobA, []*job.Upstream{unresolvedUpstreamB, unresolvedUpstreamC, upstreamD})

			upstreamB := job.NewUpstreamResolved("job-B", "external-host", resourceURNB, externalTenant, "static", taskName, true)
			upstreamC := job.NewUpstreamResolved("job-C", "external-host", resourceURNC, externalTenant, "inferred", taskName, true)
			resourceManager.On("GetOptimusUpstreams", ctx, unresolvedUpstreamB).Return([]*job.Upstream{upstreamB}, nil).Once()
			resourceManager.On("GetOptimusUpstreams", ctx, unresolvedUpstreamC).Return([]*job.Upstream{upstreamC}, nil).Once()

			logWriter.On("Write", mock.Anything, mock.Anything).Return(nil)

			extUpstreamResolver := resolver.NewTestExternalUpstreamResolver(optimusResourceManagers)
			result, err := extUpstreamResolver.BulkResolve(ctx, []*job.WithUpstream{jobWithUnresolvedUpstream}, logWriter)
			assert.Nil(t, result[0].GetUnresolvedUpstreams())
			assert.Nil(t, err)
			expected, actual := []*job.Upstream{upstreamD, upstreamB, upstreamC}, result[0].Upstreams()
			sort.SliceStable(expected, func(i, j int) bool { return expected[i].FullName() < expected[j].FullName() })
			sort.SliceStable(actual, func(i, j int) bool { return actual[i].FullName() < actual[j].FullName() })
			assert.EqualValues(t, expected, actual)
		})
		t.Run("returns unresolved upstream and upstream error if unable to fetch upstreams from external", func(t *testing.T) {
			logWriter := new(mockWriter)
			defer logWriter.AssertExpectations(t)

			unresolvedUpstreamB := job.NewUpstreamUnresolvedStatic("job-B", externalTenant.ProjectName())
			unresolvedUpstreamC := job.NewUpstreamUnresolvedInferred(resourceURNC)
			jobWithUnresolvedUpstream := job.NewWithUpstream(jobA, []*job.Upstream{unresolvedUpstreamB, unresolvedUpstreamC})

			resourceManager.On("GetOptimusUpstreams", ctx, unresolvedUpstreamB).Return([]*job.Upstream{}, errors.New("connection error")).Once()
			resourceManager.On("GetOptimusUpstreams", ctx, unresolvedUpstreamC).Return([]*job.Upstream{}, nil).Once()

			logWriter.On("Write", mock.Anything, mock.Anything).Return(nil)

			extUpstreamResolver := resolver.NewTestExternalUpstreamResolver(optimusResourceManagers)
			result, err := extUpstreamResolver.BulkResolve(ctx, []*job.WithUpstream{jobWithUnresolvedUpstream}, logWriter)
			expected, actual := []*job.Upstream{unresolvedUpstreamB, unresolvedUpstreamC}, result[0].Upstreams()
			sort.SliceStable(expected, func(i, j int) bool { return expected[i].FullName() < expected[j].FullName() })
			sort.SliceStable(actual, func(i, j int) bool { return actual[i].FullName() < actual[j].FullName() })
			assert.EqualValues(t, expected, actual)
			assert.NotNil(t, err)
		})
		t.Run("skips resolves upstream externally if no external resource manager found", func(t *testing.T) {
			logWriter := new(mockWriter)
			defer logWriter.AssertExpectations(t)

			unresolvedUpstreamB := job.NewUpstreamUnresolvedStatic("job-B", externalTenant.ProjectName())
			unresolvedUpstreamC := job.NewUpstreamUnresolvedInferred(resourceURNC)
			jobWithUnresolvedUpstream := job.NewWithUpstream(jobA, []*job.Upstream{unresolvedUpstreamB, unresolvedUpstreamC})

			extUpstreamResolver := resolver.NewTestExternalUpstreamResolver(nil)
			result, err := extUpstreamResolver.BulkResolve(ctx, []*job.WithUpstream{jobWithUnresolvedUpstream}, logWriter)
			assert.Nil(t, err)
			assert.EqualValues(t, jobWithUnresolvedUpstream, result[0])
		})
	})
	t.Run("NewExternalUpstreamResolver", func(t *testing.T) {
		t.Run("should able to construct external upstream resolver using resource manager config", func(t *testing.T) {
			optimusResourceManagerConfig := config.ResourceManager{
				Name: "sample",
				Type: "optimus",
				Config: config.ResourceManagerConfigOptimus{
					Host: "sample-host",
				},
			}

			_, err := resolver.NewExternalUpstreamResolver([]config.ResourceManager{optimusResourceManagerConfig})
			assert.NoError(t, err)
		})
		t.Run("should return error if the resource manager is unknown", func(t *testing.T) {
			optimusResourceManagerConfig := config.ResourceManager{
				Name: "sample",
				Type: "invalid-sample",
				Config: config.ResourceManagerConfigOptimus{
					Host: "sample-host",
				},
			}
			_, err := resolver.NewExternalUpstreamResolver([]config.ResourceManager{optimusResourceManagerConfig})
			assert.ErrorContains(t, err, "resource manager invalid-sample is not recognized")
		})
		t.Run("should return error if unable to construct optimus resource manager", func(t *testing.T) {
			optimusResourceManagerConfig := config.ResourceManager{
				Name: "sample",
				Type: "optimus",
			}
			_, err := resolver.NewExternalUpstreamResolver([]config.ResourceManager{optimusResourceManagerConfig})
			assert.ErrorContains(t, err, "host is empty")
		})
	})
}

// ResourceManager is an autogenerated mock type for the ResourceManager type
type ResourceManager struct {
	mock.Mock
}

// GetOptimusUpstreams provides a mock function with given fields: ctx, unresolvedDependency
func (_m *ResourceManager) GetOptimusUpstreams(ctx context.Context, unresolvedDependency *job.Upstream) ([]*job.Upstream, error) {
	ret := _m.Called(ctx, unresolvedDependency)

	var r0 []*job.Upstream
	if rf, ok := ret.Get(0).(func(context.Context, *job.Upstream) []*job.Upstream); ok {
		r0 = rf(ctx, unresolvedDependency)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]*job.Upstream)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, *job.Upstream) error); ok {
		r1 = rf(ctx, unresolvedDependency)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}
