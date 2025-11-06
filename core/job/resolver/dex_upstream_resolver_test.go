package resolver_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/goto/optimus/core/job"
	"github.com/goto/optimus/core/job/resolver"
	"github.com/goto/optimus/core/resource"
	"github.com/goto/optimus/core/tenant"
	"github.com/goto/optimus/internal/lib/window"
	"github.com/goto/optimus/internal/models"

	"github.com/goto/salt/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestDexUpstreamResolver_Resolve(t *testing.T) {
	ctx := context.Background()

	logger := log.NewNoop()

	sampleTenant, _ := tenant.NewTenant("project", "namespace")

	jobVersion := 1
	startDate, _ := job.ScheduleDateFrom("2022-10-01")
	jobSchedule, _ := job.NewScheduleBuilder(startDate).Build()
	w, _ := models.NewWindow(jobVersion, "d", "24h", "24h")
	jobWindow := window.NewCustomConfig(w)
	taskName, _ := job.TaskNameFrom("sample-task")
	jobTaskConfig, _ := job.ConfigFrom(map[string]string{"sample_task_key": "sample_value"})
	jobTask := job.NewTask(taskName, jobTaskConfig, "", nil)
	upstreamSpec, _ := job.NewSpecUpstreamBuilder().WithUpstreamNames([]job.SpecUpstreamName{"external-project/job-B"}).Build()
	specA, _ := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, jobWindow, jobTask).WithSpecUpstream(upstreamSpec).Build()
	resourceURNC, err := resource.ParseURN("store://resource-C")
	assert.NoError(t, err)

	resourceURNB, err := resource.ParseURN("store://resource-B")
	assert.NoError(t, err)

	jobA := job.NewJob(sampleTenant, specA, resource.ZeroURN(), []resource.URN{resourceURNC}, false)
	t.Run("returns error, and bypass the resolved and unresolved upstreams as is, if tenant details getter fails", func(t *testing.T) {
		logger := log.NewNoop()

		unresolvedUpstreamC := job.NewUpstreamUnresolvedInferred(resourceURNC)
		upstreamB := job.NewUpstreamResolved("job-B", "external-host", resourceURNB, sampleTenant, "static", taskName, true)
		jobWithUnresolvedUpstream := job.NewWithUpstream(jobA, []*job.Upstream{upstreamB, unresolvedUpstreamC})

		mockDexClient := new(mockThirdPartyClient)
		defer mockDexClient.AssertExpectations(t)

		mockTenantGetter := new(mockTenantDetailsGetter)
		mockTenantGetter.On("GetDetails", ctx, sampleTenant).Return(nil, fmt.Errorf("tenant details getter fails"))
		defer mockTenantGetter.AssertExpectations(t)

		dexUpstreamResolver := resolver.NewDexUpstreamResolver(logger, mockDexClient, mockTenantGetter)

		logWriter := new(mockWriter)
		defer logWriter.AssertExpectations(t)

		result, err := dexUpstreamResolver.Resolve(ctx, jobWithUnresolvedUpstream, logWriter)
		assert.ErrorContains(t, err, "tenant details getter fails")

		assert.Len(t, result.Upstreams(), 2)
		assert.Len(t, result.GetResolvedUpstreams(), 1)
		assert.Len(t, result.GetUnresolvedUpstreams(), 1)
	})

	t.Run("returns error, and bypass the resolved and unresolved upstreams as is, if dex config fetch fails", func(t *testing.T) {
		logger := log.NewNoop()

		unresolvedUpstreamC := job.NewUpstreamUnresolvedInferred(resourceURNC)
		upstreamB := job.NewUpstreamResolved("job-B", "external-host", resourceURNB, sampleTenant, "static", taskName, true)
		jobWithUnresolvedUpstream := job.NewWithUpstream(jobA, []*job.Upstream{upstreamB, unresolvedUpstreamC})

		mockDexClient := new(mockThirdPartyClient)
		defer mockDexClient.AssertExpectations(t)

		project, err := tenant.NewProject(sampleTenant.ProjectName().String(), map[string]string{
			"STORAGE_PATH":   "/data",
			"SCHEDULER_HOST": "external-host",
		}, map[string]string{})

		assert.NoError(t, err)
		namespace, err := tenant.NewNamespace(sampleTenant.NamespaceName().String(), sampleTenant.ProjectName(), map[string]string{}, map[string]string{})
		assert.NoError(t, err)
		tenantWithDetails, err := tenant.NewTenantDetails(project, namespace, tenant.PlainTextSecrets{})
		assert.NoError(t, err)

		mockTenantGetter := new(mockTenantDetailsGetter)
		mockTenantGetter.On("GetDetails", ctx, sampleTenant).Return(tenantWithDetails, nil)
		defer mockTenantGetter.AssertExpectations(t)

		dexUpstreamResolver := resolver.NewDexUpstreamResolver(logger, mockDexClient, mockTenantGetter)

		logWriter := new(mockWriter)
		defer logWriter.AssertExpectations(t)

		result, err := dexUpstreamResolver.Resolve(ctx, jobWithUnresolvedUpstream, logWriter)
		assert.ErrorContains(t, err, "failed to get dex 3rd party sensor config for tenant")

		assert.Len(t, result.Upstreams(), 2)
		assert.Len(t, result.GetResolvedUpstreams(), 1)
		assert.Len(t, result.GetUnresolvedUpstreams(), 1)
	})

	t.Run("returns original job if dex sensor is disabled", func(t *testing.T) {

		logger := log.NewNoop()

		unresolvedUpstreamC := job.NewUpstreamUnresolvedInferred(resourceURNC)
		upstreamB := job.NewUpstreamResolved("job-B", "external-host", resourceURNB, sampleTenant, "static", taskName, true)
		jobWithUnresolvedUpstream := job.NewWithUpstream(jobA, []*job.Upstream{upstreamB, unresolvedUpstreamC})

		mockDexClient := new(mockThirdPartyClient)
		defer mockDexClient.AssertExpectations(t)

		project, err := tenant.NewProject(sampleTenant.ProjectName().String(), map[string]string{
			"ENABLE_DEX_THIRD_PARTY_SENSOR": "0",
			"STORAGE_PATH":                  "/data",
			"SCHEDULER_HOST":                "external-host",
		}, map[string]string{})

		assert.NoError(t, err)
		namespace, err := tenant.NewNamespace(sampleTenant.NamespaceName().String(), sampleTenant.ProjectName(), map[string]string{}, map[string]string{})
		assert.NoError(t, err)
		tenantWithDetails, err := tenant.NewTenantDetails(project, namespace, tenant.PlainTextSecrets{})
		assert.NoError(t, err)

		mockTenantGetter := new(mockTenantDetailsGetter)
		mockTenantGetter.On("GetDetails", ctx, sampleTenant).Return(tenantWithDetails, nil)
		defer mockTenantGetter.AssertExpectations(t)

		dexUpstreamResolver := resolver.NewDexUpstreamResolver(logger, mockDexClient, mockTenantGetter)

		logWriter := new(mockWriter)
		defer logWriter.AssertExpectations(t)

		result, err := dexUpstreamResolver.Resolve(ctx, jobWithUnresolvedUpstream, logWriter)
		assert.Nil(t, err)

		assert.Len(t, result.Upstreams(), 2)
		assert.Len(t, result.GetResolvedUpstreams(), 1)
		assert.Len(t, result.GetUnresolvedUpstreams(), 1)

	})

	t.Run("adds third party upstream if dex manages the resource", func(t *testing.T) {

		logger := log.NewNoop()

		unresolvedUpstreamC := job.NewUpstreamUnresolvedInferred(resourceURNC)
		upstreamB := job.NewUpstreamResolved("job-B", "external-host", resourceURNB, sampleTenant, "static", taskName, true)
		jobWithUnresolvedUpstream := job.NewWithUpstream(jobA, []*job.Upstream{upstreamB, unresolvedUpstreamC})

		mockDexClient := new(mockThirdPartyClient)
		mockDexClient.On("IsManaged", ctx, unresolvedUpstreamC.Resource()).Return(true, nil)

		defer mockDexClient.AssertExpectations(t)

		project, err := tenant.NewProject(sampleTenant.ProjectName().String(), map[string]string{
			"ENABLE_DEX_THIRD_PARTY_SENSOR": "1",
			"STORAGE_PATH":                  "/data",
			"SCHEDULER_HOST":                "external-host",
		}, map[string]string{})

		assert.NoError(t, err)
		namespace, err := tenant.NewNamespace(sampleTenant.NamespaceName().String(), sampleTenant.ProjectName(), map[string]string{}, map[string]string{})
		assert.NoError(t, err)
		tenantWithDetails, err := tenant.NewTenantDetails(project, namespace, tenant.PlainTextSecrets{})
		assert.NoError(t, err)

		mockTenantGetter := new(mockTenantDetailsGetter)
		mockTenantGetter.On("GetDetails", ctx, sampleTenant).Return(tenantWithDetails, nil)
		defer mockTenantGetter.AssertExpectations(t)

		dexUpstreamResolver := resolver.NewDexUpstreamResolver(logger, mockDexClient, mockTenantGetter)

		logWriter := new(mockWriter)
		defer logWriter.AssertExpectations(t)

		result, err := dexUpstreamResolver.Resolve(ctx, jobWithUnresolvedUpstream, logWriter)
		assert.Nil(t, err)

		assert.Len(t, result.Upstreams(), 1)
		assert.Len(t, result.ThirdPartyUpstreams(), 1)

		assert.Equal(t, upstreamB, result.Upstreams()[0])

		thirdPartyUpstream := result.ThirdPartyUpstreams()
		assert.Equal(t, "resource-C", thirdPartyUpstream[0].Identifier())
	})

	t.Run("just bypass if there is not unresolved upstream", func(t *testing.T) {
		upstreamB := job.NewUpstreamResolved("job-B", "external-host", resourceURNB, sampleTenant, "static", taskName, true)
		jobWithUnresolvedUpstream := job.NewWithUpstream(jobA, []*job.Upstream{upstreamB})

		mockDexClient := new(mockThirdPartyClient)
		defer mockDexClient.AssertExpectations(t)

		project, err := tenant.NewProject(sampleTenant.ProjectName().String(), map[string]string{
			"ENABLE_DEX_THIRD_PARTY_SENSOR": "1",
			"STORAGE_PATH":                  "/data",
			"SCHEDULER_HOST":                "external-host",
		}, map[string]string{})

		assert.NoError(t, err)
		namespace, err := tenant.NewNamespace(sampleTenant.NamespaceName().String(), sampleTenant.ProjectName(), map[string]string{}, map[string]string{})
		assert.NoError(t, err)
		tenantWithDetails, err := tenant.NewTenantDetails(project, namespace, tenant.PlainTextSecrets{})
		assert.NoError(t, err)

		mockTenantGetter := new(mockTenantDetailsGetter)
		mockTenantGetter.On("GetDetails", ctx, sampleTenant).Return(tenantWithDetails, nil)
		defer mockTenantGetter.AssertExpectations(t)

		dexUpstreamResolver := resolver.NewDexUpstreamResolver(logger, mockDexClient, mockTenantGetter)

		logWriter := new(mockWriter)
		defer logWriter.AssertExpectations(t)

		result, err := dexUpstreamResolver.Resolve(ctx, jobWithUnresolvedUpstream, logWriter)
		assert.Nil(t, err)
		assert.Equal(t, upstreamB, result.Upstreams()[0])
	})

	t.Run("retain unresolved upstreams if dex does not manage the resources", func(t *testing.T) {
		unresolvedUpstreamC := job.NewUpstreamUnresolvedInferred(resourceURNC)
		upstreamB := job.NewUpstreamResolved("job-B", "external-host", resourceURNB, sampleTenant, "static", taskName, true)
		jobWithUnresolvedUpstream := job.NewWithUpstream(jobA, []*job.Upstream{upstreamB, unresolvedUpstreamC})

		mockDexClient := new(mockThirdPartyClient)
		mockDexClient.On("IsManaged", ctx, unresolvedUpstreamC.Resource()).Return(false, nil)

		defer mockDexClient.AssertExpectations(t)

		project, err := tenant.NewProject(sampleTenant.ProjectName().String(), map[string]string{
			"ENABLE_DEX_THIRD_PARTY_SENSOR": "1",
			"STORAGE_PATH":                  "/data",
			"SCHEDULER_HOST":                "external-host",
		}, map[string]string{})

		assert.NoError(t, err)
		namespace, err := tenant.NewNamespace(sampleTenant.NamespaceName().String(), sampleTenant.ProjectName(), map[string]string{}, map[string]string{})
		assert.NoError(t, err)
		tenantWithDetails, err := tenant.NewTenantDetails(project, namespace, tenant.PlainTextSecrets{})
		assert.NoError(t, err)

		mockTenantGetter := new(mockTenantDetailsGetter)
		mockTenantGetter.On("GetDetails", ctx, sampleTenant).Return(tenantWithDetails, nil)
		defer mockTenantGetter.AssertExpectations(t)

		dexUpstreamResolver := resolver.NewDexUpstreamResolver(logger, mockDexClient, mockTenantGetter)

		logWriter := new(mockWriter)
		defer logWriter.AssertExpectations(t)

		result, err := dexUpstreamResolver.Resolve(ctx, jobWithUnresolvedUpstream, logWriter)
		assert.Nil(t, err)

		assert.Len(t, result.Upstreams(), 2)
		assert.Len(t, result.GetResolvedUpstreams(), 1)
		assert.Len(t, result.GetUnresolvedUpstreams(), 1)
	})

}

type mockThirdPartyClient struct {
	mock.Mock
}

func (m *mockThirdPartyClient) IsManaged(ctx context.Context, resourceURN resource.URN) (bool, error) {
	args := m.Called(ctx, resourceURN)
	return args.Bool(0), args.Error(1)
}

func (m *mockThirdPartyClient) IsComplete(ctx context.Context, resourceURN resource.URN, dateFrom, dateTo time.Time) (bool, interface{}, error) {
	args := m.Called(ctx, resourceURN, dateFrom, dateTo)
	return args.Bool(0), args.Get(1).(any), args.Error(2)
}

type mockTenantDetailsGetter struct {
	mock.Mock
}

func (m *mockTenantDetailsGetter) GetDetails(ctx context.Context, t tenant.Tenant) (*tenant.WithDetails, error) {
	args := m.Called(ctx, t)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*tenant.WithDetails), args.Error(1)
}

type mockTenantDetails struct {
	mock.Mock
}

func (m *mockTenantDetails) GetConfig(key string) (string, error) {
	args := m.Called(key)
	return args.String(0), args.Error(1)
}
