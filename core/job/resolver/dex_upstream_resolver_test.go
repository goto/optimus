package resolver_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/goto/optimus/core/job"
	"github.com/goto/optimus/core/job/resolver"
	"github.com/goto/optimus/core/resource"
	"github.com/goto/optimus/core/scheduler/service"
	"github.com/goto/optimus/core/tenant"
	"github.com/goto/optimus/ext/resourcemanager"
	"github.com/goto/optimus/internal/lib/window"
	"github.com/goto/optimus/internal/models"

	"github.com/goto/salt/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestDexUpstreamResolver_Resolve(t *testing.T) {
	ctx := context.Background()
	l := log.NewNoop()
	mockDexClient := new(mockThirdPartyClient)
	mockTenantGetter := new(mockTenantDetailsGetter)
	//mockTenantDetails := new(mockTenantDetails)

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
	jobObj := job.NewJob(sampleTenant, specA, resource.ZeroURN(), []resource.URN{resourceURNC}, false)

	//t.Run("returns error if tenant details getter fails", func(t *testing.T) {
	//	mockTenantGetter.On("GetDetails", ctx, sampleTenant).Return(nil, errors.New("details error")).Once()
	//	resolver := resolver.NewDexUpstreamResolver(l, mockDexClient, mockTenantGetter)
	//
	//	mockLW := new(mockWriter)
	//	defer mockLW.AssertExpectations(t)
	//
	//	result, err := resolver.Resolve(ctx, jobWithUpstream, mockLW)
	//	assert.Nil(t, result)
	//	assert.ErrorContains(t, err, "failed to get tenant details")
	//	mockTenantGetter.AssertExpectations(t)
	//})
	//
	//t.Run("returns error if dex config fetch fails", func(t *testing.T) {
	//	mockTenantGetter.On("GetDetails", ctx, tenantObj).Return(mockTenantDetails, nil).Once()
	//	mockTenantDetails.On("GetConfig", tenant.ProjectDexThirdPartySensor).Return("", errors.New("config error")).Once()
	//	resolver := NewDexUpstreamResolver(l, mockDexClient, mockTenantGetter)
	//	result, err := resolver.Resolve(ctx, jobWithUpstream, mockLW)
	//	assert.Nil(t, result)
	//	assert.ErrorContains(t, err, "failed to get dex 3rd party sensor config")
	//	mockTenantGetter.AssertExpectations(t)
	//	mockTenantDetails.AssertExpectations(t)
	//})
	//
	//t.Run("returns original job if dex sensor is disabled", func(t *testing.T) {
	//	mockTenantGetter.On("GetDetails", ctx, tenantObj).Return(mockTenantDetails, nil).Once()
	//	mockTenantDetails.On("GetConfig", tenant.ProjectDexThirdPartySensor).Return("false", nil).Once()
	//	resolver := NewDexUpstreamResolver(l, mockDexClient, mockTenantGetter)
	//	result, err := resolver.Resolve(ctx, jobWithUpstream, mockLW)
	//	assert.Equal(t, jobWithUpstream, result)
	//	assert.NoError(t, err)
	//	mockTenantGetter.AssertExpectations(t)
	//	mockTenantDetails.AssertExpectations(t)
	//})

	t.Run("adds third party upstream if dex manages the resource", func(t *testing.T) {

		mockTenantGetter.On("GetDetails", ctx, sampleTenant).Return(mock.Anything, nil).Once()
		mockTenantDetails.On("GetConfig", tenant.ProjectDexThirdPartySensor).Return("true", nil).Once()
		mockDexClient.On("IsManaged", ctx, upstreamRes).Return(true, nil).Once()
		resolver := NewDexUpstreamResolver(l, mockDexClient, mockTenantGetter)
		result, err := resolver.Resolve(ctx, jobWithUpstream, mockLW)
		assert.NoError(t, err)
		assert.Len(t, result.GetUnresolvedUpstreams(), 0)
		assert.Len(t, result.GetThirdPartyUpstreams(), 1)
		assert.Equal(t, upstreamRes.GetName(), result.GetThirdPartyUpstreams()[0].Name())
		mockTenantGetter.AssertExpectations(t)
		mockTenantDetails.AssertExpectations(t)
		mockDexClient.AssertExpectations(t)
	})

	//t.Run("keeps upstreams unchanged if dex does not manage the resource", func(t *testing.T) {
	//	mockTenantGetter.On("GetDetails", ctx, tenantObj).Return(mockTenantDetails, nil).Once()
	//	mockTenantDetails.On("GetConfig", tenant.ProjectDexThirdPartySensor).Return("true", nil).Once()
	//	mockDexClient.On("IsManaged", ctx, upstreamRes).Return(false, nil).Once()
	//	resolver := NewDexUpstreamResolver(l, mockDexClient, mockTenantGetter)
	//	result, err := resolver.Resolve(ctx, jobWithUpstream, mockLW)
	//	assert.NoError(t, err)
	//	assert.Len(t, result.GetUnresolvedUpstreams(), 1)
	//	assert.Len(t, result.GetThirdPartyUpstreams(), 0)
	//	assert.Equal(t, upstreamRes.GetName(), result.GetUnresolvedUpstreams()[0].Resource().GetName())
	//	mockTenantGetter.AssertExpectations(t)
	//	mockTenantDetails.AssertExpectations(t)
	//	mockDexClient.AssertExpectations(t)
	//})

	//t.Run("logs error if dexClient.IsManaged returns error", func(t *testing.T) {
	//	mockTenantGetter.On("GetDetails", ctx, tenantObj).Return(mockTenantDetails, nil).Once()
	//	mockTenantDetails.On("GetConfig", tenant.ProjectDexThirdPartySensor).Return("true", nil).Once()
	//	mockDexClient.On("IsManaged", ctx, upstreamRes).Return(false, errors.New("dex error")).Once()
	//	mockLW.logs = nil
	//	resolver := NewDexUpstreamResolver(l, mockDexClient, mockTenantGetter)
	//	result, err := resolver.Resolve(ctx, jobWithUpstream, mockLW)
	//	assert.Error(t, err)
	//	assert.Contains(t, mockLW.logs[0], "dex 3rd upstream resolution errors")
	//	assert.Len(t, result.GetUnresolvedUpstreams(), 1)
	//	assert.Len(t, result.GetThirdPartyUpstreams(), 0)
	//	mockTenantGetter.AssertExpectations(t)
	//	mockTenantDetails.AssertExpectations(t)
	//	mockDexClient.AssertExpectations(t)
	//})
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
	return args.Get(0).(*tenant.WithDetails), args.Error(1)
}

type mockTenantDetails struct {
	mock.Mock
}

func (m *mockTenantDetails) GetConfig(key string) (string, error) {
	args := m.Called(key)
	return args.String(0), args.Error(1)
}
