package resolver_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/goto/optimus/core/scheduler"
	"github.com/goto/optimus/core/scheduler/resolver"
	"github.com/goto/optimus/core/tenant"
)

func TestExternalOptimusManager(t *testing.T) {
	ctx := context.Background()
	baseJobName, _ := scheduler.JobNameFrom("job_name")
	baseJobTenant, _ := tenant.NewTenant("test-proj", "test-ns")
	baseJobScheduleTime, _ := time.Parse(time.RFC3339, "2016-01-04T01:00:00Z")

	upstreamJobName, _ := scheduler.JobNameFrom("upstream_job_name")
	upstreamJobTenant, _ := tenant.NewTenant("upstream-proj", "upstream-ns")

	upstreamHost := "https://upstreamHost"

	t.Run("GetJobScheduleInterval", func(t *testing.T) {
		t.Run("return not found error when resource manager not found by upstream host", func(t *testing.T) {
			resourceManager := new(ResourceManager)
			resourceManager.On("GetHostURL").Return(upstreamHost)
			defer resourceManager.AssertExpectations(t)

			optimusResourceManagers := []resolver.OptimusResourceManager{resourceManager}

			extUpstreamResolver := resolver.NewTestExternalOptimusConnectors(optimusResourceManagers)
			scheduleInterval, err := extUpstreamResolver.GetJobScheduleInterval(ctx, "https://upstreamHost/new", baseJobTenant, baseJobName)
			assert.ErrorContains(t, err, "could not find external resource manager by host: https://upstreamHost/new")
			assert.Equal(t, "", scheduleInterval)
		})
		t.Run("return error when unable to fetch schedule interval from upstream", func(t *testing.T) {
			resourceManager := new(ResourceManager)
			resourceManager.On("GetHostURL").Return(upstreamHost)
			resourceManager.On("GetJobScheduleInterval", ctx, baseJobTenant, baseJobName).Return("", errors.New("error in getting schedule interval"))
			defer resourceManager.AssertExpectations(t)

			optimusResourceManagers := []resolver.OptimusResourceManager{resourceManager}

			extUpstreamResolver := resolver.NewTestExternalOptimusConnectors(optimusResourceManagers)
			scheduleInterval, err := extUpstreamResolver.GetJobScheduleInterval(ctx, upstreamHost, baseJobTenant, baseJobName)
			assert.ErrorContains(t, err, "error in getting schedule interval")
			assert.Equal(t, "", scheduleInterval)
		})
		t.Run("return schedule interval", func(t *testing.T) {
			resourceManager := new(ResourceManager)
			resourceManager.On("GetHostURL").Return(upstreamHost)
			resourceManager.On("GetJobScheduleInterval", ctx, baseJobTenant, baseJobName).Return("0 1 * * *", nil)
			defer resourceManager.AssertExpectations(t)

			optimusResourceManagers := []resolver.OptimusResourceManager{resourceManager}

			extUpstreamResolver := resolver.NewTestExternalOptimusConnectors(optimusResourceManagers)
			scheduleInterval, err := extUpstreamResolver.GetJobScheduleInterval(ctx, upstreamHost, baseJobTenant, baseJobName)
			assert.Equal(t, "0 1 * * *", scheduleInterval)
			assert.Nil(t, err)
		})
	})

	t.Run("GetJobRuns", func(t *testing.T) {
		sensorParameters := scheduler.JobSensorParameters{
			SubjectJobName:     baseJobName,
			SubjectProjectName: baseJobTenant.ProjectName(),
			ScheduledTime:      baseJobScheduleTime,
			UpstreamJobName:    upstreamJobName,
			UpstreamTenant:     upstreamJobTenant,
		}
		criteria := &scheduler.JobRunsCriteria{
			Name:        upstreamJobName.String(),
			StartDate:   baseJobScheduleTime.Add(-24 * time.Hour),
			EndDate:     baseJobScheduleTime,
			Filter:      nil,
			OnlyLastRun: false,
		}
		t.Run("return not found error when resource manager not found by upstream host", func(t *testing.T) {
			resourceManager := new(ResourceManager)
			resourceManager.On("GetHostURL").Return(upstreamHost)
			defer resourceManager.AssertExpectations(t)

			optimusResourceManagers := []resolver.OptimusResourceManager{resourceManager}

			extUpstreamResolver := resolver.NewTestExternalOptimusConnectors(optimusResourceManagers)

			sensorParameters := scheduler.JobSensorParameters{
				SubjectJobName:     baseJobName,
				SubjectProjectName: baseJobTenant.ProjectName(),
				ScheduledTime:      baseJobScheduleTime,
				UpstreamJobName:    upstreamJobName,
				UpstreamTenant:     upstreamJobTenant,
			}
			criteria := &scheduler.JobRunsCriteria{
				Name:        upstreamJobName.String(),
				StartDate:   baseJobScheduleTime.Add(-24 * time.Hour),
				EndDate:     baseJobScheduleTime,
				Filter:      nil,
				OnlyLastRun: false,
			}

			jobRunStatus, err := extUpstreamResolver.GetJobRuns(ctx, "https://upstreamHost/new", sensorParameters, criteria)
			assert.ErrorContains(t, err, "could not find external resource manager by host: https://upstreamHost/new")
			assert.Zero(t, len(jobRunStatus))
		})
		t.Run("return error when job runs not found on upstream host", func(t *testing.T) {
			resourceManager := new(ResourceManager)
			resourceManager.On("GetHostURL").Return(upstreamHost)
			resourceManager.On("GetJobRuns", ctx, sensorParameters, criteria).Return([]*scheduler.JobRunStatus{}, errors.New("error getting job runs"))
			defer resourceManager.AssertExpectations(t)

			optimusResourceManagers := []resolver.OptimusResourceManager{resourceManager}

			extUpstreamResolver := resolver.NewTestExternalOptimusConnectors(optimusResourceManagers)

			jobRunStatus, err := extUpstreamResolver.GetJobRuns(ctx, "https://upstreamHost", sensorParameters, criteria)
			assert.ErrorContains(t, err, "error getting job runs")
			assert.Zero(t, len(jobRunStatus))
		})

		t.Run("return job runs from upstream host", func(t *testing.T) {
			resourceManager := new(ResourceManager)
			resourceManager.On("GetHostURL").Return(upstreamHost)
			resourceManager.On("GetJobRuns", ctx, sensorParameters, criteria).Return([]*scheduler.JobRunStatus{
				{
					ScheduledAt: time.Time{},
					State:       scheduler.StateSuccess,
				},
				{
					ScheduledAt: time.Time{},
					State:       scheduler.StateSuccess,
				},
			}, nil)
			defer resourceManager.AssertExpectations(t)

			optimusResourceManagers := []resolver.OptimusResourceManager{resourceManager}

			extUpstreamResolver := resolver.NewTestExternalOptimusConnectors(optimusResourceManagers)

			jobRunStatus, err := extUpstreamResolver.GetJobRuns(ctx, "https://upstreamHost", sensorParameters, criteria)
			assert.Nil(t, err)
			assert.Equal(t, 2, len(jobRunStatus))
		})
	})
}

type ResourceManager struct {
	mock.Mock
}

func (r *ResourceManager) GetJobScheduleInterval(ctx context.Context, tnnt tenant.Tenant, jobName scheduler.JobName) (string, error) {
	args := r.Called(ctx, tnnt, jobName)
	return args.Get(0).(string), args.Error(1)
}

func (r *ResourceManager) GetJobRuns(ctx context.Context, sensorParameters scheduler.JobSensorParameters, criteria *scheduler.JobRunsCriteria) ([]*scheduler.JobRunStatus, error) {
	args := r.Called(ctx, sensorParameters, criteria)
	return args.Get(0).([]*scheduler.JobRunStatus), args.Error(1)
}

func (r *ResourceManager) GetHostURL() string {
	args := r.Called()
	return args.Get(0).(string)
}
