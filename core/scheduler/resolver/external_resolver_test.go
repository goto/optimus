package resolver_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/goto/optimus/core/scheduler"
	"github.com/goto/optimus/core/scheduler/resolver"
	"github.com/goto/optimus/core/tenant"
)

// todo: write tests here
func TestExternalOptimusManager(t *testing.T) {
	ctx := context.Background()
	tnnt1, _ := tenant.NewTenant("test-proj", "test-ns")
	// resourceManager := new(ResourceManager)

	t.Run("returns max priority for root node", func(t *testing.T) {
		j1 := &scheduler.JobWithDetails{
			Name: scheduler.JobName("RootNode"),
			Job:  &scheduler.Job{Tenant: tnnt1},
			Upstreams: scheduler.Upstreams{
				UpstreamJobs: nil,
			},
		}

		s1 := resolver.SimpleResolver{}
		err := s1.Resolve(ctx, []*scheduler.JobWithDetails{j1})
		assert.NoError(t, err)
		assert.Equal(t, 10000, j1.Priority)
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
