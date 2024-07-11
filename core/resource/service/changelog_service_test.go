package service_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/goto/salt/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/goto/optimus/core/resource"
	"github.com/goto/optimus/core/resource/service"
	"github.com/goto/optimus/core/tenant"
)

func TestChangelogService(t *testing.T) {
	ctx := context.Background()
	logger := log.NewLogrus()
	tnnt, tenantErr := tenant.NewTenant("project_test", "namespace_tes")
	assert.NoError(t, tenantErr)

	t.Run("GetChangelogs", func(t *testing.T) {
		projectName := tnnt.ProjectName()
		resourceName := "project.dataset.table"

		t.Run("success fetching resource changelogs", func(t *testing.T) {
			var (
				repo               = newChangelogRepository(t)
				rscService         = service.NewChangelogService(logger, repo)
				now                = time.Now()
				resourceChangelogs = []*resource.ChangeLog{
					{
						Type: "update",
						Time: now.Add(2 * time.Hour),
						Change: []resource.Change{
							{
								Property: "metadata.Version",
								Diff:     "- 2\n+ 3",
							},
						},
					},
					{
						Type: "update",
						Time: now,
						Change: []resource.Change{
							{
								Property: "metadata.Description",
								Diff:     "- a table used to get the booking\n+ detail of gofood booking",
							},
						},
					},
				}
			)
			defer repo.AssertExpectations(t)

			repo.On("GetChangelogs", ctx, projectName, resource.Name(resourceName)).Return(resourceChangelogs, nil).Once()

			actualChangelogs, err := rscService.GetChangelogs(ctx, projectName, resource.Name(resourceName))
			assert.NoError(t, err)
			assert.NotNil(t, actualChangelogs)
			assert.Equal(t, resourceChangelogs, actualChangelogs)
		})

		t.Run("error fetching resource changelogs", func(t *testing.T) {
			var (
				repo       = newChangelogRepository(t)
				rscService = service.NewChangelogService(logger, repo)
			)
			defer repo.AssertExpectations(t)

			repo.On("GetChangelogs", ctx, projectName, resource.Name(resourceName)).Return(nil, errors.New("error")).Once()

			actualChangelogs, err := rscService.GetChangelogs(ctx, projectName, resource.Name(resourceName))
			assert.Error(t, err)
			assert.Nil(t, actualChangelogs)
		})
	})
}

// ChangelogRepository is an autogenerated mock type for the ChangelogRepository type
type ChangelogRepository struct {
	mock.Mock
}

// GetChangelogs provides a mock function with given fields: ctx, projectName, resourceName
func (_m *ChangelogRepository) GetChangelogs(ctx context.Context, projectName tenant.ProjectName, resourceName resource.Name) ([]*resource.ChangeLog, error) {
	ret := _m.Called(ctx, projectName, resourceName)

	if len(ret) == 0 {
		panic("no return value specified for GetChangelogs")
	}

	var r0 []*resource.ChangeLog
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context, tenant.ProjectName, resource.Name) ([]*resource.ChangeLog, error)); ok {
		return rf(ctx, projectName, resourceName)
	}
	if rf, ok := ret.Get(0).(func(context.Context, tenant.ProjectName, resource.Name) []*resource.ChangeLog); ok {
		r0 = rf(ctx, projectName, resourceName)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]*resource.ChangeLog)
		}
	}

	if rf, ok := ret.Get(1).(func(context.Context, tenant.ProjectName, resource.Name) error); ok {
		r1 = rf(ctx, projectName, resourceName)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// NewChangelogRepository creates a new instance of ChangelogRepository. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func newChangelogRepository(t interface {
	mock.TestingT
	Cleanup(func())
},
) *ChangelogRepository {
	mock := &ChangelogRepository{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}