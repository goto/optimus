package service

import (
	"context"
	"testing"

	"github.com/goto/salt/log"

	"github.com/goto/optimus/core/scheduler"
	"github.com/goto/optimus/core/tenant"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type mockJobDetailsGetter struct {
	mock.Mock
}

func (m *mockJobDetailsGetter) GetJobs(ctx context.Context, projectName tenant.ProjectName, jobs []string) ([]*scheduler.JobWithDetails, error) {
	args := m.Called(ctx, projectName, jobs)
	if jobsWithDetails, ok := args.Get(0).([]*scheduler.JobWithDetails); ok {
		return jobsWithDetails, args.Error(1)
	}
	return nil, args.Error(1)
}

func (m *mockJobDetailsGetter) GetJobsByLabels(ctx context.Context, projectName tenant.ProjectName, labels map[string]string) ([]*scheduler.JobWithDetails, error) {
	args := m.Called(ctx, projectName, labels)
	if jobsWithDetails, ok := args.Get(0).([]*scheduler.JobWithDetails); ok {
		return jobsWithDetails, args.Error(1)
	}
	return nil, args.Error(1)
}

func (m *mockJobDetailsGetter) GetJobsByLabelsMultiValue(ctx context.Context, projectName tenant.ProjectName, labels map[string][]string) ([]*scheduler.JobWithDetails, error) {
	args := m.Called(ctx, projectName, labels)
	if jobsWithDetails, ok := args.Get(0).([]*scheduler.JobWithDetails); ok {
		return jobsWithDetails, args.Error(1)
	}
	return nil, args.Error(1)
}

func project(t *testing.T, name string) tenant.ProjectName {
	t.Helper()
	projectName, err := tenant.ProjectNameFrom(name)
	assert.NoError(t, err)
	return projectName
}

func jobWithName(name string) *scheduler.JobWithDetails {
	return &scheduler.JobWithDetails{Name: scheduler.JobName(name)}
}

func TestGetJobWithDetailsMultiValue(t *testing.T) {
	ctx := context.Background()
	l := log.NewNoop()

	t.Run("merges jobs fetched by names and multi-value labels", func(t *testing.T) {
		getter := &mockJobDetailsGetter{}
		projectName := project(t, "test-project")

		getter.On("GetJobs", ctx, projectName, []string{"job-a"}).
			Return([]*scheduler.JobWithDetails{jobWithName("job-a")}, nil).Once()
		getter.On("GetJobsByLabelsMultiValue", ctx, projectName, map[string][]string{"team": {"de-1", "de-2"}}).
			Return([]*scheduler.JobWithDetails{jobWithName("job-a"), jobWithName("job-b")}, nil).Once()

		jobs, err := getJobWithDetailsMultiValue(ctx, l, getter, projectName, []scheduler.JobName{"job-a"}, map[string][]string{"team": {"de-1", "de-2"}})

		assert.NoError(t, err)
		assert.Len(t, jobs, 2) // job-a appears once despite matching both sources
		names := map[scheduler.JobName]bool{}
		for _, job := range jobs {
			names[job.Name] = true
		}
		assert.True(t, names["job-a"])
		assert.True(t, names["job-b"])
		getter.AssertExpectations(t)
	})

	t.Run("fetches only by labels when no names given", func(t *testing.T) {
		getter := &mockJobDetailsGetter{}
		projectName := project(t, "test-project")

		getter.On("GetJobsByLabelsMultiValue", ctx, projectName, map[string][]string{"domain": {"dispatch"}}).
			Return([]*scheduler.JobWithDetails{jobWithName("job-c")}, nil).Once()

		jobs, err := getJobWithDetailsMultiValue(ctx, l, getter, projectName, nil, map[string][]string{"domain": {"dispatch"}})

		assert.NoError(t, err)
		assert.Len(t, jobs, 1)
		assert.Equal(t, scheduler.JobName("job-c"), jobs[0].Name)
		getter.AssertExpectations(t)
	})

	t.Run("returns error when fetch by names fails outright", func(t *testing.T) {
		getter := &mockJobDetailsGetter{}
		projectName := project(t, "test-project")

		getter.On("GetJobs", ctx, projectName, []string{"job-a"}).
			Return(nil, assert.AnError).Once()

		jobs, err := getJobWithDetailsMultiValue(ctx, l, getter, projectName, []scheduler.JobName{"job-a"}, nil)

		assert.Error(t, err)
		assert.Nil(t, jobs)
		getter.AssertExpectations(t)
	})
}
