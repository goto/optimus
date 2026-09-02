package service_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/goto/optimus/core/completeness/service"
	"github.com/goto/optimus/core/job"
	"github.com/goto/optimus/core/resource"
	"github.com/goto/optimus/core/scheduler"
	"github.com/goto/optimus/core/tenant"
	"github.com/goto/optimus/internal/errors"
	"github.com/goto/optimus/internal/lib/window"
	"github.com/goto/optimus/internal/models"
)

type mockUpstreamIdentifier struct{ mock.Mock }

func (m *mockUpstreamIdentifier) IdentifyUpstreamsFromQuery(ctx context.Context, datastoreName, svcAcc, query string) ([]resource.URN, error) {
	args := m.Called(ctx, datastoreName, svcAcc, query)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]resource.URN), args.Error(1)
}

type mockJobDestinationRepository struct{ mock.Mock }

func (m *mockJobDestinationRepository) GetAllByResourceDestination(ctx context.Context, resourceDestination resource.URN) ([]*job.Job, error) {
	args := m.Called(ctx, resourceDestination)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]*job.Job), args.Error(1)
}

type mockJobRunRepository struct{ mock.Mock }

func (m *mockJobRunRepository) GetByScheduledAt(ctx context.Context, t tenant.Tenant, jobName scheduler.JobName, scheduledAt time.Time) (*scheduler.JobRun, error) {
	args := m.Called(ctx, t, jobName, scheduledAt)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*scheduler.JobRun), args.Error(1)
}

type mockThirdPartyClient struct{ mock.Mock }

func (m *mockThirdPartyClient) IsManaged(ctx context.Context, resourceURN resource.URN) (bool, error) {
	args := m.Called(ctx, resourceURN)
	return args.Bool(0), args.Error(1)
}

func (m *mockThirdPartyClient) IsComplete(ctx context.Context, resourceURN resource.URN, dateFrom, dateTo time.Time) (bool, interface{}, error) {
	args := m.Called(ctx, resourceURN, dateFrom, dateTo)
	return args.Bool(0), args.Get(1), args.Error(2)
}

// buildJob constructs a minimal *job.Job fixture with a daily 1AM schedule, following
// the same builder chain as core/job/resolver/internal_upstream_resolver_test.go.
func buildJob(t *testing.T, tnnt tenant.Tenant, name job.Name, destination resource.URN) *job.Job {
	t.Helper()

	startDate, err := job.ScheduleDateFrom("2022-10-01")
	require.NoError(t, err)
	jobSchedule, err := job.NewScheduleBuilder(startDate).WithInterval("0 1 * * *").Build()
	require.NoError(t, err)

	w, err := models.NewWindow(1, "d", "24h", "24h")
	require.NoError(t, err)
	jobWindow := window.NewCustomConfig(w)

	taskName, err := job.TaskNameFrom("sample-task")
	require.NoError(t, err)
	jobTask := job.NewTask(taskName, map[string]string{}, "", nil)

	spec, err := job.NewSpecBuilder(1, name, "sample-owner", jobSchedule, jobWindow, jobTask).Build()
	require.NoError(t, err)

	return job.NewJob(tnnt, spec, destination, nil, false)
}

func TestCheckQueryCompleteness(t *testing.T) {
	ctx := context.Background()
	tnnt, err := tenant.NewTenant("proj", "ns1")
	require.NoError(t, err)

	tableURN, err := resource.ParseURN("maxcompute://p_gojek_id_mart.dataset.table_a")
	require.NoError(t, err)

	t.Run("unmanaged table checks the third-party client and reports managed_by_dex", func(t *testing.T) {
		upstreamIdentifier := &mockUpstreamIdentifier{}
		jobDestRepo := &mockJobDestinationRepository{}
		jobRunRepo := &mockJobRunRepository{}
		thirdParty := &mockThirdPartyClient{}

		upstreamIdentifier.On("IdentifyUpstreamsFromQuery", ctx, "maxcompute", "", "select 1").
			Return([]resource.URN{tableURN}, nil)
		jobDestRepo.On("GetAllByResourceDestination", mock.Anything, tableURN).Return([]*job.Job{}, nil)
		thirdParty.On("IsManaged", mock.Anything, tableURN).Return(true, nil)

		svc := service.NewService(upstreamIdentifier, jobDestRepo, jobRunRepo, thirdParty, service.Config{})
		result, err := svc.CheckQueryCompleteness(ctx, "maxcompute", "select 1")

		require.NoError(t, err)
		assert.Equal(t, service.OverallStatusComplete, result.OverallStatus) // vacuously complete, nothing Optimus-managed
		require.Len(t, result.UnmanagedTables, 1)
		assert.True(t, result.UnmanagedTables[0].ManagedByDex)
		assert.Empty(t, result.ManagedTables)
	})

	t.Run("single managed table reports its selected run and drives overall status", func(t *testing.T) {
		upstreamIdentifier := &mockUpstreamIdentifier{}
		jobDestRepo := &mockJobDestinationRepository{}
		jobRunRepo := &mockJobRunRepository{}

		jobName, err := job.NameFrom("job-a")
		require.NoError(t, err)
		theJob := buildJob(t, tnnt, jobName, tableURN)

		upstreamIdentifier.On("IdentifyUpstreamsFromQuery", ctx, "maxcompute", "", "select 1").
			Return([]resource.URN{tableURN}, nil)
		jobDestRepo.On("GetAllByResourceDestination", mock.Anything, tableURN).Return([]*job.Job{theJob}, nil)
		jobRunRepo.On("GetByScheduledAt", mock.Anything, tnnt, mock.Anything, mock.Anything).
			Return(&scheduler.JobRun{State: scheduler.StateSuccess, ScheduledAt: time.Now()}, nil)

		svc := service.NewService(upstreamIdentifier, jobDestRepo, jobRunRepo, nil, service.Config{})
		result, err := svc.CheckQueryCompleteness(ctx, "maxcompute", "select 1")

		require.NoError(t, err)
		require.Len(t, result.ManagedTables, 1)
		mt := result.ManagedTables[0]
		assert.Equal(t, "job-a", mt.JobName)
		assert.Equal(t, "proj", mt.OptimusProject)
		assert.Equal(t, "ns1", mt.OptimusNamespace)
		require.NotNil(t, mt.Run)
		assert.Equal(t, scheduler.StateSuccess, mt.Run.State)
		assert.Equal(t, service.OverallStatusComplete, result.OverallStatus)
	})

	t.Run("multiple jobs claiming the same destination are all surfaced, not just the first", func(t *testing.T) {
		upstreamIdentifier := &mockUpstreamIdentifier{}
		jobDestRepo := &mockJobDestinationRepository{}
		jobRunRepo := &mockJobRunRepository{}

		nameA, _ := job.NameFrom("job-a")
		nameB, _ := job.NameFrom("job-b")
		jobA := buildJob(t, tnnt, nameA, tableURN)
		jobB := buildJob(t, tnnt, nameB, tableURN)

		upstreamIdentifier.On("IdentifyUpstreamsFromQuery", ctx, "maxcompute", "", "select 1").
			Return([]resource.URN{tableURN}, nil)
		jobDestRepo.On("GetAllByResourceDestination", mock.Anything, tableURN).Return([]*job.Job{jobA, jobB}, nil)
		jobRunRepo.On("GetByScheduledAt", mock.Anything, tnnt, mock.Anything, mock.Anything).
			Return(nil, errors.NotFound(scheduler.EntityJobRun, "no run"))

		svc := service.NewService(upstreamIdentifier, jobDestRepo, jobRunRepo, nil, service.Config{})
		result, err := svc.CheckQueryCompleteness(ctx, "maxcompute", "select 1")

		require.NoError(t, err)
		require.Len(t, result.ManagedTables, 2)
		jobNames := []string{result.ManagedTables[0].JobName, result.ManagedTables[1].JobName}
		assert.ElementsMatch(t, []string{"job-a", "job-b"}, jobNames)
		// no run recorded for either -> both nil Run -> NOT_COMPLETE
		assert.Equal(t, service.OverallStatusNotComplete, result.OverallStatus)
		for _, mt := range result.ManagedTables {
			assert.Nil(t, mt.Run)
		}
	})

	t.Run("no tables found in query is rejected", func(t *testing.T) {
		upstreamIdentifier := &mockUpstreamIdentifier{}
		upstreamIdentifier.On("IdentifyUpstreamsFromQuery", ctx, "maxcompute", "", "select 1").
			Return([]resource.URN{}, nil)

		svc := service.NewService(upstreamIdentifier, &mockJobDestinationRepository{}, &mockJobRunRepository{}, nil, service.Config{})
		_, err := svc.CheckQueryCompleteness(ctx, "maxcompute", "select 1")

		require.Error(t, err)
	})
}
