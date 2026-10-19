// nolint: testpackage
package service

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/goto/salt/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/goto/optimus/core/scheduler"
	"github.com/goto/optimus/core/tenant"
	"github.com/goto/optimus/internal/lib/interval"
)

type mockJobRunRepoForFilter struct {
	mock.Mock
}

func (m *mockJobRunRepoForFilter) GetByID(ctx context.Context, id scheduler.JobRunID) (*scheduler.JobRun, error) {
	args := m.Called(ctx, id)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*scheduler.JobRun), args.Error(1)
}

func (m *mockJobRunRepoForFilter) GetByScheduledAt(ctx context.Context, tnnt tenant.Tenant, name scheduler.JobName, scheduledAt time.Time) (*scheduler.JobRun, error) {
	args := m.Called(ctx, tnnt, name, scheduledAt)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*scheduler.JobRun), args.Error(1)
}

func (m *mockJobRunRepoForFilter) GetLatestRun(ctx context.Context, project tenant.ProjectName, name scheduler.JobName, status *scheduler.State) (*scheduler.JobRun, error) {
	args := m.Called(ctx, project, name, status)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*scheduler.JobRun), args.Error(1)
}

func (m *mockJobRunRepoForFilter) GetRunsByTimeRange(ctx context.Context, project tenant.ProjectName, jobName scheduler.JobName, status *scheduler.State, since, until time.Time) ([]*scheduler.JobRun, error) {
	args := m.Called(ctx, project, jobName, status, since, until)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]*scheduler.JobRun), args.Error(1)
}

func (m *mockJobRunRepoForFilter) GetRunsByInterval(ctx context.Context, project tenant.ProjectName, jobName scheduler.JobName, interval interval.Interval) ([]*scheduler.JobRun, error) {
	args := m.Called(ctx, project, jobName, interval)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]*scheduler.JobRun), args.Error(1)
}

func (m *mockJobRunRepoForFilter) GetByScheduledTimes(ctx context.Context, tnnt tenant.Tenant, jobName scheduler.JobName, scheduledTimes []time.Time) ([]*scheduler.JobRun, error) {
	args := m.Called(ctx, tnnt, jobName, scheduledTimes)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]*scheduler.JobRun), args.Error(1)
}

func (m *mockJobRunRepoForFilter) GetPercentileDurationByJobNames(ctx context.Context, jobNames []scheduler.JobName, operators map[string][]string, referenceTime time.Time, lastNRuns, percentile int) (map[scheduler.JobName]*time.Duration, error) {
	args := m.Called(ctx, jobNames, operators, referenceTime, lastNRuns, percentile)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(map[scheduler.JobName]*time.Duration), args.Error(1)
}

func (m *mockJobRunRepoForFilter) Create(ctx context.Context, tnnt tenant.Tenant, name scheduler.JobName, scheduledAt time.Time, jobInterval interval.Interval, slaDefinitionInSec int64) error {
	args := m.Called(ctx, tnnt, name, scheduledAt, jobInterval, slaDefinitionInSec)
	return args.Error(0)
}

func (m *mockJobRunRepoForFilter) Update(ctx context.Context, jobRunID uuid.UUID, endTime time.Time, jobRunStatus scheduler.State) error {
	args := m.Called(ctx, jobRunID, endTime, jobRunStatus)
	return args.Error(0)
}

func (m *mockJobRunRepoForFilter) UpdateState(ctx context.Context, jobRunID uuid.UUID, jobRunStatus scheduler.State) error {
	args := m.Called(ctx, jobRunID, jobRunStatus)
	return args.Error(0)
}

func (m *mockJobRunRepoForFilter) UpdateSLA(ctx context.Context, jobName scheduler.JobName, project tenant.ProjectName, scheduledTimes []time.Time) error {
	args := m.Called(ctx, jobName, project, scheduledTimes)
	return args.Error(0)
}

func (m *mockJobRunRepoForFilter) UpdateMonitoring(ctx context.Context, jobRunID uuid.UUID, monitoring map[string]any) error {
	args := m.Called(ctx, jobRunID, monitoring)
	return args.Error(0)
}

func (m *mockJobRunRepoForFilter) GetRunSummaryByIdentifiers(ctx context.Context, identifiers []scheduler.JobRunIdentifier) ([]*scheduler.JobRunSummary, error) {
	args := m.Called(ctx, identifiers)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]*scheduler.JobRunSummary), args.Error(1)
}

func TestJobRunServiceFilterSLAObjects(t *testing.T) {
	tnnt, err := tenant.NewTenant("optimus-proj", "optimus-ns")
	require.NoError(t, err)
	jobName, err := scheduler.JobNameFrom("my-job")
	require.NoError(t, err)
	customBackfillRunID := "custom-backfill_9f7b0d4e-6b3a-4c11-8f2e-1d2b3c4d5e6f__2026-10-19"
	ctx := context.Background()

	t.Run("propagates the dag run id to the filtered sla objects", func(t *testing.T) {
		scheduledAt := time.Date(2026, 10, 19, 10, 0, 0, 0, time.UTC)
		event := &scheduler.Event{
			Tenant:  tnnt,
			JobName: jobName,
			SLAObjectList: []*scheduler.SLAObject{
				{JobName: jobName, JobScheduledAt: scheduledAt, RunID: customBackfillRunID},
			},
		}

		endTime := scheduledAt.Add(10 * time.Hour)
		jobRunRepo := new(mockJobRunRepoForFilter)
		jobRunRepo.On("GetByScheduledTimes", mock.Anything, tnnt, jobName, []time.Time{scheduledAt}).
			Return([]*scheduler.JobRun{{
				JobName:       jobName,
				Tenant:        tnnt,
				State:         scheduler.StateRunning,
				ScheduledAt:   scheduledAt,
				StartTime:     scheduledAt.Add(-time.Hour),
				EndTime:       &endTime,
				SLADefinition: int64(3600),
			}}, nil)

		svc := &JobRunService{repo: jobRunRepo, l: log.NewNoop()}
		filtered, _ := svc.filterSLAObjects(ctx, event)

		require.Len(t, filtered, 1)
		assert.Equal(t, customBackfillRunID, filtered[0].RunID)
		// event.SLAObjectList is replaced by the filtered list; it must keep the run id
		assert.Equal(t, customBackfillRunID, event.SLAObjectList[0].RunID)
	})

	t.Run("keeps the run id when no matching job runs are found", func(t *testing.T) {
		scheduledAt := time.Date(2026, 10, 19, 10, 0, 0, 0, time.UTC)
		event := &scheduler.Event{
			Tenant:  tnnt,
			JobName: jobName,
			SLAObjectList: []*scheduler.SLAObject{
				{JobName: jobName, JobScheduledAt: scheduledAt, RunID: customBackfillRunID},
			},
		}

		jobRunRepo := new(mockJobRunRepoForFilter)
		jobRunRepo.On("GetByScheduledTimes", mock.Anything, tnnt, jobName, []time.Time{scheduledAt}).
			Return([]*scheduler.JobRun{}, nil)

		svc := &JobRunService{repo: jobRunRepo, l: log.NewNoop()}
		filtered, _ := svc.filterSLAObjects(ctx, event)

		require.Len(t, filtered, 1)
		assert.Equal(t, customBackfillRunID, filtered[0].RunID)
		assert.Equal(t, customBackfillRunID, event.SLAObjectList[0].RunID)
	})
}
