package service // nolint:testpackage

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/goto/salt/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/goto/optimus/core/scheduler"
	"github.com/goto/optimus/core/tenant"
)

type mockAirflowSyncProjectRepo struct {
	mock.Mock
}

func (m *mockAirflowSyncProjectRepo) GetAll(ctx context.Context) ([]*tenant.Project, error) {
	args := m.Called(ctx)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]*tenant.Project), args.Error(1)
}

// concurrencyTrackingSyncStateRepo mocks AirflowSyncStateRepository. ReclaimStaleWindow is
// the first repo call processProject makes, so it's where we observe how many projects are
// being worked on at once: a short sleep widens the window during which an overlapping call
// would be visible, letting the test assert both that every project got processed and that
// concurrency never exceeded MaxConcurrentProjects.
type concurrencyTrackingSyncStateRepo struct {
	current int32
	max     int32
	calls   int32
}

func (r *concurrencyTrackingSyncStateRepo) GetWatermark(context.Context, tenant.ProjectName) (*time.Time, error) {
	return nil, nil //nolint:nilnil
}

func (r *concurrencyTrackingSyncStateRepo) ClaimWindow(context.Context, tenant.ProjectName, time.Time, time.Time, uuid.UUID, time.Duration) (uuid.UUID, bool, error) {
	return uuid.Nil, false, nil
}

func (r *concurrencyTrackingSyncStateRepo) ReclaimStaleWindow(_ context.Context, _ tenant.ProjectName, _ uuid.UUID, _ time.Duration, _ int) (*scheduler.AirflowSyncWindow, error) {
	atomic.AddInt32(&r.calls, 1)
	current := atomic.AddInt32(&r.current, 1)
	for {
		observedMax := atomic.LoadInt32(&r.max)
		if current <= observedMax || atomic.CompareAndSwapInt32(&r.max, observedMax, current) {
			break
		}
	}
	time.Sleep(20 * time.Millisecond)
	atomic.AddInt32(&r.current, -1)
	return nil, nil //nolint:nilnil
}

func (r *concurrencyTrackingSyncStateRepo) FailExhaustedWindows(context.Context, tenant.ProjectName, int, string) (int64, error) {
	return 0, nil
}

func (r *concurrencyTrackingSyncStateRepo) CompleteWindow(context.Context, uuid.UUID, uuid.UUID, *int64, int, int) (bool, error) {
	return true, nil
}

func (r *concurrencyTrackingSyncStateRepo) RecordAttemptError(context.Context, uuid.UUID, uuid.UUID, string) error {
	return nil
}

type noopWindowReconciler struct{}

func (noopWindowReconciler) ReconcileWindow(context.Context, tenant.ProjectName, time.Time, time.Time) (ReconcileWindowResult, error) {
	return ReconcileWindowResult{}, nil
}

func TestAirflowStateSyncWorkerTickConcurrency(t *testing.T) {
	const numProjects = 12
	const maxConcurrent = 3

	projects := make([]*tenant.Project, 0, numProjects)
	for i := 0; i < numProjects; i++ {
		conf := map[string]string{
			tenant.ProjectSchedulerHost:  "https://scheduler.example.com",
			tenant.ProjectStoragePathKey: "fs://bucket",
		}
		p, err := tenant.NewProject(fmt.Sprintf("proj-%d", i), conf, nil)
		assert.NoError(t, err)
		projects = append(projects, p)
	}

	projectRepo := &mockAirflowSyncProjectRepo{}
	projectRepo.On("GetAll", mock.Anything).Return(projects, nil)

	stateRepo := &concurrencyTrackingSyncStateRepo{}

	worker := NewAirflowStateSyncWorker(log.NewNoop(), projectRepo, stateRepo, noopWindowReconciler{}, AirflowStateSyncConfig{
		MaxAttempts:           3,
		MaxConcurrentProjects: maxConcurrent,
	})

	worker.tick(context.Background())

	assert.EqualValues(t, numProjects, atomic.LoadInt32(&stateRepo.calls), "every project should be processed exactly once")
	assert.LessOrEqual(t, int(atomic.LoadInt32(&stateRepo.max)), maxConcurrent, "concurrency must not exceed MaxConcurrentProjects")
	assert.Greater(t, int(atomic.LoadInt32(&stateRepo.max)), 1, "tick should actually run projects concurrently, not serially")
}
