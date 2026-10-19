// nolint: testpackage
package service

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/goto/salt/log"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/goto/optimus/core/scheduler"
	"github.com/goto/optimus/core/tenant"
)

type mockSLAOperatorRepo struct {
	mock.Mock
}

func (m *mockSLAOperatorRepo) GetExpiredSLAsForProcessing(ctx context.Context, signature string, processingDuration time.Duration) ([]*scheduler.OperatorsSLA, error) {
	args := m.Called(ctx, signature, processingDuration)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]*scheduler.OperatorsSLA), args.Error(1)
}

func (m *mockSLAOperatorRepo) RemoveProcessedSLA(ctx context.Context, slaID uuid.UUID) error {
	args := m.Called(ctx, slaID)
	return args.Error(0)
}

type mockSLAJobRepo struct {
	mock.Mock
}

func (m *mockSLAJobRepo) GetJob(ctx context.Context, projectName tenant.ProjectName, jobName scheduler.JobName) (*scheduler.Job, error) {
	args := m.Called(ctx, projectName, jobName)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*scheduler.Job), args.Error(1)
}

type mockSLARunScheduler struct {
	mock.Mock
}

func (m *mockSLARunScheduler) GetOperatorInstance(ctx context.Context, tnnt tenant.Tenant, jobName scheduler.JobName, jobRunID, operatorName string) (*scheduler.OperatorRunInstance, error) {
	args := m.Called(ctx, tnnt, jobName, jobRunID, operatorName)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*scheduler.OperatorRunInstance), args.Error(1)
}

type mockSLAAlertManager struct {
	mock.Mock
}

func (m *mockSLAAlertManager) SendOperatorSLAEvent(attr *scheduler.OperatorSLAAlertAttrs) {
	m.Called(attr)
}

func (m *mockSLAAlertManager) SendJobRunEvent(attr *scheduler.AlertAttrs) {
	m.Called(attr)
}

func (m *mockSLAAlertManager) SendReplayEvent(attr *scheduler.ReplayNotificationAttrs) {
	m.Called(attr)
}

func TestSLAWorkerProcess(t *testing.T) {
	projName, err := tenant.ProjectNameFrom("optimus-proj")
	require.NoError(t, err)
	tnnt, err := tenant.NewTenant(projName.String(), "optimus-ns")
	require.NoError(t, err)
	jobName, err := scheduler.JobNameFrom("my-job")
	require.NoError(t, err)

	ctx := context.Background()

	t.Run("should process custom backfill runs like regular runs (suppression happens at alertmanager)", func(t *testing.T) {
		slaID := uuid.New()
		slaObj := &scheduler.OperatorsSLA{
			ID:                slaID,
			JobName:           jobName,
			ProjectName:       projName,
			OperatorName:      "transformer",
			OperatorType:      scheduler.OperatorTask,
			RunID:             "custom-backfill_" + uuid.New().String() + "__2026-10-19",
			SLATime:           time.Now().Add(-time.Hour),
			ScheduledAt:       time.Now().Add(time.Hour), // custom backfills carry a future scheduled_at
			OperatorStartTime: time.Now().Add(-2 * time.Hour),
		}
		job := &scheduler.Job{Name: jobName, Tenant: tnnt, Task: &scheduler.Task{}}

		operatorSLARepo := new(mockSLAOperatorRepo)
		operatorSLARepo.On("GetExpiredSLAsForProcessing", mock.Anything, mock.Anything, mock.Anything).
			Return([]*scheduler.OperatorsSLA{slaObj}, nil)
		// the operator finished before its SLA time, so no alert is sent but the record is removed
		operatorSLARepo.On("RemoveProcessedSLA", mock.Anything, slaID).Return(nil)

		runScheduler := new(mockSLARunScheduler)
		endTime := time.Now().Add(-2 * time.Hour)
		runScheduler.On("GetOperatorInstance", mock.Anything, tnnt, jobName, slaObj.RunID, slaObj.OperatorName).
			Return(&scheduler.OperatorRunInstance{State: scheduler.StateSuccess.String(), EndTime: &endTime}, nil)

		jobRepo := new(mockSLAJobRepo)
		jobRepo.On("GetJob", mock.Anything, projName, jobName).Return(job, nil)

		alertManager := new(mockSLAAlertManager)

		worker := NewSLAWorker(log.NewNoop(), alertManager, operatorSLARepo, jobRepo, nil, nil, nil, runScheduler)
		worker.process(ctx, uuid.New().String(), time.Minute)

		operatorSLARepo.AssertExpectations(t)
		runScheduler.AssertExpectations(t)
		alertManager.AssertNotCalled(t, "SendOperatorSLAEvent", mock.Anything)
	})

	t.Run("should still process SLA breaches for regular runs", func(t *testing.T) {
		slaID := uuid.New()
		slaObj := &scheduler.OperatorsSLA{
			ID:                slaID,
			JobName:           jobName,
			ProjectName:       projName,
			OperatorName:      "transformer",
			OperatorType:      scheduler.OperatorTask,
			RunID:             "scheduled__2026-10-19T10:00:00+00:00",
			SLATime:           time.Now().Add(-time.Hour),
			ScheduledAt:       time.Now().Add(-2 * time.Hour),
			OperatorStartTime: time.Now().Add(-2 * time.Hour),
		}
		job := &scheduler.Job{Name: jobName, Tenant: tnnt, Task: &scheduler.Task{}}

		operatorSLARepo := new(mockSLAOperatorRepo)
		operatorSLARepo.On("GetExpiredSLAsForProcessing", mock.Anything, mock.Anything, mock.Anything).
			Return([]*scheduler.OperatorsSLA{slaObj}, nil)
		// the operator finished before its SLA time, so no alert is sent but the record is removed
		operatorSLARepo.On("RemoveProcessedSLA", mock.Anything, slaID).Return(nil)

		runScheduler := new(mockSLARunScheduler)
		endTime := time.Now().Add(-2 * time.Hour)
		runScheduler.On("GetOperatorInstance", mock.Anything, tnnt, jobName, slaObj.RunID, slaObj.OperatorName).
			Return(&scheduler.OperatorRunInstance{State: scheduler.StateSuccess.String(), EndTime: &endTime}, nil)

		jobRepo := new(mockSLAJobRepo)
		jobRepo.On("GetJob", mock.Anything, projName, jobName).Return(job, nil)

		alertManager := new(mockSLAAlertManager)

		worker := NewSLAWorker(log.NewNoop(), alertManager, operatorSLARepo, jobRepo, nil, nil, nil, runScheduler)
		worker.process(ctx, uuid.New().String(), time.Minute)

		operatorSLARepo.AssertExpectations(t)
		alertManager.AssertNotCalled(t, "SendOperatorSLAEvent", mock.Anything)
	})
}
