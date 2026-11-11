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

	"github.com/goto/optimus/config"
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

func (m *mockSLAJobRepo) GetJobDetails(ctx context.Context, projectName tenant.ProjectName, jobName scheduler.JobName) (*scheduler.JobWithDetails, error) {
	args := m.Called(ctx, projectName, jobName)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*scheduler.JobWithDetails), args.Error(1)
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

		worker := NewSLAWorker(log.NewNoop(), alertManager, operatorSLARepo, jobRepo, nil, nil, nil, runScheduler, config.OperatorSLAMissConfig{})
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

		worker := NewSLAWorker(log.NewNoop(), alertManager, operatorSLARepo, jobRepo, nil, nil, nil, runScheduler, config.OperatorSLAMissConfig{})
		worker.process(ctx, uuid.New().String(), time.Minute)

		operatorSLARepo.AssertExpectations(t)
		alertManager.AssertNotCalled(t, "SendOperatorSLAEvent", mock.Anything)
	})
}

// embed the full repositories so only the methods SendOperatorSLAEvent touches need stubs
type capJobRunRepo struct {
	JobRunRepository
}

func (capJobRunRepo) GetByScheduledAt(context.Context, tenant.Tenant, scheduler.JobName, time.Time) (*scheduler.JobRun, error) {
	return &scheduler.JobRun{}, nil
}

type capOperatorRunRepo struct {
	OperatorRunRepository
}

func (capOperatorRunRepo) GetOperatorRun(context.Context, string, scheduler.OperatorType, uuid.UUID) (*scheduler.OperatorRun, error) {
	return &scheduler.OperatorRun{}, nil
}

type capTenantService struct {
	TenantService
}

func (capTenantService) GetDetails(context.Context, tenant.Tenant) (*tenant.WithDetails, error) {
	return &tenant.WithDetails{}, nil
}

func TestSLAWorkerOperatorSLASeverityCap(t *testing.T) {
	ctx := context.Background()

	jobName, err := scheduler.JobNameFrom("my-job")
	require.NoError(t, err)

	criticalSLAConfig := &scheduler.SLAAlertConfig{
		DurationThreshold: 30 * time.Minute,
		Severity:          scheduler.Critical,
	}

	newSLA := func(projectName tenant.ProjectName) *scheduler.OperatorsSLA {
		return &scheduler.OperatorsSLA{
			JobName:      jobName,
			ProjectName:  projectName,
			OperatorName: "transformer",
			OperatorType: scheduler.OperatorTask,
			AlertTag:     criticalSLAConfig.Tag(),
		}
	}

	newWorker := func(cfg config.OperatorSLAMissConfig, jobLabels map[string]string) (*SLAWorker, *mockSLAAlertManager) {
		job := &scheduler.Job{
			Name:   jobName,
			Tenant: tenant.Tenant{}, // set per project below
			Task: &scheduler.Task{
				AlertConfig: &scheduler.OperatorAlertConfig{SLAAlertConfigs: []*scheduler.SLAAlertConfig{criticalSLAConfig}},
			},
		}
		jobRepo := new(mockSLAJobRepo)
		jobRepo.On("GetJob", mock.Anything, mock.Anything, jobName).Return(job, nil)
		jobRepo.On("GetJobDetails", mock.Anything, mock.Anything, jobName).
			Return(&scheduler.JobWithDetails{Job: job, JobMetadata: &scheduler.JobMetadata{Labels: jobLabels}}, nil)

		alertManager := new(mockSLAAlertManager)
		worker := NewSLAWorker(log.NewNoop(), alertManager, nil, jobRepo, capJobRunRepo{}, capOperatorRunRepo{}, capTenantService{}, nil, cfg)
		return worker, alertManager
	}

	t.Run("should cap CRITICAL to WARNING for projects in warning_only_projects", func(t *testing.T) {
		projName, err := tenant.ProjectNameFrom("noisy-proj")
		require.NoError(t, err)

		worker, alertManager := newWorker(config.OperatorSLAMissConfig{WarningOnlyProjects: []string{projName.String()}}, nil)
		job, err := worker.jobRepo.GetJob(ctx, projName, jobName)
		require.NoError(t, err)
		job.Tenant, err = tenant.NewTenant(projName.String(), "ns1")
		require.NoError(t, err)

		alertManager.On("SendOperatorSLAEvent", mock.MatchedBy(func(attr *scheduler.OperatorSLAAlertAttrs) bool {
			return attr.Severity == scheduler.Warning.String()
		})).Return(nil).Once()

		require.NoError(t, worker.SendOperatorSLAEvent(ctx, job, newSLA(projName)))
		alertManager.AssertExpectations(t)
	})

	t.Run("should keep CRITICAL for projects not in warning_only_projects", func(t *testing.T) {
		projName, err := tenant.ProjectNameFrom("quiet-proj")
		require.NoError(t, err)

		worker, alertManager := newWorker(config.OperatorSLAMissConfig{WarningOnlyProjects: []string{"other-proj"}}, nil)
		job, err := worker.jobRepo.GetJob(ctx, projName, jobName)
		require.NoError(t, err)
		job.Tenant, err = tenant.NewTenant(projName.String(), "ns1")
		require.NoError(t, err)

		alertManager.On("SendOperatorSLAEvent", mock.MatchedBy(func(attr *scheduler.OperatorSLAAlertAttrs) bool {
			return attr.Severity == scheduler.Critical.String()
		})).Return(nil).Once()

		require.NoError(t, worker.SendOperatorSLAEvent(ctx, job, newSLA(projName)))
		alertManager.AssertExpectations(t)
	})

	t.Run("should keep INFO for projects in warning_only_projects (cap, not force)", func(t *testing.T) {
		projName, err := tenant.ProjectNameFrom("noisy-proj")
		require.NoError(t, err)

		worker, alertManager := newWorker(config.OperatorSLAMissConfig{WarningOnlyProjects: []string{projName.String()}}, nil)
		job, err := worker.jobRepo.GetJob(ctx, projName, jobName)
		require.NoError(t, err)
		job.Tenant, err = tenant.NewTenant(projName.String(), "ns1")
		require.NoError(t, err)

		infoSLAConfig := &scheduler.SLAAlertConfig{DurationThreshold: 30 * time.Minute, Severity: scheduler.Info}
		slaObj := newSLA(projName)
		slaObj.AlertTag = infoSLAConfig.Tag()
		job.Task.AlertConfig.SLAAlertConfigs = []*scheduler.SLAAlertConfig{infoSLAConfig}

		alertManager.On("SendOperatorSLAEvent", mock.MatchedBy(func(attr *scheduler.OperatorSLAAlertAttrs) bool {
			return attr.Severity == scheduler.Info.String()
		})).Return(nil).Once()

		require.NoError(t, worker.SendOperatorSLAEvent(ctx, job, slaObj))
		alertManager.AssertExpectations(t)
	})

	// Asia/Jakarta is UTC+7
	criticalExpectation := func(alertManager *mockSLAAlertManager, severity scheduler.Severity) {
		alertManager.On("SendOperatorSLAEvent", mock.MatchedBy(func(attr *scheduler.OperatorSLAAlertAttrs) bool {
			return attr.Severity == severity.String()
		})).Return(nil).Once()
	}

	t.Run("should cap when SLA deadline is at the inclusive end hour in the configured timezone", func(t *testing.T) {
		projName, err := tenant.ProjectNameFrom("noisy-proj")
		require.NoError(t, err)

		worker, alertManager := newWorker(config.OperatorSLAMissConfig{
			WarningOnlyProjects:  []string{projName.String()},
			ActiveWindowTimezone: "Asia/Jakarta",
			ActiveWindowEndHour:  10,
		}, nil)
		job, err := worker.jobRepo.GetJob(ctx, projName, jobName)
		require.NoError(t, err)
		job.Tenant, err = tenant.NewTenant(projName.String(), "ns1")
		require.NoError(t, err)

		// 03:30 UTC = 10:30 WIB, still inside the inclusive end hour 10
		slaObj := newSLA(projName)
		slaObj.SLATime = time.Date(2026, 11, 11, 3, 30, 0, 0, time.UTC)
		criticalExpectation(alertManager, scheduler.Warning)

		require.NoError(t, worker.SendOperatorSLAEvent(ctx, job, slaObj))
		alertManager.AssertExpectations(t)
	})

	t.Run("should keep CRITICAL when the SLA deadline is past the end hour", func(t *testing.T) {
		projName, err := tenant.ProjectNameFrom("noisy-proj")
		require.NoError(t, err)

		worker, alertManager := newWorker(config.OperatorSLAMissConfig{
			WarningOnlyProjects:  []string{projName.String()},
			ActiveWindowTimezone: "Asia/Jakarta",
			ActiveWindowEndHour:  10,
		}, nil)
		job, err := worker.jobRepo.GetJob(ctx, projName, jobName)
		require.NoError(t, err)
		job.Tenant, err = tenant.NewTenant(projName.String(), "ns1")
		require.NoError(t, err)

		// 04:30 UTC = 11:30 WIB, past the end hour
		slaObj := newSLA(projName)
		slaObj.SLATime = time.Date(2026, 11, 11, 4, 30, 0, 0, time.UTC)
		criticalExpectation(alertManager, scheduler.Critical)

		require.NoError(t, worker.SendOperatorSLAEvent(ctx, job, slaObj))
		alertManager.AssertExpectations(t)
	})

	t.Run("should cap when the job matches a full DNF label group", func(t *testing.T) {
		projName, err := tenant.ProjectNameFrom("noisy-proj")
		require.NoError(t, err)

		// (category=business-critical AND criticality=low) OR (team=dataplatform)
		worker, alertManager := newWorker(config.OperatorSLAMissConfig{
			WarningOnlyProjects: []string{projName.String()},
			JobLabels:           []string{"category_table_tag:business-critical,criticality:low", "team:dataplatform"},
		}, map[string]string{"category_table_tag": "business-critical", "criticality": "low"})
		job, err := worker.jobRepo.GetJob(ctx, projName, jobName)
		require.NoError(t, err)
		job.Tenant, err = tenant.NewTenant(projName.String(), "ns1")
		require.NoError(t, err)

		criticalExpectation(alertManager, scheduler.Warning)

		require.NoError(t, worker.SendOperatorSLAEvent(ctx, job, newSLA(projName)))
		alertManager.AssertExpectations(t)
	})

	t.Run("should keep CRITICAL when the job matches only part of an AND group", func(t *testing.T) {
		projName, err := tenant.ProjectNameFrom("noisy-proj")
		require.NoError(t, err)

		worker, alertManager := newWorker(config.OperatorSLAMissConfig{
			WarningOnlyProjects: []string{projName.String()},
			JobLabels: []string{
				"category_table_tag:business-critical,critical_table_tag:sla-9am",
				"category_table_tag:business-critical,critical_table_tag:sla-8am",
			},
		}, map[string]string{"category_table_tag": "business-critical"}) // no critical_table_tag
		job, err := worker.jobRepo.GetJob(ctx, projName, jobName)
		require.NoError(t, err)
		job.Tenant, err = tenant.NewTenant(projName.String(), "ns1")
		require.NoError(t, err)

		criticalExpectation(alertManager, scheduler.Critical)

		require.NoError(t, worker.SendOperatorSLAEvent(ctx, job, newSLA(projName)))
		alertManager.AssertExpectations(t)
	})

	t.Run("should keep CRITICAL when the job matches none of the configured labels", func(t *testing.T) {
		projName, err := tenant.ProjectNameFrom("noisy-proj")
		require.NoError(t, err)

		worker, alertManager := newWorker(config.OperatorSLAMissConfig{
			WarningOnlyProjects: []string{projName.String()},
			JobLabels:           []string{"team:dataplatform"},
		}, map[string]string{"team": "analytics"})
		job, err := worker.jobRepo.GetJob(ctx, projName, jobName)
		require.NoError(t, err)
		job.Tenant, err = tenant.NewTenant(projName.String(), "ns1")
		require.NoError(t, err)

		criticalExpectation(alertManager, scheduler.Critical)

		require.NoError(t, worker.SendOperatorSLAEvent(ctx, job, newSLA(projName)))
		alertManager.AssertExpectations(t)
	})
}

func TestParseJobLabelGroups(t *testing.T) {
	logger := log.NewNoop()

	t.Run("should parse comma-separated AND groups and drop malformed pairs", func(t *testing.T) {
		groups := parseJobLabelGroups(logger, []string{"team:dataplatform,criticality:low", "novalue", "empty:"})
		require.Equal(t, [][]jobLabel{{
			{key: "team", value: "dataplatform"},
			{key: "criticality", value: "low"},
		}}, groups)
	})

	t.Run("should keep only groups with at least one valid pair", func(t *testing.T) {
		groups := parseJobLabelGroups(logger, []string{"a:1,broken,b:2", ",,"})
		require.Equal(t, [][]jobLabel{
			{{key: "a", value: "1"}, {key: "b", value: "2"}},
		}, groups)
	})

	t.Run("should return no groups for empty input", func(t *testing.T) {
		require.Empty(t, parseJobLabelGroups(logger, nil))
	})
}
