package service

import (
	"context"
	"testing"
	"time"

	"github.com/goto/salt/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/goto/optimus/config"
	"github.com/goto/optimus/core/scheduler"
	"github.com/goto/optimus/core/tenant"
)

type mockIncidentRepository struct {
	mock.Mock
}

// mockRootCauseLineageFetcher is a local JobLineageFetcher mock; the shared mockery mock
// lives in the external service_test package and is not visible here.
type mockRootCauseLineageFetcher struct {
	mock.Mock
}

func (m *mockRootCauseLineageFetcher) GetJobLineage(ctx context.Context, jobSchedules map[scheduler.JobName]*scheduler.JobSchedule, validLineageIntervalInHours int) (map[scheduler.JobName]*scheduler.JobLineageSummary, error) {
	args := m.Called(ctx, jobSchedules, validLineageIntervalInHours)
	if lineages, ok := args.Get(0).(map[scheduler.JobName]*scheduler.JobLineageSummary); ok {
		return lineages, args.Error(1)
	}
	return nil, args.Error(1)
}

// mockRootCauseDurationEstimator is a local DurationEstimator mock, for the same reason.
type mockRootCauseDurationEstimator struct {
	mock.Mock
}

func (m *mockRootCauseDurationEstimator) GetPercentileDurationByJobNames(ctx context.Context, referenceTime time.Time, jobNames []scheduler.JobName) (map[scheduler.JobName]*time.Duration, error) {
	args := m.Called(ctx, referenceTime, jobNames)
	if durations, ok := args.Get(0).(map[scheduler.JobName]*time.Duration); ok {
		return durations, args.Error(1)
	}
	return nil, args.Error(1)
}

func (m *mockRootCauseDurationEstimator) GetPercentileDurationByJobNamesByTask(ctx context.Context, referenceTime time.Time, jobNames []scheduler.JobName) (map[scheduler.JobName]*time.Duration, error) {
	args := m.Called(ctx, referenceTime, jobNames)
	if durations, ok := args.Get(0).(map[scheduler.JobName]*time.Duration); ok {
		return durations, args.Error(1)
	}
	return nil, args.Error(1)
}

func (m *mockRootCauseDurationEstimator) GetPercentileDurationByJobNamesByHookName(ctx context.Context, referenceTime time.Time, jobNames []scheduler.JobName, hookNames []string) (map[scheduler.JobName]*time.Duration, error) {
	args := m.Called(ctx, referenceTime, jobNames, hookNames)
	if durations, ok := args.Get(0).(map[scheduler.JobName]*time.Duration); ok {
		return durations, args.Error(1)
	}
	return nil, args.Error(1)
}

func rcDurPtr(d time.Duration) *time.Duration { return &d }

func (m *mockIncidentRepository) UpsertIncident(ctx context.Context, incident *scheduler.SLABreachIncident, impactedJobs []scheduler.ImpactedJob) error {
	args := m.Called(ctx, incident, impactedJobs)
	return args.Error(0)
}

func (m *mockIncidentRepository) GetActiveIncidents(ctx context.Context, since time.Time) ([]*scheduler.SLABreachIncidentWithImpacts, error) {
	args := m.Called(ctx, since)
	if results, ok := args.Get(0).([]*scheduler.SLABreachIncidentWithImpacts); ok {
		return results, args.Error(1)
	}
	return nil, args.Error(1)
}

func rcTestTenant(project string) tenant.Tenant {
	tnnt, err := tenant.NewTenant(project, "ns1")
	if err != nil {
		panic(err)
	}
	return tnnt
}

func rcPtr(t time.Time) *time.Time { return &t }

// rcMakeRun builds a JobRunSummary the way the resolver does: SLATime = scheduled_at + SLA.
func rcMakeRun(jobName scheduler.JobName, scheduledAt time.Time, sla time.Duration, taskStart, taskEnd *time.Time, status string) *scheduler.JobRunSummary {
	slaTime := scheduledAt.Add(sla)
	return &scheduler.JobRunSummary{
		JobName:       jobName,
		ScheduledAt:   scheduledAt,
		SLATime:       &slaTime,
		JobStatus:     status,
		TaskStartTime: taskStart,
		TaskEndTime:   taskEnd,
	}
}

// rcMakeLineage links a node to its upstreams the way the resolver does: each upstream
// carries, under the downstream's name, the run the downstream depends on.
func rcMakeLineage(jobName scheduler.JobName, tnnt tenant.Tenant, run *scheduler.JobRunSummary, upstreams ...*scheduler.JobLineageSummary) *scheduler.JobLineageSummary {
	lineage := &scheduler.JobLineageSummary{
		JobName:   jobName,
		Tenant:    tnnt,
		IsEnabled: true,
		JobRuns:   map[scheduler.JobName]*scheduler.JobRunSummary{jobName: run},
	}
	for _, upstream := range upstreams {
		upstream.JobRuns[jobName] = upstream.JobRuns[upstream.JobName]
	}
	if len(upstreams) > 0 {
		lineage.Upstreams = upstreams
	}
	return lineage
}

func TestJobRootCauseService_IdentifySLABreachRootCause(t *testing.T) {
	ctx := context.Background()
	projectName := tenant.ProjectName("p_root")
	tnnt := rcTestTenant("p_root")

	// 6-hour cadence; reference 13:30 resolves the 12:00 run as the in-range target
	base := time.Date(2026, 10, 26, 10, 0, 0, 0, time.UTC)
	referenceTime := base.Add(3*time.Hour + 30*time.Minute)
	targetScheduledAt := base.Add(2 * time.Hour) // 12:00

	jobWithDetails := &scheduler.JobWithDetails{
		Name: "job_target",
		Job: &scheduler.Job{
			Tenant: tnnt,
			Name:   "job_target",
		},
		Schedule: &scheduler.Schedule{
			StartDate: base.Add(-48 * time.Hour),
			Interval:  "0 6,12,18 * * *",
		},
		Alerts: []scheduler.Alert{{On: scheduler.EventCategorySLAMiss, Config: map[string]string{"duration": "1h"}}},
	}

	// upstream ran 119m against a 64m baseline: 55m induced, over the 30m threshold;
	// it finished a minute before the target started, so no scheduler delay is attributable
	upstreamRun := rcMakeRun("job_upstream", base,
		1*time.Hour, rcPtr(base), rcPtr(base.Add(119*time.Minute)), "success")
	targetRun := rcMakeRun("job_target", targetScheduledAt,
		1*time.Hour, rcPtr(targetScheduledAt), nil, "running")
	lineage := rcMakeLineage("job_target", tnnt, targetRun,
		rcMakeLineage("job_upstream", tnnt, upstreamRun))

	newService := func(detailsGetter *mockJobDetailsGetter, lineageFetcher *mockRootCauseLineageFetcher, estimator *mockRootCauseDurationEstimator, repo *mockIncidentRepository) *JobRootCauseService {
		return NewJobRootCauseService(
			log.NewNoop(),
			config.RootCauseAlertConfig{
				TaskHookDelayThresholdMinutes:   30,
				SchedulerDelayMinutesPerDepth:   2.5,
				ThirdPartyDelayThresholdSeconds: 600,
			},
			detailsGetter,
			lineageFetcher,
			estimator,
			repo,
			[]string{"dex"},
		)
	}

	t.Run("persists grouped incidents when alerts are enabled", func(t *testing.T) {
		detailsGetter := &mockJobDetailsGetter{}
		detailsGetter.On("GetJobs", ctx, projectName, []string{"job_target"}).Return([]*scheduler.JobWithDetails{jobWithDetails}, nil).Once()

		lineageFetcher := &mockRootCauseLineageFetcher{}
		lineageFetcher.On("GetJobLineage", ctx, mock.Anything, 2).
			Return(map[scheduler.JobName]*scheduler.JobLineageSummary{"job_target": lineage}, nil).Once()

		estimator := &mockRootCauseDurationEstimator{}
		estimator.On("GetPercentileDurationByJobNamesByTask", ctx, referenceTime, mock.Anything).
			Return(map[scheduler.JobName]*time.Duration{
				"job_target":   rcDurPtr(90 * time.Minute),
				"job_upstream": rcDurPtr(64 * time.Minute),
			}, nil).Once()

		repo := &mockIncidentRepository{}
		repo.On("UpsertIncident", ctx, mock.Anything, mock.Anything).Return(nil).Once().
			Run(func(args mock.Arguments) {
				incident := args.Get(1).(*scheduler.SLABreachIncident)
				impacts := args.Get(2).([]scheduler.ImpactedJob)

				assert.Equal(t, "p_root", incident.Key.ProjectName.String())
				assert.Equal(t, "job_upstream", incident.Key.JobName.String())
				assert.Len(t, incident.Details.Reasons, 1)
				assert.Equal(t, scheduler.TierRuntimeDelay, incident.Details.Reasons[0].Reason)
				assert.Equal(t, int(55*time.Minute/time.Second), incident.Details.Reasons[0].InducedDelaySeconds)
				assert.Equal(t, int(55*time.Minute/time.Second), incident.MaxInducedDelaySeconds)
				assert.Len(t, impacts, 1)
				assert.Equal(t, "job_target", impacts[0].JobName.String())
			})

		service := newService(detailsGetter, lineageFetcher, estimator, repo)

		incidents, err := service.IdentifySLABreachRootCause(ctx, RootCauseRequestConfig{
			ProjectNames:         []tenant.ProjectName{projectName},
			JobNames:             []scheduler.JobName{"job_target"},
			ReferenceTime:        referenceTime,
			EnableAlert:          true,
			ScheduleRangeInHours: 2 * time.Hour,
		})

		assert.NoError(t, err)
		assert.Len(t, incidents, 1)
		repo.AssertExpectations(t)
		estimator.AssertExpectations(t)
		lineageFetcher.AssertExpectations(t)
	})

	t.Run("observe mode evaluates without persisting", func(t *testing.T) {
		detailsGetter := &mockJobDetailsGetter{}
		detailsGetter.On("GetJobs", ctx, projectName, []string{"job_target"}).Return([]*scheduler.JobWithDetails{jobWithDetails}, nil).Once()

		lineageFetcher := &mockRootCauseLineageFetcher{}
		lineageFetcher.On("GetJobLineage", ctx, mock.Anything, 2).
			Return(map[scheduler.JobName]*scheduler.JobLineageSummary{"job_target": lineage}, nil).Once()

		estimator := &mockRootCauseDurationEstimator{}
		estimator.On("GetPercentileDurationByJobNamesByTask", ctx, referenceTime, mock.Anything).
			Return(map[scheduler.JobName]*time.Duration{
				"job_target":   rcDurPtr(90 * time.Minute),
				"job_upstream": rcDurPtr(64 * time.Minute),
			}, nil).Once()

		repo := &mockIncidentRepository{}

		service := newService(detailsGetter, lineageFetcher, estimator, repo)

		incidents, err := service.IdentifySLABreachRootCause(ctx, RootCauseRequestConfig{
			ProjectNames:         []tenant.ProjectName{projectName},
			JobNames:             []scheduler.JobName{"job_target"},
			ReferenceTime:        referenceTime,
			EnableAlert:          false,
			ScheduleRangeInHours: 2 * time.Hour,
		})

		assert.NoError(t, err)
		assert.Len(t, incidents, 1)
		repo.AssertNotCalled(t, "UpsertIncident")
	})

	t.Run("excluded projects are never evaluated", func(t *testing.T) {
		repo := &mockIncidentRepository{}

		service := NewJobRootCauseService(
			log.NewNoop(),
			config.RootCauseAlertConfig{ExcludeProjects: []string{"p_root"}},
			&mockJobDetailsGetter{}, &mockRootCauseLineageFetcher{}, &mockRootCauseDurationEstimator{},
			repo, []string{"dex"},
		)

		incidents, err := service.IdentifySLABreachRootCause(ctx, RootCauseRequestConfig{
			ProjectNames:         []tenant.ProjectName{projectName},
			ReferenceTime:        referenceTime,
			EnableAlert:          true,
			ScheduleRangeInHours: 2 * time.Hour,
		})

		assert.NoError(t, err)
		assert.Empty(t, incidents)
		repo.AssertNotCalled(t, "UpsertIncident")
	})
}
