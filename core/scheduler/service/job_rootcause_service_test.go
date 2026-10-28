package service_test

import (
	"context"
	"testing"
	"time"

	"github.com/goto/salt/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/goto/optimus/config"
	"github.com/goto/optimus/core/scheduler"
	"github.com/goto/optimus/core/scheduler/service"
	"github.com/goto/optimus/core/tenant"
)

type mockIncidentRepository struct {
	mock.Mock
}

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

// mockRootCauseDetailsGetter is a local JobDetailsGetter mock; the shared mockery mock
// lives in the internal service test package and is not visible from service_test.
type mockRootCauseDetailsGetter struct {
	mock.Mock
}

func (m *mockRootCauseDetailsGetter) GetJobs(ctx context.Context, projectName tenant.ProjectName, jobs []string) ([]*scheduler.JobWithDetails, error) {
	args := m.Called(ctx, projectName, jobs)
	if jobsWithDetails, ok := args.Get(0).([]*scheduler.JobWithDetails); ok {
		return jobsWithDetails, args.Error(1)
	}
	return nil, args.Error(1)
}

func (m *mockRootCauseDetailsGetter) GetJobsByLabels(ctx context.Context, projectName tenant.ProjectName, labels map[string]string) ([]*scheduler.JobWithDetails, error) {
	args := m.Called(ctx, projectName, labels)
	if jobsWithDetails, ok := args.Get(0).([]*scheduler.JobWithDetails); ok {
		return jobsWithDetails, args.Error(1)
	}
	return nil, args.Error(1)
}

func (m *mockRootCauseDetailsGetter) GetJobsByLabelsMultiValue(ctx context.Context, projectName tenant.ProjectName, labels map[string][]string) ([]*scheduler.JobWithDetails, error) {
	args := m.Called(ctx, projectName, labels)
	if jobsWithDetails, ok := args.Get(0).([]*scheduler.JobWithDetails); ok {
		return jobsWithDetails, args.Error(1)
	}
	return nil, args.Error(1)
}

// mockRootCauseLineageFetcher is a local JobLineageFetcher mock, for the same reason.
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

type mockRootCauseNotifier struct {
	mock.Mock
}

func (m *mockRootCauseNotifier) SendRootCauseBreach(alert *scheduler.RootCauseBreachAlert) {
	m.Called(alert)
}

type mockProjectTeamGetter struct {
	mock.Mock
}

func (m *mockProjectTeamGetter) Get(ctx context.Context, projectName tenant.ProjectName) (*tenant.Project, error) {
	args := m.Called(ctx, projectName)
	if project, ok := args.Get(0).(*tenant.Project); ok {
		return project, args.Error(1)
	}
	return nil, args.Error(1)
}

func rcDurPtr(d time.Duration) *time.Duration { return &d }

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

	newJobWithDetails := func(name string) *scheduler.JobWithDetails {
		return &scheduler.JobWithDetails{
			Name: scheduler.JobName(name),
			Job: &scheduler.Job{
				Tenant: tnnt,
				Name:   scheduler.JobName(name),
			},
			Schedule: &scheduler.Schedule{
				StartDate: base.Add(-48 * time.Hour),
				Interval:  "0 6,12,18 * * *",
			},
			Alerts: []scheduler.Alert{{On: scheduler.EventCategorySLAMiss, Config: map[string]string{"duration": "1h"}}},
		}
	}
	jobWithDetails := newJobWithDetails("job_target")

	// a target whose own runtime matches its baseline so it induces nothing
	targetBaseline := 150 * time.Minute

	// upstream ran 119m against a 64m baseline: 55m induced, over the 30m threshold;
	// it finished a minute before the target started, so no scheduler delay is attributable
	upstreamRun := rcMakeRun("job_upstream", base,
		1*time.Hour, rcPtr(base), rcPtr(base.Add(119*time.Minute)), "success")
	targetRun := rcMakeRun("job_target", targetScheduledAt,
		1*time.Hour, rcPtr(targetScheduledAt), nil, "running")
	lineage := rcMakeLineage("job_target", tnnt, targetRun,
		rcMakeLineage("job_upstream", tnnt, upstreamRun))

	newEstimator := func(t *testing.T, durations map[string]time.Duration) *mockRootCauseDurationEstimator {
		estimator := &mockRootCauseDurationEstimator{}
		durationsByJob := map[scheduler.JobName]*time.Duration{}
		for jobName, duration := range durations {
			duration := duration
			durationsByJob[scheduler.JobName(jobName)] = &duration
		}
		estimator.On("GetPercentileDurationByJobNamesByTask", ctx, referenceTime, mock.Anything).
			Return(durationsByJob, nil).Once()
		return estimator
	}

	newService := func(detailsGetter *mockRootCauseDetailsGetter, lineageFetcher *mockRootCauseLineageFetcher, estimator *mockRootCauseDurationEstimator,
		repo *mockIncidentRepository, notifier *mockRootCauseNotifier, teamGetter *mockProjectTeamGetter,
		alertConfig *config.RootCauseAlertConfig,
	) *service.JobRootCauseService {
		if alertConfig == nil {
			alertConfig = &config.RootCauseAlertConfig{
				RuntimeDelayThresholdMinutes:    30,
				SchedulerDelayMinutesPerDepth:   2.5,
				ThirdPartyDelayThresholdSeconds: 600,
			}
		}
		return service.NewJobRootCauseService(
			log.NewNoop(),
			*alertConfig,
			detailsGetter,
			lineageFetcher,
			estimator,
			repo,
			[]string{"dex"},
			notifier,
			teamGetter,
		)
	}

	t.Run("persists incidents and routes runtime delay to the project owner", func(t *testing.T) {
		detailsGetter := &mockRootCauseDetailsGetter{}
		detailsGetter.On("GetJobs", ctx, projectName, []string{"job_target"}).Return([]*scheduler.JobWithDetails{jobWithDetails}, nil).Once()

		lineageFetcher := &mockRootCauseLineageFetcher{}
		lineageFetcher.On("GetJobLineage", ctx, mock.Anything, 2).
			Return(map[scheduler.JobName]*scheduler.JobLineageSummary{"job_target": lineage}, nil).Once()

		// upstream ran 119m against a 64m baseline: 55m induced, over the 30m threshold;
		// it finished a minute before the target started, so no scheduler delay is attributable
		estimator := newEstimator(t, map[string]time.Duration{
			"job_target":   targetBaseline,
			"job_upstream": 64 * time.Minute,
		})

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
				assert.Len(t, impacts, 1)
				assert.Equal(t, "job_target", impacts[0].JobName.String())
			})

		notifier := &mockRootCauseNotifier{}
		notifier.On("SendRootCauseBreach", mock.Anything).Once().Run(func(args mock.Arguments) {
			alert := args.Get(0).(*scheduler.RootCauseBreachAlert)
			assert.Equal(t, "proj-team", alert.Team)
			assert.Equal(t, "job_upstream", alert.RootCauseJob)
			assert.NotNil(t, alert.RootCauseScheduledAt)
			assert.Equal(t, scheduler.TierRuntimeDelay, alert.Reason.Reason)
		})

		teamGetter := &mockProjectTeamGetter{}
		projectWithTeam, err := tenant.NewProject("p_root", map[string]string{
			tenant.ProjectStoragePathKey:   "gs://bucket",
			tenant.ProjectSchedulerHost:    "airflow.example.com",
			tenant.ProjectAlertManagerTeam: "proj-team",
		}, nil)
		assert.NoError(t, err)
		teamGetter.On("Get", ctx, projectName).Return(projectWithTeam, nil).Once()

		svc := newService(detailsGetter, lineageFetcher, estimator, repo, notifier, teamGetter, nil)

		incidents, err := svc.IdentifySLABreachRootCause(ctx, service.RootCauseRequestConfig{
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
		notifier.AssertExpectations(t)
	})

	t.Run("observe mode evaluates without persisting or alerting", func(t *testing.T) {
		detailsGetter := &mockRootCauseDetailsGetter{}
		detailsGetter.On("GetJobs", ctx, projectName, []string{"job_target"}).Return([]*scheduler.JobWithDetails{jobWithDetails}, nil).Once()

		lineageFetcher := &mockRootCauseLineageFetcher{}
		lineageFetcher.On("GetJobLineage", ctx, mock.Anything, 2).
			Return(map[scheduler.JobName]*scheduler.JobLineageSummary{"job_target": lineage}, nil).Once()

		estimator := newEstimator(t, map[string]time.Duration{
			"job_target":   targetBaseline,
			"job_upstream": 64 * time.Minute,
		})

		repo := &mockIncidentRepository{}
		notifier := &mockRootCauseNotifier{}

		svc := newService(detailsGetter, lineageFetcher, estimator, repo, notifier, &mockProjectTeamGetter{}, nil)

		incidents, err := svc.IdentifySLABreachRootCause(ctx, service.RootCauseRequestConfig{
			ProjectNames:         []tenant.ProjectName{projectName},
			JobNames:             []scheduler.JobName{"job_target"},
			ReferenceTime:        referenceTime,
			EnableAlert:          false,
			ScheduleRangeInHours: 2 * time.Hour,
		})

		assert.NoError(t, err)
		assert.Len(t, incidents, 1)
		repo.AssertNotCalled(t, "UpsertIncident")
		notifier.AssertNotCalled(t, "SendRootCauseBreach")
	})

	t.Run("routes scheduler delay to the configured platform team without per-run identity", func(t *testing.T) {
		// a2 finished 09:15; b1 was scheduled 09:00 but only started 10:40 — a scheduler-
		// attributable wait far above the 2.5m level-1 budget
		a2Run := rcMakeRun("job_a2", base.Add(-2*time.Hour),
			1*time.Hour, rcPtr(base.Add(-2*time.Hour)), rcPtr(base.Add(-45*time.Minute)), "success")
		b1Run := rcMakeRun("job_b1", base.Add(-time.Hour),
			1*time.Hour, rcPtr(base.Add(40*time.Minute)), rcPtr(base.Add(45*time.Minute)), "success")
		targetRun := rcMakeRun("job_target", targetScheduledAt,
			1*time.Hour, rcPtr(base.Add(time.Hour)), nil, "running")
		schedulerLineage := rcMakeLineage("job_target", tnnt, targetRun,
			rcMakeLineage("job_b1", tnnt, b1Run,
				rcMakeLineage("job_a2", tnnt, a2Run)))

		detailsGetter := &mockRootCauseDetailsGetter{}
		detailsGetter.On("GetJobs", ctx, projectName, []string{"job_target"}).Return([]*scheduler.JobWithDetails{jobWithDetails}, nil).Once()

		lineageFetcher := &mockRootCauseLineageFetcher{}
		lineageFetcher.On("GetJobLineage", ctx, mock.Anything, 2).
			Return(map[scheduler.JobName]*scheduler.JobLineageSummary{"job_target": schedulerLineage}, nil).Once()

		estimator := newEstimator(t, map[string]time.Duration{
			"job_target": targetBaseline,
			"job_b1":     5 * time.Minute,
			"job_a2":     75 * time.Minute,
		})

		repo := &mockIncidentRepository{}
		repo.On("UpsertIncident", ctx, mock.Anything, mock.Anything).Return(nil).Once()

		notifier := &mockRootCauseNotifier{}
		notifier.On("SendRootCauseBreach", mock.Anything).Once().Run(func(args mock.Arguments) {
			alert := args.Get(0).(*scheduler.RootCauseBreachAlert)
			assert.Equal(t, "platform-team", alert.Team)
			assert.Equal(t, scheduler.TierSchedulerDelay, alert.Reason.Reason)
			// the per-run identity is dropped so the alert deduplicates globally per reason
			assert.Empty(t, alert.RootCauseJob)
			assert.Nil(t, alert.RootCauseScheduledAt)
		})

		svc := newService(detailsGetter, lineageFetcher, estimator, repo, notifier, &mockProjectTeamGetter{}, &config.RootCauseAlertConfig{
			RuntimeDelayThresholdMinutes:    30,
			SchedulerDelayMinutesPerDepth:   2.5,
			ThirdPartyDelayThresholdSeconds: 600,
			InfraTeam:                       "platform-team",
		})

		incidents, err := svc.IdentifySLABreachRootCause(ctx, service.RootCauseRequestConfig{
			ProjectNames:         []tenant.ProjectName{projectName},
			JobNames:             []scheduler.JobName{"job_target"},
			ReferenceTime:        referenceTime,
			EnableAlert:          true,
			ScheduleRangeInHours: 2 * time.Hour,
		})

		assert.NoError(t, err)
		assert.Len(t, incidents, 1)
		notifier.AssertExpectations(t)
	})

	t.Run("an incident without a routing team is not alerted", func(t *testing.T) {
		detailsGetter := &mockRootCauseDetailsGetter{}
		detailsGetter.On("GetJobs", ctx, projectName, []string{"job_target"}).Return([]*scheduler.JobWithDetails{jobWithDetails}, nil).Once()

		lineageFetcher := &mockRootCauseLineageFetcher{}
		lineageFetcher.On("GetJobLineage", ctx, mock.Anything, 2).
			Return(map[scheduler.JobName]*scheduler.JobLineageSummary{"job_target": lineage}, nil).Once()

		estimator := newEstimator(t, map[string]time.Duration{
			"job_target":   targetBaseline,
			"job_upstream": 64 * time.Minute,
		})

		repo := &mockIncidentRepository{}
		repo.On("UpsertIncident", ctx, mock.Anything, mock.Anything).Return(nil).Once()

		notifier := &mockRootCauseNotifier{}

		teamGetter := &mockProjectTeamGetter{}
		projectWithoutTeam, err := tenant.NewProject("p_root", map[string]string{
			tenant.ProjectStoragePathKey: "gs://bucket",
			tenant.ProjectSchedulerHost:  "airflow.example.com",
		}, nil)
		assert.NoError(t, err)
		teamGetter.On("Get", ctx, projectName).Return(projectWithoutTeam, nil).Once()

		svc := newService(detailsGetter, lineageFetcher, estimator, repo, notifier, teamGetter, nil)

		incidents, err := svc.IdentifySLABreachRootCause(ctx, service.RootCauseRequestConfig{
			ProjectNames:         []tenant.ProjectName{projectName},
			JobNames:             []scheduler.JobName{"job_target"},
			ReferenceTime:        referenceTime,
			EnableAlert:          true,
			ScheduleRangeInHours: 2 * time.Hour,
		})

		assert.NoError(t, err)
		assert.Len(t, incidents, 1)
		notifier.AssertNotCalled(t, "SendRootCauseBreach")
	})

	t.Run("excluded projects are never evaluated", func(t *testing.T) {
		repo := &mockIncidentRepository{}
		notifier := &mockRootCauseNotifier{}

		svc := service.NewJobRootCauseService(
			log.NewNoop(),
			config.RootCauseAlertConfig{ExcludeProjects: []string{"p_root"}},
			&mockRootCauseDetailsGetter{}, &mockRootCauseLineageFetcher{}, &mockRootCauseDurationEstimator{},
			repo, []string{"dex"}, notifier, &mockProjectTeamGetter{},
		)

		incidents, err := svc.IdentifySLABreachRootCause(ctx, service.RootCauseRequestConfig{
			ProjectNames:         []tenant.ProjectName{projectName},
			ReferenceTime:        referenceTime,
			EnableAlert:          true,
			ScheduleRangeInHours: 2 * time.Hour,
		})

		assert.NoError(t, err)
		assert.Empty(t, incidents)
		repo.AssertNotCalled(t, "UpsertIncident")
		notifier.AssertNotCalled(t, "SendRootCauseBreach")
	})
}
