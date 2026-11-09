package service_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
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

func (m *mockIncidentRepository) GetIncidentByKey(ctx context.Context, key scheduler.SLABreachIncidentKey) (*scheduler.SLABreachIncident, error) {
	args := m.Called(ctx, key)
	if incident, ok := args.Get(0).(*scheduler.SLABreachIncident); ok {
		return incident, args.Error(1)
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

// mockRootCauseScheduleChangeGetter is a local rootcause.ScheduledChangeGetter mock.
type mockRootCauseScheduleChangeGetter struct {
	mock.Mock
}

func (m *mockRootCauseScheduleChangeGetter) GetRecentScheduleChange(ctx context.Context, jobName scheduler.JobName, tnnt tenant.Tenant, scheduledAt time.Time) (string, error) {
	args := m.Called(ctx, jobName, tnnt, scheduledAt)
	return args.String(0), args.Error(1)
}

// rcScheduleChangeGetter returns a schedule-change getter that never reports a change.
func rcScheduleChangeGetter() *mockRootCauseScheduleChangeGetter {
	getter := &mockRootCauseScheduleChangeGetter{}
	getter.On("GetRecentScheduleChange", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return("", nil)
	return getter
}

// rcPotentialConfig is the potential-SLA config the engine shares with the potential API.
func rcPotentialConfig() config.PotentialSLABreachConfig {
	return config.PotentialSLABreachConfig{DamperCoeff: 1.0}
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

func rcTestTenant() tenant.Tenant {
	tnnt, err := tenant.NewTenant("p_root", "ns1")
	if err != nil {
		panic(err)
	}
	return tnnt
}

func rcPtr(t time.Time) *time.Time { return &t }

// rcMakeRun builds a JobRunSummary the way the resolver does: SLATime = scheduled_at + SLA.
func rcMakeRun(
	jobName scheduler.JobName,
	scheduledAt time.Time,
	sla time.Duration, //nolint:unparam
	taskStart, taskEnd *time.Time,
	status string,
) *scheduler.JobRunSummary {
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
	tnnt := rcTestTenant()

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

	newEstimator := func(_ *testing.T, durations map[string]time.Duration) *mockRootCauseDurationEstimator {
		estimator := &mockRootCauseDurationEstimator{}
		durationsByJob := map[scheduler.JobName]*time.Duration{}
		for jobName, duration := range durations {
			duration := duration
			durationsByJob[scheduler.JobName(jobName)] = &duration
		}
		// the same estimator doubles as the SLA duration estimator in these fixtures
		estimator.On("GetPercentileDurationByJobNames", ctx, referenceTime, mock.Anything).
			Return(durationsByJob, nil).Times(2)
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
			rcPotentialConfig(),
			detailsGetter,
			lineageFetcher,
			estimator, estimator,
			nil,
			rcScheduleChangeGetter(),
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
		repo.On("GetIncidentByKey", ctx, mock.Anything).Return(nil, scheduler.ErrSLABreachIncidentNotFound).Once()
		repo.On("GetActiveIncidents", ctx, mock.Anything).Return(nil, nil).Maybe()
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

	t.Run("routes scheduler delay to the configured platform team", func(t *testing.T) {
		// a2 finished 09:15; b1 was scheduled 09:00 but only started 10:40 (85m the
		// scheduler owns); the target started before its own schedule, so the summed
		// wait is 85m against a 3-level budget of 7.5m, keyed on the target's run
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
		repo.On("GetIncidentByKey", ctx, mock.Anything).Return(nil, scheduler.ErrSLABreachIncidentNotFound).Once()
		repo.On("GetActiveIncidents", ctx, mock.Anything).Return(nil, nil).Maybe()
		repo.On("UpsertIncident", ctx, mock.Anything, mock.Anything).Return(nil).Once()

		notifier := &mockRootCauseNotifier{}
		notifier.On("SendRootCauseBreach", mock.Anything).Once().Run(func(args mock.Arguments) {
			alert := args.Get(0).(*scheduler.RootCauseBreachAlert)
			assert.Equal(t, "platform-team", alert.Team)
			assert.Equal(t, scheduler.TierSchedulerDelay, alert.Reason.Reason)
			// the incident is keyed on the target's run, so the dedup keys apply normally
			assert.Equal(t, "job_target", alert.RootCauseJob)
			assert.NotNil(t, alert.RootCauseScheduledAt)
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
		repo.On("GetIncidentByKey", ctx, mock.Anything).Return(nil, scheduler.ErrSLABreachIncidentNotFound).Once()
		repo.On("GetActiveIncidents", ctx, mock.Anything).Return(nil, nil).Maybe()
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
		// the resolution pass still runs under the kill switch, so previously tracked
		// incidents keep converging to resolved even while evaluation is excluded
		repo.On("GetActiveIncidents", ctx, mock.Anything).Return(nil, nil).Maybe()
		notifier := &mockRootCauseNotifier{}

		svc := service.NewJobRootCauseService(
			log.NewNoop(),
			config.RootCauseAlertConfig{ExcludeProjects: []string{"p_root"}},
			rcPotentialConfig(),
			&mockRootCauseDetailsGetter{}, &mockRootCauseLineageFetcher{},
			&mockRootCauseDurationEstimator{}, &mockRootCauseDurationEstimator{},
			nil, rcScheduleChangeGetter(),
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

	t.Run("marks reasons that disappeared as resolved when merging with the stored incident", func(t *testing.T) {
		detailsGetter := &mockRootCauseDetailsGetter{}
		detailsGetter.On("GetJobs", ctx, projectName, []string{"job_target"}).Return([]*scheduler.JobWithDetails{jobWithDetails}, nil).Once()

		lineageFetcher := &mockRootCauseLineageFetcher{}
		lineageFetcher.On("GetJobLineage", ctx, mock.Anything, 2).
			Return(map[scheduler.JobName]*scheduler.JobLineageSummary{"job_target": lineage}, nil).Once()

		estimator := newEstimator(t, map[string]time.Duration{
			"job_target":   targetBaseline,
			"job_upstream": 64 * time.Minute,
		})

		// the stored incident still has FAILED_UPSTREAM active from an earlier tick;
		// the fresh tick only re-observes RUNTIME_DELAY
		previous := &scheduler.SLABreachIncident{
			Key: scheduler.SLABreachIncidentKey{
				ProjectName: projectName,
				JobName:     "job_upstream",
				ScheduledAt: base,
			},
			Details: scheduler.RootCauseDetails{Reasons: []scheduler.RootCauseReasonDetail{
				{Reason: scheduler.TierRuntimeDelay, Status: scheduler.ReasonStatusActive, InducedDelaySeconds: 3300},
				{Reason: scheduler.TierFailedUpstream, Status: scheduler.ReasonStatusActive},
			}},
			MaxInducedDelaySeconds: 3300,
		}

		repo := &mockIncidentRepository{}
		repo.On("GetIncidentByKey", ctx, mock.Anything).Return(previous, nil).Once()
		repo.On("GetActiveIncidents", ctx, mock.Anything).Return(nil, nil).Maybe()
		repo.On("UpsertIncident", ctx, mock.Anything, mock.Anything).Return(nil).Once().Run(func(args mock.Arguments) {
			incident := args.Get(1).(*scheduler.SLABreachIncident)
			byReason := map[scheduler.SLABreachTier]scheduler.RootCauseReasonDetail{}
			for _, reason := range incident.Details.Reasons {
				byReason[reason.Reason] = reason
			}
			assert.Len(t, byReason, 2)
			assert.Equal(t, scheduler.ReasonStatusActive, byReason[scheduler.TierRuntimeDelay].Status)
			assert.Equal(t, scheduler.ReasonStatusResolved, byReason[scheduler.TierFailedUpstream].Status)
			assert.Equal(t, 3300, incident.MaxInducedDelaySeconds, "the high-water mark survives resolution")
		})

		// alerts route by ACTIVE reasons only: FAILED_UPSTREAM, now resolved, is silent
		notifier := &mockRootCauseNotifier{}
		notifier.On("SendRootCauseBreach", mock.Anything).Once().Run(func(args mock.Arguments) {
			alert := args.Get(0).(*scheduler.RootCauseBreachAlert)
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
		notifier.AssertExpectations(t)
	})

	t.Run("evaluates targets in bounded lineage batches", func(t *testing.T) {
		// three targets with a batch size of one: each target's lineage is fetched,
		// evaluated and released before the next one is built
		targetNames := []string{"job_a", "job_b", "job_c"}
		// 6-hour cadence; reference 13:30 resolves the 12:00 run as the in-range target,
		// with each upstream finishing a minute before the target starts
		targetAt := base.Add(2 * time.Hour)

		var jobs []*scheduler.JobWithDetails
		lineages := map[string]*scheduler.JobLineageSummary{}
		durations := map[string]time.Duration{}
		for _, name := range targetNames {
			jobs = append(jobs, newJobWithDetails(name))
			upstreamRun := rcMakeRun(scheduler.JobName("job_upstream_"+name), base,
				1*time.Hour, rcPtr(base), rcPtr(base.Add(119*time.Minute)), "success")
			targetRun := rcMakeRun(scheduler.JobName(name), targetAt,
				1*time.Hour, rcPtr(targetAt), nil, "running")
			lineages[name] = rcMakeLineage(scheduler.JobName(name), tnnt, targetRun,
				rcMakeLineage(scheduler.JobName("job_upstream_"+name), tnnt, upstreamRun))
			durations[name] = targetBaseline
			durations["job_upstream_"+name] = 64 * time.Minute
		}

		detailsGetter := &mockRootCauseDetailsGetter{}
		detailsGetter.On("GetJobs", ctx, projectName, []string{"job_a", "job_b", "job_c"}).
			Return(jobs, nil).Once()

		lineageFetcher := &mockRootCauseLineageFetcher{}
		for _, name := range targetNames {
			name := name
			lineageFetcher.On("GetJobLineage", ctx,
				mock.MatchedBy(func(schedules map[scheduler.JobName]*scheduler.JobSchedule) bool {
					return len(schedules) == 1 && schedules[scheduler.JobName(name)] != nil
				}), 2).
				Return(map[scheduler.JobName]*scheduler.JobLineageSummary{scheduler.JobName(name): lineages[name]}, nil).Once()
		}

		estimator := &mockRootCauseDurationEstimator{}
		durationsByJob := map[scheduler.JobName]*time.Duration{}
		for jobName, duration := range durations {
			duration := duration
			durationsByJob[scheduler.JobName(jobName)] = &duration
		}
		estimator.On("GetPercentileDurationByJobNames", ctx, referenceTime, mock.Anything).
			Return(durationsByJob, nil).Times(6)

		repo := &mockIncidentRepository{}
		repo.On("GetIncidentByKey", ctx, mock.Anything).Return(nil, scheduler.ErrSLABreachIncidentNotFound).Times(3)
		repo.On("GetActiveIncidents", ctx, mock.Anything).Return(nil, nil).Maybe()
		repo.On("UpsertIncident", ctx, mock.Anything, mock.Anything).Return(nil).Times(3)

		notifier := &mockRootCauseNotifier{}
		notifier.On("SendRootCauseBreach", mock.Anything).Times(3).Run(func(args mock.Arguments) {
			alert := args.Get(0).(*scheduler.RootCauseBreachAlert)
			assert.Contains(t, []string{"job_upstream_job_a", "job_upstream_job_b", "job_upstream_job_c"}, alert.RootCauseJob)
		})

		teamGetter := &mockProjectTeamGetter{}
		projectWithTeam, err := tenant.NewProject("p_root", map[string]string{
			tenant.ProjectStoragePathKey:   "gs://bucket",
			tenant.ProjectSchedulerHost:    "airflow.example.com",
			tenant.ProjectAlertManagerTeam: "proj-team",
		}, nil)
		assert.NoError(t, err)
		// the team cache resolves the project once regardless of the batch count
		teamGetter.On("Get", ctx, projectName).Return(projectWithTeam, nil).Maybe()

		svc := newService(detailsGetter, lineageFetcher, estimator, repo, notifier, teamGetter, &config.RootCauseAlertConfig{
			RuntimeDelayThresholdMinutes:    30,
			SchedulerDelayMinutesPerDepth:   2.5,
			ThirdPartyDelayThresholdSeconds: 600,
			LineageBatchSize:                1,
		})

		incidents, err := svc.IdentifySLABreachRootCause(ctx, service.RootCauseRequestConfig{
			ProjectNames:         []tenant.ProjectName{projectName},
			JobNames:             []scheduler.JobName{"job_a", "job_b", "job_c"},
			ReferenceTime:        referenceTime,
			EnableAlert:          true,
			ScheduleRangeInHours: 2 * time.Hour,
		})

		assert.NoError(t, err)
		assert.Len(t, incidents, 3)
		lineageFetcher.AssertNumberOfCalls(t, "GetJobLineage", 3)
		repo.AssertExpectations(t)
		notifier.AssertExpectations(t)
	})
}

func TestJobRootCauseService_AlertSuppression(t *testing.T) {
	ctx := context.Background()
	projectName := tenant.ProjectName("p_root")
	tnnt := rcTestTenant()

	base := time.Date(2026, 10, 26, 10, 0, 0, 0, time.UTC)
	referenceTime := base.Add(3*time.Hour + 30*time.Minute)
	targetScheduledAt := base.Add(2 * time.Hour)

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

	newDeps := func(t *testing.T, lineage *scheduler.JobLineageSummary, durations map[string]time.Duration) (*mockRootCauseDetailsGetter, *mockRootCauseLineageFetcher, *mockRootCauseDurationEstimator) {
		t.Helper()

		detailsGetter := &mockRootCauseDetailsGetter{}
		detailsGetter.On("GetJobs", ctx, projectName, []string{"job_target"}).Return([]*scheduler.JobWithDetails{jobWithDetails}, nil).Once()

		lineageFetcher := &mockRootCauseLineageFetcher{}
		lineageFetcher.On("GetJobLineage", ctx, mock.Anything, 2).
			Return(map[scheduler.JobName]*scheduler.JobLineageSummary{"job_target": lineage}, nil).Once()

		estimator := &mockRootCauseDurationEstimator{}
		durationsByJob := map[scheduler.JobName]*time.Duration{}
		for jobName, duration := range durations {
			duration := duration
			durationsByJob[scheduler.JobName(jobName)] = &duration
		}
		estimator.On("GetPercentileDurationByJobNames", ctx, referenceTime, mock.Anything).
			Return(durationsByJob, nil).Times(2)
		return detailsGetter, lineageFetcher, estimator
	}

	evaluate := func(t *testing.T, alertConfig *config.RootCauseAlertConfig, deps func(t *testing.T, lineage *scheduler.JobLineageSummary, durations map[string]time.Duration) (*mockRootCauseDetailsGetter, *mockRootCauseLineageFetcher, *mockRootCauseDurationEstimator), lineage *scheduler.JobLineageSummary, durations map[string]time.Duration, notifier *mockRootCauseNotifier, teamGetter *mockProjectTeamGetter) ([]*scheduler.SLABreachIncidentWithImpacts, *mockRootCauseNotifier) {
		t.Helper()

		detailsGetter, lineageFetcher, estimator := deps(t, lineage, durations)
		repo := &mockIncidentRepository{}
		repo.On("GetIncidentByKey", ctx, mock.Anything).Return(nil, scheduler.ErrSLABreachIncidentNotFound).Once()
		repo.On("GetActiveIncidents", ctx, mock.Anything).Return(nil, nil).Maybe()
		repo.On("UpsertIncident", ctx, mock.Anything, mock.Anything).Return(nil).Once()
		if notifier == nil {
			notifier = &mockRootCauseNotifier{}
		}
		if teamGetter == nil {
			teamGetter = &mockProjectTeamGetter{}
		}

		svc := service.NewJobRootCauseService(
			log.NewNoop(),
			*alertConfig,
			rcPotentialConfig(),
			detailsGetter,
			lineageFetcher,
			estimator, estimator,
			nil,
			rcScheduleChangeGetter(),
			repo,
			[]string{"dex"},
			notifier,
			teamGetter,
		)

		incidents, err := svc.IdentifySLABreachRootCause(ctx, service.RootCauseRequestConfig{
			ProjectNames:         []tenant.ProjectName{projectName},
			JobNames:             []scheduler.JobName{"job_target"},
			ReferenceTime:        referenceTime,
			EnableAlert:          true,
			ScheduleRangeInHours: 2 * time.Hour,
		})
		assert.NoError(t, err)
		repo.AssertExpectations(t)
		return incidents, notifier
	}

	t.Run("skip_reasons mutes the alert while the incident is still recorded", func(t *testing.T) {
		// the runtime-delay scenario: induced 55m on the upstream
		upstreamRun := rcMakeRun("job_upstream", base,
			1*time.Hour, rcPtr(base), rcPtr(base.Add(119*time.Minute)), "success")
		targetRun := rcMakeRun("job_target", targetScheduledAt,
			1*time.Hour, rcPtr(targetScheduledAt), nil, "running")
		lineage := rcMakeLineage("job_target", tnnt, targetRun,
			rcMakeLineage("job_upstream", tnnt, upstreamRun))

		deps := newDeps
		alertConfig := &config.RootCauseAlertConfig{
			RuntimeDelayThresholdMinutes:    30,
			SchedulerDelayMinutesPerDepth:   2.5,
			ThirdPartyDelayThresholdSeconds: 600,
			AlertSuppression: config.AlertSuppressionConfig{
				SkipReasons: []string{"runtime_delay"}, // case-insensitive
			},
		}

		incidents, notifier := evaluate(t, alertConfig, deps, lineage, map[string]time.Duration{
			"job_target":   150 * time.Minute,
			"job_upstream": 64 * time.Minute,
		}, nil, nil)

		assert.Len(t, incidents, 1, "incident is still produced and recorded")
		notifier.AssertNotCalled(t, "SendRootCauseBreach")
	})

	t.Run("min induced delay mutes a below-floor alert but keeps an above-floor one", func(t *testing.T) {
		// scheduler-delay scenario with an 85m scheduler wait
		a2Run := rcMakeRun("job_a2", base.Add(-2*time.Hour),
			1*time.Hour, rcPtr(base.Add(-2*time.Hour)), rcPtr(base.Add(-45*time.Minute)), "success")
		b1Run := rcMakeRun("job_b1", base.Add(-time.Hour),
			1*time.Hour, rcPtr(base.Add(40*time.Minute)), rcPtr(base.Add(45*time.Minute)), "success")
		targetRun := rcMakeRun("job_target", targetScheduledAt,
			1*time.Hour, rcPtr(base.Add(time.Hour)), nil, "running")
		lineage := rcMakeLineage("job_target", tnnt, targetRun,
			rcMakeLineage("job_b1", tnnt, b1Run,
				rcMakeLineage("job_a2", tnnt, a2Run)))

		deps := newDeps
		alertConfig := &config.RootCauseAlertConfig{
			RuntimeDelayThresholdMinutes:    30,
			SchedulerDelayMinutesPerDepth:   2.5,
			ThirdPartyDelayThresholdSeconds: 600,
			InfraTeam:                       "platform-team",
			AlertSuppression: config.AlertSuppressionConfig{
				MinTotalInducedDelayMinutes: 100, // 85m < 100m → muted
			},
		}

		incidents, notifier := evaluate(t, alertConfig, deps, lineage, map[string]time.Duration{
			"job_target": 150 * time.Minute,
			"job_b1":     5 * time.Minute,
			"job_a2":     75 * time.Minute,
		}, nil, nil)

		assert.Len(t, incidents, 1, "incident is still produced and recorded")
		notifier.AssertNotCalled(t, "SendRootCauseBreach")

		// same lineage, floor lowered below the 85m delay → the alert fires
		alertConfig.AlertSuppression.MinTotalInducedDelayMinutes = 60
		notifier = &mockRootCauseNotifier{}
		notifier.On("SendRootCauseBreach", mock.Anything).Once().Run(func(args mock.Arguments) {
			alert := args.Get(0).(*scheduler.RootCauseBreachAlert)
			assert.Equal(t, "platform-team", alert.Team)
			assert.Equal(t, scheduler.TierSchedulerDelay, alert.Reason.Reason)
		})

		incidents, _ = evaluate(t, alertConfig, deps, lineage, map[string]time.Duration{
			"job_target": 150 * time.Minute,
			"job_b1":     5 * time.Minute,
			"job_a2":     75 * time.Minute,
		}, notifier, nil)

		assert.Len(t, incidents, 1)
		notifier.AssertExpectations(t)
	})

	t.Run("reasons without a measured delay ignore the induced-delay floor", func(t *testing.T) {
		// FAILED_UPSTREAM carries no measured delay: a 15m floor must not mute it
		a2Run := rcMakeRun("job_a2", base.Add(-2*time.Hour),
			1*time.Hour, rcPtr(base.Add(-2*time.Hour)), rcPtr(base.Add(-110*time.Minute)), "failed")
		b1Run := rcMakeRun("job_b1", base.Add(-time.Hour),
			1*time.Hour, rcPtr(base.Add(-90*time.Minute)), rcPtr(base.Add(-80*time.Minute)), "success")
		targetRun := rcMakeRun("job_target", targetScheduledAt,
			1*time.Hour, rcPtr(base.Add(time.Hour)), nil, "running")
		lineage := rcMakeLineage("job_target", tnnt, targetRun,
			rcMakeLineage("job_b1", tnnt, b1Run,
				rcMakeLineage("job_a2", tnnt, a2Run)))

		deps := newDeps
		alertConfig := &config.RootCauseAlertConfig{
			RuntimeDelayThresholdMinutes:    30,
			SchedulerDelayMinutesPerDepth:   2.5,
			ThirdPartyDelayThresholdSeconds: 600,
			AlertSuppression: config.AlertSuppressionConfig{
				MinTotalInducedDelayMinutes: 15,
			},
		}

		projectWithTeam, err := tenant.NewProject("p_root", map[string]string{
			tenant.ProjectStoragePathKey:   "gs://bucket",
			tenant.ProjectSchedulerHost:    "airflow.example.com",
			tenant.ProjectAlertManagerTeam: "proj-team",
		}, nil)
		assert.NoError(t, err)
		projectTeam := &mockProjectTeamGetter{}
		projectTeam.On("Get", ctx, projectName).Return(projectWithTeam, nil).Once()

		notifier := &mockRootCauseNotifier{}
		notifier.On("SendRootCauseBreach", mock.Anything).Once().Run(func(args mock.Arguments) {
			alert := args.Get(0).(*scheduler.RootCauseBreachAlert)
			assert.Equal(t, scheduler.TierFailedUpstream, alert.Reason.Reason)
		})

		incidents, _ := evaluate(t, alertConfig, deps, lineage, map[string]time.Duration{
			"job_target": 150 * time.Minute,
			"job_b1":     10 * time.Minute,
		}, notifier, projectTeam)

		assert.Len(t, incidents, 1)
		notifier.AssertExpectations(t)
	})
}

// TestJobRootCauseService_AggressiveSLAAlertSuppression: an unexplained actual breach is
// recorded as AGGRESSIVE_SLA and alerts the impacted owner unless alert_suppression
// skip_reasons mutes it like any other reason.
func TestJobRootCauseService_AggressiveSLAAlertSuppression(t *testing.T) {
	ctx := context.Background()
	projectName := tenant.ProjectName("p_root")
	tnnt := rcTestTenant()

	base := time.Date(2026, 10, 26, 10, 0, 0, 0, time.UTC)
	referenceTime := base.Add(3*time.Hour + 30*time.Minute) // 30m past the target's 13:00 SLA
	targetScheduledAt := base.Add(2 * time.Hour)

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
	// started on time and running within its baseline: nothing explains the breach
	targetRun := rcMakeRun("job_target", targetScheduledAt,
		1*time.Hour, rcPtr(targetScheduledAt), nil, "running")
	lineage := rcMakeLineage("job_target", tnnt, targetRun)

	runScenario := func(skipReasons []string) ([]*scheduler.SLABreachIncidentWithImpacts, *mockRootCauseNotifier) {
		detailsGetter := &mockRootCauseDetailsGetter{}
		detailsGetter.On("GetJobs", ctx, projectName, []string{"job_target"}).Return([]*scheduler.JobWithDetails{jobWithDetails}, nil).Once()
		lineageFetcher := &mockRootCauseLineageFetcher{}
		lineageFetcher.On("GetJobLineage", ctx, mock.Anything, 2).
			Return(map[scheduler.JobName]*scheduler.JobLineageSummary{"job_target": lineage}, nil).Once()

		estimator := &mockRootCauseDurationEstimator{}
		durPtr := func(d time.Duration) *time.Duration { return &d }
		durations := map[scheduler.JobName]*time.Duration{"job_target": durPtr(150 * time.Minute)}
		estimator.On("GetPercentileDurationByJobNames", ctx, referenceTime, mock.Anything).Return(durations, nil).Times(2)

		repo := &mockIncidentRepository{}
		repo.On("GetIncidentByKey", ctx, mock.Anything).Return(nil, scheduler.ErrSLABreachIncidentNotFound).Once()
		repo.On("GetActiveIncidents", ctx, mock.Anything).Return(nil, nil).Maybe()
		repo.On("UpsertIncident", ctx, mock.Anything, mock.Anything).Return(nil).Once().
			Run(func(args mock.Arguments) {
				incident := args.Get(1).(*scheduler.SLABreachIncident)
				assert.Equal(t, "job_target", incident.Key.JobName.String())
				assert.Equal(t, scheduler.TierAggressiveSLA, incident.Details.Reasons[0].Reason)
			})

		notifier := &mockRootCauseNotifier{}
		if len(skipReasons) == 0 {
			notifier.On("SendRootCauseBreach", mock.Anything).Once().Run(func(args mock.Arguments) {
				alert := args.Get(0).(*scheduler.RootCauseBreachAlert)
				assert.Equal(t, scheduler.TierAggressiveSLA, alert.Reason.Reason)
			})
		}
		teamGetter := &mockProjectTeamGetter{}
		projectWithTeam, err := tenant.NewProject("p_root", map[string]string{
			tenant.ProjectStoragePathKey:   "gs://bucket",
			tenant.ProjectSchedulerHost:    "airflow.example.com",
			tenant.ProjectAlertManagerTeam: "proj-team",
		}, nil)
		assert.NoError(t, err)
		teamGetter.On("Get", ctx, projectName).Return(projectWithTeam, nil).Maybe()

		svc := service.NewJobRootCauseService(
			log.NewNoop(),
			config.RootCauseAlertConfig{
				RuntimeDelayThresholdMinutes:    30,
				SchedulerDelayMinutesPerDepth:   2.5,
				ThirdPartyDelayThresholdSeconds: 600,
				AlertSuppression: config.AlertSuppressionConfig{
					SkipReasons: skipReasons,
				},
			},
			rcPotentialConfig(),
			detailsGetter, lineageFetcher,
			estimator, estimator, nil, rcScheduleChangeGetter(),
			repo, []string{"dex"}, notifier, teamGetter,
		)

		incidents, err := svc.IdentifySLABreachRootCause(ctx, service.RootCauseRequestConfig{
			ProjectNames:         []tenant.ProjectName{projectName},
			JobNames:             []scheduler.JobName{"job_target"},
			ReferenceTime:        referenceTime,
			EnableAlert:          true,
			ScheduleRangeInHours: 2 * time.Hour,
		})
		assert.NoError(t, err)
		repo.AssertExpectations(t)
		return incidents, notifier
	}

	t.Run("without skip_reasons: recorded and alerted", func(t *testing.T) {
		incidents, notifier := runScenario(nil)
		assert.Len(t, incidents, 1)
		notifier.AssertExpectations(t)
	})

	t.Run("muted via skip_reasons: recorded without alerting", func(t *testing.T) {
		incidents, notifier := runScenario([]string{"aggressive_sla"}) // case-insensitive
		assert.Len(t, incidents, 1)
		notifier.AssertNotCalled(t, "SendRootCauseBreach")
	})
}

func TestJobRootCauseService_ResolutionPass(t *testing.T) {
	ctx := context.Background()
	projectName := tenant.ProjectName("p_root")
	tnnt := rcTestTenant()

	base := time.Date(2026, 10, 26, 10, 0, 0, 0, time.UTC)
	referenceTime := base.Add(3 * time.Hour)
	targetScheduledAt := base.Add(2 * time.Hour)

	// a healthy target: finished within its SLA, so the fresh tick detects nothing
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
	healthyRun := rcMakeRun("job_target", targetScheduledAt,
		1*time.Hour, rcPtr(targetScheduledAt), rcPtr(targetScheduledAt.Add(5*time.Minute)), "success")
	healthyLineage := rcMakeLineage("job_target", tnnt, healthyRun)

	t.Run("incidents that received no fresh detection are fully resolved", func(t *testing.T) {
		detailsGetter := &mockRootCauseDetailsGetter{}
		detailsGetter.On("GetJobs", ctx, projectName, []string{"job_target"}).Return([]*scheduler.JobWithDetails{jobWithDetails}, nil).Once()

		lineageFetcher := &mockRootCauseLineageFetcher{}
		lineageFetcher.On("GetJobLineage", ctx, mock.Anything, 2).
			Return(map[scheduler.JobName]*scheduler.JobLineageSummary{"job_target": healthyLineage}, nil).Once()

		estimator := &mockRootCauseDurationEstimator{}
		estimator.On("GetPercentileDurationByJobNames", ctx, referenceTime, mock.Anything).
			Return(map[scheduler.JobName]*time.Duration{}, nil).Times(2)

		// an incident that was active before this tick, with a stale blast radius
		offenderKey := scheduler.SLABreachIncidentKey{
			ProjectName: tenant.ProjectName("p_offender"),
			JobName:     "job_gone",
			ScheduledAt: base,
		}
		tracked := &scheduler.SLABreachIncidentWithImpacts{
			Incident: &scheduler.SLABreachIncident{
				ID:  uuid.New(),
				Key: offenderKey,
				Details: scheduler.RootCauseDetails{Reasons: []scheduler.RootCauseReasonDetail{
					{Reason: scheduler.TierRuntimeDelay, Status: scheduler.ReasonStatusActive, InducedDelaySeconds: 3600},
				}},
				MaxInducedDelaySeconds: 3600,
			},
			Impacts: []scheduler.ImpactedJob{{ProjectName: projectName, JobName: "job_target", ScheduledAt: targetScheduledAt}},
		}

		repo := &mockIncidentRepository{}
		repo.On("GetActiveIncidents", ctx, mock.Anything).Return([]*scheduler.SLABreachIncidentWithImpacts{tracked}, nil).Once()
		repo.On("UpsertIncident", ctx, mock.Anything, mock.Anything).Return(nil).Once().Run(func(args mock.Arguments) {
			incident := args.Get(1).(*scheduler.SLABreachIncident)
			assert.Equal(t, offenderKey, incident.Key)
			assert.Len(t, incident.Details.Reasons, 1)
			assert.Equal(t, scheduler.ReasonStatusResolved, incident.Details.Reasons[0].Status,
				"the reason that vanished from every lineage is resolved")
			assert.Equal(t, 3600, incident.MaxInducedDelaySeconds, "the high-water mark survives")
			impacts := args.Get(2).([]scheduler.ImpactedJob)
			assert.Empty(t, impacts, "the stale blast radius is cleared")
		})

		notifier := &mockRootCauseNotifier{}

		svc := service.NewJobRootCauseService(
			log.NewNoop(),
			config.RootCauseAlertConfig{
				RuntimeDelayThresholdMinutes:    30,
				SchedulerDelayMinutesPerDepth:   2.5,
				ThirdPartyDelayThresholdSeconds: 600,
			},
			rcPotentialConfig(),
			detailsGetter,
			lineageFetcher,
			estimator, estimator,
			nil,
			rcScheduleChangeGetter(),
			repo,
			[]string{"dex"},
			notifier,
			&mockProjectTeamGetter{},
		)

		incidents, err := svc.IdentifySLABreachRootCause(ctx, service.RootCauseRequestConfig{
			ProjectNames:         []tenant.ProjectName{projectName},
			JobNames:             []scheduler.JobName{"job_target"},
			ReferenceTime:        referenceTime,
			EnableAlert:          true,
			ScheduleRangeInHours: 2 * time.Hour,
		})

		assert.NoError(t, err)
		assert.Empty(t, incidents, "the healthy target produces no fresh incident")
		repo.AssertExpectations(t)
		notifier.AssertNotCalled(t, "SendRootCauseBreach")
	})
}

func TestJobRootCauseService_FinishedCauseNotifiesOnce(t *testing.T) {
	rcDurPtr := func(d time.Duration) *time.Duration { return &d }

	ctx := context.Background()
	projectName := tenant.ProjectName("p_root")
	tnnt := rcTestTenant()

	// the runtime-delay chain where the offending run has FINISHED: its reason carries
	// IsJobFinished=true, so the router must notify exactly once across ticks
	base := time.Date(2026, 10, 26, 10, 0, 0, 0, time.UTC)
	// 13:30 — 30 minutes past the target's 13:00 SLA deadline, so the breach gate fires
	referenceTime := base.Add(3*time.Hour + 30*time.Minute)
	targetScheduledAt := base.Add(2 * time.Hour)

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

	upstreamRun := rcMakeRun("job_upstream", base,
		1*time.Hour, rcPtr(base), rcPtr(base.Add(119*time.Minute)), "success")
	targetRun := rcMakeRun("job_target", targetScheduledAt,
		1*time.Hour, rcPtr(targetScheduledAt), nil, "running")
	lineage := rcMakeLineage("job_target", tnnt, targetRun,
		rcMakeLineage("job_upstream", tnnt, upstreamRun))

	detailsGetter := &mockRootCauseDetailsGetter{}
	detailsGetter.On("GetJobs", ctx, projectName, []string{"job_target"}).Return([]*scheduler.JobWithDetails{jobWithDetails}, nil)
	lineageFetcher := &mockRootCauseLineageFetcher{}
	lineageFetcher.On("GetJobLineage", ctx, mock.Anything, 2).
		Return(map[scheduler.JobName]*scheduler.JobLineageSummary{"job_target": lineage}, nil)
	estimator := &mockRootCauseDurationEstimator{}
	estimator.On("GetPercentileDurationByJobNames", ctx, referenceTime, mock.Anything).
		Return(map[scheduler.JobName]*time.Duration{
			"job_target":   rcDurPtr(150 * time.Minute),
			"job_upstream": rcDurPtr(64 * time.Minute),
		}, nil).Times(4)

	newRepo := func(previous *scheduler.SLABreachIncident) *mockIncidentRepository {
		repo := &mockIncidentRepository{}
		repo.On("GetIncidentByKey", ctx, mock.Anything).Return(previous, nil).Once()
		repo.On("GetActiveIncidents", ctx, mock.Anything).Return(nil, nil).Maybe()
		repo.On("UpsertIncident", ctx, mock.Anything, mock.Anything).Return(nil).Once()
		return repo
	}

	teamGetter := &mockProjectTeamGetter{}
	projectWithTeam, err := tenant.NewProject("p_root", map[string]string{
		tenant.ProjectStoragePathKey:   "gs://bucket",
		tenant.ProjectSchedulerHost:    "airflow.example.com",
		tenant.ProjectAlertManagerTeam: "proj-team",
	}, nil)
	assert.NoError(t, err)
	teamGetter.On("Get", ctx, projectName).Return(projectWithTeam, nil)

	// tick 1: the stored reason was last seen while the run was still going
	storedRunning := &scheduler.SLABreachIncident{
		Key: scheduler.SLABreachIncidentKey{
			ProjectName: projectName,
			JobName:     "job_upstream",
			ScheduledAt: base,
		},
		Details: scheduler.RootCauseDetails{Reasons: []scheduler.RootCauseReasonDetail{
			{Reason: scheduler.TierRuntimeDelay, Status: scheduler.ReasonStatusActive, InducedDelaySeconds: 3300, IsJobFinished: false},
		}},
	}
	// tick 2: the stored reason already carried the finished flag
	storedFinished := &scheduler.SLABreachIncident{
		Key: storedRunning.Key,
		Details: scheduler.RootCauseDetails{Reasons: []scheduler.RootCauseReasonDetail{
			{Reason: scheduler.TierRuntimeDelay, Status: scheduler.ReasonStatusActive, InducedDelaySeconds: 3300, IsJobFinished: true},
		}},
	}

	alertConfig := &config.RootCauseAlertConfig{
		RuntimeDelayThresholdMinutes:    30,
		SchedulerDelayMinutesPerDepth:   2.5,
		ThirdPartyDelayThresholdSeconds: 600,
	}

	t.Run("the running-to-finished transition notifies once", func(t *testing.T) {
		notifier := &mockRootCauseNotifier{}
		notifier.On("SendRootCauseBreach", mock.Anything).Once().Run(func(args mock.Arguments) {
			alert := args.Get(0).(*scheduler.RootCauseBreachAlert)
			assert.Equal(t, scheduler.TierRuntimeDelay, alert.Reason.Reason)
		})

		svc := service.NewJobRootCauseService(log.NewNoop(), *alertConfig, rcPotentialConfig(), detailsGetter, lineageFetcher,
			estimator, estimator, nil, rcScheduleChangeGetter(),
			newRepo(storedRunning), []string{"dex"}, notifier, teamGetter)

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

	t.Run("later ticks with the cause still finished stay silent", func(t *testing.T) {
		notifier := &mockRootCauseNotifier{}

		svc := service.NewJobRootCauseService(log.NewNoop(), *alertConfig, rcPotentialConfig(), detailsGetter, lineageFetcher,
			estimator, estimator, nil, rcScheduleChangeGetter(),
			newRepo(storedFinished), []string{"dex"}, notifier, teamGetter)

		incidents, err := svc.IdentifySLABreachRootCause(ctx, service.RootCauseRequestConfig{
			ProjectNames:         []tenant.ProjectName{projectName},
			JobNames:             []scheduler.JobName{"job_target"},
			ReferenceTime:        referenceTime,
			EnableAlert:          true,
			ScheduleRangeInHours: 2 * time.Hour,
		})

		assert.NoError(t, err)
		assert.Len(t, incidents, 1, "the incident is still detected and recorded")
		notifier.AssertNotCalled(t, "SendRootCauseBreach")
	})
}
