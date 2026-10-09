package service_test

import (
	"context"
	"fmt"
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

func TestIdentifySLABreachesBatch(t *testing.T) {
	ctx := context.Background()
	l := log.NewNoop()
	conf := config.PotentialSLABreachConfig{
		DamperCoeff:             1.0,
		EnablePersistentLogging: false,
	}

	scheduledChangeGetter := NewScheduledChangeGetter(t)
	scheduledChangeGetter.On("GetRecentScheduleChange", ctx, mock.Anything, mock.Anything, mock.Anything).Return("", nil).Maybe()

	t.Run("returns project-qualified breaches for a combo", func(t *testing.T) {
		// job-C -> job-B -> job-A (target). job-C is running late so job-A may breach.
		jobLineageFetcher := NewJobLineageFetcher(t)
		durationEstimator := NewDurationEstimator(t)
		jobDetailsGetter := NewJobDetailsGetter(t)
		svc := service.NewJobSLAPredictorService(l, conf, config.RootCauseConfig{}, nil, jobLineageFetcher, durationEstimator, jobDetailsGetter, nil, nil, scheduledChangeGetter, nil, nil)

		projectName := tenant.ProjectName("project-a")
		referenceTime := time.Now().UTC()
		reqConfig := service.JobSLAPredictorRequestConfig{
			ReferenceTime:        referenceTime,
			ScheduleRangeInHours: 10 * time.Hour,
			EnableAlert:          false,
			DamperFactor: scheduler.DamperFactor{
				Alpha: conf.DamperCoeff,
			},
		}

		tnnt, _ := tenant.NewTenant("project-a", "team-a")
		startDate := referenceTime.Add(-24 * time.Hour).Truncate(time.Hour)
		scheduledAt := referenceTime.Add(1 * time.Minute).Truncate(time.Minute)
		interval := fmt.Sprintf("%d %d * * *", scheduledAt.Minute(), scheduledAt.Hour())
		jobA := &scheduler.JobWithDetails{
			Name: "job-A",
			Job:  &scheduler.Job{Tenant: tnnt, Name: "job-A"},
			Schedule: &scheduler.Schedule{
				StartDate: startDate,
				Interval:  interval,
			},
			Alerts: []scheduler.Alert{
				{On: scheduler.EventCategorySLAMiss, Config: map[string]string{"duration": "30m"}},
			},
		}

		jobASchedule := &scheduler.JobSchedule{JobName: "job-A", ScheduledAt: scheduledAt}
		jobALineage := &scheduler.JobLineageSummary{
			JobName:          "job-A",
			ScheduleInterval: interval,
			IsEnabled:        true,
			JobRuns:          map[scheduler.JobName]*scheduler.JobRunSummary{"job-A": {ScheduledAt: scheduledAt}},
		}
		jobBLineage := &scheduler.JobLineageSummary{
			JobName:          "job-B",
			ScheduleInterval: interval,
			IsEnabled:        true,
			JobRuns:          map[scheduler.JobName]*scheduler.JobRunSummary{"job-A": {ScheduledAt: scheduledAt.Add(-15 * time.Minute)}},
		}
		jobCTaskStartTime := scheduledAt.Add(-20 * time.Minute)
		jobCLineage := &scheduler.JobLineageSummary{
			JobName:          "job-C",
			ScheduleInterval: interval,
			IsEnabled:        true,
			JobRuns: map[scheduler.JobName]*scheduler.JobRunSummary{
				"job-A": {ScheduledAt: scheduledAt.Add(-25 * time.Minute), TaskStartTime: &jobCTaskStartTime},
			},
		}
		jobALineage.Upstreams = []*scheduler.JobLineageSummary{jobBLineage}
		jobBLineage.Upstreams = []*scheduler.JobLineageSummary{jobCLineage}

		jobDetailsGetter.On("GetJobs", ctx, projectName, []string{"job-A"}).Return([]*scheduler.JobWithDetails{jobA}, nil).Once()
		jobLineageFetcher.On("GetJobLineage", ctx, map[scheduler.JobName]*scheduler.JobSchedule{"job-A": jobASchedule}, int(reqConfig.ScheduleRangeInHours.Hours())).
			Return(map[scheduler.JobName]*scheduler.JobLineageSummary{"job-A": jobALineage}, nil).Once()
		durationEstimator.On("GetPercentileDurationByJobNames", ctx, referenceTime, mock.MatchedBy(func(jobNames []scheduler.JobName) bool {
			return assert.ElementsMatch(t, []scheduler.JobName{"job-A", "job-B", "job-C"}, jobNames)
		})).Return(map[scheduler.JobName]*time.Duration{
			"job-A": durationPtr(20 * time.Minute),
			"job-B": durationPtr(15 * time.Minute),
			"job-C": durationPtr(10 * time.Minute),
		}, nil).Once()

		combos := []scheduler.SLABreachCombo{
			{ProjectName: projectName, JobNames: []scheduler.JobName{"job-A"}, GroupName: "sla-8am"},
		}

		// when
		res, err := svc.IdentifySLABreachesBatch(ctx, combos, reqConfig)

		// then
		assert.NoError(t, err)
		assert.Len(t, res, 1)
		tb, ok := res["project-a/job-A"]
		assert.True(t, ok)
		assert.Equal(t, "project-a", tb.TargetProject)
		assert.Equal(t, scheduler.JobName("job-A"), tb.TargetJobName)
		assert.Contains(t, tb.Upstreams, scheduler.JobName("job-C"))
	})

	t.Run("returns error only when every combo fails", func(t *testing.T) {
		jobLineageFetcher := NewJobLineageFetcher(t)
		durationEstimator := NewDurationEstimator(t)
		jobDetailsGetter := NewJobDetailsGetter(t)
		svc := service.NewJobSLAPredictorService(l, conf, config.RootCauseConfig{}, nil, jobLineageFetcher, durationEstimator, jobDetailsGetter, nil, nil, scheduledChangeGetter, nil, nil)

		projectName := tenant.ProjectName("project-a")
		labels := map[string]string{"criticality": "critical"}
		reqConfig := service.JobSLAPredictorRequestConfig{
			ReferenceTime:        time.Now().UTC(),
			ScheduleRangeInHours: 10 * time.Hour,
			EnableAlert:          false,
			DamperFactor: scheduler.DamperFactor{
				Alpha: conf.DamperCoeff,
			},
		}

		jobDetailsGetter.On("GetJobsByLabels", ctx, projectName, labels).Return(nil, assert.AnError).Once()

		combos := []scheduler.SLABreachCombo{{ProjectName: projectName, Labels: labels}}

		res, err := svc.IdentifySLABreachesBatch(ctx, combos, reqConfig)

		assert.Error(t, err)
		assert.Len(t, res, 0)
	})
}

func TestIdentifySLABreachesBatchImpactedJobDedup(t *testing.T) {
	ctx := context.Background()
	l := log.NewNoop()
	projectName := tenant.ProjectName("project-a")
	tnnt, _ := tenant.NewTenant("project-a", "team-a")
	projectEntity, _ := tenant.NewProject("project-a", map[string]string{
		"STORAGE_PATH":                 "file:///tmp/",
		"SCHEDULER_HOST":               "http://localhost",
		tenant.ProjectAlertManagerTeam: "dwh-team",
	}, map[string]string{})
	namespaceEntity, _ := tenant.NewNamespace("team-a", projectName, map[string]string{}, map[string]string{})
	tenantWithDetails, _ := tenant.NewTenantDetails(projectEntity, namespaceEntity, tenant.PlainTextSecrets{})

	scheduledChangeGetter := NewScheduledChangeGetter(t)
	scheduledChangeGetter.On("GetRecentScheduleChange", ctx, mock.Anything, mock.Anything, mock.Anything).Return("", nil).Maybe()

	// job-C -> job-B -> job-A (target). job-C is running late so job-A may breach.
	run := func(t *testing.T, conf config.PotentialSLABreachConfig, repo *mockSLAPredictorRepository, notifier *mockPotentialSLANotifier) time.Time {
		t.Helper()
		jobLineageFetcher := NewJobLineageFetcher(t)
		durationEstimator := NewDurationEstimator(t)
		jobDetailsGetter := NewJobDetailsGetter(t)
		tenantGetter := new(TenantGetter)
		tenantGetter.On("GetDetails", ctx, mock.Anything).Return(tenantWithDetails, nil).Maybe()
		svc := service.NewJobSLAPredictorService(l, conf, config.RootCauseConfig{}, repo, jobLineageFetcher, durationEstimator, jobDetailsGetter, notifier, tenantGetter, scheduledChangeGetter, nil, nil)

		referenceTime := time.Now().UTC()
		reqConfig := service.JobSLAPredictorRequestConfig{
			ReferenceTime:        referenceTime,
			ScheduleRangeInHours: 10 * time.Hour,
			EnableAlert:          true,
			DamperFactor:         scheduler.DamperFactor{Alpha: 1.0},
		}
		startDate := referenceTime.Add(-24 * time.Hour).Truncate(time.Hour)
		scheduledAt := referenceTime.Add(1 * time.Minute).Truncate(time.Minute)
		interval := fmt.Sprintf("%d %d * * *", scheduledAt.Minute(), scheduledAt.Hour())
		jobA := &scheduler.JobWithDetails{
			Name:     "job-A",
			Job:      &scheduler.Job{Tenant: tnnt, Name: "job-A"},
			Schedule: &scheduler.Schedule{StartDate: startDate, Interval: interval},
			Alerts: []scheduler.Alert{
				{On: scheduler.EventCategorySLAMiss, Config: map[string]string{"duration": "30m"}},
			},
		}
		jobASchedule := &scheduler.JobSchedule{JobName: "job-A", ScheduledAt: scheduledAt}
		jobALineage := &scheduler.JobLineageSummary{
			JobName: "job-A", Tenant: tnnt, ScheduleInterval: interval, IsEnabled: true,
			JobRuns: map[scheduler.JobName]*scheduler.JobRunSummary{"job-A": {ScheduledAt: scheduledAt}},
		}
		jobBLineage := &scheduler.JobLineageSummary{
			JobName: "job-B", Tenant: tnnt, ScheduleInterval: interval, IsEnabled: true,
			JobRuns: map[scheduler.JobName]*scheduler.JobRunSummary{"job-A": {ScheduledAt: scheduledAt.Add(-15 * time.Minute)}},
		}
		jobCTaskStartTime := scheduledAt.Add(-20 * time.Minute)
		jobCLineage := &scheduler.JobLineageSummary{
			JobName: "job-C", Tenant: tnnt, ScheduleInterval: interval, IsEnabled: true,
			JobRuns: map[scheduler.JobName]*scheduler.JobRunSummary{
				"job-A": {ScheduledAt: scheduledAt.Add(-25 * time.Minute), TaskStartTime: &jobCTaskStartTime},
			},
		}
		jobALineage.Upstreams = []*scheduler.JobLineageSummary{jobBLineage}
		jobBLineage.Upstreams = []*scheduler.JobLineageSummary{jobCLineage}

		jobDetailsGetter.On("GetJobs", ctx, projectName, []string{"job-A"}).Return([]*scheduler.JobWithDetails{jobA}, nil).Once()
		jobLineageFetcher.On("GetJobLineage", ctx, map[scheduler.JobName]*scheduler.JobSchedule{"job-A": jobASchedule}, int(reqConfig.ScheduleRangeInHours.Hours())).
			Return(map[scheduler.JobName]*scheduler.JobLineageSummary{"job-A": jobALineage}, nil).Once()
		durationEstimator.On("GetPercentileDurationByJobNames", ctx, referenceTime, mock.Anything).Return(map[scheduler.JobName]*time.Duration{
			"job-A": durationPtr(20 * time.Minute),
			"job-B": durationPtr(15 * time.Minute),
			"job-C": durationPtr(10 * time.Minute),
		}, nil).Once()

		combos := []scheduler.SLABreachCombo{{ProjectName: projectName, JobNames: []scheduler.JobName{"job-A"}}}
		res, err := svc.IdentifySLABreachesBatch(ctx, combos, reqConfig)
		assert.NoError(t, err)
		assert.Contains(t, res, "project-a/job-A")
		return scheduledAt
	}
	dedupConf := config.PotentialSLABreachConfig{DamperCoeff: 1.0, EnablePersistentLogging: true, DedupImpactedJobs: true}

	t.Run("suppresses an impacted job whose run was already predicted", func(t *testing.T) {
		repo := new(mockSLAPredictorRepository)
		repo.On("GetPredictedTargetJobNames", ctx, mock.Anything).Return([]scheduler.JobName{"job-A"}, nil).Once()
		repo.On("StorePredictedSLABreach", ctx, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		notifier := new(mockPotentialSLANotifier)

		scheduledAt := run(t, dedupConf, repo, notifier)

		repo.AssertExpectations(t)
		repo.AssertCalled(t, "GetPredictedTargetJobNames", ctx, []*scheduler.JobSchedule{{JobName: "job-A", ScheduledAt: scheduledAt}})
		notifier.AssertNotCalled(t, "SendPotentialSLABreach", mock.Anything)
	})

	t.Run("alerts an impacted job that was not predicted before", func(t *testing.T) {
		repo := new(mockSLAPredictorRepository)
		repo.On("GetPredictedTargetJobNames", ctx, mock.Anything).Return(nil, nil).Once()
		repo.On("StorePredictedSLABreach", ctx, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		notifier := new(mockPotentialSLANotifier)
		notifier.On("SendPotentialSLABreach", mock.MatchedBy(func(alert *scheduler.PotentialSLABreachAlert) bool {
			return alert.Team == "dwh-team" && assert.ElementsMatch(t, []string{"job-A"}, alert.ImpactedJobs)
		})).Once()

		run(t, dedupConf, repo, notifier)

		repo.AssertExpectations(t)
		notifier.AssertExpectations(t)
	})

	t.Run("still alerts when the dedup lookup fails", func(t *testing.T) {
		repo := new(mockSLAPredictorRepository)
		repo.On("GetPredictedTargetJobNames", ctx, mock.Anything).Return(nil, assert.AnError).Once()
		repo.On("StorePredictedSLABreach", ctx, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		notifier := new(mockPotentialSLANotifier)
		notifier.On("SendPotentialSLABreach", mock.Anything).Once()

		run(t, dedupConf, repo, notifier)

		notifier.AssertExpectations(t)
	})

	t.Run("does not look up predictions when dedup is disabled", func(t *testing.T) {
		repo := new(mockSLAPredictorRepository)
		repo.On("StorePredictedSLABreach", ctx, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		notifier := new(mockPotentialSLANotifier)
		notifier.On("SendPotentialSLABreach", mock.Anything).Once()

		run(t, config.PotentialSLABreachConfig{DamperCoeff: 1.0, EnablePersistentLogging: true}, repo, notifier)

		repo.AssertNotCalled(t, "GetPredictedTargetJobNames", mock.Anything, mock.Anything)
		notifier.AssertExpectations(t)
	})
}

type mockSLAPredictorRepository struct {
	mock.Mock
}

func (m *mockSLAPredictorRepository) StorePredictedSLABreach(ctx context.Context, jobTargetName, jobCauseName scheduler.JobName, targetedSLA, jobScheduledAt time.Time, cause string, referenceTime time.Time, config map[string]interface{}, lineages []interface{}) error {
	return m.Called(ctx, jobTargetName, jobCauseName, targetedSLA, jobScheduledAt, cause, referenceTime, config, lineages).Error(0)
}

func (m *mockSLAPredictorRepository) GetPredictedTargetJobNames(ctx context.Context, targets []*scheduler.JobSchedule) ([]scheduler.JobName, error) {
	args := m.Called(ctx, targets)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]scheduler.JobName), args.Error(1)
}

type mockPotentialSLANotifier struct {
	mock.Mock
}

func (m *mockPotentialSLANotifier) SendPotentialSLABreach(alert *scheduler.PotentialSLABreachAlert) {
	m.Called(alert)
}

func durationPtr(d time.Duration) *time.Duration { return &d }
