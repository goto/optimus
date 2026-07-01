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
		svc := service.NewJobSLAPredictorService(l, conf, nil, jobLineageFetcher, durationEstimator, jobDetailsGetter, nil, nil, scheduledChangeGetter)

		projectName := tenant.ProjectName("project-a")
		referenceTime := time.Now().UTC()
		reqConfig := service.JobSLAPredictorRequestConfig{
			ReferenceTime:        referenceTime,
			ScheduleRangeInHours: 10 * time.Hour,
			EnableAlert:          false,
			DamperCoeff:          conf.DamperCoeff,
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
		jobLineageFetcher.On("GetJobLineage", ctx, map[scheduler.JobName]*scheduler.JobSchedule{"job-A": jobASchedule}).
			Return(map[scheduler.JobName]*scheduler.JobLineageSummary{"job-A": jobALineage}, nil).Once()
		durationEstimator.On("GetPercentileDurationByJobNames", ctx, referenceTime, mock.MatchedBy(func(jobNames []scheduler.JobName) bool {
			return assert.ElementsMatch(t, []scheduler.JobName{"job-A", "job-B", "job-C"}, jobNames)
		})).Return(map[scheduler.JobName]*time.Duration{
			"job-A": durationPtr(20 * time.Minute),
			"job-B": durationPtr(15 * time.Minute),
			"job-C": durationPtr(10 * time.Minute),
		}, nil).Once()

		combos := []service.SLABreachCombo{
			{ProjectName: projectName, JobNames: []scheduler.JobName{"job-A"}, GroupName: "sla-8am", Severity: "CRITICAL"},
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
		svc := service.NewJobSLAPredictorService(l, conf, nil, jobLineageFetcher, durationEstimator, jobDetailsGetter, nil, nil, scheduledChangeGetter)

		projectName := tenant.ProjectName("project-a")
		labels := map[string]string{"criticality": "critical"}
		reqConfig := service.JobSLAPredictorRequestConfig{
			ReferenceTime:        time.Now().UTC(),
			ScheduleRangeInHours: 10 * time.Hour,
			EnableAlert:          false,
			DamperCoeff:          conf.DamperCoeff,
		}

		jobDetailsGetter.On("GetJobsByLabels", ctx, projectName, labels).Return(nil, assert.AnError).Once()

		combos := []service.SLABreachCombo{{ProjectName: projectName, Labels: labels}}

		res, err := svc.IdentifySLABreachesBatch(ctx, combos, reqConfig)

		assert.Error(t, err)
		assert.Len(t, res, 0)
	})
}

func durationPtr(d time.Duration) *time.Duration { return &d }
