package service_test

import (
	"context"
	"errors"
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

func dur(d time.Duration) *time.Duration { return &d }

func TestIdentifySLABreaches(t *testing.T) {
	ctx := context.Background()
	l := log.NewNoop()
	conf := config.PotentialSLABreachConfig{
		DamperCoeff:             1.0,
		EnablePersistentLogging: false,
	}

	scheduledChangeGetter := NewScheduledChangeGetter(t)
	scheduledChangeGetter.On("GetRecentScheduleChange", ctx, mock.Anything, mock.Anything, mock.Anything).Return("", nil).Maybe()

	t.Run("given no jobs, return no breaches", func(t *testing.T) {
		// given
		jobLineageFetcher := NewJobLineageFetcher(t)
		durationEstimator := NewDurationEstimator(t)
		jobDetailsGetter := NewJobDetailsGetter(t)
		jobSLAPredictorService := service.NewJobSLAPredictorService(l, conf, config.RootCauseConfig{}, nil, jobLineageFetcher, durationEstimator, jobDetailsGetter, nil, nil, scheduledChangeGetter, nil, nil)

		projectName := tenant.ProjectName("project-a")
		nextScheduledRangeInHours := 10 * time.Hour
		referenceTime := time.Now().UTC()
		jobNames := []scheduler.JobName{}
		labels := map[string]string{}

		reqConfig := service.JobSLAPredictorRequestConfig{
			ReferenceTime:        referenceTime,
			ScheduleRangeInHours: nextScheduledRangeInHours,
			EnableAlert:          false,
			DamperFactor: scheduler.DamperFactor{
				Alpha: conf.DamperCoeff,
			},
			Severity: "",
		}
		// when
		jobBreachRootCause, err := jobSLAPredictorService.IdentifySLABreaches(ctx, projectName, jobNames, labels, reqConfig)
		// then
		assert.NoError(t, err)
		assert.Len(t, jobBreachRootCause, 0)
	})

	t.Run("given no jobs found, return no breaches", func(t *testing.T) {
		// given
		jobLineageFetcher := NewJobLineageFetcher(t)
		durationEstimator := NewDurationEstimator(t)
		jobDetailsGetter := NewJobDetailsGetter(t)
		jobSLAPredictorService := service.NewJobSLAPredictorService(l, conf, config.RootCauseConfig{}, nil, jobLineageFetcher, durationEstimator, jobDetailsGetter, nil, nil, scheduledChangeGetter, nil, nil)

		projectName := tenant.ProjectName("project-a")
		nextScheduledRangeInHours := 10 * time.Hour
		referenceTime := time.Now().UTC()
		jobNames := []scheduler.JobName{scheduler.JobName("job-A")}
		labels := map[string]string{}

		reqConfig := service.JobSLAPredictorRequestConfig{
			ReferenceTime:        referenceTime,
			ScheduleRangeInHours: nextScheduledRangeInHours,
			EnableAlert:          false,
			DamperFactor: scheduler.DamperFactor{
				Alpha: conf.DamperCoeff,
			},
			Severity: "",
		}

		jobDetailsGetter.On("GetJobs", ctx, projectName, []string{"job-A"}).Return([]*scheduler.JobWithDetails{}, nil).Once()

		// when
		jobBreachRootCause, err := jobSLAPredictorService.IdentifySLABreaches(ctx, projectName, jobNames, labels, reqConfig)

		// then
		assert.NoError(t, err)
		assert.Len(t, jobBreachRootCause, 0)
	})

	t.Run("given no jobs found given labels, return no breaches", func(t *testing.T) {
		// given
		jobLineageFetcher := NewJobLineageFetcher(t)
		durationEstimator := NewDurationEstimator(t)
		jobDetailsGetter := NewJobDetailsGetter(t)
		jobSLAPredictorService := service.NewJobSLAPredictorService(l, conf, config.RootCauseConfig{}, nil, jobLineageFetcher, durationEstimator, jobDetailsGetter, nil, nil, scheduledChangeGetter, nil, nil)

		projectName := tenant.ProjectName("project-a")
		nextScheduledRangeInHours := 10 * time.Hour
		referenceTime := time.Now().UTC()
		jobNames := []scheduler.JobName{}
		labels := map[string]string{"criticality": "critical"}

		reqConfig := service.JobSLAPredictorRequestConfig{
			ReferenceTime:        referenceTime,
			ScheduleRangeInHours: nextScheduledRangeInHours,
			EnableAlert:          false,
			DamperFactor: scheduler.DamperFactor{
				Alpha: conf.DamperCoeff,
			},
			Severity: "",
		}

		jobDetailsGetter.On("GetJobsByLabels", ctx, projectName, labels).Return([]*scheduler.JobWithDetails{}, nil).Once()

		// when
		jobBreachRootCause, err := jobSLAPredictorService.IdentifySLABreaches(ctx, projectName, jobNames, labels, reqConfig)

		// then
		assert.NoError(t, err)
		assert.Len(t, jobBreachRootCause, 0)
	})

	t.Run("given error fetching jobs, return error", func(t *testing.T) {
		// given
		jobLineageFetcher := NewJobLineageFetcher(t)
		durationEstimator := NewDurationEstimator(t)
		jobDetailsGetter := NewJobDetailsGetter(t)
		jobSLAPredictorService := service.NewJobSLAPredictorService(l, conf, config.RootCauseConfig{}, nil, jobLineageFetcher, durationEstimator, jobDetailsGetter, nil, nil, scheduledChangeGetter, nil, nil)

		projectName := tenant.ProjectName("project-a")
		nextScheduledRangeInHours := 10 * time.Hour
		referenceTime := time.Now().UTC()
		jobNames := []scheduler.JobName{scheduler.JobName("job-A")}
		labels := map[string]string{}

		reqConfig := service.JobSLAPredictorRequestConfig{
			ReferenceTime:        referenceTime,
			ScheduleRangeInHours: nextScheduledRangeInHours,
			EnableAlert:          false,
			DamperFactor: scheduler.DamperFactor{
				Alpha: conf.DamperCoeff,
			},
			Severity: "",
		}

		jobDetailsGetter.On("GetJobs", ctx, projectName, []string{"job-A"}).Return(nil, assert.AnError).Once()

		// when
		jobBreachRootCause, err := jobSLAPredictorService.IdentifySLABreaches(ctx, projectName, jobNames, labels, reqConfig)

		// then
		assert.Error(t, err)
		assert.Len(t, jobBreachRootCause, 0)
	})

	t.Run("given jobs without schedule, return no breaches", func(t *testing.T) {
		// given
		jobLineageFetcher := NewJobLineageFetcher(t)
		durationEstimator := NewDurationEstimator(t)
		jobDetailsGetter := NewJobDetailsGetter(t)
		jobSLAPredictorService := service.NewJobSLAPredictorService(l, conf, config.RootCauseConfig{}, nil, jobLineageFetcher, durationEstimator, jobDetailsGetter, nil, nil, scheduledChangeGetter, nil, nil)

		projectName := tenant.ProjectName("project-a")
		nextScheduledRangeInHours := 10 * time.Hour
		referenceTime := time.Now().UTC()
		jobNames := []scheduler.JobName{scheduler.JobName("job-A")}
		labels := map[string]string{}

		reqConfig := service.JobSLAPredictorRequestConfig{
			ReferenceTime:        referenceTime,
			ScheduleRangeInHours: nextScheduledRangeInHours,
			EnableAlert:          false,
			DamperFactor: scheduler.DamperFactor{
				Alpha: conf.DamperCoeff,
			},
			Severity: "",
		}

		tenant, _ := tenant.NewTenant("project-a", "team-a")
		jobA := &scheduler.JobWithDetails{
			Name: "job-A",
			Job: &scheduler.Job{
				Tenant: tenant,
				Name:   "job-A",
			},
		}

		jobDetailsGetter.On("GetJobs", ctx, projectName, []string{"job-A"}).Return([]*scheduler.JobWithDetails{jobA}, nil).Once()

		// when
		jobBreachRootCause, err := jobSLAPredictorService.IdentifySLABreaches(ctx, projectName, jobNames, labels, reqConfig)

		// then
		assert.NoError(t, err)
		assert.Len(t, jobBreachRootCause, 0)
	})

	t.Run("given jobs without SLA, return no breaches", func(t *testing.T) {
		// given
		jobLineageFetcher := NewJobLineageFetcher(t)
		durationEstimator := NewDurationEstimator(t)
		jobDetailsGetter := NewJobDetailsGetter(t)
		jobSLAPredictorService := service.NewJobSLAPredictorService(l, conf, config.RootCauseConfig{}, nil, jobLineageFetcher, durationEstimator, jobDetailsGetter, nil, nil, scheduledChangeGetter, nil, nil)

		projectName := tenant.ProjectName("project-a")
		nextScheduledRangeInHours := 10 * time.Hour
		referenceTime := time.Now().UTC()
		jobNames := []scheduler.JobName{scheduler.JobName("job-A")}
		labels := map[string]string{}

		reqConfig := service.JobSLAPredictorRequestConfig{
			ReferenceTime:        referenceTime,
			ScheduleRangeInHours: nextScheduledRangeInHours,
			EnableAlert:          false,
			DamperFactor: scheduler.DamperFactor{
				Alpha: conf.DamperCoeff,
			},
			Severity: "",
		}

		tenant, _ := tenant.NewTenant("project-a", "team-a")
		startDate := time.Now().Add(-24 * time.Hour).Truncate(time.Hour)
		jobA := &scheduler.JobWithDetails{
			Name: "job-A",
			Job: &scheduler.Job{
				Tenant: tenant,
				Name:   "job-A",
			},
			Schedule: &scheduler.Schedule{
				StartDate: startDate,
			},
		}

		jobDetailsGetter.On("GetJobs", ctx, projectName, []string{"job-A"}).Return([]*scheduler.JobWithDetails{jobA}, nil).Once()

		// when
		jobBreachRootCause, err := jobSLAPredictorService.IdentifySLABreaches(ctx, projectName, jobNames, labels, reqConfig)

		// then
		assert.NoError(t, err)
		assert.Len(t, jobBreachRootCause, 0)
	})

	t.Run("given jobs with SLA but not in range of next schedule range, return no breaches", func(t *testing.T) {
		// given
		jobLineageFetcher := NewJobLineageFetcher(t)
		durationEstimator := NewDurationEstimator(t)
		jobDetailsGetter := NewJobDetailsGetter(t)
		jobSLAPredictorService := service.NewJobSLAPredictorService(l, conf, config.RootCauseConfig{}, nil, jobLineageFetcher, durationEstimator, jobDetailsGetter, nil, nil, scheduledChangeGetter, nil, nil)

		projectName := tenant.ProjectName("project-a")
		nextScheduledRangeInHours := 10 * time.Hour
		referenceTime := time.Now().UTC()
		jobNames := []scheduler.JobName{scheduler.JobName("job-A")}
		labels := map[string]string{}

		reqConfig := service.JobSLAPredictorRequestConfig{
			ReferenceTime:        referenceTime,
			ScheduleRangeInHours: nextScheduledRangeInHours,
			EnableAlert:          false,
			DamperFactor: scheduler.DamperFactor{
				Alpha: conf.DamperCoeff,
			},
			Severity: "",
		}

		tenant, _ := tenant.NewTenant("project-a", "team-a")
		startDate := referenceTime.Add(-24 * time.Hour).Truncate(time.Hour)
		// get hour from now + nextScheduledRangeInHours + 1 hours to make sure it's outside of next schedule range
		scheduledAt := referenceTime.Add(nextScheduledRangeInHours + 1*time.Hour).Truncate(time.Hour)
		interval := fmt.Sprintf("0 %d * * *", scheduledAt.Hour()) // daily
		jobA := &scheduler.JobWithDetails{
			Name: "job-A",
			Job: &scheduler.Job{
				Tenant: tenant,
				Name:   "job-A",
			},
			Schedule: &scheduler.Schedule{
				StartDate: startDate,
				Interval:  interval,
			},
			Alerts: []scheduler.Alert{
				{
					On:     scheduler.EventCategorySLAMiss,
					Config: map[string]string{"duration": "30m"},
				},
			},
		}

		jobDetailsGetter.On("GetJobs", ctx, projectName, []string{"job-A"}).Return([]*scheduler.JobWithDetails{jobA}, nil).Once()

		// when
		jobBreachRootCause, err := jobSLAPredictorService.IdentifySLABreaches(ctx, projectName, jobNames, labels, reqConfig)

		// then
		assert.NoError(t, err)
		assert.Len(t, jobBreachRootCause, 0)
	})

	t.Run("given error fetching job lineage, return error", func(t *testing.T) {
		// given
		jobLineageFetcher := NewJobLineageFetcher(t)
		durationEstimator := NewDurationEstimator(t)
		jobDetailsGetter := NewJobDetailsGetter(t)
		jobSLAPredictorService := service.NewJobSLAPredictorService(l, conf, config.RootCauseConfig{}, nil, jobLineageFetcher, durationEstimator, jobDetailsGetter, nil, nil, scheduledChangeGetter, nil, nil)

		projectName := tenant.ProjectName("project-a")
		nextScheduledRangeInHours := 10 * time.Hour
		referenceTime := time.Now().UTC()
		jobNames := []scheduler.JobName{scheduler.JobName("job-A")}
		labels := map[string]string{}

		reqConfig := service.JobSLAPredictorRequestConfig{
			ReferenceTime:        referenceTime,
			ScheduleRangeInHours: nextScheduledRangeInHours,
			EnableAlert:          false,
			Severity:             "",
			DamperFactor: scheduler.DamperFactor{
				Alpha: conf.DamperCoeff,
			},
		}

		tenant, _ := tenant.NewTenant("project-a", "team-a")
		startDate := referenceTime.Add(-24 * time.Hour).Truncate(time.Hour)
		// get hour from now + nextScheduledRangeInHours - 1 hours to make sure it's within next schedule range
		scheduledAt := referenceTime.Add(nextScheduledRangeInHours - 1*time.Hour).Truncate(time.Hour)
		interval := fmt.Sprintf("0 %d * * *", scheduledAt.Hour()) // daily

		jobA := &scheduler.JobWithDetails{
			Name: "job-A",
			Job: &scheduler.Job{
				Tenant: tenant,
				Name:   "job-A",
			},
			Schedule: &scheduler.Schedule{
				StartDate: startDate,
				Interval:  interval,
			},
			Alerts: []scheduler.Alert{
				{
					On:     scheduler.EventCategorySLAMiss,
					Config: map[string]string{"duration": "30m"},
				},
			},
		}

		jobDetailsGetter.On("GetJobs", ctx, projectName, []string{"job-A"}).Return([]*scheduler.JobWithDetails{jobA}, nil).Once()
		jobLineageFetcher.On("GetJobLineage", ctx, mock.Anything, mock.Anything).Return(nil, assert.AnError).Once()

		// when
		jobBreachRootCause, err := jobSLAPredictorService.IdentifySLABreaches(ctx, projectName, jobNames, labels, reqConfig)

		// then
		assert.Error(t, err)
		assert.Len(t, jobBreachRootCause, 0)
	})

	t.Run("given error get P95 duration, return error", func(t *testing.T) {
		// given
		jobLineageFetcher := NewJobLineageFetcher(t)
		durationEstimator := NewDurationEstimator(t)
		jobDetailsGetter := NewJobDetailsGetter(t)
		jobSLAPredictorService := service.NewJobSLAPredictorService(l, conf, config.RootCauseConfig{}, nil, jobLineageFetcher, durationEstimator, jobDetailsGetter, nil, nil, scheduledChangeGetter, nil, nil)

		projectName := tenant.ProjectName("project-a")
		nextScheduledRangeInHours := 10 * time.Hour
		referenceTime := time.Now().UTC()
		jobNames := []scheduler.JobName{scheduler.JobName("job-A")}
		labels := map[string]string{}

		reqConfig := service.JobSLAPredictorRequestConfig{
			ReferenceTime:        referenceTime,
			ScheduleRangeInHours: nextScheduledRangeInHours,
			EnableAlert:          false,
			DamperFactor: scheduler.DamperFactor{
				Alpha: conf.DamperCoeff,
			},
			Severity: "",
		}

		tenant, _ := tenant.NewTenant("project-a", "team-a")
		startDate := referenceTime.Add(-24 * time.Hour).Truncate(time.Hour)
		// get hour from now + nextScheduledRangeInHours - 1 hours to make sure it's within next schedule range
		scheduledAt := referenceTime.Add(nextScheduledRangeInHours - 1*time.Hour).Truncate(time.Hour)
		interval := fmt.Sprintf("0 %d * * *", scheduledAt.Hour()) // daily
		jobA := &scheduler.JobWithDetails{
			Name: "job-A",
			Job: &scheduler.Job{
				Tenant: tenant,
				Name:   "job-A",
			},
			Schedule: &scheduler.Schedule{
				StartDate: startDate,
				Interval:  interval,
			},
			Alerts: []scheduler.Alert{
				{
					On:     scheduler.EventCategorySLAMiss,
					Config: map[string]string{"duration": "30m"},
				},
			},
		}

		jobAScheduledAt, _ := jobA.Schedule.GetScheduleStartTime()
		jobASchedule := &scheduler.JobSchedule{
			JobName:     "job-A",
			ScheduledAt: jobAScheduledAt,
		}
		jobALineage := &scheduler.JobLineageSummary{
			JobName:          "job-A",
			ScheduleInterval: interval,
		}

		jobDetailsGetter.On("GetJobs", ctx, projectName, []string{"job-A"}).Return([]*scheduler.JobWithDetails{jobA}, nil).Once()
		jobLineageFetcher.On("GetJobLineage", ctx, mock.MatchedBy(func(jobSchedules map[scheduler.JobName]*scheduler.JobSchedule) bool {
			return len(jobSchedules) == 1 && jobSchedules[jobASchedule.JobName].ScheduledAt.Equal(jobASchedule.ScheduledAt)
		}), int(reqConfig.ScheduleRangeInHours.Hours())).Return(map[scheduler.JobName]*scheduler.JobLineageSummary{
			jobASchedule.JobName: jobALineage,
		}, nil)
		durationEstimator.On("GetPercentileDurationByJobNames", ctx, referenceTime, []scheduler.JobName{jobA.Name}).Return(nil, assert.AnError).Once()

		// when
		jobBreachRootCause, err := jobSLAPredictorService.IdentifySLABreaches(ctx, projectName, jobNames, labels, reqConfig)
		// then
		assert.Error(t, err)
		assert.Len(t, jobBreachRootCause, 0)
	})

	t.Run("given 1 job that is on time, return no breaches", func(t *testing.T) {
		// job-C -> job-B -> job-A
		// is the target job
		// all jobs are on time, so no breaches

		// | job | estimated duration |
		// |-----|--------------------|
		// | A   | 20 mins            | -> targeted SLA 30 mins from now
		// | B   | 15 mins            | inferredSLA: now+10 mins, must start: now-5 mins
		// | C   | 10 mins            | inferredSLA: now-5 mins, must start: now-15 mins

		// given
		jobLineageFetcher := NewJobLineageFetcher(t)
		durationEstimator := NewDurationEstimator(t)
		jobDetailsGetter := NewJobDetailsGetter(t)
		jobSLAPredictorService := service.NewJobSLAPredictorService(l, conf, config.RootCauseConfig{}, nil, jobLineageFetcher, durationEstimator, jobDetailsGetter, nil, nil, scheduledChangeGetter, nil, nil)

		projectName := tenant.ProjectName("project-a")
		nextScheduledRangeInHours := 10 * time.Hour
		referenceTime := time.Now().UTC()
		jobNames := []scheduler.JobName{scheduler.JobName("job-A")}
		labels := map[string]string{}

		reqConfig := service.JobSLAPredictorRequestConfig{
			ReferenceTime:        referenceTime,
			ScheduleRangeInHours: nextScheduledRangeInHours,
			EnableAlert:          false,
			DamperFactor: scheduler.DamperFactor{
				Alpha: conf.DamperCoeff,
			},
			Severity: "",
		}

		tenant, _ := tenant.NewTenant("project-a", "team-a")
		startDate := referenceTime.Add(-24 * time.Hour).Truncate(time.Hour)
		// get hour from now for simulation purpose, we set scheduledAt to be now
		scheduledAt := referenceTime.Add(1 * time.Minute).Truncate(time.Minute)
		interval := fmt.Sprintf("%d %d * * *", scheduledAt.Minute(), scheduledAt.Hour()) // daily
		jobA := &scheduler.JobWithDetails{
			Name: "job-A",
			Job: &scheduler.Job{
				Tenant: tenant,
				Name:   "job-A",
			},
			Schedule: &scheduler.Schedule{
				StartDate: startDate,
				Interval:  interval,
			},
			Alerts: []scheduler.Alert{
				{
					On:     scheduler.EventCategorySLAMiss,
					Config: map[string]string{"duration": "30m"},
				},
			},
		}

		jobASchedule := &scheduler.JobSchedule{
			JobName:     "job-A",
			ScheduledAt: scheduledAt,
		}
		jobALineage := &scheduler.JobLineageSummary{
			JobName:          "job-A",
			ScheduleInterval: interval,
		}
		jobALineage.RecordOwnRun(&scheduler.JobRunSummary{
			ScheduledAt: scheduledAt,
		})
		jobBTaskStartTime := scheduledAt.Add(-10 * time.Minute)
		jobBScheduledAt := scheduledAt.Add(-15 * time.Minute)
		jobBLineage := &scheduler.JobLineageSummary{
			JobName:          "job-B",
			ScheduleInterval: interval,
		}
		jobBLineage.RecordRun(scheduler.JobRunIdentifier{JobName: jobALineage.JobName, ScheduledAt: scheduledAt}, &scheduler.JobRunSummary{
			ScheduledAt:   jobBScheduledAt,
			TaskStartTime: &jobBTaskStartTime,
		})
		jobCTaskStartTime := scheduledAt.Add(-20 * time.Minute)
		jobCTaskEndTime := scheduledAt.Add(-10 * time.Minute)
		jobCLineage := &scheduler.JobLineageSummary{
			JobName:          "job-C",
			ScheduleInterval: interval,
		}
		jobCLineage.RecordRun(scheduler.JobRunIdentifier{JobName: jobBLineage.JobName, ScheduledAt: jobBScheduledAt}, &scheduler.JobRunSummary{
			ScheduledAt:   scheduledAt.Add(-25 * time.Minute),
			TaskStartTime: &jobCTaskStartTime,
			TaskEndTime:   &jobCTaskEndTime,
			JobEndTime:    &jobCTaskEndTime,
		})
		jobALineage.AddUpstream(jobBLineage)
		jobBLineage.AddUpstream(jobCLineage)

		jobDetailsGetter.On("GetJobs", ctx, projectName, []string{"job-A"}).Return([]*scheduler.JobWithDetails{jobA}, nil).Once()
		jobLineageFetcher.On("GetJobLineage", ctx, map[scheduler.JobName]*scheduler.JobSchedule{
			jobASchedule.JobName: jobASchedule,
		}, int(reqConfig.ScheduleRangeInHours.Hours())).Return(map[scheduler.JobName]*scheduler.JobLineageSummary{
			jobASchedule.JobName: jobALineage,
		}, nil).Once()
		durationEstimator.On("GetPercentileDurationByJobNames", ctx, referenceTime, mock.MatchedBy(func(jobNames []scheduler.JobName) bool {
			return assert.ElementsMatch(t, []scheduler.JobName{"job-A", "job-B", "job-C"}, jobNames)
		})).Return(map[scheduler.JobName]*time.Duration{
			"job-A": func() *time.Duration { d := 20 * time.Minute; return &d }(),
			"job-B": func() *time.Duration { d := 15 * time.Minute; return &d }(),
			"job-C": func() *time.Duration { d := 10 * time.Minute; return &d }(),
		}, nil).Once()

		// when
		jobBreachRootCause, err := jobSLAPredictorService.IdentifySLABreaches(ctx, projectName, jobNames, labels, reqConfig)

		// then
		assert.NoError(t, err)
		assert.Len(t, jobBreachRootCause, 0)
	})

	t.Run("given 1 job that potentially breach due to upstream late, return job causes", func(t *testing.T) {
		// job-C -> job-B -> job-A
		// is the target job
		// job-C is running late, so job-A might breach its SLA

		// | job | estimated duration |
		// |-----|--------------------|
		// | A   | 20 mins            | -> targeted SLA 30 mins from now
		// | B   | 15 mins            | inferredSLA: now+10 mins, must start: now-5 mins
		// | C   | 10 mins            | inferredSLA: now-5 mins, must start: now-15 mins

		// given
		jobLineageFetcher := NewJobLineageFetcher(t)
		durationEstimator := NewDurationEstimator(t)
		jobDetailsGetter := NewJobDetailsGetter(t)
		jobSLAPredictorService := service.NewJobSLAPredictorService(l, conf, config.RootCauseConfig{}, nil, jobLineageFetcher, durationEstimator, jobDetailsGetter, nil, nil, scheduledChangeGetter, nil, nil)

		projectName := tenant.ProjectName("project-a")
		nextScheduledRangeInHours := 10 * time.Hour
		referenceTime := time.Now().UTC()
		jobNames := []scheduler.JobName{scheduler.JobName("job-A")}
		labels := map[string]string{}

		reqConfig := service.JobSLAPredictorRequestConfig{
			ReferenceTime:        referenceTime,
			ScheduleRangeInHours: nextScheduledRangeInHours,
			EnableAlert:          false,
			DamperFactor: scheduler.DamperFactor{
				Alpha: conf.DamperCoeff,
			},
			Severity: "",
		}

		tenant, _ := tenant.NewTenant("project-a", "team-a")
		startDate := referenceTime.Add(-24 * time.Hour).Truncate(time.Hour)
		// get hour from now for simulation purpose, we set scheduledAt to be now
		scheduledAt := referenceTime.Add(1 * time.Minute).Truncate(time.Minute)
		interval := fmt.Sprintf("%d %d * * *", scheduledAt.Minute(), scheduledAt.Hour()) // daily
		jobA := &scheduler.JobWithDetails{
			Name: "job-A",
			Job: &scheduler.Job{
				Tenant: tenant,
				Name:   "job-A",
			},
			Schedule: &scheduler.Schedule{
				StartDate: startDate,
				Interval:  interval,
			},
			Alerts: []scheduler.Alert{
				{
					On:     scheduler.EventCategorySLAMiss,
					Config: map[string]string{"duration": "30m"},
				},
			},
		}

		jobASchedule := &scheduler.JobSchedule{
			JobName:     "job-A",
			ScheduledAt: scheduledAt,
		}
		jobALineage := &scheduler.JobLineageSummary{
			JobName:          "job-A",
			ScheduleInterval: interval,
			IsEnabled:        true,
		}
		jobALineage.RecordOwnRun(&scheduler.JobRunSummary{
			ScheduledAt: scheduledAt,
		})
		jobBScheduledAt := scheduledAt.Add(-15 * time.Minute)
		jobBLineage := &scheduler.JobLineageSummary{
			JobName:          "job-B",
			ScheduleInterval: interval,
			IsEnabled:        true,
		}
		jobBLineage.RecordRun(scheduler.JobRunIdentifier{JobName: jobALineage.JobName, ScheduledAt: scheduledAt}, &scheduler.JobRunSummary{
			ScheduledAt: jobBScheduledAt,
		})
		jobCTaskStartTime := scheduledAt.Add(-20 * time.Minute)
		jobCLineage := &scheduler.JobLineageSummary{
			JobName:          "job-C",
			ScheduleInterval: interval,
			IsEnabled:        true,
		}
		jobCLineage.RecordRun(scheduler.JobRunIdentifier{JobName: jobBLineage.JobName, ScheduledAt: jobBScheduledAt}, &scheduler.JobRunSummary{
			ScheduledAt:   scheduledAt.Add(-25 * time.Minute),
			TaskStartTime: &jobCTaskStartTime, // job-C is running, but not done yet
		})
		jobALineage.AddUpstream(jobBLineage)
		jobBLineage.AddUpstream(jobCLineage)

		jobDetailsGetter.On("GetJobs", ctx, projectName, []string{"job-A"}).Return([]*scheduler.JobWithDetails{jobA}, nil).Once()
		jobLineageFetcher.On("GetJobLineage", ctx, map[scheduler.JobName]*scheduler.JobSchedule{
			jobASchedule.JobName: jobASchedule,
		}, int(reqConfig.ScheduleRangeInHours.Hours())).Return(map[scheduler.JobName]*scheduler.JobLineageSummary{
			jobASchedule.JobName: jobALineage,
		}, nil).Once()
		durationEstimator.On("GetPercentileDurationByJobNames", ctx, referenceTime, mock.MatchedBy(func(jobNames []scheduler.JobName) bool {
			return assert.ElementsMatch(t, []scheduler.JobName{"job-A", "job-B", "job-C"}, jobNames)
		})).Return(map[scheduler.JobName]*time.Duration{
			"job-A": func() *time.Duration { d := 20 * time.Minute; return &d }(),
			"job-B": func() *time.Duration { d := 15 * time.Minute; return &d }(),
			"job-C": func() *time.Duration { d := 10 * time.Minute; return &d }(),
		}, nil).Once()

		// when
		jobBreachRootCause, err := jobSLAPredictorService.IdentifySLABreaches(ctx, projectName, jobNames, labels, reqConfig)

		// then
		assert.NoError(t, err)
		assert.Len(t, jobBreachRootCause, 1)
		assert.Len(t, jobBreachRootCause["job-A"], 1)
		assert.Equal(t, scheduler.JobName("job-C"), jobBreachRootCause["job-A"]["job-C"].JobName)
		assert.Equal(t, 2, jobBreachRootCause["job-A"]["job-C"].RelativeLevel)
		assert.Equal(t, scheduler.SLABreachCauseRunningLate, jobBreachRootCause["job-A"]["job-C"].Status)
	})

	t.Run("given 1 job that potentially breach due to upstream not started, return job causes", func(t *testing.T) {
		// job-C -> job-B -> job-A
		// is the target job
		// job-C is done, but job-B is not started, so job-A might breach its SLA

		// | job | estimated duration |
		// |-----|--------------------|
		// | A   | 20 mins            | -> targeted SLA 30 mins from now
		// | B   | 15 mins            | inferredSLA: now+10 mins, must start: now-5 mins
		// | C   | 10 mins            | inferredSLA: now-5 mins, must start: now-15 mins

		// given
		jobLineageFetcher := NewJobLineageFetcher(t)
		durationEstimator := NewDurationEstimator(t)
		jobDetailsGetter := NewJobDetailsGetter(t)
		jobSLAPredictorService := service.NewJobSLAPredictorService(l, conf, config.RootCauseConfig{}, nil, jobLineageFetcher, durationEstimator, jobDetailsGetter, nil, nil, scheduledChangeGetter, nil, nil)

		projectName := tenant.ProjectName("project-a")
		nextScheduledRangeInHours := 10 * time.Hour
		referenceTime := time.Now().UTC()
		jobNames := []scheduler.JobName{scheduler.JobName("job-A")}
		labels := map[string]string{}

		reqConfig := service.JobSLAPredictorRequestConfig{
			ReferenceTime:        referenceTime,
			ScheduleRangeInHours: nextScheduledRangeInHours,
			EnableAlert:          false,
			DamperFactor: scheduler.DamperFactor{
				Alpha: conf.DamperCoeff,
			},
			Severity: "",
		}

		tenant, _ := tenant.NewTenant("project-a", "team-a")
		startDate := referenceTime.Add(-24 * time.Hour).Truncate(time.Hour)
		// get hour from now for simulation purpose, we set scheduledAt to be now
		scheduledAt := referenceTime.Add(1 * time.Minute).Truncate(time.Minute)
		interval := fmt.Sprintf("%d %d * * *", scheduledAt.Minute(), scheduledAt.Hour()) // daily
		jobA := &scheduler.JobWithDetails{
			Name: "job-A",
			Job: &scheduler.Job{
				Tenant: tenant,
				Name:   "job-A",
			},
			Schedule: &scheduler.Schedule{
				StartDate: startDate,
				Interval:  interval,
			},
			Alerts: []scheduler.Alert{
				{
					On:     scheduler.EventCategorySLAMiss,
					Config: map[string]string{"duration": "30m"},
				},
			},
		}

		jobASchedule := &scheduler.JobSchedule{
			JobName:     "job-A",
			ScheduledAt: scheduledAt,
		}
		jobALineage := &scheduler.JobLineageSummary{
			JobName:          "job-A",
			ScheduleInterval: interval,
			IsEnabled:        true,
		}
		jobALineage.RecordOwnRun(&scheduler.JobRunSummary{
			ScheduledAt: scheduledAt,
		})
		jobBScheduledAt := scheduledAt.Add(-15 * time.Minute)
		jobBLineage := &scheduler.JobLineageSummary{
			JobName:          "job-B",
			ScheduleInterval: interval,
			IsEnabled:        true,
		}
		jobBLineage.RecordRun(scheduler.JobRunIdentifier{JobName: jobALineage.JobName, ScheduledAt: scheduledAt}, &scheduler.JobRunSummary{
			ScheduledAt: jobBScheduledAt, // job-B is not started yet, it should have started 5 mins ago
		})
		jobCTaskStartTime := scheduledAt.Add(-20 * time.Minute)
		jobCTaskEndTime := scheduledAt.Add(-10 * time.Minute)
		jobCLineage := &scheduler.JobLineageSummary{
			JobName:          "job-C",
			ScheduleInterval: interval,
			IsEnabled:        true,
		}
		jobCLineage.RecordRun(scheduler.JobRunIdentifier{JobName: jobBLineage.JobName, ScheduledAt: jobBScheduledAt}, &scheduler.JobRunSummary{
			ScheduledAt:   scheduledAt.Add(-25 * time.Minute),
			TaskStartTime: &jobCTaskStartTime,
			TaskEndTime:   &jobCTaskEndTime,
			JobEndTime:    &jobCTaskEndTime,
		})
		jobALineage.AddUpstream(jobBLineage)
		jobBLineage.AddUpstream(jobCLineage)

		jobDetailsGetter.On("GetJobs", ctx, projectName, []string{"job-A"}).Return([]*scheduler.JobWithDetails{jobA}, nil).Once()
		jobLineageFetcher.On("GetJobLineage", ctx, map[scheduler.JobName]*scheduler.JobSchedule{
			jobASchedule.JobName: jobASchedule,
		}, int(reqConfig.ScheduleRangeInHours.Hours())).Return(map[scheduler.JobName]*scheduler.JobLineageSummary{
			jobASchedule.JobName: jobALineage,
		}, nil).Once()
		durationEstimator.On("GetPercentileDurationByJobNames", ctx, referenceTime, mock.MatchedBy(func(jobNames []scheduler.JobName) bool {
			return assert.ElementsMatch(t, []scheduler.JobName{"job-A", "job-B", "job-C"}, jobNames)
		})).Return(map[scheduler.JobName]*time.Duration{
			"job-A": func() *time.Duration { d := 20 * time.Minute; return &d }(),
			"job-B": func() *time.Duration { d := 15 * time.Minute; return &d }(),
			"job-C": func() *time.Duration { d := 10 * time.Minute; return &d }(),
		}, nil).Once()

		// when
		jobBreachRootCause, err := jobSLAPredictorService.IdentifySLABreaches(ctx, projectName, jobNames, labels, reqConfig)

		// then
		assert.NoError(t, err)
		assert.Len(t, jobBreachRootCause, 1)
		assert.Len(t, jobBreachRootCause["job-A"], 1)
		assert.Equal(t, scheduler.JobName("job-B"), jobBreachRootCause["job-A"]["job-B"].JobName)
		assert.Equal(t, 1, jobBreachRootCause["job-A"]["job-B"].RelativeLevel)
		assert.Equal(t, scheduler.SLABreachCauseNotStarted, jobBreachRootCause["job-A"]["job-B"].Status)
	})

	t.Run("given 2 jobs that refer to the same upstream job, and only 1 job that has potential breach, return 1 job and its cause", func(t *testing.T) {
		// job-C -> job-B -> job-A1
		//       -> job-A2
		// job-A1 and job-A2 are the target jobs
		// job-C is running late, so job-A1 might breach its SLA, but job-A2 is safe

		// | job | estimated duration |
		// |-----|--------------------|
		// | A1  | 20 mins            | -> targeted SLA 30 mins from now
		// | A2  | 25 mins            | -> targeted SLA 30 mins from now
		// | B   | 15 mins            | (A1) inferredSLA: now+10 mins, must start: now-5 mins
		// | C   | 10 mins            | (A1) inferredSLA: now-5 mins, must start: now-15 mins; (A2) inferredSLA: now+5 mins, must start: now-5 mins

		// given
		jobLineageFetcher := NewJobLineageFetcher(t)
		durationEstimator := NewDurationEstimator(t)
		jobDetailsGetter := NewJobDetailsGetter(t)
		jobSLAPredictorService := service.NewJobSLAPredictorService(l, conf, config.RootCauseConfig{}, nil, jobLineageFetcher, durationEstimator, jobDetailsGetter, nil, nil, scheduledChangeGetter, nil, nil)

		projectName := tenant.ProjectName("project-a")
		nextScheduledRangeInHours := 10 * time.Hour
		referenceTime := time.Now().UTC()
		jobNames := []scheduler.JobName{scheduler.JobName("job-A1"), scheduler.JobName("job-A2")}
		labels := map[string]string{}

		reqConfig := service.JobSLAPredictorRequestConfig{
			ReferenceTime:        referenceTime,
			ScheduleRangeInHours: nextScheduledRangeInHours,
			EnableAlert:          false,
			DamperFactor: scheduler.DamperFactor{
				Alpha: conf.DamperCoeff,
			},
			Severity: "",
		}

		tenant, _ := tenant.NewTenant("project-a", "team-a")
		startDate := referenceTime.Add(-24 * time.Hour).Truncate(time.Hour)
		// get hour from now for simulation purpose, we set scheduledAt to be now
		scheduledAt := referenceTime.Add(1 * time.Minute).Truncate(time.Minute)
		interval := fmt.Sprintf("%d %d * * *", scheduledAt.Minute(), scheduledAt.Hour()) // daily
		jobA1 := &scheduler.JobWithDetails{
			Name: "job-A1",
			Job: &scheduler.Job{
				Tenant: tenant,
				Name:   "job-A1",
			},
			Schedule: &scheduler.Schedule{
				StartDate: startDate,
				Interval:  interval,
			},
			Alerts: []scheduler.Alert{
				{
					On:     scheduler.EventCategorySLAMiss,
					Config: map[string]string{"duration": "30m"},
				},
			},
		}
		jobA2 := &scheduler.JobWithDetails{
			Name: "job-A2",
			Job: &scheduler.Job{
				Tenant: tenant,
				Name:   "job-A2",
			},
			Schedule: &scheduler.Schedule{
				StartDate: startDate,
				Interval:  interval,
			},
			Alerts: []scheduler.Alert{
				{
					On:     scheduler.EventCategorySLAMiss,
					Config: map[string]string{"duration": "30m"},
				},
			},
		}

		jobASchedule1 := &scheduler.JobSchedule{
			JobName:     "job-A1",
			ScheduledAt: scheduledAt,
		}
		jobASchedule2 := &scheduler.JobSchedule{
			JobName:     "job-A2",
			ScheduledAt: scheduledAt,
		}
		jobA1Lineage := &scheduler.JobLineageSummary{
			JobName:          "job-A1",
			ScheduleInterval: interval,
			IsEnabled:        true,
		}
		jobA1Lineage.RecordOwnRun(&scheduler.JobRunSummary{
			ScheduledAt: scheduledAt,
		})
		jobA2Lineage := &scheduler.JobLineageSummary{
			JobName:          "job-A2",
			ScheduleInterval: interval,
			IsEnabled:        true,
		}
		jobA2Lineage.RecordOwnRun(&scheduler.JobRunSummary{
			ScheduledAt: scheduledAt,
		})
		jobBScheduledAt := scheduledAt.Add(-15 * time.Minute)
		jobBLineage := &scheduler.JobLineageSummary{
			JobName:          "job-B",
			ScheduleInterval: interval,
			IsEnabled:        true,
		}
		jobBLineage.RecordRun(scheduler.JobRunIdentifier{JobName: jobA1Lineage.JobName, ScheduledAt: scheduledAt}, &scheduler.JobRunSummary{
			ScheduledAt: jobBScheduledAt,
		})
		jobCTaskStartTime := scheduledAt.Add(-20 * time.Minute)
		jobCLineage := &scheduler.JobLineageSummary{
			JobName:          "job-C",
			ScheduleInterval: interval,
			IsEnabled:        true,
		}
		jobCLineage.RecordRun(scheduler.JobRunIdentifier{JobName: jobA2Lineage.JobName, ScheduledAt: scheduledAt}, &scheduler.JobRunSummary{
			ScheduledAt:   scheduledAt.Add(-25 * time.Minute),
			TaskStartTime: &jobCTaskStartTime, // job-C is running, but not done yet
		})
		jobCLineage.RecordRun(scheduler.JobRunIdentifier{JobName: jobBLineage.JobName, ScheduledAt: jobBScheduledAt}, &scheduler.JobRunSummary{
			ScheduledAt:   scheduledAt.Add(-25 * time.Minute),
			TaskStartTime: &jobCTaskStartTime, // job-C is running, but not done yet
		})
		jobA1Lineage.AddUpstream(jobBLineage)
		jobA2Lineage.AddUpstream(jobCLineage)
		jobBLineage.AddUpstream(jobCLineage)

		jobDetailsGetter.On("GetJobs", ctx, projectName, []string{"job-A1", "job-A2"}).Return([]*scheduler.JobWithDetails{jobA1, jobA2}, nil).Once()
		jobLineageFetcher.On("GetJobLineage", ctx, map[scheduler.JobName]*scheduler.JobSchedule{
			jobASchedule1.JobName: jobASchedule1,
			jobASchedule2.JobName: jobASchedule2,
		}, int(reqConfig.ScheduleRangeInHours.Hours())).Return(map[scheduler.JobName]*scheduler.JobLineageSummary{
			jobASchedule1.JobName: jobA1Lineage,
			jobASchedule2.JobName: jobA2Lineage,
		}, nil).Once()
		durationEstimator.On("GetPercentileDurationByJobNames", ctx, referenceTime, mock.MatchedBy(func(jobNames []scheduler.JobName) bool {
			return assert.ElementsMatch(t, []scheduler.JobName{"job-A1", "job-A2", "job-B", "job-C"}, jobNames)
		})).Return(map[scheduler.JobName]*time.Duration{
			"job-A1": func() *time.Duration { d := 20 * time.Minute; return &d }(), // target sla now + 30 mins
			"job-A2": func() *time.Duration { d := 25 * time.Minute; return &d }(), // target sla now + 30 mins
			"job-B":  func() *time.Duration { d := 15 * time.Minute; return &d }(),
			"job-C":  func() *time.Duration { d := 10 * time.Minute; return &d }(),
		}, nil).Once()

		// when
		jobBreachRootCause, err := jobSLAPredictorService.IdentifySLABreaches(ctx, projectName, jobNames, labels, reqConfig)

		// then
		assert.NoError(t, err)
		assert.Len(t, jobBreachRootCause, 1)
		assert.Len(t, jobBreachRootCause["job-A1"], 1)
		assert.Equal(t, scheduler.JobName("job-C"), jobBreachRootCause["job-A1"]["job-C"].JobName)
		assert.Equal(t, 2, jobBreachRootCause["job-A1"]["job-C"].RelativeLevel)
		assert.Equal(t, scheduler.SLABreachCauseRunningLate, jobBreachRootCause["job-A1"]["job-C"].Status)
	})

	t.Run("given 1 job with nonblocking error, should still return the job SLA breach without error", func(t *testing.T) {
		// job-C -> job-B -> job-A
		// is the target job
		// job-A contain error when fetching the job detail, but the error is non-blocking, so we should still return the potential SLA breach for job-A

		// use same scenario as "given 1 job that is on time, return no breaches", since this only tests the error handling part

		jobLineageFetcher := NewJobLineageFetcher(t)
		durationEstimator := NewDurationEstimator(t)
		jobDetailsGetter := NewJobDetailsGetter(t)
		jobSLAPredictorService := service.NewJobSLAPredictorService(l, conf, config.RootCauseConfig{}, nil, jobLineageFetcher, durationEstimator, jobDetailsGetter, nil, nil, scheduledChangeGetter, nil, nil)

		projectName := tenant.ProjectName("project-a")
		nextScheduledRangeInHours := 10 * time.Hour
		referenceTime := time.Now().UTC()
		jobNames := []scheduler.JobName{scheduler.JobName("job-A")}
		labels := map[string]string{}

		reqConfig := service.JobSLAPredictorRequestConfig{
			ReferenceTime:        referenceTime,
			ScheduleRangeInHours: nextScheduledRangeInHours,
			EnableAlert:          false,
			DamperFactor: scheduler.DamperFactor{
				Alpha: conf.DamperCoeff,
			},
			Severity: "",
		}

		tenant, _ := tenant.NewTenant("project-a", "team-a")
		startDate := referenceTime.Add(-24 * time.Hour).Truncate(time.Hour)
		// get hour from now for simulation purpose, we set scheduledAt to be now
		scheduledAt := referenceTime.Add(1 * time.Minute).Truncate(time.Minute)
		interval := fmt.Sprintf("%d %d * * *", scheduledAt.Minute(), scheduledAt.Hour()) // daily
		jobA := &scheduler.JobWithDetails{
			Name: "job-A",
			Job: &scheduler.Job{
				Tenant: tenant,
				Name:   "job-A",
			},
			Schedule: &scheduler.Schedule{
				StartDate: startDate,
				Interval:  interval,
			},
			Alerts: []scheduler.Alert{
				{
					On:     scheduler.EventCategorySLAMiss,
					Config: map[string]string{"duration": "30m"},
				},
			},
		}

		jobASchedule := &scheduler.JobSchedule{
			JobName:     "job-A",
			ScheduledAt: scheduledAt,
		}
		jobALineage := &scheduler.JobLineageSummary{
			JobName:          "job-A",
			ScheduleInterval: interval,
		}
		jobALineage.RecordOwnRun(&scheduler.JobRunSummary{
			ScheduledAt: scheduledAt,
		})
		jobBTaskStartTime := scheduledAt.Add(-10 * time.Minute)
		jobBScheduledAt := scheduledAt.Add(-15 * time.Minute)
		jobBLineage := &scheduler.JobLineageSummary{
			JobName:          "job-B",
			ScheduleInterval: interval,
		}
		jobBLineage.RecordRun(scheduler.JobRunIdentifier{JobName: jobALineage.JobName, ScheduledAt: scheduledAt}, &scheduler.JobRunSummary{
			ScheduledAt:   jobBScheduledAt,
			TaskStartTime: &jobBTaskStartTime,
		})
		jobCTaskStartTime := scheduledAt.Add(-20 * time.Minute)
		jobCTaskEndTime := scheduledAt.Add(-10 * time.Minute)
		jobCLineage := &scheduler.JobLineageSummary{
			JobName:          "job-C",
			ScheduleInterval: interval,
		}
		jobCLineage.RecordRun(scheduler.JobRunIdentifier{JobName: jobBLineage.JobName, ScheduledAt: jobBScheduledAt}, &scheduler.JobRunSummary{
			ScheduledAt:   scheduledAt.Add(-25 * time.Minute),
			TaskStartTime: &jobCTaskStartTime,
			TaskEndTime:   &jobCTaskEndTime,
			JobEndTime:    &jobCTaskEndTime,
		})
		jobALineage.AddUpstream(jobBLineage)
		jobBLineage.AddUpstream(jobCLineage)

		jobDetailsGetter.On("GetJobs", ctx, projectName, []string{"job-A"}).Return([]*scheduler.JobWithDetails{jobA}, errors.New("nonblocking error")).Once()
		jobLineageFetcher.On("GetJobLineage", ctx, map[scheduler.JobName]*scheduler.JobSchedule{
			jobASchedule.JobName: jobASchedule,
		}, int(reqConfig.ScheduleRangeInHours.Hours())).Return(map[scheduler.JobName]*scheduler.JobLineageSummary{
			jobASchedule.JobName: jobALineage,
		}, nil).Once()
		durationEstimator.On("GetPercentileDurationByJobNames", ctx, referenceTime, mock.MatchedBy(func(jobNames []scheduler.JobName) bool {
			return assert.ElementsMatch(t, []scheduler.JobName{"job-A", "job-B", "job-C"}, jobNames)
		})).Return(map[scheduler.JobName]*time.Duration{
			"job-A": func() *time.Duration { d := 20 * time.Minute; return &d }(),
			"job-B": func() *time.Duration { d := 15 * time.Minute; return &d }(),
			"job-C": func() *time.Duration { d := 10 * time.Minute; return &d }(),
		}, nil).Once()

		// when
		jobBreachRootCause, err := jobSLAPredictorService.IdentifySLABreaches(ctx, projectName, jobNames, labels, reqConfig)

		// then
		assert.NoError(t, err)
		assert.Len(t, jobBreachRootCause, 0)
	})

	t.Run("given targeted job that is running late, return that targeted job", func(t *testing.T) {
		// job-C -> job-B -> job-A
		// is the target job
		// upstream jobs are on time, breach happening at target job itself

		// | job | estimated duration |
		// |-----|--------------------|
		// | A   | 20 mins            | -> targeted SLA 30 mins from now
		// | B   | 15 mins            | inferredSLA: now+10 mins, must start: now-5 mins
		// | C   | 10 mins            | inferredSLA: now-5 mins, must start: now-15 mins

		// reference time = now + 35 mins and job-A is still running

		// given
		jobLineageFetcher := NewJobLineageFetcher(t)
		durationEstimator := NewDurationEstimator(t)
		jobDetailsGetter := NewJobDetailsGetter(t)
		jobSLAPredictorService := service.NewJobSLAPredictorService(l, conf, config.RootCauseConfig{}, nil, jobLineageFetcher, durationEstimator, jobDetailsGetter, nil, nil, scheduledChangeGetter, nil, nil)

		projectName := tenant.ProjectName("project-a")
		nextScheduledRangeInHours := 10 * time.Hour
		now := time.Now().UTC()
		referenceTime := now.Add(35 * time.Minute)
		jobNames := []scheduler.JobName{scheduler.JobName("job-A")}
		labels := map[string]string{}

		reqConfig := service.JobSLAPredictorRequestConfig{
			ReferenceTime:        referenceTime,
			ScheduleRangeInHours: nextScheduledRangeInHours,
			EnableAlert:          false,
			DamperFactor: scheduler.DamperFactor{
				Alpha: conf.DamperCoeff,
			},
			Severity: "",
		}

		tenant, _ := tenant.NewTenant("project-a", "team-a")
		startDate := referenceTime.Add(-24 * time.Hour).Truncate(time.Hour)
		// get hour from now for simulation purpose, we set scheduledAt to be now
		scheduledAt := now.Add(1 * time.Minute).Truncate(time.Minute)
		interval := fmt.Sprintf("%d %d * * *", scheduledAt.Minute(), scheduledAt.Hour()) // daily
		jobA := &scheduler.JobWithDetails{
			Name: "job-A",
			Job: &scheduler.Job{
				Tenant: tenant,
				Name:   "job-A",
			},
			Schedule: &scheduler.Schedule{
				StartDate: startDate,
				Interval:  interval,
			},
			Alerts: []scheduler.Alert{
				{
					On:     scheduler.EventCategorySLAMiss,
					Config: map[string]string{"duration": "30m"},
				},
			},
		}

		jobASchedule := &scheduler.JobSchedule{
			JobName:     "job-A",
			ScheduledAt: scheduledAt,
		}
		jobATaskStartTime := scheduledAt.Add(5 * time.Minute)
		jobALineage := &scheduler.JobLineageSummary{
			JobName:          "job-A",
			ScheduleInterval: interval,
			IsEnabled:        true,
		}
		jobALineage.RecordOwnRun(&scheduler.JobRunSummary{
			ScheduledAt:   scheduledAt,
			TaskStartTime: &jobATaskStartTime,
		})
		jobBTaskStartTime := scheduledAt.Add(-10 * time.Minute)
		jobBTaskEndTime := scheduledAt.Add(5 * time.Minute)
		jobBScheduledAt := scheduledAt.Add(-15 * time.Minute)
		jobBLineage := &scheduler.JobLineageSummary{
			JobName:          "job-B",
			ScheduleInterval: interval,
			IsEnabled:        true,
		}
		jobBLineage.RecordRun(scheduler.JobRunIdentifier{JobName: jobALineage.JobName, ScheduledAt: scheduledAt}, &scheduler.JobRunSummary{
			ScheduledAt:   jobBScheduledAt,
			TaskStartTime: &jobBTaskStartTime,
			TaskEndTime:   &jobBTaskEndTime,
			JobEndTime:    &jobBTaskEndTime,
		})
		jobCTaskStartTime := scheduledAt.Add(-20 * time.Minute)
		jobCTaskEndTime := scheduledAt.Add(-10 * time.Minute)
		jobCLineage := &scheduler.JobLineageSummary{
			JobName:          "job-C",
			ScheduleInterval: interval,
			IsEnabled:        true,
		}
		jobCLineage.RecordRun(scheduler.JobRunIdentifier{JobName: jobBLineage.JobName, ScheduledAt: jobBScheduledAt}, &scheduler.JobRunSummary{
			ScheduledAt:   scheduledAt.Add(-25 * time.Minute),
			TaskStartTime: &jobCTaskStartTime,
			TaskEndTime:   &jobCTaskEndTime,
			JobEndTime:    &jobCTaskEndTime,
		})
		jobALineage.AddUpstream(jobBLineage)
		jobBLineage.AddUpstream(jobCLineage)

		jobDetailsGetter.On("GetJobs", ctx, projectName, []string{"job-A"}).Return([]*scheduler.JobWithDetails{jobA}, nil).Once()
		jobLineageFetcher.On("GetJobLineage", ctx, map[scheduler.JobName]*scheduler.JobSchedule{
			jobASchedule.JobName: jobASchedule,
		}, int(reqConfig.ScheduleRangeInHours.Hours())).Return(map[scheduler.JobName]*scheduler.JobLineageSummary{
			jobASchedule.JobName: jobALineage,
		}, nil).Once()
		durationEstimator.On("GetPercentileDurationByJobNames", ctx, referenceTime, mock.MatchedBy(func(jobNames []scheduler.JobName) bool {
			return assert.ElementsMatch(t, []scheduler.JobName{"job-A", "job-B", "job-C"}, jobNames)
		})).Return(map[scheduler.JobName]*time.Duration{
			"job-A": func() *time.Duration { d := 20 * time.Minute; return &d }(),
			"job-B": func() *time.Duration { d := 15 * time.Minute; return &d }(),
			"job-C": func() *time.Duration { d := 10 * time.Minute; return &d }(),
		}, nil).Once()

		// when
		jobBreachRootCause, err := jobSLAPredictorService.IdentifySLABreaches(ctx, projectName, jobNames, labels, reqConfig)

		// then
		assert.NoError(t, err)
		assert.Len(t, jobBreachRootCause, 1)
		assert.Len(t, jobBreachRootCause["job-A"], 1)
		assert.Equal(t, scheduler.JobName("job-A"), jobBreachRootCause["job-A"]["job-A"].JobName)
		assert.Equal(t, 0, jobBreachRootCause["job-A"]["job-A"].RelativeLevel)
		assert.Equal(t, scheduler.SLABreachCauseRunningLate, jobBreachRootCause["job-A"]["job-A"].Status)
	})
}

func TestIdentifySLABreaches_AsymmetricCases(t *testing.T) {
	// Diamond lineage with an asymmetric shared merge point:
	//
	//        job-B ------------\
	//       /                   job-D -> job-E   (job-E is the deepest shared root)
	//   job-A                  /
	//       \                 /
	//        job-C -> job-F --
	//
	// Upstream edges: A->{B,C}, B->D, C->F, F->D, D->E. The two branches out of job-A have
	// different lengths and merge back at the shared job-D. scheduledAt is a fixed anchor;
	// each case only varies referenceTime ("now") and per-job run states.
	//
	// Durations (mins): A=20, B=15, C=15, F=10, D=10, E=5.
	// With a daily 1h SLA and damper 1.0 (s = scheduledAt):
	// 	 job-D & job-E's inferred SLA should be derived from the longest path itself, which is A-C-F-D-E
	//   inferred SLA: S(A)=s+60, S(B)=S(C)=s+40, S(F)=s+25, S(D)=s+15 (not s+25), S(E)=s+5 (not s+15)
	//   must-start (=inferredSLA-duration): A=s+40, B=C=s+25, F=s+15, D=s+5, E=s+0

	ctx := context.Background()
	l := log.NewNoop()
	conf := config.PotentialSLABreachConfig{DamperCoeff: 1.0, EnablePersistentLogging: false}

	scheduledChangeGetter := NewScheduledChangeGetter(t)
	scheduledChangeGetter.On("GetRecentScheduleChange", ctx, mock.Anything, mock.Anything, mock.Anything).Return("", nil).Maybe()

	projectName := tenant.ProjectName("project-a")
	tnnt, _ := tenant.NewTenant("project-a", "team-a")
	scheduledAt := time.Date(2026, 7, 2, 10, 0, 0, 0, time.UTC)
	interval := fmt.Sprintf("%d %d * * *", scheduledAt.Minute(), scheduledAt.Hour()) // daily
	nextScheduledRangeInHours := 10 * time.Hour

	durations := map[scheduler.JobName]*time.Duration{
		"job-A": dur(20 * time.Minute),
		"job-B": dur(15 * time.Minute),
		"job-C": dur(15 * time.Minute),
		"job-F": dur(10 * time.Minute),
		"job-D": dur(10 * time.Minute),
		"job-E": dur(5 * time.Minute),
	}

	// per-job run state, offsets relative to scheduledAt: nil start = not started;
	// start set with nil end = running; both set = done.
	type runState struct {
		start *time.Duration
		end   *time.Duration
	}

	buildLineage := func(states map[scheduler.JobName]runState) *scheduler.JobLineageSummary {
		// parentID builds the identifier of the immediate downstream occurrence that requires a
		// given upstream's run - every job in this fixture shares the same scheduledAt, so the
		// only thing that varies is which job name is asking.
		parentID := func(name scheduler.JobName) scheduler.JobRunIdentifier {
			return scheduler.JobRunIdentifier{JobName: name, ScheduledAt: scheduledAt}
		}
		newNode := func(name scheduler.JobName) (*scheduler.JobLineageSummary, *scheduler.JobRunSummary) {
			run := &scheduler.JobRunSummary{ScheduledAt: scheduledAt}
			if st := states[name]; st.start != nil {
				start := scheduledAt.Add(*st.start)
				run.JobStartTime = &start
				run.TaskStartTime = &start
				if st.end != nil {
					end := scheduledAt.Add(*st.end)
					run.TaskEndTime = &end
					run.JobEndTime = &end
				}
			}
			return &scheduler.JobLineageSummary{
				JobName:          name,
				ScheduleInterval: interval,
				IsEnabled:        true,
			}, run
		}
		a, aRun := newNode("job-A")
		b, bRun := newNode("job-B")
		c, cRun := newNode("job-C")
		f, fRun := newNode("job-F")
		d, dRun := newNode("job-D")
		e, eRun := newNode("job-E")

		a.RecordOwnRun(aRun)
		b.RecordRun(parentID("job-A"), bRun)
		c.RecordRun(parentID("job-A"), cRun)
		// job-D is reached via both job-B and job-F (asymmetric diamond merge point), so its one
		// physical run is recorded under both immediate-downstream identifiers.
		d.RecordRun(parentID("job-B"), dRun)
		d.RecordRun(parentID("job-F"), dRun)
		f.RecordRun(parentID("job-C"), fRun)
		e.RecordRun(parentID("job-D"), eRun)

		a.AddUpstream(b)
		a.AddUpstream(c)
		b.AddUpstream(d)
		c.AddUpstream(f)
		f.AddUpstream(d)
		d.AddUpstream(e)
		return a
	}

	run := func(t *testing.T, referenceTime time.Time, states map[scheduler.JobName]runState) map[scheduler.JobName]map[scheduler.JobName]*scheduler.JobState {
		t.Helper()
		jobLineageFetcher := NewJobLineageFetcher(t)
		durationEstimator := NewDurationEstimator(t)
		jobDetailsGetter := NewJobDetailsGetter(t)
		svc := service.NewJobSLAPredictorService(l, conf, config.RootCauseConfig{}, nil, jobLineageFetcher, durationEstimator, jobDetailsGetter, nil, nil, scheduledChangeGetter, nil, nil)

		jobA := &scheduler.JobWithDetails{
			Name: "job-A",
			Job:  &scheduler.Job{Tenant: tnnt, Name: "job-A"},
			Schedule: &scheduler.Schedule{
				StartDate: scheduledAt.Add(-24 * time.Hour).Truncate(time.Hour),
				Interval:  interval,
			},
			Alerts: []scheduler.Alert{{On: scheduler.EventCategorySLAMiss, Config: map[string]string{"duration": "1h"}}},
		}
		jobASchedule := &scheduler.JobSchedule{JobName: "job-A", ScheduledAt: scheduledAt}

		jobDetailsGetter.On("GetJobs", ctx, projectName, []string{"job-A"}).Return([]*scheduler.JobWithDetails{jobA}, nil).Once()
		jobLineageFetcher.On("GetJobLineage", ctx, map[scheduler.JobName]*scheduler.JobSchedule{
			jobASchedule.JobName: jobASchedule,
		}, int(nextScheduledRangeInHours.Hours())).Return(map[scheduler.JobName]*scheduler.JobLineageSummary{
			jobASchedule.JobName: buildLineage(states),
		}, nil).Once()
		durationEstimator.On("GetPercentileDurationByJobNames", ctx, referenceTime, mock.MatchedBy(func(jobNames []scheduler.JobName) bool {
			return assert.ElementsMatch(t, []scheduler.JobName{"job-A", "job-B", "job-C", "job-D", "job-E", "job-F"}, jobNames)
		})).Return(durations, nil).Once()

		reqConfig := service.JobSLAPredictorRequestConfig{
			ReferenceTime:        referenceTime,
			ScheduleRangeInHours: nextScheduledRangeInHours,
			DamperFactor: scheduler.DamperFactor{
				Alpha: conf.DamperCoeff,
			},
		}
		res, err := svc.IdentifySLABreaches(ctx, projectName, []scheduler.JobName{"job-A"}, map[string]string{}, reqConfig)
		assert.NoError(t, err)
		return res
	}

	t.Run("reference time mid-lineage, deepest root running late -> single root cause job-E", func(t *testing.T) {
		// now = s+6. job-E started (s+2) and still running; everything else not started.
		// B/C/F/D are also flagged (not started, past must-start) but each has a breaching
		// upstream, so only job-E (no breaching upstream) is the root cause.
		res := run(t, scheduledAt.Add(16*time.Minute), map[scheduler.JobName]runState{
			"job-E": {start: dur(2 * time.Minute)},
		})
		assert.Len(t, res, 1)
		assert.Len(t, res["job-A"], 1)
		assert.Equal(t, scheduler.JobName("job-E"), res["job-A"]["job-E"].JobName)
		assert.Equal(t, scheduler.SLABreachCauseRunningLate, res["job-A"]["job-E"].Status)
		assert.Equal(t, 4, res["job-A"]["job-E"].RelativeLevel)
	})

	t.Run("reference time just past job-E inferred SLA, only job-E flagged (not started)", func(t *testing.T) {
		// now = s+12: past job-E's inferred SLA (s+10) but before every other inferred SLA
		// (>= s+15), so job-E is the only breaching job. Nothing has started.
		res := run(t, scheduledAt.Add(12*time.Minute), map[scheduler.JobName]runState{})
		assert.Len(t, res, 1)
		assert.Len(t, res["job-A"], 1)
		assert.Equal(t, scheduler.JobName("job-E"), res["job-A"]["job-E"].JobName)
		assert.Equal(t, scheduler.SLABreachCauseNotStarted, res["job-A"]["job-E"].Status)
		assert.Equal(t, 4, res["job-A"]["job-E"].RelativeLevel)
	})

	t.Run("breach isolated to one branch upstream -> root cause job-F", func(t *testing.T) {
		// now = s+30. The shared job-D (end s+14 <= S(D)=s+15) and job-E (end s+4 <= S(E)=s+5)
		// finished on time; only job-F (C-branch) is running late. job-C is blocked behind it
		// (flagged not-started) but excluded because its upstream job-F breaches. The B-branch
		// is entirely done and clean.
		res := run(t, scheduledAt.Add(30*time.Minute), map[scheduler.JobName]runState{
			"job-E": {start: dur(0), end: dur(4 * time.Minute)},
			"job-D": {start: dur(4 * time.Minute), end: dur(14 * time.Minute)},
			"job-F": {start: dur(14 * time.Minute)},
			"job-B": {start: dur(15 * time.Minute), end: dur(28 * time.Minute)},
			// job-C not started (blocked behind late job-F)
		})
		assert.Len(t, res, 1)
		assert.Len(t, res["job-A"], 1)
		assert.Equal(t, scheduler.JobName("job-F"), res["job-A"]["job-F"].JobName)
		assert.Equal(t, scheduler.SLABreachCauseRunningLate, res["job-A"]["job-F"].Status)
		assert.Equal(t, 2, res["job-A"]["job-F"].RelativeLevel)
	})

	t.Run("breach is in 2 upstream branch job-F and job-B", func(t *testing.T) {
		// now = s+30. The shared job-D and job-E finished on time; job-F is running late
		// and job-B has not started. Both are graph leaves, but identification keeps the
		// max induced delay: job-F overran by 6m vs job-B started-late by 5m.
		res := run(t, scheduledAt.Add(30*time.Minute), map[scheduler.JobName]runState{
			"job-E": {start: dur(0), end: dur(4 * time.Minute)},
			"job-D": {start: dur(4 * time.Minute), end: dur(14 * time.Minute)},
			"job-F": {start: dur(14 * time.Minute)},
			// job-B not started due to some reason
			// job-C not started (blocked behind late job-F)
		})
		assert.Len(t, res, 1)
		assert.Len(t, res["job-A"], 1)
		assert.Equal(t, scheduler.JobName("job-F"), res["job-A"]["job-F"].JobName)
		assert.Equal(t, scheduler.SLABreachCauseRunningLate, res["job-A"]["job-F"].Status)
		assert.Equal(t, 2, res["job-A"]["job-F"].RelativeLevel)
	})

	t.Run("shared merge point job-D running late -> root cause job-D", func(t *testing.T) {
		// now = s+30. job-E finished on time (end s+4 <= S(E)=s+5) but the shared merge job-D
		// is running late. B/C/F are blocked behind job-D and excluded; job-D has no breaching
		// upstream.
		res := run(t, scheduledAt.Add(30*time.Minute), map[scheduler.JobName]runState{
			"job-E": {start: dur(0), end: dur(4 * time.Minute)},
			"job-D": {start: dur(6 * time.Minute)},
			// B, C, F not started (blocked behind late job-D)
		})
		assert.Len(t, res, 1)
		assert.Len(t, res["job-A"], 1)
		assert.Equal(t, scheduler.JobName("job-D"), res["job-A"]["job-D"].JobName)
		assert.Equal(t, scheduler.SLABreachCauseRunningLate, res["job-A"]["job-D"].Status)
		assert.Equal(t, 3, res["job-A"]["job-D"].RelativeLevel)
	})

	t.Run("shared merge point job-D finished late -> root cause job-D", func(t *testing.T) {
		// now = s+30. job-D is finishing late (s+17 > s+15). B/C/F are blocked behind job-D and excluded; job-D has no breaching
		// upstream.
		res := run(t, scheduledAt.Add(30*time.Minute), map[scheduler.JobName]runState{
			"job-E": {start: dur(0), end: dur(4 * time.Minute)},
			"job-D": {start: dur(4 * time.Minute), end: dur(17 * time.Minute)},
			// B, C, F not started yet (blocked behind late job-D)
		})
		assert.Len(t, res, 1)
		assert.Len(t, res["job-A"], 1)
		assert.Equal(t, scheduler.JobName("job-D"), res["job-A"]["job-D"].JobName)
		assert.Equal(t, scheduler.SLABreachCauseRunningLate, res["job-A"]["job-D"].Status)
		assert.Equal(t, 3, res["job-A"]["job-D"].RelativeLevel)
	})

	t.Run("target job itself running late, upstreams done -> root cause job-A", func(t *testing.T) {
		// now = s+65, past job-A's own SLA (s+60). All upstreams finished before their
		// inferred SLAs (E<=s+5, D<=s+15, F<=s+25, B/C<=s+40), so job-A itself is the only
		// breaching job (level 0).
		res := run(t, scheduledAt.Add(65*time.Minute), map[scheduler.JobName]runState{
			"job-A": {start: dur(50 * time.Minute)},
			"job-B": {start: dur(25 * time.Minute), end: dur(38 * time.Minute)},
			"job-C": {start: dur(25 * time.Minute), end: dur(38 * time.Minute)},
			"job-F": {start: dur(14 * time.Minute), end: dur(24 * time.Minute)},
			"job-D": {start: dur(4 * time.Minute), end: dur(14 * time.Minute)},
			"job-E": {start: dur(0), end: dur(4 * time.Minute)},
		})
		assert.Len(t, res, 1)
		assert.Len(t, res["job-A"], 1)
		assert.Equal(t, scheduler.JobName("job-A"), res["job-A"]["job-A"].JobName)
		assert.Equal(t, scheduler.SLABreachCauseRunningLate, res["job-A"]["job-A"].Status)
		assert.Equal(t, 0, res["job-A"]["job-A"].RelativeLevel)
	})

	t.Run("every job completed before its inferred SLA -> no breach", func(t *testing.T) {
		// now = s+50, everything done ahead of its inferred SLA
		// (E<=s+5, D<=s+15, F<=s+25, B/C<=s+40, A<=s+60).
		res := run(t, scheduledAt.Add(50*time.Minute), map[scheduler.JobName]runState{
			"job-A": {start: dur(40 * time.Minute), end: dur(45 * time.Minute)},
			"job-B": {start: dur(25 * time.Minute), end: dur(38 * time.Minute)},
			"job-C": {start: dur(25 * time.Minute), end: dur(38 * time.Minute)},
			"job-F": {start: dur(14 * time.Minute), end: dur(24 * time.Minute)},
			"job-D": {start: dur(4 * time.Minute), end: dur(14 * time.Minute)},
			"job-E": {start: dur(0), end: dur(4 * time.Minute)},
		})
		assert.Len(t, res, 0)
	})
}

func TestIdentifySLABreach(t *testing.T) {
	l := log.NewNoop()
	conf := config.PotentialSLABreachConfig{
		DamperCoeff:             1.0,
		EnablePersistentLogging: false,
	}
	ctx := context.Background()

	scheduledChangeGetter := NewScheduledChangeGetter(t)
	scheduledChangeGetter.On("GetRecentScheduleChange", ctx, mock.Anything, mock.Anything, mock.Anything).Return("", nil).Maybe()

	slaPredictorService := service.NewJobSLAPredictorService(l, conf, config.RootCauseConfig{}, nil, nil, nil, nil, nil, nil, scheduledChangeGetter, nil, nil)

	t.Run("double-scheduled upstream: divergent runs required at different levels", func(t *testing.T) {
		// job-U is double-scheduled: it has two independent real runs (run1 and run2, at
		// different ScheduledAt), each required by a different downstream chain at a different
		// depth:
		//
		//   job-A -> job-B -> job-U (run2, level 2, tighter deadline)
		//   job-A -> job-C -> job-D -> job-U (run1, level 3, looser deadline)
		//
		// run2 (the shallower chain's run, with the numerically tighter deadline) finishes on
		// time. run1 (the deeper chain's run, with a looser deadline despite being reached at a
		// greater depth) is still running past its own deadline. The reported breach for job-U
		// must carry run1's own level/predecessor chain (level 3, via job-C -> job-D) - not
		// job-B's chain, even though job-B's occurrence of job-U happens to have the tighter
		// deadline of the two.
		base := time.Date(2026, 7, 3, 10, 0, 0, 0, time.UTC)
		targetSLA := base.Add(60 * time.Minute)

		jobA := &scheduler.JobLineageSummary{JobName: "job-A", IsEnabled: true}
		jobB := &scheduler.JobLineageSummary{JobName: "job-B", IsEnabled: true}
		jobC := &scheduler.JobLineageSummary{JobName: "job-C", IsEnabled: true}
		jobD := &scheduler.JobLineageSummary{JobName: "job-D", IsEnabled: true}
		jobU := &scheduler.JobLineageSummary{JobName: "job-U", IsEnabled: true}

		aRun := &scheduler.JobRunSummary{ScheduledAt: base}

		bStart, bEnd := base.Add(5*time.Minute), base.Add(20*time.Minute)
		bRun := &scheduler.JobRunSummary{ScheduledAt: base, TaskStartTime: &bStart, TaskEndTime: &bEnd, JobEndTime: &bEnd}

		cStart, cEnd := base.Add(2*time.Minute), base.Add(6*time.Minute)
		cRun := &scheduler.JobRunSummary{ScheduledAt: base, TaskStartTime: &cStart, TaskEndTime: &cEnd, JobEndTime: &cEnd}

		dStart, dEnd := base.Add(7*time.Minute), base.Add(9*time.Minute)
		dRun := &scheduler.JobRunSummary{ScheduledAt: base, TaskStartTime: &dStart, TaskEndTime: &dEnd, JobEndTime: &dEnd}

		// run1: job-U's run required via job-D (the deeper, looser-deadline chain). Started but
		// still running past its deadline.
		run1ScheduledAt := base
		run1Start := base.Add(10 * time.Minute)
		run1 := &scheduler.JobRunSummary{ScheduledAt: run1ScheduledAt, TaskStartTime: &run1Start}

		// run2: job-U's other run, required via job-B (the shallower, tighter-deadline chain).
		// A distinct real execution - same job name, different scheduled time - that finishes
		// comfortably before its own (tighter) deadline.
		run2ScheduledAt := base.Add(12 * time.Hour)
		run2Start, run2End := base.Add(21*time.Minute), base.Add(25*time.Minute)
		run2 := &scheduler.JobRunSummary{ScheduledAt: run2ScheduledAt, TaskStartTime: &run2Start, TaskEndTime: &run2End, JobEndTime: &run2End}

		jobA.RecordOwnRun(aRun)
		jobB.RecordRun(scheduler.JobRunIdentifier{JobName: "job-A", ScheduledAt: base}, bRun)
		jobC.RecordRun(scheduler.JobRunIdentifier{JobName: "job-A", ScheduledAt: base}, cRun)
		jobD.RecordRun(scheduler.JobRunIdentifier{JobName: "job-C", ScheduledAt: base}, dRun)
		jobU.RecordRun(scheduler.JobRunIdentifier{JobName: "job-D", ScheduledAt: base}, run1)
		jobU.RecordRun(scheduler.JobRunIdentifier{JobName: "job-B", ScheduledAt: base}, run2)

		jobA.AddUpstream(jobB)
		jobA.AddUpstream(jobC)
		jobB.AddUpstream(jobU)
		jobC.AddUpstream(jobD)
		jobD.AddUpstream(jobU)

		durations := map[scheduler.JobName]*time.Duration{
			"job-A": dur(10 * time.Minute),
			"job-B": dur(15 * time.Minute),
			"job-C": dur(3 * time.Minute),
			"job-D": dur(2 * time.Minute),
			"job-U": dur(5 * time.Minute),
		}

		// inferred SLAs (damper=1.0, full duration subtracted per hop):
		//   S(A) = base+60                        (level 0)
		//   S(B) = S(C) = base+60-10 = base+50     (level 1)
		//   S(U via B, run2) = base+50-15 = base+35 (level 2, tighter)
		//   S(D) = base+50-3 = base+47             (level 2)
		//   S(U via D, run1) = base+47-2 = base+45 (level 3, looser, despite being deeper)
		//
		// referenceTime is past run1's deadline (base+45) but before every other job's own
		// deadline, and run2 already finished before its own (tighter) deadline regardless.
		referenceTime := base.Add(46 * time.Minute)

		breachesCauses, fullBreachesCauses := slaPredictorService.IdentifySLABreach(ctx, jobA, durations, &targetSLA, map[scheduler.JobName]bool{}, scheduler.DamperFactor{Alpha: 1.0}, referenceTime)

		assert.Len(t, breachesCauses, 1)
		if uState, ok := breachesCauses["job-U"]; assert.True(t, ok) {
			assert.Equal(t, scheduler.SLABreachCauseRunningLate, uState.Status)
			assert.True(t, uState.JobRun.ScheduledAt.Equal(run1ScheduledAt), "breach should be attributed to run1, not run2")
			assert.Equal(t, 3, uState.RelativeLevel, "level must reflect run1's own chain (via job-D), not run2's (via job-B)")
		}

		if path, ok := fullBreachesCauses["job-U"]; assert.True(t, ok) && assert.Len(t, path, 4) {
			wantChain := []scheduler.JobName{"job-A", "job-C", "job-D", "job-U"}
			wantLevels := []int{0, 1, 2, 3}
			for i, name := range wantChain {
				assert.Equal(t, name, path[i].JobName)
				assert.Equal(t, wantLevels[i], path[i].RelativeLevel)
			}
		}
	})

	t.Run("given job lineage with no upstream issues", func(t *testing.T) {
		referenceTime := time.Now().UTC()
		targetedSLAOffset := 30 * time.Minute
		targetSLA := referenceTime.Add(targetedSLAOffset)
		durations := map[scheduler.JobName]*time.Duration{
			"job-A": func() *time.Duration { d := 20 * time.Minute; return &d }(),
			"job-B": func() *time.Duration { d := 15 * time.Minute; return &d }(),
			"job-C": func() *time.Duration { d := 10 * time.Minute; return &d }(),
		}
		jobNames := []scheduler.JobName{"job-A", "job-B", "job-C"}

		jobTargetLineageMap, _ := generateLineageWithSLAStates(slaPredictorService, durations, jobNames, referenceTime, scheduler.DamperFactor{Alpha: 1.0}, targetSLA)

		jobTargetLineage := jobTargetLineageMap["job-A"]

		breachCauses, allUpstreamStates := slaPredictorService.IdentifySLABreach(ctx, jobTargetLineage, durations, &targetSLA, map[scheduler.JobName]bool{}, scheduler.DamperFactor{Alpha: 1.0}, referenceTime)

		assert.Len(t, breachCauses, 0)
		assert.Len(t, allUpstreamStates, 0)
	})

	t.Run("given job lineage with 1 upstreams running late, return breach causes with full upstreams", func(t *testing.T) {
		// job-C -> job-B -> job-A
		// is the target job
		// job-C is running late, so job-A might breach its SLA

		// | job | estimated duration |
		// |-----|--------------------|
		// | A   | 20 mins            | -> targeted SLA 30 mins from now
		// | B   | 15 mins            | inferredSLA: now+10 mins, must start: now-5 mins
		// | C   | 10 mins            | inferredSLA: now-5 mins, must start: now-15 mins

		referenceTime := time.Now().UTC()
		targetedSLAOffset := 30 * time.Minute
		targetSLA := referenceTime.Add(targetedSLAOffset)
		durations := map[scheduler.JobName]*time.Duration{
			"job-A": func() *time.Duration { d := 20 * time.Minute; return &d }(),
			"job-B": func() *time.Duration { d := 15 * time.Minute; return &d }(),
			"job-C": func() *time.Duration { d := 10 * time.Minute; return &d }(),
		}
		jobNames := []scheduler.JobName{"job-A", "job-B", "job-C"}

		jobTargetLineageMap, runsByJobName := generateLineageWithSLAStates(slaPredictorService, durations, jobNames, referenceTime, scheduler.DamperFactor{Alpha: 1.0}, targetSLA)

		jobTargetLineage := jobTargetLineageMap["job-A"]

		runsByJobName["job-A"].TaskStartTime = nil
		runsByJobName["job-A"].TaskEndTime = nil
		runsByJobName["job-A"].JobEndTime = nil
		runsByJobName["job-B"].TaskStartTime = nil
		runsByJobName["job-B"].TaskEndTime = nil
		runsByJobName["job-B"].JobEndTime = nil
		runsByJobName["job-C"].TaskEndTime = nil
		runsByJobName["job-C"].JobEndTime = nil

		skipJobNames := map[scheduler.JobName]bool{}

		breachesCauses, fullBreachesCauses := slaPredictorService.IdentifySLABreach(ctx, jobTargetLineage, durations, &targetSLA, skipJobNames, scheduler.DamperFactor{Alpha: 1.0}, referenceTime)

		assert.Len(t, breachesCauses, 1)
		assert.Equal(t, breachesCauses["job-C"].Status, scheduler.SLABreachCauseRunningLate)

		assert.Len(t, fullBreachesCauses, 1)
		assert.Len(t, fullBreachesCauses["job-C"], 3)
	})

	t.Run("given long lineage 20 levels deep, with dampering factor 1.0, return breach causes on job-18", func(t *testing.T) {
		referenceTime := time.Now().UTC()
		targetedSLAOffset := 15 * 5 * time.Minute
		targetSLA := referenceTime.Add(targetedSLAOffset)
		damperCoeff := 1.0

		durations := map[scheduler.JobName]*time.Duration{}
		jobNames := []scheduler.JobName{}
		for i := 0; i < 20; i++ {
			jobName := scheduler.JobName(fmt.Sprintf("job-%d", i+1))
			durations[jobName] = func() *time.Duration { d := 5 * time.Minute; return &d }()
			jobNames = append(jobNames, jobName)
		}
		jobTargetLineageMap, runsByJobName := generateLineageWithSLAStates(slaPredictorService, durations, jobNames, referenceTime, scheduler.DamperFactor{Alpha: damperCoeff}, targetSLA)

		jobTargetLineage := jobTargetLineageMap["job-1"]
		// job-1 .. job-17 are not started yet
		for i := 1; i < 18; i++ {
			jobName := scheduler.JobName(fmt.Sprintf("job-%d", i))
			runsByJobName[jobName].TaskStartTime = nil
			runsByJobName[jobName].TaskEndTime = nil
			runsByJobName[jobName].JobEndTime = nil
		}
		// job-18 is running late
		runsByJobName["job-18"].TaskEndTime = nil
		runsByJobName["job-18"].JobEndTime = nil

		skipJobNames := map[scheduler.JobName]bool{}

		breachesCauses, fullBreachesCauses := slaPredictorService.IdentifySLABreach(ctx, jobTargetLineage, durations, &targetSLA, skipJobNames, scheduler.DamperFactor{Alpha: damperCoeff}, referenceTime)

		assert.Len(t, breachesCauses, 1)
		assert.Equal(t, breachesCauses["job-18"].Status, scheduler.SLABreachCauseRunningLate)

		assert.Len(t, fullBreachesCauses, 1)
		assert.Len(t, fullBreachesCauses["job-18"], 18)
	})

	t.Run("given long lineage 20 levels deep, with dampering factor 0.6, return no breach, as 15 deep level will be dampened", func(t *testing.T) {
		referenceTime := time.Now().UTC()
		targetedSLAOffset := 15 * 5 * time.Minute
		targetSLA := referenceTime.Add(targetedSLAOffset)
		damperCoeff := 0.6

		durations := map[scheduler.JobName]*time.Duration{}
		jobNames := []scheduler.JobName{}
		for i := 0; i < 20; i++ {
			jobName := scheduler.JobName(fmt.Sprintf("job-%d", i+1))
			durations[jobName] = func() *time.Duration { d := 5 * time.Minute; return &d }()
			jobNames = append(jobNames, jobName)
		}
		jobTargetLineageMap, runsByJobName := generateLineageWithSLAStates(slaPredictorService, durations, jobNames, referenceTime, scheduler.DamperFactor{Alpha: damperCoeff}, targetSLA)

		jobTargetLineage := jobTargetLineageMap["job-1"]
		// job-1 .. job-17 are not started yet
		for i := 1; i < 18; i++ {
			jobName := scheduler.JobName(fmt.Sprintf("job-%d", i))
			runsByJobName[jobName].TaskStartTime = nil
			runsByJobName[jobName].TaskEndTime = nil
			runsByJobName[jobName].JobEndTime = nil
		}
		// job-18 is running late
		runsByJobName["job-18"].TaskEndTime = nil
		runsByJobName["job-18"].JobEndTime = nil

		skipJobNames := map[scheduler.JobName]bool{}

		breachesCauses, fullBreachesCauses := slaPredictorService.IdentifySLABreach(ctx, jobTargetLineage, durations, &targetSLA, skipJobNames, scheduler.DamperFactor{Alpha: damperCoeff}, referenceTime)

		assert.Len(t, breachesCauses, 0)
		assert.Len(t, fullBreachesCauses, 0)
	})

	t.Run("target job already finished before its SLA, no alert even if upstream is running late", func(t *testing.T) {
		// job-C -> job-B -> job-A
		// (target) completed 5 min before its SLA — the SLA was met.
		// job-C is running late (past its inferredSLA): without the guard it would trigger a
		// RUNNING_LATE alert for job-A. The guard should suppress all upstream alerts.

		// | job | estimated duration | inferredSLA            |
		// |-----|--------------------|------------------------|
		// | A   | 20 mins            | now+30min (SLA)        | finished at now+25min ✓
		// | B   | 15 mins            | now+10min              | not started
		// | C   | 10 mins            | now-5min (past!)       | running — late without guard

		referenceTime := time.Now().UTC()
		targetSLA := referenceTime.Add(30 * time.Minute)

		jobAEndTime := targetSLA.Add(-5 * time.Minute) // finished 5 min before SLA
		jobAStartTime := jobAEndTime.Add(-20 * time.Minute)
		jobCStartTime := referenceTime.Add(-10 * time.Minute) // running, not done

		durations := map[scheduler.JobName]*time.Duration{
			"job-A": dur(20 * time.Minute),
			"job-B": dur(15 * time.Minute),
			"job-C": dur(10 * time.Minute),
		}

		jobCLineage := &scheduler.JobLineageSummary{
			JobName:   "job-C",
			IsEnabled: true,
		}
		jobBLineage := &scheduler.JobLineageSummary{
			JobName:   "job-B",
			IsEnabled: true,
		}
		jobCLineage.RecordRun(scheduler.JobRunIdentifier{JobName: jobBLineage.JobName, ScheduledAt: referenceTime}, &scheduler.JobRunSummary{
			ScheduledAt: referenceTime, TaskStartTime: &jobCStartTime,
		})
		jobBLineage.AddUpstream(jobCLineage)
		jobALineage := &scheduler.JobLineageSummary{
			JobName:   "job-A",
			IsEnabled: true,
		}
		jobBLineage.RecordRun(scheduler.JobRunIdentifier{JobName: jobALineage.JobName, ScheduledAt: referenceTime}, &scheduler.JobRunSummary{
			ScheduledAt: referenceTime,
		})
		jobALineage.RecordOwnRun(&scheduler.JobRunSummary{
			ScheduledAt:   referenceTime,
			TaskStartTime: &jobAStartTime,
			TaskEndTime:   &jobAEndTime,
			JobEndTime:    &jobAEndTime,
		})
		jobALineage.AddUpstream(jobBLineage)

		breachesCauses, fullBreachesCauses := slaPredictorService.IdentifySLABreach(
			ctx, jobALineage, durations, &targetSLA, map[scheduler.JobName]bool{}, scheduler.DamperFactor{Alpha: 1.0}, referenceTime,
		)

		assert.Len(t, breachesCauses, 0)
		assert.Len(t, fullBreachesCauses, 0)
	})

	t.Run("upstream with no job run stops DFS — its own upstreams are not evaluated for breach", func(t *testing.T) {
		// job-D -> job-C (no run for job-A) -> job-B -> job-A
		// job-C has no job run keyed by job-A, so DFS stops at job-C.
		// job-D is past its inferredSLA and still running (RUNNING_LATE if visited),
		// but it is never reached and therefore not reported.

		// | job | estimated duration | inferredSLA    |
		// |-----|--------------------|----------------|
		// | A   | 5 mins             | now+10min      |
		// | B   | 5 mins             | now+5min       |
		// | C   | 5 mins             | now (no run)   | DFS stops here
		// | D   | 5 mins             | now-5min       | running late but not visited

		referenceTime := time.Now().UTC()
		targetSLA := referenceTime.Add(10 * time.Minute)

		jobDStartTime := referenceTime.Add(-20 * time.Minute) // started 20 min ago, still running

		durations := map[scheduler.JobName]*time.Duration{
			"job-A": dur(5 * time.Minute),
			"job-B": dur(5 * time.Minute),
			"job-C": dur(5 * time.Minute),
			"job-D": dur(5 * time.Minute),
		}

		jobDLineage := &scheduler.JobLineageSummary{
			JobName:   "job-D",
			IsEnabled: true,
		}
		jobCLineage := &scheduler.JobLineageSummary{
			JobName:   "job-C",
			IsEnabled: true,
			// no run recorded for job-C at all: its own downstream identifier is deliberately
			// omitted below so DFS stops here.
		}
		jobDLineage.RecordRun(scheduler.JobRunIdentifier{JobName: jobCLineage.JobName, ScheduledAt: referenceTime}, &scheduler.JobRunSummary{
			ScheduledAt: referenceTime, TaskStartTime: &jobDStartTime,
		})
		jobCLineage.AddUpstream(jobDLineage)
		jobBLineage := &scheduler.JobLineageSummary{
			JobName:   "job-B",
			IsEnabled: true,
		}
		jobBLineage.AddUpstream(jobCLineage)
		jobALineage := &scheduler.JobLineageSummary{
			JobName:   "job-A",
			IsEnabled: true,
		}
		jobBLineage.RecordRun(scheduler.JobRunIdentifier{JobName: jobALineage.JobName, ScheduledAt: referenceTime}, &scheduler.JobRunSummary{
			ScheduledAt: referenceTime,
		})
		jobALineage.RecordOwnRun(&scheduler.JobRunSummary{ScheduledAt: referenceTime})
		jobALineage.AddUpstream(jobBLineage)

		breachesCauses, fullBreachesCauses := slaPredictorService.IdentifySLABreach(
			ctx, jobALineage, durations, &targetSLA, map[scheduler.JobName]bool{}, scheduler.DamperFactor{Alpha: 1.0}, referenceTime,
		)

		assert.Len(t, breachesCauses, 0)
		assert.Len(t, fullBreachesCauses, 0)
	})
}

func TestCalculateInferredSLAs(t *testing.T) {
	l := log.NewNoop()
	svc := service.NewJobSLAPredictorService(l, config.PotentialSLABreachConfig{}, config.RootCauseConfig{}, nil, nil, nil, nil, nil, nil, nil, nil, nil)

	base := time.Date(2026, 7, 3, 10, 0, 0, 0, time.UTC)

	// id builds the identifier of name's own occurrence - every job in this suite shares the same
	// base scheduledAt, so that's the only thing that varies.
	id := func(name scheduler.JobName) scheduler.JobRunIdentifier {
		return scheduler.JobRunIdentifier{JobName: name, ScheduledAt: base}
	}

	// inferredSLANode pairs a lineage node with its own run (nil when the node has no run at all,
	// mirroring the old "no job run" test cases).
	type inferredSLANode struct {
		summary *scheduler.JobLineageSummary
		run     *scheduler.JobRunSummary
	}

	// node builds a lineage node named name, wires it to its upstreams, and - for each upstream
	// that itself has a run - records that upstream's run as required by this node's own
	// occurrence. A diamond (the same upstream passed to two different node calls) naturally ends
	// up with one RunSet entry per requiring downstream, all sharing the same underlying run.
	node := func(name scheduler.JobName, scheduledAt *time.Time, upstreams ...inferredSLANode) inferredSLANode {
		summary := &scheduler.JobLineageSummary{JobName: name}
		var run *scheduler.JobRunSummary
		if scheduledAt != nil {
			run = &scheduler.JobRunSummary{ScheduledAt: *scheduledAt}
		}
		for _, u := range upstreams {
			summary.AddUpstream(u.summary)
			if run != nil && u.run != nil {
				u.summary.RecordRun(id(name), u.run)
			}
		}
		return inferredSLANode{summary: summary, run: run}
	}

	// root finalizes n as the lineage's root (self-keys its own run) and returns the node to pass
	// into CalculateInferredSLAs.
	root := func(n inferredSLANode) *scheduler.JobLineageSummary {
		n.summary.RecordOwnRun(n.run)
		return n.summary
	}

	t.Run("single target node with no upstreams", func(t *testing.T) {
		a := node("job-A", &base)
		gotSLAs, gotPath := svc.CalculateInferredSLAs(root(a), map[scheduler.JobName]*time.Duration{"job-A": dur(20 * time.Minute)}, &base, scheduler.DamperFactor{Alpha: 1.0})

		assert.Equal(t, base, gotSLAs[id("job-A")])
		assert.Len(t, gotSLAs, 1)
		assert.Empty(t, gotPath.Pred)
		assert.Equal(t, map[scheduler.JobRunIdentifier]int{id("job-A"): 0}, gotPath.Level)
	})

	t.Run("linear chain damper=1.0 propagates full duration at each hop", func(t *testing.T) {
		// topology: A -> B -> C
		// S(B) = base - dur_A*1.0 = base-20m   (level 1)
		// S(C) = base-20m - dur_B*1.0 = base-35m (level 2)
		c := node("job-C", &base)
		b := node("job-B", &base, c)
		a := node("job-A", &base, b)
		durations := map[scheduler.JobName]*time.Duration{
			"job-A": dur(20 * time.Minute),
			"job-B": dur(15 * time.Minute),
			"job-C": dur(10 * time.Minute),
		}
		gotSLAs, gotPath := svc.CalculateInferredSLAs(root(a), durations, &base, scheduler.DamperFactor{Alpha: 1.0})

		assert.Equal(t, base, gotSLAs[id("job-A")])
		assert.Equal(t, base.Add(-20*time.Minute), gotSLAs[id("job-B")])
		assert.Equal(t, base.Add(-35*time.Minute), gotSLAs[id("job-C")])
		assert.Equal(t, map[scheduler.JobRunIdentifier]scheduler.JobRunIdentifier{id("job-B"): id("job-A"), id("job-C"): id("job-B")}, gotPath.Pred)
		assert.Equal(t, map[scheduler.JobRunIdentifier]int{id("job-A"): 0, id("job-B"): 1, id("job-C"): 2}, gotPath.Level)
	})

	t.Run("linear chain damper=0.5 attenuates duration at each level", func(t *testing.T) {
		// topology: A -> B -> C, alpha=0.5
		// S(B) = base - dur_A*1.0 = base-20m        (level 1, damper still 1.0 at target level)
		// S(C) = base-20m - dur_B*0.5 = base-30m    (level 2, damper = 0.5)
		c := node("job-C", &base)
		b := node("job-B", &base, c)
		a := node("job-A", &base, b)
		durations := map[scheduler.JobName]*time.Duration{
			"job-A": dur(20 * time.Minute),
			"job-B": dur(20 * time.Minute),
			"job-C": dur(20 * time.Minute),
		}
		gotSLAs, gotPath := svc.CalculateInferredSLAs(root(a), durations, &base, scheduler.DamperFactor{Alpha: 0.5})

		assert.Equal(t, base, gotSLAs[id("job-A")])
		assert.Equal(t, base.Add(-20*time.Minute), gotSLAs[id("job-B")])
		assert.Equal(t, base.Add(-30*time.Minute), gotSLAs[id("job-C")])
		assert.Equal(t, map[scheduler.JobRunIdentifier]scheduler.JobRunIdentifier{id("job-B"): id("job-A"), id("job-C"): id("job-B")}, gotPath.Pred)
		assert.Equal(t, map[scheduler.JobRunIdentifier]int{id("job-A"): 0, id("job-B"): 1, id("job-C"): 2}, gotPath.Level)
	})

	t.Run("simple diamond: shared node gets the tightest inferred SLA", func(t *testing.T) {
		// topology: A -> {B, C}, both B and C -> D
		// dur_A=20m, dur_B=5m, dur_C=15m, damper=1.0
		// Via B (processed first in BFS): S(D) = base-20m - 5m  = base-25m
		// Via C (processed second):       S(D) = base-20m - 15m = base-35m  <- tighter, wins
		// winningPred[D] = C, winningLevel[D] = 2
		d := node("job-D", &base)
		b := node("job-B", &base, d)
		c := node("job-C", &base, d)
		a := node("job-A", &base, b, c)
		durations := map[scheduler.JobName]*time.Duration{
			"job-A": dur(20 * time.Minute),
			"job-B": dur(5 * time.Minute),
			"job-C": dur(15 * time.Minute),
			"job-D": dur(10 * time.Minute),
		}
		gotSLAs, gotPath := svc.CalculateInferredSLAs(root(a), durations, &base, scheduler.DamperFactor{Alpha: 1.0})

		assert.Equal(t, base, gotSLAs[id("job-A")])
		assert.Equal(t, base.Add(-20*time.Minute), gotSLAs[id("job-B")])
		assert.Equal(t, base.Add(-20*time.Minute), gotSLAs[id("job-C")])
		assert.Equal(t, base.Add(-35*time.Minute), gotSLAs[id("job-D")])
		assert.Equal(t, map[scheduler.JobRunIdentifier]scheduler.JobRunIdentifier{id("job-B"): id("job-A"), id("job-C"): id("job-A"), id("job-D"): id("job-C")}, gotPath.Pred)
		assert.Equal(t, map[scheduler.JobRunIdentifier]int{id("job-A"): 0, id("job-B"): 1, id("job-C"): 1, id("job-D"): 2}, gotPath.Level)
	})

	t.Run("asymmetric diamond: shared node anchored to the longest (bottleneck) path", func(t *testing.T) {
		// topology: A->{B,C}, B->D, C->F, F->D, D->E   (same as the breach detection suite)
		// dur: A=20m, B=15m, C=15m, F=10m, D=10m, E=5m, damper=1.0
		//
		// Paths to D:
		//   via B (level 2): S(D) = base-20m - 15m = base-35m
		//   via C-F (level 3): S(D) = base-35m - 10m = base-45m  <- tighter, wins
		// winningPred[D]=F, winningLevel[D]=3
		// S(E) = base-45m - 10m = base-55m, winningPred[E]=D, winningLevel[E]=4
		e := node("job-E", &base)
		d := node("job-D", &base, e)
		f := node("job-F", &base, d)
		b := node("job-B", &base, d)
		c := node("job-C", &base, f)
		a := node("job-A", &base, b, c)
		durations := map[scheduler.JobName]*time.Duration{
			"job-A": dur(20 * time.Minute),
			"job-B": dur(15 * time.Minute),
			"job-C": dur(15 * time.Minute),
			"job-F": dur(10 * time.Minute),
			"job-D": dur(10 * time.Minute),
			"job-E": dur(5 * time.Minute),
		}
		gotSLAs, gotPath := svc.CalculateInferredSLAs(root(a), durations, &base, scheduler.DamperFactor{Alpha: 1.0})

		assert.Equal(t, base, gotSLAs[id("job-A")])
		assert.Equal(t, base.Add(-20*time.Minute), gotSLAs[id("job-B")])
		assert.Equal(t, base.Add(-20*time.Minute), gotSLAs[id("job-C")])
		assert.Equal(t, base.Add(-35*time.Minute), gotSLAs[id("job-F")])
		assert.Equal(t, base.Add(-45*time.Minute), gotSLAs[id("job-D")])
		assert.Equal(t, base.Add(-55*time.Minute), gotSLAs[id("job-E")])
		assert.Equal(t, map[scheduler.JobRunIdentifier]scheduler.JobRunIdentifier{
			id("job-B"): id("job-A"),
			id("job-C"): id("job-A"),
			id("job-F"): id("job-C"),
			id("job-D"): id("job-F"),
			id("job-E"): id("job-D"),
		}, gotPath.Pred)
		assert.Equal(t, map[scheduler.JobRunIdentifier]int{
			id("job-A"): 0, id("job-B"): 1, id("job-C"): 1, id("job-F"): 2, id("job-D"): 3, id("job-E"): 4,
		}, gotPath.Level)
	})

	t.Run("job with missing duration stops propagation to its upstreams", func(t *testing.T) {
		// topology: A -> B -> C; B has no duration entry
		// S(B) = base-20m (A propagates to B), but B is skipped so C gets no SLA
		c := node("job-C", &base)
		b := node("job-B", &base, c)
		a := node("job-A", &base, b)
		durations := map[scheduler.JobName]*time.Duration{
			"job-A": dur(20 * time.Minute),
			// job-B intentionally missing
			"job-C": dur(10 * time.Minute),
		}
		gotSLAs, gotPath := svc.CalculateInferredSLAs(root(a), durations, &base, scheduler.DamperFactor{Alpha: 1.0})

		assert.Equal(t, base, gotSLAs[id("job-A")])
		assert.Equal(t, base.Add(-20*time.Minute), gotSLAs[id("job-B")])
		_, ok := gotSLAs[id("job-C")]
		assert.False(t, ok)
		assert.Equal(t, map[scheduler.JobRunIdentifier]scheduler.JobRunIdentifier{id("job-B"): id("job-A")}, gotPath.Pred)
		assert.Equal(t, map[scheduler.JobRunIdentifier]int{id("job-A"): 0, id("job-B"): 1}, gotPath.Level)
	})

	t.Run("upstream job with missing job runs should not calculate inferred SLA", func(t *testing.T) {
		// topology: A -> B -> C -> D, C does not have any associated job runs w.r.t job B dependency
		// S(C) is empty , then S(D) is empty also
		d := node("job-D", nil)
		c := node("job-C", nil, d)
		b := node("job-B", &base, c)
		a := node("job-A", &base, b)

		durations := map[scheduler.JobName]*time.Duration{
			"job-A": dur(20 * time.Minute),
			"job-B": dur(15 * time.Minute),
			"job-C": dur(10 * time.Minute),
			"job-D": dur(5 * time.Minute),
		}

		gotSLAs, gotPath := svc.CalculateInferredSLAs(root(a), durations, &base, scheduler.DamperFactor{Alpha: 1.0})

		assert.Equal(t, base, gotSLAs[id("job-A")])
		assert.Equal(t, base.Add(-20*time.Minute), gotSLAs[id("job-B")])
		_, cOk := gotSLAs[id("job-C")]
		assert.False(t, cOk)
		_, dOk := gotSLAs[id("job-D")]
		assert.False(t, dOk)
		assert.Equal(t, map[scheduler.JobRunIdentifier]scheduler.JobRunIdentifier{id("job-B"): id("job-A")}, gotPath.Pred)
		assert.Equal(t, map[scheduler.JobRunIdentifier]int{id("job-A"): 0, id("job-B"): 1}, gotPath.Level)
	})
}

// generateLineageWithSLAStates builds a linear chain jobNamePath[0] -> jobNamePath[1] -> ... and
// computes each job's inferred SLA-derived task start/end times. It returns both the lineage
// nodes and, keyed by job name, the *JobRunSummary each node's own run was recorded with - so
// callers can mutate specific runs afterward (e.g. to simulate a job not having started yet)
// without needing to know each node's own scheduled time or its immediate downstream identifier.
func generateLineageWithSLAStates(slaPredictorService *service.JobSLAPredictorService, jobDurations map[scheduler.JobName]*time.Duration, jobNamePath []scheduler.JobName, referenceTime time.Time, damperFactor scheduler.DamperFactor, targetSLA time.Time) (map[scheduler.JobName]*scheduler.JobLineageSummary, map[scheduler.JobName]*scheduler.JobRunSummary) {
	// get hour from now for simulation purpose, we set scheduledAt to be now
	scheduledAt := referenceTime.Add(1 * time.Minute).Truncate(time.Minute)
	interval := fmt.Sprintf("%d %d * * *", scheduledAt.Minute(), scheduledAt.Hour()) // daily
	jobNameTarget := jobNamePath[0]

	jobTargetLineageMap := map[scheduler.JobName]*scheduler.JobLineageSummary{}
	runsByJobName := map[scheduler.JobName]*scheduler.JobRunSummary{}

	for _, jobName := range jobNamePath {
		currentJobLineage := &scheduler.JobLineageSummary{
			JobName:          jobName,
			ScheduleInterval: interval,
			IsEnabled:        true,
		}

		jobTargetLineageMap[jobName] = currentJobLineage
		runsByJobName[jobName] = &scheduler.JobRunSummary{ScheduledAt: scheduledAt}
	}
	jobTargetLineageMap[jobNameTarget].RecordOwnRun(runsByJobName[jobNameTarget])

	for i := len(jobNamePath) - 2; i >= 0; i-- {
		currentJobName := jobNamePath[i]
		upstreamJobName := jobNamePath[i+1]

		currentJobLineage := jobTargetLineageMap[currentJobName]
		upstreamJobLineage := jobTargetLineageMap[upstreamJobName]

		currentJobLineage.AddUpstream(upstreamJobLineage)
		// upstreamJobLineage's run is required by currentJobLineage's own occurrence - every node
		// here shares the same scheduledAt, so that's the identifier's ScheduledAt too.
		upstreamJobLineage.RecordRun(scheduler.JobRunIdentifier{JobName: currentJobName, ScheduledAt: scheduledAt}, runsByJobName[upstreamJobName])
	}

	slaStates, _ := slaPredictorService.CalculateInferredSLAs(jobTargetLineageMap[jobNameTarget], jobDurations, &targetSLA, damperFactor)

	for _, jobName := range jobNamePath {
		currentInferredSLA := slaStates[scheduler.JobRunIdentifier{JobName: jobName, ScheduledAt: scheduledAt}]
		currentEstimatedDuration := jobDurations[jobName]

		taskStartTime := currentInferredSLA.Add(-*currentEstimatedDuration).Add(-1 * time.Minute) // started 1 min earlier than must start time
		taskEndTime := currentInferredSLA.Add(-1 * time.Minute)                                   // ended 1 min earlier than inferred SLA

		jobRunSummary := runsByJobName[jobName]
		jobRunSummary.TaskStartTime = &taskStartTime
		jobRunSummary.TaskEndTime = &taskEndTime
		jobRunSummary.JobEndTime = &taskEndTime
	}

	return jobTargetLineageMap, runsByJobName
}

// JobLineageFetcher is an autogenerated mock type for the JobLineageFetcher type
type JobLineageFetcher struct {
	mock.Mock
}

// GetJobLineage provides a mock function with given fields: ctx, jobSchedules
func (_m *JobLineageFetcher) GetJobLineage(ctx context.Context, jobSchedules map[scheduler.JobName]*scheduler.JobSchedule, validLineageIntervalInHours int) (map[scheduler.JobName]*scheduler.JobLineageSummary, error) {
	ret := _m.Called(ctx, jobSchedules, validLineageIntervalInHours)

	if len(ret) == 0 {
		panic("no return value specified for GetJobLineage")
	}

	var r0 map[scheduler.JobName]*scheduler.JobLineageSummary
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context, map[scheduler.JobName]*scheduler.JobSchedule, int) (map[scheduler.JobName]*scheduler.JobLineageSummary, error)); ok {
		return rf(ctx, jobSchedules, validLineageIntervalInHours)
	}
	if rf, ok := ret.Get(0).(func(context.Context, map[scheduler.JobName]*scheduler.JobSchedule, int) map[scheduler.JobName]*scheduler.JobLineageSummary); ok {
		r0 = rf(ctx, jobSchedules, validLineageIntervalInHours)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(map[scheduler.JobName]*scheduler.JobLineageSummary)
		}
	}

	if rf, ok := ret.Get(1).(func(context.Context, map[scheduler.JobName]*scheduler.JobSchedule, int) error); ok {
		r1 = rf(ctx, jobSchedules, validLineageIntervalInHours)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// NewJobLineageFetcher creates a new instance of JobLineageFetcher. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewJobLineageFetcher(t interface {
	mock.TestingT
	Cleanup(func())
},
) *JobLineageFetcher {
	mock := &JobLineageFetcher{}
	mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}

// DurationEstimator is an autogenerated mock type for the DurationEstimator type
type DurationEstimator struct {
	mock.Mock
}

// GetPercentileDurationByJobNames provides a mock function with given fields: ctx, referenceTime, jobNames
func (_m *DurationEstimator) GetPercentileDurationByJobNames(ctx context.Context, referenceTime time.Time, jobNames []scheduler.JobName) (map[scheduler.JobName]*time.Duration, error) {
	ret := _m.Called(ctx, referenceTime, jobNames)

	if len(ret) == 0 {
		panic("no return value specified for GetPercentileDurationByJobNames")
	}

	var r0 map[scheduler.JobName]*time.Duration
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context, time.Time, []scheduler.JobName) (map[scheduler.JobName]*time.Duration, error)); ok {
		return rf(ctx, referenceTime, jobNames)
	}
	if rf, ok := ret.Get(0).(func(context.Context, time.Time, []scheduler.JobName) map[scheduler.JobName]*time.Duration); ok {
		r0 = rf(ctx, referenceTime, jobNames)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(map[scheduler.JobName]*time.Duration)
		}
	}

	if rf, ok := ret.Get(1).(func(context.Context, time.Time, []scheduler.JobName) error); ok {
		r1 = rf(ctx, referenceTime, jobNames)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// GetPercentileDurationByJobNamesByHookName provides a mock function with given fields: ctx, referenceTime, jobNames, hookNames
func (_m *DurationEstimator) GetPercentileDurationByJobNamesByHookName(ctx context.Context, referenceTime time.Time, jobNames []scheduler.JobName, hookNames []string) (map[scheduler.JobName]*time.Duration, error) {
	ret := _m.Called(ctx, referenceTime, jobNames, hookNames)

	if len(ret) == 0 {
		panic("no return value specified for GetPercentileDurationByJobNamesByHookName")
	}

	var r0 map[scheduler.JobName]*time.Duration
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context, time.Time, []scheduler.JobName, []string) (map[scheduler.JobName]*time.Duration, error)); ok {
		return rf(ctx, referenceTime, jobNames, hookNames)
	}
	if rf, ok := ret.Get(0).(func(context.Context, time.Time, []scheduler.JobName, []string) map[scheduler.JobName]*time.Duration); ok {
		r0 = rf(ctx, referenceTime, jobNames, hookNames)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(map[scheduler.JobName]*time.Duration)
		}
	}

	if rf, ok := ret.Get(1).(func(context.Context, time.Time, []scheduler.JobName, []string) error); ok {
		r1 = rf(ctx, referenceTime, jobNames, hookNames)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// GetPercentileDurationByJobNamesByTask provides a mock function with given fields: ctx, referenceTime, jobNames
func (_m *DurationEstimator) GetPercentileDurationByJobNamesByTask(ctx context.Context, referenceTime time.Time, jobNames []scheduler.JobName) (map[scheduler.JobName]*time.Duration, error) {
	ret := _m.Called(ctx, referenceTime, jobNames)

	if len(ret) == 0 {
		panic("no return value specified for GetPercentileDurationByJobNamesByTask")
	}

	var r0 map[scheduler.JobName]*time.Duration
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context, time.Time, []scheduler.JobName) (map[scheduler.JobName]*time.Duration, error)); ok {
		return rf(ctx, referenceTime, jobNames)
	}
	if rf, ok := ret.Get(0).(func(context.Context, time.Time, []scheduler.JobName) map[scheduler.JobName]*time.Duration); ok {
		r0 = rf(ctx, referenceTime, jobNames)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(map[scheduler.JobName]*time.Duration)
		}
	}

	if rf, ok := ret.Get(1).(func(context.Context, time.Time, []scheduler.JobName) error); ok {
		r1 = rf(ctx, referenceTime, jobNames)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// NewDurationEstimator creates a new instance of DurationEstimator. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewDurationEstimator(t interface {
	mock.TestingT
	Cleanup(func())
},
) *DurationEstimator {
	mock := &DurationEstimator{}
	mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}

// JobDetailsGetter is an autogenerated mock type for the JobDetailsGetter type
type JobDetailsGetter struct {
	mock.Mock
}

// GetJobs provides a mock function with given fields: ctx, projectName, jobs
func (_m *JobDetailsGetter) GetJobs(ctx context.Context, projectName tenant.ProjectName, jobs []string) ([]*scheduler.JobWithDetails, error) {
	ret := _m.Called(ctx, projectName, jobs)

	if len(ret) == 0 {
		panic("no return value specified for GetJobs")
	}

	var r0 []*scheduler.JobWithDetails
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context, tenant.ProjectName, []string) ([]*scheduler.JobWithDetails, error)); ok {
		return rf(ctx, projectName, jobs)
	}
	if rf, ok := ret.Get(0).(func(context.Context, tenant.ProjectName, []string) []*scheduler.JobWithDetails); ok {
		r0 = rf(ctx, projectName, jobs)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]*scheduler.JobWithDetails)
		}
	}

	if rf, ok := ret.Get(1).(func(context.Context, tenant.ProjectName, []string) error); ok {
		r1 = rf(ctx, projectName, jobs)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// GetJobsByLabels provides a mock function with given fields: ctx, projectName, labels
func (_m *JobDetailsGetter) GetJobsByLabels(ctx context.Context, projectName tenant.ProjectName, labels map[string]string) ([]*scheduler.JobWithDetails, error) {
	ret := _m.Called(ctx, projectName, labels)

	if len(ret) == 0 {
		panic("no return value specified for GetJobsByLabels")
	}

	var r0 []*scheduler.JobWithDetails
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context, tenant.ProjectName, map[string]string) ([]*scheduler.JobWithDetails, error)); ok {
		return rf(ctx, projectName, labels)
	}
	if rf, ok := ret.Get(0).(func(context.Context, tenant.ProjectName, map[string]string) []*scheduler.JobWithDetails); ok {
		r0 = rf(ctx, projectName, labels)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]*scheduler.JobWithDetails)
		}
	}

	if rf, ok := ret.Get(1).(func(context.Context, tenant.ProjectName, map[string]string) error); ok {
		r1 = rf(ctx, projectName, labels)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// GetJobsByLabelsMultiValue provides a mock function with given fields: ctx, projectName, labels
func (_m *JobDetailsGetter) GetJobsByLabelsMultiValue(ctx context.Context, projectName tenant.ProjectName, labels map[string][]string) ([]*scheduler.JobWithDetails, error) {
	ret := _m.Called(ctx, projectName, labels)

	if len(ret) == 0 {
		panic("no return value specified for GetJobsByLabelsMultiValue")
	}

	var r0 []*scheduler.JobWithDetails
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context, tenant.ProjectName, map[string][]string) ([]*scheduler.JobWithDetails, error)); ok {
		return rf(ctx, projectName, labels)
	}
	if rf, ok := ret.Get(0).(func(context.Context, tenant.ProjectName, map[string][]string) []*scheduler.JobWithDetails); ok {
		r0 = rf(ctx, projectName, labels)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]*scheduler.JobWithDetails)
		}
	}

	if rf, ok := ret.Get(1).(func(context.Context, tenant.ProjectName, map[string][]string) error); ok {
		r1 = rf(ctx, projectName, labels)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// NewJobDetailsGetter creates a new instance of JobDetailsGetter. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewJobDetailsGetter(t interface {
	mock.TestingT
	Cleanup(func())
},
) *JobDetailsGetter {
	mock := &JobDetailsGetter{}
	mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}

// ScheduledChangeGetter is an autogenerated mock type for the ScheduledChangeGetter type
type ScheduledChangeGetter struct {
	mock.Mock
}

// GetRecentScheduleChange provides a mock function with given fields: ctx, jobName, tnnt, startTime
func (_m *ScheduledChangeGetter) GetRecentScheduleChange(ctx context.Context, jobName scheduler.JobName, tnnt tenant.Tenant, startTime time.Time) (string, error) {
	ret := _m.Called(ctx, jobName, tnnt, startTime)

	if len(ret) == 0 {
		panic("no return value specified for GetRecentScheduleChange")
	}

	var r0 string
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context, scheduler.JobName, tenant.Tenant, time.Time) (string, error)); ok {
		return rf(ctx, jobName, tnnt, startTime)
	}
	if rf, ok := ret.Get(0).(func(context.Context, scheduler.JobName, tenant.Tenant, time.Time) string); ok {
		r0 = rf(ctx, jobName, tnnt, startTime)
	} else {
		r0 = ret.Get(0).(string)
	}

	if rf, ok := ret.Get(1).(func(context.Context, scheduler.JobName, tenant.Tenant, time.Time) error); ok {
		r1 = rf(ctx, jobName, tnnt, startTime)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// NewScheduledChangeGetter creates a new instance of ScheduledChangeGetter. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewScheduledChangeGetter(t interface {
	mock.TestingT
	Cleanup(func())
},
) *ScheduledChangeGetter {
	mock := &ScheduledChangeGetter{}
	mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
