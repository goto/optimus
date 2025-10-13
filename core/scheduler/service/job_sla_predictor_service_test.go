package service_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/goto/salt/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/goto/optimus/core/scheduler"
	"github.com/goto/optimus/core/scheduler/service"
	"github.com/goto/optimus/core/tenant"
)

func TestIdentifySLABreaches(t *testing.T) {
	ctx := context.Background()
	l := log.NewNoop()

	t.Run("given no jobs, return no breaches", func(t *testing.T) {
		// given
		jobLineageFetcher := NewJobLineageFetcher(t)
		durationEstimator := NewDurationEstimator(t)
		jobDetailsGetter := NewJobDetailsGetter(t)
		jobSLAPredictorService := service.NewJobSLAPredictorService(l, jobLineageFetcher, durationEstimator, jobDetailsGetter, nil, nil)

		projectName := tenant.ProjectName("project-a")
		nextScheduledRangeInHours := 10 * time.Hour
		jobNames := []scheduler.JobName{}
		labels := map[string]string{}

		// when
		jobBreachRootCause, err := jobSLAPredictorService.IdentifySLABreaches(ctx, projectName, nextScheduledRangeInHours, jobNames, labels, false, "")
		// then
		assert.NoError(t, err)
		assert.Len(t, jobBreachRootCause, 0)
	})

	t.Run("given no jobs found, return no breaches", func(t *testing.T) {
		// given
		jobLineageFetcher := NewJobLineageFetcher(t)
		durationEstimator := NewDurationEstimator(t)
		jobDetailsGetter := NewJobDetailsGetter(t)
		jobSLAPredictorService := service.NewJobSLAPredictorService(l, jobLineageFetcher, durationEstimator, jobDetailsGetter, nil, nil)

		projectName := tenant.ProjectName("project-a")
		nextScheduledRangeInHours := 10 * time.Hour
		jobNames := []scheduler.JobName{scheduler.JobName("job-A")}
		labels := map[string]string{}

		jobDetailsGetter.On("GetJobs", ctx, projectName, []string{"job-A"}).Return([]*scheduler.JobWithDetails{}, nil).Once()

		// when
		jobBreachRootCause, err := jobSLAPredictorService.IdentifySLABreaches(ctx, projectName, nextScheduledRangeInHours, jobNames, labels, false, "")

		// then
		assert.NoError(t, err)
		assert.Len(t, jobBreachRootCause, 0)
	})

	t.Run("given no jobs found given labels, return no breaches", func(t *testing.T) {
		// given
		jobLineageFetcher := NewJobLineageFetcher(t)
		durationEstimator := NewDurationEstimator(t)
		jobDetailsGetter := NewJobDetailsGetter(t)
		jobSLAPredictorService := service.NewJobSLAPredictorService(l, jobLineageFetcher, durationEstimator, jobDetailsGetter, nil, nil)

		projectName := tenant.ProjectName("project-a")
		nextScheduledRangeInHours := 10 * time.Hour
		jobNames := []scheduler.JobName{}
		labels := map[string]string{"criticality": "critical"}

		jobDetailsGetter.On("GetJobsByLabels", ctx, projectName, labels, false, "").Return([]*scheduler.JobWithDetails{}, nil).Once()

		// when
		jobBreachRootCause, err := jobSLAPredictorService.IdentifySLABreaches(ctx, projectName, nextScheduledRangeInHours, jobNames, labels, false, "")

		// then
		assert.NoError(t, err)
		assert.Len(t, jobBreachRootCause, 0)
	})

	t.Run("given error fetching jobs, return error", func(t *testing.T) {
		// given
		jobLineageFetcher := NewJobLineageFetcher(t)
		durationEstimator := NewDurationEstimator(t)
		jobDetailsGetter := NewJobDetailsGetter(t)
		jobSLAPredictorService := service.NewJobSLAPredictorService(l, jobLineageFetcher, durationEstimator, jobDetailsGetter, nil, nil)

		projectName := tenant.ProjectName("project-a")
		nextScheduledRangeInHours := 10 * time.Hour
		jobNames := []scheduler.JobName{scheduler.JobName("job-A")}
		labels := map[string]string{}

		jobDetailsGetter.On("GetJobs", ctx, projectName, []string{"job-A"}).Return(nil, assert.AnError).Once()

		// when
		jobBreachRootCause, err := jobSLAPredictorService.IdentifySLABreaches(ctx, projectName, nextScheduledRangeInHours, jobNames, labels, false, "")

		// then
		assert.Error(t, err)
		assert.Len(t, jobBreachRootCause, 0)
	})

	t.Run("given jobs without schedule, return no breaches", func(t *testing.T) {
		// given
		jobLineageFetcher := NewJobLineageFetcher(t)
		durationEstimator := NewDurationEstimator(t)
		jobDetailsGetter := NewJobDetailsGetter(t)
		jobSLAPredictorService := service.NewJobSLAPredictorService(l, jobLineageFetcher, durationEstimator, jobDetailsGetter, nil, nil)

		projectName := tenant.ProjectName("project-a")
		nextScheduledRangeInHours := 10 * time.Hour
		jobNames := []scheduler.JobName{scheduler.JobName("job-A")}
		labels := map[string]string{}

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
		jobBreachRootCause, err := jobSLAPredictorService.IdentifySLABreaches(ctx, projectName, nextScheduledRangeInHours, jobNames, labels, false, "")

		// then
		assert.NoError(t, err)
		assert.Len(t, jobBreachRootCause, 0)
	})

	t.Run("given jobs without SLA, return no breaches", func(t *testing.T) {
		// given
		jobLineageFetcher := NewJobLineageFetcher(t)
		durationEstimator := NewDurationEstimator(t)
		jobDetailsGetter := NewJobDetailsGetter(t)
		jobSLAPredictorService := service.NewJobSLAPredictorService(l, jobLineageFetcher, durationEstimator, jobDetailsGetter, nil, nil)

		projectName := tenant.ProjectName("project-a")
		nextScheduledRangeInHours := 10 * time.Hour
		jobNames := []scheduler.JobName{scheduler.JobName("job-A")}
		labels := map[string]string{}

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
		jobBreachRootCause, err := jobSLAPredictorService.IdentifySLABreaches(ctx, projectName, nextScheduledRangeInHours, jobNames, labels, false, "")

		// then
		assert.NoError(t, err)
		assert.Len(t, jobBreachRootCause, 0)
	})

	t.Run("given jobs with SLA but not in range of next schedule range, return no breaches", func(t *testing.T) {
		// given
		jobLineageFetcher := NewJobLineageFetcher(t)
		durationEstimator := NewDurationEstimator(t)
		jobDetailsGetter := NewJobDetailsGetter(t)
		jobSLAPredictorService := service.NewJobSLAPredictorService(l, jobLineageFetcher, durationEstimator, jobDetailsGetter, nil, nil)

		projectName := tenant.ProjectName("project-a")
		nextScheduledRangeInHours := 10 * time.Hour
		jobNames := []scheduler.JobName{scheduler.JobName("job-A")}
		labels := map[string]string{}

		referenceTime := time.Now().UTC()
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
		jobBreachRootCause, err := jobSLAPredictorService.IdentifySLABreaches(ctx, projectName, nextScheduledRangeInHours, jobNames, labels, false, "")

		// then
		assert.NoError(t, err)
		assert.Len(t, jobBreachRootCause, 0)
	})

	t.Run("given error fetching job lineage, return error", func(t *testing.T) {
		// given
		jobLineageFetcher := NewJobLineageFetcher(t)
		durationEstimator := NewDurationEstimator(t)
		jobDetailsGetter := NewJobDetailsGetter(t)
		jobSLAPredictorService := service.NewJobSLAPredictorService(l, jobLineageFetcher, durationEstimator, jobDetailsGetter, nil, nil)

		projectName := tenant.ProjectName("project-a")
		nextScheduledRangeInHours := 10 * time.Hour
		jobNames := []scheduler.JobName{scheduler.JobName("job-A")}
		labels := map[string]string{}

		referenceTime := time.Now().UTC()
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
		jobLineageFetcher.On("GetJobLineage", ctx, mock.Anything).Return(nil, assert.AnError).Once()

		// when
		jobBreachRootCause, err := jobSLAPredictorService.IdentifySLABreaches(ctx, projectName, nextScheduledRangeInHours, jobNames, labels, false, "")

		// then
		assert.Error(t, err)
		assert.Len(t, jobBreachRootCause, 0)
	})

	t.Run("given error get P95 duration, return error", func(t *testing.T) {
		// given
		jobLineageFetcher := NewJobLineageFetcher(t)
		durationEstimator := NewDurationEstimator(t)
		jobDetailsGetter := NewJobDetailsGetter(t)
		jobSLAPredictorService := service.NewJobSLAPredictorService(l, jobLineageFetcher, durationEstimator, jobDetailsGetter, nil, nil)

		projectName := tenant.ProjectName("project-a")
		nextScheduledRangeInHours := 10 * time.Hour
		jobNames := []scheduler.JobName{scheduler.JobName("job-A")}
		labels := map[string]string{}

		referenceTime := time.Now().UTC()
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
			JobRuns:          map[string]*scheduler.JobRunSummary{},
			Upstreams:        []*scheduler.JobLineageSummary{},
		}

		jobDetailsGetter.On("GetJobs", ctx, projectName, []string{"job-A"}).Return([]*scheduler.JobWithDetails{jobA}, nil).Once()
		jobLineageFetcher.On("GetJobLineage", ctx, mock.MatchedBy(func(jobSchedules []*scheduler.JobSchedule) bool {
			return len(jobSchedules) == 1 && jobSchedules[0].JobName == jobASchedule.JobName && jobSchedules[0].ScheduledAt.Equal(jobASchedule.ScheduledAt)
		})).Return(map[scheduler.JobName]*scheduler.JobLineageSummary{
			jobASchedule.JobName: jobALineage,
		}, nil)
		durationEstimator.On("GetPercentileDurationByJobNames", ctx, []scheduler.JobName{jobA.Name}).Return(nil, assert.AnError).Once()

		// when
		jobBreachRootCause, err := jobSLAPredictorService.IdentifySLABreaches(ctx, projectName, nextScheduledRangeInHours, jobNames, labels, false, "")
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
		jobSLAPredictorService := service.NewJobSLAPredictorService(l, jobLineageFetcher, durationEstimator, jobDetailsGetter, nil, nil)

		projectName := tenant.ProjectName("project-a")
		nextScheduledRangeInHours := 10 * time.Hour
		jobNames := []scheduler.JobName{scheduler.JobName("job-A")}
		labels := map[string]string{}

		referenceTime := time.Now().UTC()
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
			JobRuns: map[string]*scheduler.JobRunSummary{
				scheduledAt.String(): {
					ScheduledAt: scheduledAt,
				},
			},
			Upstreams: []*scheduler.JobLineageSummary{},
		}
		jobBTaskStartTime := scheduledAt.Add(-10 * time.Minute)
		jobBLineage := &scheduler.JobLineageSummary{
			JobName:          "job-B",
			ScheduleInterval: interval,
			JobRuns: map[string]*scheduler.JobRunSummary{
				scheduledAt.Add(-15 * time.Minute).String(): {
					ScheduledAt:   scheduledAt.Add(-15 * time.Minute),
					TaskStartTime: &jobBTaskStartTime,
				},
			},
			Upstreams: []*scheduler.JobLineageSummary{},
		}
		jobCTaskStartTime := scheduledAt.Add(-20 * time.Minute)
		jobCTaskEndTime := scheduledAt.Add(-10 * time.Minute)
		jobCLineage := &scheduler.JobLineageSummary{
			JobName:          "job-C",
			ScheduleInterval: interval,
			JobRuns: map[string]*scheduler.JobRunSummary{
				scheduledAt.Add(-25 * time.Minute).String(): {
					ScheduledAt:   scheduledAt.Add(-25 * time.Minute),
					TaskStartTime: &jobCTaskStartTime,
					TaskEndTime:   &jobCTaskEndTime,
					JobEndTime:    &jobCTaskEndTime,
				},
			},
			Upstreams: []*scheduler.JobLineageSummary{},
		}
		jobALineage.Upstreams = []*scheduler.JobLineageSummary{jobBLineage}
		jobBLineage.Upstreams = []*scheduler.JobLineageSummary{jobCLineage}

		jobDetailsGetter.On("GetJobs", ctx, projectName, []string{"job-A"}).Return([]*scheduler.JobWithDetails{jobA}, nil).Once()
		jobLineageFetcher.On("GetJobLineage", ctx, []*scheduler.JobSchedule{jobASchedule}).Return(map[scheduler.JobName]*scheduler.JobLineageSummary{
			jobASchedule.JobName: jobALineage,
		}, nil).Once()
		durationEstimator.On("GetPercentileDurationByJobNames", ctx, mock.MatchedBy(func(jobNames []scheduler.JobName) bool {
			return assert.ElementsMatch(t, []scheduler.JobName{"job-A", "job-B", "job-C"}, jobNames)
		})).Return(map[scheduler.JobName]*time.Duration{
			"job-A": func() *time.Duration { d := 20 * time.Minute; return &d }(),
			"job-B": func() *time.Duration { d := 15 * time.Minute; return &d }(),
			"job-C": func() *time.Duration { d := 10 * time.Minute; return &d }(),
		}, nil).Once()

		// when
		jobBreachRootCause, err := jobSLAPredictorService.IdentifySLABreaches(ctx, projectName, nextScheduledRangeInHours, jobNames, labels, false, "")

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
		jobSLAPredictorService := service.NewJobSLAPredictorService(l, jobLineageFetcher, durationEstimator, jobDetailsGetter, nil, nil)

		projectName := tenant.ProjectName("project-a")
		nextScheduledRangeInHours := 10 * time.Hour
		jobNames := []scheduler.JobName{scheduler.JobName("job-A")}
		labels := map[string]string{}

		referenceTime := time.Now().UTC()
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
			JobRuns: map[string]*scheduler.JobRunSummary{
				scheduledAt.String(): {
					ScheduledAt: scheduledAt,
				},
			},
			Upstreams: []*scheduler.JobLineageSummary{},
		}
		jobBLineage := &scheduler.JobLineageSummary{
			JobName:          "job-B",
			ScheduleInterval: interval,
			JobRuns: map[string]*scheduler.JobRunSummary{
				scheduledAt.Add(-15 * time.Minute).String(): {
					ScheduledAt: scheduledAt.Add(-15 * time.Minute),
				},
			},
			Upstreams: []*scheduler.JobLineageSummary{},
		}
		jobCTaskStartTime := scheduledAt.Add(-20 * time.Minute)
		jobCLineage := &scheduler.JobLineageSummary{
			JobName:          "job-C",
			ScheduleInterval: interval,
			JobRuns: map[string]*scheduler.JobRunSummary{
				scheduledAt.Add(-25 * time.Minute).String(): {
					ScheduledAt:   scheduledAt.Add(-25 * time.Minute),
					TaskStartTime: &jobCTaskStartTime, // job-C is running, but not done yet
				},
			},
			Upstreams: []*scheduler.JobLineageSummary{},
		}
		jobALineage.Upstreams = []*scheduler.JobLineageSummary{jobBLineage}
		jobBLineage.Upstreams = []*scheduler.JobLineageSummary{jobCLineage}

		jobDetailsGetter.On("GetJobs", ctx, projectName, []string{"job-A"}).Return([]*scheduler.JobWithDetails{jobA}, nil).Once()
		jobLineageFetcher.On("GetJobLineage", ctx, []*scheduler.JobSchedule{jobASchedule}).Return(map[scheduler.JobName]*scheduler.JobLineageSummary{
			jobASchedule.JobName: jobALineage,
		}, nil).Once()
		durationEstimator.On("GetPercentileDurationByJobNames", ctx, mock.MatchedBy(func(jobNames []scheduler.JobName) bool {
			return assert.ElementsMatch(t, []scheduler.JobName{"job-A", "job-B", "job-C"}, jobNames)
		})).Return(map[scheduler.JobName]*time.Duration{
			"job-A": func() *time.Duration { d := 20 * time.Minute; return &d }(),
			"job-B": func() *time.Duration { d := 15 * time.Minute; return &d }(),
			"job-C": func() *time.Duration { d := 10 * time.Minute; return &d }(),
		}, nil).Once()

		// when
		jobBreachRootCause, err := jobSLAPredictorService.IdentifySLABreaches(ctx, projectName, nextScheduledRangeInHours, jobNames, labels, false, "")

		// then
		assert.NoError(t, err)
		assert.Len(t, jobBreachRootCause, 1)
		assert.Len(t, jobBreachRootCause["job-A"], 1)
		assert.Equal(t, scheduler.JobName("job-C"), jobBreachRootCause["job-A"]["job-C"].JobName)
		assert.Equal(t, 2, jobBreachRootCause["job-A"]["job-C"].RelativeLevel)
		assert.Equal(t, service.SLABreachCauseRunningLate, jobBreachRootCause["job-A"]["job-C"].Status)
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
		jobSLAPredictorService := service.NewJobSLAPredictorService(l, jobLineageFetcher, durationEstimator, jobDetailsGetter, nil, nil)

		projectName := tenant.ProjectName("project-a")
		nextScheduledRangeInHours := 10 * time.Hour
		jobNames := []scheduler.JobName{scheduler.JobName("job-A")}
		labels := map[string]string{}

		referenceTime := time.Now().UTC()
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
			JobRuns: map[string]*scheduler.JobRunSummary{
				scheduledAt.String(): {
					ScheduledAt: scheduledAt,
				},
			},
			Upstreams: []*scheduler.JobLineageSummary{},
		}
		jobBLineage := &scheduler.JobLineageSummary{
			JobName:          "job-B",
			ScheduleInterval: interval,
			JobRuns: map[string]*scheduler.JobRunSummary{
				scheduledAt.Add(-15 * time.Minute).String(): {
					ScheduledAt: scheduledAt.Add(-15 * time.Minute), // job-B is not started yet, it should have started 5 mins ago
				},
			},
			Upstreams: []*scheduler.JobLineageSummary{},
		}
		jobCTaskStartTime := scheduledAt.Add(-20 * time.Minute)
		jobCTaskEndTime := scheduledAt.Add(-10 * time.Minute)
		jobCLineage := &scheduler.JobLineageSummary{
			JobName:          "job-C",
			ScheduleInterval: interval,
			JobRuns: map[string]*scheduler.JobRunSummary{
				scheduledAt.Add(-25 * time.Minute).String(): {
					ScheduledAt:   scheduledAt.Add(-25 * time.Minute),
					TaskStartTime: &jobCTaskStartTime,
					TaskEndTime:   &jobCTaskEndTime,
					JobEndTime:    &jobCTaskEndTime,
				},
			},
			Upstreams: []*scheduler.JobLineageSummary{},
		}
		jobALineage.Upstreams = []*scheduler.JobLineageSummary{jobBLineage}
		jobBLineage.Upstreams = []*scheduler.JobLineageSummary{jobCLineage}

		jobDetailsGetter.On("GetJobs", ctx, projectName, []string{"job-A"}).Return([]*scheduler.JobWithDetails{jobA}, nil).Once()
		jobLineageFetcher.On("GetJobLineage", ctx, []*scheduler.JobSchedule{jobASchedule}).Return(map[scheduler.JobName]*scheduler.JobLineageSummary{
			jobASchedule.JobName: jobALineage,
		}, nil).Once()
		durationEstimator.On("GetPercentileDurationByJobNames", ctx, mock.MatchedBy(func(jobNames []scheduler.JobName) bool {
			return assert.ElementsMatch(t, []scheduler.JobName{"job-A", "job-B", "job-C"}, jobNames)
		})).Return(map[scheduler.JobName]*time.Duration{
			"job-A": func() *time.Duration { d := 20 * time.Minute; return &d }(),
			"job-B": func() *time.Duration { d := 15 * time.Minute; return &d }(),
			"job-C": func() *time.Duration { d := 10 * time.Minute; return &d }(),
		}, nil).Once()

		// when
		jobBreachRootCause, err := jobSLAPredictorService.IdentifySLABreaches(ctx, projectName, nextScheduledRangeInHours, jobNames, labels, false, "")

		// then
		assert.NoError(t, err)
		assert.Len(t, jobBreachRootCause, 1)
		assert.Len(t, jobBreachRootCause["job-A"], 1)
		assert.Equal(t, scheduler.JobName("job-B"), jobBreachRootCause["job-A"]["job-B"].JobName)
		assert.Equal(t, 1, jobBreachRootCause["job-A"]["job-B"].RelativeLevel)
		assert.Equal(t, service.SLABreachCauseNotStarted, jobBreachRootCause["job-A"]["job-B"].Status)
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
		jobSLAPredictorService := service.NewJobSLAPredictorService(l, jobLineageFetcher, durationEstimator, jobDetailsGetter, nil, nil)

		projectName := tenant.ProjectName("project-a")
		nextScheduledRangeInHours := 10 * time.Hour
		jobNames := []scheduler.JobName{scheduler.JobName("job-A1"), scheduler.JobName("job-A2")}
		labels := map[string]string{}

		referenceTime := time.Now().UTC()
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
			JobRuns: map[string]*scheduler.JobRunSummary{
				scheduledAt.String(): {
					ScheduledAt: scheduledAt,
				},
			},
			Upstreams: []*scheduler.JobLineageSummary{},
		}
		jobA2Lineage := &scheduler.JobLineageSummary{
			JobName:          "job-A2",
			ScheduleInterval: interval,
			JobRuns: map[string]*scheduler.JobRunSummary{
				scheduledAt.String(): {
					ScheduledAt: scheduledAt,
				},
			},
			Upstreams: []*scheduler.JobLineageSummary{},
		}
		jobBLineage := &scheduler.JobLineageSummary{
			JobName:          "job-B",
			ScheduleInterval: interval,
			JobRuns: map[string]*scheduler.JobRunSummary{
				scheduledAt.Add(-15 * time.Minute).String(): {
					ScheduledAt: scheduledAt.Add(-15 * time.Minute),
				},
			},
			Upstreams: []*scheduler.JobLineageSummary{},
		}
		jobCTaskStartTime := scheduledAt.Add(-20 * time.Minute)
		jobCLineage := &scheduler.JobLineageSummary{
			JobName:          "job-C",
			ScheduleInterval: interval,
			JobRuns: map[string]*scheduler.JobRunSummary{
				scheduledAt.Add(-25 * time.Minute).String(): {
					ScheduledAt:   scheduledAt.Add(-25 * time.Minute),
					TaskStartTime: &jobCTaskStartTime, // job-C is running, but not done yet
				},
			},
			Upstreams: []*scheduler.JobLineageSummary{},
		}
		jobA1Lineage.Upstreams = []*scheduler.JobLineageSummary{jobBLineage}
		jobA2Lineage.Upstreams = []*scheduler.JobLineageSummary{jobCLineage}
		jobBLineage.Upstreams = []*scheduler.JobLineageSummary{jobCLineage}

		jobDetailsGetter.On("GetJobs", ctx, projectName, []string{"job-A1", "job-A2"}).Return([]*scheduler.JobWithDetails{jobA1, jobA2}, nil).Once()
		jobLineageFetcher.On("GetJobLineage", ctx, []*scheduler.JobSchedule{jobASchedule1, jobASchedule2}).Return(map[scheduler.JobName]*scheduler.JobLineageSummary{
			jobASchedule1.JobName: jobA1Lineage,
			jobASchedule2.JobName: jobA2Lineage,
		}, nil).Once()
		durationEstimator.On("GetPercentileDurationByJobNames", ctx, mock.MatchedBy(func(jobNames []scheduler.JobName) bool {
			return assert.ElementsMatch(t, []scheduler.JobName{"job-A1", "job-A2", "job-B", "job-C"}, jobNames)
		})).Return(map[scheduler.JobName]*time.Duration{
			"job-A1": func() *time.Duration { d := 20 * time.Minute; return &d }(), // target sla now + 30 mins
			"job-A2": func() *time.Duration { d := 25 * time.Minute; return &d }(), // target sla now + 30 mins
			"job-B":  func() *time.Duration { d := 15 * time.Minute; return &d }(),
			"job-C":  func() *time.Duration { d := 10 * time.Minute; return &d }(),
		}, nil).Once()

		// when
		jobBreachRootCause, err := jobSLAPredictorService.IdentifySLABreaches(ctx, projectName, nextScheduledRangeInHours, jobNames, labels, false, "")

		// then
		assert.NoError(t, err)
		assert.Len(t, jobBreachRootCause, 1)
		assert.Len(t, jobBreachRootCause["job-A1"], 1)
		assert.Equal(t, scheduler.JobName("job-C"), jobBreachRootCause["job-A1"]["job-C"].JobName)
		assert.Equal(t, 2, jobBreachRootCause["job-A1"]["job-C"].RelativeLevel)
		assert.Equal(t, service.SLABreachCauseRunningLate, jobBreachRootCause["job-A1"]["job-C"].Status)
	})
}

// JobLineageFetcher is an autogenerated mock type for the JobLineageFetcher type
type JobLineageFetcher struct {
	mock.Mock
}

// GetJobLineage provides a mock function with given fields: ctx, jobSchedules
func (_m *JobLineageFetcher) GetJobLineage(ctx context.Context, jobSchedules []*scheduler.JobSchedule) (map[scheduler.JobName]*scheduler.JobLineageSummary, error) {
	ret := _m.Called(ctx, jobSchedules)

	if len(ret) == 0 {
		panic("no return value specified for GetJobLineage")
	}

	var r0 map[scheduler.JobName]*scheduler.JobLineageSummary
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context, []*scheduler.JobSchedule) (map[scheduler.JobName]*scheduler.JobLineageSummary, error)); ok {
		return rf(ctx, jobSchedules)
	}
	if rf, ok := ret.Get(0).(func(context.Context, []*scheduler.JobSchedule) map[scheduler.JobName]*scheduler.JobLineageSummary); ok {
		r0 = rf(ctx, jobSchedules)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(map[scheduler.JobName]*scheduler.JobLineageSummary)
		}
	}

	if rf, ok := ret.Get(1).(func(context.Context, []*scheduler.JobSchedule) error); ok {
		r1 = rf(ctx, jobSchedules)
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

// GetPercentileDurationByJobNames provides a mock function with given fields: ctx, jobNames
func (_m *DurationEstimator) GetPercentileDurationByJobNames(ctx context.Context, jobNames []scheduler.JobName) (map[scheduler.JobName]*time.Duration, error) {
	ret := _m.Called(ctx, jobNames)

	if len(ret) == 0 {
		panic("no return value specified for GetPercentileDurationByJobNames")
	}

	var r0 map[scheduler.JobName]*time.Duration
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context, []scheduler.JobName) (map[scheduler.JobName]*time.Duration, error)); ok {
		return rf(ctx, jobNames)
	}
	if rf, ok := ret.Get(0).(func(context.Context, []scheduler.JobName) map[scheduler.JobName]*time.Duration); ok {
		r0 = rf(ctx, jobNames)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(map[scheduler.JobName]*time.Duration)
		}
	}

	if rf, ok := ret.Get(1).(func(context.Context, []scheduler.JobName) error); ok {
		r1 = rf(ctx, jobNames)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// GetPercentileDurationByJobNamesByHookName provides a mock function with given fields: ctx, jobNames, hookNames
func (_m *DurationEstimator) GetPercentileDurationByJobNamesByHookName(ctx context.Context, jobNames []scheduler.JobName, hookNames []string) (map[scheduler.JobName]*time.Duration, error) {
	ret := _m.Called(ctx, jobNames, hookNames)

	if len(ret) == 0 {
		panic("no return value specified for GetPercentileDurationByJobNamesByHookName")
	}

	var r0 map[scheduler.JobName]*time.Duration
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context, []scheduler.JobName, []string) (map[scheduler.JobName]*time.Duration, error)); ok {
		return rf(ctx, jobNames, hookNames)
	}
	if rf, ok := ret.Get(0).(func(context.Context, []scheduler.JobName, []string) map[scheduler.JobName]*time.Duration); ok {
		r0 = rf(ctx, jobNames, hookNames)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(map[scheduler.JobName]*time.Duration)
		}
	}

	if rf, ok := ret.Get(1).(func(context.Context, []scheduler.JobName, []string) error); ok {
		r1 = rf(ctx, jobNames, hookNames)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// GetPercentileDurationByJobNamesByTask provides a mock function with given fields: ctx, jobNames
func (_m *DurationEstimator) GetPercentileDurationByJobNamesByTask(ctx context.Context, jobNames []scheduler.JobName) (map[scheduler.JobName]*time.Duration, error) {
	ret := _m.Called(ctx, jobNames)

	if len(ret) == 0 {
		panic("no return value specified for GetPercentileDurationByJobNamesByTask")
	}

	var r0 map[scheduler.JobName]*time.Duration
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context, []scheduler.JobName) (map[scheduler.JobName]*time.Duration, error)); ok {
		return rf(ctx, jobNames)
	}
	if rf, ok := ret.Get(0).(func(context.Context, []scheduler.JobName) map[scheduler.JobName]*time.Duration); ok {
		r0 = rf(ctx, jobNames)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(map[scheduler.JobName]*time.Duration)
		}
	}

	if rf, ok := ret.Get(1).(func(context.Context, []scheduler.JobName) error); ok {
		r1 = rf(ctx, jobNames)
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
}) *DurationEstimator {
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
	ret := _m.Called(ctx, projectName, labels, false, "")

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
