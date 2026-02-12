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

	"github.com/goto/optimus/core/scheduler"
	"github.com/goto/optimus/core/scheduler/service"
	"github.com/goto/optimus/core/tenant"
)

func TestGenerateExpectedFinishTimes(t *testing.T) {
	ctx := context.Background()
	projectName := tenant.ProjectName("project-a")
	referenceTime := time.Now()
	scheduleRangeInHours := 10 * time.Hour
	l := log.NewNoop()

	t.Run("given no jobs, should return empty map", func(t *testing.T) {
		// given
		jobRunExpectationDetailsRepo := NewJobRunExpectationDetailsRepository(t)
		jobDetailsGetter := NewJobDetailsGetter(t)
		jobLineageFetcher := NewJobLineageFetcher(t)
		durationEstimator := NewDurationEstimator(t)

		jobExpectatorService := service.NewJobExpectatorService(
			l,
			jobRunExpectationDetailsRepo,
			jobDetailsGetter,
			jobLineageFetcher,
			durationEstimator,
		)

		// when
		expectedFinishTimes, err := jobExpectatorService.GenerateExpectedFinishTimes(ctx, projectName, []scheduler.JobName{}, map[string]string{}, referenceTime, scheduleRangeInHours)

		// then
		assert.NoError(t, err)
		assert.Empty(t, expectedFinishTimes)
	})

	t.Run("given jobs, when get job detail error, return error", func(t *testing.T) {
		// given
		jobRunExpectationDetailsRepo := NewJobRunExpectationDetailsRepository(t)
		jobDetailsGetter := NewJobDetailsGetter(t)
		jobLineageFetcher := NewJobLineageFetcher(t)
		durationEstimator := NewDurationEstimator(t)

		jobExpectatorService := service.NewJobExpectatorService(
			l,
			jobRunExpectationDetailsRepo,
			jobDetailsGetter,
			jobLineageFetcher,
			durationEstimator,
		)

		jobAName := scheduler.JobName("job-A")

		jobDetailsGetter.On("GetJobs", ctx, projectName, []string{jobAName.String()}).Return([]*scheduler.JobWithDetails{}, errors.New("some error"))

		// when
		expectedFinishTimes, err := jobExpectatorService.GenerateExpectedFinishTimes(ctx, projectName, []scheduler.JobName{jobAName}, map[string]string{}, referenceTime, scheduleRangeInHours)

		// then
		assert.Nil(t, expectedFinishTimes)
		assert.EqualError(t, err, "some error")
	})

	t.Run("given job label, when get job detail error, return error", func(t *testing.T) {
		// given
		jobRunExpectationDetailsRepo := NewJobRunExpectationDetailsRepository(t)
		jobDetailsGetter := NewJobDetailsGetter(t)
		jobLineageFetcher := NewJobLineageFetcher(t)
		durationEstimator := NewDurationEstimator(t)

		jobExpectatorService := service.NewJobExpectatorService(
			l,
			jobRunExpectationDetailsRepo,
			jobDetailsGetter,
			jobLineageFetcher,
			durationEstimator,
		)

		labels := map[string]string{"category": "some-category"}

		jobDetailsGetter.On("GetJobsByLabels", ctx, projectName, labels).Return([]*scheduler.JobWithDetails{}, errors.New("some error"))

		// when
		expectedFinishTimes, err := jobExpectatorService.GenerateExpectedFinishTimes(ctx, projectName, []scheduler.JobName{}, labels, referenceTime, scheduleRangeInHours)

		// then
		assert.Nil(t, expectedFinishTimes)
		assert.EqualError(t, err, "some error")
	})

	t.Run("given jobs, with no job details, should return empty map", func(t *testing.T) {
		// given
		jobRunExpectationDetailsRepo := NewJobRunExpectationDetailsRepository(t)
		jobDetailsGetter := NewJobDetailsGetter(t)
		jobLineageFetcher := NewJobLineageFetcher(t)
		durationEstimator := NewDurationEstimator(t)

		jobExpectatorService := service.NewJobExpectatorService(
			l,
			jobRunExpectationDetailsRepo,
			jobDetailsGetter,
			jobLineageFetcher,
			durationEstimator,
		)

		jobAName := scheduler.JobName("job-A")

		jobDetailsGetter.On("GetJobs", ctx, projectName, []string{jobAName.String()}).Return([]*scheduler.JobWithDetails{}, nil)

		// when
		expectedFinishTimes, err := jobExpectatorService.GenerateExpectedFinishTimes(ctx, projectName, []scheduler.JobName{jobAName}, map[string]string{}, referenceTime, scheduleRangeInHours)

		// then
		assert.NoError(t, err)
		assert.Empty(t, expectedFinishTimes)
	})

	t.Run("given job, with no job schedules, should return empty map", func(t *testing.T) {
		// given
		jobRunExpectationDetailsRepo := NewJobRunExpectationDetailsRepository(t)
		jobDetailsGetter := NewJobDetailsGetter(t)
		jobLineageFetcher := NewJobLineageFetcher(t)
		durationEstimator := NewDurationEstimator(t)

		jobExpectatorService := service.NewJobExpectatorService(
			l,
			jobRunExpectationDetailsRepo,
			jobDetailsGetter,
			jobLineageFetcher,
			durationEstimator,
		)

		tenant, _ := tenant.NewTenant("project-a", "team-a")
		jobAName := scheduler.JobName("job-A")

		jobWithDetails := &scheduler.JobWithDetails{
			Name: jobAName,
			Job: &scheduler.Job{
				Tenant: tenant,
				Name:   jobAName,
			},
			Schedule: nil, // no schedule
		}

		jobDetailsGetter.On("GetJobs", ctx, projectName, []string{jobAName.String()}).Return([]*scheduler.JobWithDetails{jobWithDetails}, nil)

		// when
		expectedFinishTimes, err := jobExpectatorService.GenerateExpectedFinishTimes(ctx, projectName, []scheduler.JobName{jobAName}, map[string]string{}, referenceTime, scheduleRangeInHours)

		// then
		assert.NoError(t, err)
		assert.Empty(t, expectedFinishTimes)
	})

	t.Run("given job, with job schedules, when get lineage error, return error", func(t *testing.T) {
		// given
		jobRunExpectationDetailsRepo := NewJobRunExpectationDetailsRepository(t)
		jobDetailsGetter := NewJobDetailsGetter(t)
		jobLineageFetcher := NewJobLineageFetcher(t)
		durationEstimator := NewDurationEstimator(t)

		jobExpectatorService := service.NewJobExpectatorService(
			l,
			jobRunExpectationDetailsRepo,
			jobDetailsGetter,
			jobLineageFetcher,
			durationEstimator,
		)

		tenant, _ := tenant.NewTenant("project-a", "team-a")
		jobAName := scheduler.JobName("job-A")
		startDate := referenceTime.Add(-24 * time.Hour).Truncate(time.Hour)
		// get hour from now + scheduleRangeInHours - 1 hours to make sure it's within next schedule range
		scheduledAt := referenceTime.Add(scheduleRangeInHours - 1*time.Hour).Truncate(time.Hour)
		interval := fmt.Sprintf("0 %d * * *", scheduledAt.Hour()) // daily

		jobWithDetails := &scheduler.JobWithDetails{
			Name: jobAName,
			Job: &scheduler.Job{
				Tenant: tenant,
				Name:   jobAName,
			},
			Schedule: &scheduler.Schedule{
				StartDate: startDate,
				Interval:  interval,
			},
		}

		jobDetailsGetter.On("GetJobs", ctx, projectName, []string{jobAName.String()}).Return([]*scheduler.JobWithDetails{jobWithDetails}, nil)
		jobLineageFetcher.On("GetJobLineage", ctx, map[scheduler.JobName]*scheduler.JobSchedule{jobAName: {JobName: jobAName, ScheduledAt: scheduledAt}}).Return(nil, errors.New("some error"))

		// when
		expectedFinishTimes, err := jobExpectatorService.GenerateExpectedFinishTimes(ctx, projectName, []scheduler.JobName{jobAName}, map[string]string{}, referenceTime, scheduleRangeInHours)

		// then
		assert.Nil(t, expectedFinishTimes)
		assert.EqualError(t, err, "some error")
	})

	t.Run("given job, with job schedules and lineage, when estimate duration error, return error", func(t *testing.T) {
		// given
		jobRunExpectationDetailsRepo := NewJobRunExpectationDetailsRepository(t)
		jobDetailsGetter := NewJobDetailsGetter(t)
		jobLineageFetcher := NewJobLineageFetcher(t)
		durationEstimator := NewDurationEstimator(t)

		jobExpectatorService := service.NewJobExpectatorService(
			l,
			jobRunExpectationDetailsRepo,
			jobDetailsGetter,
			jobLineageFetcher,
			durationEstimator,
		)

		tenant, _ := tenant.NewTenant("project-a", "team-a")
		jobAName := scheduler.JobName("job-A")
		startDate := referenceTime.Add(-24 * time.Hour).Truncate(time.Hour)
		scheduledAt := referenceTime.Add(scheduleRangeInHours - 1*time.Hour).Truncate(time.Hour)
		interval := fmt.Sprintf("0 %d * * *", scheduledAt.Hour()) // daily

		jobWithDetails := &scheduler.JobWithDetails{
			Name: jobAName,
			Job: &scheduler.Job{
				Tenant: tenant,
				Name:   jobAName,
			},
			Schedule: &scheduler.Schedule{
				StartDate: startDate,
				Interval:  interval,
			},
		}

		jobLineageSummary := &scheduler.JobLineageSummary{
			JobName:   jobAName,
			IsEnabled: true,
			Upstreams: []*scheduler.JobLineageSummary{},
		}

		jobDetailsGetter.On("GetJobs", ctx, projectName, []string{jobAName.String()}).Return([]*scheduler.JobWithDetails{jobWithDetails}, nil)
		jobLineageFetcher.On("GetJobLineage", ctx, map[scheduler.JobName]*scheduler.JobSchedule{jobAName: {JobName: jobAName, ScheduledAt: scheduledAt}}).Return(map[scheduler.JobName]*scheduler.JobLineageSummary{jobAName: jobLineageSummary}, nil)
		durationEstimator.On("GetPercentileDurationByJobNames", ctx, referenceTime, []scheduler.JobName{jobAName}).Return(nil, errors.New("some error"))

		// when
		expectedFinishTimes, err := jobExpectatorService.GenerateExpectedFinishTimes(ctx, projectName, []scheduler.JobName{jobAName}, map[string]string{}, referenceTime, scheduleRangeInHours)

		// then
		assert.Nil(t, expectedFinishTimes)
		assert.EqualError(t, err, "some error")
	})

	t.Run("given job, with job schedules, lineage and duration estimation, should return expected finish time", func(t *testing.T) {
		// given
		jobRunExpectationDetailsRepo := NewJobRunExpectationDetailsRepository(t)
		jobDetailsGetter := NewJobDetailsGetter(t)
		jobLineageFetcher := NewJobLineageFetcher(t)
		durationEstimator := NewDurationEstimator(t)

		jobExpectatorService := service.NewJobExpectatorService(
			l,
			jobRunExpectationDetailsRepo,
			jobDetailsGetter,
			jobLineageFetcher,
			durationEstimator,
		)

		tenant, _ := tenant.NewTenant("project-a", "team-a")
		jobAName := scheduler.JobName("job-A")
		startDate := referenceTime.Add(-24 * time.Hour).Truncate(time.Hour)
		scheduledAt := referenceTime.Add(scheduleRangeInHours - 1*time.Hour).Truncate(time.Hour)
		interval := fmt.Sprintf("0 %d * * *", scheduledAt.Hour()) // daily

		jobWithDetails := &scheduler.JobWithDetails{
			Name: jobAName,
			Job: &scheduler.Job{
				Tenant: tenant,
				Name:   jobAName,
			},
			Schedule: &scheduler.Schedule{
				StartDate: startDate,
				Interval:  interval,
			},
		}

		jobLineageSummary := &scheduler.JobLineageSummary{
			JobName:   jobAName,
			IsEnabled: true,
			JobRuns: map[scheduler.JobName]*scheduler.JobRunSummary{
				jobAName: {
					JobName:     jobAName,
					ScheduledAt: scheduledAt,
				},
			},
			Upstreams: []*scheduler.JobLineageSummary{},
		}

		jobDetailsGetter.On("GetJobs", ctx, projectName, []string{jobAName.String()}).Return([]*scheduler.JobWithDetails{jobWithDetails}, nil)
		jobLineageFetcher.On("GetJobLineage", ctx, map[scheduler.JobName]*scheduler.JobSchedule{jobAName: {JobName: jobAName, ScheduledAt: scheduledAt}}).Return(map[scheduler.JobName]*scheduler.JobLineageSummary{jobAName: jobLineageSummary}, nil)
		durationEstimator.On("GetPercentileDurationByJobNames", ctx, referenceTime, []scheduler.JobName{jobAName}).Return(map[scheduler.JobName]*time.Duration{jobAName: func() *time.Duration { d := 30 * time.Minute; return &d }()}, nil)
		jobRunExpectationDetailsRepo.On("UpsertExpectedFinishTime", ctx, projectName, jobAName, scheduledAt, scheduledAt.Add(30*time.Minute)).Return(nil)

		// when
		expectedFinishTimes, err := jobExpectatorService.GenerateExpectedFinishTimes(ctx, projectName, []scheduler.JobName{jobAName}, map[string]string{}, referenceTime, scheduleRangeInHours)

		// then
		assert.NoError(t, err)
		expectedExpectedFinishTime := scheduledAt.Add(30 * time.Minute)
		assert.Equal(t, map[scheduler.JobSchedule]service.FinishTimeDetail{{JobName: jobAName, ScheduledAt: scheduledAt}: {FinishTime: expectedExpectedFinishTime, Status: service.FinishTimeStatusInprogress}}, expectedFinishTimes)
	})
}

func TestPopulateExpectedFinishTime(t *testing.T) {
	l := log.NewNoop()
	referenceTime := time.Now()
	scheduleRangeInHours := 10 * time.Hour
	bufferTime := 10 * time.Minute

	t.Run("when no current job run exists, should skip", func(t *testing.T) {
		// given
		jobRunExpectationDetailsRepo := NewJobRunExpectationDetailsRepository(t)
		jobDetailsGetter := NewJobDetailsGetter(t)
		jobLineageFetcher := NewJobLineageFetcher(t)
		durationEstimator := NewDurationEstimator(t)

		jobExpectatorService := service.NewJobExpectatorService(
			l,
			jobRunExpectationDetailsRepo,
			jobDetailsGetter,
			jobLineageFetcher,
			durationEstimator,
		)

		jobRunExpectedFinishTime := map[scheduler.JobSchedule]service.FinishTimeDetail{}
		jobWithLineageMap := map[scheduler.JobName]*scheduler.JobLineageSummary{}
		jobDurationEstimation := map[scheduler.JobName]*time.Duration{}

		scheduledAt := referenceTime.Add(scheduleRangeInHours - 1*time.Hour).Truncate(time.Hour)
		jobTarget := &scheduler.JobSchedule{
			JobName:     scheduler.JobName("job-A"),
			ScheduledAt: scheduledAt,
		}
		currentJobWithLineage := &scheduler.JobLineageSummary{
			JobName:   jobTarget.JobName,
			IsEnabled: true,
			JobRuns:   map[scheduler.JobName]*scheduler.JobRunSummary{}, // no current job run
			Upstreams: []*scheduler.JobLineageSummary{},
		}
		jobWithLineageMap[jobTarget.JobName] = currentJobWithLineage
		jobDurationEstimation[jobTarget.JobName] = func() *time.Duration { d := 30 * time.Minute; return &d }()

		// when
		err := jobExpectatorService.PopulateExpectedFinishTime(jobTarget, currentJobWithLineage, jobRunExpectedFinishTime, jobDurationEstimation, referenceTime)

		// then
		assert.NoError(t, err)
		assert.Empty(t, jobRunExpectedFinishTime)
	})

	t.Run("when duration estimation not found, should skip", func(t *testing.T) {
		// given
		jobRunExpectationDetailsRepo := NewJobRunExpectationDetailsRepository(t)
		jobDetailsGetter := NewJobDetailsGetter(t)
		jobLineageFetcher := NewJobLineageFetcher(t)
		durationEstimator := NewDurationEstimator(t)

		jobExpectatorService := service.NewJobExpectatorService(
			l,
			jobRunExpectationDetailsRepo,
			jobDetailsGetter,
			jobLineageFetcher,
			durationEstimator,
		)
		jobRunExpectedFinishTime := map[scheduler.JobSchedule]service.FinishTimeDetail{}
		jobWithLineageMap := map[scheduler.JobName]*scheduler.JobLineageSummary{}
		jobDurationEstimation := map[scheduler.JobName]*time.Duration{}

		scheduledAt := referenceTime.Add(scheduleRangeInHours - 1*time.Hour).Truncate(time.Hour)
		jobTarget := &scheduler.JobSchedule{
			JobName:     scheduler.JobName("job-A"),
			ScheduledAt: scheduledAt,
		}
		currentJobWithLineage := &scheduler.JobLineageSummary{
			JobName:   jobTarget.JobName,
			IsEnabled: true,
			JobRuns: map[scheduler.JobName]*scheduler.JobRunSummary{
				jobTarget.JobName: {
					JobName:     jobTarget.JobName,
					ScheduledAt: scheduledAt,
				},
			},
			Upstreams: []*scheduler.JobLineageSummary{},
		}
		jobWithLineageMap[jobTarget.JobName] = currentJobWithLineage
		// no duration estimation added

		// when
		err := jobExpectatorService.PopulateExpectedFinishTime(jobTarget, currentJobWithLineage, jobRunExpectedFinishTime, jobDurationEstimation, referenceTime)

		// then
		assert.NoError(t, err)
		assert.Empty(t, jobRunExpectedFinishTime)
	})

	t.Run("when expected finish time already calculated, should skip", func(t *testing.T) {
		// given
		jobRunExpectationDetailsRepo := NewJobRunExpectationDetailsRepository(t)
		jobDetailsGetter := NewJobDetailsGetter(t)
		jobLineageFetcher := NewJobLineageFetcher(t)
		durationEstimator := NewDurationEstimator(t)

		jobExpectatorService := service.NewJobExpectatorService(
			l,
			jobRunExpectationDetailsRepo,
			jobDetailsGetter,
			jobLineageFetcher,
			durationEstimator,
		)
		jobRunExpectedFinishTime := map[scheduler.JobSchedule]service.FinishTimeDetail{}
		jobWithLineageMap := map[scheduler.JobName]*scheduler.JobLineageSummary{}
		jobDurationEstimation := map[scheduler.JobName]*time.Duration{}

		scheduledAt := referenceTime.Add(scheduleRangeInHours - 1*time.Hour).Truncate(time.Hour)
		jobTarget := &scheduler.JobSchedule{
			JobName:     scheduler.JobName("job-A"),
			ScheduledAt: scheduledAt,
		}
		currentJobWithLineage := &scheduler.JobLineageSummary{
			JobName:   jobTarget.JobName,
			IsEnabled: true,
			JobRuns: map[scheduler.JobName]*scheduler.JobRunSummary{
				jobTarget.JobName: {
					JobName:     jobTarget.JobName,
					ScheduledAt: scheduledAt,
				},
			},
			Upstreams: []*scheduler.JobLineageSummary{},
		}
		jobWithLineageMap[jobTarget.JobName] = currentJobWithLineage
		jobDurationEstimation[jobTarget.JobName] = func() *time.Duration { d := 30 * time.Minute; return &d }()
		// already calculated
		jobRunExpectedFinishTime[*jobTarget] = service.FinishTimeDetail{
			FinishTime: scheduledAt.Add(25 * time.Minute),
			Status:     service.FinishTimeStatusInprogress,
		}

		// when
		err := jobExpectatorService.PopulateExpectedFinishTime(jobTarget, currentJobWithLineage, jobRunExpectedFinishTime, jobDurationEstimation, referenceTime)
		// then
		assert.NoError(t, err)
		// should not be updated
		assert.Equal(t, scheduledAt.Add(25*time.Minute), jobRunExpectedFinishTime[*jobTarget].FinishTime)
	})

	t.Run("when end_time is nil and running late, should set expected finish time to reference time + buffer", func(t *testing.T) {
		// given
		jobRunExpectationDetailsRepo := NewJobRunExpectationDetailsRepository(t)
		jobDetailsGetter := NewJobDetailsGetter(t)
		jobLineageFetcher := NewJobLineageFetcher(t)
		durationEstimator := NewDurationEstimator(t)

		jobExpectatorService := service.NewJobExpectatorService(
			l,
			jobRunExpectationDetailsRepo,
			jobDetailsGetter,
			jobLineageFetcher,
			durationEstimator,
		)
		jobRunExpectedFinishTime := map[scheduler.JobSchedule]service.FinishTimeDetail{}
		jobWithLineageMap := map[scheduler.JobName]*scheduler.JobLineageSummary{}
		jobDurationEstimation := map[scheduler.JobName]*time.Duration{}

		scheduledAt := referenceTime.Add(-1 * time.Hour) // scheduled in the past
		jobTarget := &scheduler.JobSchedule{
			JobName:     scheduler.JobName("job-A"),
			ScheduledAt: scheduledAt,
		}
		currentJobWithLineage := &scheduler.JobLineageSummary{
			JobName:   jobTarget.JobName,
			IsEnabled: true,
			JobRuns: map[scheduler.JobName]*scheduler.JobRunSummary{
				jobTarget.JobName: {
					JobName:     jobTarget.JobName,
					ScheduledAt: scheduledAt,
					JobEndTime:  nil, // still running
				},
			},
			Upstreams: []*scheduler.JobLineageSummary{},
		}
		jobWithLineageMap[jobTarget.JobName] = currentJobWithLineage
		jobDurationEstimation[jobTarget.JobName] = func() *time.Duration { d := 30 * time.Minute; return &d }()

		// when
		err := jobExpectatorService.PopulateExpectedFinishTime(jobTarget, currentJobWithLineage, jobRunExpectedFinishTime, jobDurationEstimation, referenceTime)

		// then
		assert.NoError(t, err)
		expectedExpectedFinishTime := referenceTime.Add(bufferTime)
		assert.Equal(t, expectedExpectedFinishTime, jobRunExpectedFinishTime[*jobTarget].FinishTime)
	})

	t.Run("when end_time is not nil, should set expected finish time to job end time", func(t *testing.T) {
		// given
		jobRunExpectationDetailsRepo := NewJobRunExpectationDetailsRepository(t)
		jobDetailsGetter := NewJobDetailsGetter(t)
		jobLineageFetcher := NewJobLineageFetcher(t)
		durationEstimator := NewDurationEstimator(t)

		jobExpectatorService := service.NewJobExpectatorService(
			l,
			jobRunExpectationDetailsRepo,
			jobDetailsGetter,
			jobLineageFetcher,
			durationEstimator,
		)
		jobRunExpectedFinishTime := map[scheduler.JobSchedule]service.FinishTimeDetail{}
		jobWithLineageMap := map[scheduler.JobName]*scheduler.JobLineageSummary{}
		jobDurationEstimation := map[scheduler.JobName]*time.Duration{}

		scheduledAt := referenceTime.Add(-1 * time.Hour) // scheduled in the past
		jobEndTime := referenceTime.Add(-30 * time.Minute)
		jobTarget := &scheduler.JobSchedule{
			JobName:     scheduler.JobName("job-A"),
			ScheduledAt: scheduledAt,
		}
		currentJobWithLineage := &scheduler.JobLineageSummary{
			JobName:   jobTarget.JobName,
			IsEnabled: true,
			JobRuns: map[scheduler.JobName]*scheduler.JobRunSummary{
				jobTarget.JobName: {
					JobName:     jobTarget.JobName,
					ScheduledAt: scheduledAt,
					JobEndTime:  &jobEndTime,
				},
			},
			Upstreams: []*scheduler.JobLineageSummary{},
		}
		jobWithLineageMap[jobTarget.JobName] = currentJobWithLineage
		jobDurationEstimation[jobTarget.JobName] = func() *time.Duration { d := 30 * time.Minute; return &d }()

		// when
		err := jobExpectatorService.PopulateExpectedFinishTime(jobTarget, currentJobWithLineage, jobRunExpectedFinishTime, jobDurationEstimation, referenceTime)

		// then
		assert.NoError(t, err)
		assert.Equal(t, jobEndTime, jobRunExpectedFinishTime[*jobTarget].FinishTime)
	})

	t.Run("when targeted job will run in the future, should set expected finish time to scheduled at + expected duration", func(t *testing.T) {
		// given
		jobRunExpectationDetailsRepo := NewJobRunExpectationDetailsRepository(t)
		jobDetailsGetter := NewJobDetailsGetter(t)
		jobLineageFetcher := NewJobLineageFetcher(t)
		durationEstimator := NewDurationEstimator(t)

		jobExpectatorService := service.NewJobExpectatorService(
			l,
			jobRunExpectationDetailsRepo,
			jobDetailsGetter,
			jobLineageFetcher,
			durationEstimator,
		)
		jobRunExpectedFinishTime := map[scheduler.JobSchedule]service.FinishTimeDetail{}
		jobWithLineageMap := map[scheduler.JobName]*scheduler.JobLineageSummary{}
		jobDurationEstimation := map[scheduler.JobName]*time.Duration{}

		scheduledAt := referenceTime.Add(1 * time.Hour) // scheduled in the future
		jobTarget := &scheduler.JobSchedule{
			JobName:     scheduler.JobName("job-A"),
			ScheduledAt: scheduledAt,
		}
		currentJobWithLineage := &scheduler.JobLineageSummary{
			JobName:   jobTarget.JobName,
			IsEnabled: true,
			JobRuns: map[scheduler.JobName]*scheduler.JobRunSummary{
				jobTarget.JobName: {
					JobName:     jobTarget.JobName,
					ScheduledAt: scheduledAt,
				},
			},
			Upstreams: []*scheduler.JobLineageSummary{},
		}
		jobWithLineageMap[jobTarget.JobName] = currentJobWithLineage
		jobDurationEstimation[jobTarget.JobName] = func() *time.Duration { d := 30 * time.Minute; return &d }()

		// when
		err := jobExpectatorService.PopulateExpectedFinishTime(jobTarget, currentJobWithLineage, jobRunExpectedFinishTime, jobDurationEstimation, referenceTime)

		// then
		assert.NoError(t, err)
		expectedExpectedFinishTime := scheduledAt.Add(30 * time.Minute)
		assert.Equal(t, expectedExpectedFinishTime, jobRunExpectedFinishTime[*jobTarget].FinishTime)
	})

	t.Run("when targeted job will run in the future, and there's an upstream job running late, should set expected finish time to max(upstream expected finish time, scheduled_at) + expected duration", func(t *testing.T) {
		// given
		jobRunExpectationDetailsRepo := NewJobRunExpectationDetailsRepository(t)
		jobDetailsGetter := NewJobDetailsGetter(t)
		jobLineageFetcher := NewJobLineageFetcher(t)
		durationEstimator := NewDurationEstimator(t)

		jobExpectatorService := service.NewJobExpectatorService(
			l,
			jobRunExpectationDetailsRepo,
			jobDetailsGetter,
			jobLineageFetcher,
			durationEstimator,
		)
		jobRunExpectedFinishTime := map[scheduler.JobSchedule]service.FinishTimeDetail{}
		jobWithLineageMap := map[scheduler.JobName]*scheduler.JobLineageSummary{}
		jobDurationEstimation := map[scheduler.JobName]*time.Duration{}

		scheduledAt := referenceTime.Add(1 * time.Hour)          // scheduled in the future
		upstreamScheduledAt := referenceTime.Add(-1 * time.Hour) // upstream scheduled in the past
		jobTarget := &scheduler.JobSchedule{
			JobName:     scheduler.JobName("job-A"),
			ScheduledAt: scheduledAt,
		}
		currentJobWithLineage := &scheduler.JobLineageSummary{
			JobName:   jobTarget.JobName,
			IsEnabled: true,
			JobRuns: map[scheduler.JobName]*scheduler.JobRunSummary{
				jobTarget.JobName: {
					JobName:     jobTarget.JobName,
					ScheduledAt: scheduledAt,
				},
			},
			Upstreams: []*scheduler.JobLineageSummary{},
		}
		jobUpstreamWithLineage := &scheduler.JobLineageSummary{
			JobName:   scheduler.JobName("job-B"),
			IsEnabled: true,
			JobRuns: map[scheduler.JobName]*scheduler.JobRunSummary{
				jobTarget.JobName: {
					JobName:     scheduler.JobName("job-B"),
					ScheduledAt: upstreamScheduledAt,
				},
			},
			Upstreams: []*scheduler.JobLineageSummary{},
		}
		currentJobWithLineage.Upstreams = append(currentJobWithLineage.Upstreams, jobUpstreamWithLineage)
		jobWithLineageMap[jobTarget.JobName] = currentJobWithLineage

		jobDurationEstimation[jobTarget.JobName] = func() *time.Duration { d := 30 * time.Minute; return &d }()
		jobDurationEstimation[jobUpstreamWithLineage.JobName] = func() *time.Duration { d := 45 * time.Minute; return &d }()

		// when
		err := jobExpectatorService.PopulateExpectedFinishTime(jobTarget, currentJobWithLineage, jobRunExpectedFinishTime, jobDurationEstimation, referenceTime)

		// then
		assert.NoError(t, err)
		expectedExpectedFinishTime := scheduledAt.Add(30 * time.Minute)
		assert.Equal(t, expectedExpectedFinishTime, jobRunExpectedFinishTime[*jobTarget].FinishTime)
	})

	t.Run("when targeted job will run in the future, and there's an upstream job running late, and expected finish time for upstream is greater than scheduled_at, should set expected finish time to max(upstream expected finish time, scheduled_at) + expected duration", func(t *testing.T) {
		// given
		jobRunExpectationDetailsRepo := NewJobRunExpectationDetailsRepository(t)
		jobDetailsGetter := NewJobDetailsGetter(t)
		jobLineageFetcher := NewJobLineageFetcher(t)
		durationEstimator := NewDurationEstimator(t)

		jobExpectatorService := service.NewJobExpectatorService(
			l,
			jobRunExpectationDetailsRepo,
			jobDetailsGetter,
			jobLineageFetcher,
			durationEstimator,
		)
		jobRunExpectedFinishTime := map[scheduler.JobSchedule]service.FinishTimeDetail{}
		jobWithLineageMap := map[scheduler.JobName]*scheduler.JobLineageSummary{}
		jobDurationEstimation := map[scheduler.JobName]*time.Duration{}

		scheduledAt := referenceTime.Add(5 * time.Minute)        // scheduled in the future
		upstreamScheduledAt := referenceTime.Add(-1 * time.Hour) // upstream scheduled in the past
		jobTarget := &scheduler.JobSchedule{
			JobName:     scheduler.JobName("job-A"),
			ScheduledAt: scheduledAt,
		}
		currentJobWithLineage := &scheduler.JobLineageSummary{
			JobName:   jobTarget.JobName,
			IsEnabled: true,
			JobRuns: map[scheduler.JobName]*scheduler.JobRunSummary{
				jobTarget.JobName: {
					JobName:     jobTarget.JobName,
					ScheduledAt: scheduledAt,
				},
			},
			Upstreams: []*scheduler.JobLineageSummary{},
		}
		jobUpstreamWithLineage := &scheduler.JobLineageSummary{
			JobName:   scheduler.JobName("job-B"),
			IsEnabled: true,
			JobRuns: map[scheduler.JobName]*scheduler.JobRunSummary{
				jobTarget.JobName: {
					JobName:     scheduler.JobName("job-B"),
					ScheduledAt: upstreamScheduledAt,
				},
			},
			Upstreams: []*scheduler.JobLineageSummary{},
		}
		currentJobWithLineage.Upstreams = append(currentJobWithLineage.Upstreams, jobUpstreamWithLineage)
		jobWithLineageMap[jobTarget.JobName] = currentJobWithLineage

		jobDurationEstimation[jobTarget.JobName] = func() *time.Duration { d := 30 * time.Minute; return &d }()
		jobDurationEstimation[jobUpstreamWithLineage.JobName] = func() *time.Duration { d := 45 * time.Minute; return &d }()

		// when
		err := jobExpectatorService.PopulateExpectedFinishTime(jobTarget, currentJobWithLineage, jobRunExpectedFinishTime, jobDurationEstimation, referenceTime)

		// then
		assert.NoError(t, err)
		expectedExpectedFinishTime := referenceTime.Add(10 * time.Minute).Add(30 * time.Minute)
		assert.Equal(t, expectedExpectedFinishTime, jobRunExpectedFinishTime[*jobTarget].FinishTime)
	})
}

// jobRunExpectationDetailsRepository is an autogenerated mock type for the jobRunExpectationDetailsRepository type
type JobRunExpectationDetailsRepository struct {
	mock.Mock
}

// UpsertExpectedFinishTime provides a mock function with given fields: ctx, projectName, jobName, scheduledAt, expectedFinishTime
func (_m *JobRunExpectationDetailsRepository) UpsertExpectedFinishTime(ctx context.Context, projectName tenant.ProjectName, jobName scheduler.JobName, scheduledAt, expectedFinishTime time.Time) error {
	ret := _m.Called(ctx, projectName, jobName, scheduledAt, expectedFinishTime)

	if len(ret) == 0 {
		panic("no return value specified for UpsertExpectedFinishTime")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, tenant.ProjectName, scheduler.JobName, time.Time, time.Time) error); ok {
		r0 = rf(ctx, projectName, jobName, scheduledAt, expectedFinishTime)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// NewJobRunExpectationDetailsRepository creates a new instance of jobRunExpectationDetailsRepository. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewJobRunExpectationDetailsRepository(t interface {
	mock.TestingT
	Cleanup(func())
},
) *JobRunExpectationDetailsRepository {
	mock := &JobRunExpectationDetailsRepository{}
	mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
