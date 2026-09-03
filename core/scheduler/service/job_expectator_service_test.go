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
	referenceTime := time.Now().UTC()
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
			10,
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
			10,
			jobRunExpectationDetailsRepo,
			jobDetailsGetter,
			jobLineageFetcher,
			durationEstimator,
		)

		jobAName := scheduler.JobName("job-A")

		jobDetailsGetter.On("GetJobs", ctx, projectName, []string{jobAName.String()}).Return(nil, errors.New("some error"))

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
			10,
			jobRunExpectationDetailsRepo,
			jobDetailsGetter,
			jobLineageFetcher,
			durationEstimator,
		)

		labels := map[string]string{"category": "some-category"}

		jobDetailsGetter.On("GetJobsByLabels", ctx, projectName, labels).Return(nil, errors.New("some error"))

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
			10,
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
			10,
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
			10,
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
		jobLineageFetcher.On("GetJobLineage", ctx, map[scheduler.JobName]*scheduler.JobSchedule{jobAName: {JobName: jobAName, ScheduledAt: scheduledAt}}, int(scheduleRangeInHours.Hours())).Return(nil, errors.New("some error"))

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
			10,
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
		jobLineageFetcher.On("GetJobLineage", ctx, map[scheduler.JobName]*scheduler.JobSchedule{jobAName: {JobName: jobAName, ScheduledAt: scheduledAt}}, int(scheduleRangeInHours.Hours())).Return(map[scheduler.JobName]*scheduler.JobLineageSummary{jobAName: jobLineageSummary}, nil)
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
			10,
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
		jobLineageFetcher.On("GetJobLineage", ctx, map[scheduler.JobName]*scheduler.JobSchedule{jobAName: {JobName: jobAName, ScheduledAt: scheduledAt}}, int(scheduleRangeInHours.Hours())).Return(map[scheduler.JobName]*scheduler.JobLineageSummary{jobAName: jobLineageSummary}, nil)
		durationEstimator.On("GetPercentileDurationByJobNames", ctx, referenceTime, []scheduler.JobName{jobAName}).Return(map[scheduler.JobName]*time.Duration{jobAName: func() *time.Duration { d := 30 * time.Minute; return &d }()}, nil)
		jobRunExpectationDetailsRepo.On("UpsertExpectedFinishTime", ctx, projectName, jobAName, scheduledAt, scheduledAt.Add(30*time.Minute)).Return(nil)

		// when
		expectedFinishTimes, err := jobExpectatorService.GenerateExpectedFinishTimes(ctx, projectName, []scheduler.JobName{jobAName}, map[string]string{}, referenceTime, scheduleRangeInHours)

		// then
		assert.NoError(t, err)
		expectedExpectedFinishTime := scheduledAt.Add(30 * time.Minute)
		assert.Equal(t, map[scheduler.JobSchedule]service.FinishTimeDetail{{JobName: jobAName, ScheduledAt: scheduledAt}: {FinishTime: expectedExpectedFinishTime, Status: service.FinishTimeStatusInprogress}}, expectedFinishTimes)
	})

	t.Run("given a complete job but with nonblocking error, should still return the expected finish time", func(t *testing.T) {
		// given
		jobRunExpectationDetailsRepo := NewJobRunExpectationDetailsRepository(t)
		jobDetailsGetter := NewJobDetailsGetter(t)
		jobLineageFetcher := NewJobLineageFetcher(t)
		durationEstimator := NewDurationEstimator(t)

		jobExpectatorService := service.NewJobExpectatorService(
			l,
			10,
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

		jobDetailsGetter.On("GetJobs", ctx, projectName, []string{jobAName.String()}).Return([]*scheduler.JobWithDetails{jobWithDetails}, errors.New("nonblocking error")).Once()
		jobLineageFetcher.On("GetJobLineage", ctx, map[scheduler.JobName]*scheduler.JobSchedule{jobAName: {JobName: jobAName, ScheduledAt: scheduledAt}}, int(scheduleRangeInHours.Hours())).Return(map[scheduler.JobName]*scheduler.JobLineageSummary{jobAName: jobLineageSummary}, nil).Once()
		durationEstimator.On("GetPercentileDurationByJobNames", ctx, referenceTime, []scheduler.JobName{jobAName}).Return(map[scheduler.JobName]*time.Duration{jobAName: func() *time.Duration { d := 30 * time.Minute; return &d }()}, nil).Once()
		jobRunExpectationDetailsRepo.On("UpsertExpectedFinishTime", ctx, projectName, jobAName, scheduledAt, scheduledAt.Add(30*time.Minute)).Return(nil).Once()

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
			10,
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
		err := jobExpectatorService.PopulateExpectedFinishTime(jobTarget.JobName, currentJobWithLineage, jobRunExpectedFinishTime, jobDurationEstimation, referenceTime)

		// then
		assert.NoError(t, err)
		assert.Empty(t, jobRunExpectedFinishTime)
	})

	t.Run("when end_time is not nil, should set expected finish time to job end time", func(t *testing.T) {
		// given
		jobRunExpectationDetailsRepo := NewJobRunExpectationDetailsRepository(t)
		jobDetailsGetter := NewJobDetailsGetter(t)
		jobLineageFetcher := NewJobLineageFetcher(t)
		durationEstimator := NewDurationEstimator(t)

		jobExpectatorService := service.NewJobExpectatorService(
			l,
			10,
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
		err := jobExpectatorService.PopulateExpectedFinishTime(jobTarget.JobName, currentJobWithLineage, jobRunExpectedFinishTime, jobDurationEstimation, referenceTime)

		// then
		assert.NoError(t, err)
		assert.Equal(t, jobEndTime, jobRunExpectedFinishTime[*jobTarget].FinishTime)
	})

	t.Run("when duration estimation not found, use buffer duration", func(t *testing.T) {
		// given
		jobRunExpectationDetailsRepo := NewJobRunExpectationDetailsRepository(t)
		jobDetailsGetter := NewJobDetailsGetter(t)
		jobLineageFetcher := NewJobLineageFetcher(t)
		durationEstimator := NewDurationEstimator(t)

		jobExpectatorService := service.NewJobExpectatorService(
			l,
			10,
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
		err := jobExpectatorService.PopulateExpectedFinishTime(jobTarget.JobName, currentJobWithLineage, jobRunExpectedFinishTime, jobDurationEstimation, referenceTime)

		// then
		assert.NoError(t, err)
		assert.NotEmpty(t, jobRunExpectedFinishTime)
		assert.Equal(t, scheduledAt.Add(bufferTime), jobRunExpectedFinishTime[*jobTarget].FinishTime)
	})

	t.Run("when expected finish time already calculated, should skip", func(t *testing.T) {
		// given
		jobRunExpectationDetailsRepo := NewJobRunExpectationDetailsRepository(t)
		jobDetailsGetter := NewJobDetailsGetter(t)
		jobLineageFetcher := NewJobLineageFetcher(t)
		durationEstimator := NewDurationEstimator(t)

		jobExpectatorService := service.NewJobExpectatorService(
			l,
			10,
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
		err := jobExpectatorService.PopulateExpectedFinishTime(jobTarget.JobName, currentJobWithLineage, jobRunExpectedFinishTime, jobDurationEstimation, referenceTime)
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
			10,
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
					JobName:       jobTarget.JobName,
					ScheduledAt:   scheduledAt,
					TaskStartTime: &scheduledAt, // started on time
					JobEndTime:    nil,          // still running
				},
			},
			Upstreams: []*scheduler.JobLineageSummary{},
		}
		jobWithLineageMap[jobTarget.JobName] = currentJobWithLineage
		jobDurationEstimation[jobTarget.JobName] = func() *time.Duration { d := 30 * time.Minute; return &d }()

		// when
		err := jobExpectatorService.PopulateExpectedFinishTime(jobTarget.JobName, currentJobWithLineage, jobRunExpectedFinishTime, jobDurationEstimation, referenceTime)

		// then
		assert.NoError(t, err)
		expectedExpectedFinishTime := referenceTime.Add(bufferTime)
		assert.Equal(t, expectedExpectedFinishTime, jobRunExpectedFinishTime[*jobTarget].FinishTime)
	})

	t.Run("when end_time is nil and job still running, should set expected finish time to task start time + estimated duration", func(t *testing.T) {
		// given
		jobRunExpectationDetailsRepo := NewJobRunExpectationDetailsRepository(t)
		jobDetailsGetter := NewJobDetailsGetter(t)
		jobLineageFetcher := NewJobLineageFetcher(t)
		durationEstimator := NewDurationEstimator(t)

		jobExpectatorService := service.NewJobExpectatorService(
			l,
			10,
			jobRunExpectationDetailsRepo,
			jobDetailsGetter,
			jobLineageFetcher,
			durationEstimator,
		)
		jobRunExpectedFinishTime := map[scheduler.JobSchedule]service.FinishTimeDetail{}
		jobWithLineageMap := map[scheduler.JobName]*scheduler.JobLineageSummary{}
		jobDurationEstimation := map[scheduler.JobName]*time.Duration{}

		scheduledAt := referenceTime.Add(-5 * time.Minute) // scheduled in the past, but not running late yet
		jobTarget := &scheduler.JobSchedule{
			JobName:     scheduler.JobName("job-A"),
			ScheduledAt: scheduledAt,
		}
		currentJobWithLineage := &scheduler.JobLineageSummary{
			JobName:   jobTarget.JobName,
			IsEnabled: true,
			JobRuns: map[scheduler.JobName]*scheduler.JobRunSummary{
				jobTarget.JobName: {
					JobName:       jobTarget.JobName,
					ScheduledAt:   scheduledAt,
					TaskStartTime: &scheduledAt, // started on time
					JobEndTime:    nil,          // still running
				},
			},
			Upstreams: []*scheduler.JobLineageSummary{},
		}
		jobWithLineageMap[jobTarget.JobName] = currentJobWithLineage
		jobDurationEstimation[jobTarget.JobName] = func() *time.Duration { d := 30 * time.Minute; return &d }()

		// when
		err := jobExpectatorService.PopulateExpectedFinishTime(jobTarget.JobName, currentJobWithLineage, jobRunExpectedFinishTime, jobDurationEstimation, referenceTime)

		// then
		assert.NoError(t, err)
		expectedExpectedFinishTime := scheduledAt.Add(30 * time.Minute)
		assert.Equal(t, expectedExpectedFinishTime, jobRunExpectedFinishTime[*jobTarget].FinishTime)
	})

	t.Run("when targeted job will run in the future, should set expected finish time to scheduled at + expected duration", func(t *testing.T) {
		// given
		jobRunExpectationDetailsRepo := NewJobRunExpectationDetailsRepository(t)
		jobDetailsGetter := NewJobDetailsGetter(t)
		jobLineageFetcher := NewJobLineageFetcher(t)
		durationEstimator := NewDurationEstimator(t)

		jobExpectatorService := service.NewJobExpectatorService(
			l,
			10,
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
		err := jobExpectatorService.PopulateExpectedFinishTime(jobTarget.JobName, currentJobWithLineage, jobRunExpectedFinishTime, jobDurationEstimation, referenceTime)

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
			10,
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
		err := jobExpectatorService.PopulateExpectedFinishTime(jobTarget.JobName, currentJobWithLineage, jobRunExpectedFinishTime, jobDurationEstimation, referenceTime)

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
			10,
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
					TaskStartTime: func() *time.Time {
						t := upstreamScheduledAt.Add(25 * time.Minute) // started late
						return &t
					}(),
					JobEndTime: nil, // still running
				},
			},
			Upstreams: []*scheduler.JobLineageSummary{},
		}
		currentJobWithLineage.Upstreams = append(currentJobWithLineage.Upstreams, jobUpstreamWithLineage)
		jobWithLineageMap[jobTarget.JobName] = currentJobWithLineage

		jobDurationEstimation[jobTarget.JobName] = func() *time.Duration { d := 30 * time.Minute; return &d }()
		jobDurationEstimation[jobUpstreamWithLineage.JobName] = func() *time.Duration { d := 45 * time.Minute; return &d }()

		// when
		err := jobExpectatorService.PopulateExpectedFinishTime(jobTarget.JobName, currentJobWithLineage, jobRunExpectedFinishTime, jobDurationEstimation, referenceTime)

		// then
		assert.NoError(t, err)
		expectedExpectedFinishTime := referenceTime.Add(10 * time.Minute).Add(30 * time.Minute)
		assert.Equal(t, expectedExpectedFinishTime, jobRunExpectedFinishTime[*jobTarget].FinishTime)
	})

	t.Run("when upstream chain is 2 levels deep, should propagate expected finish time through the full chain", func(t *testing.T) {
		// given
		jobRunExpectationDetailsRepo := NewJobRunExpectationDetailsRepository(t)
		jobDetailsGetter := NewJobDetailsGetter(t)
		jobLineageFetcher := NewJobLineageFetcher(t)
		durationEstimator := NewDurationEstimator(t)

		jobExpectatorService := service.NewJobExpectatorService(
			l,
			10,
			jobRunExpectationDetailsRepo,
			jobDetailsGetter,
			jobLineageFetcher,
			durationEstimator,
		)
		jobRunExpectedFinishTime := map[scheduler.JobSchedule]service.FinishTimeDetail{}
		jobDurationEstimation := map[scheduler.JobName]*time.Duration{}

		scheduledAtA := referenceTime.Add(10 * time.Minute)
		scheduledAtB := referenceTime.Add(1 * time.Hour)
		scheduledAtC := referenceTime.Add(-2 * time.Hour)
		jobCEndTime := referenceTime.Add(90 * time.Minute) // job-C finished after job-B's own scheduled_at

		jobTarget := &scheduler.JobSchedule{
			JobName:     scheduler.JobName("job-A"),
			ScheduledAt: scheduledAtA,
		}
		jobBName := scheduler.JobName("job-B")
		jobCName := scheduler.JobName("job-C")

		// job-C's run is keyed by job-B (its immediate downstream), not by the root job-A,
		// matching LineageResolver.BuildLineage's diamond-safe keying convention.
		jobCWithLineage := &scheduler.JobLineageSummary{
			JobName:   jobCName,
			IsEnabled: true,
			JobRuns: map[scheduler.JobName]*scheduler.JobRunSummary{
				jobBName: {
					JobName:     jobCName,
					ScheduledAt: scheduledAtC,
					JobEndTime:  &jobCEndTime,
				},
			},
			Upstreams: []*scheduler.JobLineageSummary{},
		}
		jobBWithLineage := &scheduler.JobLineageSummary{
			JobName:   jobBName,
			IsEnabled: true,
			JobRuns: map[scheduler.JobName]*scheduler.JobRunSummary{
				jobTarget.JobName: {
					JobName:     jobBName,
					ScheduledAt: scheduledAtB,
				},
			},
			Upstreams: []*scheduler.JobLineageSummary{jobCWithLineage},
		}
		currentJobWithLineage := &scheduler.JobLineageSummary{
			JobName:   jobTarget.JobName,
			IsEnabled: true,
			JobRuns: map[scheduler.JobName]*scheduler.JobRunSummary{
				jobTarget.JobName: {
					JobName:     jobTarget.JobName,
					ScheduledAt: scheduledAtA,
				},
			},
			Upstreams: []*scheduler.JobLineageSummary{jobBWithLineage},
		}

		jobDurationEstimation[jobTarget.JobName] = func() *time.Duration { d := 30 * time.Minute; return &d }()
		jobDurationEstimation[jobBName] = func() *time.Duration { d := 45 * time.Minute; return &d }()

		// when
		err := jobExpectatorService.PopulateExpectedFinishTime(jobTarget.JobName, currentJobWithLineage, jobRunExpectedFinishTime, jobDurationEstimation, referenceTime)

		// then
		assert.NoError(t, err)
		// job-C already finished; its finish time is its own end time
		assert.Equal(t, jobCEndTime, jobRunExpectedFinishTime[scheduler.JobSchedule{JobName: jobCName, ScheduledAt: scheduledAtC}].FinishTime)
		// job-B hasn't started; its expected finish time is max(its own scheduled_at, job-C's finish time) + job-B duration
		expectedBFinish := jobCEndTime.Add(45 * time.Minute)
		assert.Equal(t, expectedBFinish, jobRunExpectedFinishTime[scheduler.JobSchedule{JobName: jobBName, ScheduledAt: scheduledAtB}].FinishTime)
		// job-A hasn't started; its expected finish time is max(its own scheduled_at, job-B's finish time) + job-A duration,
		// proving job-C's contribution propagated two levels up through job-B
		expectedAFinish := expectedBFinish.Add(30 * time.Minute)
		assert.Equal(t, expectedAFinish, jobRunExpectedFinishTime[*jobTarget].FinishTime)
	})

	t.Run("when an upstream job occurs twice in the lineage, once shallow and once deep (diamond), should reuse the shared node's cached finish time instead of recomputing or conflicting", func(t *testing.T) {
		// given
		jobRunExpectationDetailsRepo := NewJobRunExpectationDetailsRepository(t)
		jobDetailsGetter := NewJobDetailsGetter(t)
		jobLineageFetcher := NewJobLineageFetcher(t)
		durationEstimator := NewDurationEstimator(t)

		jobExpectatorService := service.NewJobExpectatorService(
			l,
			10,
			jobRunExpectationDetailsRepo,
			jobDetailsGetter,
			jobLineageFetcher,
			durationEstimator,
		)
		jobRunExpectedFinishTime := map[scheduler.JobSchedule]service.FinishTimeDetail{}
		jobDurationEstimation := map[scheduler.JobName]*time.Duration{}

		scheduledAtA := referenceTime.Add(30 * time.Minute)
		scheduledAtB := referenceTime.Add(1 * time.Hour)
		scheduledAtX := referenceTime.Add(-3 * time.Hour)
		jobXEndTime := referenceTime.Add(3 * time.Hour)

		jobTarget := &scheduler.JobSchedule{
			JobName:     scheduler.JobName("job-A"),
			ScheduledAt: scheduledAtA,
		}
		jobBName := scheduler.JobName("job-B")
		jobXName := scheduler.JobName("job-X")

		// job-X is a diamond: it is job-A's DIRECT upstream (shallow) and also job-B's upstream
		// (deep, via job-A -> job-B -> job-X). LineageResolver.BuildLineage's buildLineageTree
		// memoizes by job name, so both edges point at the very same *JobLineageSummary object,
		// and calculateAllUpstreamRuns adds one JobRuns entry per immediate-downstream path -
		// here both paths resolve to the same actual run, so both keys share the same run pointer.
		jobXRun := &scheduler.JobRunSummary{
			JobName:     jobXName,
			ScheduledAt: scheduledAtX,
			JobEndTime:  &jobXEndTime,
		}
		jobXWithLineage := &scheduler.JobLineageSummary{
			JobName:   jobXName,
			IsEnabled: true,
			JobRuns: map[scheduler.JobName]*scheduler.JobRunSummary{
				jobTarget.JobName: jobXRun, // shallow path: job-A -> job-X
				jobBName:          jobXRun, // deep path: job-A -> job-B -> job-X (same run)
			},
			Upstreams: []*scheduler.JobLineageSummary{},
		}
		jobBWithLineage := &scheduler.JobLineageSummary{
			JobName:   jobBName,
			IsEnabled: true,
			JobRuns: map[scheduler.JobName]*scheduler.JobRunSummary{
				jobTarget.JobName: {
					JobName:     jobBName,
					ScheduledAt: scheduledAtB,
				},
			},
			Upstreams: []*scheduler.JobLineageSummary{jobXWithLineage},
		}
		currentJobWithLineage := &scheduler.JobLineageSummary{
			JobName:   jobTarget.JobName,
			IsEnabled: true,
			JobRuns: map[scheduler.JobName]*scheduler.JobRunSummary{
				jobTarget.JobName: {
					JobName:     jobTarget.JobName,
					ScheduledAt: scheduledAtA,
				},
			},
			// job-X listed before job-B, so the shallow edge is visited first
			Upstreams: []*scheduler.JobLineageSummary{jobXWithLineage, jobBWithLineage},
		}

		jobDurationEstimation[jobTarget.JobName] = func() *time.Duration { d := 30 * time.Minute; return &d }()
		jobDurationEstimation[jobBName] = func() *time.Duration { d := 45 * time.Minute; return &d }()

		// when
		err := jobExpectatorService.PopulateExpectedFinishTime(jobTarget.JobName, currentJobWithLineage, jobRunExpectedFinishTime, jobDurationEstimation, referenceTime)

		// then
		assert.NoError(t, err)
		// job-X's finish time is computed once (from whichever path is visited first) and shared
		assert.Equal(t, jobXEndTime, jobRunExpectedFinishTime[scheduler.JobSchedule{JobName: jobXName, ScheduledAt: scheduledAtX}].FinishTime)
		// job-B's expected finish incorporates job-X's finish time via the deep path
		expectedBFinish := jobXEndTime.Add(45 * time.Minute)
		assert.Equal(t, expectedBFinish, jobRunExpectedFinishTime[scheduler.JobSchedule{JobName: jobBName, ScheduledAt: scheduledAtB}].FinishTime)
		// job-A's expected finish is max(its own scheduled_at, job-X's direct finish, job-B's finish) + job-A duration;
		// job-B's finish (which itself folds in job-X) dominates, proving the diamond didn't get double-counted or dropped
		expectedAFinish := expectedBFinish.Add(30 * time.Minute)
		assert.Equal(t, expectedAFinish, jobRunExpectedFinishTime[*jobTarget].FinishTime)
	})
}

func TestGenerateJobExpectedCompletionTimeReport(t *testing.T) {
	ctx := context.Background()
	referenceTime := time.Now().UTC()
	scheduleRangeInHours := 10 * time.Hour
	l := log.NewNoop()

	newService := func() (*service.JobExpectatorService, *JobRunExpectationDetailsRepository, *JobDetailsGetter, *JobLineageFetcher, *DurationEstimator) {
		jobRunExpectationDetailsRepo := NewJobRunExpectationDetailsRepository(t)
		jobDetailsGetter := NewJobDetailsGetter(t)
		jobLineageFetcher := NewJobLineageFetcher(t)
		durationEstimator := NewDurationEstimator(t)
		svc := service.NewJobExpectatorService(l, 10, jobRunExpectationDetailsRepo, jobDetailsGetter, jobLineageFetcher, durationEstimator)
		return svc, jobRunExpectationDetailsRepo, jobDetailsGetter, jobLineageFetcher, durationEstimator
	}

	makeJobWithDetails := func(projectName tenant.ProjectName, jobName scheduler.JobName, scheduledAt time.Time) *scheduler.JobWithDetails {
		tnnt, _ := tenant.NewTenant(projectName.String(), "team-a")
		startDate := scheduledAt.Add(-24 * time.Hour).Truncate(time.Hour)
		interval := fmt.Sprintf("0 %d * * *", scheduledAt.Hour())
		return &scheduler.JobWithDetails{
			Name: jobName,
			Job: &scheduler.Job{
				Tenant: tnnt,
				Name:   jobName,
			},
			Schedule: &scheduler.Schedule{
				StartDate: startDate,
				Interval:  interval,
			},
		}
	}

	t.Run("given no combos, should return empty report with nil MeanDelay", func(t *testing.T) {
		svc, _, _, _, _ := newService()

		summary, err := svc.GenerateJobExpectedCompletionTimeReport(ctx, []scheduler.JobFilterRequest{}, referenceTime, scheduleRangeInHours)

		assert.NoError(t, err)
		assert.Empty(t, summary.Reports)
		assert.Nil(t, summary.MeanDelay)
	})

	t.Run("given a single combo with a mix of finished and in-progress jobs, should populate all three fields per job", func(t *testing.T) {
		svc, _, jobDetailsGetter, jobLineageFetcher, durationEstimator := newService()

		projectName := tenant.ProjectName("project-a")
		jobAName := scheduler.JobName("job-A") // not started yet
		jobBName := scheduler.JobName("job-B") // started, not finished, not late
		scheduledAt := referenceTime.Add(scheduleRangeInHours - 1*time.Hour).Truncate(time.Hour)

		jobA := makeJobWithDetails(projectName, jobAName, scheduledAt)
		jobB := makeJobWithDetails(projectName, jobBName, scheduledAt)

		lineageA := &scheduler.JobLineageSummary{
			JobName:   jobAName,
			IsEnabled: true,
			JobRuns: map[scheduler.JobName]*scheduler.JobRunSummary{
				jobAName: {JobName: jobAName, ScheduledAt: scheduledAt},
			},
			Upstreams: []*scheduler.JobLineageSummary{},
		}
		lineageB := &scheduler.JobLineageSummary{
			JobName:   jobBName,
			IsEnabled: true,
			JobRuns: map[scheduler.JobName]*scheduler.JobRunSummary{
				jobBName: {JobName: jobBName, ScheduledAt: scheduledAt, TaskStartTime: &scheduledAt},
			},
			Upstreams: []*scheduler.JobLineageSummary{},
		}

		jobDetailsGetter.On("GetJobs", ctx, projectName, []string{jobAName.String(), jobBName.String()}).
			Return([]*scheduler.JobWithDetails{jobA, jobB}, nil)
		jobLineageFetcher.On("GetJobLineage", ctx, map[scheduler.JobName]*scheduler.JobSchedule{
			jobAName: {JobName: jobAName, ScheduledAt: scheduledAt},
			jobBName: {JobName: jobBName, ScheduledAt: scheduledAt},
		}, int(scheduleRangeInHours.Hours())).Return(map[scheduler.JobName]*scheduler.JobLineageSummary{jobAName: lineageA, jobBName: lineageB}, nil)
		durationEstimator.On("GetPercentileDurationByJobNames", ctx, referenceTime, mock.Anything).
			Return(map[scheduler.JobName]*time.Duration{
				jobAName: func() *time.Duration { d := 30 * time.Minute; return &d }(),
				jobBName: func() *time.Duration { d := 45 * time.Minute; return &d }(),
			}, nil)

		summary, err := svc.GenerateJobExpectedCompletionTimeReport(ctx, []scheduler.JobFilterRequest{
			{ProjectName: projectName, JobNames: []scheduler.JobName{jobAName, jobBName}},
		}, referenceTime, scheduleRangeInHours)

		assert.NoError(t, err)
		assert.Len(t, summary.Reports, 2)

		byJob := map[scheduler.JobName]scheduler.JobCompletionTimeReport{}
		for _, r := range summary.Reports {
			byJob[r.JobName] = r
		}

		assert.Equal(t, scheduledAt.Add(30*time.Minute), byJob[jobAName].ExpectedFinishTime)
		assert.Nil(t, byJob[jobAName].ActualFinishTime)
		assert.Equal(t, scheduledAt.Add(45*time.Minute), byJob[jobBName].ExpectedFinishTime)
		assert.Nil(t, byJob[jobBName].ActualFinishTime)
	})

	t.Run("given combos across multiple projects, should isolate lineage fetches per project and tag results correctly", func(t *testing.T) {
		svc, _, jobDetailsGetter, jobLineageFetcher, durationEstimator := newService()

		projectA := tenant.ProjectName("project-a")
		projectB := tenant.ProjectName("project-b")
		jobAName := scheduler.JobName("job-A")
		jobBName := scheduler.JobName("job-B")
		scheduledAt := referenceTime.Add(scheduleRangeInHours - 1*time.Hour).Truncate(time.Hour)

		jobA := makeJobWithDetails(projectA, jobAName, scheduledAt)
		jobB := makeJobWithDetails(projectB, jobBName, scheduledAt)
		lineageA := &scheduler.JobLineageSummary{
			JobName: jobAName, IsEnabled: true,
			JobRuns:   map[scheduler.JobName]*scheduler.JobRunSummary{jobAName: {JobName: jobAName, ScheduledAt: scheduledAt}},
			Upstreams: []*scheduler.JobLineageSummary{},
		}
		lineageB := &scheduler.JobLineageSummary{
			JobName: jobBName, IsEnabled: true,
			JobRuns:   map[scheduler.JobName]*scheduler.JobRunSummary{jobBName: {JobName: jobBName, ScheduledAt: scheduledAt}},
			Upstreams: []*scheduler.JobLineageSummary{},
		}

		jobDetailsGetter.On("GetJobs", ctx, projectA, []string{jobAName.String()}).Return([]*scheduler.JobWithDetails{jobA}, nil).Once()
		jobDetailsGetter.On("GetJobs", ctx, projectB, []string{jobBName.String()}).Return([]*scheduler.JobWithDetails{jobB}, nil).Once()
		jobLineageFetcher.On("GetJobLineage", ctx, map[scheduler.JobName]*scheduler.JobSchedule{jobAName: {JobName: jobAName, ScheduledAt: scheduledAt}}, int(scheduleRangeInHours.Hours())).
			Return(map[scheduler.JobName]*scheduler.JobLineageSummary{jobAName: lineageA}, nil).Once()
		jobLineageFetcher.On("GetJobLineage", ctx, map[scheduler.JobName]*scheduler.JobSchedule{jobBName: {JobName: jobBName, ScheduledAt: scheduledAt}}, int(scheduleRangeInHours.Hours())).
			Return(map[scheduler.JobName]*scheduler.JobLineageSummary{jobBName: lineageB}, nil).Once()
		durationEstimator.On("GetPercentileDurationByJobNames", ctx, referenceTime, []scheduler.JobName{jobAName}).
			Return(map[scheduler.JobName]*time.Duration{jobAName: func() *time.Duration { d := 30 * time.Minute; return &d }()}, nil).Once()
		durationEstimator.On("GetPercentileDurationByJobNames", ctx, referenceTime, []scheduler.JobName{jobBName}).
			Return(map[scheduler.JobName]*time.Duration{jobBName: func() *time.Duration { d := 30 * time.Minute; return &d }()}, nil).Once()

		summary, err := svc.GenerateJobExpectedCompletionTimeReport(ctx, []scheduler.JobFilterRequest{
			{ProjectName: projectA, JobNames: []scheduler.JobName{jobAName}},
			{ProjectName: projectB, JobNames: []scheduler.JobName{jobBName}},
		}, referenceTime, scheduleRangeInHours)

		assert.NoError(t, err)
		assert.Len(t, summary.Reports, 2)
		for _, r := range summary.Reports {
			if r.JobName == jobAName {
				assert.Equal(t, projectA, r.ProjectName)
			} else {
				assert.Equal(t, projectB, r.ProjectName)
			}
		}
	})

	t.Run("given a run that finishes after referenceTime, should hide it from ExpectedFinishTime but still report the real ActualFinishTime", func(t *testing.T) {
		svc, _, jobDetailsGetter, jobLineageFetcher, durationEstimator := newService()

		projectName := tenant.ProjectName("project-a")
		jobAName := scheduler.JobName("job-A")
		scheduledAt := referenceTime.Add(-2 * time.Hour).Truncate(time.Hour)
		startDate := scheduledAt.Add(-24 * time.Hour).Truncate(time.Hour)
		interval := fmt.Sprintf("0 %d * * *", scheduledAt.Hour())
		tnnt, _ := tenant.NewTenant(projectName.String(), "team-a")
		jobA := &scheduler.JobWithDetails{
			Name: jobAName,
			Job:  &scheduler.Job{Tenant: tnnt, Name: jobAName},
			Schedule: &scheduler.Schedule{
				StartDate: startDate,
				Interval:  interval,
			},
		}

		realHookEndTime := referenceTime.Add(1 * time.Hour) // finished, but only AFTER referenceTime
		realJobEndTime := realHookEndTime
		lineageA := &scheduler.JobLineageSummary{
			JobName:   jobAName,
			IsEnabled: true,
			JobRuns: map[scheduler.JobName]*scheduler.JobRunSummary{
				jobAName: {
					JobName:       jobAName,
					ScheduledAt:   scheduledAt,
					TaskStartTime: &scheduledAt,
					JobEndTime:    &realJobEndTime,
					HookEndTime:   &realHookEndTime,
					JobStatus:     "success",
				},
			},
			Upstreams: []*scheduler.JobLineageSummary{},
		}

		jobDetailsGetter.On("GetJobs", ctx, projectName, []string{jobAName.String()}).Return([]*scheduler.JobWithDetails{jobA}, nil)
		jobLineageFetcher.On("GetJobLineage", ctx, map[scheduler.JobName]*scheduler.JobSchedule{jobAName: {JobName: jobAName, ScheduledAt: scheduledAt}}, int(scheduleRangeInHours.Hours())).
			Return(map[scheduler.JobName]*scheduler.JobLineageSummary{jobAName: lineageA}, nil)
		durationEstimator.On("GetPercentileDurationByJobNames", ctx, referenceTime, []scheduler.JobName{jobAName}).
			Return(map[scheduler.JobName]*time.Duration{jobAName: func() *time.Duration { d := 30 * time.Minute; return &d }()}, nil)

		summary, err := svc.GenerateJobExpectedCompletionTimeReport(ctx, []scheduler.JobFilterRequest{
			{ProjectName: projectName, JobNames: []scheduler.JobName{jobAName}},
		}, referenceTime, scheduleRangeInHours)

		assert.NoError(t, err)
		assert.Len(t, summary.Reports, 1)
		report := summary.Reports[0]

		// taskStartTime (scheduledAt) + 30m duration is before referenceTime -> "running late" branch
		assert.Equal(t, referenceTime.Add(10*time.Minute), report.ExpectedFinishTime)
		// the real end time, hidden from the expected-finish-time calculation, is still reported as-is
		assert.NotNil(t, report.ActualFinishTime)
		assert.Equal(t, realHookEndTime, *report.ActualFinishTime)
		assert.NotEqual(t, report.ExpectedFinishTime, *report.ActualFinishTime)
	})

	t.Run("given a run that finished strictly before referenceTime, ExpectedFinishTime and ActualFinishTime should agree", func(t *testing.T) {
		svc, _, jobDetailsGetter, jobLineageFetcher, durationEstimator := newService()

		projectName := tenant.ProjectName("project-a")
		jobAName := scheduler.JobName("job-A")
		scheduledAt := referenceTime.Add(-3 * time.Hour).Truncate(time.Hour)
		startDate := scheduledAt.Add(-24 * time.Hour).Truncate(time.Hour)
		interval := fmt.Sprintf("0 %d * * *", scheduledAt.Hour())
		tnnt, _ := tenant.NewTenant(projectName.String(), "team-a")
		jobA := &scheduler.JobWithDetails{
			Name: jobAName,
			Job:  &scheduler.Job{Tenant: tnnt, Name: jobAName},
			Schedule: &scheduler.Schedule{
				StartDate: startDate,
				Interval:  interval,
			},
		}

		endTime := referenceTime.Add(-1 * time.Hour) // finished before referenceTime, so nothing is clipped
		lineageA := &scheduler.JobLineageSummary{
			JobName:   jobAName,
			IsEnabled: true,
			JobRuns: map[scheduler.JobName]*scheduler.JobRunSummary{
				jobAName: {
					JobName:       jobAName,
					ScheduledAt:   scheduledAt,
					TaskStartTime: &scheduledAt,
					TaskEndTime:   &endTime,
					JobEndTime:    &endTime,
					JobStatus:     "success",
				},
			},
			Upstreams: []*scheduler.JobLineageSummary{},
		}

		jobDetailsGetter.On("GetJobs", ctx, projectName, []string{jobAName.String()}).Return([]*scheduler.JobWithDetails{jobA}, nil)
		jobLineageFetcher.On("GetJobLineage", ctx, map[scheduler.JobName]*scheduler.JobSchedule{jobAName: {JobName: jobAName, ScheduledAt: scheduledAt}}, int(scheduleRangeInHours.Hours())).
			Return(map[scheduler.JobName]*scheduler.JobLineageSummary{jobAName: lineageA}, nil)
		durationEstimator.On("GetPercentileDurationByJobNames", ctx, referenceTime, []scheduler.JobName{jobAName}).
			Return(map[scheduler.JobName]*time.Duration{jobAName: func() *time.Duration { d := 30 * time.Minute; return &d }()}, nil)

		summary, err := svc.GenerateJobExpectedCompletionTimeReport(ctx, []scheduler.JobFilterRequest{
			{ProjectName: projectName, JobNames: []scheduler.JobName{jobAName}},
		}, referenceTime, scheduleRangeInHours)

		assert.NoError(t, err)
		assert.Len(t, summary.Reports, 1)
		report := summary.Reports[0]
		assert.Equal(t, endTime, report.ExpectedFinishTime)
		assert.NotNil(t, report.ActualFinishTime)
		assert.Equal(t, endTime, *report.ActualFinishTime)
	})

	t.Run("MeanDelay should average only jobs with an ActualFinishTime, not divided by total report count", func(t *testing.T) {
		svc, _, jobDetailsGetter, jobLineageFetcher, durationEstimator := newService()

		projectName := tenant.ProjectName("project-a")
		jobAName := scheduler.JobName("job-A") // finishes 30m later than expected
		jobBName := scheduler.JobName("job-B") // finishes 8m earlier than expected
		scheduledAt := referenceTime.Add(-2 * time.Hour).Truncate(time.Hour)
		startDate := scheduledAt.Add(-24 * time.Hour).Truncate(time.Hour)
		interval := fmt.Sprintf("0 %d * * *", scheduledAt.Hour())
		tnnt, _ := tenant.NewTenant(projectName.String(), "team-a")

		makeJob := func(name scheduler.JobName) *scheduler.JobWithDetails {
			return &scheduler.JobWithDetails{
				Name:     name,
				Job:      &scheduler.Job{Tenant: tnnt, Name: name},
				Schedule: &scheduler.Schedule{StartDate: startDate, Interval: interval},
			}
		}
		jobA := makeJob(jobAName)
		jobB := makeJob(jobBName)

		hookEndA := referenceTime.Add(40 * time.Minute) // expected = referenceTime+10m -> delay +30m
		hookEndB := referenceTime.Add(2 * time.Minute)  // expected = referenceTime+10m -> delay -8m
		lineageA := &scheduler.JobLineageSummary{
			JobName: jobAName, IsEnabled: true,
			JobRuns: map[scheduler.JobName]*scheduler.JobRunSummary{
				jobAName: {JobName: jobAName, ScheduledAt: scheduledAt, TaskStartTime: &scheduledAt, HookEndTime: &hookEndA, JobStatus: "success"},
			},
			Upstreams: []*scheduler.JobLineageSummary{},
		}
		lineageB := &scheduler.JobLineageSummary{
			JobName: jobBName, IsEnabled: true,
			JobRuns: map[scheduler.JobName]*scheduler.JobRunSummary{
				jobBName: {JobName: jobBName, ScheduledAt: scheduledAt, TaskStartTime: &scheduledAt, HookEndTime: &hookEndB, JobStatus: "success"},
			},
			Upstreams: []*scheduler.JobLineageSummary{},
		}

		jobDetailsGetter.On("GetJobs", ctx, projectName, []string{jobAName.String(), jobBName.String()}).
			Return([]*scheduler.JobWithDetails{jobA, jobB}, nil)
		jobLineageFetcher.On("GetJobLineage", ctx, map[scheduler.JobName]*scheduler.JobSchedule{
			jobAName: {JobName: jobAName, ScheduledAt: scheduledAt},
			jobBName: {JobName: jobBName, ScheduledAt: scheduledAt},
		}, int(scheduleRangeInHours.Hours())).Return(map[scheduler.JobName]*scheduler.JobLineageSummary{jobAName: lineageA, jobBName: lineageB}, nil)
		durationEstimator.On("GetPercentileDurationByJobNames", ctx, referenceTime, mock.Anything).
			Return(map[scheduler.JobName]*time.Duration{
				jobAName: func() *time.Duration { d := 30 * time.Minute; return &d }(),
				jobBName: func() *time.Duration { d := 30 * time.Minute; return &d }(),
			}, nil)

		summary, err := svc.GenerateJobExpectedCompletionTimeReport(ctx, []scheduler.JobFilterRequest{
			{ProjectName: projectName, JobNames: []scheduler.JobName{jobAName, jobBName}},
		}, referenceTime, scheduleRangeInHours)

		assert.NoError(t, err)
		assert.Len(t, summary.Reports, 2)
		if assert.NotNil(t, summary.MeanDelay) {
			assert.Equal(t, 11*time.Minute, *summary.MeanDelay)
		}
	})

	t.Run("MeanDelay should be nil when no job has an ActualFinishTime yet", func(t *testing.T) {
		svc, _, jobDetailsGetter, jobLineageFetcher, durationEstimator := newService()

		projectName := tenant.ProjectName("project-a")
		jobAName := scheduler.JobName("job-A")
		scheduledAt := referenceTime.Add(scheduleRangeInHours - 1*time.Hour).Truncate(time.Hour)
		jobA := makeJobWithDetails(projectName, jobAName, scheduledAt)
		lineageA := &scheduler.JobLineageSummary{
			JobName: jobAName, IsEnabled: true,
			JobRuns:   map[scheduler.JobName]*scheduler.JobRunSummary{jobAName: {JobName: jobAName, ScheduledAt: scheduledAt}},
			Upstreams: []*scheduler.JobLineageSummary{},
		}

		jobDetailsGetter.On("GetJobs", ctx, projectName, []string{jobAName.String()}).Return([]*scheduler.JobWithDetails{jobA}, nil)
		jobLineageFetcher.On("GetJobLineage", ctx, map[scheduler.JobName]*scheduler.JobSchedule{jobAName: {JobName: jobAName, ScheduledAt: scheduledAt}}, int(scheduleRangeInHours.Hours())).
			Return(map[scheduler.JobName]*scheduler.JobLineageSummary{jobAName: lineageA}, nil)
		durationEstimator.On("GetPercentileDurationByJobNames", ctx, referenceTime, []scheduler.JobName{jobAName}).
			Return(map[scheduler.JobName]*time.Duration{jobAName: func() *time.Duration { d := 30 * time.Minute; return &d }()}, nil)

		summary, err := svc.GenerateJobExpectedCompletionTimeReport(ctx, []scheduler.JobFilterRequest{
			{ProjectName: projectName, JobNames: []scheduler.JobName{jobAName}},
		}, referenceTime, scheduleRangeInHours)

		assert.NoError(t, err)
		assert.Nil(t, summary.Reports[0].ActualFinishTime)
		assert.Nil(t, summary.MeanDelay)
	})

	t.Run("a failing combo is skipped without discarding the succeeding combo's reports, but errors when nothing succeeds", func(t *testing.T) {
		svc, _, jobDetailsGetter, jobLineageFetcher, durationEstimator := newService()

		projectA := tenant.ProjectName("project-a")
		projectB := tenant.ProjectName("project-b")
		jobAName := scheduler.JobName("job-A")
		jobBName := scheduler.JobName("job-B")
		scheduledAt := referenceTime.Add(scheduleRangeInHours - 1*time.Hour).Truncate(time.Hour)

		jobA := makeJobWithDetails(projectA, jobAName, scheduledAt)
		jobB := makeJobWithDetails(projectB, jobBName, scheduledAt)
		lineageA := &scheduler.JobLineageSummary{
			JobName: jobAName, IsEnabled: true,
			JobRuns:   map[scheduler.JobName]*scheduler.JobRunSummary{jobAName: {JobName: jobAName, ScheduledAt: scheduledAt}},
			Upstreams: []*scheduler.JobLineageSummary{},
		}

		jobDetailsGetter.On("GetJobs", ctx, projectA, []string{jobAName.String()}).Return([]*scheduler.JobWithDetails{jobA}, nil).Once()
		jobDetailsGetter.On("GetJobs", ctx, projectB, []string{jobBName.String()}).Return([]*scheduler.JobWithDetails{jobB}, nil).Once()
		jobLineageFetcher.On("GetJobLineage", ctx, map[scheduler.JobName]*scheduler.JobSchedule{jobAName: {JobName: jobAName, ScheduledAt: scheduledAt}}, int(scheduleRangeInHours.Hours())).
			Return(map[scheduler.JobName]*scheduler.JobLineageSummary{jobAName: lineageA}, nil).Once()
		jobLineageFetcher.On("GetJobLineage", ctx, map[scheduler.JobName]*scheduler.JobSchedule{jobBName: {JobName: jobBName, ScheduledAt: scheduledAt}}, int(scheduleRangeInHours.Hours())).
			Return(nil, errors.New("lineage service unavailable")).Once()
		durationEstimator.On("GetPercentileDurationByJobNames", ctx, referenceTime, []scheduler.JobName{jobAName}).
			Return(map[scheduler.JobName]*time.Duration{jobAName: func() *time.Duration { d := 30 * time.Minute; return &d }()}, nil).Once()

		summary, err := svc.GenerateJobExpectedCompletionTimeReport(ctx, []scheduler.JobFilterRequest{
			{ProjectName: projectA, JobNames: []scheduler.JobName{jobAName}},
			{ProjectName: projectB, JobNames: []scheduler.JobName{jobBName}},
		}, referenceTime, scheduleRangeInHours)

		assert.NoError(t, err, "partial success should not surface an error")
		assert.Len(t, summary.Reports, 1)
		assert.Equal(t, jobAName, summary.Reports[0].JobName)
	})

	t.Run("all combos failing returns an error with an empty report", func(t *testing.T) {
		svc, _, jobDetailsGetter, _, _ := newService()

		projectName := tenant.ProjectName("project-a")
		jobAName := scheduler.JobName("job-A")

		jobDetailsGetter.On("GetJobs", ctx, projectName, []string{jobAName.String()}).Return(nil, errors.New("service unavailable"))

		summary, err := svc.GenerateJobExpectedCompletionTimeReport(ctx, []scheduler.JobFilterRequest{
			{ProjectName: projectName, JobNames: []scheduler.JobName{jobAName}},
		}, referenceTime, scheduleRangeInHours)

		assert.Error(t, err)
		assert.Empty(t, summary.Reports)
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
