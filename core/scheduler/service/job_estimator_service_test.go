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

func TestGenerateEstimatedFinishTimes(t *testing.T) {
	ctx := context.Background()
	projectName := tenant.ProjectName("project-a")
	referenceTime := time.Now()
	scheduleRangeInHours := 10 * time.Hour
	l := log.NewNoop()

	t.Run("given no jobs, should return empty map", func(t *testing.T) {
		// given
		jobRunDetailsRepo := NewJobRunDetailsRepository(t)
		jobDetailsGetter := NewJobDetailsGetter(t)
		jobLineageFetcher := NewJobLineageFetcher(t)
		durationEstimator := NewDurationEstimator(t)

		jobEstimatorService := service.NewJobEstimatorService(
			l,
			jobRunDetailsRepo,
			jobDetailsGetter,
			jobLineageFetcher,
			durationEstimator,
		)

		// when
		estimatedFinishTimes, err := jobEstimatorService.GenerateEstimatedFinishTimes(ctx, projectName, []scheduler.JobName{}, map[string]string{}, referenceTime, scheduleRangeInHours)

		// then
		assert.NoError(t, err)
		assert.Empty(t, estimatedFinishTimes)
	})

	t.Run("given jobs, when get job detail error, return error", func(t *testing.T) {
		// given
		jobRunDetailsRepo := NewJobRunDetailsRepository(t)
		jobDetailsGetter := NewJobDetailsGetter(t)
		jobLineageFetcher := NewJobLineageFetcher(t)
		durationEstimator := NewDurationEstimator(t)

		jobEstimatorService := service.NewJobEstimatorService(
			l,
			jobRunDetailsRepo,
			jobDetailsGetter,
			jobLineageFetcher,
			durationEstimator,
		)

		jobAName := scheduler.JobName("job-A")

		jobDetailsGetter.On("GetJobs", ctx, projectName, []string{jobAName.String()}).Return([]*scheduler.JobWithDetails{}, errors.New("some error"))

		// when
		estimatedFinishTimes, err := jobEstimatorService.GenerateEstimatedFinishTimes(ctx, projectName, []scheduler.JobName{jobAName}, map[string]string{}, referenceTime, scheduleRangeInHours)

		// then
		assert.Nil(t, estimatedFinishTimes)
		assert.EqualError(t, err, "some error")
	})

	t.Run("given job label, when get job detail error, return error", func(t *testing.T) {
		// given
		jobRunDetailsRepo := NewJobRunDetailsRepository(t)
		jobDetailsGetter := NewJobDetailsGetter(t)
		jobLineageFetcher := NewJobLineageFetcher(t)
		durationEstimator := NewDurationEstimator(t)

		jobEstimatorService := service.NewJobEstimatorService(
			l,
			jobRunDetailsRepo,
			jobDetailsGetter,
			jobLineageFetcher,
			durationEstimator,
		)

		labels := map[string]string{"category": "some-category"}

		jobDetailsGetter.On("GetJobsByLabels", ctx, projectName, labels).Return([]*scheduler.JobWithDetails{}, errors.New("some error"))

		// when
		estimatedFinishTimes, err := jobEstimatorService.GenerateEstimatedFinishTimes(ctx, projectName, []scheduler.JobName{}, labels, referenceTime, scheduleRangeInHours)

		// then
		assert.Nil(t, estimatedFinishTimes)
		assert.EqualError(t, err, "some error")
	})

	t.Run("given jobs, with no job details, should return empty map", func(t *testing.T) {
		// given
		jobRunDetailsRepo := NewJobRunDetailsRepository(t)
		jobDetailsGetter := NewJobDetailsGetter(t)
		jobLineageFetcher := NewJobLineageFetcher(t)
		durationEstimator := NewDurationEstimator(t)

		jobEstimatorService := service.NewJobEstimatorService(
			l,
			jobRunDetailsRepo,
			jobDetailsGetter,
			jobLineageFetcher,
			durationEstimator,
		)

		jobAName := scheduler.JobName("job-A")

		jobDetailsGetter.On("GetJobs", ctx, projectName, []string{jobAName.String()}).Return([]*scheduler.JobWithDetails{}, nil)

		// when
		estimatedFinishTimes, err := jobEstimatorService.GenerateEstimatedFinishTimes(ctx, projectName, []scheduler.JobName{jobAName}, map[string]string{}, referenceTime, scheduleRangeInHours)

		// then
		assert.NoError(t, err)
		assert.Empty(t, estimatedFinishTimes)
	})

	t.Run("given job, with no job schedules, should return empty map", func(t *testing.T) {
		// given
		jobRunDetailsRepo := NewJobRunDetailsRepository(t)
		jobDetailsGetter := NewJobDetailsGetter(t)
		jobLineageFetcher := NewJobLineageFetcher(t)
		durationEstimator := NewDurationEstimator(t)

		jobEstimatorService := service.NewJobEstimatorService(
			l,
			jobRunDetailsRepo,
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
		estimatedFinishTimes, err := jobEstimatorService.GenerateEstimatedFinishTimes(ctx, projectName, []scheduler.JobName{jobAName}, map[string]string{}, referenceTime, scheduleRangeInHours)

		// then
		assert.NoError(t, err)
		assert.Empty(t, estimatedFinishTimes)
	})

	t.Run("given job, with job schedules, when get lineage error, return error", func(t *testing.T) {
		// given
		jobRunDetailsRepo := NewJobRunDetailsRepository(t)
		jobDetailsGetter := NewJobDetailsGetter(t)
		jobLineageFetcher := NewJobLineageFetcher(t)
		durationEstimator := NewDurationEstimator(t)

		jobEstimatorService := service.NewJobEstimatorService(
			l,
			jobRunDetailsRepo,
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
		estimatedFinishTimes, err := jobEstimatorService.GenerateEstimatedFinishTimes(ctx, projectName, []scheduler.JobName{jobAName}, map[string]string{}, referenceTime, scheduleRangeInHours)

		// then
		assert.Nil(t, estimatedFinishTimes)
		assert.EqualError(t, err, "some error")
	})

	t.Run("given job, with job schedules and lineage, when estimate duration error, return error", func(t *testing.T) {
		// given
		jobRunDetailsRepo := NewJobRunDetailsRepository(t)
		jobDetailsGetter := NewJobDetailsGetter(t)
		jobLineageFetcher := NewJobLineageFetcher(t)
		durationEstimator := NewDurationEstimator(t)

		jobEstimatorService := service.NewJobEstimatorService(
			l,
			jobRunDetailsRepo,
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
			Upstreams: []*scheduler.JobLineageSummary{},
		}

		jobDetailsGetter.On("GetJobs", ctx, projectName, []string{jobAName.String()}).Return([]*scheduler.JobWithDetails{jobWithDetails}, nil)
		jobLineageFetcher.On("GetJobLineage", ctx, map[scheduler.JobName]*scheduler.JobSchedule{jobAName: {JobName: jobAName, ScheduledAt: scheduledAt}}).Return(map[scheduler.JobName]*scheduler.JobLineageSummary{jobAName: jobLineageSummary}, nil)
		durationEstimator.On("GetPercentileDurationByJobNames", ctx, referenceTime, []scheduler.JobName{jobAName}).Return(nil, errors.New("some error"))

		// when
		estimatedFinishTimes, err := jobEstimatorService.GenerateEstimatedFinishTimes(ctx, projectName, []scheduler.JobName{jobAName}, map[string]string{}, referenceTime, scheduleRangeInHours)

		// then
		assert.Nil(t, estimatedFinishTimes)
		assert.EqualError(t, err, "some error")
	})

	t.Run("given job, with job schedules, lineage and duration estimation, should return estimated finish time", func(t *testing.T) {
		// given
		jobRunDetailsRepo := NewJobRunDetailsRepository(t)
		jobDetailsGetter := NewJobDetailsGetter(t)
		jobLineageFetcher := NewJobLineageFetcher(t)
		durationEstimator := NewDurationEstimator(t)

		jobEstimatorService := service.NewJobEstimatorService(
			l,
			jobRunDetailsRepo,
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
			JobName: jobAName,
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
		jobRunDetailsRepo.On("UpsertEstimatedFinishTime", ctx, projectName, jobAName, scheduledAt, scheduledAt.Add(30*time.Minute)).Return(nil)

		// when
		estimatedFinishTimes, err := jobEstimatorService.GenerateEstimatedFinishTimes(ctx, projectName, []scheduler.JobName{jobAName}, map[string]string{}, referenceTime, scheduleRangeInHours)

		// then
		assert.NoError(t, err)
		expectedEstimatedFinishTime := scheduledAt.Add(30 * time.Minute)
		assert.Equal(t, map[scheduler.JobSchedule]time.Time{{JobName: jobAName, ScheduledAt: scheduledAt}: expectedEstimatedFinishTime}, estimatedFinishTimes)
	})
}

func TestPopulateEstimatedFinishTime(t *testing.T) {
	l := log.NewNoop()
	referenceTime := time.Now()
	scheduleRangeInHours := 10 * time.Hour
	bufferTime := 10 * time.Minute

	t.Run("when no current job run exists, should skip", func(t *testing.T) {
		// given
		jobRunDetailsRepo := NewJobRunDetailsRepository(t)
		jobDetailsGetter := NewJobDetailsGetter(t)
		jobLineageFetcher := NewJobLineageFetcher(t)
		durationEstimator := NewDurationEstimator(t)

		jobEstimatorService := service.NewJobEstimatorService(
			l,
			jobRunDetailsRepo,
			jobDetailsGetter,
			jobLineageFetcher,
			durationEstimator,
		)

		jobRunEstimatedFinishTime := map[scheduler.JobSchedule]time.Time{}
		jobWithLineageMap := map[scheduler.JobName]*scheduler.JobLineageSummary{}
		jobDurationEstimation := map[scheduler.JobName]*time.Duration{}

		scheduledAt := referenceTime.Add(scheduleRangeInHours - 1*time.Hour).Truncate(time.Hour)
		jobTarget := &scheduler.JobSchedule{
			JobName:     scheduler.JobName("job-A"),
			ScheduledAt: scheduledAt,
		}
		currentJobWithLineage := &scheduler.JobLineageSummary{
			JobName:   jobTarget.JobName,
			JobRuns:   map[scheduler.JobName]*scheduler.JobRunSummary{}, // no current job run
			Upstreams: []*scheduler.JobLineageSummary{},
		}
		jobWithLineageMap[jobTarget.JobName] = currentJobWithLineage
		jobDurationEstimation[jobTarget.JobName] = func() *time.Duration { d := 30 * time.Minute; return &d }()

		// when
		err := jobEstimatorService.PopulateEstimatedFinishTime(jobTarget, currentJobWithLineage, jobRunEstimatedFinishTime, jobWithLineageMap, jobDurationEstimation, referenceTime)

		// then
		assert.NoError(t, err)
		assert.Empty(t, jobRunEstimatedFinishTime)
	})

	t.Run("when duration estimation not found, should skip", func(t *testing.T) {
		// given
		jobRunDetailsRepo := NewJobRunDetailsRepository(t)
		jobDetailsGetter := NewJobDetailsGetter(t)
		jobLineageFetcher := NewJobLineageFetcher(t)
		durationEstimator := NewDurationEstimator(t)

		jobEstimatorService := service.NewJobEstimatorService(
			l,
			jobRunDetailsRepo,
			jobDetailsGetter,
			jobLineageFetcher,
			durationEstimator,
		)
		jobRunEstimatedFinishTime := map[scheduler.JobSchedule]time.Time{}
		jobWithLineageMap := map[scheduler.JobName]*scheduler.JobLineageSummary{}
		jobDurationEstimation := map[scheduler.JobName]*time.Duration{}

		scheduledAt := referenceTime.Add(scheduleRangeInHours - 1*time.Hour).Truncate(time.Hour)
		jobTarget := &scheduler.JobSchedule{
			JobName:     scheduler.JobName("job-A"),
			ScheduledAt: scheduledAt,
		}
		currentJobWithLineage := &scheduler.JobLineageSummary{
			JobName: jobTarget.JobName,
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
		err := jobEstimatorService.PopulateEstimatedFinishTime(jobTarget, currentJobWithLineage, jobRunEstimatedFinishTime, jobWithLineageMap, jobDurationEstimation, referenceTime)

		// then
		assert.NoError(t, err)
		assert.Empty(t, jobRunEstimatedFinishTime)
	})

	t.Run("when estimated finish time already calculated, should skip", func(t *testing.T) {
		// given
		jobRunDetailsRepo := NewJobRunDetailsRepository(t)
		jobDetailsGetter := NewJobDetailsGetter(t)
		jobLineageFetcher := NewJobLineageFetcher(t)
		durationEstimator := NewDurationEstimator(t)

		jobEstimatorService := service.NewJobEstimatorService(
			l,
			jobRunDetailsRepo,
			jobDetailsGetter,
			jobLineageFetcher,
			durationEstimator,
		)
		jobRunEstimatedFinishTime := map[scheduler.JobSchedule]time.Time{}
		jobWithLineageMap := map[scheduler.JobName]*scheduler.JobLineageSummary{}
		jobDurationEstimation := map[scheduler.JobName]*time.Duration{}

		scheduledAt := referenceTime.Add(scheduleRangeInHours - 1*time.Hour).Truncate(time.Hour)
		jobTarget := &scheduler.JobSchedule{
			JobName:     scheduler.JobName("job-A"),
			ScheduledAt: scheduledAt,
		}
		currentJobWithLineage := &scheduler.JobLineageSummary{
			JobName: jobTarget.JobName,
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
		jobRunEstimatedFinishTime[*jobTarget] = scheduledAt.Add(25 * time.Minute)

		// when
		err := jobEstimatorService.PopulateEstimatedFinishTime(jobTarget, currentJobWithLineage, jobRunEstimatedFinishTime, jobWithLineageMap, jobDurationEstimation, referenceTime)
		// then
		assert.NoError(t, err)
		// should not be updated
		assert.Equal(t, scheduledAt.Add(25*time.Minute), jobRunEstimatedFinishTime[*jobTarget])
	})

	t.Run("when end_time is nil and running late, should set estimated finish time to reference time + buffer", func(t *testing.T) {
		// given
		jobRunDetailsRepo := NewJobRunDetailsRepository(t)
		jobDetailsGetter := NewJobDetailsGetter(t)
		jobLineageFetcher := NewJobLineageFetcher(t)
		durationEstimator := NewDurationEstimator(t)

		jobEstimatorService := service.NewJobEstimatorService(
			l,
			jobRunDetailsRepo,
			jobDetailsGetter,
			jobLineageFetcher,
			durationEstimator,
		)
		jobRunEstimatedFinishTime := map[scheduler.JobSchedule]time.Time{}
		jobWithLineageMap := map[scheduler.JobName]*scheduler.JobLineageSummary{}
		jobDurationEstimation := map[scheduler.JobName]*time.Duration{}

		scheduledAt := referenceTime.Add(-1 * time.Hour) // scheduled in the past
		jobTarget := &scheduler.JobSchedule{
			JobName:     scheduler.JobName("job-A"),
			ScheduledAt: scheduledAt,
		}
		currentJobWithLineage := &scheduler.JobLineageSummary{
			JobName: jobTarget.JobName,
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
		err := jobEstimatorService.PopulateEstimatedFinishTime(jobTarget, currentJobWithLineage, jobRunEstimatedFinishTime, jobWithLineageMap, jobDurationEstimation, referenceTime)

		// then
		assert.NoError(t, err)
		expectedEstimatedFinishTime := referenceTime.Add(bufferTime)
		assert.Equal(t, expectedEstimatedFinishTime, jobRunEstimatedFinishTime[*jobTarget])
	})

	t.Run("when end_time is not nil, should set estimated finish time to job end time", func(t *testing.T) {
		// given
		jobRunDetailsRepo := NewJobRunDetailsRepository(t)
		jobDetailsGetter := NewJobDetailsGetter(t)
		jobLineageFetcher := NewJobLineageFetcher(t)
		durationEstimator := NewDurationEstimator(t)

		jobEstimatorService := service.NewJobEstimatorService(
			l,
			jobRunDetailsRepo,
			jobDetailsGetter,
			jobLineageFetcher,
			durationEstimator,
		)
		jobRunEstimatedFinishTime := map[scheduler.JobSchedule]time.Time{}
		jobWithLineageMap := map[scheduler.JobName]*scheduler.JobLineageSummary{}
		jobDurationEstimation := map[scheduler.JobName]*time.Duration{}

		scheduledAt := referenceTime.Add(-1 * time.Hour) // scheduled in the past
		jobEndTime := referenceTime.Add(-30 * time.Minute)
		jobTarget := &scheduler.JobSchedule{
			JobName:     scheduler.JobName("job-A"),
			ScheduledAt: scheduledAt,
		}
		currentJobWithLineage := &scheduler.JobLineageSummary{
			JobName: jobTarget.JobName,
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
		err := jobEstimatorService.PopulateEstimatedFinishTime(jobTarget, currentJobWithLineage, jobRunEstimatedFinishTime, jobWithLineageMap, jobDurationEstimation, referenceTime)

		// then
		assert.NoError(t, err)
		assert.Equal(t, jobEndTime, jobRunEstimatedFinishTime[*jobTarget])
	})

	t.Run("when targeted job will run in the future, should set estimated finish time to scheduled at + estimated duration", func(t *testing.T) {
		// given
		jobRunDetailsRepo := NewJobRunDetailsRepository(t)
		jobDetailsGetter := NewJobDetailsGetter(t)
		jobLineageFetcher := NewJobLineageFetcher(t)
		durationEstimator := NewDurationEstimator(t)

		jobEstimatorService := service.NewJobEstimatorService(
			l,
			jobRunDetailsRepo,
			jobDetailsGetter,
			jobLineageFetcher,
			durationEstimator,
		)
		jobRunEstimatedFinishTime := map[scheduler.JobSchedule]time.Time{}
		jobWithLineageMap := map[scheduler.JobName]*scheduler.JobLineageSummary{}
		jobDurationEstimation := map[scheduler.JobName]*time.Duration{}

		scheduledAt := referenceTime.Add(1 * time.Hour) // scheduled in the future
		jobTarget := &scheduler.JobSchedule{
			JobName:     scheduler.JobName("job-A"),
			ScheduledAt: scheduledAt,
		}
		currentJobWithLineage := &scheduler.JobLineageSummary{
			JobName: jobTarget.JobName,
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
		err := jobEstimatorService.PopulateEstimatedFinishTime(jobTarget, currentJobWithLineage, jobRunEstimatedFinishTime, jobWithLineageMap, jobDurationEstimation, referenceTime)

		// then
		assert.NoError(t, err)
		expectedEstimatedFinishTime := scheduledAt.Add(30 * time.Minute)
		assert.Equal(t, expectedEstimatedFinishTime, jobRunEstimatedFinishTime[*jobTarget])
	})

	t.Run("when targeted job will run in the future, and there's an upstream job running late, should set estimated finish time to max(upstream estimated finish time, scheduled_at) + estimated duration", func(t *testing.T) {
		// given
		jobRunDetailsRepo := NewJobRunDetailsRepository(t)
		jobDetailsGetter := NewJobDetailsGetter(t)
		jobLineageFetcher := NewJobLineageFetcher(t)
		durationEstimator := NewDurationEstimator(t)

		jobEstimatorService := service.NewJobEstimatorService(
			l,
			jobRunDetailsRepo,
			jobDetailsGetter,
			jobLineageFetcher,
			durationEstimator,
		)
		jobRunEstimatedFinishTime := map[scheduler.JobSchedule]time.Time{}
		jobWithLineageMap := map[scheduler.JobName]*scheduler.JobLineageSummary{}
		jobDurationEstimation := map[scheduler.JobName]*time.Duration{}

		scheduledAt := referenceTime.Add(1 * time.Hour)          // scheduled in the future
		upstreamScheduledAt := referenceTime.Add(-1 * time.Hour) // upstream scheduled in the past
		jobTarget := &scheduler.JobSchedule{
			JobName:     scheduler.JobName("job-A"),
			ScheduledAt: scheduledAt,
		}
		currentJobWithLineage := &scheduler.JobLineageSummary{
			JobName: jobTarget.JobName,
			JobRuns: map[scheduler.JobName]*scheduler.JobRunSummary{
				jobTarget.JobName: {
					JobName:     jobTarget.JobName,
					ScheduledAt: scheduledAt,
				},
			},
			Upstreams: []*scheduler.JobLineageSummary{},
		}
		jobUpstreamWithLineage := &scheduler.JobLineageSummary{
			JobName: scheduler.JobName("job-B"),
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
		err := jobEstimatorService.PopulateEstimatedFinishTime(jobTarget, currentJobWithLineage, jobRunEstimatedFinishTime, jobWithLineageMap, jobDurationEstimation, referenceTime)

		// then
		assert.NoError(t, err)
		expectedEstimatedFinishTime := scheduledAt.Add(30 * time.Minute)
		assert.Equal(t, expectedEstimatedFinishTime, jobRunEstimatedFinishTime[*jobTarget])
	})

	t.Run("when targeted job will run in the future, and there's an upstream job running late, and estimated finish time for upstream is greater than scheduled_at, should set estimated finish time to max(upstream estimated finish time, scheduled_at) + estimated duration", func(t *testing.T) {
		// given
		jobRunDetailsRepo := NewJobRunDetailsRepository(t)
		jobDetailsGetter := NewJobDetailsGetter(t)
		jobLineageFetcher := NewJobLineageFetcher(t)
		durationEstimator := NewDurationEstimator(t)

		jobEstimatorService := service.NewJobEstimatorService(
			l,
			jobRunDetailsRepo,
			jobDetailsGetter,
			jobLineageFetcher,
			durationEstimator,
		)
		jobRunEstimatedFinishTime := map[scheduler.JobSchedule]time.Time{}
		jobWithLineageMap := map[scheduler.JobName]*scheduler.JobLineageSummary{}
		jobDurationEstimation := map[scheduler.JobName]*time.Duration{}

		scheduledAt := referenceTime.Add(5 * time.Minute)        // scheduled in the future
		upstreamScheduledAt := referenceTime.Add(-1 * time.Hour) // upstream scheduled in the past
		jobTarget := &scheduler.JobSchedule{
			JobName:     scheduler.JobName("job-A"),
			ScheduledAt: scheduledAt,
		}
		currentJobWithLineage := &scheduler.JobLineageSummary{
			JobName: jobTarget.JobName,
			JobRuns: map[scheduler.JobName]*scheduler.JobRunSummary{
				jobTarget.JobName: {
					JobName:     jobTarget.JobName,
					ScheduledAt: scheduledAt,
				},
			},
			Upstreams: []*scheduler.JobLineageSummary{},
		}
		jobUpstreamWithLineage := &scheduler.JobLineageSummary{
			JobName: scheduler.JobName("job-B"),
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
		err := jobEstimatorService.PopulateEstimatedFinishTime(jobTarget, currentJobWithLineage, jobRunEstimatedFinishTime, jobWithLineageMap, jobDurationEstimation, referenceTime)

		// then
		assert.NoError(t, err)
		expectedEstimatedFinishTime := referenceTime.Add(10 * time.Minute).Add(30 * time.Minute)
		assert.Equal(t, expectedEstimatedFinishTime, jobRunEstimatedFinishTime[*jobTarget])
	})
}

// JobRunDetailsRepository is an autogenerated mock type for the JobRunDetailsRepository type
type JobRunDetailsRepository struct {
	mock.Mock
}

// UpsertEstimatedFinishTime provides a mock function with given fields: ctx, projectName, jobName, scheduledAt, estimatedFinishTime
func (_m *JobRunDetailsRepository) UpsertEstimatedFinishTime(ctx context.Context, projectName tenant.ProjectName, jobName scheduler.JobName, scheduledAt, estimatedFinishTime time.Time) error {
	ret := _m.Called(ctx, projectName, jobName, scheduledAt, estimatedFinishTime)

	if len(ret) == 0 {
		panic("no return value specified for UpsertEstimatedFinishTime")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, tenant.ProjectName, scheduler.JobName, time.Time, time.Time) error); ok {
		r0 = rf(ctx, projectName, jobName, scheduledAt, estimatedFinishTime)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// NewJobRunDetailsRepository creates a new instance of JobRunDetailsRepository. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewJobRunDetailsRepository(t interface {
	mock.TestingT
	Cleanup(func())
},
) *JobRunDetailsRepository {
	mock := &JobRunDetailsRepository{}
	mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
