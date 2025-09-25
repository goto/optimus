package service_test

import (
	"context"
	"testing"
	"time"

	"github.com/goto/optimus/core/scheduler"
	"github.com/goto/optimus/core/scheduler/service"
	"github.com/goto/salt/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestPredictJobSLAs(t *testing.T) {
	ctx := context.Background()
	l := log.NewNoop()
	t.Run("given 1 job that potentially breach due to upstream late, return job lineage with its path", func(t *testing.T) {
		// job-C -> job-B -> job-A
		// job-A is the target job
		// job-C is running late, so job-A might breach its SLA
		// | job | estimated duration |
		// |-----|--------------------|
		// | A   | 20 mins            | -> targeted SLA 30 mins from now
		// | B   | 15 mins            |
		// | C   | 10 mins            | -> running late by 15 mins (actual run time 25 mins)
		// given
		jobLineageFetcher := new(MockJobLineageFetcher)
		durationEstimator := new(MockDurationEstimator)
		jobSLAPredictorService := service.NewJobSLAPredictorService(l, 0*time.Minute, 7, jobLineageFetcher, durationEstimator)

		now := time.Now().Truncate(time.Second)
		targetedSLA := now.Add(30 * time.Minute)
		jobA := scheduler.JobName("job-A")
		jobB := scheduler.JobName("job-B")
		jobC := scheduler.JobName("job-C")
		// input job schedule
		jobASchedule := &scheduler.JobSchedule{
			JobName: jobA,
		}
		// mock job lineage fetcher
		jobALineage := &scheduler.JobLineageSummary{
			JobName: jobA,
			JobRuns: map[string]*scheduler.JobRunSummary{
				"job-A": {
					ScheduledAt: now,
				},
			},
		}
		jobBLineage := &scheduler.JobLineageSummary{
			JobName: jobB,
			JobRuns: map[string]*scheduler.JobRunSummary{
				"job-A": {
					ScheduledAt: now.Add(-15 * time.Minute),
				},
			},
		}
		jobCTaskStartTime := now.Add(-20 * time.Minute)
		jobCLineage := &scheduler.JobLineageSummary{
			JobName: jobC,
			JobRuns: map[string]*scheduler.JobRunSummary{
				"job-A": {
					ScheduledAt:   now.Add(-25 * time.Minute),
					TaskStartTime: &jobCTaskStartTime, // job-C is running, but not done yet
				},
			},
		}
		jobALineage.Upstreams = []*scheduler.JobLineageSummary{jobBLineage}
		jobBLineage.Upstreams = []*scheduler.JobLineageSummary{jobCLineage}
		jobLineageFetcher.On("GetJobLineage", ctx, []*scheduler.JobSchedule{jobASchedule}).Return([]*scheduler.JobLineageSummary{jobALineage}, nil).Once()
		// mock duration estimator
		durationEstimator.On("GetP95DurationByJobNames", ctx, mock.MatchedBy(func(jobNames []scheduler.JobName) bool {
			return assert.ElementsMatch(t, []scheduler.JobName{"job-A", "job-B", "job-C"}, jobNames)
		}), 7).Return(map[scheduler.JobName]time.Duration{
			"job-A": 20 * time.Minute, // target sla now + 30 mins
			"job-B": 15 * time.Minute, // inferred sla now + 10 mins
			"job-C": 10 * time.Minute, // inferred sla now - 5 mins
		}, nil).Once()

		// when
		jobBreachRootCause, _, err := jobSLAPredictorService.PredictJobSLAs(ctx, []*scheduler.JobSchedule{jobASchedule}, targetedSLA)
		// then
		assert.NoError(t, err)
		assert.Len(t, jobBreachRootCause, 1)
		assert.Equal(t, jobBreachRootCause, map[*scheduler.JobLineageSummary][]*scheduler.JobLineageSummary{
			jobALineage: {jobCLineage},
		})
	})
	t.Run("given 1 job that potentially breach due to upstream not started, return job lineage with its path", func(t *testing.T) {
		// job-C -> job-B -> job-A
		// job-A is the target job
		// job-C is done, but job-B is not started yet, so job-A might breach its SLA
		// | job | estimated duration |
		// |-----|--------------------|
		// | A   | 20 mins            | -> targeted SLA 30 mins from now
		// | B   | 15 mins            | -> not started yet
		// | C   | 10 mins            | -> done successfully
		// given
		jobLineageFetcher := new(MockJobLineageFetcher)
		durationEstimator := new(MockDurationEstimator)
		jobSLAPredictorService := service.NewJobSLAPredictorService(l, 0*time.Minute, 7, jobLineageFetcher, durationEstimator)

		now := time.Now().Truncate(time.Second)
		targetedSLA := now.Add(30 * time.Minute)
		jobA := scheduler.JobName("job-A")
		jobB := scheduler.JobName("job-B")
		jobC := scheduler.JobName("job-C")
		// input job schedule
		jobASchedule := &scheduler.JobSchedule{
			JobName: jobA,
		}
		// mock job lineage fetcher
		jobALineage := &scheduler.JobLineageSummary{
			JobName: jobA,
			JobRuns: map[string]*scheduler.JobRunSummary{
				"job-A": {
					ScheduledAt: now,
				},
			},
		}
		jobBLineage := &scheduler.JobLineageSummary{
			JobName: jobB,
			JobRuns: map[string]*scheduler.JobRunSummary{
				"job-A": {
					ScheduledAt: now.Add(-15 * time.Minute),
					// job-B is not started yet
				},
			},
		}
		jobCTaskStartTime := now.Add(-20 * time.Minute)
		jobCTaskEndTime := now.Add(-10 * time.Minute) // running 10 mins, done successfully
		jobCLineage := &scheduler.JobLineageSummary{
			JobName: jobC,
			JobRuns: map[string]*scheduler.JobRunSummary{
				"job-A": {
					ScheduledAt:   now.Add(-25 * time.Minute),
					TaskStartTime: &jobCTaskStartTime,
					TaskEndTime:   &jobCTaskEndTime,
				},
			},
		}
		jobALineage.Upstreams = []*scheduler.JobLineageSummary{jobBLineage}
		jobBLineage.Upstreams = []*scheduler.JobLineageSummary{jobCLineage}
		jobLineageFetcher.On("GetJobLineage", ctx, []*scheduler.JobSchedule{jobASchedule}).Return([]*scheduler.JobLineageSummary{jobALineage}, nil).Once()
		// mock duration estimator
		durationEstimator.On("GetP95DurationByJobNames", ctx, mock.MatchedBy(func(jobNames []scheduler.JobName) bool {
			return assert.ElementsMatch(t, []scheduler.JobName{"job-A", "job-B", "job-C"}, jobNames)
		}), 7).Return(map[scheduler.JobName]time.Duration{
			"job-A": 20 * time.Minute, // target sla now + 30 mins
			"job-B": 15 * time.Minute, // inferred sla now + 10 mins
			"job-C": 10 * time.Minute, // inferred sla now - 5 mins
		}, nil).Once()

		// when
		jobBreachRootCause, _, err := jobSLAPredictorService.PredictJobSLAs(ctx, []*scheduler.JobSchedule{jobASchedule}, targetedSLA)
		// then
		assert.NoError(t, err)
		assert.Len(t, jobBreachRootCause, 1)
		assert.Equal(t, jobBreachRootCause, map[*scheduler.JobLineageSummary][]*scheduler.JobLineageSummary{
			jobALineage: {jobBLineage},
		})
	})
	t.Run("given 1 job that no potentially breach, return no breaches", func(t *testing.T) {
		// job-C -> job-B -> job-A
		// job-A is the target job
		// | job | estimated duration | actual duration |
		// |-----|--------------------|-----------------|
		// | A   | 20 mins            | -               | -> targeted SLA 30 mins from now
		// | B   | 15 mins            | -               |
		// | C   | 10 mins            | 10 mins         | -> done successfully
		// given
		jobLineageFetcher := new(MockJobLineageFetcher)
		durationEstimator := new(MockDurationEstimator)
		jobSLAPredictorService := service.NewJobSLAPredictorService(l, 0*time.Minute, 7, jobLineageFetcher, durationEstimator)

		now := time.Now().Truncate(time.Second)
		targetedSLA := now.Add(30 * time.Minute)
		jobA := scheduler.JobName("job-A")
		jobB := scheduler.JobName("job-B")
		jobC := scheduler.JobName("job-C")
		// input job schedule
		jobASchedule := &scheduler.JobSchedule{
			JobName: jobA,
		}
		// mock job lineage fetcher
		jobALineage := &scheduler.JobLineageSummary{
			JobName: jobA,
			JobRuns: map[string]*scheduler.JobRunSummary{
				"job-A": {
					ScheduledAt: now,
				},
			},
		}
		jobBTaskStartTime := now.Add(-10 * time.Minute)
		jobBLineage := &scheduler.JobLineageSummary{
			JobName: jobB,
			JobRuns: map[string]*scheduler.JobRunSummary{
				"job-A": {
					ScheduledAt:   now.Add(-15 * time.Minute),
					TaskStartTime: &jobBTaskStartTime, // job-B is already started
				},
			},
		}
		jobCTaskStartTime := now.Add(-20 * time.Minute)
		jobCTaskEndTime := now.Add(-10 * time.Minute) // running 10 mins, done successfully
		jobCLineage := &scheduler.JobLineageSummary{
			JobName: jobC,
			JobRuns: map[string]*scheduler.JobRunSummary{
				"job-A": {
					ScheduledAt:   now.Add(-25 * time.Minute),
					TaskStartTime: &jobCTaskStartTime,
					TaskEndTime:   &jobCTaskEndTime,
				},
			},
		}
		jobALineage.Upstreams = []*scheduler.JobLineageSummary{jobBLineage}
		jobBLineage.Upstreams = []*scheduler.JobLineageSummary{jobCLineage}
		jobLineageFetcher.On("GetJobLineage", ctx, []*scheduler.JobSchedule{jobASchedule}).Return([]*scheduler.JobLineageSummary{jobALineage}, nil).Once()
		// mock duration estimator
		durationEstimator.On("GetP95DurationByJobNames", ctx, mock.MatchedBy(func(jobNames []scheduler.JobName) bool {
			return assert.ElementsMatch(t, []scheduler.JobName{"job-A", "job-B", "job-C"}, jobNames)
		}), 7).Return(map[scheduler.JobName]time.Duration{
			"job-A": 20 * time.Minute, // target sla now + 30 mins
			"job-B": 15 * time.Minute, // inferred sla now + 10 mins
			"job-C": 10 * time.Minute, // inferred sla now - 5 mins
		}, nil).Once()

		// when
		jobBreachRootCause, _, err := jobSLAPredictorService.PredictJobSLAs(ctx, []*scheduler.JobSchedule{jobASchedule}, targetedSLA)
		// then
		assert.NoError(t, err)
		assert.Len(t, jobBreachRootCause, 0)
	})
	t.Run("given 2 jobs that refer to the same upstream job, and only 1 job that has potential breach, return 1 job lineages with its path", func(t *testing.T) {
		// job-C -> job-B -> job-A1
		//       -> job-A2
		// job-A1 and job-A2 are the target jobs
		// job-C is running late, so job-A1 might breach its SLA, but job-A2 is safe
		// | job | estimated duration |
		// |-----|--------------------|
		// | A1  | 20 mins            | -> targeted SLA 30 mins from now
		// | A2  | 25 mins            | -> targeted SLA 30 mins from now
		// | B   | 15 mins            |
		// | C   | 10 mins            | -> running late by 15 mins (actual run time 25 mins)
		// given
		jobLineageFetcher := new(MockJobLineageFetcher)
		durationEstimator := new(MockDurationEstimator)
		jobSLAPredictorService := service.NewJobSLAPredictorService(l, 0*time.Minute, 7, jobLineageFetcher, durationEstimator)

		now := time.Now().Truncate(time.Second)
		targetedSLA := now.Add(30 * time.Minute)
		jobA1 := scheduler.JobName("job-A1")
		jobA2 := scheduler.JobName("job-A2")
		jobB := scheduler.JobName("job-B")
		jobC := scheduler.JobName("job-C")
		// input job schedule
		jobA1Schedule := &scheduler.JobSchedule{
			JobName: jobA1,
		}
		jobA2Schedule := &scheduler.JobSchedule{
			JobName: jobA2,
		}
		// mock job lineage fetcher
		jobA1Lineage := &scheduler.JobLineageSummary{
			JobName: jobA1,
			JobRuns: map[string]*scheduler.JobRunSummary{
				"job-A1": {
					ScheduledAt: now,
				},
			},
		}
		jobA2Lineage := &scheduler.JobLineageSummary{
			JobName: jobA2,
			JobRuns: map[string]*scheduler.JobRunSummary{
				"job-A2": {
					ScheduledAt: now,
				},
			},
		}
		jobBLineage := &scheduler.JobLineageSummary{
			JobName: jobB,
			JobRuns: map[string]*scheduler.JobRunSummary{
				"job-A1": {
					ScheduledAt: now.Add(-15 * time.Minute),
				},
			},
		}
		jobCTaskStartTime := now.Add(-20 * time.Minute)
		jobCLineage := &scheduler.JobLineageSummary{
			JobName: jobC,
			JobRuns: map[string]*scheduler.JobRunSummary{
				"job-A1": {
					ScheduledAt:   now.Add(-25 * time.Minute),
					TaskStartTime: &jobCTaskStartTime, // job-C is running, but not done yet
				},
				"job-A2": {
					ScheduledAt:   now.Add(-25 * time.Minute),
					TaskStartTime: &jobCTaskStartTime, // job-C is running, but not done yet
				},
			},
		}
		jobA1Lineage.Upstreams = []*scheduler.JobLineageSummary{jobBLineage}
		jobA2Lineage.Upstreams = []*scheduler.JobLineageSummary{jobCLineage}
		jobBLineage.Upstreams = []*scheduler.JobLineageSummary{jobCLineage}
		jobLineageFetcher.On("GetJobLineage", ctx, []*scheduler.JobSchedule{jobA1Schedule, jobA2Schedule}).Return([]*scheduler.JobLineageSummary{jobA1Lineage, jobA2Lineage}, nil).Once()
		// mock duration estimator
		durationEstimator.On("GetP95DurationByJobNames", ctx, mock.MatchedBy(func(jobNames []scheduler.JobName) bool {
			return assert.ElementsMatch(t, []scheduler.JobName{"job-A1", "job-A2", "job-B", "job-C"}, jobNames)
		}), 7).Return(map[scheduler.JobName]time.Duration{
			"job-A1": 20 * time.Minute, // target sla now + 30 mins
			"job-A2": 25 * time.Minute, // target sla now + 30 mins
			"job-B":  15 * time.Minute, // inferred sla w.r.t job-A1 now + 10 mins
			"job-C":  10 * time.Minute, // inferred sla w.r.t job-A1 now - 5 mins, w.r.t job-A2 now + 5 mins
		}, nil).Once()

		// when
		jobBreachRootCause, _, err := jobSLAPredictorService.PredictJobSLAs(ctx, []*scheduler.JobSchedule{jobA1Schedule, jobA2Schedule}, targetedSLA)
		// then
		assert.NoError(t, err)
		assert.Len(t, jobBreachRootCause, 1)
		assert.Equal(t, jobBreachRootCause, map[*scheduler.JobLineageSummary][]*scheduler.JobLineageSummary{
			jobA1Lineage: {jobCLineage},
		})
	})
}

type MockJobLineageFetcher struct {
	mock.Mock
}

func (m *MockJobLineageFetcher) GetJobLineage(
	ctx context.Context,
	jobSchedules []*scheduler.JobSchedule,
) ([]*scheduler.JobLineageSummary, error) {
	args := m.Called(ctx, jobSchedules)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]*scheduler.JobLineageSummary), args.Error(1)
}

type MockDurationEstimator struct {
	mock.Mock
}

func (m *MockDurationEstimator) GetP95DurationByJobNames(
	ctx context.Context,
	jobNames []scheduler.JobName,
	lastNDays int,
) (map[scheduler.JobName]time.Duration, error) {
	args := m.Called(ctx, jobNames, lastNDays)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(map[scheduler.JobName]time.Duration), args.Error(1)
}
