package service

import (
	"context"
	"time"

	"github.com/goto/optimus/core/scheduler"
	"github.com/goto/optimus/core/tenant"
	"github.com/goto/salt/log"
)

type JobRunDetailsRepository interface {
	UpsertEstimatedFinishTime(ctx context.Context, projectName tenant.ProjectName, jobName scheduler.JobName, scheduledAt time.Time, estimatedFinishTime time.Time) error
}

type JobEstimatorService struct {
	l                 log.Logger
	bufferTime        time.Duration
	jobRunDetailsRepo JobRunDetailsRepository
	jobDetailsGetter  JobDetailsGetter
	jobLineageFetcher JobLineageFetcher
	durationEstimator DurationEstimator
}

func NewJobEstimatorService(
	logger log.Logger,
	jobRunDetailsRepo JobRunDetailsRepository,
	jobDetailsGetter JobDetailsGetter,
	jobLineageFetcher JobLineageFetcher,
	durationEstimator DurationEstimator,
) *JobEstimatorService {
	return &JobEstimatorService{
		l:                 logger,
		bufferTime:        10 * time.Minute, // TODO: make this configurable
		jobRunDetailsRepo: jobRunDetailsRepo,
		jobDetailsGetter:  jobDetailsGetter,
		jobLineageFetcher: jobLineageFetcher,
		durationEstimator: durationEstimator,
	}
}

func (s *JobEstimatorService) GenerateEstimatedFinishTimes(ctx context.Context, projectName tenant.ProjectName, jobNames []scheduler.JobName, labels map[string]string, referenceTime time.Time, scheduleRangeInHours time.Duration) (map[scheduler.JobSchedule]time.Time, error) {
	jobRunEstimatedFinishTimes := make(map[scheduler.JobSchedule]time.Time)

	if len(jobNames) == 0 && len(labels) == 0 {
		s.l.Warn("no job names or labels provided, skipping estimated finish time generation")
		return jobRunEstimatedFinishTimes, nil
	}

	// fetch job details
	jobsWithDetails, err := getJobWithDetails(ctx, s.l, s.jobDetailsGetter, projectName, jobNames, labels)
	if err != nil {
		return nil, err
	}
	if len(jobsWithDetails) == 0 {
		return jobRunEstimatedFinishTimes, nil
	}

	// get scheduled at
	jobSchedules := getJobSchedules(s.l, jobsWithDetails, scheduleRangeInHours, referenceTime)
	if len(jobSchedules) == 0 {
		s.l.Warn("no job schedules found for the given jobs in the next schedule range, skipping estimated finish time generation")
		return jobRunEstimatedFinishTimes, nil
	}

	// get lineage
	jobsWithLineageMap, err := s.jobLineageFetcher.GetJobLineage(ctx, jobSchedules)
	if err != nil {
		s.l.Error("failed to get job lineage, skipping estimated finish time generation", "error", err)
		return nil, err
	}

	uniqueJobNames := collectJobNames(jobsWithLineageMap)

	// get job durations estimation
	jobDurationsEstimation, err := s.durationEstimator.GetPercentileDurationByJobNames(ctx, referenceTime, uniqueJobNames)
	if err != nil {
		s.l.Error("failed to estimate job durations, skipping estimated finish time generation", "error", err)
		return nil, err
	}

	// calculate estimated finish time for each job
	for _, jobSchedule := range jobSchedules {
		key := *jobSchedule
		if _, ok := jobRunEstimatedFinishTimes[key]; ok { // already calculated
			continue
		}
		err := s.populateEstimatedFinishTime(ctx, jobSchedule, jobSchedule, jobRunEstimatedFinishTimes, jobsWithLineageMap, jobDurationsEstimation, referenceTime)
		if err != nil {
			s.l.Error("failed to populate estimated finish time for job", "job", jobSchedule.JobName, "error", err)
			return nil, err
		}
	}

	// save to db
	for _, jobSchedule := range jobSchedules {
		key := *jobSchedule
		estimatedFinishTime, ok := jobRunEstimatedFinishTimes[key]
		if !ok {
			s.l.Warn("estimated finish time not found for job schedule", "job", jobSchedule.JobName, "scheduled_at", jobSchedule.ScheduledAt)
			continue
		}
		s.l.Info("estimated finish time calculated", "job", jobSchedule.JobName, "scheduled_at", jobSchedule.ScheduledAt, "estimated_finish_time", estimatedFinishTime)
		err := s.jobRunDetailsRepo.UpsertEstimatedFinishTime(ctx, projectName, jobSchedule.JobName, jobSchedule.ScheduledAt, estimatedFinishTime)
		if err != nil {
			s.l.Error("failed to upsert estimated finish time for job schedule", "job", jobSchedule.JobName, "scheduled_at", jobSchedule.ScheduledAt, "error", err)
			return nil, err
		}
	}

	return jobRunEstimatedFinishTimes, nil
}

func (s *JobEstimatorService) populateEstimatedFinishTime(ctx context.Context, jobTarget, jobSchedule *scheduler.JobSchedule, jobRunEstimatedFinishTimes map[scheduler.JobSchedule]time.Time, jobsWithLineageMap map[scheduler.JobName]*scheduler.JobLineageSummary, jobDurationsEstimation map[scheduler.JobName]*time.Duration, referenceTime time.Time) error {
	key := *jobSchedule
	estimatedDuration, ok := jobDurationsEstimation[jobSchedule.JobName]
	if !ok {
		// if no estimation found, we cannot proceed
		s.l.Warn("no duration estimation found for job, cannot calculate estimated finish time", "job", jobSchedule.JobName)
		return nil
	}

	// termination condition
	// 1. cache if already calculated
	if _, ok := jobRunEstimatedFinishTimes[key]; ok {
		return nil
	}
	// 2. if end_time is nil and scheduled_time+duration<ref_time+buffer_time
	jobEndTime := jobsWithLineageMap[jobSchedule.JobName].JobRuns[jobTarget.JobName].JobEndTime
	if jobEndTime == nil && jobSchedule.ScheduledAt.Add(*estimatedDuration).Before(referenceTime.Add(s.bufferTime)) {
		// running late
		jobRunEstimatedFinishTimes[key] = referenceTime.Add(s.bufferTime)
		return nil
	}
	// 3. if end_time is not nil
	if jobEndTime != nil {
		jobRunEstimatedFinishTimes[key] = *jobEndTime
		return nil
	}

	// calculate max upstream estimated finish time
	maxUpstreamEstimatedFinishTime := jobSchedule.ScheduledAt
	for _, upstream := range jobsWithLineageMap[jobSchedule.JobName].Upstreams {
		upstreamSchedule := scheduler.JobSchedule{
			JobName:     upstream.JobName,
			ScheduledAt: upstream.JobRuns[jobTarget.JobName].ScheduledAt,
		}
		err := s.populateEstimatedFinishTime(ctx, jobTarget, &upstreamSchedule, jobRunEstimatedFinishTimes, jobsWithLineageMap, jobDurationsEstimation, referenceTime)
		if err != nil {
			return err
		}
		upstreamEstimatedFinishTime := jobRunEstimatedFinishTimes[upstreamSchedule]
		maxUpstreamEstimatedFinishTime = maxTime(maxUpstreamEstimatedFinishTime, upstreamEstimatedFinishTime)
	}

	jobRunEstimatedFinishTimes[key] = maxUpstreamEstimatedFinishTime.Add(*estimatedDuration)

	return nil
}

func maxTime(t1, t2 time.Time) time.Time {
	if t1.After(t2) {
		return t1
	}
	return t2
}
