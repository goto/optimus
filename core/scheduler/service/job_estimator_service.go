package service

import (
	"context"
	"time"

	"github.com/goto/salt/log"

	"github.com/goto/optimus/core/scheduler"
	"github.com/goto/optimus/core/tenant"
)

type JobRunDetailsRepository interface {
	UpsertEstimatedFinishTime(ctx context.Context, projectName tenant.ProjectName, jobName scheduler.JobName, scheduledAt, estimatedFinishTime time.Time) error
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
		if jobSchedule == nil { // safety check
			s.l.Warn("nil job schedule provided, cannot calculate estimated finish time")
			continue
		}
		key := *jobSchedule
		if _, ok := jobRunEstimatedFinishTimes[key]; ok { // already calculated
			continue
		}
		if _, ok := jobsWithLineageMap[jobSchedule.JobName]; !ok { // safety check
			s.l.Warn("no lineage found for job, cannot calculate estimated finish time", "job", jobSchedule.JobName)
			continue
		}
		s.l.Debug("calculating estimated finish time for job", "job", jobSchedule.JobName, "scheduled_at", jobSchedule.ScheduledAt)
		err := s.PopulateEstimatedFinishTime(jobSchedule, jobsWithLineageMap[jobSchedule.JobName], jobRunEstimatedFinishTimes, jobsWithLineageMap, jobDurationsEstimation, referenceTime)
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

	// estimated finish time generated for target jobs
	finalJobRunEstimatedFinishTimes := make(map[scheduler.JobSchedule]time.Time)
	for _, jobSchedule := range jobSchedules {
		key := *jobSchedule
		estimatedFinishTime, ok := jobRunEstimatedFinishTimes[key]
		if !ok {
			s.l.Warn("estimated finish time not found for job schedule", "job", jobSchedule.JobName, "scheduled_at", jobSchedule.ScheduledAt)
			continue
		}
		finalJobRunEstimatedFinishTimes[key] = estimatedFinishTime
	}

	return finalJobRunEstimatedFinishTimes, nil
}

func (s *JobEstimatorService) PopulateEstimatedFinishTime(jobTarget *scheduler.JobSchedule, currentJobWithLineage *scheduler.JobLineageSummary, jobRunEstimatedFinishTimes map[scheduler.JobSchedule]time.Time, jobsWithLineageMap map[scheduler.JobName]*scheduler.JobLineageSummary, jobDurationsEstimation map[scheduler.JobName]*time.Duration, referenceTime time.Time) error {
	if currentJobWithLineage == nil || currentJobWithLineage.JobRuns[jobTarget.JobName] == nil {
		s.l.Warn("no job run found for job, skipping estimated finish time calculation", "job", currentJobWithLineage.JobName)
		return nil
	}
	currentJobRun := currentJobWithLineage.JobRuns[jobTarget.JobName]
	currentJobScheduleKey := scheduler.JobSchedule{
		JobName:     currentJobWithLineage.JobName,
		ScheduledAt: currentJobRun.ScheduledAt,
	}
	estimatedDuration, ok := jobDurationsEstimation[currentJobWithLineage.JobName]
	if !ok || estimatedDuration == nil {
		// if no estimation found, we cannot proceed
		s.l.Warn("no duration estimation found for job, cannot calculate estimated finish time", "job", currentJobWithLineage.JobName)
		return nil
	}

	// termination condition
	// 1. cache if already calculated
	if _, ok := jobRunEstimatedFinishTimes[currentJobScheduleKey]; ok {
		s.l.Debug("estimated finish time already calculated for job, skipping", "job", currentJobWithLineage.JobName, "scheduled_at", currentJobRun.ScheduledAt)
		return nil
	}
	// 2. if end_time is nil and scheduled_time+duration<ref_time+buffer_time
	jobEndTime := currentJobRun.JobEndTime
	if jobEndTime == nil && currentJobRun.ScheduledAt.Add(*estimatedDuration).Before(referenceTime.Add(s.bufferTime)) {
		// running late
		s.l.Debug("job is running late, setting estimated finish time to reference time + buffer time", "job", currentJobWithLineage.JobName, "scheduled_at", currentJobRun.ScheduledAt)
		jobRunEstimatedFinishTimes[currentJobScheduleKey] = referenceTime.Add(s.bufferTime)
		return nil
	}
	// 3. if end_time is not nil
	if jobEndTime != nil {
		s.l.Debug("job has already ended, setting estimated finish time to job end time", "job", currentJobWithLineage.JobName, "scheduled_at", currentJobRun.ScheduledAt)
		jobRunEstimatedFinishTimes[currentJobScheduleKey] = *jobEndTime
		return nil
	}

	// calculate max upstream estimated finish time
	maxUpstreamEstimatedFinishTime := currentJobRun.ScheduledAt
	// avoid cyclic loop by temporarily setting the current job's estimated finish time
	jobRunEstimatedFinishTimes[currentJobScheduleKey] = maxUpstreamEstimatedFinishTime.Add(*estimatedDuration)
	for _, upstream := range jobsWithLineageMap[currentJobWithLineage.JobName].Upstreams {
		if upstream.JobRuns[jobTarget.JobName] == nil {
			s.l.Debug("no upstream job run found for job, skipping upstream in estimated finish time calculation", "job", currentJobWithLineage.JobName, "upstream_job", upstream.JobName)
			continue
		}
		err := s.PopulateEstimatedFinishTime(jobTarget, upstream, jobRunEstimatedFinishTimes, jobsWithLineageMap, jobDurationsEstimation, referenceTime)
		if err != nil {
			return err
		}

		upstreamScheduleKey := scheduler.JobSchedule{
			JobName:     upstream.JobName,
			ScheduledAt: upstream.JobRuns[jobTarget.JobName].ScheduledAt,
		}
		upstreamEstimatedFinishTime := jobRunEstimatedFinishTimes[upstreamScheduleKey]
		maxUpstreamEstimatedFinishTime = maxTime(maxUpstreamEstimatedFinishTime, upstreamEstimatedFinishTime)
	}

	jobRunEstimatedFinishTimes[currentJobScheduleKey] = maxUpstreamEstimatedFinishTime.Add(*estimatedDuration)

	return nil
}

func maxTime(t1, t2 time.Time) time.Time {
	if t1.After(t2) {
		return t1
	}
	return t2
}
