package service

import (
	"context"
	"fmt"
	"time"

	"github.com/goto/salt/log"

	"github.com/goto/optimus/core/scheduler"
	"github.com/goto/optimus/core/tenant"
)

type FinishTimeStatus string

const (
	FinishTimeStatusInprogress FinishTimeStatus = "inprogress"
	FinishTimeStatusFinished   FinishTimeStatus = "finished"
)

type FinishTimeDetail struct {
	Status     FinishTimeStatus
	FinishTime time.Time
}

type JobRunExpectationDetailsRepository interface {
	UpsertExpectedFinishTime(ctx context.Context, projectName tenant.ProjectName, jobName scheduler.JobName, scheduledAt, expectedFinishTime time.Time) error
}

type JobExpectatorService struct {
	l                            log.Logger
	bufferTime                   time.Duration
	jobRunExpectationDetailsRepo JobRunExpectationDetailsRepository
	jobDetailsGetter             JobDetailsGetter
	jobLineageFetcher            JobLineageFetcher
	durationEstimator            DurationEstimator
}

func NewJobExpectatorService(
	logger log.Logger,
	jobRunExpectationDetailsRepo JobRunExpectationDetailsRepository,
	jobDetailsGetter JobDetailsGetter,
	jobLineageFetcher JobLineageFetcher,
	durationEstimator DurationEstimator,
) *JobExpectatorService {
	return &JobExpectatorService{
		l:                            logger,
		bufferTime:                   10 * time.Minute, // TODO: make this configurable
		jobRunExpectationDetailsRepo: jobRunExpectationDetailsRepo,
		jobDetailsGetter:             jobDetailsGetter,
		jobLineageFetcher:            jobLineageFetcher,
		durationEstimator:            durationEstimator,
	}
}

func (s *JobExpectatorService) GenerateExpectedFinishTimes(ctx context.Context, projectName tenant.ProjectName, jobNames []scheduler.JobName, labels map[string]string, referenceTime time.Time, scheduleRangeInHours time.Duration) (map[scheduler.JobSchedule]FinishTimeDetail, error) {
	jobRunExpectedFinishTimeDetail := make(map[scheduler.JobSchedule]FinishTimeDetail)

	if len(jobNames) == 0 && len(labels) == 0 {
		s.l.Warn("no job names or labels provided, skipping expected finish time generation")
		return jobRunExpectedFinishTimeDetail, nil
	}

	// fetch job details
	jobsWithDetails, err := getJobWithDetails(ctx, s.l, s.jobDetailsGetter, projectName, jobNames, labels)
	if err != nil {
		return nil, err
	}
	if len(jobsWithDetails) == 0 {
		return jobRunExpectedFinishTimeDetail, nil
	}

	// get scheduled at
	jobSchedules := getJobSchedules(s.l, jobsWithDetails, scheduleRangeInHours, referenceTime)
	if len(jobSchedules) == 0 {
		s.l.Warn("no job schedules found for the given jobs in the next schedule range, skipping expected finish time generation")
		return jobRunExpectedFinishTimeDetail, nil
	}

	// get lineage
	jobsWithLineageMap, err := s.jobLineageFetcher.GetJobLineage(ctx, jobSchedules)
	if err != nil {
		s.l.Error(fmt.Sprintf("failed to get job lineage, skipping expected finish time generation: %s", err.Error()))
		return nil, err
	}

	uniqueJobNames := collectJobNames(jobsWithLineageMap)

	// get job durations estimation
	jobDurationsEstimation, err := s.durationEstimator.GetPercentileDurationByJobNames(ctx, referenceTime, uniqueJobNames)
	if err != nil {
		s.l.Error(fmt.Sprintf("failed to estimate job durations, skipping expected finish time generation: %s", err.Error()))
		return nil, err
	}

	// calculate expected finish time for each job
	for _, jobSchedule := range jobSchedules {
		if jobSchedule == nil { // safety check
			s.l.Warn("nil job schedule provided, cannot calculate expected finish time")
			continue
		}
		key := *jobSchedule
		if _, ok := jobRunExpectedFinishTimeDetail[key]; ok { // already calculated
			continue
		}
		if _, ok := jobsWithLineageMap[jobSchedule.JobName]; !ok { // safety check
			s.l.Warn(fmt.Sprintf("no lineage found for job [%s], cannot calculate expected finish time", jobSchedule.JobName))
			continue
		}
		s.l.Debug("calculating expected finish time for job", "job", jobSchedule.JobName, "scheduled_at", jobSchedule.ScheduledAt)
		err := s.PopulateExpectedFinishTime(jobSchedule, jobsWithLineageMap[jobSchedule.JobName], jobRunExpectedFinishTimeDetail, jobDurationsEstimation, referenceTime)
		if err != nil {
			s.l.Error(fmt.Sprintf("failed to populate expected finish time for job [%s]: %s", jobSchedule.JobName, err.Error()))
			return nil, err
		}
	}

	// save to db
	for _, jobSchedule := range jobSchedules {
		key := *jobSchedule
		expectedFinishTimeDetail, ok := jobRunExpectedFinishTimeDetail[key]
		if !ok {
			s.l.Warn(fmt.Sprintf("expected finish time not found for job schedule [job: %s, scheduled_at: %s]", jobSchedule.JobName, jobSchedule.ScheduledAt))
			continue
		}
		// only upsert if still in progress
		if expectedFinishTimeDetail.Status == FinishTimeStatusInprogress {
			s.l.Info(fmt.Sprintf("expected finish time calculated [job: %s, scheduled_at: %s, expected_finish_time: %s, status: %s]", jobSchedule.JobName, jobSchedule.ScheduledAt, expectedFinishTimeDetail.FinishTime, expectedFinishTimeDetail.Status))
			err := s.jobRunExpectationDetailsRepo.UpsertExpectedFinishTime(ctx, projectName, jobSchedule.JobName, jobSchedule.ScheduledAt, expectedFinishTimeDetail.FinishTime)
			if err != nil {
				s.l.Error(fmt.Sprintf("failed to upsert expected finish time for job schedule [job: %s, scheduled_at: %s, error: %s]", jobSchedule.JobName, jobSchedule.ScheduledAt, err.Error()))
				return nil, err
			}
		}
	}

	// expected finish time generated for target jobs
	finalJobRunExpectedFinishTimes := make(map[scheduler.JobSchedule]FinishTimeDetail)
	for _, jobSchedule := range jobSchedules {
		key := *jobSchedule
		expectedFinishTime, ok := jobRunExpectedFinishTimeDetail[key]
		if !ok {
			s.l.Warn(fmt.Sprintf("expected finish time not found for job schedule [job: %s, scheduled_at: %s]", jobSchedule.JobName, jobSchedule.ScheduledAt))
			continue
		}
		finalJobRunExpectedFinishTimes[key] = expectedFinishTime
	}

	return finalJobRunExpectedFinishTimes, nil
}

func (s *JobExpectatorService) PopulateExpectedFinishTime(jobTarget *scheduler.JobSchedule, currentJobWithLineage *scheduler.JobLineageSummary, jobRunExpectedFinishTimes map[scheduler.JobSchedule]FinishTimeDetail, jobDurationsEstimation map[scheduler.JobName]*time.Duration, referenceTime time.Time) error {
	// pre condition check
	if currentJobWithLineage == nil || currentJobWithLineage.JobRuns[jobTarget.JobName] == nil {
		s.l.Warn(fmt.Sprintf("no job run found for job [%s], skipping expected finish time calculation", currentJobWithLineage.JobName))
		return nil
	}
	if !currentJobWithLineage.IsEnabled {
		s.l.Debug(fmt.Sprintf("job is disabled, skipping expected finish time calculation [%s]", currentJobWithLineage.JobName))
		return nil
	}

	currentJobRun := currentJobWithLineage.JobRuns[jobTarget.JobName]
	currentJobScheduleKey := scheduler.JobSchedule{
		JobName:     currentJobWithLineage.JobName,
		ScheduledAt: currentJobRun.ScheduledAt,
	}

	jobStartTime := currentJobRun.JobStartTime
	jobEndTime := currentJobRun.JobEndTime

	// termination condition: 1. if start_time is not nil and end_time is not nil
	if jobStartTime != nil && jobEndTime != nil {
		// if job has already ended, we can set the expected finish time to job end time
		s.l.Debug(fmt.Sprintf("job has already ended, setting expected finish time to job end time [job: %s, scheduled_at: %s]", currentJobWithLineage.JobName, currentJobRun.ScheduledAt))
		jobRunExpectedFinishTimes[currentJobScheduleKey] = FinishTimeDetail{
			Status:     FinishTimeStatusFinished,
			FinishTime: *jobEndTime,
		}
		return nil
	}

	// get estimated duration, once we know the job is not finished yet
	// this information is needed to calculate expected finish time
	estimatedDuration, ok := jobDurationsEstimation[currentJobWithLineage.JobName]
	if !ok || estimatedDuration == nil {
		// if no estimation found, we cannot proceed
		s.l.Warn(fmt.Sprintf("no duration estimation found for job [%s], cannot calculate expected finish time", currentJobWithLineage.JobName))
		// rest of the logic can still work with 0 duration, which means expected finish time will be the same as max upstream expected finish time.
		// this is a better approach than skipping expected finish time calculation entirely, as we can still provide some expected finish time estimation based on upstream jobs,
		// rather than having no estimation at all.
		zeroDuration := time.Duration(0)
		estimatedDuration = &zeroDuration
	}

	// termination condition: 2. cache if already calculated
	if _, ok := jobRunExpectedFinishTimes[currentJobScheduleKey]; ok {
		s.l.Debug(fmt.Sprintf("expected finish time already calculated for job [%s], skipping", currentJobWithLineage.JobName))
		return nil
	}
	// termination condition: 3. if start_time is not nil, end_time is nil, and scheduled_time+duration<ref_time
	if jobStartTime != nil && jobEndTime == nil && currentJobRun.ScheduledAt.Add(*estimatedDuration).Before(referenceTime) {
		// running late
		s.l.Debug(fmt.Sprintf("job is running late, setting expected finish time to reference time + buffer time [job: %s, scheduled_at: %s]", currentJobWithLineage.JobName, currentJobRun.ScheduledAt))
		jobRunExpectedFinishTimes[currentJobScheduleKey] = FinishTimeDetail{
			Status:     FinishTimeStatusInprogress,
			FinishTime: referenceTime.Add(s.bufferTime),
		}
		return nil
	}

	// calculate max upstream expected finish time
	maxUpstreamExpectedFinishTime := currentJobRun.ScheduledAt
	// avoid cyclic loop by temporarily setting the current job's expected finish time
	jobRunExpectedFinishTimes[currentJobScheduleKey] = FinishTimeDetail{
		Status:     FinishTimeStatusInprogress,
		FinishTime: maxUpstreamExpectedFinishTime.Add(*estimatedDuration),
	}
	for _, upstream := range currentJobWithLineage.Upstreams {
		if upstream.JobRuns[jobTarget.JobName] == nil {
			s.l.Debug(fmt.Sprintf("no upstream job run found for job, skipping upstream in expected finish time calculation [job: %s, upstream_job: %s]", currentJobWithLineage.JobName, upstream.JobName))
			continue
		}
		err := s.PopulateExpectedFinishTime(jobTarget, upstream, jobRunExpectedFinishTimes, jobDurationsEstimation, referenceTime)
		if err != nil {
			return err
		}

		upstreamScheduleKey := scheduler.JobSchedule{
			JobName:     upstream.JobName,
			ScheduledAt: upstream.JobRuns[jobTarget.JobName].ScheduledAt,
		}
		upstreamExpectedFinishTime, ok := jobRunExpectedFinishTimes[upstreamScheduleKey]
		if !ok {
			s.l.Warn(fmt.Sprintf("expected finish time not found for upstream job, skipping in expected finish time calculation [job: %s, upstream_job: %s]", currentJobWithLineage.JobName, upstream.JobName))
			continue
		}
		maxUpstreamExpectedFinishTime = maxTime(maxUpstreamExpectedFinishTime, upstreamExpectedFinishTime.FinishTime)
	}

	expectedFinishedTime := maxUpstreamExpectedFinishTime.Add(*estimatedDuration)
	// if start_time is nil and scheduled_time+duration<ref_time
	if jobStartTime == nil && currentJobRun.ScheduledAt.Add(*estimatedDuration).Before(referenceTime) {
		// job is not started yet
		s.l.Debug(fmt.Sprintf("job is not started yet but expected to have finished, setting expected finish time to reference time + buffer time [job: %s, scheduled_at: %s]", currentJobWithLineage.JobName, currentJobRun.ScheduledAt))
		expectedFinishedTime = referenceTime.Add(s.bufferTime) // need to add buffer time to give some room for the job to start
	}

	jobRunExpectedFinishTimes[currentJobScheduleKey] = FinishTimeDetail{
		Status:     FinishTimeStatusInprogress,
		FinishTime: expectedFinishedTime,
	}

	return nil
}

func maxTime(t1, t2 time.Time) time.Time {
	if t1.After(t2) {
		return t1
	}
	return t2
}
