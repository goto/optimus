package service

import (
	"context"
	"fmt"
	"time"

	"github.com/goto/salt/log"

	"github.com/goto/optimus/core/scheduler"
	"github.com/goto/optimus/core/tenant"
	"github.com/goto/optimus/internal/errors"
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
	bufferDuration               time.Duration
	jobRunExpectationDetailsRepo JobRunExpectationDetailsRepository
	jobDetailsGetter             JobDetailsGetter
	jobLineageFetcher            JobLineageFetcher
	durationEstimator            DurationEstimator
}

func NewJobExpectatorService(
	logger log.Logger,
	bufferDurationInMinutes int,
	jobRunExpectationDetailsRepo JobRunExpectationDetailsRepository,
	jobDetailsGetter JobDetailsGetter,
	jobLineageFetcher JobLineageFetcher,
	durationEstimator DurationEstimator,
) *JobExpectatorService {
	return &JobExpectatorService{
		l:                            logger,
		bufferDuration:               time.Duration(bufferDurationInMinutes) * time.Minute,
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
	jobsWithLineageMap, err := s.jobLineageFetcher.GetJobLineage(ctx, jobSchedules, int(scheduleRangeInHours.Hours()))
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
		err := s.PopulateExpectedFinishTime(jobSchedule.JobName, jobsWithLineageMap[jobSchedule.JobName], jobRunExpectedFinishTimeDetail, jobDurationsEstimation, referenceTime)
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

// selfParent is the immediate downstream job that led to currentJobWithLineage in the current
// traversal - currentJobWithLineage's own run is keyed by that name in JobRuns, since
// LineageResolver.BuildLineage keys a node's JobRuns by its immediate downstream
// to support lineages where a shared upstream carries a distinct run per downstream path.
func (s *JobExpectatorService) PopulateExpectedFinishTime(selfParent scheduler.JobName, currentJobWithLineage *scheduler.JobLineageSummary, jobRunExpectedFinishTimes map[scheduler.JobSchedule]FinishTimeDetail, jobDurationsEstimation map[scheduler.JobName]*time.Duration, referenceTime time.Time) error {
	// pre condition check
	if currentJobWithLineage == nil || currentJobWithLineage.GetRunForJob(selfParent) == nil {
		// TODO: add metric to track how many times this happens
		s.l.Error(fmt.Sprintf("[critical] no job run found for job [%s], skipping expected finish time calculation", currentJobWithLineage.JobName))
		return nil
	}
	if !currentJobWithLineage.IsEnabled {
		s.l.Debug(fmt.Sprintf("job is disabled, skipping expected finish time calculation [%s]", currentJobWithLineage.JobName))
		return nil
	}

	currentJobRun := currentJobWithLineage.GetRunForJob(selfParent)
	currentJobScheduleKey := scheduler.JobSchedule{
		// TODO: add project name as well, PR: https://github.com/goto/optimus/pull/501
		JobName:     currentJobWithLineage.JobName,
		ScheduledAt: currentJobRun.ScheduledAt,
	}

	taskStartTime := currentJobRun.TaskStartTime
	jobEndTime := currentJobRun.JobEndTime

	// termination condition: 1. if end_time is not nil
	if jobEndTime != nil {
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
	// estimationDuration already has buffer time included, so we don't need to add extra buffer time in the expected finish time calculation
	estimatedDuration, ok := jobDurationsEstimation[currentJobWithLineage.JobName]
	if !ok || estimatedDuration == nil {
		// if no estimation found, we cannot proceed
		s.l.Warn(fmt.Sprintf("no duration estimation found for job [%s], cannot calculate expected finish time", currentJobWithLineage.JobName))
		// rest of the logic can still work with buffer duration, which means expected finish time will be the same as max upstream expected finish time.
		// this is a better approach than skipping expected finish time calculation entirely, as we can still provide some expected finish time estimation based on upstream jobs,
		// rather than having no estimation at all.
		estimatedDuration = &s.bufferDuration // use buffer time as default duration
	}

	// termination condition: 2. cache if already calculated
	if _, ok := jobRunExpectedFinishTimes[currentJobScheduleKey]; ok {
		s.l.Debug(fmt.Sprintf("expected finish time already calculated for job [%s], skipping", currentJobWithLineage.JobName))
		return nil
	}

	if taskStartTime != nil {
		// termination condition: 3. if start_time is not nil, end_time is nil, and scheduled_time+duration<ref_time
		if taskStartTime.Add(*estimatedDuration).Before(referenceTime) {
			// running late
			s.l.Debug(fmt.Sprintf("job is running late, setting expected finish time to reference time + buffer time [job: %s, scheduled_at: %s]", currentJobWithLineage.JobName, currentJobRun.ScheduledAt))
			jobRunExpectedFinishTimes[currentJobScheduleKey] = FinishTimeDetail{
				Status:     FinishTimeStatusInprogress,
				FinishTime: referenceTime.Add(s.bufferDuration),
			}
			return nil
		}
		// termination condition: 4. if start_time is not nil
		// job already started but not running late
		s.l.Debug(fmt.Sprintf("job already started but not running late, setting expected finish time to task start time + estimated duration + buffer time [job: %s, scheduled_at: %s]", currentJobWithLineage.JobName, currentJobRun.ScheduledAt))
		jobRunExpectedFinishTimes[currentJobScheduleKey] = FinishTimeDetail{
			Status:     FinishTimeStatusInprogress,
			FinishTime: taskStartTime.Add(*estimatedDuration),
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
		err := s.PopulateExpectedFinishTime(currentJobWithLineage.JobName, upstream, jobRunExpectedFinishTimes, jobDurationsEstimation, referenceTime)
		if err != nil {
			return err
		}
		upstreamJobRun := upstream.GetRunForJob(currentJobWithLineage.JobName)
		if upstream.JobRuns[currentJobWithLineage.JobName] == nil {
			s.l.Debug(fmt.Sprintf("no upstream job run found for job, skipping upstream in expected finish time calculation [job: %s, upstream_job: %s]", currentJobWithLineage.JobName, upstream.JobName))
			continue
		}
		upstreamScheduleKey := scheduler.JobSchedule{
			JobName:     upstream.JobName,
			ScheduledAt: upstreamJobRun.ScheduledAt,
		}
		upstreamExpectedFinishTime, ok := jobRunExpectedFinishTimes[upstreamScheduleKey]
		if !ok {
			s.l.Warn(fmt.Sprintf("expected finish time not found for upstream job, skipping in expected finish time calculation [job: %s, upstream_job: %s]", currentJobWithLineage.JobName, upstream.JobName))
			continue
		}
		maxUpstreamExpectedFinishTime = maxTime(maxUpstreamExpectedFinishTime, upstreamExpectedFinishTime.FinishTime)
	}

	expectedFinishedTime := maxUpstreamExpectedFinishTime.Add(*estimatedDuration)
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

func (s *JobExpectatorService) GenerateJobExpectedCompletionTimeReport(ctx context.Context, reqs []scheduler.JobFilterRequest, referenceTime time.Time, scheduleRangeInHours time.Duration) (*scheduler.JobCompletionTimeSummary, error) {
	var allReports scheduler.JobCompletionTimeReports
	me := errors.NewMultiError("GenerateJobExpectedCompletionTimeReport")
	for _, req := range reqs {
		reports, err := s.computeCompletionTimeReports(ctx, req, referenceTime, scheduleRangeInHours)
		if err != nil {
			s.l.Error(fmt.Sprintf("failed to compute completion time report for combo, skipping [project: %s]: %s", req.ProjectName.String(), err.Error()))
			me.Append(err)
			continue
		}
		allReports = append(allReports, reports...)
	}
	if len(allReports) == 0 && len(reqs) > 0 {
		return nil, me.ToErr()
	}

	return &scheduler.JobCompletionTimeSummary{
		Reports:   allReports,
		MeanDelay: allReports.ComputeMeanDelay(),
	}, nil
}

func (s *JobExpectatorService) computeCompletionTimeReports(ctx context.Context, req scheduler.JobFilterRequest, referenceTime time.Time, scheduleRangeInHours time.Duration) ([]scheduler.JobCompletionTimeReport, error) {
	jobsWithDetails, err := s.getJobWithDetails(ctx, req)
	if err != nil {
		return nil, err
	}
	if len(jobsWithDetails) == 0 {
		return nil, nil
	}

	jobSchedules := getJobSchedules(s.l, jobsWithDetails, scheduleRangeInHours, referenceTime)
	if len(jobSchedules) == 0 {
		return nil, nil
	}

	jobsWithLineageMap, err := s.jobLineageFetcher.GetJobLineage(ctx, jobSchedules, int(scheduleRangeInHours.Hours()))
	if err != nil {
		return nil, err
	}

	// filter out schedules & lineages which is already finished & invalid ones
	unfinishedJobSchedules := []scheduler.JobSchedule{}
	unfinishedJobsWithLineageMap := map[scheduler.JobName]*scheduler.JobLineageSummary{}
	actualFinishTimes := map[scheduler.JobSchedule]*time.Time{}
	for _, jobSchedule := range jobSchedules {
		if jobSchedule == nil {
			continue
		}

		lineage, ok := jobsWithLineageMap[jobSchedule.JobName]
		if !ok {
			continue
		}

		if run := lineage.GetRunForJob(jobSchedule.JobName); run != nil && (run.GetActualEndTime() == nil ||
			run.GetActualEndTime().After(referenceTime)) {

			unfinishedJobSchedules = append(unfinishedJobSchedules, *jobSchedule)
			unfinishedJobsWithLineageMap[jobSchedule.JobName] = lineage
			actualFinishTimes[*jobSchedule] = run.GetActualEndTime()
		}
	}

	scheduler.ClipLineageRunsToReferenceTime(unfinishedJobsWithLineageMap, referenceTime)

	uniqueJobNames := collectJobNames(unfinishedJobsWithLineageMap)
	jobDurationsEstimation, err := s.durationEstimator.GetPercentileDurationByJobNames(ctx, referenceTime, uniqueJobNames)
	if err != nil {
		return nil, err
	}

	jobRunExpectedFinishTimeDetail := map[scheduler.JobSchedule]FinishTimeDetail{}
	for _, jobSchedule := range unfinishedJobSchedules {
		if _, ok := jobRunExpectedFinishTimeDetail[jobSchedule]; ok {
			continue
		}
		lineage, ok := unfinishedJobsWithLineageMap[jobSchedule.JobName]
		if !ok {
			s.l.Warn(fmt.Sprintf("no lineage found for job [%s], cannot calculate expected finish time", jobSchedule.JobName))
			continue
		}
		if err := s.PopulateExpectedFinishTime(jobSchedule.JobName, lineage, jobRunExpectedFinishTimeDetail, jobDurationsEstimation, referenceTime); err != nil {
			return nil, err
		}
	}

	reports := make([]scheduler.JobCompletionTimeReport, 0, len(unfinishedJobSchedules))
	for _, jobSchedule := range unfinishedJobSchedules {
		detail, ok := jobRunExpectedFinishTimeDetail[jobSchedule]
		if !ok {
			s.l.Warn(fmt.Sprintf("expected finish time not found for job schedule [job: %s, scheduled_at: %s]", jobSchedule.JobName, jobSchedule.ScheduledAt))
			continue
		}
		reports = append(reports, scheduler.JobCompletionTimeReport{
			ProjectName:        req.ProjectName,
			JobName:            jobSchedule.JobName,
			ScheduledAt:        jobSchedule.ScheduledAt,
			ExpectedFinishTime: detail.FinishTime,
			ActualFinishTime:   actualFinishTimes[jobSchedule],
		})
	}

	return reports, nil
}

func (s *JobExpectatorService) getJobWithDetails(ctx context.Context, req scheduler.JobFilterRequest) ([]*scheduler.JobWithDetails, error) {
	filteredJobMerged := map[scheduler.JobName]*scheduler.JobWithDetails{}

	if len(req.JobNames) > 0 {
		jobNames := make([]string, 0, len(req.JobNames))
		for _, jn := range req.JobNames {
			jobNames = append(jobNames, jn.String())
		}
		jobsWithDetails, err := s.jobDetailsGetter.GetJobs(ctx, req.ProjectName, jobNames)
		if err != nil {
			if jobsWithDetails == nil {
				return nil, err
			}
			s.l.Error("[getJobWithDetailsMultiValueLabels] encountered non-blocking error when fetching jobs by names: %s", err.Error())
		}
		for _, job := range jobsWithDetails {
			filteredJobMerged[job.Name] = job
		}
	}

	if len(req.Labels) > 0 {
		jobsWithDetails, err := s.jobDetailsGetter.GetJobsByLabelsMultiValue(ctx, req.ProjectName, req.Labels)
		if err != nil {
			if jobsWithDetails == nil {
				return nil, err
			}
			s.l.Error("[getJobWithDetailsMultiValueLabels] encountered non-blocking error when fetching jobs by labels: %s", err.Error())
		}
		for _, job := range jobsWithDetails {
			filteredJobMerged[job.Name] = job
		}
	}

	filteredJobs := make([]*scheduler.JobWithDetails, 0, len(filteredJobMerged))
	for _, job := range filteredJobMerged {
		filteredJobs = append(filteredJobs, job)
	}
	return filteredJobs, nil
}
