package service

import (
	"context"
	"time"

	"github.com/goto/optimus/core/scheduler"
	"github.com/goto/optimus/core/tenant"
	"github.com/goto/optimus/internal/lib/cron"
	"github.com/goto/optimus/internal/lib/interval"
)

type ExternalOptimusManager interface {
	GetJobScheduleInterval(ctx context.Context, upstreamHost string, tnnt tenant.Tenant, jobName scheduler.JobName) (string, error)
	GetJobRuns(ctx context.Context, upstreamHost string, sensorParams scheduler.JobSensorParameters, criteria *scheduler.JobRunsCriteria) ([]*scheduler.JobRunStatus, error)
}

func (s *JobRunService) getUpstreamJobInterval(ctx context.Context, upstreamHost string, sensorParameters scheduler.JobSensorParameters) (string, error) {
	if len(upstreamHost) != 0 {
		return s.externalOptimusManager.GetJobScheduleInterval(ctx, upstreamHost, sensorParameters.UpstreamTenant, sensorParameters.UpstreamJobName)
	}
	upstreamJobWithDetails, err := s.jobRepo.GetJobDetails(ctx, sensorParameters.UpstreamTenant.ProjectName(), sensorParameters.UpstreamJobName)
	if err != nil {
		return "", err
	}
	return upstreamJobWithDetails.Schedule.Interval, nil
}

func getLatestUpstreamJobScheduleTime(upstreamJobCronInterval string, baseJobScheduleTime time.Time) (time.Time, error) {
	jobCron, err := cron.ParseCronSchedule(upstreamJobCronInterval)
	if err != nil {
		return time.Time{}, err
	}

	// 1 Second Time is added because the upstream job might be scheduled at the same time as the base job
	return jobCron.Prev(baseJobScheduleTime.Add(time.Second)), nil
}

func (s *JobRunService) getWindowInterval(ctx context.Context, upstreamScheduleInterval string, sensorParameters scheduler.JobSensorParameters) (interval.Interval, error) {
	latestUpstreamJobScheduleTime, err := getLatestUpstreamJobScheduleTime(upstreamScheduleInterval, sensorParameters.ScheduledTime)
	if err != nil {
		return interval.Interval{}, err
	}

	return s.GetInterval(ctx, sensorParameters.SubjectProjectName, sensorParameters.SubjectJobName, latestUpstreamJobScheduleTime)
}

func (s *JobRunService) GetUpstreamJobRuns(ctx context.Context, upstreamHost string, sensorParameters scheduler.JobSensorParameters, filter []string) ([]*scheduler.JobRunStatus, error) {
	upstreamScheduleInterval, err := s.getUpstreamJobInterval(ctx, upstreamHost, sensorParameters)
	if err != nil {
		return nil, err
	}

	jobWindowInterval, err := s.getWindowInterval(ctx, upstreamScheduleInterval, sensorParameters)
	if err != nil {
		return nil, err
	}

	runCriteria := &scheduler.JobRunsCriteria{
		Name:      sensorParameters.UpstreamJobName.String(),
		StartDate: jobWindowInterval.Start(),
		EndDate:   jobWindowInterval.End(),
		Filter:    filter,
	}

	if len(upstreamHost) != 0 {
		return s.externalOptimusManager.GetJobRuns(ctx, upstreamHost, sensorParameters, runCriteria)
	}
	return s.GetJobRuns(ctx, sensorParameters.UpstreamTenant.ProjectName(), sensorParameters.UpstreamJobName, runCriteria)
}
