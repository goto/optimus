package service

import (
	"context"

	"github.com/goto/optimus/core/scheduler"
	"github.com/goto/optimus/core/tenant"
)

type JobGetter interface {
	GetJobs(ctx context.Context, projectName tenant.ProjectName, jobs []string) ([]*scheduler.JobWithDetails, error)
	GetJobsByLabels(ctx context.Context, projectName tenant.ProjectName, labels map[string]string) ([]*scheduler.JobWithDetails, error)
}

type JobScheduleService struct {
	jobGetter JobGetter
}

func NewJobScheduleService(jobGetter JobGetter) *JobScheduleService {
	return &JobScheduleService{jobGetter: jobGetter}
}

func (j *JobScheduleService) GetJobWithDetails(ctx context.Context, projectName tenant.ProjectName, jobNames []scheduler.JobName, labels map[string]string) ([]*scheduler.JobWithDetails, error) {
	filteredJobSchedules := []*scheduler.JobWithDetails{}

	if len(jobNames) == 0 && len(labels) == 0 {
		return filteredJobSchedules, nil
	}

	if len(jobNames) > 0 {
		jobNameStr := []string{}
		for _, jn := range jobNames {
			jobNameStr = append(jobNameStr, string(jn))
		}
		jobsWithDetails, err := j.jobGetter.GetJobs(ctx, projectName, jobNameStr)
		if err != nil {
			return nil, err
		}
		filteredJobSchedules = append(filteredJobSchedules, jobsWithDetails...)
	}

	if len(labels) > 0 {
		jobsWithDetails, err := j.jobGetter.GetJobsByLabels(ctx, projectName, labels)
		if err != nil {
			return nil, err
		}
		filteredJobSchedules = append(filteredJobSchedules, jobsWithDetails...)
	}

	return filteredJobSchedules, nil
}
