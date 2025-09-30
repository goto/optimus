package service

import (
	"context"
	"time"

	"github.com/goto/optimus/core/scheduler"
	"github.com/goto/optimus/core/tenant"
	"github.com/goto/optimus/internal/utils/filter"
)

type JobGetter interface {
	GetJobs(ctx context.Context, projectName tenant.ProjectName, jobs []string) ([]*scheduler.JobWithDetails, error)
	GetAll(ctx context.Context, projectName tenant.ProjectName) ([]*scheduler.JobWithDetails, error)
}

type JobScheduleService struct {
	jobGetter JobGetter
}

func NewJobScheduleService(jobGetter JobGetter) *JobScheduleService {
	return &JobScheduleService{jobGetter: jobGetter}
}

func (j *JobScheduleService) GetJobSchedulesByFilter(ctx context.Context, projectName tenant.ProjectName, nextScheduleRangeInHours time.Duration, opts ...filter.FilterOpt) ([]*scheduler.JobSchedule, error) {
	f := filter.NewFilter(opts...)
	var filteredJobSchedules []*scheduler.JobSchedule

	if f.Contains(filter.JobNames) && !f.Contains(filter.Labels) {
		jobsWithDetails, err := j.jobGetter.GetJobs(ctx, projectName, f.GetStringArrayValue(filter.JobNames))
		if err != nil {
			return nil, err
		}
		for _, j := range jobsWithDetails {
			if j.Schedule != nil {
				scheduledAt, err := j.Schedule.GetScheduleStartTime()
				if err != nil {
					return nil, err
				}
				filteredJobSchedules = append(filteredJobSchedules, &scheduler.JobSchedule{
					JobName:     j.Name,
					ScheduledAt: scheduledAt,
				})
			}
		}
	}

	if f.Contains(filter.Labels) {
		jobsWithDetails, err := j.jobGetter.GetAll(ctx, projectName)
		if err != nil {
			return nil, err
		}
		labels := f.GetMapArrayValue(filter.Labels)
		for _, j := range jobsWithDetails {
			for _, lbl := range labels {
				for k, v := range lbl {
					if _, ok := j.GetSafeLabels()[k]; !ok || j.GetSafeLabels()[k] != v {
						continue
					}
					if j.Schedule != nil {
						scheduledAt, err := j.Schedule.GetScheduleStartTime()
						if err != nil {
							return nil, err
						}
						filteredJobSchedules = append(filteredJobSchedules, &scheduler.JobSchedule{
							JobName:     j.Name,
							ScheduledAt: scheduledAt,
						})
					}
				}
			}
		}
		if f.Contains(filter.JobNames) {
			for _, j := range jobsWithDetails {
				for _, name := range f.GetStringArrayValue(filter.JobNames) {
					if j.Name == scheduler.JobName(name) {
						if j.Schedule != nil {
							scheduledAt, err := j.Schedule.GetScheduleStartTime()
							if err != nil {
								return nil, err
							}
							filteredJobSchedules = append(filteredJobSchedules, &scheduler.JobSchedule{
								JobName:     j.Name,
								ScheduledAt: scheduledAt,
							})
						}
					}
				}
			}
		}
	}

	// filter jobs with next schedule within next nextScheduleRangeInHours hours
	if nextScheduleRangeInHours > 0 {
		cutoffTime := time.Now().Add(nextScheduleRangeInHours)
		var tempJobSchedules []*scheduler.JobSchedule
		for _, js := range filteredJobSchedules {
			if js.ScheduledAt.Before(cutoffTime) || js.ScheduledAt.Equal(cutoffTime) {
				tempJobSchedules = append(tempJobSchedules, js)
			}
		}
		filteredJobSchedules = tempJobSchedules
	}

	return filteredJobSchedules, nil
}

func (j *JobScheduleService) GetTargetedSLAByJobNames(ctx context.Context, projectName tenant.ProjectName, jobNames []scheduler.JobName) (map[time.Time][]scheduler.JobName, error) {
	jobNameStr := []string{}
	for _, jn := range jobNames {
		jobNameStr = append(jobNameStr, string(jn))
	}
	jobsWithDetails, err := j.jobGetter.GetJobs(ctx, projectName, jobNameStr)
	if err != nil {
		return nil, err
	}

	targetedSLAMap := make(map[time.Time][]scheduler.JobName)
	for _, j := range jobsWithDetails {
		if j.Schedule == nil {
			continue
		}
		slaDuration, err := j.SLADuration()
		if err != nil {
			return nil, err
		}
		if slaDuration == 0 {
			continue
		}
		scheduledAt, err := j.Schedule.GetScheduleStartTime()
		if err != nil {
			return nil, err
		}
		targetedSLATime := scheduledAt.Add(time.Duration(slaDuration) * time.Second)
		targetedSLAMap[targetedSLATime] = append(targetedSLAMap[targetedSLATime], j.Name)
	}
	return targetedSLAMap, nil
}
