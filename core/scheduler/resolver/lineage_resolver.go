package resolver

import (
	"context"

	"github.com/goto/optimus/core/scheduler"
)

type JobUpstreamRepository interface {
	GetAllResolvedUpstreams(context.Context) (map[scheduler.JobName][]scheduler.JobName, error)
}

type JobRepository interface {
	FindByNames(ctx context.Context, jobNames []scheduler.JobName) (map[scheduler.JobName]*scheduler.JobScheduleDetail, error)
}

type LineageResolver struct {
	upstreamRepo JobUpstreamRepository
	jobRepo      JobRepository
}

func NewLineageResolver(
	upstreamRepo JobUpstreamRepository,
	jobRepo JobRepository,
) *LineageResolver {
	return &LineageResolver{
		upstreamRepo: upstreamRepo,
		jobRepo:      jobRepo,
	}
}

func (r *LineageResolver) BuildLineage(ctx context.Context, jobSchedules []*scheduler.JobSchedule) ([]*scheduler.JobLineageSummary, error) {
	upstreamsByJob, err := r.upstreamRepo.GetAllResolvedUpstreams(ctx)
	if err != nil {
		return nil, err
	}

	queue := []*scheduler.JobLineageSummary{}
	for _, jobSchedule := range jobSchedules {
		queue = append(queue, &scheduler.JobLineageSummary{
			JobName:   jobSchedule.JobName,
			Upstreams: []*scheduler.JobLineageSummary{},
		})
	}

	uniqueJobs := map[scheduler.JobName]struct{}{}
	jobWithLineages := queue
	jobWithLineageByNameMap := map[scheduler.JobName]*scheduler.JobLineageSummary{}

	for len(queue) > 0 {
		jobWithLineage := queue[0]
		queue = queue[1:]

		for _, upstreamJobName := range upstreamsByJob[jobWithLineage.JobName] {
			if _, ok := jobWithLineageByNameMap[upstreamJobName]; !ok {
				upstreamJobWithLineage := &scheduler.JobLineageSummary{
					JobName:   upstreamJobName,
					Upstreams: []*scheduler.JobLineageSummary{},
				}

				jobWithLineageByNameMap[upstreamJobName] = upstreamJobWithLineage
			}

			jobWithLineage.Upstreams = append(jobWithLineage.Upstreams, jobWithLineageByNameMap[upstreamJobName])
			uniqueJobs[upstreamJobName] = struct{}{}
		}
	}

	jobNames := make([]scheduler.JobName, 0, len(uniqueJobs))
	for jobName := range uniqueJobs {
		jobNames = append(jobNames, jobName)
	}
	jobsByName, err := r.jobRepo.FindByNames(ctx, jobNames)
	if err != nil {
		return nil, err
	}

	queue = jobWithLineages
	for len(queue) > 0 {
		jobWithLineage := queue[0]
		queue = queue[1:]

		if job, ok := jobsByName[jobWithLineage.JobName]; ok {
			jobWithLineage.Tenant = job.Tenant
			jobWithLineage.Window = job.Window
			jobWithLineage.ScheduleInterval = job.ScheduleInterval
			jobWithLineage.SLA = job.SLA
		}
	}

	return jobWithLineages, nil
}
