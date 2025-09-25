package resolver

import (
	"context"
	"time"

	"github.com/goto/optimus/core/scheduler"
	"github.com/goto/optimus/core/tenant"
)

type JobUpstreamRepository interface {
	GetAllResolvedUpstreams(context.Context) (map[scheduler.JobName][]scheduler.JobName, error)
}

type JobRepository interface {
	FindByNames(ctx context.Context, jobNames []scheduler.JobName) (map[scheduler.JobName]*scheduler.JobWithDetails, error)
}

type JobRunService interface {
	GetExpectedRunSchedules(ctx context.Context, sourceProject *tenant.Project, sourceJob *scheduler.JobWithDetails, upstreamJob *scheduler.JobWithDetails, referenceTime time.Time) ([]time.Time, error)
	GetJobRunsByIdentifiers(ctx context.Context, jobRuns []scheduler.JobRunIdentifier) ([]*scheduler.JobRunSummary, error)
}

type ProjectGetter interface {
	Get(context.Context, tenant.ProjectName) (*tenant.Project, error)
}

type LineageResolver struct {
	upstreamRepo  JobUpstreamRepository
	jobRepo       JobRepository
	jobRunService JobRunService
	projectGetter ProjectGetter
}

func NewLineageResolver(
	upstreamRepo JobUpstreamRepository,
	jobRepo JobRepository,
	jobRunService JobRunService,
	projectGetter ProjectGetter,
) *LineageResolver {
	return &LineageResolver{
		upstreamRepo:  upstreamRepo,
		jobRepo:       jobRepo,
		jobRunService: jobRunService,
		projectGetter: projectGetter,
	}
}

func (r *LineageResolver) BuildLineage(ctx context.Context, jobSchedules []*scheduler.JobSchedule) ([]*scheduler.JobLineageSummary, error) {
	upstreamsByJob, err := r.upstreamRepo.GetAllResolvedUpstreams(ctx)
	if err != nil {
		return nil, err
	}

	uniqueJobs := map[scheduler.JobName]struct{}{}
	queue := []*scheduler.JobLineageSummary{}
	for _, jobSchedule := range jobSchedules {
		queue = append(queue, &scheduler.JobLineageSummary{
			JobName: jobSchedule.JobName,
			JobRuns: map[string]*scheduler.JobRunSummary{
				jobSchedule.ScheduledAt.Format(time.RFC3339): {
					ScheduledAt: jobSchedule.ScheduledAt,
				},
			},
			Upstreams: []*scheduler.JobLineageSummary{},
		})
		uniqueJobs[jobSchedule.JobName] = struct{}{}
	}

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

	projectMap := map[tenant.ProjectName]*tenant.Project{}
	for _, job := range jobsByName {
		projectName := job.Job.Tenant.ProjectName()
		if _, ok := projectMap[projectName]; !ok {
			project, err := r.projectGetter.Get(ctx, projectName)
			if err != nil {
				return nil, err
			}
			projectMap[projectName] = project
		}
	}

	jobRunsByJobName := map[scheduler.JobName]map[string]*scheduler.JobRunSummary{}

	queue = jobWithLineages
	for len(queue) > 0 {
		jobWithLineage := queue[0]
		queue = queue[1:]

		if job, ok := jobsByName[jobWithLineage.JobName]; ok {
			jobWithLineage.Tenant = job.Job.Tenant
			jobWithLineage.Window = &job.Job.WindowConfig
			jobWithLineage.ScheduleInterval = job.Schedule.Interval

			slaDuration, _ := job.SLADuration()
			jobWithLineage.SLA = scheduler.SLAConfig{
				Duration: time.Duration(slaDuration) * time.Second,
			}

			for _, jobRun := range jobWithLineage.JobRuns {
				for i := range jobWithLineage.Upstreams {
					upstream := jobWithLineage.Upstreams[i]
					if upstream.JobRuns == nil {
						upstream.JobRuns = make(map[string]*scheduler.JobRunSummary)
					}

					upstreamJob, exists := jobsByName[upstream.JobName]
					if !exists {
						continue
					}

					expectedRunSchedules, err := r.jobRunService.GetExpectedRunSchedules(
						ctx, projectMap[jobWithLineage.Tenant.ProjectName()],
						job, upstreamJob, jobRun.ScheduledAt,
					)
					if err != nil {
						return nil, err
					}

					if _, ok := jobRunsByJobName[upstream.JobName]; !ok {
						jobRunsByJobName[upstream.JobName] = make(map[string]*scheduler.JobRunSummary)
					}

					for _, expectedRunSchedule := range expectedRunSchedules {
						scheduleKey := expectedRunSchedule.Format(time.RFC3339)
						jobRunSummary := &scheduler.JobRunSummary{
							ScheduledAt: expectedRunSchedule,
						}
						upstream.JobRuns[scheduleKey] = jobRunSummary
						jobRunsByJobName[upstream.JobName][scheduleKey] = jobRunSummary
					}
				}
			}
		}

		queue = append(queue, jobWithLineage.Upstreams...)
	}

	allJobRunsToFetch := []scheduler.JobRunIdentifier{}
	for jobName, jobRuns := range jobRunsByJobName {
		for _, jobRun := range jobRuns {
			allJobRunsToFetch = append(allJobRunsToFetch, scheduler.JobRunIdentifier{
				JobName:     jobName,
				ScheduledAt: jobRun.ScheduledAt,
			})
		}
	}

	if len(allJobRunsToFetch) > 0 {
		jobRunDetails, err := r.jobRunService.GetJobRunsByIdentifiers(ctx, allJobRunsToFetch)
		if err != nil {
			return nil, err
		}

		for _, jobRunDetail := range jobRunDetails {
			scheduleKey := jobRunDetail.ScheduledAt.Format(time.RFC3339)
			if jobRuns, exists := jobRunsByJobName[jobRunDetail.JobName]; exists {
				if jobRun, exists := jobRuns[scheduleKey]; exists {
					jobRun.JobStartTime = jobRunDetail.JobStartTime
					jobRun.JobEndTime = jobRunDetail.JobEndTime
					jobRun.WaitStartTime = jobRunDetail.WaitStartTime
					jobRun.WaitEndTime = jobRunDetail.WaitEndTime
					jobRun.TaskStartTime = jobRunDetail.TaskStartTime
					jobRun.TaskEndTime = jobRunDetail.TaskEndTime
					jobRun.HookStartTime = jobRunDetail.HookStartTime
					jobRun.HookEndTime = jobRunDetail.HookEndTime
				}
			}
		}
	}

	for _, jobWithLineage := range jobWithLineages {
		r.populateJobRunsInTree(jobWithLineage, jobRunsByJobName)
	}

	return jobWithLineages, nil
}

func (r *LineageResolver) populateJobRunsInTree(jobWithLineage *scheduler.JobLineageSummary, jobRunsByJobName map[scheduler.JobName]map[string]*scheduler.JobRunSummary) {
	if jobRuns, exists := jobRunsByJobName[jobWithLineage.JobName]; exists {
		if jobWithLineage.JobRuns == nil {
			jobWithLineage.JobRuns = make(map[string]*scheduler.JobRunSummary)
		}
		for scheduleKey := range jobWithLineage.JobRuns {
			jobWithLineage.JobRuns[scheduleKey] = jobRuns[scheduleKey]
		}
	}

	for _, upstream := range jobWithLineage.Upstreams {
		r.populateJobRunsInTree(upstream, jobRunsByJobName)
	}
}
