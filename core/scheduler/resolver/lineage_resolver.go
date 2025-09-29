package resolver

import (
	"context"
	"maps"
	"time"

	"github.com/goto/optimus/core/scheduler"
	"github.com/goto/optimus/core/tenant"
	"github.com/goto/salt/log"
)

type JobUpstreamRepository interface {
	GetAllResolvedUpstreams(context.Context) (map[scheduler.JobName][]scheduler.JobName, error)
}

type JobRepository interface {
	FindByNames(ctx context.Context, jobNames []scheduler.JobName) (map[scheduler.JobName]*scheduler.JobWithDetails, error)
}

type JobRunService interface {
	GetExpectedRunSchedules(ctx context.Context, sourceProject *tenant.Project, sourceJob, upstreamJob *scheduler.JobWithDetails, referenceTime time.Time) ([]time.Time, error)
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
	logger        log.Logger
}

func NewLineageResolver(
	upstreamRepo JobUpstreamRepository,
	jobRepo JobRepository,
	jobRunService JobRunService,
	projectGetter ProjectGetter,
	logger log.Logger,
) *LineageResolver {
	return &LineageResolver{
		upstreamRepo:  upstreamRepo,
		jobRepo:       jobRepo,
		jobRunService: jobRunService,
		projectGetter: projectGetter,
		logger:        logger,
	}
}

func (r *LineageResolver) BuildLineage(ctx context.Context, jobSchedules []*scheduler.JobSchedule) ([]*scheduler.JobLineageSummary, error) {
	lineage, err := r.buildLineageStructure(ctx, jobSchedules)
	if err != nil {
		return nil, err
	}

	if err := r.enrichWithJobDetails(ctx, lineage); err != nil {
		return nil, err
	}

	if err := r.enrichWithJobRunDetails(ctx, lineage); err != nil {
		return nil, err
	}

	return lineage, nil
}

func (r *LineageResolver) buildLineageStructure(ctx context.Context, jobSchedules []*scheduler.JobSchedule) ([]*scheduler.JobLineageSummary, error) {
	stack := []*scheduler.JobLineageSummary{}
	for _, jobSchedule := range jobSchedules {
		stack = append(stack, &scheduler.JobLineageSummary{
			JobName: jobSchedule.JobName,
			JobRuns: map[string]*scheduler.JobRunSummary{
				jobSchedule.ScheduledAt.Format(time.RFC3339): {
					ScheduledAt: jobSchedule.ScheduledAt,
				},
			},
			Upstreams: []*scheduler.JobLineageSummary{},
		})
	}

	// fetch list of jobs and their direct upstreams
	upstreamsByJob, err := r.upstreamRepo.GetAllResolvedUpstreams(ctx)
	if err != nil {
		return nil, err
	}

	jobWithLineages := stack
	visited := map[scheduler.JobName]bool{}

	for len(stack) > 0 {
		jobWithLineage := stack[len(stack)-1]
		stack = stack[:len(stack)-1]

		if _, ok := visited[jobWithLineage.JobName]; ok {
			continue
		}
		visited[jobWithLineage.JobName] = true

		for _, upstreamJobName := range upstreamsByJob[jobWithLineage.JobName] {
			jobWithLineage.Upstreams = append(jobWithLineage.Upstreams, &scheduler.JobLineageSummary{
				JobName:   upstreamJobName,
				JobRuns:   map[string]*scheduler.JobRunSummary{},
				Upstreams: []*scheduler.JobLineageSummary{},
			})
		}

		stack = append(stack, jobWithLineage.Upstreams...)
	}

	return jobWithLineages, nil
}

func (r *LineageResolver) enrichWithJobDetails(ctx context.Context, lineage []*scheduler.JobLineageSummary) error {
	uniqueJobs := collectUniqueJobs(lineage)

	jobNames := make([]scheduler.JobName, 0, len(uniqueJobs))
	for jobName := range uniqueJobs {
		jobNames = append(jobNames, jobName)
	}

	jobsByName, err := r.jobRepo.FindByNames(ctx, jobNames)
	if err != nil {
		return err
	}

	queue := make([]*scheduler.JobLineageSummary, 0, len(lineage))
	queue = append(queue, lineage...)

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
		}

		queue = append(queue, jobWithLineage.Upstreams...)
	}

	return nil
}

func (r *LineageResolver) enrichWithJobRunDetails(ctx context.Context, lineage []*scheduler.JobLineageSummary) error {
	jobsByName, projectMap, err := r.getJobsAndProjects(ctx, lineage)
	if err != nil {
		return err
	}

	jobRunsByJobName, err := r.calculateExpectedJobRuns(ctx, lineage, jobsByName, projectMap)
	if err != nil {
		return err
	}

	if err := r.fetchAndUpdateJobRunDetails(ctx, jobRunsByJobName); err != nil {
		return err
	}

	for _, jobWithLineage := range lineage {
		r.populateJobRunsInTree(jobWithLineage, jobRunsByJobName)
	}

	return nil
}

func (r *LineageResolver) getJobsAndProjects(ctx context.Context, lineage []*scheduler.JobLineageSummary) (map[scheduler.JobName]*scheduler.JobWithDetails, map[tenant.ProjectName]*tenant.Project, error) {
	uniqueJobs := collectUniqueJobs(lineage)

	jobNames := make([]scheduler.JobName, 0, len(uniqueJobs))
	for jobName := range uniqueJobs {
		jobNames = append(jobNames, jobName)
	}

	jobsByName, err := r.jobRepo.FindByNames(ctx, jobNames)
	if err != nil {
		return nil, nil, err
	}

	projectMap := map[tenant.ProjectName]*tenant.Project{}
	for _, job := range jobsByName {
		projectName := job.Job.Tenant.ProjectName()
		if _, ok := projectMap[projectName]; !ok {
			project, err := r.projectGetter.Get(ctx, projectName)
			if err != nil {
				return nil, nil, err
			}
			projectMap[projectName] = project
		}
	}

	return jobsByName, projectMap, nil
}

func (r *LineageResolver) calculateExpectedJobRuns(ctx context.Context, lineage []*scheduler.JobLineageSummary, jobsByName map[scheduler.JobName]*scheduler.JobWithDetails, projectMap map[tenant.ProjectName]*tenant.Project) (map[scheduler.JobName]map[string]*scheduler.JobRunSummary, error) {
	jobRunsByJobName := map[scheduler.JobName]map[string]*scheduler.JobRunSummary{}
	for _, jobWithLineage := range lineage {
		if _, ok := jobRunsByJobName[jobWithLineage.JobName]; !ok {
			jobRunsByJobName[jobWithLineage.JobName] = make(map[string]*scheduler.JobRunSummary)
		}
		maps.Copy(jobRunsByJobName[jobWithLineage.JobName], jobWithLineage.JobRuns)
	}

	queue := make([]*scheduler.JobLineageSummary, 0, len(lineage))
	queue = append(queue, lineage...)

	for len(queue) > 0 {
		jobWithLineage := queue[0]
		queue = queue[1:]

		if job, ok := jobsByName[jobWithLineage.JobName]; ok {
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

	return jobRunsByJobName, nil
}

func (r *LineageResolver) fetchAndUpdateJobRunDetails(ctx context.Context, jobRunsByJobName map[scheduler.JobName]map[string]*scheduler.JobRunSummary) error {
	allJobRunsToFetch := []scheduler.JobRunIdentifier{}
	for jobName, jobRuns := range jobRunsByJobName {
		for _, jobRun := range jobRuns {
			allJobRunsToFetch = append(allJobRunsToFetch, scheduler.JobRunIdentifier{
				JobName:     jobName,
				ScheduledAt: jobRun.ScheduledAt,
			})
		}
	}

	if len(allJobRunsToFetch) == 0 {
		return nil
	}

	jobRunDetails, err := r.jobRunService.GetJobRunsByIdentifiers(ctx, allJobRunsToFetch)
	if err != nil {
		return err
	}

	for _, jobRunDetail := range jobRunDetails {
		scheduleKey := jobRunDetail.ScheduledAt.Format(time.RFC3339)
		if jobRuns, exists := jobRunsByJobName[jobRunDetail.JobName]; exists {
			if jobRun, exists := jobRuns[scheduleKey]; exists {
				jobRun.JobName = jobRunDetail.JobName
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

	return nil
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

func collectUniqueJobs(lineage []*scheduler.JobLineageSummary) map[scheduler.JobName]struct{} {
	uniqueJobs := map[scheduler.JobName]struct{}{}
	queue := make([]*scheduler.JobLineageSummary, 0, len(lineage))
	queue = append(queue, lineage...)

	for len(queue) > 0 {
		jobWithLineage := queue[0]
		queue = queue[1:]
		uniqueJobs[jobWithLineage.JobName] = struct{}{}
		queue = append(queue, jobWithLineage.Upstreams...)
	}

	return uniqueJobs
}
