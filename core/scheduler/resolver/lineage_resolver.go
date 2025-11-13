package resolver

import (
	"context"
	"time"

	"github.com/goto/salt/log"

	"github.com/goto/optimus/core/scheduler"
	"github.com/goto/optimus/core/tenant"
	"github.com/goto/optimus/internal/lib/window"
)

type JobUpstreamRepository interface {
	GetAllResolvedUpstreams(context.Context) (map[scheduler.JobIdentifier][]scheduler.JobIdentifier, error)
}

type JobRepository interface {
	GetSummaryByNames(ctx context.Context, jobNames []scheduler.JobIdentifier) (map[scheduler.JobIdentifier]*scheduler.JobSummary, error)
}

type JobRunService interface {
	GetExpectedRunSchedules(ctx context.Context, sourceProject *tenant.Project, sourceSchedule string, sourceWindow window.Config, upstreamSchedule string, referenceTime time.Time) ([]time.Time, error)
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

type LineageData struct {
	UpstreamsByJob map[scheduler.JobIdentifier][]scheduler.JobIdentifier
	JobsByName     map[scheduler.JobIdentifier]*scheduler.JobSummary
	ProjectsByName map[tenant.ProjectName]*tenant.Project
}

func (r *LineageResolver) BuildLineage(ctx context.Context, jobSchedules []*scheduler.JobSchedule, maxUpstreamsPerLevel int) (map[*scheduler.JobSchedule]*scheduler.JobLineageSummary, error) {
	lineageData, err := r.prepareAllLineageData(ctx, jobSchedules)
	if err != nil {
		return nil, err
	}

	results := make(map[*scheduler.JobSchedule]*scheduler.JobLineageSummary)
	for _, schedule := range jobSchedules {
		lineage, err := r.buildSingleJobLineage(ctx, schedule, lineageData, maxUpstreamsPerLevel)
		if err != nil {
			return nil, err
		}
		results[schedule] = lineage
	}

	return results, nil
}

func (r *LineageResolver) prepareAllLineageData(ctx context.Context, jobSchedules []*scheduler.JobSchedule) (*LineageData, error) {
	upstreamsByJob, err := r.upstreamRepo.GetAllResolvedUpstreams(ctx)
	if err != nil {
		return nil, err
	}

	allJobNames := r.collectAllRequiredJobs(jobSchedules, upstreamsByJob)
	jobsByIdentifierMap, err := r.jobRepo.GetSummaryByNames(ctx, allJobNames)
	if err != nil {
		return nil, err
	}

	projectsByName, err := r.getAllPresets(ctx, jobsByIdentifierMap)
	if err != nil {
		return nil, err
	}

	return &LineageData{
		UpstreamsByJob: upstreamsByJob,
		JobsByName:     jobsByIdentifierMap,
		ProjectsByName: projectsByName,
	}, nil
}

func (r *LineageResolver) getAllPresets(ctx context.Context, jobsByIdentifierMap map[scheduler.JobIdentifier]*scheduler.JobSummary) (map[tenant.ProjectName]*tenant.Project, error) {
	projectsByName := make(map[tenant.ProjectName]*tenant.Project)

	for _, job := range jobsByIdentifierMap {
		projectName := job.Tenant.ProjectName()
		if _, exists := projectsByName[projectName]; !exists {
			project, err := r.projectGetter.Get(ctx, projectName)
			if err != nil {
				return nil, err
			}
			projectsByName[projectName] = project
		}
	}

	return projectsByName, nil
}

func (r *LineageResolver) collectAllRequiredJobs(jobSchedules []*scheduler.JobSchedule, upstreamsByJob map[scheduler.JobIdentifier][]scheduler.JobIdentifier) []scheduler.JobIdentifier {
	visited := map[scheduler.JobIdentifier]bool{}
	allJobIdentifiers := []scheduler.JobIdentifier{}

	for _, schedule := range jobSchedules {
		jobID := scheduler.JobIdentifier{
			JobName:     schedule.JobName,
			ProjectName: schedule.ProjectName,
		}
		r.collectJobs(jobID, upstreamsByJob, visited, &allJobIdentifiers, 0)
	}

	return allJobIdentifiers
}

func (r *LineageResolver) collectJobs(jobID scheduler.JobIdentifier, upstreamsByJob map[scheduler.JobIdentifier][]scheduler.JobIdentifier, visited map[scheduler.JobIdentifier]bool, allJobs *[]scheduler.JobIdentifier, depth int) {
	if visited[jobID] {
		return
	}

	visited[jobID] = true
	*allJobs = append(*allJobs, jobID)

	for _, upstream := range upstreamsByJob[jobID] {
		r.collectJobs(upstream, upstreamsByJob, visited, allJobs, depth+1)
	}
}

func (r *LineageResolver) buildSingleJobLineage(ctx context.Context, schedule *scheduler.JobSchedule, lineageData *LineageData, maxUpstreamsPerLevel int) (*scheduler.JobLineageSummary, error) {
	jobIdentifer := scheduler.JobIdentifier{
		JobName:     schedule.JobName,
		ProjectName: schedule.ProjectName,
	}
	lineage := r.buildLineageTree(jobIdentifer, lineageData, map[scheduler.JobIdentifier]*scheduler.JobLineageSummary{}, 0)

	finalLineage, err := r.getAllUpstreamRuns(ctx, lineage, schedule.ScheduledAt, lineageData)
	if err != nil {
		return nil, err
	}

	if maxUpstreamsPerLevel > 0 {
		finalLineage = finalLineage.PruneLineage(maxUpstreamsPerLevel, scheduler.MaxLineageDepth)
	}

	return finalLineage, nil
}

func (r *LineageResolver) buildLineageTree(jobIdentifier scheduler.JobIdentifier, lineageData *LineageData, result map[scheduler.JobIdentifier]*scheduler.JobLineageSummary, depth int) *scheduler.JobLineageSummary {
	if _, ok := result[jobIdentifier]; ok {
		return result[jobIdentifier]
	}

	result[jobIdentifier] = &scheduler.JobLineageSummary{
		JobName: jobIdentifier.JobName,
		JobRuns: make(map[string]*scheduler.JobRunSummary),
	}

	if job, exists := lineageData.JobsByName[jobIdentifier]; exists {
		result[jobIdentifier].Tenant = job.Tenant
		result[jobIdentifier].Window = &job.Window
		result[jobIdentifier].ScheduleInterval = job.ScheduleInterval
		result[jobIdentifier].SLA = job.SLA
	}

	for _, upstreamName := range lineageData.UpstreamsByJob[jobIdentifier] {
		result[jobIdentifier].Upstreams = append(result[jobIdentifier].Upstreams, r.buildLineageTree(upstreamName, lineageData, result, depth+1))
	}
	return result[jobIdentifier]
}

func (r *LineageResolver) getAllUpstreamRuns(ctx context.Context, lineage *scheduler.JobLineageSummary, scheduledAt time.Time, lineageData *LineageData) (*scheduler.JobLineageSummary, error) {
	allJobRunsMap := make(map[scheduler.JobIdentifier]map[string]*scheduler.JobRunSummary)
	// initialize first job run in the lineage
	baseSLATime := scheduledAt.Add(lineage.SLA.Duration)
	lineage.JobRuns = map[string]*scheduler.JobRunSummary{
		scheduledAt.UTC().Format(time.RFC3339): {
			ScheduledAt: scheduledAt,
			SLATime:     &baseSLATime,
		},
	}

	err := r.calculateAllUpstreamRuns(ctx, lineage, lineageData, allJobRunsMap, make(map[scheduler.JobIdentifier]bool))
	if err != nil {
		return nil, err
	}

	jobRunDetails, err := r.fetchJobRunDetails(ctx, allJobRunsMap)
	if err != nil {
		return nil, err
	}

	return r.populateLineageWithJobRuns(lineage, jobRunDetails, make(map[scheduler.JobName]*scheduler.JobLineageSummary)), nil
}

func (r *LineageResolver) calculateAllUpstreamRuns(ctx context.Context, lineage *scheduler.JobLineageSummary, lineageData *LineageData, allJobRunsMap map[scheduler.JobIdentifier]map[string]*scheduler.JobRunSummary, visited map[scheduler.JobIdentifier]bool) error {
	jobID := lineage.JobIdentifier()
	if _, ok := visited[jobID]; ok {
		return nil
	}

	visited[jobID] = true

	currentJob := lineageData.JobsByName[jobID]
	if currentJob == nil {
		return nil
	}

	if _, exists := allJobRunsMap[jobID]; !exists {
		allJobRunsMap[jobID] = make(map[string]*scheduler.JobRunSummary)
	}
	for key, jobRun := range lineage.JobRuns {
		allJobRunsMap[jobID][key] = jobRun
	}

	for _, upstream := range lineage.Upstreams {
		upstreamID := upstream.JobIdentifier()
		upstreamJob := lineageData.JobsByName[upstreamID]
		if upstreamJob == nil {
			continue
		}

		if _, exists := allJobRunsMap[upstreamID]; !exists {
			allJobRunsMap[upstreamID] = make(map[string]*scheduler.JobRunSummary)
		}

		for _, jobRun := range lineage.JobRuns {
			upstreamSchedules, err := r.getUpstreamRuns(ctx, currentJob, upstreamJob, jobRun.ScheduledAt, lineageData.ProjectsByName)
			if err != nil {
				return err
			}

			for _, schedule := range upstreamSchedules {
				scheduleKey := schedule.Format(time.RFC3339)
				if _, exists := allJobRunsMap[upstreamID][scheduleKey]; !exists {
					baseSLATime := schedule.Add(upstream.SLA.Duration)
					allJobRunsMap[upstreamID][scheduleKey] = &scheduler.JobRunSummary{
						ScheduledAt: schedule,
						SLATime:     &baseSLATime,
					}
				}
				if upstream.JobRuns == nil {
					upstream.JobRuns = make(map[string]*scheduler.JobRunSummary)
				}
				upstream.JobRuns[scheduleKey] = allJobRunsMap[upstreamID][scheduleKey]
			}
		}

		err := r.calculateAllUpstreamRuns(ctx, upstream, lineageData, allJobRunsMap, visited)
		if err != nil {
			return err
		}
	}

	return nil
}

func (r *LineageResolver) getUpstreamRuns(ctx context.Context, sourceJob, upstreamJob *scheduler.JobSummary, referenceTime time.Time, projectsByName map[tenant.ProjectName]*tenant.Project) ([]time.Time, error) {
	project := projectsByName[sourceJob.Tenant.ProjectName()]
	if project == nil {
		return []time.Time{}, nil
	}

	// GetExpectedRunSchedules return sorted schedule from the earliest to the latest
	schedules, err := r.jobRunService.GetExpectedRunSchedules(ctx, project, sourceJob.ScheduleInterval, sourceJob.Window, upstreamJob.ScheduleInterval, referenceTime)
	if err != nil {
		return nil, err
	}

	// assumption: only fetch the latest schedule from the interval
	if len(schedules) > 1 {
		schedules = []time.Time{schedules[len(schedules)-1]}
	}

	return schedules, nil
}

func (r *LineageResolver) fetchJobRunDetails(ctx context.Context, allJobRunsMap map[scheduler.JobIdentifier]map[string]*scheduler.JobRunSummary) (map[scheduler.JobIdentifier]map[string]*scheduler.JobRunSummary, error) {
	var identifiers []scheduler.JobRunIdentifier
	for jobID, jobRuns := range allJobRunsMap {
		for _, jobRun := range jobRuns {
			identifiers = append(identifiers, scheduler.JobRunIdentifier{
				JobName:     jobID.JobName,
				ProjectName: jobID.ProjectName,
				ScheduledAt: jobRun.ScheduledAt,
			})
		}
	}

	if len(identifiers) == 0 {
		return allJobRunsMap, nil
	}

	jobRunDetails, err := r.jobRunService.GetJobRunsByIdentifiers(ctx, identifiers)
	if err != nil {
		return nil, err
	}

	result := make(map[scheduler.JobIdentifier]map[string]*scheduler.JobRunSummary)
	for jobID, jobRuns := range allJobRunsMap {
		result[jobID] = make(map[string]*scheduler.JobRunSummary)
		for scheduleKey, jobRun := range jobRuns {
			result[jobID][scheduleKey] = copyJobRun(jobRun)
		}
	}

	for _, detail := range jobRunDetails {
		scheduleKey := detail.ScheduledAt.UTC().Format(time.RFC3339)
		jobID := scheduler.JobIdentifier{
			JobName:     detail.JobName,
			ProjectName: detail.ProjectName,
		}
		if jobRuns, exists := result[jobID]; exists {
			if jobRun, exists := jobRuns[scheduleKey]; exists {
				jobRun.JobName = detail.JobName
				jobRun.JobStartTime = detail.JobStartTime
				jobRun.JobEndTime = detail.JobEndTime
				jobRun.WaitStartTime = detail.WaitStartTime
				jobRun.WaitEndTime = detail.WaitEndTime
				jobRun.TaskStartTime = detail.TaskStartTime
				jobRun.TaskEndTime = detail.TaskEndTime
				jobRun.HookStartTime = detail.HookStartTime
				jobRun.HookEndTime = detail.HookEndTime
			}
		}
	}

	return result, nil
}

func (r *LineageResolver) populateLineageWithJobRuns(lineage *scheduler.JobLineageSummary, jobRunDetails map[scheduler.JobIdentifier]map[string]*scheduler.JobRunSummary, result map[scheduler.JobName]*scheduler.JobLineageSummary) *scheduler.JobLineageSummary {
	if _, ok := result[lineage.JobName]; ok {
		return result[lineage.JobName]
	}

	result[lineage.JobName] = &scheduler.JobLineageSummary{
		JobName:          lineage.JobName,
		Tenant:           lineage.Tenant,
		Window:           lineage.Window,
		ScheduleInterval: lineage.ScheduleInterval,
		SLA:              lineage.SLA,
		Upstreams:        make([]*scheduler.JobLineageSummary, len(lineage.Upstreams)),
	}

	jobID := lineage.JobIdentifier()
	if jobRuns, exists := jobRunDetails[jobID]; exists {
		// only fetch job runs that are necessary in the lineage
		result[lineage.JobName].JobRuns = map[string]*scheduler.JobRunSummary{}
		for scheduleKey := range lineage.JobRuns {
			if jobRun, exists := jobRuns[scheduleKey]; exists {
				result[lineage.JobName].JobRuns[scheduleKey] = jobRun
			}
		}
	} else {
		result[lineage.JobName].JobRuns = lineage.JobRuns
	}

	for i, upstream := range lineage.Upstreams {
		result[lineage.JobName].Upstreams[i] = r.populateLineageWithJobRuns(upstream, jobRunDetails, result)
	}

	return result[lineage.JobName]
}

func copyJobRun(source *scheduler.JobRunSummary) *scheduler.JobRunSummary {
	if source == nil {
		return nil
	}
	return &scheduler.JobRunSummary{
		JobName:       source.JobName,
		ScheduledAt:   source.ScheduledAt,
		SLATime:       source.SLATime,
		JobStartTime:  source.JobStartTime,
		JobEndTime:    source.JobEndTime,
		WaitStartTime: source.WaitStartTime,
		WaitEndTime:   source.WaitEndTime,
		TaskStartTime: source.TaskStartTime,
		TaskEndTime:   source.TaskEndTime,
		HookStartTime: source.HookStartTime,
		HookEndTime:   source.HookEndTime,
	}
}
