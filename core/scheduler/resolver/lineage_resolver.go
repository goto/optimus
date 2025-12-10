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
	GetAllResolvedUpstreams(context.Context) (map[scheduler.JobName][]scheduler.JobName, error)
}

type JobRepository interface {
	GetSummaryByNames(ctx context.Context, jobNames []scheduler.JobName) (map[scheduler.JobName]*scheduler.JobSummary, error)
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
	UpstreamsByJob map[scheduler.JobName][]scheduler.JobName
	JobsByName     map[scheduler.JobName]*scheduler.JobSummary
	ProjectsByName map[tenant.ProjectName]*tenant.Project
}

func (r *LineageResolver) BuildLineage(ctx context.Context, jobSchedules []*scheduler.JobSchedule, maxUpstreamsPerLevel, validLineageIntervalInHours int) (map[*scheduler.JobSchedule]*scheduler.JobLineageSummary, error) {
	lineageData, err := r.prepareAllLineageData(ctx, jobSchedules)
	if err != nil {
		return nil, err
	}

	results := make(map[*scheduler.JobSchedule]*scheduler.JobLineageSummary)
	for _, schedule := range jobSchedules {
		lineage, err := r.buildSingleJobLineage(ctx, schedule, lineageData, maxUpstreamsPerLevel, validLineageIntervalInHours)
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
	jobsByName, err := r.jobRepo.GetSummaryByNames(ctx, allJobNames)
	if err != nil {
		return nil, err
	}

	projectsByName, err := r.getAllPresets(ctx, jobsByName)
	if err != nil {
		return nil, err
	}

	return &LineageData{
		UpstreamsByJob: upstreamsByJob,
		JobsByName:     jobsByName,
		ProjectsByName: projectsByName,
	}, nil
}

func (r *LineageResolver) getAllPresets(ctx context.Context, jobsByName map[scheduler.JobName]*scheduler.JobSummary) (map[tenant.ProjectName]*tenant.Project, error) {
	projectsByName := make(map[tenant.ProjectName]*tenant.Project)

	for _, job := range jobsByName {
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

func (r *LineageResolver) collectAllRequiredJobs(jobSchedules []*scheduler.JobSchedule, upstreamsByJob map[scheduler.JobName][]scheduler.JobName) []scheduler.JobName {
	visited := map[scheduler.JobName]bool{}
	allJobs := []scheduler.JobName{}

	for _, schedule := range jobSchedules {
		r.collectJobs(schedule.JobName, upstreamsByJob, visited, &allJobs, 0)
	}

	return allJobs
}

func (r *LineageResolver) collectJobs(jobName scheduler.JobName, upstreamsByJob map[scheduler.JobName][]scheduler.JobName, visited map[scheduler.JobName]bool, allJobs *[]scheduler.JobName, depth int) {
	if visited[jobName] {
		return
	}

	visited[jobName] = true
	*allJobs = append(*allJobs, jobName)

	for _, upstream := range upstreamsByJob[jobName] {
		r.collectJobs(upstream, upstreamsByJob, visited, allJobs, depth+1)
	}
}

func (r *LineageResolver) buildSingleJobLineage(ctx context.Context, schedule *scheduler.JobSchedule, lineageData *LineageData, maxUpstreamsPerLevel, validLineageIntervalInHours int) (*scheduler.JobLineageSummary, error) {
	lineage := r.buildLineageTree(schedule.JobName, lineageData, map[scheduler.JobName]*scheduler.JobLineageSummary{}, 0)

	finalLineage, err := r.getAllUpstreamRuns(ctx, lineage, schedule.ScheduledAt, lineageData, validLineageIntervalInHours)
	if err != nil {
		return nil, err
	}

	if maxUpstreamsPerLevel > 0 {
		finalLineage = finalLineage.PruneLineage(maxUpstreamsPerLevel, scheduler.MaxLineageDepth)
	}

	return finalLineage, nil
}

func (r *LineageResolver) buildLineageTree(jobName scheduler.JobName, lineageData *LineageData, result map[scheduler.JobName]*scheduler.JobLineageSummary, depth int) *scheduler.JobLineageSummary {
	if _, ok := result[jobName]; ok {
		return result[jobName]
	}

	result[jobName] = &scheduler.JobLineageSummary{
		JobName: jobName,
		JobRuns: make(map[scheduler.JobName]*scheduler.JobRunSummary),
	}

	if job, exists := lineageData.JobsByName[jobName]; exists && job.IsEnabled {
		result[jobName].Tenant = job.Tenant
		result[jobName].IsEnabled = job.IsEnabled
		result[jobName].Window = &job.Window
		result[jobName].ScheduleInterval = job.ScheduleInterval
		result[jobName].SLA = job.SLA
	}

	for _, upstreamName := range lineageData.UpstreamsByJob[jobName] {
		result[jobName].Upstreams = append(result[jobName].Upstreams, r.buildLineageTree(upstreamName, lineageData, result, depth+1))
	}
	return result[jobName]
}

type visitKey struct {
	jobName     scheduler.JobName
	scheduledAt time.Time
}

func (r *LineageResolver) getAllUpstreamRuns(ctx context.Context, lineage *scheduler.JobLineageSummary, scheduledAt time.Time, lineageData *LineageData, validLineageIntervalInHours int) (*scheduler.JobLineageSummary, error) {
	allJobRunsMap := make(map[scheduler.JobName]map[time.Time]*scheduler.JobRunSummary)
	// initialize first job run in the lineage
	baseSLATime := scheduledAt.Add(lineage.SLA.Duration)
	lineage.JobRuns = map[scheduler.JobName]*scheduler.JobRunSummary{
		lineage.JobName: {
			ScheduledAt: scheduledAt,
			SLATime:     &baseSLATime,
		},
	}

	// calculate upstream job runs within the valid lineage interval
	referenceTime := scheduledAt.Add(-time.Duration(validLineageIntervalInHours) * time.Hour)
	err := r.calculateAllUpstreamRuns(ctx, lineage, lineage.JobName, lineageData, allJobRunsMap, make(map[visitKey]bool), referenceTime)
	if err != nil {
		return nil, err
	}

	jobRunDetails, err := r.fetchJobRunDetails(ctx, allJobRunsMap)
	if err != nil {
		return nil, err
	}

	return r.populateLineageWithJobRuns(lineage, jobRunDetails, make(map[scheduler.JobName]*scheduler.JobLineageSummary)), nil
}

func (r *LineageResolver) calculateAllUpstreamRuns(ctx context.Context, lineage *scheduler.JobLineageSummary, targetJob scheduler.JobName, lineageData *LineageData, allJobRunsMap map[scheduler.JobName]map[time.Time]*scheduler.JobRunSummary, visited map[visitKey]bool, referenceTime time.Time) error {
	if len(lineage.JobRuns) == 0 {
		return nil
	}

	currentJob := lineageData.JobsByName[lineage.JobName]
	if currentJob == nil || !currentJob.IsEnabled {
		return nil
	}

	if _, exists := allJobRunsMap[lineage.JobName]; !exists {
		allJobRunsMap[lineage.JobName] = make(map[time.Time]*scheduler.JobRunSummary)
	}

	for _, jobRun := range lineage.JobRuns {
		visitedKey := visitKey{
			jobName:     lineage.JobName,
			scheduledAt: jobRun.ScheduledAt,
		}
		if _, ok := visited[visitedKey]; ok {
			continue
		}

		visited[visitedKey] = true
		allJobRunsMap[lineage.JobName][jobRun.ScheduledAt] = copyJobRun(jobRun)

		for _, upstream := range lineage.Upstreams {
			upstreamJob := lineageData.JobsByName[upstream.JobName]
			if upstreamJob == nil || !upstreamJob.IsEnabled {
				continue
			}

			upstreamSchedule, err := r.getUpstreamRun(ctx, currentJob, upstreamJob, jobRun.ScheduledAt, lineageData.ProjectsByName)
			if err != nil {
				return err
			}

			if upstreamSchedule.IsZero() || upstreamSchedule.Before(referenceTime) {
				continue
			}

			if _, exists := allJobRunsMap[upstream.JobName]; !exists {
				allJobRunsMap[upstream.JobName] = make(map[time.Time]*scheduler.JobRunSummary)
			}

			if _, exists := allJobRunsMap[upstream.JobName][upstreamSchedule]; !exists {
				baseSLATime := upstreamSchedule.Add(upstream.SLA.Duration)
				allJobRunsMap[upstream.JobName][upstreamSchedule] = &scheduler.JobRunSummary{
					ScheduledAt: upstreamSchedule,
					SLATime:     &baseSLATime,
				}
			}

			if upstream.JobRuns == nil {
				upstream.JobRuns = make(map[scheduler.JobName]*scheduler.JobRunSummary)
			}
			upstream.JobRuns[targetJob] = allJobRunsMap[upstream.JobName][upstreamSchedule]

			err = r.calculateAllUpstreamRuns(ctx, upstream, targetJob, lineageData, allJobRunsMap, visited, referenceTime)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (r *LineageResolver) getUpstreamRun(ctx context.Context, sourceJob, upstreamJob *scheduler.JobSummary, referenceTime time.Time, projectsByName map[tenant.ProjectName]*tenant.Project) (time.Time, error) {
	project := projectsByName[sourceJob.Tenant.ProjectName()]
	if project == nil {
		return time.Time{}, nil
	}

	// GetExpectedRunSchedules return sorted schedule from the earliest to the latest
	schedules, err := r.jobRunService.GetExpectedRunSchedules(ctx, project, sourceJob.ScheduleInterval, sourceJob.Window, upstreamJob.ScheduleInterval, referenceTime)
	if err != nil || len(schedules) < 1 {
		return time.Time{}, err
	}

	// assumption: only fetch the latest schedule from the interval
	return schedules[len(schedules)-1], nil
}

func (r *LineageResolver) fetchJobRunDetails(ctx context.Context, allJobRunsMap map[scheduler.JobName]map[time.Time]*scheduler.JobRunSummary) (map[scheduler.JobName]map[time.Time]*scheduler.JobRunSummary, error) {
	var identifiers []scheduler.JobRunIdentifier
	for jobName, jobRuns := range allJobRunsMap {
		for _, jobRun := range jobRuns {
			identifiers = append(identifiers, scheduler.JobRunIdentifier{
				JobName:     jobName,
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

	result := make(map[scheduler.JobName]map[time.Time]*scheduler.JobRunSummary)
	for jobName, jobRuns := range allJobRunsMap {
		result[jobName] = make(map[time.Time]*scheduler.JobRunSummary)
		for scheduleKey, jobRun := range jobRuns {
			result[jobName][scheduleKey] = copyJobRun(jobRun)
		}
	}

	for _, detail := range jobRunDetails {
		if jobRuns, exists := result[detail.JobName]; exists {
			scheduleKey := detail.ScheduledAt.UTC()
			if _, exists := jobRuns[scheduleKey]; exists {
				jobRuns[scheduleKey].JobName = detail.JobName
				jobRuns[scheduleKey].JobStartTime = detail.JobStartTime
				jobRuns[scheduleKey].JobEndTime = detail.JobEndTime
				jobRuns[scheduleKey].JobStatus = detail.JobStatus
				jobRuns[scheduleKey].WaitStartTime = detail.WaitStartTime
				jobRuns[scheduleKey].WaitEndTime = detail.WaitEndTime
				jobRuns[scheduleKey].TaskStartTime = detail.TaskStartTime
				jobRuns[scheduleKey].TaskEndTime = detail.TaskEndTime
				jobRuns[scheduleKey].HookStartTime = detail.HookStartTime
				jobRuns[scheduleKey].HookEndTime = detail.HookEndTime
			}
		}
	}

	return result, nil
}

func (r *LineageResolver) populateLineageWithJobRuns(lineage *scheduler.JobLineageSummary, jobRunDetails map[scheduler.JobName]map[time.Time]*scheduler.JobRunSummary, result map[scheduler.JobName]*scheduler.JobLineageSummary) *scheduler.JobLineageSummary {
	if _, ok := result[lineage.JobName]; ok {
		return result[lineage.JobName]
	}

	result[lineage.JobName] = &scheduler.JobLineageSummary{
		JobName:          lineage.JobName,
		Tenant:           lineage.Tenant,
		IsEnabled:        lineage.IsEnabled,
		Window:           lineage.Window,
		ScheduleInterval: lineage.ScheduleInterval,
		SLA:              lineage.SLA,
		Upstreams:        make([]*scheduler.JobLineageSummary, len(lineage.Upstreams)),
	}

	if jobRuns, exists := jobRunDetails[lineage.JobName]; exists {
		// only fetch job runs that are necessary in the lineage
		result[lineage.JobName].JobRuns = map[scheduler.JobName]*scheduler.JobRunSummary{}
		for targetJobName, jobRun := range lineage.JobRuns {
			if jobRun, exists := jobRuns[jobRun.ScheduledAt]; exists {
				result[lineage.JobName].JobRuns[targetJobName] = jobRun
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
		JobStatus:     source.JobStatus,
		WaitStartTime: source.WaitStartTime,
		WaitEndTime:   source.WaitEndTime,
		TaskStartTime: source.TaskStartTime,
		TaskEndTime:   source.TaskEndTime,
		HookStartTime: source.HookStartTime,
		HookEndTime:   source.HookEndTime,
	}
}
