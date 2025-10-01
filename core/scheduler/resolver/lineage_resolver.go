package resolver

import (
	"context"
	"sort"
	"time"

	"github.com/goto/optimus/core/scheduler"
	"github.com/goto/optimus/core/tenant"
	"github.com/goto/optimus/internal/lib/window"
	"github.com/goto/salt/log"
)

const (
	// MaxLineageDepth is a safeguard to avoid infinite recursion in case of unexpected cycles
	MaxLineageDepth = 50
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

func (r *LineageResolver) buildSingleJobLineage(ctx context.Context, schedule *scheduler.JobSchedule, lineageData *LineageData, maxUpstreamsPerLevel int) (*scheduler.JobLineageSummary, error) {

	lineage := r.buildLineageTree(schedule.JobName, lineageData, make(map[scheduler.JobName]bool), 0)

	finalLineage, err := r.getAllUpstreamRuns(ctx, lineage, schedule.ScheduledAt, lineageData)
	if err != nil {
		return nil, err
	}

	if maxUpstreamsPerLevel > 0 {
		finalLineage = r.pruneLineage(finalLineage, maxUpstreamsPerLevel, 0)
	}

	return finalLineage, nil
}

type upstreamCandidate struct {
	JobName  scheduler.JobName
	Duration time.Duration
}

func (r *LineageResolver) pruneLineage(lineage *scheduler.JobLineageSummary, maxUpstreamsPerLevel, depth int) *scheduler.JobLineageSummary {
	// base case: stop if max depth reached or number of upstreams is already within limit
	if depth > MaxLineageDepth || len(lineage.Upstreams) <= maxUpstreamsPerLevel {
		prunedUpstreams := make([]*scheduler.JobLineageSummary, len(lineage.Upstreams))
		for i, upstream := range lineage.Upstreams {
			prunedUpstreams[i] = r.pruneLineage(upstream, maxUpstreamsPerLevel, depth+1)
		}

		return &scheduler.JobLineageSummary{
			JobName:          lineage.JobName,
			Tenant:           lineage.Tenant,
			Window:           lineage.Window,
			ScheduleInterval: lineage.ScheduleInterval,
			SLA:              lineage.SLA,
			JobRuns:          r.copyJobRuns(lineage.JobRuns),
			Upstreams:        prunedUpstreams,
		}
	}

	candidates := r.extractUpstreamCandidatesSortedByDuration(lineage)

	topUpstreams := []*scheduler.JobLineageSummary{}
	for i := 0; i < maxUpstreamsPerLevel && i < len(candidates); i++ {
		targetJobName := candidates[i].JobName
		for _, upstream := range lineage.Upstreams {
			if upstream.JobName == targetJobName {
				prunedUpstream := r.pruneLineage(upstream, maxUpstreamsPerLevel, depth+1)
				topUpstreams = append(topUpstreams, prunedUpstream)
				break
			}
		}
	}

	return &scheduler.JobLineageSummary{
		JobName:          lineage.JobName,
		Tenant:           lineage.Tenant,
		Window:           lineage.Window,
		ScheduleInterval: lineage.ScheduleInterval,
		SLA:              lineage.SLA,
		JobRuns:          r.copyJobRuns(lineage.JobRuns),
		Upstreams:        topUpstreams,
	}
}

func (r *LineageResolver) extractUpstreamCandidatesSortedByDuration(lineage *scheduler.JobLineageSummary) []upstreamCandidate {
	candidates := []upstreamCandidate{}

	for _, upstream := range lineage.Upstreams {
		duration := r.getJobRunDuration(upstream.JobRuns)
		candidates = append(candidates, upstreamCandidate{
			JobName:  upstream.JobName,
			Duration: duration,
		})
	}

	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].Duration > candidates[j].Duration
	})

	return candidates
}

func (r *LineageResolver) getJobRunDuration(jobRuns map[string]*scheduler.JobRunSummary) time.Duration {
	var duration time.Duration

	// for now, we only select the latest scheduled job run to calculate duration
	// there is an assumption made here that latest scheduled job run should be the one
	// contributing to the bottleneck
	var latestRun *scheduler.JobRunSummary
	for _, jobRun := range jobRuns {
		if latestRun == nil || jobRun.ScheduledAt.After(latestRun.ScheduledAt) {
			latestRun = jobRun
		}
	}

	if latestRun != nil && latestRun.JobStartTime != nil && latestRun.JobEndTime != nil {
		duration = latestRun.JobEndTime.Sub(*latestRun.JobStartTime)
	}

	return duration
}

func (r *LineageResolver) buildLineageTree(jobName scheduler.JobName, lineageData *LineageData, visited map[scheduler.JobName]bool, depth int) *scheduler.JobLineageSummary {
	if visited[jobName] || depth > MaxLineageDepth {
		return &scheduler.JobLineageSummary{
			JobName:   jobName,
			JobRuns:   make(map[string]*scheduler.JobRunSummary),
			Upstreams: []*scheduler.JobLineageSummary{},
		}
	}

	visited[jobName] = true
	defer delete(visited, jobName)

	var upstreams []*scheduler.JobLineageSummary
	for _, upstreamName := range lineageData.UpstreamsByJob[jobName] {
		upstreamLineage := r.buildLineageTree(upstreamName, lineageData, visited, depth+1)
		upstreams = append(upstreams, upstreamLineage)
	}

	enriched := &scheduler.JobLineageSummary{
		JobName:   jobName,
		JobRuns:   make(map[string]*scheduler.JobRunSummary),
		Upstreams: upstreams,
	}

	if job, exists := lineageData.JobsByName[jobName]; exists {
		enriched.Tenant = job.Tenant
		enriched.Window = &job.Window
		enriched.ScheduleInterval = job.ScheduleInterval
		enriched.SLA = job.SLA
	}

	return enriched
}

func (r *LineageResolver) getAllUpstreamRuns(ctx context.Context, lineage *scheduler.JobLineageSummary, scheduledAt time.Time, lineageData *LineageData) (*scheduler.JobLineageSummary, error) {
	allJobRunsMap := make(map[scheduler.JobName]map[string]*scheduler.JobRunSummary)
	// initialize first job run in the lineage
	lineage.JobRuns = map[string]*scheduler.JobRunSummary{
		scheduledAt.UTC().Format(time.RFC3339): {
			ScheduledAt: scheduledAt,
		},
	}

	err := r.calculateAllUpstreamRuns(ctx, lineage, lineageData, allJobRunsMap, make(map[scheduler.JobName]bool))
	if err != nil {
		return nil, err
	}

	jobRunDetails, err := r.fetchJobRunDetails(ctx, allJobRunsMap)
	if err != nil {
		return nil, err
	}

	return r.populateLineageWithJobRuns(lineage, jobRunDetails), nil
}

func (r *LineageResolver) calculateAllUpstreamRuns(ctx context.Context, lineage *scheduler.JobLineageSummary, lineageData *LineageData, allJobRunsMap map[scheduler.JobName]map[string]*scheduler.JobRunSummary, visited map[scheduler.JobName]bool) error {
	if visited[lineage.JobName] || len(visited) > 1000 {
		return nil
	}
	visited[lineage.JobName] = true

	if _, exists := allJobRunsMap[lineage.JobName]; !exists {
		allJobRunsMap[lineage.JobName] = make(map[string]*scheduler.JobRunSummary)
	}
	for key, jobRun := range lineage.JobRuns {
		allJobRunsMap[lineage.JobName][key] = jobRun
	}

	currentJob := lineageData.JobsByName[lineage.JobName]
	if currentJob == nil {
		return nil
	}

	for _, upstream := range lineage.Upstreams {
		upstreamJob := lineageData.JobsByName[upstream.JobName]
		if upstreamJob == nil {
			continue
		}

		for _, jobRun := range lineage.JobRuns {
			upstreamSchedules, err := r.getUpstreamRuns(ctx, currentJob, upstreamJob, jobRun.ScheduledAt, lineageData.ProjectsByName)
			if err != nil {
				return err
			}

			if _, exists := allJobRunsMap[upstream.JobName]; !exists {
				allJobRunsMap[upstream.JobName] = make(map[string]*scheduler.JobRunSummary)
			}

			for _, schedule := range upstreamSchedules {
				scheduleKey := schedule.Format(time.RFC3339)
				if _, exists := allJobRunsMap[upstream.JobName][scheduleKey]; !exists {
					allJobRunsMap[upstream.JobName][scheduleKey] = &scheduler.JobRunSummary{
						ScheduledAt: schedule,
					}
				}
				if upstream.JobRuns == nil {
					upstream.JobRuns = make(map[string]*scheduler.JobRunSummary)
				}
				upstream.JobRuns[scheduleKey] = allJobRunsMap[upstream.JobName][scheduleKey]
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

	// GetExpectedRunSchedules return sorted schedule
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

func (r *LineageResolver) fetchJobRunDetails(ctx context.Context, allJobRunsMap map[scheduler.JobName]map[string]*scheduler.JobRunSummary) (map[scheduler.JobName]map[string]*scheduler.JobRunSummary, error) {
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

	result := make(map[scheduler.JobName]map[string]*scheduler.JobRunSummary)
	for jobName, jobRuns := range allJobRunsMap {
		result[jobName] = make(map[string]*scheduler.JobRunSummary)
		for scheduleKey, jobRun := range jobRuns {
			result[jobName][scheduleKey] = r.copyJobRun(jobRun)
		}
	}

	for _, detail := range jobRunDetails {
		scheduleKey := detail.ScheduledAt.UTC().Format(time.RFC3339)
		if jobRuns, exists := result[detail.JobName]; exists {
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

func (r *LineageResolver) populateLineageWithJobRuns(lineage *scheduler.JobLineageSummary, jobRunDetails map[scheduler.JobName]map[string]*scheduler.JobRunSummary) *scheduler.JobLineageSummary {
	result := &scheduler.JobLineageSummary{
		JobName:          lineage.JobName,
		Tenant:           lineage.Tenant,
		Window:           lineage.Window,
		ScheduleInterval: lineage.ScheduleInterval,
		SLA:              lineage.SLA,
		Upstreams:        make([]*scheduler.JobLineageSummary, len(lineage.Upstreams)),
	}

	if jobRuns, exists := jobRunDetails[lineage.JobName]; exists {
		result.JobRuns = r.copyJobRuns(jobRuns)
	} else {
		result.JobRuns = r.copyJobRuns(lineage.JobRuns)
	}

	for i, upstream := range lineage.Upstreams {
		result.Upstreams[i] = r.populateLineageWithJobRuns(upstream, jobRunDetails)
	}

	return result
}

func (r *LineageResolver) copyJobRuns(source map[string]*scheduler.JobRunSummary) map[string]*scheduler.JobRunSummary {
	if source == nil {
		return make(map[string]*scheduler.JobRunSummary)
	}
	result := make(map[string]*scheduler.JobRunSummary, len(source))
	for key, jobRun := range source {
		result[key] = r.copyJobRun(jobRun)
	}
	return result
}

func (r *LineageResolver) copyJobRun(source *scheduler.JobRunSummary) *scheduler.JobRunSummary {
	if source == nil {
		return nil
	}
	return &scheduler.JobRunSummary{
		JobName:       source.JobName,
		ScheduledAt:   source.ScheduledAt,
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
