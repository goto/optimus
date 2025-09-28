package service

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/google/uuid"
	"github.com/goto/salt/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/goto/optimus/config"
	"github.com/goto/optimus/core/event"
	"github.com/goto/optimus/core/event/moderator"
	"github.com/goto/optimus/core/job"
	"github.com/goto/optimus/core/scheduler"
	"github.com/goto/optimus/core/tenant"
	"github.com/goto/optimus/internal/errors"
	"github.com/goto/optimus/internal/lib/cron"
	"github.com/goto/optimus/internal/lib/duration"
	"github.com/goto/optimus/internal/lib/interval"
	"github.com/goto/optimus/internal/lib/window"
	"github.com/goto/optimus/internal/models"
	"github.com/goto/optimus/internal/utils/filter"
)

type metricType string

func (m metricType) String() string {
	return string(m)
}

const (
	scheduleDelay metricType = "schedule_delay"
	SensorV1      string     = "USE_DEPRECATED_SENSOR_V1"
)

var jobRunEventsMetric = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "operator_stats",
	Help: "total job run events received",
}, []string{"operator_name", "operator_type", "event_type"})

var jobRunDdurationsBreakdownSeconds = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Name: "jobrun_durations_breakdown_seconds",
	Help: "operator wise time spent",
}, []string{"project", "namespace", "job", "type"})

var jobRunStatus = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Name: "job_run_status",
	Help: "status of the runs for sensor",
}, []string{"project", "job", "version"})

type JobRepository interface {
	GetJob(ctx context.Context, name tenant.ProjectName, jobName scheduler.JobName) (*scheduler.Job, error)
	GetJobDetails(ctx context.Context, projectName tenant.ProjectName, jobName scheduler.JobName) (*scheduler.JobWithDetails, error)
	GetAll(ctx context.Context, projectName tenant.ProjectName) ([]*scheduler.JobWithDetails, error)
	GetJobs(ctx context.Context, projectName tenant.ProjectName, jobs []string) ([]*scheduler.JobWithDetails, error)
}

type JobRunRepository interface {
	GetByID(ctx context.Context, id scheduler.JobRunID) (*scheduler.JobRun, error)
	GetByScheduledAt(ctx context.Context, tenant tenant.Tenant, name scheduler.JobName, scheduledAt time.Time) (*scheduler.JobRun, error)
	GetLatestRun(ctx context.Context, project tenant.ProjectName, name scheduler.JobName, status *scheduler.State) (*scheduler.JobRun, error)
	GetRunsByTimeRange(ctx context.Context, project tenant.ProjectName, jobName scheduler.JobName, status *scheduler.State, since, until time.Time) ([]*scheduler.JobRun, error)
	GetRunsByInterval(ctx context.Context, project tenant.ProjectName, jobName scheduler.JobName, interval interval.Interval) ([]*scheduler.JobRun, error)
	GetByScheduledTimes(ctx context.Context, tenant tenant.Tenant, jobName scheduler.JobName, scheduledTimes []time.Time) ([]*scheduler.JobRun, error)
	Create(ctx context.Context, tenant tenant.Tenant, name scheduler.JobName, scheduledAt time.Time, interval interval.Interval, slaDefinitionInSec int64) error
	Update(ctx context.Context, jobRunID uuid.UUID, endTime time.Time, jobRunStatus scheduler.State) error
	UpdateState(ctx context.Context, jobRunID uuid.UUID, jobRunStatus scheduler.State) error
	UpdateSLA(ctx context.Context, jobName scheduler.JobName, project tenant.ProjectName, scheduledTimes []time.Time) error
	UpdateMonitoring(ctx context.Context, jobRunID uuid.UUID, monitoring map[string]any) error
}

type JobReplayRepository interface {
	GetReplayJobConfig(ctx context.Context, jobTenant tenant.Tenant, jobName scheduler.JobName, scheduledAt time.Time) (map[string]string, error)
}

type OperatorRunRepository interface {
	GetOperatorRun(ctx context.Context, operatorName string, operator scheduler.OperatorType, jobRunID uuid.UUID) (*scheduler.OperatorRun, error)
	CreateOperatorRun(ctx context.Context, operatorName string, operator scheduler.OperatorType, jobRunID uuid.UUID, startTime time.Time) error
	UpdateOperatorRun(ctx context.Context, operator scheduler.OperatorType, jobRunID uuid.UUID, eventTime time.Time, state scheduler.State) error
}

type SLARepository interface {
	RegisterSLA(ctx context.Context, projectName tenant.ProjectName, jobName, operatorName, operatorType, runID string, slaTime time.Time, description string) error
	UpdateSLA(ctx context.Context, projectName tenant.ProjectName, jobName, OperatorName, operatorType, runID string, slaTime time.Time) error
	FinishSLA(ctx context.Context, projectName tenant.ProjectName, jobName, OperatorName, operatorType, runID string, operatorEndTime time.Time) error
}

type JobInputCompiler interface {
	Compile(ctx context.Context, job *scheduler.JobWithDetails, config scheduler.RunConfig, executedAt time.Time) (*scheduler.ExecutorInput, error)
}

type PriorityResolver interface {
	Resolve(context.Context, []*scheduler.JobWithDetails) error
}

type Scheduler interface {
	GetJobRuns(ctx context.Context, t tenant.Tenant, criteria *scheduler.JobRunsCriteria, jobCron *cron.ScheduleSpec) ([]*scheduler.JobRunStatus, error)
	DeployJobs(ctx context.Context, t tenant.Tenant, jobs []*scheduler.JobWithDetails) error
	ListJobs(ctx context.Context, t tenant.Tenant) ([]string, error)
	DeleteJobs(ctx context.Context, t tenant.Tenant, jobsToDelete []string) error
	UpdateJobState(ctx context.Context, projectName tenant.ProjectName, jobName []job.Name, state string) error
	GetJobState(ctx context.Context, projectName tenant.ProjectName) (map[string]bool, error)
	GetRolePermissions(ctx context.Context, t tenant.Tenant, roleName string) ([]string, error)
	AddRole(ctx context.Context, t tenant.Tenant, roleName string, ifNotExist bool) error
}

type EventHandler interface {
	HandleEvent(moderator.Event)
}

type ProjectGetter interface {
	Get(context.Context, tenant.ProjectName) (*tenant.Project, error)
}

type JobRunService struct {
	l                log.Logger
	repo             JobRunRepository
	replayRepo       JobReplayRepository
	slaRepo          SLARepository
	operatorRunRepo  OperatorRunRepository
	eventHandler     EventHandler
	scheduler        Scheduler
	jobRepo          JobRepository
	priorityResolver PriorityResolver
	compiler         JobInputCompiler
	projectGetter    ProjectGetter
	features         config.FeaturesConfig
}

func (s *JobRunService) JobRunInput(ctx context.Context, projectName tenant.ProjectName, jobName scheduler.JobName, config scheduler.RunConfig) (*scheduler.ExecutorInput, error) {
	details, err := s.jobRepo.GetJobDetails(ctx, projectName, jobName)
	if err != nil {
		s.l.Error("error getting job [%s]: %s", jobName, err)
		return nil, err
	}
	// TODO: Use scheduled_at instead of executed_at for computations, for deterministic calculations
	// Todo: later, always return scheduleTime, for scheduleTimes greater than a given date
	var jobRun *scheduler.JobRun
	if config.JobRunID.IsEmpty() {
		s.l.Warn("getting job run by scheduled at")
		jobRun, err = s.repo.GetByScheduledAt(ctx, details.Job.Tenant, jobName, config.ScheduledAt)
	} else {
		s.l.Warn("getting job run by id")
		jobRun, err = s.repo.GetByID(ctx, config.JobRunID)
	}

	var executedAt time.Time
	if err != nil { // Fallback for executed_at to scheduled_at
		executedAt = config.ScheduledAt
		s.l.Warn("suppressed error is encountered when getting job run: %s", err)
	} else {
		executedAt = jobRun.StartTime
	}
	// Additional task config from existing replay
	replayJobConfig, err := s.replayRepo.GetReplayJobConfig(ctx, details.Job.Tenant, details.Job.Name, config.ScheduledAt)
	if err != nil {
		s.l.Error("error getting replay job config from db: %s", err)
		return nil, err
	}
	for k, v := range replayJobConfig {
		details.Job.Task.Config[k] = v
	}

	return s.compiler.Compile(ctx, details, config, executedAt)
}

func (s *JobRunService) GetJobRunsByFilter(ctx context.Context, projectName tenant.ProjectName, jobName scheduler.JobName, filters ...filter.FilterOpt) ([]*scheduler.JobRun, error) {
	f := filter.NewFilter(filters...)
	var runState *scheduler.State
	state, err := scheduler.StateFromString(f.GetStringValue(filter.RunState))
	if err == nil {
		runState = &state
	}

	if f.Contains(filter.StartDate) && f.Contains(filter.EndDate) {
		//	get job run by scheduled at between start date and end date, filter by runState if applicable
		return s.repo.GetRunsByTimeRange(ctx, projectName, jobName, runState,
			f.GetTimeValue(filter.StartDate), f.GetTimeValue(filter.EndDate))
	}

	jobRun, err := s.repo.GetLatestRun(ctx, projectName, jobName, runState)
	if err != nil {
		if errors.IsErrorType(err, errors.ErrNotFound) {
			return []*scheduler.JobRun{}, nil
		}
		return nil, err
	}
	return []*scheduler.JobRun{jobRun}, nil
}

func (s *JobRunService) GetJobRuns(ctx context.Context, projectName tenant.ProjectName, jobName scheduler.JobName, requestCriteria *scheduler.JobRunsCriteria) ([]*scheduler.JobRunStatus, string, error) {
	jobWithDetails, err := s.jobRepo.GetJobDetails(ctx, projectName, jobName)
	if err != nil {
		msg := fmt.Sprintf("unable to get job details for jobName: %s, project:%s", jobName, projectName)
		s.l.Error(msg)
		return nil, "", errors.AddErrContext(err, scheduler.EntityJobRun, msg)
	}
	interval := jobWithDetails.Schedule.Interval
	if interval == "" {
		s.l.Error("job schedule interval is empty")
		return nil, "", errors.InvalidArgument(scheduler.EntityJobRun, "cannot get job runs, job interval is empty")
	}
	jobCron, err := cron.ParseCronSchedule(interval)
	if err != nil {
		msg := fmt.Sprintf("unable to parse job cron interval: %s", err)
		s.l.Error(msg)
		return nil, "", errors.InternalError(scheduler.EntityJobRun, msg, nil)
	}

	if requestCriteria.OnlyLastRun {
		return s.getLastRun(ctx, jobWithDetails.Job.Tenant, requestCriteria, jobCron)
	}

	project, err := s.projectGetter.Get(ctx, projectName)
	if err != nil {
		return nil, "", err
	}

	w, err := getWindow(project, jobWithDetails)
	if err != nil {
		return nil, "", err
	}

	criteria, msg, err := s.cleanupJobQuery(*requestCriteria, jobWithDetails)
	if err != nil {
		msg += fmt.Sprintf("invalid job query: %s\n", err)
		s.l.Error(msg)
		return nil, msg, err
	}

	if criteria.EndDate.Before(criteria.StartDate) {
		msg += fmt.Sprintf("[GetJobRuns] for job:[%s], criteria EndDate:[%s] is before criteria StartDate:[%s], incomming Request Criteria : [%#v]\n",
			jobWithDetails.Name, criteria.EndDate.String(), criteria.StartDate.String(), requestCriteria)
		s.l.Warn(msg)
		return scheduler.JobRunStatusList{}, msg, nil
	}

	expectedRuns := getExpectedRuns(jobCron, criteria.StartDate, criteria.EndDate)

	actualRuns, err := s.scheduler.GetJobRuns(ctx, jobWithDetails.Job.Tenant, &criteria, jobCron)
	if err != nil {
		s.l.Error("unable to get job runs from airflow err: %s", err)
		actualRuns = []*scheduler.JobRunStatus{}
	}

	result, c1 := filterRunsV1(expectedRuns, actualRuns, criteria)
	jobRunStatus.WithLabelValues(string(projectName), jobName.String(), "V1").Set(float64(c1))

	conf := jobWithDetails.Job.Task.Config
	if _, ok := conf[SensorV1]; ok {
		return result, msg, nil
	}

	result2, c2 := filterRunsV2(expectedRuns, actualRuns, criteria, w)
	jobRunStatus.WithLabelValues(string(projectName), jobName.String(), "V2").Set(float64(c2))

	result3, c3 := s.FilterRunsV3(ctx, jobWithDetails.Job.Tenant, criteria)
	jobRunStatus.WithLabelValues(string(projectName), jobName.String(), "V3").Set(float64(c3))

	if !s.features.EnableV3Sensor {
		c3 = -1
	}

	s.l.Debug("[%s] The count for each v1=%d, v2=%d, v3=%d", jobName, c1, c2, c3)
	m1 := max1(c1, c2, c3)
	if m1 == c3 {
		return result3, msg, nil
	}
	if m1 == c2 {
		return result2, msg, nil
	}
	return result, msg, nil
}

func (s *JobRunService) getLastRun(ctx context.Context, tnnt tenant.Tenant, requestCriteria *scheduler.JobRunsCriteria, jobCron *cron.ScheduleSpec) ([]*scheduler.JobRunStatus, string, error) {
	response := scheduler.JobRunStatusList{}
	c1 := -1
	msg := "getting last run only"
	name := scheduler.JobName(requestCriteria.Name)

	run, err := s.repo.GetLatestRun(ctx, tnnt.ProjectName(), name, nil)
	if err == nil {
		r1 := &scheduler.JobRunStatus{
			ScheduledAt: run.ScheduledAt,
			State:       run.State,
		}
		response = []*scheduler.JobRunStatus{r1}
		c1 = response.GetSuccessRuns()
		jobRunStatus.WithLabelValues(tnnt.ProjectName().String(), name.String(), "V3").Set(float64(c1))
	}

	s.l.Warn(msg)
	c2 := 0
	var airflowResp scheduler.JobRunStatusList
	runs, err2 := s.scheduler.GetJobRuns(ctx, tnnt, requestCriteria, jobCron)
	if err2 == nil {
		airflowResp = runs
		c2 = airflowResp.GetSuccessRuns()
		jobRunStatus.WithLabelValues(tnnt.ProjectName().String(), name.String(), "V1").Set(float64(c2))
	} else {
		s.l.Error("Error getting job runs from airflow")
		if c1 == -1 {
			return nil, msg, err2
		}
	}

	if !s.features.EnableV3Sensor {
		if err2 != nil {
			return nil, msg, err2
		}
		c1 = -1
	}

	if c1 > c2 {
		return response, msg, nil
	}

	return airflowResp, msg, nil
}

func filterRunsV1(expectedRuns, actualRuns scheduler.JobRunStatusList, criteria scheduler.JobRunsCriteria) (scheduler.JobRunStatusList, int) {
	totalRuns := mergeRuns(expectedRuns, actualRuns) // main function to target

	// FilterRuns does not do anything, filter is empty
	result := filterRuns(totalRuns, createFilterSet(criteria.Filter))
	count := result.GetSuccessRuns()
	return result, count
}

func filterRunsV2(expectedRuns, actualRuns scheduler.JobRunStatusList, criteria scheduler.JobRunsCriteria, w window.Window) (scheduler.JobRunStatusList, int) {
	expectedRange := createIntervals(expectedRuns, w)
	actualRange := createIntervals(actualRuns, w)

	updatedRange := expectedRange.UpdateDataFrom(actualRange)
	result := filterRuns(updatedRange.Values(), createFilterSet(criteria.Filter))
	count := result.GetSuccessRuns()
	return result, count
}

func (s *JobRunService) FilterRunsV3(ctx context.Context, tnnt tenant.Tenant, criteria scheduler.JobRunsCriteria) (scheduler.JobRunStatusList, int) {
	name := scheduler.JobName(criteria.Name)
	intr := interval.NewInterval(criteria.StartDate, criteria.EndDate)
	jobRuns, err := s.repo.GetRunsByInterval(ctx, tnnt.ProjectName(), name, intr)
	if err != nil {
		s.l.Error("Error getting job runs from db")
		return nil, -1
	}

	r1 := createRangesFromRuns(jobRuns)
	r2 := fillMissingIntervals(r1, intr)
	result := filterRuns(r2.Values(), createFilterSet(criteria.Filter))
	count := result.GetSuccessRuns()
	return result, count
}

func createRangesFromRuns(runs []*scheduler.JobRun) interval.Range[*scheduler.JobRunStatus] {
	mapping := map[interval.Interval]*scheduler.JobRunStatus{}
	for _, run := range runs {
		start := run.WindowStart
		end := run.WindowEnd
		if start == nil || end == nil {
			continue
		}

		in := interval.NewInterval(*start, *end)
		status := &scheduler.JobRunStatus{
			ScheduledAt: run.ScheduledAt,
			State:       run.State,
		}
		v, ok := mapping[in]
		if !ok {
			mapping[in] = status
		} else if v.State != scheduler.StateSuccess {
			mapping[in] = status
		}
	}
	ranges := interval.Range[*scheduler.JobRunStatus]{}
	for in, v := range mapping {
		ranges = append(ranges, interval.Data[*scheduler.JobRunStatus]{
			In:   in,
			Data: v,
		})
	}
	return ranges
}

func fillMissingIntervals(r1 interval.Range[*scheduler.JobRunStatus], expected interval.Interval) interval.Range[*scheduler.JobRunStatus] {
	sort.Slice(r1, func(i, j int) bool {
		return r1[i].In.Start().Before(r1[j].In.Start())
	})

	missing := []interval.Data[*scheduler.JobRunStatus]{}
	currentStart := expected.Start()
	currentEnd := time.Time{}
	for _, r := range r1 {
		if r.In.Start().After(currentStart) {
			missing = append(missing, interval.Data[*scheduler.JobRunStatus]{
				In: interval.NewInterval(currentStart, r.In.Start()),
				Data: &scheduler.JobRunStatus{
					ScheduledAt: r.In.Start(),
					State:       scheduler.StatePending,
				},
			})
		}
		currentStart = r.In.End()
		currentEnd = r.In.End()
	}

	if currentEnd.Before(expected.End()) {
		missing = append(missing, interval.Data[*scheduler.JobRunStatus]{
			In: interval.NewInterval(currentEnd, expected.End()),
			Data: &scheduler.JobRunStatus{
				ScheduledAt: currentEnd,
				State:       scheduler.StatePending,
			},
		})
	}

	return append(r1, missing...)
}

func createIntervals(runs scheduler.JobRunStatusList, w window.Window) interval.Range[*scheduler.JobRunStatus] {
	ranges := interval.Range[*scheduler.JobRunStatus]{}
	for _, run := range runs.GetSortedRunsByScheduledAt() {
		i1, err := w.GetInterval(run.ScheduledAt.UTC())
		if err != nil {
			continue
		}
		ranges = append(ranges, interval.Data[*scheduler.JobRunStatus]{
			In:   i1,
			Data: run,
		})
	}
	return ranges
}

func (s *JobRunService) GetInterval(ctx context.Context, projectName tenant.ProjectName, jobName scheduler.JobName, referenceTime time.Time) (interval.Interval, error) {
	project, err := s.projectGetter.Get(ctx, projectName)
	if err != nil {
		s.l.Error("error getting project [%s]: %s", projectName, err)
		return interval.Interval{}, err
	}

	job, err := s.jobRepo.GetJobDetails(ctx, projectName, jobName)
	if err != nil {
		s.l.Error("error getting job detail [%s] under project [%s]: %s", jobName, projectName, err)
		return interval.Interval{}, err
	}

	return s.getInterval(project, job, referenceTime)
}

// TODO: this method is only for backward compatibility, it will be deprecated soon
func (s *JobRunService) getInterval(project *tenant.Project, job *scheduler.JobWithDetails, referenceTime time.Time) (interval.Interval, error) {
	windowConfig := job.Job.WindowConfig

	if windowConfig.Type() == window.Incremental {
		w, err := window.FromSchedule(job.Schedule.Interval)
		if err != nil {
			s.l.Error("error getting window with type incremental: %v", err)
			return interval.Interval{}, err
		}

		return w.GetInterval(referenceTime)
	}

	if windowConfig.Type() == window.Preset || windowConfig.GetVersion() == window.NewWindowVersion {
		var config window.SimpleConfig
		if windowConfig.Type() == window.Preset {
			preset, err := project.GetPreset(windowConfig.Preset)
			if err != nil {
				s.l.Error("error getting preset [%s] for project [%s]: %v", windowConfig.Preset, project.Name(), err)
				return interval.Interval{}, err
			}

			config = preset.Config()
		} else {
			config = windowConfig.GetSimpleConfig()
		}

		config.ShiftBy = ""
		config.TruncateTo = string(duration.None)

		cw, err := window.FromCustomConfig(config)
		if err != nil {
			return interval.Interval{}, err
		}
		return cw.GetInterval(referenceTime)
	}

	w, err := models.NewWindow(windowConfig.GetVersion(), "", "0", windowConfig.GetSize())
	if err != nil {
		s.l.Error("error initializing window: %v", err)
		return interval.Interval{}, err
	}

	if err := w.Validate(); err != nil {
		s.l.Error("error validating window: %v", err)
		return interval.Interval{}, err
	}

	startTime, err := w.GetStartTime(referenceTime)
	if err != nil {
		return interval.Interval{}, err
	}

	endTime, err := w.GetEndTime(referenceTime)
	if err != nil {
		return interval.Interval{}, err
	}

	return interval.NewInterval(startTime, endTime), nil
}

func getExpectedRuns(spec *cron.ScheduleSpec, startTime, endTime time.Time) []*scheduler.JobRunStatus {
	var jobRuns []*scheduler.JobRunStatus
	start := spec.Next(startTime.Add(-time.Second * 1))
	end := endTime
	exit := spec.Next(end)
	for start.Before(exit) {
		jobRuns = append(jobRuns, &scheduler.JobRunStatus{
			State:       scheduler.StatePending,
			ScheduledAt: start,
		})
		start = spec.Next(start)
	}
	return jobRuns
}

func mergeRuns(expected, actual []*scheduler.JobRunStatus) []*scheduler.JobRunStatus {
	var merged []*scheduler.JobRunStatus
	m := actualRunMap(actual)
	for _, exp := range expected {
		if act, ok := m[exp.ScheduledAt.UTC().String()]; ok {
			merged = append(merged, &act)
		} else {
			merged = append(merged, exp)
		}
	}
	return merged
}

func actualRunMap(runs []*scheduler.JobRunStatus) map[string]scheduler.JobRunStatus {
	m := map[string]scheduler.JobRunStatus{}
	for _, v := range runs {
		m[v.ScheduledAt.UTC().String()] = *v
	}
	return m
}

func filterRuns(runs []*scheduler.JobRunStatus, filter map[string]struct{}) scheduler.JobRunStatusList {
	var filteredRuns []*scheduler.JobRunStatus
	if len(filter) == 0 {
		return runs
	}
	for _, v := range runs {
		if _, ok := filter[v.State.String()]; ok {
			filteredRuns = append(filteredRuns, v)
		}
	}
	return filteredRuns
}

func createFilterSet(filter []string) map[string]struct{} {
	m := map[string]struct{}{}
	for _, v := range filter {
		m[v] = struct{}{}
	}
	return m
}

func (s *JobRunService) cleanupJobQuery(requestCriteria scheduler.JobRunsCriteria, jobWithDetails *scheduler.JobWithDetails) (scheduler.JobRunsCriteria, string, error) {
	scheduleStartTime, err := jobWithDetails.Schedule.GetScheduleStartTime()
	if err != nil {
		return requestCriteria, "", err
	}
	var msg string
	if requestCriteria.StartDate.Before(scheduleStartTime) {
		msg += fmt.Sprintf("[GetJobRuns] for job:[%s] , request criteria StartDate:[%s] is earlier than Job StartDate:[%s], "+
			"truncating request criteria StartDate to Job StartDate \n", jobWithDetails.Name, requestCriteria.StartDate.String(), jobWithDetails.Schedule.StartDate.String())
		s.l.Warn(msg)
		requestCriteria.StartDate = scheduleStartTime
	}

	if jobWithDetails.Schedule.EndDate != nil && requestCriteria.EndDate.After(*jobWithDetails.Schedule.EndDate) {
		msg += fmt.Sprintf("[GetJobRuns] for job:[%s] , request criteria End:[%s] is after Job EndDate:[%s], "+
			"truncating request criteria EndDate to Job EndDate \n", jobWithDetails.Name, requestCriteria.EndDate.String(), jobWithDetails.Schedule.EndDate.String())
		s.l.Warn(msg)
		requestCriteria.EndDate = *jobWithDetails.Schedule.EndDate
	}

	return requestCriteria, msg, nil
}

func (s *JobRunService) registerNewJobRun(ctx context.Context, tenant tenant.Tenant, jobName scheduler.JobName, scheduledAt time.Time) error {
	job, err := s.jobRepo.GetJobDetails(ctx, tenant.ProjectName(), jobName)
	if err != nil {
		s.l.Error("error getting job details for job [%s]: %s", jobName, err)
		return err
	}
	slaDefinitionInSec, err := job.SLADuration()
	if err != nil {
		s.l.Error("error getting sla duration: %s", err)
		return err
	}

	project, err := s.projectGetter.Get(ctx, job.Job.Tenant.ProjectName())
	if err != nil {
		return err
	}

	w, err := getWindow(project, job)
	if err != nil {
		return err
	}

	windowInterval, err := w.GetInterval(scheduledAt)
	if err != nil {
		return err
	}

	err = s.repo.Create(ctx, tenant, jobName, scheduledAt, windowInterval, slaDefinitionInSec)
	if err != nil {
		s.l.Error("error creating job run: %s", err)
		return err
	}

	jobRunDdurationsBreakdownSeconds.WithLabelValues(
		tenant.ProjectName().String(),
		tenant.NamespaceName().String(),
		jobName.String(),
		scheduleDelay.String(),
	).Set(float64(time.Now().Unix() - scheduledAt.Unix()))

	return nil
}

func (s *JobRunService) getJobRunByScheduledAt(ctx context.Context, tenant tenant.Tenant, jobName scheduler.JobName, scheduledAt time.Time) (*scheduler.JobRun, error) {
	var jobRun *scheduler.JobRun
	jobRun, err := s.repo.GetByScheduledAt(ctx, tenant, jobName, scheduledAt)
	if err != nil {
		if !errors.IsErrorType(err, errors.ErrNotFound) {
			s.l.Error("error getting job run by scheduled at: %s", err)
			return nil, err
		}
		// TODO: consider moving below call outside as the caller is a 'getter'
		err = s.registerNewJobRun(ctx, tenant, jobName, scheduledAt)
		if err != nil {
			s.l.Error("error registering new job run: %s", err)
			return nil, err
		}
		jobRun, err = s.repo.GetByScheduledAt(ctx, tenant, jobName, scheduledAt)
		if err != nil {
			s.l.Error("error getting the registered job run: %s", err)
			return nil, err
		}
	}
	return jobRun, nil
}

func (s *JobRunService) updateJobRun(ctx context.Context, event *scheduler.Event) error {
	var jobRun *scheduler.JobRun
	jobRun, err := s.getJobRunByScheduledAt(ctx, event.Tenant, event.JobName, event.JobScheduledAt)
	if err != nil {
		s.l.Error("error getting job run by schedule time [%s]: %s", event.JobScheduledAt, err)
		return err
	}
	if err := s.repo.Update(ctx, jobRun.ID, event.EventTime, event.Status); err != nil {
		s.l.Error("error updating job run with id [%s]: %s", jobRun.ID, err)
		return err
	}
	jobRun.State = event.Status
	s.raiseJobRunStateChangeEvent(jobRun)
	monitoringValues := s.getMonitoringValues(event)
	return s.repo.UpdateMonitoring(ctx, jobRun.ID, monitoringValues)
}

func (*JobRunService) getMonitoringValues(event *scheduler.Event) map[string]any {
	var output map[string]any
	if value, ok := event.Values["monitoring"]; ok && value != nil {
		output, _ = value.(map[string]any)
	}
	return output
}

func (s *JobRunService) filterSLAObjects(ctx context.Context, event *scheduler.Event) ([]*scheduler.SLAObject, []time.Time) {
	scheduleTimesList := make([]time.Time, len(event.SLAObjectList))
	unfilteredSLAObj := make([]*scheduler.SLAObject, len(event.SLAObjectList))
	var slaBreachedJobRunScheduleTimes []time.Time

	for i, SLAObject := range event.SLAObjectList {
		scheduleTimesList[i] = SLAObject.JobScheduledAt
		unfilteredSLAObj[i] = &scheduler.SLAObject{JobName: SLAObject.JobName, JobScheduledAt: SLAObject.JobScheduledAt}
	}
	jobRuns, err := s.repo.GetByScheduledTimes(ctx, event.Tenant, event.JobName, scheduleTimesList)
	if err != nil {
		s.l.Error("error getting job runs by schedule time, skipping the filter. jobName: %s, scheduleTimesList: %v, err: %s", event.JobName, scheduleTimesList, err.Error())
		return unfilteredSLAObj, slaBreachedJobRunScheduleTimes
	}
	if len(jobRuns) == 0 {
		s.l.Error("no job runs found for given schedule time, skipping the filter (perhaps the sla is due to schedule delay, in such cases the job wont be persisted in optimus DB). jobName: %s, scheduleTimes: %v",
			event.JobName, scheduleTimesList)
		event.Status = scheduler.StateNotScheduled
		event.JobScheduledAt = event.SLAObjectList[0].JobScheduledAt // pick the first reported sla
		return unfilteredSLAObj, slaBreachedJobRunScheduleTimes
	}

	var filteredSLAObject []*scheduler.SLAObject
	var latestScheduleTime time.Time
	var latestJobRun *scheduler.JobRun
	for _, jobRun := range jobRuns {
		if !jobRun.HasSLABreached() {
			s.l.Error("received sla miss callback for job run that has not breached SLA, jobName: %s, scheduled_at: %s, start_time: %s, end_time: %s, SLA definition: %s",
				jobRun.JobName, jobRun.ScheduledAt.String(), jobRun.StartTime, jobRun.EndTime, time.Second*time.Duration(jobRun.SLADefinition))
			continue
		}
		filteredSLAObject = append(filteredSLAObject, &scheduler.SLAObject{
			JobName:        jobRun.JobName,
			JobScheduledAt: jobRun.ScheduledAt,
		})
		if jobRun.ScheduledAt.Unix() > latestScheduleTime.Unix() {
			latestScheduleTime = jobRun.ScheduledAt
			latestJobRun = jobRun
		}
		slaBreachedJobRunScheduleTimes = append(slaBreachedJobRunScheduleTimes, jobRun.ScheduledAt)
	}
	if latestJobRun != nil {
		event.Status = latestJobRun.State
		event.JobScheduledAt = latestJobRun.ScheduledAt
	}

	return filteredSLAObject, slaBreachedJobRunScheduleTimes
}

func (s *JobRunService) updateJobRunSLA(ctx context.Context, event *scheduler.Event) error {
	if len(event.SLAObjectList) < 1 {
		return nil
	}
	var slaBreachedJobRunScheduleTimes []time.Time
	event.SLAObjectList, slaBreachedJobRunScheduleTimes = s.filterSLAObjects(ctx, event)
	err := s.repo.UpdateSLA(ctx, event.JobName, event.Tenant.ProjectName(), slaBreachedJobRunScheduleTimes)
	if err != nil {
		s.l.Error("error updating job run sla status", err)
		return err
	}
	return nil
}

func operatorStartToJobState(operatorType scheduler.OperatorType) (scheduler.State, error) {
	switch operatorType {
	case scheduler.OperatorTask:
		return scheduler.StateInProgress, nil
	case scheduler.OperatorSensor:
		return scheduler.StateWaitUpstream, nil
	case scheduler.OperatorHook:
		return scheduler.StateInProgress, nil
	default:
		return "", errors.InvalidArgument(scheduler.EntityJobRun, "Invalid operator type")
	}
}

func (s *JobRunService) raiseJobRunStateChangeEvent(jobRun *scheduler.JobRun) {
	var schedulerEvent moderator.Event
	var err error
	switch jobRun.State {
	case scheduler.StateWaitUpstream:
		schedulerEvent, err = event.NewJobRunWaitUpstreamEvent(jobRun)
	case scheduler.StateInProgress:
		schedulerEvent, err = event.NewJobRunInProgressEvent(jobRun)
	case scheduler.StateSuccess:
		schedulerEvent, err = event.NewJobRunSuccessEvent(jobRun)
	case scheduler.StateFailed:
		schedulerEvent, err = event.NewJobRunFailedEvent(jobRun)
	default:
		s.l.Error("state [%s] is unrecognized, event is not published", jobRun.State)
		return
	}
	if err != nil {
		s.l.Error("error creating event for job run state change : %s", err)
		return
	}
	s.eventHandler.HandleEvent(schedulerEvent)
}

func (s *JobRunService) registerConfiguredSLA(ctx context.Context, event *scheduler.Event) error {
	job, err := s.jobRepo.GetJob(ctx, event.Tenant.ProjectName(), event.JobName)
	if err != nil {
		s.l.Error("error getting job by job name [%s]: %s", event.JobName, err)
		return err
	}
	eventCtx := event.EventContext

	alertConfig := job.GetOperatorAlertConfigByName(eventCtx.OperatorType, eventCtx.OperatorRunInstance.OperatorName)
	if alertConfig == nil {
		return nil
	}
	return s.registerSLAs(ctx, eventCtx, alertConfig.SLAAlertConfigs)
}

func (s *JobRunService) registerSLAs(ctx context.Context, eventCtx *scheduler.EventContext, slaAlertConfigs []*scheduler.SLAAlertConfig) error {
	jobRunID := eventCtx.DagRun.RunID
	operatorStartTime := eventCtx.OperatorRunInstance.StartTime
	operatorType := eventCtx.OperatorType
	operatorName := eventCtx.OperatorRunInstance.OperatorName
	jobName := eventCtx.DagRun.JobName

	me := errors.NewMultiError("errorInRegisterSLA")
	for _, slaAlertConfig := range slaAlertConfigs {
		slaBoundary := operatorStartTime.Add(slaAlertConfig.DurationThreshold)
		err := s.slaRepo.RegisterSLA(ctx, eventCtx.Tenant.ProjectName(), jobName, operatorName, operatorType.String(), jobRunID, slaBoundary, slaAlertConfig.Tag())
		if err != nil {
			errMsg := fmt.Sprintf("error registering sla for operator Run Id: %s, Type: %s, Sla: %s, err: %s", jobRunID, operatorType.String(), slaAlertConfig.Tag(), err.Error())
			me.Append(errors.Wrap(scheduler.EntityEvent, errMsg, err))
			s.l.Error(errMsg)
		}
	}
	return me.ToErr()
}

func (s *JobRunService) getExistingOperatorRun(ctx context.Context, event *scheduler.Event) (*scheduler.OperatorRun, error) {
	jobRun, err := s.getJobRunByScheduledAt(ctx, event.Tenant, event.JobName, event.JobScheduledAt)
	if err != nil {
		s.l.Error("error getting job run by scheduled time [%s]: %s", event.JobScheduledAt, err)
		return nil, err
	}
	existingOperatorRun, err := s.operatorRunRepo.GetOperatorRun(ctx, event.OperatorName, event.EventContext.OperatorType, jobRun.ID)
	if err != nil {
		if errors.IsErrorType(err, errors.ErrNotFound) {
			return nil, nil //nolint:nilnil
		}
		s.l.Error("error checking existing operator run state [%s], operatorName [%s] : %s", jobRun.ID, event.OperatorName, err)
		return nil, err
	}
	return existingOperatorRun, nil
}

func (s *JobRunService) createOperatorRun(ctx context.Context, event *scheduler.Event, operatorType scheduler.OperatorType) error {
	jobRun, err := s.getJobRunByScheduledAt(ctx, event.Tenant, event.JobName, event.JobScheduledAt)
	if err != nil {
		s.l.Error("error getting job run by scheduled time [%s]: %s", event.JobScheduledAt, err)
		return err
	}
	jobState, err := operatorStartToJobState(operatorType)
	if err != nil {
		s.l.Error("error converting operator to job state: %s", err)
		return err
	}
	if jobRun.State != jobState {
		err := s.repo.UpdateState(ctx, jobRun.ID, jobState)
		if err != nil {
			s.l.Error("error updating state for job run id [%d] to [%s]: %s", jobRun.ID, jobState, err)
			return err
		}
		jobRun.State = jobState
		s.raiseJobRunStateChangeEvent(jobRun)
	}

	err = s.operatorRunRepo.CreateOperatorRun(ctx, event.OperatorName, operatorType, jobRun.ID, event.EventTime)
	if err != nil {
		s.l.Error("error registering operator run job [%s], type [%s], operator [%s] : %s", event.JobName, operatorType, event.OperatorName, err)
		return err
	}

	return err
}

func (s *JobRunService) getOperatorRun(ctx context.Context, event *scheduler.Event, operatorType scheduler.OperatorType, jobRunID uuid.UUID) (*scheduler.OperatorRun, error) {
	var operatorRun *scheduler.OperatorRun
	operatorRun, err := s.operatorRunRepo.GetOperatorRun(ctx, event.OperatorName, operatorType, jobRunID)
	if err != nil {
		if !errors.IsErrorType(err, errors.ErrNotFound) {
			s.l.Error("error getting operator for job run [%s]: %s", jobRunID, err)
			return nil, err
		}
		s.l.Warn("operator is not found, creating it")

		// TODO: consider moving below call outside as the caller is a 'getter'
		err = s.createOperatorRun(ctx, event, operatorType)
		if err != nil {
			s.l.Error("error creating operator run: %s", err)
			return nil, err
		}
		operatorRun, err = s.operatorRunRepo.GetOperatorRun(ctx, event.OperatorName, operatorType, jobRunID)
		if err != nil {
			s.l.Error("error getting the registered operator run: %s", err)
			return nil, err
		}
	}
	return operatorRun, nil
}

func (s *JobRunService) CleanupSLA(ctx context.Context, event *scheduler.Event) error {
	if event.EventContext.Type == scheduler.OperatorSuccessEvent || event.EventContext.Type == scheduler.OperatorFailEvent {
		operatorEndTime := *event.EventContext.OperatorRunInstance.EndTime
		err := s.slaRepo.FinishSLA(ctx, event.Tenant.ProjectName(), event.JobName.String(), event.OperatorName, event.EventContext.OperatorType.String(), event.EventContext.DagRun.RunID, operatorEndTime)
		if err != nil {
			s.l.Error("error finishing sla update for operator run [%s:%s:%s]: %s",
				event.JobName.String(), event.OperatorName, event.JobScheduledAt.Format(time.RFC3339), err)
			return err
		}
	}
	return nil
}

func (s *JobRunService) updateOperatorRun(ctx context.Context, event *scheduler.Event, operatorType scheduler.OperatorType) error {
	jobRun, err := s.getJobRunByScheduledAt(ctx, event.Tenant, event.JobName, event.JobScheduledAt)
	if err != nil {
		s.l.Error("error getting job run by scheduled time [%s]: %s", event.JobScheduledAt, err)
		return err
	}
	operatorRun, err := s.getOperatorRun(ctx, event, operatorType, jobRun.ID)
	if err != nil {
		s.l.Error("error getting operator for job run id [%s]: %s", jobRun.ID, err)
		return err
	}
	err = s.operatorRunRepo.UpdateOperatorRun(ctx, operatorType, operatorRun.ID, event.EventTime, event.Status)
	if err != nil {
		s.l.Error("error updating operator run id [%s]: %s", operatorRun.ID, err)
		return err
	}

	jobRunDdurationsBreakdownSeconds.WithLabelValues(
		event.Tenant.ProjectName().String(),
		event.Tenant.NamespaceName().String(),
		event.JobName.String(),
		operatorType.String(),
	).Set(float64(event.EventTime.Unix() - operatorRun.StartTime.Unix()))

	return nil
}

func (s *JobRunService) trackEvent(event *scheduler.Event) {
	if event.Type.IsOfType(scheduler.EventCategorySLAMiss) {
		jsonSLAObjectList, err := json.Marshal(event.SLAObjectList)
		if err != nil {
			jsonSLAObjectList = []byte("unable to json Marshal SLAObjectList")
		}
		s.l.Info("received job sla_miss event, jobName: %v , slaPayload: %s", event.JobName, string(jsonSLAObjectList))
	} else {
		s.l.Info("received event: %v, eventTime: %s, jobName: %v, Operator: %v, schedule: %s, status: %s",
			event.Type, event.EventTime.Format("01/02/06 15:04:05 MST"), event.JobName, event.OperatorName, event.JobScheduledAt.Format("01/02/06 15:04:05 MST"), event.Status)
	}

	if event.Type != scheduler.SLAMissEvent && event.Type != scheduler.JobSuccessEvent && event.Type != scheduler.JobFailureEvent {
		jobRunEventsMetric.WithLabelValues(
			event.OperatorName,
			event.EventContext.OperatorType.String(),
			event.Type.String(),
		).Inc()
	}
}

func (s *JobRunService) UpdateJobState(ctx context.Context, event *scheduler.Event) error {
	s.trackEvent(event)

	switch event.Type {
	case scheduler.SLAMissEvent:
		return s.updateJobRunSLA(ctx, event)
	case scheduler.JobSuccessEvent, scheduler.JobFailureEvent:
		return s.updateJobRun(ctx, event)
	case scheduler.SensorStartEvent:
		return s.createOperatorRun(ctx, event, scheduler.OperatorSensor)
	case scheduler.SensorSuccessEvent, scheduler.SensorRetryEvent, scheduler.SensorFailEvent:
		return s.updateOperatorRun(ctx, event, scheduler.OperatorSensor)
	case scheduler.TaskStartEvent, scheduler.HookStartEvent:
		existingOperatorRun, err := s.getExistingOperatorRun(ctx, event)
		if err != nil {
			return err
		}
		err = s.createOperatorRun(ctx, event, event.EventContext.OperatorType)
		if err != nil {
			return err
		}
		if existingOperatorRun != nil && existingOperatorRun.Status == scheduler.StateRetry {
			return nil
		}
		return s.registerConfiguredSLA(ctx, event)
	case scheduler.TaskSuccessEvent, scheduler.TaskFailEvent, scheduler.HookSuccessEvent, scheduler.HookFailEvent:
		err := s.updateOperatorRun(ctx, event, event.EventContext.OperatorType)
		if err != nil {
			return err
		}
		return s.CleanupSLA(ctx, event)
	case scheduler.TaskRetryEvent, scheduler.HookRetryEvent:
		return s.updateOperatorRun(ctx, event, event.EventContext.OperatorType)
	default:
		return errors.InvalidArgument(scheduler.EntityEvent, "invalid event type: "+string(event.Type))
	}
}

func NewJobRunService(logger log.Logger, jobRepo JobRepository, jobRunRepo JobRunRepository, replayRepo JobReplayRepository,
	operatorRunRepo OperatorRunRepository, slaRepo SLARepository, scheduler Scheduler, resolver PriorityResolver,
	compiler JobInputCompiler, eventHandler EventHandler, projectGetter ProjectGetter, features config.FeaturesConfig,
) *JobRunService {
	return &JobRunService{
		l:                logger,
		repo:             jobRunRepo,
		operatorRunRepo:  operatorRunRepo,
		slaRepo:          slaRepo,
		scheduler:        scheduler,
		eventHandler:     eventHandler,
		replayRepo:       replayRepo,
		jobRepo:          jobRepo,
		priorityResolver: resolver,
		compiler:         compiler,
		projectGetter:    projectGetter,
		features:         features,
	}
}

func max1(x int, y ...int) int {
	m1 := x
	for _, t := range y {
		if t > m1 {
			m1 = t
		}
	}
	return m1
}
