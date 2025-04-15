package service

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/goto/salt/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

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
)

var jobRunEventsMetric = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "operator_stats",
	Help: "total job run events received",
}, []string{"operator_name", "event_type"})

var jobRunDdurationsBreakdownSeconds = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Name: "jobrun_durations_breakdown_seconds",
	Help: "operator wise time spent",
}, []string{"project", "namespace", "job", "type"})

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
	GetByScheduledTimes(ctx context.Context, tenant tenant.Tenant, jobName scheduler.JobName, scheduledTimes []time.Time) ([]*scheduler.JobRun, error)
	Create(ctx context.Context, tenant tenant.Tenant, name scheduler.JobName, scheduledAt time.Time, slaDefinitionInSec int64) error
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
	UpdateJobState(ctx context.Context, tnnt tenant.Tenant, jobName []job.Name, state string) error
	GetJobState(ctx context.Context, tnnt tenant.Tenant) (map[string]bool, error)
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
	operatorRunRepo  OperatorRunRepository
	eventHandler     EventHandler
	scheduler        Scheduler
	jobRepo          JobRepository
	priorityResolver PriorityResolver
	compiler         JobInputCompiler
	projectGetter    ProjectGetter
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

func (s *JobRunService) GetJobRuns(ctx context.Context, projectName tenant.ProjectName, jobName scheduler.JobName, criteria *scheduler.JobRunsCriteria) ([]*scheduler.JobRunStatus, error) {
	jobWithDetails, err := s.jobRepo.GetJobDetails(ctx, projectName, jobName)
	if err != nil {
		msg := fmt.Sprintf("unable to get job details for jobName: %s, project:%s", jobName, projectName)
		s.l.Error(msg)
		return nil, errors.AddErrContext(err, scheduler.EntityJobRun, msg)
	}
	interval := jobWithDetails.Schedule.Interval
	if interval == "" {
		s.l.Error("job schedule interval is empty")
		return nil, errors.InvalidArgument(scheduler.EntityJobRun, "cannot get job runs, job interval is empty")
	}
	jobCron, err := cron.ParseCronSchedule(interval)
	if err != nil {
		msg := fmt.Sprintf("unable to parse job cron interval: %s", err)
		s.l.Error(msg)
		return nil, errors.InternalError(scheduler.EntityJobRun, msg, nil)
	}

	if criteria.OnlyLastRun {
		s.l.Warn("getting last run only")
		return s.scheduler.GetJobRuns(ctx, jobWithDetails.Job.Tenant, criteria, jobCron)
	}
	err = validateJobQuery(criteria, jobWithDetails)
	if err != nil {
		s.l.Error("invalid job query: %s", err)
		return nil, err
	}
	expectedRuns := getExpectedRuns(jobCron, criteria.StartDate, criteria.EndDate)

	actualRuns, err := s.scheduler.GetJobRuns(ctx, jobWithDetails.Job.Tenant, criteria, jobCron)
	if err != nil {
		s.l.Error("unable to get job runs from airflow err: %s", err)
		actualRuns = []*scheduler.JobRunStatus{}
	}
	totalRuns := mergeRuns(expectedRuns, actualRuns)

	result := filterRuns(totalRuns, createFilterSet(criteria.Filter))

	return result, nil
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

func filterRuns(runs []*scheduler.JobRunStatus, filter map[string]struct{}) []*scheduler.JobRunStatus {
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

func validateJobQuery(jobQuery *scheduler.JobRunsCriteria, jobWithDetails *scheduler.JobWithDetails) error {
	jobStartDate := jobWithDetails.Schedule.StartDate
	if jobStartDate.IsZero() {
		return errors.InternalError(scheduler.EntityJobRun, "job schedule startDate not found in job", nil)
	}
	if jobQuery.StartDate.Before(jobStartDate) || jobQuery.EndDate.Before(jobStartDate) {
		return errors.InvalidArgument(scheduler.EntityJobRun, "invalid date range, interval contains dates before job start")
	}
	return nil
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
	err = s.repo.Create(ctx, tenant, jobName, scheduledAt, slaDefinitionInSec)
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
		s.l.Error("error getting job runs by schedule time, skipping the filter", err)
		return unfilteredSLAObj, slaBreachedJobRunScheduleTimes
	}
	if len(jobRuns) == 0 {
		s.l.Error("no job runs found for given schedule time, skipping the filter (perhaps the sla is due to schedule delay, in such cases the job wont be persisted in optimus DB)", err)
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

	return s.operatorRunRepo.CreateOperatorRun(ctx, event.OperatorName, operatorType, jobRun.ID, event.EventTime)
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

	switch event.Type {
	case scheduler.SensorSuccessEvent, scheduler.SensorRetryEvent, scheduler.SensorFailEvent:
		jobRunEventsMetric.WithLabelValues(
			scheduler.OperatorSensor.String(),
			event.Type.String(),
		).Inc()
	case scheduler.TaskSuccessEvent, scheduler.TaskRetryEvent, scheduler.TaskFailEvent, scheduler.HookSuccessEvent, scheduler.HookRetryEvent, scheduler.HookFailEvent:
		jobRunEventsMetric.WithLabelValues(
			event.OperatorName,
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
	case scheduler.TaskStartEvent:
		return s.createOperatorRun(ctx, event, scheduler.OperatorTask)
	case scheduler.TaskSuccessEvent, scheduler.TaskRetryEvent, scheduler.TaskFailEvent:
		return s.updateOperatorRun(ctx, event, scheduler.OperatorTask)
	case scheduler.SensorStartEvent:
		return s.createOperatorRun(ctx, event, scheduler.OperatorSensor)
	case scheduler.SensorSuccessEvent, scheduler.SensorRetryEvent, scheduler.SensorFailEvent:
		return s.updateOperatorRun(ctx, event, scheduler.OperatorSensor)
	case scheduler.HookStartEvent:
		return s.createOperatorRun(ctx, event, scheduler.OperatorHook)
	case scheduler.HookSuccessEvent, scheduler.HookRetryEvent, scheduler.HookFailEvent:
		return s.updateOperatorRun(ctx, event, scheduler.OperatorHook)
	default:
		return errors.InvalidArgument(scheduler.EntityEvent, "invalid event type: "+string(event.Type))
	}
}

func NewJobRunService(logger log.Logger, jobRepo JobRepository, jobRunRepo JobRunRepository, replayRepo JobReplayRepository,
	operatorRunRepo OperatorRunRepository, scheduler Scheduler, resolver PriorityResolver, compiler JobInputCompiler, eventHandler EventHandler,
	projectGetter ProjectGetter,
) *JobRunService {
	return &JobRunService{
		l:                logger,
		repo:             jobRunRepo,
		operatorRunRepo:  operatorRunRepo,
		scheduler:        scheduler,
		eventHandler:     eventHandler,
		replayRepo:       replayRepo,
		jobRepo:          jobRepo,
		priorityResolver: resolver,
		compiler:         compiler,
		projectGetter:    projectGetter,
	}
}
