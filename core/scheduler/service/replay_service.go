package service

import (
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/goto/salt/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"golang.org/x/net/context"

	"github.com/goto/optimus/core/scheduler"
	"github.com/goto/optimus/core/tenant"
	"github.com/goto/optimus/internal/errors"
	"github.com/goto/optimus/internal/lib/cron"
	"github.com/goto/optimus/internal/utils/filter"
)

const (
	getReplaysDayLimit = 30 // TODO: make it configurable via cli

	tenantReplayExecutionProjectConfigKey = "REPLAY_EXECUTION_PROJECT"
)

var jobReplayMetric = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "jobrun_replay_requests_total",
	Help: "replay request count with status",
}, []string{"project", "namespace", "name", "status"})

type SchedulerRunGetter interface {
	GetJobRuns(ctx context.Context, t tenant.Tenant, criteria *scheduler.JobRunsCriteria, jobCron *cron.ScheduleSpec) ([]*scheduler.JobRunStatus, error)
}

type ReplayRepository interface {
	RegisterReplay(ctx context.Context, replay *scheduler.Replay, runs []*scheduler.JobRunStatus) (uuid.UUID, error)
	UpdateReplay(ctx context.Context, replayID uuid.UUID, state scheduler.ReplayState, runs []*scheduler.JobRunStatus, message string) error
	UpdateReplayRuns(ctx context.Context, replayID uuid.UUID, runs []*scheduler.JobRunStatus) error
	UpdateReplayStatus(ctx context.Context, replayID uuid.UUID, state scheduler.ReplayState, message string) error
	ScanAbandonedReplayRequests(ctx context.Context, unhandledClassifierDuration time.Duration) ([]*scheduler.Replay, error)
	AcquireReplayRequest(ctx context.Context, replayID uuid.UUID, unhandledClassifierDuration time.Duration) error

	GetReplayByFilters(ctx context.Context, projectName tenant.ProjectName, filters ...filter.FilterOpt) ([]*scheduler.ReplayWithRun, error)
	GetReplayRequestsByStatus(ctx context.Context, statusList []scheduler.ReplayState) ([]*scheduler.Replay, error)
	GetReplaysByProject(ctx context.Context, projectName tenant.ProjectName, dayLimits int) ([]*scheduler.Replay, error)
	GetReplayByID(ctx context.Context, replayID uuid.UUID) (*scheduler.ReplayWithRun, error)
}

type TenantGetter interface {
	GetDetails(ctx context.Context, tnnt tenant.Tenant) (*tenant.WithDetails, error)
}

type ReplayValidator interface {
	Validate(ctx context.Context, replayRequest *scheduler.Replay, jobCron *cron.ScheduleSpec) error
}

type ReplayExecutor interface {
	Execute(replayID uuid.UUID, jobTenant tenant.Tenant, jobName scheduler.JobName)
	SyncStatus(ctx context.Context, replayWithRun *scheduler.ReplayWithRun, jobCron *cron.ScheduleSpec) (scheduler.JobRunStatusList, error)
	CancelReplayRunsOnScheduler(ctx context.Context, replay *scheduler.Replay, jobCron *cron.ScheduleSpec, runs []*scheduler.JobRunStatus) []*scheduler.JobRunStatus
}

type ReplayService struct {
	replayRepo ReplayRepository
	jobRepo    JobRepository
	runGetter  SchedulerRunGetter

	validator ReplayValidator
	executor  ReplayExecutor

	tenantGetter TenantGetter

	alertManager AlertManager

	logger log.Logger

	// stores mapping of task names (optimus plugin names) to its respective execution project config names.
	// this mapping is needed because our bq plugins supporting execution project uses different config names inside the plugins.
	// after the config naming is standardized, this map can be omitted
	pluginToExecutionProjectKeyMap map[string]string
}

func (r *ReplayService) CreateReplay(ctx context.Context, t tenant.Tenant, jobName scheduler.JobName, config *scheduler.ReplayConfig) (replayID uuid.UUID, err error) {
	jobCron, err := getJobCron(ctx, r.logger, r.jobRepo, t, jobName)
	if err != nil {
		r.logger.Error("unable to get cron value for job [%s]: %s", jobName.String(), err.Error())
		return uuid.Nil, err
	}

	newConfig, err := r.injectJobConfigWithTenantConfigs(ctx, t, jobName, config)
	if err != nil {
		r.logger.Error("unable to get namespace details for job %s: %s", jobName.String(), err)
		return uuid.Nil, err
	}
	config.JobConfig = newConfig

	replayReq := scheduler.NewReplayRequest(jobName, t, config, scheduler.ReplayStateCreated)
	if err := r.validator.Validate(ctx, replayReq, jobCron); err != nil {
		r.logger.Error("error validating replay request: %s", err)
		return uuid.Nil, err
	}

	runs := getExpectedRuns(jobCron, config.StartTime, config.EndTime)
	replayID, err = r.replayRepo.RegisterReplay(ctx, replayReq, runs)
	if err != nil {
		return uuid.Nil, err
	}

	jobReplayMetric.WithLabelValues(t.ProjectName().String(),
		t.NamespaceName().String(),
		jobName.String(),
		replayReq.State().String(),
	).Inc()

	r.alertManager.SendReplayEvent(&scheduler.ReplayNotificationAttrs{
		JobName:  jobName.String(),
		ReplayID: replayID.String(),
		Tenant:   t,
		JobURN:   jobName.GetJobURN(t),
		State:    scheduler.ReplayStateCreated,
	})

	go r.executor.Execute(replayID, replayReq.Tenant(), jobName)

	return replayID, nil
}

func (r *ReplayService) injectJobConfigWithTenantConfigs(ctx context.Context, tnnt tenant.Tenant, jobName scheduler.JobName, config *scheduler.ReplayConfig) (map[string]string, error) {
	// copy ReplayConfig to a new map to mutate it
	newConfig := map[string]string{}
	for cfgKey, cfgVal := range config.JobConfig {
		newConfig[cfgKey] = cfgVal
	}

	// get tenant (project & namespace) configuration to obtain the execution project specifically for replay.
	// note that the current behavior of GetDetails in the implementing struct prioritized namespace config over project config.
	tenantWithDetails, err := r.tenantGetter.GetDetails(ctx, tnnt)
	if err != nil {
		return nil, errors.AddErrContext(err, scheduler.EntityReplay,
			fmt.Sprintf("failed to get tenant details for project [%s], namespace [%s]",
				tnnt.ProjectName(), tnnt.NamespaceName()))
	}

	job, err := r.jobRepo.GetJob(ctx, tnnt.ProjectName(), jobName)
	if err != nil {
		return nil, errors.AddErrContext(err, scheduler.EntityReplay,
			fmt.Sprintf("failed to get job for job name [%s]", jobName))
	}

	tenantConfig := tenantWithDetails.GetConfigs()

	// override the default execution project with the one in tenant config.
	// only inject tenant-level config if execution project is not provided in ReplayConfig
	overridedConfigKey, isSupported := r.pluginToExecutionProjectKeyMap[job.Task.Name]
	tenantExecutionProject := tenantConfig[tenantReplayExecutionProjectConfigKey]
	if isSupported && tenantExecutionProject != "" {
		newConfig[overridedConfigKey] = tenantExecutionProject
	}

	return newConfig, nil
}

func (r *ReplayService) GetReplayList(ctx context.Context, projectName tenant.ProjectName) (replays []*scheduler.Replay, err error) {
	return r.replayRepo.GetReplaysByProject(ctx, projectName, getReplaysDayLimit)
}

func (r *ReplayService) GetByFilter(ctx context.Context, project tenant.ProjectName, filters ...filter.FilterOpt) ([]*scheduler.ReplayWithRun, error) {
	f := filter.NewFilter(filters...)
	if f.Contains(filter.ReplayID) {
		r.logger.Debug("getting all replays by replayId [%s]", f.GetStringValue(filter.ReplayID))
		replayIDString := f.GetStringValue(filter.ReplayID)
		id, err := uuid.Parse(replayIDString)
		if err != nil {
			r.logger.Error("error parsing replay id [%s]: %s", replayIDString, err)
			err = errors.InvalidArgument(scheduler.EntityReplay, err.Error())
			return nil, errors.GRPCErr(err, "unable to get replay for replayID "+replayIDString)
		}
		replay, err := r.GetReplayByID(ctx, id)
		if err != nil {
			return nil, err
		}
		return []*scheduler.ReplayWithRun{replay}, nil
	}

	replayWithRuns, err := r.replayRepo.GetReplayByFilters(ctx, project, filters...)
	if err != nil {
		return nil, err
	}
	return replayWithRuns, nil
}

func (r *ReplayService) GetReplayByID(ctx context.Context, replayID uuid.UUID) (*scheduler.ReplayWithRun, error) {
	replayWithRun, err := r.replayRepo.GetReplayByID(ctx, replayID)
	if err != nil {
		return nil, err
	}
	replayWithRun.Runs = scheduler.JobRunStatusList(replayWithRun.Runs).GetSortedRunsByScheduledAt()
	return replayWithRun, nil
}

func (r *ReplayService) GetRunsStatus(ctx context.Context, tenant tenant.Tenant, jobName scheduler.JobName, config *scheduler.ReplayConfig) ([]*scheduler.JobRunStatus, error) {
	jobRunCriteria := &scheduler.JobRunsCriteria{
		Name:      jobName.String(),
		StartDate: config.StartTime,
		EndDate:   config.EndTime,
	}
	jobCron, err := getJobCron(ctx, r.logger, r.jobRepo, tenant, jobName)
	if err != nil {
		r.logger.Error("unable to get cron value for job [%s]: %s", jobName.String(), err.Error())
		return nil, err
	}
	existingRuns, err := r.runGetter.GetJobRuns(ctx, tenant, jobRunCriteria, jobCron)
	if err != nil {
		return nil, err
	}
	expectedRuns := getExpectedRuns(jobCron, config.StartTime, config.EndTime)
	tobeCreatedRuns := getMissingRuns(expectedRuns, existingRuns)
	tobeCreatedRuns = scheduler.JobRunStatusList(tobeCreatedRuns).OverrideWithStatus(scheduler.StateMissing)
	runs := tobeCreatedRuns
	runs = append(runs, existingRuns...)
	runs = scheduler.JobRunStatusList(runs).GetSortedRunsByScheduledAt()
	return runs, nil
}

func (r *ReplayService) cancelReplayRuns(ctx context.Context, replayWithRun *scheduler.ReplayWithRun) error {
	// get list of in progress runs
	// stop them on the scheduler
	replay := replayWithRun.Replay
	jobName := replay.JobName()
	jobCron, err := getJobCron(ctx, r.logger, r.jobRepo, replay.Tenant(), jobName)
	if err != nil {
		r.logger.Error("unable to get cron value for job [%s]: %s", jobName.String(), err.Error())
		return err
	}

	syncedRunStatus, err := r.executor.SyncStatus(ctx, replayWithRun, jobCron)
	if err != nil {
		r.logger.Error("unable to sync replay runs status for job [%s]: %s", jobName.String(), err.Error())
		return err
	}

	statesForCanceling := []scheduler.State{scheduler.StateRunning, scheduler.StateInProgress, scheduler.StateQueued}
	toBeCanceledRuns := syncedRunStatus.GetSortedRunsByStates(statesForCanceling)
	if len(toBeCanceledRuns) == 0 {
		return nil
	}

	canceledRuns := r.executor.CancelReplayRunsOnScheduler(ctx, replay, jobCron, toBeCanceledRuns)

	// update the status of these runs as failed in DB
	return r.replayRepo.UpdateReplayRuns(ctx, replay.ID(), canceledRuns)
}

func (r *ReplayService) CancelReplay(ctx context.Context, replayWithRun *scheduler.ReplayWithRun) error {
	if replayWithRun.Replay.IsTerminated() {
		return errors.InvalidArgument(scheduler.EntityReplay, fmt.Sprintf("replay has already been terminated with status %s", replayWithRun.Replay.State().String()))
	}
	statusSummary := scheduler.JobRunStatusList(replayWithRun.Runs).GetJobRunStatusSummary()
	cancelMessage := fmt.Sprintf("replay cancelled with run status %s", statusSummary)

	err := r.replayRepo.UpdateReplayStatus(ctx, replayWithRun.Replay.ID(), scheduler.ReplayStateCancelled, cancelMessage)
	if err != nil {
		return err
	}
	return r.cancelReplayRuns(ctx, replayWithRun)
}

func NewReplayService(
	replayRepo ReplayRepository,
	jobRepo JobRepository,
	tenantGetter TenantGetter,
	validator ReplayValidator,
	worker ReplayExecutor,
	runGetter SchedulerRunGetter,
	logger log.Logger,
	pluginToExecutionProjectKeyMap map[string]string,
	alertManager AlertManager,
) *ReplayService {
	return &ReplayService{
		replayRepo:                     replayRepo,
		jobRepo:                        jobRepo,
		tenantGetter:                   tenantGetter,
		validator:                      validator,
		executor:                       worker,
		runGetter:                      runGetter,
		logger:                         logger,
		pluginToExecutionProjectKeyMap: pluginToExecutionProjectKeyMap,
		alertManager:                   alertManager,
	}
}

func getJobCron(ctx context.Context, l log.Logger, jobRepo JobRepository, tnnt tenant.Tenant, jobName scheduler.JobName) (*cron.ScheduleSpec, error) {
	jobWithDetails, err := jobRepo.GetJobDetails(ctx, tnnt.ProjectName(), jobName)
	if err != nil || jobWithDetails == nil {
		return nil, errors.AddErrContext(err, scheduler.EntityReplay,
			fmt.Sprintf("unable to get job details for jobName: %s, project: %s", jobName, tnnt.ProjectName()))
	}

	if jobWithDetails.Job.Tenant.NamespaceName() != tnnt.NamespaceName() {
		l.Error("job [%s] resides in namespace [%s], expecting it under [%s]", jobName, jobWithDetails.Job.Tenant.NamespaceName(), tnnt.NamespaceName())
		return nil, errors.InvalidArgument(scheduler.EntityReplay, fmt.Sprintf("job %s does not exist in %s namespace", jobName, tnnt.NamespaceName().String()))
	}

	interval := jobWithDetails.Schedule.Interval
	if interval == "" {
		l.Error("job interval is empty")
		return nil, errors.InvalidArgument(scheduler.EntityReplay, "job schedule interval is empty")
	}
	jobCron, err := cron.ParseCronSchedule(interval)
	if err != nil {
		l.Error("error parsing cron interval: %s", err)
		return nil, errors.InternalError(scheduler.EntityReplay, "unable to parse job cron interval", err)
	}
	return jobCron, nil
}

func getMissingRuns(expectedRuns, existingRuns []*scheduler.JobRunStatus) []*scheduler.JobRunStatus {
	var runsToBeCreated []*scheduler.JobRunStatus
	existedRunsMap := scheduler.JobRunStatusList(existingRuns).ToRunStatusMap()
	for _, run := range expectedRuns {
		if _, ok := existedRunsMap[run.ScheduledAt]; !ok {
			runsToBeCreated = append(runsToBeCreated, run)
		}
	}
	return runsToBeCreated
}
