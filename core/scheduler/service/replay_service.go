package service

import (
	"fmt"

	"github.com/google/uuid"
	"github.com/goto/salt/log"
	"golang.org/x/net/context"

	"github.com/goto/optimus/core/scheduler"
	"github.com/goto/optimus/core/tenant"
	"github.com/goto/optimus/internal/errors"
	"github.com/goto/optimus/internal/lib/cron"
	"github.com/goto/optimus/internal/telemetry"
)

const (
	getReplaysDayLimit = 30 // TODO: make it configurable via cli

	metricJobReplay = "jobrun_replay_requests_total"

	defaultExecutionProjectConfigKey = "EXECUTION_PROJECT"
)

var namespaceConfigForReplayMap = map[string]string{
	"REPLAY_EXECUTION_PROJECT": defaultExecutionProjectConfigKey,
}

type SchedulerRunGetter interface {
	GetJobRuns(ctx context.Context, t tenant.Tenant, criteria *scheduler.JobRunsCriteria, jobCron *cron.ScheduleSpec) ([]*scheduler.JobRunStatus, error)
}

type ReplayRepository interface {
	RegisterReplay(ctx context.Context, replay *scheduler.Replay, runs []*scheduler.JobRunStatus) (uuid.UUID, error)
	UpdateReplay(ctx context.Context, replayID uuid.UUID, state scheduler.ReplayState, runs []*scheduler.JobRunStatus, message string) error
	UpdateReplayStatus(ctx context.Context, replayID uuid.UUID, state scheduler.ReplayState, message string) error

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
}

type ReplayService struct {
	replayRepo ReplayRepository
	jobRepo    JobRepository
	runGetter  SchedulerRunGetter

	validator ReplayValidator
	executor  ReplayExecutor

	tenantGetter TenantGetter

	logger log.Logger
}

func (r *ReplayService) CreateReplay(ctx context.Context, tenant tenant.Tenant, jobName scheduler.JobName, config *scheduler.ReplayConfig) (replayID uuid.UUID, err error) {
	jobCron, err := getJobCron(ctx, r.logger, r.jobRepo, tenant, jobName)
	if err != nil {
		r.logger.Error("unable to get cron value for job [%s]: %s", jobName.String(), err.Error())
		return uuid.Nil, err
	}

	newConfig, err := r.injectJobConfigWithTenantConfigs(ctx, tenant, config)
	if err != nil {
		r.logger.Error("unable to get namespace details for job %s: %s", jobName.String(), err)
		return uuid.Nil, err
	}
	config.JobConfig = newConfig

	replayReq := scheduler.NewReplayRequest(jobName, tenant, config, scheduler.ReplayStateCreated)
	if err := r.validator.Validate(ctx, replayReq, jobCron); err != nil {
		r.logger.Error("error validating replay request: %s", err)
		return uuid.Nil, err
	}

	runs := getExpectedRuns(jobCron, config.StartTime, config.EndTime)
	replayID, err = r.replayRepo.RegisterReplay(ctx, replayReq, runs)
	if err != nil {
		return uuid.Nil, err
	}

	telemetry.NewCounter(metricJobReplay, map[string]string{
		"project":   tenant.ProjectName().String(),
		"namespace": tenant.NamespaceName().String(),
		"job":       jobName.String(),
		"status":    replayReq.State().String(),
	}).Inc()

	go r.executor.Execute(replayID, replayReq.Tenant(), jobName)

	return replayID, nil
}

func (r *ReplayService) injectJobConfigWithTenantConfigs(ctx context.Context, tnnt tenant.Tenant, config *scheduler.ReplayConfig) (map[string]string, error) {
	// copy JobConfig to a new map to mutate it
	newConfig := map[string]string{}
	for cfgKey, cfgVal := range config.JobConfig {
		newConfig[cfgKey] = cfgVal
	}

	// get tenant (project & namespace) configuration to obtain the execution project specifically for replay.
	// note that the current behavior of GetDetails in the implementing struct prioritized namespace config over project config.
	// so replay execution project in project config will be replaced by the one in namespace level config, if present
	tenantWithDetails, err := r.tenantGetter.GetDetails(ctx, tnnt)
	if err != nil {
		return nil, err
	}

	tenantConfig := tenantWithDetails.GetConfigs()
	for cfgKey, cfgVal := range tenantConfig {
		// only inject tenant-level configs for replay if they are not present in the ReplayConfig
		if overridedConfigKey, found := namespaceConfigForReplayMap[cfgKey]; found {
			if _, configExists := config.JobConfig[overridedConfigKey]; !configExists {
				newConfig[overridedConfigKey] = cfgVal
			}
		}
	}

	return newConfig, nil
}

func (r *ReplayService) GetReplayList(ctx context.Context, projectName tenant.ProjectName) (replays []*scheduler.Replay, err error) {
	return r.replayRepo.GetReplaysByProject(ctx, projectName, getReplaysDayLimit)
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

func (r *ReplayService) CancelReplay(ctx context.Context, replayWithRun *scheduler.ReplayWithRun) error {
	if replayWithRun.Replay.IsTerminated() {
		return errors.InvalidArgument(scheduler.EntityReplay, fmt.Sprintf("replay has already been terminated with status %s", replayWithRun.Replay.State().String()))
	}
	statusSummary := scheduler.JobRunStatusList(replayWithRun.Runs).GetJobRunStatusSummary()
	cancelMessage := fmt.Sprintf("replay cancelled with run status %s", statusSummary)
	return r.replayRepo.UpdateReplayStatus(ctx, replayWithRun.Replay.ID(), scheduler.ReplayStateCancelled, cancelMessage)
}

func NewReplayService(replayRepo ReplayRepository, jobRepo JobRepository, tenantGetter TenantGetter, validator ReplayValidator, worker ReplayExecutor, runGetter SchedulerRunGetter, logger log.Logger) *ReplayService {
	return &ReplayService{
		replayRepo:   replayRepo,
		jobRepo:      jobRepo,
		tenantGetter: tenantGetter,
		validator:    validator,
		executor:     worker,
		runGetter:    runGetter,
		logger:       logger,
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
