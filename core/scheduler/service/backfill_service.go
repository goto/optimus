package service

import (
	"fmt"
	"maps"
	"time"

	"github.com/google/uuid"
	"github.com/goto/salt/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"golang.org/x/net/context"

	"github.com/goto/optimus/core/scheduler"
	"github.com/goto/optimus/core/tenant"
	"github.com/goto/optimus/internal/errors"
	"github.com/goto/optimus/internal/utils/filter"
)

const (
	CustomBackfillPrefix = "custom-backfill"
)

var jobBackfillMetric = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "jobrun_backfill_requests_total",
	Help: "backfill request count with status",
}, []string{"project", "namespace", "name", "status"})

type BackfillRepository interface {
	RegisterBackfill(ctx context.Context, backfill *scheduler.Backfill) (uuid.UUID, error)
	UpdateBackfillSchedulerRunID(ctx context.Context, backfillID uuid.UUID, schedulerRunID string) error
	GetBackfillDetails(ctx context.Context, backfillID uuid.UUID) (*scheduler.Backfill, error)
	GetBackfillsByFilter(ctx context.Context, projectName tenant.ProjectName, filters ...filter.FilterOpt) ([]*scheduler.Backfill, error)
	CancelBackfill(ctx context.Context, backfillID uuid.UUID, canceledBy string) error
}

type Executor interface {
	CreateRun(ctx context.Context, tnnt tenant.Tenant, jobName scheduler.JobName, executionTime time.Time, dagRunIDPrefix string) (string, error)
	GetRunStatusByDagRunID(ctx context.Context, tnnt tenant.Tenant, jobName scheduler.JobName, dagRunID string) (*scheduler.JobRunStatus, error)
	CancelRun(ctx context.Context, tnnt tenant.Tenant, jobName scheduler.JobName, dagRunID string) error
}

type BackfillService struct {
	backfillRepo BackfillRepository
	jobRepo      JobRepository

	compiler JobInputCompiler

	validator ReplayValidator
	executor  Executor

	tenantGetter TenantGetter

	alertManager AlertManager

	logger log.Logger

	pluginToExecutionProjectKeyMap map[string]string
}

func (b *BackfillService) BackfillDryRun(ctx context.Context, backfillReq *scheduler.Backfill) (*scheduler.ExecutorInput, error) {
	t := backfillReq.Tenant()
	jobName := backfillReq.JobName()
	jobCron, err := getJobCron(ctx, b.logger, b.jobRepo, t, jobName)
	if err != nil {
		b.logger.Error("unable to get cron value for job [%s]: %s", jobName.String(), err.Error())
		return nil, err
	}

	jobWithDetails, err := b.jobRepo.GetJobDetails(ctx, t.ProjectName(), jobName)
	if err != nil {
		return nil, err
	}

	tenantWithDetails, err := b.tenantGetter.GetDetails(ctx, t)
	if err != nil {
		return nil, errors.AddErrContext(err, scheduler.EntityReplay,
			fmt.Sprintf("failed to get tenant details for project [%s], namespace [%s]",
				t.ProjectName(), t.NamespaceName()))
	}

	cfg := injectJobConfigWithTenantConfigs(backfillReq.Config().JobConfig, tenantWithDetails.GetConfigs(), jobWithDetails.Job.Task.Name, b.pluginToExecutionProjectKeyMap)
	backfillReq.SetJobConfig(cfg)

	if err := b.validator.ValidateBackfill(ctx, backfillReq, jobCron); err != nil {
		b.logger.Error("error validating backfill request: %s", err)
		return nil, err
	}

	customOverrideConfig := map[string]string{
		configDstart: backfillReq.Config().Dstart.Format(TimeISOFormat),
		configDend:   backfillReq.Config().Dend.Format(TimeISOFormat),
	}
	for k, v := range backfillReq.Config().Assets {
		jobWithDetails.Job.Assets[k] = v
	}
	for k, v := range backfillReq.GetJobConfig() {
		jobWithDetails.Job.Task.Config[k] = v
	}

	instanceTypeTask := "task"
	executor, err := scheduler.ExecutorFromEnum(jobWithDetails.Job.Task.Name, instanceTypeTask)
	if err != nil {
		b.logger.Error("error adapting executor from instance type [%s]: %s", instanceTypeTask, err)
		return nil, errors.GRPCErr(err, "unable to get job run input for "+jobName.String())
	}
	runConfig, err := scheduler.RunConfigFrom(executor, time.Now(), "", "")
	if err != nil {
		b.logger.Error("error creating run config for job [%s]: %s", jobName.String(), err)
		return nil, errors.GRPCErr(err, "unable to get job run input for "+jobName.String())
	}
	return b.compiler.Compile(ctx, jobWithDetails, runConfig, time.Now(), customOverrideConfig)
}

func (b *BackfillService) GetBackfillByID(ctx context.Context, backfillID uuid.UUID) (*scheduler.Backfill, error) {
	return b.backfillRepo.GetBackfillDetails(ctx, backfillID)
}

func (b *BackfillService) CreateBackfill(ctx context.Context, backfillReq *scheduler.Backfill) (backfillID uuid.UUID, dagRunID string, err error) {
	t := backfillReq.Tenant()
	jobName := backfillReq.JobName()
	jobCron, err := getJobCron(ctx, b.logger, b.jobRepo, t, jobName)
	if err != nil {
		b.logger.Error("unable to get cron value for job [%s]: %s", jobName.String(), err.Error())
		return uuid.Nil, "", err
	}

	jobWithDetails, err := b.jobRepo.GetJobDetails(ctx, t.ProjectName(), jobName)
	if err != nil {
		return uuid.Nil, "", err
	}

	tenantWithDetails, err := b.tenantGetter.GetDetails(ctx, t)
	if err != nil {
		return uuid.Nil, "", errors.AddErrContext(err, scheduler.EntityReplay,
			fmt.Sprintf("failed to get tenant details for project [%s], namespace [%s]",
				t.ProjectName(), t.NamespaceName()))
	}

	cfg := injectJobConfigWithTenantConfigs(backfillReq.Config().JobConfig, tenantWithDetails.GetConfigs(), jobWithDetails.Job.Task.Name, b.pluginToExecutionProjectKeyMap)
	backfillReq.SetJobConfig(cfg)

	if err := b.validator.ValidateBackfill(ctx, backfillReq, jobCron); err != nil {
		b.logger.Error("error validating backfill request: %s", err)
		return uuid.Nil, "", err
	}

	backfillID, err = b.backfillRepo.RegisterBackfill(ctx, backfillReq)
	if err != nil {
		return uuid.Nil, "", err
	}

	jobBackfillMetric.WithLabelValues(t.ProjectName().String(),
		t.NamespaceName().String(),
		jobName.String(),
		backfillReq.State().String(),
	).Inc()

	backfillPrefix := fmt.Sprintf("%s_%s", CustomBackfillPrefix, backfillID.String())

	dagRunID, err = b.executor.CreateRun(ctx, t, jobName, time.Now(), backfillPrefix) //nolint:contextcheck
	if err != nil {
		b.logger.Error("error creating DAG run for backfill [%s]: %s", backfillID.String(), err)
		return uuid.Nil, dagRunID, err
	}

	err = b.backfillRepo.UpdateBackfillSchedulerRunID(ctx, backfillID, dagRunID)
	if err != nil {
		b.logger.Error("error updating backfill scheduler run ID for backfill [%s]: %s", backfillID.String(), err)
		return uuid.Nil, "", err
	}
	return backfillID, dagRunID, nil
}

func (b *BackfillService) GetBackfills(ctx context.Context, projectName tenant.ProjectName, filters ...filter.FilterOpt) ([]*scheduler.Backfill, error) {
	return b.backfillRepo.GetBackfillsByFilter(ctx, projectName, filters...)
}

func (b *BackfillService) CancelBackfill(ctx context.Context, backfillID uuid.UUID, canceledBy string) error {
	backfill, err := b.backfillRepo.GetBackfillDetails(ctx, backfillID)
	if err != nil {
		return err
	}

	if backfill.IsTerminated() {
		return errors.InvalidArgument(scheduler.EntityBackfill,
			fmt.Sprintf("backfill has already terminated with status %s", backfill.State().String()))
	}

	if backfill.SchedulerRunID != "" {
		if err := b.executor.CancelRun(ctx, backfill.Tenant(), backfill.JobName(), backfill.SchedulerRunID); err != nil {
			b.logger.Error("error cancelling DAG run [%s] for backfill [%s]: %s", backfill.SchedulerRunID, backfillID, err)
			return err
		}
	}

	return b.backfillRepo.CancelBackfill(ctx, backfillID, canceledBy)
}

func injectJobConfigWithTenantConfigs(backfillConfig, tenantConfig map[string]string, operatorName string, pluginToExecutionProjectKeyMap map[string]string) map[string]string {
	newConfig := map[string]string{}
	maps.Copy(newConfig, backfillConfig)

	overridedExecutionProjectKey, isSupported := pluginToExecutionProjectKeyMap[operatorName]
	if isSupported && newConfig[overridedExecutionProjectKey] == "" {
		tenantExecutionProject := tenantConfig[tenantReplayExecutionProjectConfigKey]
		if tenantExecutionProject != "" {
			newConfig[overridedExecutionProjectKey] = tenantExecutionProject
		}
	}

	return newConfig
}

func NewBackfillService(
	backfillRepo BackfillRepository,
	jobRepo JobRepository,
	tenantGetter TenantGetter,
	validator ReplayValidator,
	executor Executor,
	logger log.Logger,
	pluginToExecutionProjectKeyMap map[string]string,
	alertManager AlertManager,
	compiler JobInputCompiler,
) *BackfillService {
	return &BackfillService{
		backfillRepo:                   backfillRepo,
		jobRepo:                        jobRepo,
		tenantGetter:                   tenantGetter,
		validator:                      validator,
		executor:                       executor,
		logger:                         logger,
		pluginToExecutionProjectKeyMap: pluginToExecutionProjectKeyMap,
		alertManager:                   alertManager,
		compiler:                       compiler,
	}
}
