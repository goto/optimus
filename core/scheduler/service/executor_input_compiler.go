package service

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/goto/salt/log"

	"github.com/goto/optimus/core/scheduler"
	"github.com/goto/optimus/core/tenant"
	"github.com/goto/optimus/internal/compiler"
	"github.com/goto/optimus/internal/lib/interval"
	"github.com/goto/optimus/internal/lib/window"
	"github.com/goto/optimus/internal/utils"
)

const (
	// taskConfigPrefix will be used to prefix all the config variables of
	// transformation instance, i.e. task
	taskConfigPrefix = "TASK__"

	// projectConfigPrefix will be used to prefix all the config variables of
	// a project, i.e. registered entities
	projectConfigPrefix = "GLOBAL__"

	contextProject       = "proj"
	contextSecret        = "secret"
	contextSystemDefined = "inst"
	contextTask          = "task"

	SecretsStringToMatch = ".secret."

	TimeISOFormat = time.RFC3339
	TimeSQLFormat = time.DateTime

	// Configuration for system defined variables
	configDstart        = "DSTART"
	configStartDate     = "START_DATE"
	configDend          = "DEND"
	configEndDate       = "END_DATE"
	configExecutionTime = "EXECUTION_TIME"
	configScheduleTime  = "SCHEDULE_TIME"
	configScheduleDate  = "SCHEDULE_DATE"
	configDestination   = "JOB_DESTINATION"

	JobAttributionLabelsKey = "JOB_LABELS"

	maxJobAttributionLabelLength = 63
)

var invalidLabelCharacterRegex *regexp.Regexp

type TenantService interface {
	GetDetails(ctx context.Context, tnnt tenant.Tenant) (*tenant.WithDetails, error)
	GetSecrets(ctx context.Context, tnnt tenant.Tenant) ([]*tenant.PlainTextSecret, error)
}

type TemplateCompiler interface {
	Compile(templateMap map[string]string, context map[string]any) (map[string]string, error)
}

type AssetCompiler interface {
	CompileJobRunAssets(ctx context.Context, job *scheduler.Job, systemEnvVars map[string]string, interval interval.Interval, contextForTask map[string]interface{}) (map[string]string, error)
}

type InputCompiler struct {
	tenantService TenantService
	compiler      TemplateCompiler
	assetCompiler AssetCompiler

	logger log.Logger
}

// sanitiseLabel implements validation from https://cloud.google.com/bigquery/docs/labels-intro#requirements
func sanitiseLabel(key string) string {
	if len(key) > maxJobAttributionLabelLength {
		key = "__" + key[len(key)-61:]
	}
	key = strings.ToLower(key)
	key = invalidLabelCharacterRegex.ReplaceAllString(key, "-")
	return key
}

func getJobLabelsString(labels map[string]string) string {
	var labelStringArray []string
	for key, value := range labels {
		labelStringArray = append(labelStringArray, fmt.Sprintf("%s=%s", sanitiseLabel(key), sanitiseLabel(value)))
	}
	return strings.Join(labelStringArray, ",")
}

func (i InputCompiler) Compile(ctx context.Context, job *scheduler.JobWithDetails, config scheduler.RunConfig, executedAt time.Time) (*scheduler.ExecutorInput, error) {
	tenantDetails, err := i.tenantService.GetDetails(ctx, job.Job.Tenant)
	if err != nil {
		i.logger.Error("error getting tenant details: %s", err)
		return nil, err
	}

	w, err := getWindow(tenantDetails.Project(), job)
	if err != nil {
		return nil, err
	}

	interval, err := w.GetInterval(config.ScheduledAt)
	if err != nil {
		return nil, err
	}

	systemDefinedVars := getSystemDefinedConfigs(job.Job, interval, executedAt, config.ScheduledAt)

	// Prepare template context and compile task config
	taskContext := compiler.PrepareContext(
		compiler.From(tenantDetails.GetVariables()).WithName(contextProject).WithKeyPrefix(projectConfigPrefix),
		compiler.From(tenantDetails.SecretsMap()).WithName(contextSecret),
		compiler.From(systemDefinedVars).WithName(contextSystemDefined).AddToContext(),
	)

	confs, secretConfs, err := i.compileConfigs(job.Job.Task.Config, taskContext)
	if err != nil {
		i.logger.Error("error compiling task config: %s", err)
		return nil, err
	}

	// Compile asset files
	allTaskConfigs := compiler.PrepareContext(
		compiler.From(confs, secretConfs).WithName(contextTask).WithKeyPrefix(taskConfigPrefix),
	)

	mergedContext := utils.MergeAnyMaps(taskContext, allTaskConfigs)
	fileMap, err := i.assetCompiler.CompileJobRunAssets(ctx, job.Job, systemDefinedVars, interval, mergedContext)
	if err != nil {
		i.logger.Error("error compiling job run assets: %s", err)
		return nil, err
	}

	jobLabelsToAdd := map[string]string{
		"project":   job.Job.Tenant.ProjectName().String(),
		"namespace": job.Job.Tenant.NamespaceName().String(),
		"job_name":  job.Job.Name.String(),
		"job_id":    job.Job.ID.String(),
	}

	if job.JobMetadata != nil {
		for key, value := range job.JobMetadata.Labels {
			if _, ok := jobLabelsToAdd[key]; !ok {
				jobLabelsToAdd[key] = value
			}
		}
	}

	jobAttributionLabels := getJobLabelsString(jobLabelsToAdd)

	if config.Executor.Type == scheduler.ExecutorTask {
		enriched := i.getEnrichedWithJobLabelsValue(confs, jobAttributionLabels)
		return &scheduler.ExecutorInput{
			Configs: utils.MergeMaps(enriched, systemDefinedVars),
			Secrets: secretConfs,
			Files:   fileMap,
		}, nil
	}

	hook, err := job.Job.GetHook(config.Executor.Name)
	if err != nil {
		i.logger.Error("error getting hook [%s]: %s", config.Executor.Name, err)
		return nil, err
	}

	hookConfs, hookSecrets, err := i.compileConfigs(hook.Config, mergedContext)
	if err != nil {
		i.logger.Error("error compiling configs for hook [%s]: %s", hook.Name, err)
		return nil, err
	}

	enriched := i.getEnrichedWithJobLabelsValue(hookConfs, jobAttributionLabels)
	return &scheduler.ExecutorInput{
		Configs: utils.MergeMaps(enriched, systemDefinedVars),
		Secrets: hookSecrets,
		Files:   fileMap,
	}, nil
}

func (InputCompiler) getEnrichedWithJobLabelsValue(config map[string]string, incomingValue string) map[string]string {
	output := make(map[string]string)
	for key, value := range config {
		output[key] = value
	}

	if existingValue, ok := output[JobAttributionLabelsKey]; ok {
		if len(existingValue) == 0 {
			output[JobAttributionLabelsKey] = incomingValue
		} else {
			output[JobAttributionLabelsKey] = existingValue + "," + incomingValue
		}
	} else {
		output[JobAttributionLabelsKey] = incomingValue
	}

	return output
}

func (i InputCompiler) compileConfigs(configs map[string]string, templateCtx map[string]any) (map[string]string, map[string]string, error) {
	conf, secretsConfig := splitConfigWithSecrets(configs)

	var err error
	if conf, err = i.compiler.Compile(conf, templateCtx); err != nil {
		i.logger.Error("error compiling template with config: %s", err)
		return nil, nil, err
	}

	if secretsConfig, err = i.compiler.Compile(secretsConfig, templateCtx); err != nil {
		i.logger.Error("error compiling template with secret: %s", err)
		return nil, nil, err
	}

	return conf, secretsConfig, nil
}

func getSystemDefinedConfigs(job *scheduler.Job, interval interval.Interval, executedAt, scheduledAt time.Time) map[string]string {
	vars := map[string]string{
		configDstart:        interval.Start().Format(TimeISOFormat),
		configDend:          interval.End().Format(TimeISOFormat),
		configExecutionTime: executedAt.Format(TimeSQLFormat), // TODO: remove this once ali support RFC3339 format
		configDestination:   job.Destination.String(),
	}
	// TODO: remove this condition after v1/v2 removal, add to map directly
	if job.WindowConfig.GetVersion() == window.NewWindowVersion {
		vars[configStartDate] = interval.Start().Format(time.DateOnly)
		vars[configEndDate] = interval.End().Format(time.DateOnly)
		vars[configExecutionTime] = scheduledAt.Format(TimeSQLFormat) // TODO: remove this once ali support RFC3339 format
		vars[configScheduleTime] = scheduledAt.Format(TimeISOFormat)
		vars[configScheduleDate] = scheduledAt.Format(time.DateOnly)
	}

	return vars
}

func splitConfigWithSecrets(conf map[string]string) (map[string]string, map[string]string) {
	configs := map[string]string{}
	configWithSecrets := map[string]string{}
	for name, val := range conf {
		if strings.Contains(val, SecretsStringToMatch) {
			configWithSecrets[name] = val
			continue
		}
		configs[name] = val
	}

	return configs, configWithSecrets
}

func NewJobInputCompiler(tenantService TenantService, compiler TemplateCompiler, assetCompiler AssetCompiler, logger log.Logger) *InputCompiler {
	invalidLabelCharacterRegex = regexp.MustCompile(`[^\w-]`)
	return &InputCompiler{
		tenantService: tenantService,
		compiler:      compiler,
		assetCompiler: assetCompiler,
		logger:        logger,
	}
}

func getWindow(project *tenant.Project, job *scheduler.JobWithDetails) (window.Window, error) {
	return window.From(job.Job.WindowConfig, job.Schedule.Interval, project.GetPreset)
}
