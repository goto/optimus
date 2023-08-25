package service

import (
	"bytes"
	"context"
	"fmt"
	"html/template"
	"strings"
	"time"

	"github.com/goto/salt/log"

	"github.com/goto/optimus/core/scheduler"
	"github.com/goto/optimus/sdk/plugin"
)

const (
	typeEnv = "env"
)

type FilesCompiler interface {
	Compile(fileMap map[string]string, context map[string]any) (map[string]string, error)
}

type PluginRepo interface {
	GetByName(name string) (*plugin.Plugin, error)
}

type JobRunAssetsCompiler struct {
	compiler   FilesCompiler
	pluginRepo PluginRepo

	logger log.Logger
}

func NewJobAssetsCompiler(engine FilesCompiler, pluginRepo PluginRepo, logger log.Logger) *JobRunAssetsCompiler {
	return &JobRunAssetsCompiler{
		compiler:   engine,
		pluginRepo: pluginRepo,
		logger:     logger,
	}
}

func (c *JobRunAssetsCompiler) CompileJobRunAssets(ctx context.Context, job *scheduler.Job, systemEnvVars map[string]string, scheduledAt time.Time, contextForTask map[string]interface{}) (map[string]string, error) {
	startTime, err := job.Window.GetStartTime(scheduledAt)
	if err != nil {
		c.logger.Error("error getting window start time: %s", err)
		return nil, fmt.Errorf("error getting start time: %w", err)
	}
	endTime, err := job.Window.GetEndTime(scheduledAt)
	if err != nil {
		c.logger.Error("error getting window end time: %s", err)
		return nil, fmt.Errorf("error getting end time: %w", err)
	}

	inputFiles := job.Assets

	if job.Task.Name == "bq2bq" { // for now, compile asset is exclusive for bq2bq
		// check if task needs to override the compilation behaviour
		compiledAssets, err := compileAssets(ctx, startTime, endTime, job.Task.Config, job.Assets, systemEnvVars)
		if err != nil {
			c.logger.Error("error compiling assets through plugin dependency mod: %s", err)
			return nil, err
		}
		inputFiles = compiledAssets
	}

	fileMap, err := c.compiler.Compile(inputFiles, contextForTask)
	if err != nil {
		c.logger.Error("error compiling assets: %s", err)
		return nil, err
	}
	return fileMap, nil
}

// compileAssets taken from https://github.com/goto/transformers/blob/a96357a7d170f9c89a52600eab4bc57a8e18489c/task/bq2bq/main.go#L85
func compileAssets(ctx context.Context, startTime, endTime time.Time, configs, systemEnvVars, assets map[string]string) (map[string]string, error) {
	method, ok := configs["LOAD_METHOD"]
	if !ok || method != "REPLACE" {
		return assets, nil
	}

	// partition window in range
	instanceEnvMap := map[string]interface{}{}
	for name, value := range systemEnvVars {
		instanceEnvMap[name] = value
	}

	// TODO: making few assumptions here, should be documented
	// assume destination table is time partitioned
	// assume table is partitioned as DAY
	const dayHours = time.Duration(24)
	partitionDelta := time.Hour * dayHours

	// find destination partitions
	var destinationsPartitions []struct {
		start time.Time
		end   time.Time
	}
	for currentPart := startTime; currentPart.Before(endTime); currentPart = currentPart.Add(partitionDelta) {
		destinationsPartitions = append(destinationsPartitions, struct {
			start time.Time
			end   time.Time
		}{
			start: currentPart,
			end:   currentPart.Add(partitionDelta),
		})
	}

	// check if window size is greater than partition delta(a DAY), if not do nothing
	if endTime.Sub(startTime) <= partitionDelta {
		return assets, nil
	}

	var parsedQueries []string
	var err error

	compiledAssetMap := assets
	// append job spec assets to list of files need to write
	fileMap := compiledAssetMap
	queryFileReplaceBreakMarker := "\n--*--optimus-break-marker--*--\n"
	for _, part := range destinationsPartitions {
		instanceEnvMap["DSTART"] = part.start.Format(time.RFC3339)
		instanceEnvMap["DEND"] = part.end.Format(time.RFC3339)
		if compiledAssetMap, err = compile(fileMap, instanceEnvMap); err != nil {
			return nil, err
		}
		parsedQueries = append(parsedQueries, compiledAssetMap["query.sql"])
	}
	compiledAssetMap["query.sql"] = strings.Join(parsedQueries, queryFileReplaceBreakMarker)

	return compiledAssetMap, nil
}

func compile(templateMap map[string]string, context map[string]any) (map[string]string, error) {
	rendered := map[string]string{}
	baseTemplate := template.
		New("bq2bq_template_compiler").
		Funcs(map[string]any{
			"Date": dateFn,
		})

	for name, content := range templateMap {
		tmpl, err := baseTemplate.New(name).Parse(content)
		if err != nil {
			return nil, fmt.Errorf("unable to parse template content: %w", err)
		}

		var buf bytes.Buffer
		err = tmpl.Execute(&buf, context)
		if err != nil {
			return nil, fmt.Errorf("unable to render template: %w", err)
		}
		rendered[name] = strings.TrimSpace(buf.String())
	}
	return rendered, nil
}

func dateFn(timeStr string) (string, error) {
	t, err := time.Parse(time.RFC3339, timeStr)
	if err != nil {
		return "", err
	}
	return t.Format(time.RFC3339), nil
}

// TODO: deprecate after changing type for plugin
func toJobRunSpecData(mapping map[string]string) []plugin.JobRunSpecData {
	var jobRunData []plugin.JobRunSpecData
	for name, value := range mapping {
		jrData := plugin.JobRunSpecData{
			Name:  name,
			Value: value,
			Type:  typeEnv,
		}
		jobRunData = append(jobRunData, jrData)
	}
	return jobRunData
}

// TODO: deprecate
func toPluginAssets(assets map[string]string) plugin.Assets {
	var modelAssets plugin.Assets
	for name, val := range assets {
		pa := plugin.Asset{
			Name:  name,
			Value: val,
		}
		modelAssets = append(modelAssets, pa)
	}
	return modelAssets
}

// TODO: deprecate
func toPluginConfig(conf map[string]string) plugin.Configs {
	var pluginConfigs plugin.Configs
	for name, val := range conf {
		pc := plugin.Config{
			Name:  name,
			Value: val,
		}
		pluginConfigs = append(pluginConfigs, pc)
	}
	return pluginConfigs
}
