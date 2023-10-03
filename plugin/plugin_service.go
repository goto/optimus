package plugin

import (
	"context"
	"fmt"
	"html/template"
	"strings"

	"github.com/goto/salt/log"

	"github.com/goto/optimus/internal/errors"
	ug "github.com/goto/optimus/plugin/upstream_generator"
	"github.com/goto/optimus/plugin/upstream_generator/evaluator"
	"github.com/goto/optimus/sdk/plugin"
)

type (
	Assets map[string]string
)

type PluginGetter interface {
	GetByName(name string) (*plugin.Plugin, error)
}

type EvaluatorFactory interface {
	GetFileEvaluator(filepath string) (evaluator.Evaluator, error)
}

type UpstreamGeneratorFactory interface {
	GetBQUpstreamGenerator(ctx context.Context, evaluator evaluator.Evaluator, svcAcc string) (ug.UpstreamGenerator, error)
}

type PluginService struct {
	l            log.Logger
	pluginGetter PluginGetter

	upstreamGeneratorFactory UpstreamGeneratorFactory
	evaluatorFactory         EvaluatorFactory
}

func NewPluginService(logger log.Logger, pluginGetter PluginGetter, upstreamGeneratorFactory UpstreamGeneratorFactory, evaluatorFactory EvaluatorFactory) (*PluginService, error) {
	me := errors.NewMultiError("construct plugin service errors")
	if logger == nil {
		me.Append(fmt.Errorf("logger is nil"))
	}
	if pluginGetter == nil {
		me.Append(fmt.Errorf("pluginGetter is nil"))
	}
	if upstreamGeneratorFactory == nil {
		me.Append(fmt.Errorf("upstreamGeneratorFactory is nil"))
	}
	if evaluatorFactory == nil {
		me.Append(fmt.Errorf("evaluatorFactory is nil"))
	}

	return &PluginService{
		l:                        logger,
		pluginGetter:             pluginGetter,
		upstreamGeneratorFactory: upstreamGeneratorFactory,
		evaluatorFactory:         evaluatorFactory,
	}, me.ToErr()
}

func (s PluginService) Info(_ context.Context, taskName string) (*plugin.Info, error) {
	taskPlugin, err := s.pluginGetter.GetByName(taskName)
	if err != nil {
		s.l.Error("error getting plugin [%s]: %s", taskName, err)
		return nil, err
	}
	if taskPlugin.YamlMod == nil {
		s.l.Error("task plugin yaml mod is not found")
		return nil, fmt.Errorf("yaml mod not exist")
	}

	return taskPlugin.Info(), nil
}

func (s PluginService) GenerateUpstreams(ctx context.Context, taskName string, config map[string]string, assets map[string]string) ([]string, error) {
	plugin, err := s.pluginGetter.GetByName(taskName)
	if err != nil {
		return nil, err
	}

	assetParser := plugin.Info().AssetParser
	if assetParser == nil {
		// if plugin doesn't contain parser, then it doesn't support auto upstream generation
		s.l.Debug("plugin %s doesn't contain parser, auto upstream generation is not supported.", plugin.Info().Name)
		return []string{}, nil
	}

	// for now the evaluator is only scoped for file evaluator
	evaluator, err := s.evaluatorFactory.GetFileEvaluator(assetParser.FilePath)
	if err != nil {
		return nil, err
	}

	// for now upstream generator is only scoped for bigquery
	svcAcc, ok := config["BQ_SERVICE_ACCOUNT"]
	if !ok {
		return nil, fmt.Errorf("secret BQ_SERVICE_ACCOUNT required to generate upstream is not found")
	}
	upstreamGenerator, err := s.upstreamGeneratorFactory.GetBQUpstreamGenerator(ctx, evaluator, svcAcc)
	if err != nil {
		return nil, err
	}

	return upstreamGenerator.GenerateResources(ctx, assets)
}

func (s PluginService) ConstructDestinationURN(ctx context.Context, taskName string, config map[string]string) (string, error) {
	plugin, err := s.pluginGetter.GetByName(taskName)
	if err != nil {
		return "", err
	}

	// for now only support single template
	destinationURNTemplate := plugin.Info().DestinationURNTemplate
	if destinationURNTemplate == "" {
		// if plugin doesn't contain destination template, then it doesn't support auto destination generation
		s.l.Debug("plugin %s doesn't contain destination template, auto destination generation is not supported.", plugin.Info().Name)
		return "", nil
	}

	convertedURNTemplate := convertToGoTemplate(destinationURNTemplate)
	tmpl, err := template.New("destination_urn_" + plugin.Info().Name).Parse(convertedURNTemplate)
	if err != nil {
		return "", err
	}

	return generateResourceURNFromTemplate(tmpl, config)
}

// convertToGoTemplate transforms plugin destination urn template format to go template format
// eg. `bigquery://<PROJECT_NAME>:<DATASET_NAME>.<TABLE_NAME>` with map name config
// will be converted to `bigquery://{{ .PROJECT_NAME }}:{{ .DATASET_NAME }}.{{ .TABLE_NAME }}`
func convertToGoTemplate(destinationURNTemplate string) string {
	convertedTemplate := destinationURNTemplate
	convertedTemplate = strings.ReplaceAll(convertedTemplate, "<", `{{ .`)
	convertedTemplate = strings.ReplaceAll(convertedTemplate, ">", ` }}`)
	return convertedTemplate
}

func generateResourceURNFromTemplate(tmpl *template.Template, config map[string]string) (string, error) {
	s := &strings.Builder{}
	if err := tmpl.Execute(s, config); err != nil {
		return "", err
	}
	return s.String(), nil
}
