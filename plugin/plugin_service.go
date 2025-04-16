package plugin

import (
	"context"
	"fmt"
	"html/template"
	"path/filepath"
	"strings"

	"github.com/goto/salt/log"

	"github.com/goto/optimus/core/resource"
	"github.com/goto/optimus/internal/errors"
	upstreamidentifier "github.com/goto/optimus/plugin/upstream_identifier"
	"github.com/goto/optimus/plugin/upstream_identifier/evaluator"
	"github.com/goto/optimus/sdk/plugin"
)

const (
	bqSvcAccKey = "BQ_SERVICE_ACCOUNT"
	mcSvcAccKey = "MC_SERVICE_ACCOUNT"
)

type (
	Assets map[string]string
)

type PluginGetter interface {
	GetByName(name string) (*plugin.Plugin, error)
}

type EvaluatorFactory interface {
	GetFileEvaluator(filepath string) (evaluator.Evaluator, error)
	GetYamlPathEvaluator(filepath, selector string) (evaluator.Evaluator, error)
	GetEnvEvaluator(env string) (evaluator.Evaluator, error)
}

type UpstreamIdentifierFactory interface {
	GetBQUpstreamIdentifier(ctx context.Context, svcAcc string, evaluators ...evaluator.Evaluator) (upstreamidentifier.UpstreamIdentifier, error)
	GetMaxcomputeUpstreamIdentifier(ctx context.Context, svcAcc string, evaluators ...evaluator.Evaluator) (upstreamidentifier.UpstreamIdentifier, error)
}

type PluginService struct {
	l            log.Logger
	pluginGetter PluginGetter

	upstreamIdentifierFactory UpstreamIdentifierFactory
	evaluatorFactory          EvaluatorFactory
}

func NewPluginService(logger log.Logger, pluginGetter PluginGetter, upstreamIdentifierFactory UpstreamIdentifierFactory, evaluatorFactory EvaluatorFactory) (*PluginService, error) {
	me := errors.NewMultiError("construct plugin service errors")
	if logger == nil {
		me.Append(fmt.Errorf("logger is nil"))
	}
	if pluginGetter == nil {
		me.Append(fmt.Errorf("pluginGetter is nil"))
	}
	if upstreamIdentifierFactory == nil {
		me.Append(fmt.Errorf("upstreamIdentifierFactory is nil"))
	}
	if evaluatorFactory == nil {
		me.Append(fmt.Errorf("evaluatorFactory is nil"))
	}

	return &PluginService{
		l:                         logger,
		pluginGetter:              pluginGetter,
		upstreamIdentifierFactory: upstreamIdentifierFactory,
		evaluatorFactory:          evaluatorFactory,
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

func (s PluginService) IdentifyUpstreams(ctx context.Context, taskName string, compiledConfig, assets map[string]string) ([]resource.URN, error) {
	taskPlugin, err := s.pluginGetter.GetByName(taskName)
	if err != nil {
		return nil, err
	}

	assetParsers := taskPlugin.Info().AssetParsers
	if assetParsers == nil {
		// if plugin doesn't contain parser, then it doesn't support auto upstream generation
		s.l.Debug("plugin %s doesn't contain parser, auto upstream generation is not supported.", taskPlugin.Info().Name)
		return []resource.URN{}, nil
	}

	// construct all possible identifier from given parser
	upstreamIdentifiers := []upstreamidentifier.UpstreamIdentifier{}
	for parserType, evaluatorSpecs := range assetParsers {
		// instantiate evaluators
		evaluators := []evaluator.Evaluator{}
		for _, evaluatorSpec := range evaluatorSpecs {
			evaluator, err := s.getEvaluator(evaluatorSpec)
			if err != nil {
				return nil, err
			}
			evaluators = append(evaluators, evaluator)
		}

		switch parserType {
		case plugin.MaxcomputeParser:
			svcAcc, ok := compiledConfig[mcSvcAccKey]
			if !ok {
				return nil, fmt.Errorf("secret " + mcSvcAccKey + " required to generate upstream is not found")
			}
			upstreamIdentifier, err := s.upstreamIdentifierFactory.GetMaxcomputeUpstreamIdentifier(ctx, svcAcc, evaluators...)
			if err != nil {
				return nil, err
			}
			upstreamIdentifiers = append(upstreamIdentifiers, upstreamIdentifier)
		case plugin.BQParser:
			svcAcc, ok := compiledConfig[bqSvcAccKey]
			if !ok {
				return nil, fmt.Errorf("secret " + bqSvcAccKey + " required to generate upstream is not found")
			}
			upstreamIdentifier, err := s.upstreamIdentifierFactory.GetBQUpstreamIdentifier(ctx, svcAcc, evaluators...)
			if err != nil {
				return nil, err
			}
			upstreamIdentifiers = append(upstreamIdentifiers, upstreamIdentifier)
		default:
			s.l.Warn("parserType %s is not supported", parserType)
			continue
		}
	}

	// identify all upstream resource urns by all identifier from given asset
	var resourceURNs []resource.URN
	me := errors.NewMultiError("identify upstream errors")
	for _, upstreamIdentifier := range upstreamIdentifiers {
		currentResourceURNs, err := upstreamIdentifier.IdentifyResources(ctx, assets, compiledConfig)
		if err != nil {
			s.l.Error("error when identify upstream")
			me.Append(err)
			continue
		}
		resourceURNs = append(resourceURNs, currentResourceURNs...)
	}

	// ignore destination urns
	destinationURN, err := s.ConstructDestinationURN(ctx, taskName, compiledConfig)
	if err != nil {
		return nil, err
	}

	filteredResourceURNs := make([]resource.URN, 0)
	for _, resourceURN := range resourceURNs {
		if resourceURN == destinationURN {
			s.l.Warn("ignore destination resource %s", resourceURN)
			continue
		}
		filteredResourceURNs = append(filteredResourceURNs, resourceURN)
	}

	return filteredResourceURNs, me.ToErr()
}

func (s PluginService) ConstructDestinationURN(_ context.Context, taskName string, compiledConfig map[string]string) (resource.URN, error) {
	taskPlugin, err := s.pluginGetter.GetByName(taskName)
	if err != nil {
		return resource.ZeroURN(), err
	}

	// for now only support single template
	destinationURNTemplate := taskPlugin.Info().DestinationURNTemplate
	if destinationURNTemplate == "" {
		// if plugin doesn't contain destination template, then it doesn't support auto destination generation
		s.l.Debug("plugin %s doesn't contain destination template, auto destination generation is not supported.", taskPlugin.Info().Name)
		return resource.ZeroURN(), nil
	}

	convertedURNTemplate := convertToGoTemplate(destinationURNTemplate)
	tmpl, err := template.New("destination_urn_" + taskPlugin.Info().Name).Parse(convertedURNTemplate)
	if err != nil {
		return resource.ZeroURN(), err
	}

	rawURN, err := generateResourceURNFromTemplate(tmpl, compiledConfig)
	if err != nil {
		return resource.ZeroURN(), err
	}

	return resource.ParseURN(rawURN)
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

func (s PluginService) getEvaluator(evaluator plugin.Evaluator) (evaluator.Evaluator, error) {
	if evaluator.Selector == "" && evaluator.FilePath != "" {
		return s.evaluatorFactory.GetFileEvaluator(evaluator.FilePath)
	} else if evaluator.Selector != "" && evaluator.FilePath != "" {
		fileExension := filepath.Ext(evaluator.FilePath)
		if fileExension == ".yaml" || fileExension == ".yml" {
			return s.evaluatorFactory.GetYamlPathEvaluator(evaluator.FilePath, evaluator.Selector)
		}
	} else if evaluator.Env != "" {
		return s.evaluatorFactory.GetEnvEvaluator(evaluator.Env)
	}

	return nil, fmt.Errorf("evaluator for filepath %s is not supported", evaluator.FilePath)
}
