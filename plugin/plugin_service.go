package plugin

import (
	"context"

	"github.com/goto/optimus/internal/models"
)

type (
	ResourceURN string
	Name        string
	TaskConfig  map[string]string
	Assets      map[string]string

	ParserFunc               func(rawResource string) []ResourceURN
	ExtractorFunc            func(ctx context.Context, resourceURNs []ResourceURN) (map[ResourceURN]string, error)
	GenerateUpstreamFunc     func(assets Assets) []ResourceURN
	EvalAssetFunc            func(assets Assets) (rawResource string)
	ConstructDestinationFunc func(config TaskConfig) ResourceURN
)

type UptreamGenerator interface {
	GenerateUpstreams(rawResource string) []ResourceURN
}

type PluginService struct {
	pluginRepo *models.PluginRepository
}

func (s PluginService) GetUpstreamGeneratorFunc(taskName Name, config TaskConfig) GenerateUpstreamFunc {
	// return upstream generator based on the plugin spec
	plugin, _ := s.pluginRepo.GetByName(string(taskName))

	// plugin.Info().AssetParser.Type -> bq
	filepath := plugin.Info().AssetParser.FilePath
	fileEvaluator := FileEvaluator{ // example for now is file evaluator
		filepath: filepath, // use filepath from plugin yaml spec
	}

	return BQUpstreamGenerator{ // example for now is BQ
		parserFunc:    nil, // use bq parser -> parser.ParseTopLevelUpstreamsFromQuery
		extractorFunc: nil, // use extractor -> extractor.NewBQExtractor(client, l).Extract                                                   // use bq extractor
		evaluate:      fileEvaluator.Evaluate,
	}.GenerateUpstreams
}

func (s PluginService) GetDestinationConstructorFunc(taskName Name) ConstructDestinationFunc {
	// return destination constructor based on the template provided from plugin spec
	plugin, _ := s.pluginRepo.GetByName(string(taskName))

	template := plugin.Info().DestinationURNTemplate // get template from plugin spec
	return func(config TaskConfig) ResourceURN {
		// construct resource urn from template and task config
		return generateResourceURNFromTemplate(template, config)
	}
}

func generateResourceURNFromTemplate(template string, config TaskConfig) ResourceURN {
	// implement resource urn construction
	return ResourceURN("")
}
