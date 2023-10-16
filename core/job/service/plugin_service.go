package service

import (
	"fmt"
	"time"

	"github.com/goto/salt/log"
	"golang.org/x/net/context"

	"github.com/goto/optimus/core/job"
	"github.com/goto/optimus/ext/extractor"
	"github.com/goto/optimus/ext/parser"
	"github.com/goto/optimus/ext/store/bigquery"
	"github.com/goto/optimus/internal/errors"
	"github.com/goto/optimus/sdk/plugin"
)

const (
	projectConfigPrefix = "GLOBAL__"

	TimeISOFormat = time.RFC3339

	// TODO: remove bq dependencies
	MaxBQApiRetries = 3
)

var ErrYamlModNotExist = fmt.Errorf("yaml mod not found for plugin")

type (
	ParserFunc    func(rawResource string) []job.ResourceURN
	ExtractorFunc func(ctx context.Context, resourceURNs []job.ResourceURN) (map[job.ResourceURN]string, error)
)

// TODO: decouple extractor from plugin
type ExtractorFactory interface {
	New(ctx context.Context, svcAcc string, l log.Logger) (extractor.BQExtractorFunc, error)
}

type PluginRepo interface {
	GetByName(string) (*plugin.Plugin, error)
}

type JobPluginService struct {
	pluginRepo PluginRepo

	// TODO(generic deps resolution): move this components alongside with resource parser implementation(?)
	parserFunc   ParserFunc
	extractorFac ExtractorFactory

	logger log.Logger
}

func NewJobPluginService(pluginRepo PluginRepo, extractorFac ExtractorFactory, logger log.Logger) *JobPluginService {
	return &JobPluginService{
		logger:     logger,
		pluginRepo: pluginRepo,

		// TODO(generic deps resolution): move this components alongside with resource parser implementation(?)
		parserFunc:   bqParserDecorator(parser.ParseTopLevelUpstreamsFromQuery),
		extractorFac: extractorFac,
	}
}

func (p JobPluginService) Info(_ context.Context, taskName job.TaskName) (*plugin.Info, error) {
	taskPlugin, err := p.pluginRepo.GetByName(taskName.String())
	if err != nil {
		p.logger.Error("error getting plugin [%s]: %s", taskName.String(), err)
		return nil, err
	}

	if taskPlugin.YamlMod == nil {
		p.logger.Error("task plugin yaml mod is not found")
		return nil, ErrYamlModNotExist
	}

	return taskPlugin.Info(), nil
}

func (p JobPluginService) GenerateDestination(_ context.Context, taskName job.TaskName, configs map[string]string) (job.ResourceURN, error) {
	taskPlugin, err := p.pluginRepo.GetByName(taskName.String())
	if err != nil {
		p.logger.Error("error getting plugin [%s]: %s", taskName.String(), err)
		return "", err
	}
	const bq2bq = "bq2bq"
	if taskPlugin.Info().Name != bq2bq {
		return "", nil
	}

	// TODO(generic deps resolution): make it generic by leverage desination templating from plugin
	proj, ok1 := configs["PROJECT"]
	dataset, ok2 := configs["DATASET"]
	tab, ok3 := configs["TABLE"]
	if ok1 && ok2 && ok3 {
		return job.ResourceURN("bigquery://" + fmt.Sprintf("%s:%s.%s", proj, dataset, tab)), nil
	}
	return "", fmt.Errorf("missing config key required to generate destination")
}

func (p JobPluginService) GenerateDependencies(ctx context.Context, taskName job.TaskName, svcAcc, query string, destinationURN job.ResourceURN) ([]job.ResourceURN, error) {
	taskPlugin, err := p.pluginRepo.GetByName(taskName.String())
	if err != nil {
		p.logger.Error("error getting plugin [%s]: %s", taskName.String(), err)
		return nil, err
	}
	const bq2bq = "bq2bq"
	if taskPlugin.Info().Name != bq2bq {
		return []job.ResourceURN{}, nil
	}

	// TODO(generic deps resolution): make it generic by leverage the parser
	visited := map[job.ResourceURN][]*job.ResourceURNWithUpstreams{}
	visited[destinationURN] = []*job.ResourceURNWithUpstreams{}
	bqExtractorFunc, err := p.extractorFac.New(ctx, svcAcc, p.logger)
	if err != nil {
		return nil, err
	}

	resources, err := p.generateResources(ctx, bqExtractorDecorator(p.logger, bqExtractorFunc), query, visited, map[job.ResourceURN]bool{})
	if err != nil {
		return nil, err
	}

	resourceURNs := []job.ResourceURN{}
	for _, r := range job.ResourceURNWithUpstreamsList(resources).Flatten() {
		if r.URN == destinationURN {
			p.logger.Warn("ignore destination resource %s", r.URN)
			continue
		}
		resourceURNs = append(resourceURNs, r.URN)
	}
	return resourceURNs, nil
}

func (p JobPluginService) generateResources(ctx context.Context, extractorFunc ExtractorFunc, rawResource string, visited map[job.ResourceURN][]*job.ResourceURNWithUpstreams, paths map[job.ResourceURN]bool) ([]*job.ResourceURNWithUpstreams, error) {
	errs := errors.NewMultiError("generate resources")
	resourceURNs := p.parserFunc(rawResource)
	resources := []*job.ResourceURNWithUpstreams{}
	urnToRawResource, err := extractorFunc(ctx, resourceURNs)
	if err != nil {
		p.logger.Error(fmt.Sprintf("error when extract ddl resource: %s", err.Error()))
		return resources, nil
	}

	for _, resourceURN := range resourceURNs {
		resource := &job.ResourceURNWithUpstreams{URN: resourceURN}

		if paths[resourceURN] {
			errs.Append(fmt.Errorf("circular reference is detected"))
			continue
		}

		if _, ok := visited[resourceURN]; !ok {
			rawResource := urnToRawResource[resourceURN]
			paths[resourceURN] = true
			upstreamResources, err := p.generateResources(ctx, extractorFunc, rawResource, visited, paths)
			visited[resourceURN] = upstreamResources
			errs.Append(err)
			delete(paths, resourceURN)
		}
		resource.Upstreams = visited[resourceURN]
		resources = append(resources, resource)
	}

	return resources, errs.ToErr()
}

// bqParserDecorator to convert bigquery resource urn to job resource urn
func bqParserDecorator(fn parser.ParserFunc) ParserFunc {
	return func(rawResource string) []job.ResourceURN {
		bqURNs := fn(rawResource)
		urns := make([]job.ResourceURN, len(bqURNs))
		for i, bqURN := range bqURNs {
			urns[i] = job.ResourceURN(bqURN.URN())
		}
		return urns
	}
}

// bqExtractorDecorator to convert bigquery resource urn to job resource urn
func bqExtractorDecorator(logger log.Logger, fn extractor.BQExtractorFunc) ExtractorFunc {
	return func(ctx context.Context, resourceURNs []job.ResourceURN) (map[job.ResourceURN]string, error) {
		urnToDDL := make(map[job.ResourceURN]string, len(resourceURNs))
		bqURNs := []bigquery.ResourceURN{}
		for _, resourceURN := range resourceURNs {
			urnToDDL[resourceURN] = "" // initialization
			bqURN, err := bigquery.NewResourceURNFromString(resourceURN.String())
			if err != nil {
				logger.Error(err.Error())
				continue
			}
			bqURNs = append(bqURNs, bqURN)
		}

		extractedBqURNToQuery, err := fn(ctx, bqURNs)
		if err != nil {
			return nil, err
		}

		for bqURN, query := range extractedBqURNToQuery {
			urnToDDL[job.ResourceURN(bqURN.URN())] = query
		}
		return urnToDDL, nil
	}
}
