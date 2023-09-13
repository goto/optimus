package service

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/goto/salt/log"
	"golang.org/x/net/context"

	"github.com/goto/optimus/core/job"
	"github.com/goto/optimus/ext/extractor/upstream"
	"github.com/goto/optimus/sdk/plugin"
)

const (
	projectConfigPrefix = "GLOBAL__"

	configKeyDstart        = "DSTART"
	configKeyDend          = "DEND"
	configKeyExecutionTime = "EXECUTION_TIME"
	configKeyDestination   = "JOB_DESTINATION"

	TimeISOFormat = time.RFC3339

	// TODO: remove bq dependencies
	MaxBQApiRetries = 3
)

var (
	ErrYamlModNotExist = errors.New("yaml mod not found for plugin")
)

// TODO: decouple extractor from plugin
type UpstreamExtractorFactory interface {
	New(ctx context.Context, svcAcc string, parserFunc upstream.QueryParser) (upstream.Extractor, error)
}

type PluginRepo interface {
	GetByName(string) (*plugin.Plugin, error)
}

type JobPluginService struct {
	pluginRepo   PluginRepo
	extractorFac UpstreamExtractorFactory

	logger log.Logger
}

func NewJobPluginService(pluginRepo PluginRepo, extractorFac UpstreamExtractorFactory, logger log.Logger) *JobPluginService {
	return &JobPluginService{pluginRepo: pluginRepo, extractorFac: extractorFac, logger: logger}
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

func (p JobPluginService) GenerateDestination(ctx context.Context, taskName job.TaskName, configs map[string]string) (job.ResourceURN, error) {
	taskPlugin, err := p.pluginRepo.GetByName(taskName.String())
	if err != nil {
		p.logger.Error("error getting plugin [%s]: %s", taskName.String(), err)
		return "", err
	}
	if taskPlugin.Info().Name != "bq2bq" {
		return "", nil
	}

	// TODO(generic deps resolution): delegate destination construction to resolver(?)
	proj, ok1 := configs["PROJECT"]
	dataset, ok2 := configs["DATASET"]
	tab, ok3 := configs["TABLE"]
	if ok1 && ok2 && ok3 {
		return job.ResourceURN("bigquery://" + fmt.Sprintf("%s:%s.%s", proj, dataset, tab)), nil
	}
	return "", errors.New("missing config key required to generate destination")
}

func (p JobPluginService) GenerateDependencies(ctx context.Context, taskName job.TaskName, svcAcc, query string, destinationURN job.ResourceURN) ([]job.ResourceURN, error) {
	taskPlugin, err := p.pluginRepo.GetByName(taskName.String())
	if err != nil {
		p.logger.Error("error getting plugin [%s]: %s", taskName.String(), err)
		return nil, err
	}
	if taskPlugin.Info().Name != "bq2bq" {
		return []job.ResourceURN{}, nil
	}

	// TODO(generic deps resolution): delegate upstream generator to resolver(?)
	upstreamExtractor, err := p.extractorFac.New(ctx, svcAcc, upstream.ParseTopLevelUpstreamsFromQuery) // currently parse the upstream based on query
	if err != nil {
		return nil, fmt.Errorf("error create upstream extractor: %w", err)
	}
	destinationResource, err := upstream.FromDestinationURN(destinationURN.String())
	if err != nil {
		return nil, fmt.Errorf("error getting destination resource: %w", err)
	}

	upstreams, err := p.extractUpstreams(ctx, upstreamExtractor, query, destinationResource)
	if err != nil {
		return nil, fmt.Errorf("error extracting upstreams: %w", err)
	}

	flattenedUpstreams := upstream.Resources(upstreams).GetFlattened()
	uniqueUpstreams := upstream.Resources(flattenedUpstreams).GetUnique()

	upstreamResourceURNs := []job.ResourceURN{}
	for _, u := range uniqueUpstreams {
		upstreamResourceURNs = append(upstreamResourceURNs, job.ResourceURN(u.URN()))
	}

	return upstreamResourceURNs, nil
}

func (p JobPluginService) extractUpstreams(ctx context.Context, extractor upstream.Extractor, query string, destinationResource *upstream.Resource) ([]*upstream.Resource, error) {
	for try := 1; try <= MaxBQApiRetries; try++ {
		upstreams, err := extractor.ExtractUpstreams(ctx, query, destinationResource)
		if err != nil {
			if strings.Contains(err.Error(), "net/http: TLS handshake timeout") ||
				strings.Contains(err.Error(), "unexpected EOF") ||
				strings.Contains(err.Error(), "i/o timeout") ||
				strings.Contains(err.Error(), "connection reset by peer") {
				// retry
				continue
			}

			p.logger.Error("error extracting upstreams", err)
		}

		return upstreams, nil
	}
	return nil, errors.New("bigquery api retries exhausted")
}
