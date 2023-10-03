package upstream_generator

import (
	"context"
	"fmt"

	"github.com/goto/salt/log"

	"github.com/goto/optimus/ext/extractor"
	"github.com/goto/optimus/ext/store/bigquery"
	"github.com/goto/optimus/plugin/upstream_generator/evaluator"
	"github.com/goto/optimus/plugin/upstream_generator/parser"
)

type (
	// ParserFunc parses given raw and return list of resource urns
	ParserFunc func(rawResource string) (resourceURNs []string)
	// EvalAssetFunc returns raw string from a given asset
	EvalAssetFunc func(assets map[string]string) (rawResource string)
)

type UpstreamGeneratorFactory struct {
	l log.Logger
}

type UpstreamGenerator interface {
	GenerateResources(ctx context.Context, assets map[string]string) ([]string, error)
}

func (u *UpstreamGeneratorFactory) GetBQUpstreamGenerator(ctx context.Context, evaluator evaluator.Evaluator, svcAcc string) (UpstreamGenerator, error) {
	client, err := bigquery.NewClient(ctx, svcAcc)
	if err != nil {
		return nil, err
	}
	e, err := extractor.NewBQExtractor(client, u.l)
	if err != nil {
		return nil, err
	}

	return NewBQUpstreamGenerator(u.l, parser.ParseTopLevelUpstreamsFromQuery, e.Extract, evaluator.Evaluate)
}

func NewUpstreamGeneratorFactory(logger log.Logger) (*UpstreamGeneratorFactory, error) {
	if logger == nil {
		return nil, fmt.Errorf("logger is nil")
	}
	return &UpstreamGeneratorFactory{l: logger}, nil
}
