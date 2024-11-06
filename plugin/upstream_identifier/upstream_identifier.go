package upstreamidentifier

import (
	"context"
	"fmt"

	"github.com/goto/salt/log"

	"github.com/goto/optimus/core/resource"
	"github.com/goto/optimus/ext/extractor"
	"github.com/goto/optimus/ext/store/bigquery"
	"github.com/goto/optimus/plugin/upstream_identifier/evaluator"
	"github.com/goto/optimus/plugin/upstream_identifier/parser"
)

type (
	// ParserFunc parses given raw and return list of resource urns
	ParserFunc func(rawResource string) (resources []string)
	// EvalAssetFunc returns raw string from a given asset
	EvalAssetFunc func(assets map[string]string) (rawResource string)
	// ExtractorFunc extracts the ddl from the given resource urns
	ExtractorFunc func(ctx context.Context, resources []string) (map[string]string, error)
)

type UpstreamIdentifierFactory struct {
	l log.Logger
}

type UpstreamIdentifier interface {
	IdentifyResources(ctx context.Context, assets map[string]string) ([]resource.URN, error)
}

func (u *UpstreamIdentifierFactory) GetBQUpstreamIdentifier(ctx context.Context, svcAcc string, evaluators ...evaluator.Evaluator) (UpstreamIdentifier, error) {
	client, err := bigquery.NewClient(ctx, svcAcc)
	if err != nil {
		return nil, err
	}
	e, err := extractor.NewBQExtractor(client, u.l)
	if err != nil {
		return nil, err
	}
	evaluatorFuncs := make([]EvalAssetFunc, len(evaluators))
	for i, evaluator := range evaluators {
		evaluatorFuncs[i] = evaluator.Evaluate
	}

	return NewBQUpstreamIdentifier(u.l, parser.ParseTopLevelUpstreamsFromQuery, e.Extract, evaluatorFuncs...)
}

func (u *UpstreamIdentifierFactory) GetMaxcomputeUpstreamIdentifier(_ context.Context, evaluators ...evaluator.Evaluator) (UpstreamIdentifier, error) {
	extract := func(ctx context.Context, resources []string) (map[string]string, error) {
		mp := make(map[string]string)
		for _, resource := range resources {
			mp[resource] = ""
		}
		return mp, nil
	}

	evaluatorFuncs := make([]EvalAssetFunc, len(evaluators))
	for i, evaluator := range evaluators {
		evaluatorFuncs[i] = evaluator.Evaluate
	}
	return NewMaxcomputeUpstreamIdentifier(u.l, parser.ParseTopLevelUpstreamsFromQuery, extract, evaluatorFuncs...)
}

func NewUpstreamIdentifierFactory(logger log.Logger) (*UpstreamIdentifierFactory, error) {
	if logger == nil {
		return nil, fmt.Errorf("logger is nil")
	}
	return &UpstreamIdentifierFactory{l: logger}, nil
}
