package upstreamidentifier

import (
	"context"
	"fmt"

	"github.com/goto/salt/log"

	"github.com/goto/optimus/ext/extractor"
	"github.com/goto/optimus/ext/store/bigquery"
	"github.com/goto/optimus/internal/lib"
	"github.com/goto/optimus/plugin/upstream_identifier/evaluator"
	"github.com/goto/optimus/plugin/upstream_identifier/parser"
)

type (
	// ParserFunc parses given raw and return list of resource urns
	ParserFunc func(rawResource string) (resourceURNs []string)
	// EvalAssetFunc returns raw string from a given asset
	EvalAssetFunc func(assets map[string]string) (rawResource string)
)

type UpstreamIdentifierFactory struct {
	l log.Logger
}

type UpstreamIdentifier interface {
	IdentifyResources(ctx context.Context, assets map[string]string) ([]lib.URN, error)
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

func NewUpstreamIdentifierFactory(logger log.Logger) (*UpstreamIdentifierFactory, error) {
	if logger == nil {
		return nil, fmt.Errorf("logger is nil")
	}
	return &UpstreamIdentifierFactory{l: logger}, nil
}
