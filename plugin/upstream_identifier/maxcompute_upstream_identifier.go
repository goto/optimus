package upstreamidentifier

import (
	"context"

	"github.com/goto/optimus/core/resource"
	"github.com/goto/optimus/plugin/upstream_identifier/parser"
	"github.com/goto/salt/log"
)

type MaxcomputeUpstreamIdentifier struct {
	logger         log.Logger
	parserFunc     ParserFunc
	evaluatorFuncs []EvalAssetFunc
}

func NewMaxcomputeUpstreamIdentifier(logger log.Logger, parserFunc ParserFunc, evaluatorFuncs ...EvalAssetFunc) (*MaxcomputeUpstreamIdentifier, error) {
	return &MaxcomputeUpstreamIdentifier{
		logger:         logger,
		parserFunc:     parser.MaxcomputeURNDecorator(parserFunc),
		evaluatorFuncs: evaluatorFuncs,
	}, nil
}

func (g MaxcomputeUpstreamIdentifier) IdentifyResources(ctx context.Context, assets map[string]string) ([]resource.URN, error) {
	resourceURNs := []resource.URN{}

	// generate resource urn with upstream from each evaluator
	for _, evaluatorFunc := range g.evaluatorFuncs {
		query := evaluatorFunc(assets)
		if query == "" {
			continue
		}
		resources := g.identifyResources(query)
		resourceURNs = append(resourceURNs, resources...)
	}
	return resourceURNs, nil
}

func (g MaxcomputeUpstreamIdentifier) identifyResources(query string) []resource.URN {
	resources := g.parserFunc(query)
	resourceURNs := make([]resource.URN, len(resources))
	for _, r := range resources {
		resourceURN, _ := resource.NewURN("maxcompute", r) // TODO: use dedicated function new resource from string
		resourceURNs = append(resourceURNs, resourceURN)
	}
	return resourceURNs
}
