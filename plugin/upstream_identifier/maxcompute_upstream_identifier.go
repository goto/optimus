package upstreamidentifier

import (
	"context"
	"fmt"

	"github.com/goto/salt/log"

	"github.com/goto/optimus/core/resource"
	"github.com/goto/optimus/internal/errors"
	"github.com/goto/optimus/plugin/upstream_identifier/parser"
)

type MaxcomputeUpstreamIdentifier struct {
	logger         log.Logger
	parserFunc     ParserFunc
	evaluatorFuncs []EvalAssetFunc
}

func NewMaxcomputeUpstreamIdentifier(logger log.Logger, parserFunc ParserFunc, evaluatorFuncs ...EvalAssetFunc) (*MaxcomputeUpstreamIdentifier, error) {
	me := errors.NewMultiError("create maxcompute upstream generator errors")
	if logger == nil {
		me.Append(fmt.Errorf("logger is nil"))
	}
	if parserFunc == nil {
		me.Append(fmt.Errorf("parserFunc is nil"))
	}
	sanitizedEvaluatorFuncs := []EvalAssetFunc{}
	for _, evaluatorFunc := range evaluatorFuncs {
		if evaluatorFunc != nil {
			sanitizedEvaluatorFuncs = append(sanitizedEvaluatorFuncs, evaluatorFunc)
		}
	}
	if len(sanitizedEvaluatorFuncs) == 0 {
		me.Append(fmt.Errorf("non-nil evaluatorFuncs is needed"))
	}
	if me.ToErr() != nil {
		return nil, me.ToErr()
	}
	return &MaxcomputeUpstreamIdentifier{
		logger:         logger,
		parserFunc:     parser.MaxcomputeURNDecorator(parserFunc),
		evaluatorFuncs: evaluatorFuncs,
	}, nil
}

func (g MaxcomputeUpstreamIdentifier) IdentifyResources(_ context.Context, assets map[string]string) ([]resource.URN, error) {
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
	for i, r := range resources {
		resourceURN, err := resource.ParseURN(r)
		if err != nil {
			g.logger.Error("error when parsing resource urn %s", r)
			continue
		}
		resourceURNs[i] = resourceURN
	}
	return resourceURNs
}
