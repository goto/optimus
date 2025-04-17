package upstreamidentifier

import (
	"context"
	"fmt"

	"github.com/goto/salt/log"

	"github.com/goto/optimus/core/resource"
	"github.com/goto/optimus/internal/errors"
)

type MaxcomputeUpstreamIdentifier struct {
	logger         log.Logger
	parserFunc     ParserFunc
	extractorFunc  ExtractorFunc
	evaluatorFuncs []EvalFunc
}

func NewMaxcomputeUpstreamIdentifier(logger log.Logger, parserFunc ParserFunc, extractorFunc ExtractorFunc, evaluatorFuncs ...EvalFunc) (*MaxcomputeUpstreamIdentifier, error) {
	me := errors.NewMultiError("create maxcompute upstream generator errors")
	if logger == nil {
		me.Append(fmt.Errorf("logger is nil"))
	}
	if extractorFunc == nil {
		me.Append(fmt.Errorf("extractorFunc is nil"))
	}
	if parserFunc == nil {
		me.Append(fmt.Errorf("parserFunc is nil"))
	}
	sanitizedEvaluatorFuncs := []EvalFunc{}
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
		parserFunc:     parserFunc,
		extractorFunc:  extractorFunc,
		evaluatorFuncs: evaluatorFuncs,
	}, nil
}

func (g MaxcomputeUpstreamIdentifier) IdentifyResources(ctx context.Context, assets, config map[string]string) ([]resource.URN, error) {
	resources := []string{}

	// generate resource urn with upstream from each evaluator
	for _, evaluatorFunc := range g.evaluatorFuncs {
		query := evaluatorFunc(assets, config)
		if query == "" {
			continue
		}
		visited := map[string]bool{}
		resources = append(resources, g.identifyResources(ctx, visited, query)...)
	}

	// generate resource URNs
	resourceURNs := []resource.URN{}
	for _, r := range resources {
		urn, err := resource.NewURN(resource.MaxCompute.String(), r)
		if err != nil {
			return nil, err
		}
		resourceURNs = append(resourceURNs, urn)
	}
	return resourceURNs, nil
}

func (g MaxcomputeUpstreamIdentifier) identifyResources(ctx context.Context, visited map[string]bool, query string) []string {
	resources := g.parserFunc(query)
	g.logger.Debug(fmt.Sprintf("resources from parsed query: %v", resources))
	resourceToDDL, err := g.extractorFunc(ctx, resources)
	if err != nil {
		g.logger.Error(fmt.Sprintf("error when extract ddl resource: %s", err.Error()))
		return nil
	}

	// collect the actual resources
	actualResources := []string{}
	for _, resource := range resources {
		if _, ok := visited[resource]; ok {
			continue
		}

		// mark the resource as visited
		g.logger.Debug(fmt.Sprintf("check ddl for resource: %s", resource))
		visited[resource] = true
		ddl := resourceToDDL[resource]
		if ddl == "" {
			// indicate that the resource is not a view
			actualResources = append(actualResources, resource)
			continue
		}

		// otherwise, recursively identify the upstreams
		g.logger.Debug(fmt.Sprintf("recursively identify upstreams for resource view: %s", resource))
		actualResources = append(actualResources, g.identifyResources(ctx, visited, ddl)...)
	}

	return actualResources
}
