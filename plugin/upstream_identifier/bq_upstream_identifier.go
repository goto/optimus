package upstreamidentifier

import (
	"context"
	"fmt"

	"github.com/goto/salt/log"

	"github.com/goto/optimus/core/resource"
	"github.com/goto/optimus/ext/store/bigquery"
	"github.com/goto/optimus/internal/errors"
	"github.com/goto/optimus/plugin/upstream_identifier/parser"
)

type (
	BQExtractorFunc func(context.Context, []bigquery.ResourceURN) (map[bigquery.ResourceURN]string, error)
	extractorFunc   func(ctx context.Context, resourceURNs []string) (map[string]string, error)
)

type BQUpstreamIdentifier struct {
	logger         log.Logger
	parserFunc     ParserFunc
	extractorFunc  extractorFunc
	evaluatorFuncs []EvalAssetFunc
}

func (g BQUpstreamIdentifier) IdentifyResources(ctx context.Context, assets map[string]string) ([]resource.URN, error) {
	resourcesAccumulation := []*bigquery.ResourceURNWithUpstreams{}

	// generate resource urn with upstream from each evaluator
	me := errors.NewMultiError("identify resource errors")
	for _, evaluatorFunc := range g.evaluatorFuncs {
		query := evaluatorFunc(assets)
		if query == "" {
			continue
		}

		visited := map[string][]*bigquery.ResourceURNWithUpstreams{}
		paths := map[string]bool{}
		resources, err := g.identifyResources(ctx, query, visited, paths)
		if err != nil {
			me.Append(err)
			continue
		}
		resourcesAccumulation = append(resourcesAccumulation, resources...)
	}

	// compiled all collected resources and extract its urns
	flattenedResources := bigquery.ResourceURNWithUpstreamsList(resourcesAccumulation).FlattenUnique()
	var resourceURNs []resource.URN
	for _, r := range flattenedResources {
		rawURN := r.ResourceURN.URN()

		urn, err := resource.ParseURN(rawURN)
		if err != nil {
			me.Append(err)
			continue
		}

		resourceURNs = append(resourceURNs, urn)
	}
	return resourceURNs, me.ToErr()
}

func (g BQUpstreamIdentifier) identifyResources(ctx context.Context, query string, visited map[string][]*bigquery.ResourceURNWithUpstreams, paths map[string]bool) ([]*bigquery.ResourceURNWithUpstreams, error) {
	me := errors.NewMultiError("identify resources errors")
	resourceURNs := g.parserFunc(query)
	resources := []*bigquery.ResourceURNWithUpstreams{}
	if len(resourceURNs) == 0 {
		return resources, nil
	}

	urnToQuery, err := g.extractorFunc(ctx, resourceURNs)
	if err != nil {
		g.logger.Error(fmt.Sprintf("error when extract ddl resource: %s", err.Error()))
		return nil, err
	}

	for _, resourceURN := range resourceURNs {
		bqResourceURN, err := bigquery.NewResourceURNFromString(resourceURN)
		if err != nil {
			g.logger.Error(err.Error())
			continue
		}
		resource := &bigquery.ResourceURNWithUpstreams{ResourceURN: bqResourceURN}

		if paths[resourceURN] {
			me.Append(fmt.Errorf("circular reference is detected"))
			continue
		}

		if _, ok := visited[resourceURN]; !ok {
			query := urnToQuery[resourceURN]
			paths[resourceURN] = true
			upstreamResources, err := g.identifyResources(ctx, query, visited, paths)
			visited[resourceURN] = upstreamResources
			me.Append(err)
			delete(paths, resourceURN)
		}
		resource.Upstreams = visited[resourceURN]
		resources = append(resources, resource)
	}

	return resources, me.ToErr()
}

func NewBQUpstreamIdentifier(logger log.Logger, parserFunc ParserFunc, bqExtractorFunc BQExtractorFunc, evaluatorFuncs ...EvalAssetFunc) (*BQUpstreamIdentifier, error) {
	me := errors.NewMultiError("create bq upstream generator errors")
	if logger == nil {
		me.Append(fmt.Errorf("logger is nil"))
	}
	if parserFunc == nil {
		me.Append(fmt.Errorf("parserFunc is nil"))
	}
	if bqExtractorFunc == nil {
		me.Append(fmt.Errorf("bqExtractorFunc is nil"))
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

	return &BQUpstreamIdentifier{
		logger:         logger,
		parserFunc:     parser.BQURNDecorator(parserFunc),
		extractorFunc:  bqExtractorDecorator(logger, bqExtractorFunc),
		evaluatorFuncs: sanitizedEvaluatorFuncs,
	}, nil
}

// bqExtractorDecorator to convert bigquery resource urn to plain resource urn string
func bqExtractorDecorator(logger log.Logger, fn BQExtractorFunc) extractorFunc {
	return func(ctx context.Context, resourceURNs []string) (map[string]string, error) {
		urnToQuery := make(map[string]string, len(resourceURNs))
		bqURNs := []bigquery.ResourceURN{}
		for _, resourceURN := range resourceURNs {
			urnToQuery[resourceURN] = "" // initialization
			bqURN, err := bigquery.NewResourceURNFromString(resourceURN)
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
			urnToQuery[bqURN.URN()] = query
		}
		return urnToQuery, nil
	}
}
