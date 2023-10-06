package upstreamgenerator

import (
	"context"
	"fmt"

	"github.com/goto/salt/log"

	"github.com/goto/optimus/ext/store/bigquery"
	"github.com/goto/optimus/internal/errors"
)

type (
	BQExtractorFunc func(context.Context, []bigquery.ResourceURN) (map[bigquery.ResourceURN]string, error)
	extractorFunc   func(ctx context.Context, resourceURNs []string) (map[string]string, error)
)

type BQUpstreamGenerator struct {
	logger         log.Logger
	parserFunc     ParserFunc
	extractorFunc  extractorFunc
	evaluatorFuncs []EvalAssetFunc
}

func (g BQUpstreamGenerator) GenerateResources(ctx context.Context, assets map[string]string) ([]string, error) {
	resourcesAccumulation := []*bigquery.ResourceURNWithUpstreams{}

	// generate resource urn with upstream from each evaluator
	for _, evaluatorFunc := range g.evaluatorFuncs {
		query := evaluatorFunc(assets)
		if query == "" {
			return []string{}, nil
		}

		visited := map[string][]*bigquery.ResourceURNWithUpstreams{}
		paths := map[string]bool{}
		resources, err := g.generateResources(ctx, query, visited, paths)
		if err != nil {
			return nil, err
		}
		resourcesAccumulation = append(resourcesAccumulation, resources...)
	}

	// compiled all collcted resources and extract its urns
	resourceURNs := []string{}
	for _, r := range bigquery.ResourceURNWithUpstreamsList(resourcesAccumulation).FlattenUnique() {
		resourceURNs = append(resourceURNs, r.ResourceURN.URN())
	}
	return resourceURNs, nil
}

func (g BQUpstreamGenerator) generateResources(ctx context.Context, query string, visited map[string][]*bigquery.ResourceURNWithUpstreams, paths map[string]bool) ([]*bigquery.ResourceURNWithUpstreams, error) {
	me := errors.NewMultiError("generate resources errors")
	resourceURNs := g.parserFunc(query)
	resources := []*bigquery.ResourceURNWithUpstreams{}
	if len(resourceURNs) == 0 {
		return resources, nil
	}

	urnToQuery, err := g.extractorFunc(ctx, resourceURNs)
	if err != nil {
		g.logger.Error(fmt.Sprintf("error when extract ddl resource: %s", err.Error()))
		return resources, nil
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
			upstreamResources, err := g.generateResources(ctx, query, visited, paths)
			visited[resourceURN] = upstreamResources
			me.Append(err)
			delete(paths, resourceURN)
		}
		resource.Upstreams = visited[resourceURN]
		resources = append(resources, resource)
	}

	return resources, me.ToErr()
}

func NewBQUpstreamGenerator(logger log.Logger, parserFunc ParserFunc, bqExtractorFunc BQExtractorFunc, evaluatorFuncs ...EvalAssetFunc) (*BQUpstreamGenerator, error) {
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
	if len(evaluatorFuncs) == 0 {
		me.Append(fmt.Errorf("evaluatorFuncs is needed"))
	}
	for _, evaluatorFunc := range evaluatorFuncs {
		if evaluatorFunc == nil {
			me.Append(fmt.Errorf("evaluatorFunc is nil"))
			break
		}
	}
	if me.ToErr() != nil {
		return nil, me.ToErr()
	}

	return &BQUpstreamGenerator{
		logger:         logger,
		parserFunc:     parserFunc,
		extractorFunc:  bqExtractorDecorator(logger, bqExtractorFunc),
		evaluatorFuncs: evaluatorFuncs,
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
