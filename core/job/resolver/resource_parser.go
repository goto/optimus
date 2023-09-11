package resolver

import (
	"context"
	"fmt"

	"github.com/goto/optimus/core/resource"
	"github.com/goto/optimus/internal/errors"
)

type (
	// ParserFunc parses rawResource to list of resource
	ParserFunc func(rawResource string) []*resource.Resource
	// ExtractorFunc extracts the rawSources from given list of resource
	ExtractorFunc func([]*resource.Resource) map[string]string
)

type resourceParser struct {
	parserFunc    ParserFunc
	extractorFunc ExtractorFunc
}

func (rp *resourceParser) GenerateResources(ctx context.Context, rawResource string) ([]*resource.Resource, error) {
	return rp.generateResources(ctx, rawResource, map[string][]*resource.Resource{}, map[string]bool{})
}

func (rp *resourceParser) generateResources(ctx context.Context, rawResource string, visited map[string][]*resource.Resource, paths map[string]bool) ([]*resource.Resource, error) {
	errs := errors.NewMultiError("generate resources")
	resources := rp.parserFunc(rawResource)
	urnToRawResource := rp.extractorFunc(resources)

	for i, r := range resources {
		if paths[r.URN()] {
			errs.Append(fmt.Errorf("circular reference is detected"))
			continue
		}
		if visited[r.URN()] == nil {
			rawResource := urnToRawResource[r.URN()]
			paths[r.URN()] = true
			upstreamResources, err := rp.generateResources(ctx, rawResource, visited, paths)
			visited[r.URN()] = upstreamResources
			errs.Append(err)
			delete(paths, r.URN())
		}
		resources[i].Upstreams = visited[r.URN()]
	}

	return resources, errs.ToErr()
}
