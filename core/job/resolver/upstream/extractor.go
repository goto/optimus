package upstream

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/googleapis/google-cloud-go-testing/bigquery/bqiface"
)

type Extractor struct {
	mutex  *sync.Mutex
	client bqiface.Client

	schemaToUpstreams map[string][]*Upstream
}

func NewExtractor(client bqiface.Client) (*Extractor, error) {
	if client == nil {
		return nil, errors.New("client is nil")
	}

	return &Extractor{
		mutex:             &sync.Mutex{},
		client:            client,
		schemaToUpstreams: make(map[string][]*Upstream),
	}, nil
}

func (e *Extractor) ExtractUpstreams(ctx context.Context, query string, resourcesToIgnore []Resource) ([]*Upstream, error) {
	ignoredResources := make(map[Resource]bool)
	for _, r := range resourcesToIgnore {
		ignoredResources[r] = true
	}

	metResource := make(map[Resource]bool)
	return e.extractUpstreamsFromQuery(ctx, query, ignoredResources, metResource, ParseTopLevelUpstreamsFromQuery)
}

func (e *Extractor) extractUpstreamsFromQuery(
	ctx context.Context, query string,
	ignoredResources, metResource map[Resource]bool,
	parseFn QueryParser,
) ([]*Upstream, error) {
	upstreamResources := parseFn(query)

	uniqueUpstreamResources := UniqueFilterResources(upstreamResources)

	filteredUpstreamResources := FilterResources(uniqueUpstreamResources, func(r Resource) bool { return ignoredResources[r] })

	resourceGroups := GroupResources(filteredUpstreamResources)

	var output []*Upstream
	var errorMessages []string

	for _, group := range resourceGroups {
		schemas, err := ReadSchemasUnderGroup(ctx, e.client, group)
		if err != nil {
			errorMessages = append(errorMessages, err.Error())
		}

		nestedable, rest := splitNestedableFromRest(schemas)

		restsNodes := convertSchemasToNodes(rest)
		output = append(output, restsNodes...)

		nestedNodes, err := e.extractNestedNodes(ctx, nestedable, ignoredResources, metResource)
		if err != nil {
			errorMessages = append(errorMessages, err.Error())
		}

		output = append(output, nestedNodes...)
	}

	if len(errorMessages) > 0 {
		return output, fmt.Errorf("error reading upstream: [%s]", strings.Join(errorMessages, ", "))
	}
	return output, nil
}

func (e *Extractor) extractNestedNodes(
	ctx context.Context, schemas []*Schema,
	ignoredResources, metResource map[Resource]bool,
) ([]*Upstream, error) {
	var output []*Upstream
	var errorMessages []string

	for _, sch := range schemas {
		if metResource[sch.Resource] {
			msg := fmt.Sprintf("circular reference is detected: [%s]", e.getCircularURNs(metResource))
			errorMessages = append(errorMessages, msg)
			continue
		}
		metResource[sch.Resource] = true

		nodes, err := e.getNodes(ctx, sch, ignoredResources, metResource)
		if err != nil {
			errorMessages = append(errorMessages, err.Error())
		}

		output = append(output, &Upstream{
			Resource:  sch.Resource,
			Upstreams: nodes,
		})
	}

	if len(errorMessages) > 0 {
		return output, fmt.Errorf("error getting nested upstream: [%s]", strings.Join(errorMessages, ", "))
	}
	return output, nil
}

func (e *Extractor) getNodes(
	ctx context.Context, schema *Schema,
	ignoredResources, metResource map[Resource]bool,
) ([]*Upstream, error) {
	key := schema.Resource.URN()

	e.mutex.Lock()
	existingNodes, ok := e.schemaToUpstreams[key]
	e.mutex.Unlock()

	if ok {
		return existingNodes, nil
	}

	nodes, err := e.extractUpstreamsFromQuery(ctx, schema.DDL, ignoredResources, metResource, ParseNestedUpsreamsFromDDL)

	e.mutex.Lock()
	e.schemaToUpstreams[key] = nodes
	e.mutex.Unlock()

	return nodes, err
}

func (*Extractor) getCircularURNs(metResource map[Resource]bool) string {
	var urns []string
	for resource := range metResource {
		urns = append(urns, resource.URN())
	}

	return strings.Join(urns, ", ")
}
