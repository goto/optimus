package upstream

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/googleapis/google-cloud-go-testing/bigquery/bqiface"
)

type Extractor struct {
	client bqiface.Client

	resourceURNToUpstreams map[string][]*Resource
	parserFunc             QueryParser
}

func NewExtractor(client bqiface.Client, parserFunc QueryParser) (*Extractor, error) {
	if client == nil {
		return nil, errors.New("client is nil")
	}

	if parserFunc == nil {
		return nil, errors.New("parser function is nil")
	}

	return &Extractor{
		client:                 client,
		resourceURNToUpstreams: make(map[string][]*Resource),
		parserFunc:             parserFunc,
	}, nil
}

func (e *Extractor) ExtractUpstreams(ctx context.Context, query string, resourceDestination *Resource) ([]*Resource, error) {
	return e.extractResourcesFromQuery(ctx, query, resourceDestination, map[string]bool{})
}

func (e *Extractor) extractResourcesFromQuery(ctx context.Context, query string, resourceDestination *Resource, metResource map[string]bool) ([]*Resource, error) {
	resources := Resources(e.parserFunc(query))
	uniqueResources := Resources(resources).GetUnique()
	filteredResources := Resources(uniqueResources).GetWithoutResource(resourceDestination)
	resourceGroups := Resources(filteredResources).GroupResources()

	var output []*Resource
	var errorMessages []string

	for _, group := range resourceGroups {
		schemas, err := ReadSchemasUnderGroup(ctx, e.client, group)
		if err != nil {
			errorMessages = append(errorMessages, err.Error())
		}

		nestedtableSchemas, rest := Schemas(schemas).SplitSchemasByType(View)
		output = append(output, rest.ToResources()...)

		nestedResources, err := e.extractNestedSchemas(ctx, nestedtableSchemas, resourceDestination, metResource)
		if err != nil {
			errorMessages = append(errorMessages, err.Error())
		}

		output = append(output, nestedResources...)
	}

	if len(errorMessages) > 0 {
		return output, fmt.Errorf("error reading upstream: [%s]", strings.Join(errorMessages, ", "))
	}
	return output, nil
}

func (e *Extractor) extractNestedSchemas(ctx context.Context, schemas []*Schema, resourceDestination *Resource, metResource map[string]bool) ([]*Resource, error) {
	var output []*Resource
	var errorMessages []string

	for _, sch := range schemas {
		if metResource[sch.Resource.URN()] {
			msg := fmt.Sprintf("circular reference is detected: [%s]", e.getCircularURNs(metResource))
			errorMessages = append(errorMessages, msg)
			continue
		}
		metResource[sch.Resource.URN()] = true

		upstreamResources, err := e.getResourcesFromSchema(ctx, sch, resourceDestination, metResource)
		if err != nil {
			errorMessages = append(errorMessages, err.Error())
		}

		resource := sch.Resource
		resource.Upstreams = upstreamResources
		output = append(output, &resource)
	}

	if len(errorMessages) > 0 {
		return output, fmt.Errorf("error getting nested upstream: [%s]", strings.Join(errorMessages, ", "))
	}
	return output, nil
}

func (e *Extractor) getResourcesFromSchema(ctx context.Context, schema *Schema, destinationResource *Resource, metResource map[string]bool) ([]*Resource, error) {
	resourceURN := schema.Resource.URN()

	if _, ok := e.resourceURNToUpstreams[resourceURN]; !ok {
		upstreamResources, err := e.extractResourcesFromQuery(ctx, schema.DDL, destinationResource, metResource)
		e.resourceURNToUpstreams[resourceURN] = upstreamResources
		return upstreamResources, err
	}

	return e.resourceURNToUpstreams[resourceURN], nil
}

func (*Extractor) getCircularURNs(metResource map[string]bool) string {
	var urns []string
	for resourceURN := range metResource {
		urns = append(urns, resourceURN)
	}

	return strings.Join(urns, ", ")
}
