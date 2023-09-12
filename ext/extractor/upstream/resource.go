package upstream

import (
	"fmt"
	"strings"
)

type Resource struct {
	Project   string
	Dataset   string
	Name      string
	Upstreams []*Resource
}

func FromDestinationURN(destinationURN string) (*Resource, error) { // as of now only support bigquery
	destination := strings.TrimPrefix(destinationURN, "bigquery://")
	splitDestination := strings.Split(destination, ":")
	if len(splitDestination) != 2 {
		return nil, fmt.Errorf("cannot get project from destination [%s]", destination)
	}

	project, datasetTable := splitDestination[0], splitDestination[1]

	splitDataset := strings.Split(datasetTable, ".")
	if len(splitDataset) != 2 {
		return nil, fmt.Errorf("cannot get dataset and table from [%s]", datasetTable)
	}

	return &Resource{
		Project: project,
		Dataset: splitDataset[0],
		Name:    splitDataset[1],
	}, nil
}

func (r Resource) URN() string { // as of now only support bigquery
	return "bigquery://" + r.Project + ":" + r.Dataset + "." + r.Name
}

type Resources []*Resource

func (r Resources) GetWithoutResource(resourceToIgnore *Resource) []*Resource {
	var output []*Resource
	for _, resource := range r {
		if resourceToIgnore != nil && resource.URN() == resourceToIgnore.URN() {
			continue
		}
		output = append(output, resource)
	}
	return output
}

func (r Resources) GetUnique() []*Resource {
	ref := make(map[string]*Resource)
	for _, resource := range r {
		ref[resource.URN()] = resource
	}

	var output []*Resource
	for _, r := range ref {
		output = append(output, r)
	}
	return output
}

func (r Resources) GroupResources() map[string][]string {
	output := make(map[string][]string)

	for _, resource := range r {
		key := resource.Project + "." + resource.Dataset
		if _, ok := output[key]; !ok {
			output[key] = []string{}
		}
		output[key] = append(output[key], resource.Name)
	}

	return output
}

func (r Resources) GetFlattened() []*Resource {
	var output []*Resource
	for _, u := range r {
		if u == nil {
			continue
		}
		nested := Resources(u.Upstreams).GetFlattened()
		u.Upstreams = nil
		output = append(output, u)
		output = append(output, nested...)
	}

	return output
}
