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

func FromDestinationURN(destination string) (*Resource, error) {
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

func (r Resource) URN() string {
	return r.Project + "." + r.Dataset + "." + r.Name
}

type ResourceGroup struct {
	Project string
	Dataset string
	Names   []string
}

func (r ResourceGroup) URN() string {
	return r.Project + "." + r.Dataset
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

func (r Resources) GroupResources() []*ResourceGroup {
	ref := make(map[string]*ResourceGroup)

	for _, info := range r {
		key := info.Project + "." + info.Dataset

		if _, ok := ref[key]; ok {
			ref[key].Names = append(ref[key].Names, info.Name)
		} else {
			ref[key] = &ResourceGroup{
				Project: info.Project,
				Dataset: info.Dataset,
				Names:   []string{info.Name},
			}
		}
	}

	var output []*ResourceGroup
	for _, r := range ref {
		output = append(output, r)
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
