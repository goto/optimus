package bigquery

import (
	"fmt"
	"regexp"

	"github.com/goto/optimus/internal/errors"
)

var bqResourceURNRegex = regexp.MustCompile(`bigquery:\/\/([^:]+):([^\.]+)\.(.+)`)

type ResourceURN struct {
	project string
	dataset string
	name    string
}

func NewResourceURNFromString(urn string) (ResourceURN, error) {
	const lengthMatchedString = 3
	matchedString := bqResourceURNRegex.FindStringSubmatch(urn)
	if len(matchedString) != lengthMatchedString+1 {
		return ResourceURN{}, fmt.Errorf("urn %s can't be parsed to bigquery urn format", urn)
	}
	project, dataset, table := matchedString[1], matchedString[2], matchedString[3]
	return ResourceURN{project: project, dataset: dataset, name: table}, nil
}

func NewResourceURN(project, dataset, name string) (ResourceURN, error) {
	me := errors.NewMultiError("resource urn constructor errors")
	if project == "" {
		me.Append(fmt.Errorf("project is empty"))
	}
	if dataset == "" {
		me.Append(fmt.Errorf("dataset is empty"))
	}
	if name == "" {
		me.Append(fmt.Errorf("name is empty"))
	}

	if len(me.Errors) > 0 {
		return ResourceURN{}, me.ToErr()
	}

	return ResourceURN{
		project: project,
		dataset: dataset,
		name:    name,
	}, nil
}

func (n ResourceURN) URN() string {
	return "bigquery://" + fmt.Sprintf("%s:%s.%s", n.project, n.dataset, n.name)
}

func (n ResourceURN) Project() string {
	return n.project
}

func (n ResourceURN) Dataset() string {
	return n.dataset
}

func (n ResourceURN) Name() string {
	return n.name
}

type ResourceURNWithUpstreams struct {
	ResourceURN ResourceURN
	Upstreams   []*ResourceURNWithUpstreams
}

type ResourceURNWithUpstreamsList []*ResourceURNWithUpstreams

func (rs ResourceURNWithUpstreamsList) FlattenUnique() []*ResourceURNWithUpstreams {
	var output []*ResourceURNWithUpstreams
	for _, r := range rs {
		if r == nil {
			continue
		}
		newResource := *r
		newResource.Upstreams = nil
		nested := ResourceURNWithUpstreamsList(r.Upstreams).FlattenUnique()
		output = append(output, &newResource)
		output = append(output, nested...)
	}

	return ResourceURNWithUpstreamsList(output).unique()
}

func (rs ResourceURNWithUpstreamsList) unique() ResourceURNWithUpstreamsList {
	mapUnique := map[ResourceURN]*ResourceURNWithUpstreams{}
	for _, r := range rs {
		mapUnique[r.ResourceURN] = r
	}

	output := make([]*ResourceURNWithUpstreams, len(mapUnique))
	i := 0
	for _, u := range mapUnique {
		output[i] = u
		i++
	}
	return output
}

type ProjectDataset struct {
	Project string
	Dataset string
}

type ResourceURNs []ResourceURN

func (n ResourceURNs) GroupByProjectDataset() map[ProjectDataset][]string {
	output := make(map[ProjectDataset][]string)

	for _, resourceURN := range n {
		pd := ProjectDataset{Project: resourceURN.project, Dataset: resourceURN.dataset}
		if _, ok := output[pd]; !ok {
			output[pd] = []string{}
		}
		output[pd] = append(output[pd], resourceURN.name)
	}

	return output
}
