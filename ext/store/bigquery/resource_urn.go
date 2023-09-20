package bigquery

import (
	"fmt"

	"github.com/goto/optimus/internal/errors"
)

type ResourceURN struct {
	project string
	dataset string
	name    string
}

func NewResourceURN(project, dataset, name string) (*ResourceURN, error) {
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
		return nil, me.ToErr()
	}

	return &ResourceURN{
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

type ProjectDataset struct {
	Project string
	Dataset string
}

type ResourceURNs []*ResourceURN

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
