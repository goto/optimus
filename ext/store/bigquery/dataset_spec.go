package bigquery

import (
	"fmt"
	"regexp"

	"github.com/mitchellh/mapstructure"

	"github.com/goto/optimus/core/resource"
	"github.com/goto/optimus/internal/errors"
)

const (
	EntityDataset = "dataset"

	DatesetNameSections = 2
	TableNameSections   = 3
)

var (
	validProjectName = regexp.MustCompile(`^[a-z][a-z0-9-]{4,28}[a-z0-9]$`)
	validDatasetName = regexp.MustCompile(`^[a-zA-Z0-9_]+$`)
	validTableName   = regexp.MustCompile(`^[a-zA-Z0-9_-]+$`)

	validResourceName = regexp.MustCompile(`^[a-z][a-zA-Z0-9._-]+$`)
)

type DatasetDetails struct {
	Name resource.Name

	Description string                 `mapstructure:"description,omitempty"`
	ExtraConfig map[string]interface{} `mapstructure:",remain"`
}

func (d DatasetDetails) FullName() string {
	return d.Name.String()
}

func (DatasetDetails) Validate() error {
	return nil
}

func ConvertSpecTo[T DatasetDetails | Table | View | ExternalTable](res *resource.Resource) (*T, error) {
	var spec T
	if err := mapstructure.Decode(res.Spec(), &spec); err != nil {
		msg := fmt.Sprintf("%s: not able to decode spec for %s", err, res.FullName())
		return nil, errors.InvalidArgument(resource.EntityResource, msg)
	}
	return &spec, nil
}

type Dataset struct {
	Project     string
	DatasetName string
}

func DataSetFrom(project, datasetName string) (Dataset, error) {
	if project == "" {
		return Dataset{}, errors.InvalidArgument(EntityDataset, "bigquery project name is empty")
	}

	if datasetName == "" {
		return Dataset{}, errors.InvalidArgument(EntityDataset, "bigquery dataset name is empty")
	}

	return Dataset{
		Project:     project,
		DatasetName: datasetName,
	}, nil
}

func (d Dataset) FullName() string {
	return d.Project + "." + d.DatasetName
}

func DataSetFor(name resource.Name) (Dataset, error) {
	sections := name.Sections()
	if len(sections) < DatesetNameSections {
		return Dataset{}, errors.InvalidArgument(EntityDataset, "invalid dataset name: "+name.String())
	}

	return DataSetFrom(sections[0], sections[1])
}

func ResourceNameFor(name resource.Name, kind string) (string, error) {
	sections := name.Sections()
	var strName string
	if kind == KindDataset {
		if len(sections) < DatesetNameSections {
			return "", errors.InvalidArgument(resource.EntityResource, "invalid resource name: "+name.String())
		}
		strName = sections[1]
	} else {
		if len(sections) < TableNameSections {
			return "", errors.InvalidArgument(resource.EntityResource, "invalid resource name: "+name.String())
		}
		strName = sections[2]
	}

	if strName == "" {
		return "", errors.InvalidArgument(resource.EntityResource, "invalid resource name: "+name.String())
	}
	return strName, nil
}

func ValidateName(res *resource.Resource) error {
	if res.Version() == resource.ResourceSpecV2 {
		return validateNameV2(res)
	}

	return validateNameV1(res)
}

func validateNameV1(res *resource.Resource) error {
	sections := res.Name().Sections()
	if len(sections) < DatesetNameSections {
		return errors.InvalidArgument(resource.EntityResource, "invalid sections in name: "+res.FullName())
	}

	if !validProjectName.MatchString(sections[0]) {
		return errors.InvalidArgument(resource.EntityResource, "invalid character in project name "+res.FullName())
	}

	if !validDatasetName.MatchString(sections[1]) {
		return errors.InvalidArgument(resource.EntityResource, "invalid character in dataset name "+res.FullName())
	}

	if res.Kind() != KindDataset {
		if len(sections) != TableNameSections {
			return errors.InvalidArgument(resource.EntityResource, "invalid resource name sections: "+res.FullName())
		}

		if !validTableName.MatchString(sections[2]) {
			return errors.InvalidArgument(resource.EntityResource, "invalid character in resource name "+res.FullName())
		}
	}
	return nil
}

func validateNameV2(res *resource.Resource) error {
	if !validResourceName.MatchString(res.FullName()) {
		return errors.InvalidArgument(resource.EntityResource, "invalid character in resource name "+res.FullName())
	}

	var spec URNComponent
	if err := mapstructure.Decode(res.Spec(), &spec); err != nil {
		return errors.InvalidArgument(resource.EntityResource, "not able to decode spec")
	}

	if !validProjectName.MatchString(spec.Project) {
		return errors.InvalidArgument(resource.EntityResource, fmt.Sprintf("invalid character in project name: %s from %s ", spec.Project, res.FullName()))
	}

	if !validDatasetName.MatchString(spec.Dataset) {
		return errors.InvalidArgument(resource.EntityResource, fmt.Sprintf("invalid character in dataset name: %s from %s ", spec.Dataset, res.FullName()))
	}

	if res.Kind() != KindDataset && !validTableName.MatchString(spec.Name) {
		return errors.InvalidArgument(resource.EntityResource, fmt.Sprintf("invalid character in table/view name: %s from %s ", spec.Name, res.FullName()))
	}

	return nil
}

type URNComponent struct {
	Project string `mapstructure:"project"`
	Dataset string `mapstructure:"dataset"`
	Name    string `mapstructure:"name"`
}

func URNFor(res *resource.Resource) (resource.URN, error) {
	if res.Version() == resource.ResourceSpecV2 {
		return urnForV2(res)
	}

	return urnForV1(res)
}

func urnForV1(res *resource.Resource) (resource.URN, error) {
	dataset, err := DataSetFor(res.Name())
	if err != nil {
		return resource.ZeroURN(), err
	}

	if res.Kind() == KindDataset {
		name := dataset.Project + ":" + dataset.DatasetName
		return resource.NewURN(resource.Bigquery.String(), name)
	}

	resourceName, err := ResourceNameFor(res.Name(), res.Kind())
	if err != nil {
		return resource.ZeroURN(), err
	}

	name := dataset.Project + ":" + dataset.DatasetName + "." + resourceName
	return resource.NewURN(resource.Bigquery.String(), name)
}

func urnForV2(res *resource.Resource) (resource.URN, error) {
	var spec URNComponent
	if err := mapstructure.Decode(res.Spec(), &spec); err != nil {
		return resource.ZeroURN(), errors.InvalidArgument(resource.EntityResource, "not able to decode spec")
	}

	name := spec.Project + ":" + spec.Dataset
	if res.Kind() != KindDataset {
		name = name + "." + spec.Name
	}

	return resource.NewURN(resource.Bigquery.String(), name)
}
