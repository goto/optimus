package maxcompute

import (
	"github.com/goto/optimus/core/resource"
	"github.com/goto/optimus/internal/errors"
)

type SchemaDetails struct {
	Name        resource.Name `mapstructure:"name,omitempty"`
	Project     string        `mapstructure:"project,omitempty"`
	Database    string        `mapstructure:"database,omitempty"`
	Description string        `mapstructure:"description,omitempty"`
}

func (v *SchemaDetails) Validate() error {
	if v.Name == "" {
		return errors.InvalidArgument(EntityView, "resource name is empty")
	}

	if v.Project == "" {
		return errors.InvalidArgument(EntitySchema, "project name is empty for "+v.Name.String())
	}

	if v.Database == "" {
		return errors.InvalidArgument(EntitySchema, "database name is empty for "+v.Name.String())
	}

	return nil
}

func ConvertSpecToSchemaDetails(res *resource.Resource) (*SchemaDetails, error) {
	schemaDetails, err := ConvertSpecTo[SchemaDetails](res)
	if err != nil {
		return nil, errors.AddErrContext(err, EntitySchema, "invalid schema spec for "+res.Name().String())
	}
	schemaDetails.Name = res.Name()
	return schemaDetails, nil
}
