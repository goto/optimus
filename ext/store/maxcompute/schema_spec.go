package maxcompute

import (
	"github.com/goto/optimus/core/resource"
	"github.com/goto/optimus/internal/errors"
)

type SchemaDetails struct {
	Name resource.Name `mapstructure:"name,omitempty"`

	Description string `mapstructure:"description,omitempty"`
}

func (v *SchemaDetails) Validate() error {
	if v.Name == "" {
		return errors.InvalidArgument(EntityView, "schema name is empty for "+v.Name.String())
	}

	if _, _, err := v.ProjectSchema(); err != nil {
		return errors.AddErrContext(err, EntitySchema, "invalid schema name "+v.Name.String())
	}

	return nil
}

func (v *SchemaDetails) ProjectSchema() (string, string, error) {
	urn, err := ProjectSchemaFor(v.Name)
	if err != nil {
		return "", "", errors.AddErrContext(err, EntitySchema, "invalid schema name "+v.Name.String())
	}
	return urn.Project, urn.Schema, nil
}

func ConvertSpecToSchemaDetails(res *resource.Resource) (*SchemaDetails, error) {
	schemaDetails, err := ConvertSpecTo[SchemaDetails](res)
	if err != nil {
		return nil, errors.AddErrContext(err, EntitySchema, "invalid schema spec for "+res.Name().String())
	}
	schemaDetails.Name = res.Name()
	return schemaDetails, nil
}
