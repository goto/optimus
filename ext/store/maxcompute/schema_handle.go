package maxcompute

import (
	"github.com/goto/optimus/core/resource"
	"github.com/goto/optimus/internal/errors"
)

type SchemaInteractor interface {
	Exists() (bool, error)
}

type SchemaHandle struct {
	schema           McSchema
	schemaInteractor SchemaInteractor
}

func (sh SchemaHandle) Create(res *resource.Resource) error {
	schemaDetails, err := ConvertSpecToSchemaDetails(res)
	if err != nil {
		return err
	}

	projectSchema, err := ProjectSchemaFor(schemaDetails.Name)
	if err != nil {
		return errors.AddErrContext(err, EntitySchema, "invalid schema name "+res.Name().String())
	}

	if err := sh.schema.Create(projectSchema.Schema, true, schemaDetails.Description); err != nil {
		return errors.InternalError(EntitySchema, "error while creating schema on maxcompute", err)
	}

	return nil
}

func (sh SchemaHandle) Update(res *resource.Resource) error {
	schemaDetails, err := ConvertSpecToSchemaDetails(res)
	if err != nil {
		return err
	}

	projectSchema, err := ProjectSchemaFor(schemaDetails.Name)
	if err != nil {
		return errors.AddErrContext(err, EntitySchema, "invalid schema name "+res.Name().String())
	}

	if err := sh.schema.Create(projectSchema.Schema, false, schemaDetails.Description); err != nil {
		return errors.InternalError(EntitySchema, "error while updating schema on maxcompute", err)
	}

	return nil
}

func (sh SchemaHandle) Exists(_ string) bool {
	exists, _ := sh.schemaInteractor.Exists()
	return exists
}

func NewSchemaHandle(schema McSchema, schemaInteractor SchemaInteractor) *SchemaHandle {
	return &SchemaHandle{
		schema:           schema,
		schemaInteractor: schemaInteractor,
	}
}
