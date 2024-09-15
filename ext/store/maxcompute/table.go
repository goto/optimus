package maxcompute

import (
	"github.com/aliyun/aliyun-odps-go-sdk/odps"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/tableschema"

	"github.com/goto/optimus/core/resource"
	"github.com/goto/optimus/internal/errors"
	"github.com/goto/optimus/internal/utils"
)

func Create(odps *odps.Odps, res *resource.Resource) error {
	table, err := ConvertSpecTo[Table](res)
	if err != nil {
		return err
	}

	schema, err := buildTableSchema(table)
	if err != nil {
		return err
	}

	// We can use  odps.ExecSQl() to run sql queries on maxcompute for some operation if
	// not supported by sdk, but it will require some more work from our side

	tablesIns := odps.Tables()
	err = tablesIns.Create(schema, true, table.Hints, nil)
	if err != nil {
		// TODO: Check for the error type and handle it properly
		return errors.Wrap(EntityTable, "error while creating table on maxcompute", err)
	}
	return nil
}

func buildTableSchema(t *Table) (tableschema.TableSchema, error) {
	b1 := tableschema.NewSchemaBuilder()
	builder := &b1 // Builder returns non-pointer type
	builder.
		Name(t.Name.String()).
		Comment(t.Description)

	// We can populate columns and partition columns
	// Currently SDK does not allow setting up Clustering
	// We accept the config, but we cannot pass it to the sdk
	err := populateColumns(t, builder)
	if err != nil {
		return tableschema.TableSchema{}, err
	}

	return builder.Build(), nil
}

func populateColumns(t *Table, schemaBuilder *tableschema.SchemaBuilder) error {
	mu := errors.NewMultiError("column creation")
	partitionColNames := utils.ListToMap(t.Partition.Columns)

	for _, field := range t.Schema {
		column, err := field.ToColumn()
		if err != nil {
			mu.Append(err)
			continue
		}

		if _, ok := partitionColNames[field.Name]; ok {
			schemaBuilder.Column(column)
		} else {
			schemaBuilder.PartitionColumn(column)
		}
	}

	return mu.ToErr()
}
