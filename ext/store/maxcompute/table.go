package maxcompute

import (
	"github.com/aliyun/aliyun-odps-go-sdk/odps/tableschema"
	"strings"

	"github.com/goto/optimus/core/resource"
	"github.com/goto/optimus/internal/errors"
	"github.com/goto/optimus/internal/utils"
)

type McTable interface {
	Create(schema tableschema.TableSchema, createIfNotExists bool, hints, alias map[string]string) error
}

type TableHandle struct {
	mcTable McTable
}

func (t TableHandle) Create(res *resource.Resource) error {
	table, err := ConvertSpecTo[Table](res)
	if err != nil {
		return err
	}
	table.Name = res.Name()

	schema, err := buildTableSchema(table)
	if err != nil {
		return errors.Wrap(EntityTable, "failed to build table schema to create for "+res.FullName(), err)
	}

	// We can use  odps.ExecSQl() to run sql queries on maxcompute for some operation if
	// not supported by sdk, but it will require some more work from our side

	err = t.mcTable.Create(schema, false, table.Hints, nil)
	if err != nil {
		if strings.Contains(err.Error(), "Table or view already exists") {
			return errors.AlreadyExists(EntityTable, "table already exists on maxcompute: "+res.FullName())
		}
		return errors.Wrap(EntityTable, "error while creating table on maxcompute", err)
	}
	return nil
}

func buildTableSchema(t *Table) (tableschema.TableSchema, error) {
	builder := tableschema.NewSchemaBuilder()
	builder.
		Name(t.Name.String()).
		Comment(t.Description).
		Lifecycle(t.Lifecycle)

	// We can populate columns and partition columns
	// Currently SDK does not allow setting up Clustering
	// We accept the config, but we cannot pass it to the sdk
	err := populateColumns(t, &builder)
	if err != nil {
		return tableschema.TableSchema{}, err
	}

	return builder.Build(), nil
}

func populateColumns(t *Table, schemaBuilder *tableschema.SchemaBuilder) error {
	mu := errors.NewMultiError("column creation")
	partitionColNames := map[string]struct{}{}
	if t.Partition != nil {
		partitionColNames = utils.ListToMap(t.Partition.Columns)
	}

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

func NewTableHandle(mc McTable) *TableHandle {
	return &TableHandle{mcTable: mc}
}