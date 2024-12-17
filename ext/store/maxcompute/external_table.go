package maxcompute

import (
	"strings"

	"github.com/aliyun/aliyun-odps-go-sdk/odps"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/tableschema"

	"github.com/goto/optimus/core/resource"
	"github.com/goto/optimus/internal/errors"
)

type McExternalTable interface {
	CreateExternal(
		schema tableschema.TableSchema,
		createIfNotExists bool,
		serdeProperties map[string]string,
		jars []string,
		hints, alias map[string]string,
	) error
	BatchLoadTables(tableNames []string) ([]*odps.Table, error)
}

type ExternalTableHandle struct {
	mcSQLExecutor   McSQLExecutor
	mcSchema        McSchema
	mcExternalTable McExternalTable
}

func (e ExternalTableHandle) Create(res *resource.Resource) error {
	table, err := ConvertSpecTo[ExternalTable](res)
	if err != nil {
		return err
	}

	_, table.Name, err = getCompleteComponentName(res)
	if err != nil {
		return err
	}

	//if err := e.mcSchema.Create(e.mcSQLExecutor.CurrentSchemaName(), true, ""); err != nil {
	//	return errors.InternalError(EntitySchema, "error while creating schema on maxcompute", err)
	//}

	tableSchema, err := buildExternalTableSchema(table)
	if err != nil {
		return errors.AddErrContext(err, EntityExternalTable, "failed to build table schema to create for "+res.FullName())
	}

	err = e.mcExternalTable.CreateExternal(tableSchema, false, table.Source.SerdeProperties, table.Source.Jars, table.Hints, nil)
	if err != nil {
		if strings.Contains(err.Error(), "Table or view already exists") {
			return errors.AlreadyExists(EntityExternalTable, "external table already exists on maxcompute: "+res.FullName())
		}
		return errors.InternalError(EntityExternalTable, "error while creating table on maxcompute", err)
	}
	return nil
}

func (ExternalTableHandle) Update(_ *resource.Resource) error {
	// TODO implement me
	panic("implement me")
}

func (e ExternalTableHandle) Exists(tableName string) bool {
	_, err := e.mcExternalTable.BatchLoadTables([]string{tableName})
	return err == nil
}

func NewExternalTableHandle(mcSQLExecutor McSQLExecutor, mcSchema McSchema, mcExternalTable McExternalTable) *ExternalTableHandle {
	return &ExternalTableHandle{mcSQLExecutor: mcSQLExecutor, mcSchema: mcSchema, mcExternalTable: mcExternalTable}
}

func buildExternalTableSchema(t *ExternalTable) (tableschema.TableSchema, error) {
	handler := handlerForFormat(t.Source.SourceType)

	builder := tableschema.NewSchemaBuilder()
	builder.
		Name(t.Name.String()).
		Comment(t.Description).
		StorageHandler(handler).
		Location(t.Source.Location).
		TblProperties(t.Source.TableProperties)

	err := externalTableColumns(t, builder)
	if err != nil {
		return tableschema.TableSchema{}, err
	}

	return builder.Build(), nil
}

func externalTableColumns(t *ExternalTable, schemaBuilder *tableschema.SchemaBuilder) error {
	partitionColNames := map[string]struct{}{}

	return t.Schema.ToMaxComputeColumns(partitionColNames, nil, schemaBuilder)
}
