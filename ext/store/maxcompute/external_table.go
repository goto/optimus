package maxcompute

import (
	"fmt"
	"strings"

	"github.com/aliyun/aliyun-odps-go-sdk/odps"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/common"
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
	Delete(tableName string, ifExists bool) error
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
	p, tableName, err := getCompleteComponentName(res)
	if err != nil {
		return err
	}

	tSchema, err := buildExternalTableSchema(table, res.FullName())
	table.Name = tableName
	if err != nil {
		return errors.AddErrContext(err, EntityExternalTable, "failed to build table schema to create for "+res.FullName())
	}
	e.mcSQLExecutor.SetCurrentSchemaName(p.Schema)
	if !(tSchema.StorageHandler == CSVHandler || tSchema.StorageHandler == TSVHandler) {
		return e.createOtherTypeExternalTable(p, table, tSchema)
	}

	err = e.mcExternalTable.CreateExternal(tSchema, false, table.Source.SerdeProperties, table.Source.Jars, table.Hints, nil)
	if err != nil {
		if strings.Contains(err.Error(), "Table or view already exists") {
			return errors.AlreadyExists(EntityExternalTable, "external table already exists on maxcompute: "+res.FullName())
		}
		return errors.InternalError(EntityExternalTable, "error while creating table on maxcompute", err)
	}
	return nil
}

func (e ExternalTableHandle) createOtherTypeExternalTable(ps ProjectSchema, et *ExternalTable, tSchema tableschema.TableSchema) error {
	sql, err := ToOtherExternalSQLString(ps.Project, ps.Schema, et.Source.SerdeProperties, tSchema, et.Source.SourceType)
	if err != nil {
		return err
	}

	et.Hints["odps.namespace.schema"] = "true"
	inst, err := e.mcSQLExecutor.ExecSQlWithHints(sql, et.Hints)
	if err != nil {
		return errors.AddErrContext(err, EntityExternalTable, "failed to create sql task to create view "+et.FullName())
	}

	if err = inst.WaitForSuccess(); err != nil {
		if strings.Contains(err.Error(), "Table or view already exists") {
			return errors.AlreadyExists(EntityExternalTable, "view already exists on maxcompute: "+et.FullName())
		}
		return errors.InternalError(EntityExternalTable, "failed to create external table "+et.FullName(), err)
	}

	return nil
}

func (e ExternalTableHandle) Update(res *resource.Resource) error {
	err := e.mcExternalTable.Delete(res.FullName(), true)
	if err != nil {
		return err
	}
	return e.Create(res)
}

func (e ExternalTableHandle) Exists(tableName string) bool {
	_, err := e.mcExternalTable.BatchLoadTables([]string{tableName})
	return err == nil
}

func NewExternalTableHandle(mcSQLExecutor McSQLExecutor, mcSchema McSchema, mcExternalTable McExternalTable) *ExternalTableHandle {
	return &ExternalTableHandle{mcSQLExecutor: mcSQLExecutor, mcSchema: mcSchema, mcExternalTable: mcExternalTable}
}

func getLocation(source *ExternalSource, tableName string) string {
	if !strings.EqualFold(source.SourceType, GoogleSheet) {
		return source.Location
	}
	l, _ := strings.CutSuffix(source.Location, "/")
	return l + "/" + tableName + "/"
}

func buildExternalTableSchema(t *ExternalTable, tableName string) (tableschema.TableSchema, error) {
	handler := handlerForFormat(t.Source.SourceType)

	builder := tableschema.NewSchemaBuilder()
	builder.
		Name(t.Name.String()).
		Comment(t.Description).
		StorageHandler(handler).
		Location(getLocation(t.Source, tableName)).
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

func ToOtherExternalSQLString(projectName string, schemaName string, serdeProperties map[string]string, schema tableschema.TableSchema, format string) (string, error) {
	baseSQL, err := schema.ToBaseSQLString(projectName, schemaName, true, true)
	if err != nil {
		return "", errors.InternalError(EntityExternalTable, "failed to generate external SQL string", err)
	}

	var builder strings.Builder
	builder.WriteString(baseSQL)

	builder.WriteString(fmt.Sprintf("\nrow format serde '%s'\n", schema.StorageHandler))

	if len(serdeProperties) > 0 {
		builder.WriteString("with serdeproperties(")
		i, n := 0, len(serdeProperties)

		for key, value := range serdeProperties {
			builder.WriteString(fmt.Sprintf("%s=%s", common.QuoteString(key), common.QuoteString(value)))
			i += 1
			if i < n {
				builder.WriteString(", ")
			}
		}

		builder.WriteString(")\n")
	}

	fileFmt := format
	if strings.EqualFold(format, JSON) {
		fileFmt = TxtFile
	}

	builder.WriteString(fmt.Sprintf("stored AS %s\n", fileFmt))
	builder.WriteString(fmt.Sprintf("location '%s'\n", schema.Location))

	if len(schema.TblProperties) > 0 {
		builder.WriteString("TBLPROPERTIES (")
		i := 0
		sortColsNum := len(schema.TblProperties)

		for k, v := range schema.TblProperties {
			builder.WriteString(fmt.Sprintf("%s=%s", common.QuoteString(k), common.QuoteString(v)))
			i++
			if i < sortColsNum {
				builder.WriteString(", ")
			}
		}

		builder.WriteString(")\n")
	}

	if schema.Lifecycle > 0 {
		builder.WriteString(fmt.Sprintf("lifecycle %d", schema.Lifecycle))
	}

	builder.WriteRune(';')
	return builder.String(), nil
}
