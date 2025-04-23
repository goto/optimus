package maxcompute

import (
	"context"
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
	mcSQLExecutor       McSQLExecutor
	mcSchema            McSchema
	mcExternalTable     McExternalTable
	maskingPolicyHandle TableMaskingPolicyHandle
	tenantDetailsGetter TenantDetailsGetter
}

func addQuoteSerde(serdeProperties map[string]string) map[string]string {
	if serdeProperties == nil {
		serdeProperties = make(map[string]string)
	}
	if _, ok := serdeProperties[UseQuoteSerde]; !ok {
		serdeProperties[UseQuoteSerde] = "true"
	}
	return serdeProperties
}

func (e ExternalTableHandle) Create(res *resource.Resource) error {
	et, err := ConvertSpecTo[ExternalTable](res)
	if err != nil {
		return err
	}

	location, err := e.getLocation(context.Background(), et, res)
	if err != nil {
		return errors.AddErrContext(err, EntityExternalTable, "failed to get source location for "+et.FullName())
	}
	tSchema, err := buildExternalTableSchema(et, location)
	if err != nil {
		return errors.AddErrContext(err, EntityExternalTable, "failed to build external table schema to create for "+et.FullName())
	}

	e.mcSQLExecutor.SetCurrentSchemaName(et.Database)
	if !(tSchema.StorageHandler == CSVHandler || tSchema.StorageHandler == TSVHandler) {
		return e.createOtherTypeExternalTable(et, tSchema)
	}

	err = e.mcExternalTable.CreateExternal(tSchema, false, addQuoteSerde(et.Source.SerdeProperties), et.Source.Jars, et.Hints, nil)
	if err != nil {
		if strings.Contains(err.Error(), "Table or view already exists") {
			return errors.AlreadyExists(EntityExternalTable, "external table already exists on maxcompute: "+et.FullName())
		}
		return errors.InternalError(EntityExternalTable, "error while creating external table on maxcompute", err)
	}

	err = e.maskingPolicyHandle.Process(et.Name, et.Schema)
	if err != nil {
		return errors.AddErrContext(err, EntityExternalTable, "failed to apply masking policy for "+et.FullName())
	}

	return nil
}

func (e ExternalTableHandle) createOtherTypeExternalTable(et *ExternalTable, tSchema tableschema.TableSchema) error {
	sql, err := ToOtherExternalSQLString(et.Project, et.Database, et.Source.SerdeProperties, tSchema, et.Source.SourceType)
	if err != nil {
		return err
	}
	if et.Hints == nil {
		et.Hints = make(map[string]string)
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

	err = e.maskingPolicyHandle.Process(et.Name, et.Schema)
	if err != nil {
		return errors.AddErrContext(err, EntityExternalTable, "failed to apply masking policy for "+et.FullName())
	}

	return nil
}

func (e ExternalTableHandle) Update(res *resource.Resource) error {
	et, err := ConvertSpecTo[ExternalTable](res)
	if err != nil {
		return err
	}

	e.mcSQLExecutor.SetCurrentSchemaName(et.Database)
	err = e.mcExternalTable.Delete(et.Name, true)
	if err != nil {
		return err
	}
	return e.Create(res)
}

func (e ExternalTableHandle) Exists(tableName string) bool {
	_, err := e.mcExternalTable.BatchLoadTables([]string{tableName})
	return err == nil
}

func NewExternalTableHandle(
	mcSQLExecutor McSQLExecutor,
	mcSchema McSchema,
	mcExternalTable McExternalTable,
	getter TenantDetailsGetter,
	maskingPolicyHandle TableMaskingPolicyHandle,
) *ExternalTableHandle {
	return &ExternalTableHandle{
		mcSQLExecutor:       mcSQLExecutor,
		mcSchema:            mcSchema,
		mcExternalTable:     mcExternalTable,
		tenantDetailsGetter: getter,
		maskingPolicyHandle: maskingPolicyHandle,
	}
}

func (e ExternalTableHandle) getLocation(ctx context.Context, et *ExternalTable, res *resource.Resource) (string, error) {
	switch strings.ToUpper(et.Source.SourceType) {
	case GoogleSheet, GoogleDrive:
		loc := et.Source.Location
		if loc == "" {
			tenantWithDetails, err := e.tenantDetailsGetter.GetDetails(ctx, res.Tenant())
			if err != nil {
				return "", err
			}
			commonLoc := tenantWithDetails.GetConfigs()[ExtLocation]
			if commonLoc == "" {
				err = errors.NotFound(EntityExternalTable, "location for the external table is empty")
				return "", err
			}
			loc = commonLoc
		}
		return fmt.Sprintf("%s/%s/", strings.TrimSuffix(loc, "/"), strings.ReplaceAll(et.FullName(), ".", "/")), nil
	default:
		return et.Source.Location, nil
	}
}

func buildExternalTableSchema(t *ExternalTable, location string) (tableschema.TableSchema, error) {
	handler := handlerForFormat(t.Source.ContentType)

	builder := tableschema.NewSchemaBuilder()
	builder.
		Name(t.Name).
		Comment(t.Description).
		StorageHandler(handler).
		Location(location).
		TblProperties(t.Source.TableProperties)

	err := externalTableColumns(t, builder)
	if err != nil {
		return tableschema.TableSchema{}, err
	}

	return builder.Build(), nil
}

func externalTableColumns(t *ExternalTable, schemaBuilder *tableschema.SchemaBuilder) error {
	partitionColNames := map[string]struct{}{}

	return t.Schema.ToMaxComputeColumns(partitionColNames, nil, schemaBuilder, "external")
}

func ToOtherExternalSQLString(projectName, schemaName string, serdeProperties map[string]string, schema tableschema.TableSchema, format string) (string, error) {
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
			i++
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
