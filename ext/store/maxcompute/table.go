package maxcompute

import (
	"fmt"
	"strings"

	"github.com/aliyun/aliyun-odps-go-sdk/odps"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/common"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/datatype"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/tableschema"

	"github.com/goto/optimus/core/resource"
	"github.com/goto/optimus/internal/errors"
	"github.com/goto/optimus/internal/utils"
)

type ColumnRecord struct {
	columnStructure string
	columnValue     tableschema.Column
}

type McSQLExecutor interface {
	ExecSQlWithHints(sql string, hints map[string]string) (*odps.Instance, error)
	CurrentSchemaName() string
	SetCurrentSchemaName(schemaName string)
}

type McSchema interface {
	Create(schemaName string, createIfNotExists bool, comment string) error
}

type McTable interface {
	Create(schema tableschema.TableSchema, createIfNotExists bool, hints, alias map[string]string) error
	BatchLoadTables(tableNames []string) ([]*odps.Table, error)
}

type TableMaskingPolicyHandle interface {
	Process(tableName string, schema Schema) error
}

type TableHandle struct {
	mcSQLExecutor       McSQLExecutor
	mcSchema            McSchema
	mcTable             McTable
	maskingPolicyHandle TableMaskingPolicyHandle
}

func (t TableHandle) Create(res *resource.Resource) error {
	table, err := ConvertSpecTo[Table](res)
	if err != nil {
		return err
	}

	if err := t.mcSchema.Create(t.mcSQLExecutor.CurrentSchemaName(), true, ""); err != nil {
		return errors.InternalError(EntitySchema, "error while creating schema on maxcompute", err)
	}

	tableSchema, err := buildTableSchema(table)
	if err != nil {
		return errors.AddErrContext(err, EntityTable, "failed to build table schema to create for "+table.FullName())
	}

	err = t.mcTable.Create(tableSchema, false, table.Hints, nil)
	if err != nil {
		if strings.Contains(err.Error(), "Table or view already exists") {
			return errors.AlreadyExists(EntityTable, "table already exists on maxcompute: "+table.FullName())
		}
		return errors.InternalError(EntityTable, "error while creating table on maxcompute", err)
	}

	err = t.maskingPolicyHandle.Process(table.Name, table.Schema)
	if err != nil {
		return errors.InternalError(EntityTable, "error while processing masking policy on maxcompute table: "+res.FullName(), err)
	}

	return nil
}

func (t TableHandle) Update(res *resource.Resource) error {
	table, err := ConvertSpecTo[Table](res)
	if err != nil {
		return err
	}

	existing, err := t.mcTable.BatchLoadTables([]string{table.Name})
	if err != nil {
		return errors.InternalError(EntityTable, "error while get table on maxcompute", err)
	}
	existingSchema := existing[0].Schema()

	if table.Hints == nil {
		table.Hints = make(map[string]string)
	}
	table.Hints["odps.sql.schema.evolution.json.enable"] = "true"

	tableSchema, err := buildTableSchema(table)
	if err != nil {
		return errors.AddErrContext(err, EntityTable, "failed to build table schema to update for "+table.FullName())
	}

	sqlTasks, err := generateUpdateQuery(tableSchema, existingSchema, table.Database)
	if err != nil {
		return errors.AddErrContext(err, EntityTable, "invalid schema for table "+table.FullName())
	}

	for _, task := range sqlTasks {
		ins, err := t.mcSQLExecutor.ExecSQlWithHints(task, table.Hints)
		if err != nil {
			return errors.AddErrContext(err, EntityTable, "failed to create sql task to update for "+table.FullName())
		}

		if err = ins.WaitForSuccess(); err != nil {
			return errors.InternalError(EntityTable, "error while execute sql query on maxcompute", err)
		}
	}

	err = t.maskingPolicyHandle.Process(table.Name, table.Schema)
	if err != nil {
		return errors.InternalError(EntityTable, "error while processing masking policy on maxcompute table: "+res.FullName(), err)
	}

	return nil
}

func (t TableHandle) Exists(tableName string) bool {
	_, err := t.mcTable.BatchLoadTables([]string{tableName})
	return err == nil
}

func buildTableSchema(t *Table) (tableschema.TableSchema, error) {
	builder := tableschema.NewSchemaBuilder()
	builder.
		Name(t.Name).
		Comment(t.Description).
		Lifecycle(t.Lifecycle).
		TblProperties(t.TableProperties)

	err := populateColumns(t, builder)
	if err != nil {
		return tableschema.TableSchema{}, err
	}

	if t.Type == "" || t.Type == "common" {
		return builder.Build(), nil
	}
	return builder.TblProperties(map[string]string{"transactional": "true"}).Build(), nil
}

func populateColumns(t *Table, schemaBuilder *tableschema.SchemaBuilder) error {
	partitionColNames := map[string]struct{}{}
	if t.Partition != nil {
		partitionColNames = utils.ListToMap(t.Partition.Columns)
	}

	return t.Schema.ToMaxComputeColumns(partitionColNames, t.Cluster, schemaBuilder, t.Type)
}

func generateUpdateQuery(incoming, existing tableschema.TableSchema, schemaName string) ([]string, error) {
	var sqlTasks []string
	if incoming.Comment != existing.Comment {
		sqlTasks = append(sqlTasks, fmt.Sprintf("alter table %s.%s set comment %s;", SafeKeyword(schemaName), SafeKeyword(existing.TableName), common.QuoteString(incoming.Comment)))
	}

	if incoming.Lifecycle != existing.Lifecycle {
		if incoming.Lifecycle <= 0 && existing.Lifecycle >= 0 {
			sqlTasks = append(sqlTasks, fmt.Sprintf("alter table %s.%s disable lifecycle;", SafeKeyword(schemaName), SafeKeyword(existing.TableName)))
		} else if incoming.Lifecycle > 0 {
			sqlTasks = append(sqlTasks, fmt.Sprintf("alter table %s.%s set lifecycle %d;", SafeKeyword(schemaName), SafeKeyword(existing.TableName), incoming.Lifecycle))
		}
	}

	_, incomingFlattenSchema := flattenSchema(incoming, false)
	existingFlattenSchema, _ := flattenSchema(existing, true)

	if err := getNormalColumnDifferences(existing.TableName, schemaName, incomingFlattenSchema, existingFlattenSchema, &sqlTasks); err != nil {
		return []string{}, err
	}

	return sqlTasks, nil
}

func flattenSchema(tableSchema tableschema.TableSchema, isExistingTable bool) (map[string]tableschema.Column, []ColumnRecord) {
	columnCollection := make(map[string]tableschema.Column)
	var columnList []ColumnRecord
	for _, column := range tableSchema.Columns {
		trackSchema("", column, columnCollection, &columnList, false, isExistingTable)
	}

	return columnCollection, columnList
}

func trackSchema(parent string, column tableschema.Column, columnCollection map[string]tableschema.Column, columnList *[]ColumnRecord, isArrayStructType, isExistingTable bool) {
	if isStruct(column.Type) || isArrayStruct(column.Type) {
		storeColumn(specifyColumnStructure(parent, column.Name, isArrayStructType), tableschema.Column{
			Name:            column.Name,
			Type:            column.Type,
			Comment:         column.Comment,
			NotNull:         false,
			HasDefaultValue: false,
		}, columnCollection, columnList, isExistingTable)

		var structData datatype.StructType
		if isArrayStruct(column.Type) {
			structData = column.Type.(datatype.ArrayType).ElementType.(datatype.StructType)
		} else {
			structData = column.Type.(datatype.StructType)
		}

		for _, field := range structData.Fields {
			trackSchema(
				specifyColumnStructure(parent, column.Name, isArrayStructType),
				tableschema.Column{
					Name:            field.Name,
					Type:            field.Type,
					NotNull:         false,
					HasDefaultValue: false,
				},
				columnCollection,
				columnList,
				isArrayStruct(column.Type),
				isExistingTable,
			)
		}

		return
	}

	storeColumn(specifyColumnStructure(parent, column.Name, isArrayStructType), column, columnCollection, columnList, isExistingTable)
}

func storeColumn(key string, column tableschema.Column, columnCollection map[string]tableschema.Column, columnList *[]ColumnRecord, isExistingTable bool) {
	if isExistingTable {
		columnCollection[key] = column
	} else {
		*columnList = append(*columnList, ColumnRecord{
			columnStructure: key,
			columnValue:     column,
		})
	}
}

func specifyColumnStructure(parent, columnName string, isArrayStruct bool) string {
	if parent == "" {
		return fmt.Sprintf("`%s`", columnName)
	}
	if isArrayStruct {
		return fmt.Sprintf("%s.element.`%s`", parent, columnName)
	}
	return fmt.Sprintf("%s.`%s`", parent, columnName)
}

func getNormalColumnDifferences(tableName, schemaName string, incoming []ColumnRecord, existing map[string]tableschema.Column, sqlTasks *[]string) error {
	var columnAddition []string
	for _, incomingColumnRecord := range incoming {
		columnFound, ok := existing[incomingColumnRecord.columnStructure]
		if !ok {
			if incomingColumnRecord.columnValue.NotNull {
				return fmt.Errorf("unable to add new required column")
			}
			segment := fmt.Sprintf("if not exists %s %s", SafeKeyword(incomingColumnRecord.columnStructure), incomingColumnRecord.columnValue.Type.Name())
			if incomingColumnRecord.columnValue.Comment != "" {
				segment += fmt.Sprintf(" comment %s", common.QuoteString(incomingColumnRecord.columnValue.Comment))
			}
			columnAddition = append(columnAddition, segment)
			continue
		}

		if !columnFound.NotNull && incomingColumnRecord.columnValue.NotNull {
			return fmt.Errorf("unable to modify column mode from nullable to required")
		} else if columnFound.NotNull && !incomingColumnRecord.columnValue.NotNull {
			*sqlTasks = append(*sqlTasks, fmt.Sprintf("alter table %s.%s change column %s null;", SafeKeyword(schemaName), SafeKeyword(tableName), SafeKeyword(columnFound.Name)))
		}

		if columnFound.Type.ID() != incomingColumnRecord.columnValue.Type.ID() {
			return fmt.Errorf("unable to modify column data type")
		}

		if incomingColumnRecord.columnValue.Comment != columnFound.Comment {
			*sqlTasks = append(*sqlTasks, fmt.Sprintf("alter table %s.%s change column %s %s %s comment %s;",
				SafeKeyword(schemaName), SafeKeyword(tableName), SafeKeyword(columnFound.Name), SafeKeyword(incomingColumnRecord.columnValue.Name), columnFound.Type, common.QuoteString(incomingColumnRecord.columnValue.Comment)))
		}
		delete(existing, incomingColumnRecord.columnStructure)
	}

	if len(existing) != 0 {
		for column := range existing {
			return fmt.Errorf("field %s is missing in new schema", column)
		}
	}

	if len(columnAddition) > 0 {
		for _, segment := range columnAddition {
			addColumnQuery := fmt.Sprintf("alter table %s.%s add column ", SafeKeyword(schemaName), SafeKeyword(tableName)) + segment + ";"
			*sqlTasks = append(*sqlTasks, addColumnQuery)
		}
	}

	return nil
}

func NewTableHandle(mcSQLExecutor McSQLExecutor, mcSchema McSchema, mcTable McTable, maskingPolicyHandle TableMaskingPolicyHandle) *TableHandle {
	return &TableHandle{mcSQLExecutor: mcSQLExecutor, mcSchema: mcSchema, mcTable: mcTable, maskingPolicyHandle: maskingPolicyHandle}
}
