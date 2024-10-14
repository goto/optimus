package maxcompute

import (
	"fmt"
	"github.com/aliyun/aliyun-odps-go-sdk/odps"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/datatype"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/tableschema"
	"strings"

	"github.com/goto/optimus/core/resource"
	"github.com/goto/optimus/internal/errors"
	"github.com/goto/optimus/internal/utils"
)

type ColumnRecord struct {
	columnStructure string
	columnValue     tableschema.Column
}

type McSqlExecutor interface {
	ExecSQlWithHints(sql string, hints map[string]string) (*odps.Instance, error)
}

type McTable interface {
	Create(schema tableschema.TableSchema, createIfNotExists bool, hints, alias map[string]string) error
	BatchLoadTables(tableNames []string) ([]odps.Table, error)
}

type TableHandle struct {
	mcSqlExecutor McSqlExecutor
	mcTable       McTable
}

func (t TableHandle) Create(res *resource.Resource) error {
	table, err := ConvertSpecTo[Table](res)
	if err != nil {
		return err
	}
	table.Name = res.Name()

	schema, err := buildTableSchema(table)
	if err != nil {
		return errors.AddErrContext(err, EntityTable, "failed to build table schema to create for "+res.FullName())
	}

	err = t.mcTable.Create(schema, false, table.Hints, nil)
	if err != nil {
		if strings.Contains(err.Error(), "Table or view already exists") {
			return errors.AlreadyExists(EntityTable, "table already exists on maxcompute: "+res.FullName())
		}
		return errors.InternalError(EntityTable, "error while creating table on maxcompute", err)
	}
	return nil
}

func (t TableHandle) Update(res *resource.Resource) error {
	existing, err := t.mcTable.BatchLoadTables([]string{res.FullName()})
	if err != nil {
		return errors.InternalError(EntityTable, "error while get table on maxcompute", err)
	}

	existingSchema, err := existing[0].GetSchema()
	if err != nil {
		return errors.AddErrContext(err, EntityTable, "failed to get old table schema to update for "+res.FullName())
	}

	table, err := ConvertSpecTo[Table](res)
	if err != nil {
		return err
	}

	if table.Hints == nil {
		table.Hints = make(map[string]string)
	}
	table.Hints["odps.sql.schema.evolution.json.enable"] = "true"

	schema, err := buildTableSchema(table)
	if err != nil {
		return errors.AddErrContext(err, EntityTable, "failed to build table schema to update for "+res.FullName())
	}

	sqlTasks, err := generateUpdateQuery(schema, *existingSchema)
	if err != nil {
		return errors.AddErrContext(err, EntityTable, "invalid schema for table "+res.FullName())
	}

	for _, task := range sqlTasks {
		ins, err := t.mcSqlExecutor.ExecSQlWithHints(task, table.Hints)
		if err != nil {
			return errors.AddErrContext(err, EntityTable, "failed to create sql task to update for "+res.FullName())
		}

		err = ins.WaitForSuccess()
		if err != nil {
			return errors.InternalError(EntityTable, "error while execute sql query on maxcompute", err)
		}
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
		Name(t.Name.String()).
		Comment(t.Description).
		Lifecycle(t.Lifecycle)

	err := populateColumns(t, &builder)
	if err != nil {
		return tableschema.TableSchema{}, err
	}

	return builder.Build(), nil
}

func populateColumns(t *Table, schemaBuilder *tableschema.SchemaBuilder) error {
	partitionColNames := map[string]struct{}{}
	if t.Partition != nil {
		partitionColNames = utils.ListToMap(t.Partition.Columns)
	}

	return t.Schema.ToMaxComputeColumns(partitionColNames, t.Cluster, schemaBuilder)
}

func generateUpdateQuery(incoming, existing tableschema.TableSchema) ([]string, error) {
	var sqlTasks []string
	if incoming.Comment != existing.Comment {
		sqlTasks = append(sqlTasks, fmt.Sprintf("alter table %s set comment '%s';", existing.TableName, incoming.Comment))
	}

	if incoming.Lifecycle != existing.Lifecycle {
		sqlTasks = append(sqlTasks, fmt.Sprintf("alter table %s set lifecycle %d;", existing.TableName, incoming.Lifecycle))
	}

	_, incomingFlattenSchema := flattenSchema(incoming, false)
	existingFlattenSchema, _ := flattenSchema(existing, true)

	if err := getNormalColumnDifferences(existing.TableName, incomingFlattenSchema, existingFlattenSchema, &sqlTasks); err != nil {
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

func trackSchema(parent string, column tableschema.Column, columnCollection map[string]tableschema.Column, columnList *[]ColumnRecord, isArrayStructType bool, isExistingTable bool) {
	if isStruct(column.Type) || isArrayStruct(column.Type) {
		storeColumn(specifyColumnStructure(parent, column.Name, isArrayStructType), tableschema.Column{
			Name:            column.Name,
			Type:            column.Type,
			Comment:         column.Comment,
			IsNullable:      true,
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
					IsNullable:      true,
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

func specifyColumnStructure(parent string, columnName string, isArrayStruct bool) string {
	if parent == "" {
		return columnName
	}
	if isArrayStruct {
		return fmt.Sprintf("%s.element.%s", parent, columnName)
	}
	return fmt.Sprintf("%s.%s", parent, columnName)
}

func getNormalColumnDifferences(tableName string, incoming []ColumnRecord, existing map[string]tableschema.Column, sqlTasks *[]string) error {
	var columnAddition []string
	for _, incomingColumnRecord := range incoming {
		columnFound, ok := existing[incomingColumnRecord.columnStructure]
		if !ok {
			if !incomingColumnRecord.columnValue.IsNullable {
				return fmt.Errorf("unable to add new required column")
			}
			segment := fmt.Sprintf("if not exists %s %s", incomingColumnRecord.columnStructure, incomingColumnRecord.columnValue.Type.Name())
			if incomingColumnRecord.columnValue.HasDefaultValue {
				segment += fmt.Sprintf(" default %s", incomingColumnRecord.columnValue.DefaultValue)
			}
			if incomingColumnRecord.columnValue.Comment != "" {
				segment += fmt.Sprintf(" comment '%s'", incomingColumnRecord.columnValue.Comment)
			}
			columnAddition = append(columnAddition, segment)
			continue
		}

		if columnFound.IsNullable && !incomingColumnRecord.columnValue.IsNullable {
			return fmt.Errorf("unable to modify column mode from nullable to required")
		} else if !columnFound.IsNullable && incomingColumnRecord.columnValue.IsNullable {
			*sqlTasks = append(*sqlTasks, fmt.Sprintf("alter table %s change column %s null;", tableName, columnFound.Name))
		}

		if columnFound.Type.ID() != incomingColumnRecord.columnValue.Type.ID() {
			return fmt.Errorf("unable to modify column data type")
		}

		if incomingColumnRecord.columnValue.Comment != columnFound.Comment {
			*sqlTasks = append(*sqlTasks, fmt.Sprintf("alter table %s change column %s %s %s comment '%s';",
				tableName, columnFound.Name, incomingColumnRecord.columnValue.Name, columnFound.Type, incomingColumnRecord.columnValue.Comment))
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
			addColumnQuery := fmt.Sprintf("alter table %s add column ", tableName) + segment + ";"
			*sqlTasks = append(*sqlTasks, addColumnQuery)
		}
	}

	return nil
}

func NewTableHandle(mcSqlExecutor McSqlExecutor, mc McTable) *TableHandle {
	return &TableHandle{mcSqlExecutor: mcSqlExecutor, mcTable: mc}
}
