package maxcompute_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/aliyun/aliyun-odps-go-sdk/odps"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/tableschema"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/goto/optimus/core/resource"
	"github.com/goto/optimus/core/tenant"
	"github.com/goto/optimus/ext/store/maxcompute"
)

func TestExternalTableHandle(t *testing.T) {
	projectName, schemaName, tableName := "proj", "schema", "test_table"
	fullName := projectName + "." + schemaName + "." + tableName
	mcStore := resource.MaxCompute
	tnnt, _ := tenant.NewTenant(projectName, "ns")
	metadata := resource.Metadata{
		Version:     1,
		Description: "resource description",
		Labels:      map[string]string{"owner": "optimus"},
	}
	spec := map[string]any{
		"description": "test create",
		"schema": []map[string]any{
			{
				"name": "customer_id",
				"type": "STRING",
			},
		},
		"source": map[string]any{
			"type":     "csv",
			"location": "oss://my_bucket",
		},
	}
	var emptyJars []string

	t.Run("Create", func(t *testing.T) {
		t.Run("returns error when cannot convert spec", func(t *testing.T) {
			table := new(mockExternalTable)
			schema := new(mockMaxComputeSchema)
			odpsIns := new(mockOdpsIns)
			tableHandle := maxcompute.NewExternalTableHandle(odpsIns, schema, table)

			spec := map[string]any{
				"description": []string{"test create"},
				"schema": []map[string]any{
					{
						"name": "customer_id",
						"type": "STRING",
					},
				},
			}
			res, err := resource.NewResource(fullName, maxcompute.KindExternalTable, mcStore, tnnt, &metadata, spec)
			assert.Nil(t, err)

			err = tableHandle.Create(res)
			assert.NotNil(t, err)
			assert.ErrorContains(t, err, "not able to decode spec for "+fullName)
		})
		t.Run("returns error when table name is empty", func(t *testing.T) {
			table := new(mockExternalTable)
			schema := new(mockMaxComputeSchema)
			odpsIns := new(mockOdpsIns)
			tableHandle := maxcompute.NewExternalTableHandle(odpsIns, schema, table)

			res, err := resource.NewResource(projectName+"."+schemaName, maxcompute.KindExternalTable, mcStore, tnnt, &metadata, spec)
			assert.Nil(t, err)

			err = tableHandle.Create(res)
			assert.NotNil(t, err)
			assert.ErrorContains(t, err, "invalid resource name: "+projectName+"."+schemaName)
		})
		t.Run("returns error when failed to create schema", func(t *testing.T) {
			table := new(mockExternalTable)
			schema := new(mockMaxComputeSchema)
			schema.On("Create", schemaName, true, mock.Anything).Return(fmt.Errorf("error while creating schema on maxcompute"))
			defer schema.AssertExpectations(t)
			odpsIns := new(mockOdpsIns)
			odpsIns.On("CurrentSchemaName").Return(schemaName)
			defer odpsIns.AssertExpectations(t)
			tableHandle := maxcompute.NewExternalTableHandle(odpsIns, schema, table)

			res, err := resource.NewResource(fullName, maxcompute.KindExternalTable, mcStore, tnnt, &metadata, spec)
			assert.Nil(t, err)

			err = tableHandle.Create(res)
			assert.NotNil(t, err)
			assert.ErrorContains(t, err, "error while creating schema on maxcompute")
		})
		t.Run("returns error when use invalid table schema data type", func(t *testing.T) {
			table := new(mockExternalTable)
			schema := new(mockMaxComputeSchema)
			schema.On("Create", schemaName, true, mock.Anything).Return(nil)
			defer schema.AssertExpectations(t)
			odpsIns := new(mockOdpsIns)
			odpsIns.On("CurrentSchemaName").Return(schemaName)
			defer odpsIns.AssertExpectations(t)
			tableHandle := maxcompute.NewExternalTableHandle(odpsIns, schema, table)

			spec := map[string]any{
				"description": "test create",
				"source": map[string]any{
					"type": "csv",
				},
				"schema": []map[string]any{
					{
						"name": "customer_id",
						"type": "STRING_ERROR",
					},
				},
			}
			res, err := resource.NewResource(fullName, maxcompute.KindExternalTable, mcStore, tnnt, &metadata, spec)
			assert.Nil(t, err)

			err = tableHandle.Create(res)
			assert.NotNil(t, err)
			assert.ErrorContains(t, err, "failed to build table schema to create for "+fullName)
		})
		t.Run("returns error when table already present on maxcompute", func(t *testing.T) {
			existTableErr := errors.New("Table or view already exists - table or view proj.test_table is already defined")
			table := new(mockExternalTable)
			table.On("CreateExternal", mock.Anything, false, emptyStringMap, emptyJars, emptyStringMap, emptyStringMap).Return(existTableErr)
			defer table.AssertExpectations(t)
			schema := new(mockMaxComputeSchema)
			schema.On("Create", schemaName, true, mock.Anything).Return(nil)
			defer schema.AssertExpectations(t)
			odpsIns := new(mockOdpsIns)
			odpsIns.On("CurrentSchemaName").Return(schemaName)
			defer odpsIns.AssertExpectations(t)
			tableHandle := maxcompute.NewExternalTableHandle(odpsIns, schema, table)

			res, err := resource.NewResource(fullName, maxcompute.KindExternalTable, mcStore, tnnt, &metadata, spec)
			assert.Nil(t, err)

			err = tableHandle.Create(res)
			assert.NotNil(t, err)
			assert.ErrorContains(t, err, "table already exists on maxcompute: "+fullName)
		})
		t.Run("returns error when table creation returns error", func(t *testing.T) {
			table := new(mockExternalTable)
			table.On("CreateExternal", mock.Anything, false, emptyStringMap, emptyJars, emptyStringMap, emptyStringMap).Return(errors.New("some error"))
			defer table.AssertExpectations(t)
			schema := new(mockMaxComputeSchema)
			schema.On("Create", schemaName, true, mock.Anything).Return(nil)
			defer schema.AssertExpectations(t)
			odpsIns := new(mockOdpsIns)
			odpsIns.On("CurrentSchemaName").Return(schemaName)
			defer odpsIns.AssertExpectations(t)
			tableHandle := maxcompute.NewExternalTableHandle(odpsIns, schema, table)

			res, err := resource.NewResource(fullName, maxcompute.KindExternalTable, mcStore, tnnt, &metadata, spec)
			assert.Nil(t, err)

			err = tableHandle.Create(res)
			assert.NotNil(t, err)
			assert.ErrorContains(t, err, "error while creating table on maxcompute")
		})
		t.Run("return success when create the external table", func(t *testing.T) {
			table := new(mockExternalTable)
			table.On("CreateExternal", mock.Anything, false, emptyStringMap, emptyJars, emptyStringMap, emptyStringMap).Return(nil)
			defer table.AssertExpectations(t)
			schema := new(mockMaxComputeSchema)
			schema.On("Create", schemaName, true, mock.Anything).Return(nil)
			defer schema.AssertExpectations(t)
			odpsIns := new(mockOdpsIns)
			odpsIns.On("CurrentSchemaName").Return(schemaName)
			defer odpsIns.AssertExpectations(t)
			tableHandle := maxcompute.NewExternalTableHandle(odpsIns, schema, table)

			res, err := resource.NewResource(fullName, maxcompute.KindExternalTable, mcStore, tnnt, &metadata, spec)
			assert.Nil(t, err)

			err = tableHandle.Create(res)
			assert.Nil(t, err)
		})
	})

	t.Run("Exists", func(t *testing.T) {
		t.Run("returns false when error in checking existing tables", func(t *testing.T) {
			table := new(mockExternalTable)
			table.On("BatchLoadTables", mock.Anything).Return(nil, errors.New("error in get"))
			defer table.AssertExpectations(t)

			schema := new(mockMaxComputeSchema)
			odpsIns := new(mockOdpsIns)
			tableHandle := maxcompute.NewExternalTableHandle(odpsIns, schema, table)

			exists := tableHandle.Exists(tableName)
			assert.False(t, exists)
		})
		t.Run("returns true when checking existing tables", func(t *testing.T) {
			odpsIns := new(mockOdpsIns)
			schema := new(mockMaxComputeSchema)

			table := new(mockExternalTable)
			extTab := odps.NewTable(nil, projectName, schemaName, tableName)
			table.On("BatchLoadTables", mock.Anything).Return([]*odps.Table{extTab}, nil)
			defer table.AssertExpectations(t)

			tableHandle := maxcompute.NewExternalTableHandle(odpsIns, schema, table)

			exists := tableHandle.Exists(tableName)
			assert.True(t, exists)
		})
	})
}

type mockExternalTable struct {
	mock.Mock
}

func (m *mockExternalTable) CreateExternal(schema tableschema.TableSchema, createIfNotExists bool, serdeProperties map[string]string, jars []string, hints, alias map[string]string) error {
	args := m.Called(schema, createIfNotExists, serdeProperties, jars, hints, alias)
	return args.Error(0)
}

func (m *mockExternalTable) BatchLoadTables(tableNames []string) ([]*odps.Table, error) {
	args := m.Called(tableNames)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]*odps.Table), args.Error(1)
}
