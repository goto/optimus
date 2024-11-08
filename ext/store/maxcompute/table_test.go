package maxcompute_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/aliyun/aliyun-odps-go-sdk/odps"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/account"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/tableschema"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/goto/optimus/core/resource"
	"github.com/goto/optimus/core/tenant"
	"github.com/goto/optimus/ext/store/maxcompute"
)

var emptyStringMap map[string]string

type mockMaxComputeTable struct {
	mock.Mock
}

func (m *mockMaxComputeTable) Create(schema tableschema.TableSchema, createIfNotExists bool, hints, alias map[string]string) error {
	args := m.Called(schema, createIfNotExists, hints, alias)
	return args.Error(0)
}

func (m *mockMaxComputeTable) BatchLoadTables(tableNames []string) ([]*odps.Table, error) {
	args := m.Called(tableNames)
	return args.Get(0).([]*odps.Table), args.Error(1)
}

type mockMaxComputeSchema struct {
	mock.Mock
}

func (m *mockMaxComputeSchema) Create(schemaName string, createIfNotExists bool, comment string) error {
	args := m.Called(schemaName, createIfNotExists, comment)
	return args.Error(0)
}

type mockOdpsIns struct {
	mock.Mock
}

func (m *mockOdpsIns) ExecSQl(sql string, hints ...map[string]string) (*odps.Instance, error) { // nolint
	args := m.Called(sql)
	return args.Get(0).(*odps.Instance), args.Error(1)
}

func (m *mockOdpsIns) ExecSQlWithHints(sql string, hints map[string]string) (*odps.Instance, error) { // nolint
	args := m.Called(sql, hints)
	return args.Get(0).(*odps.Instance), args.Error(1)
}

func (m *mockOdpsIns) CurrentSchemaName() string {
	args := m.Called()
	return args.String(0)
}

func TestTableHandle(t *testing.T) {
	accessID, accessKey, endpoint := "LNRJ5tH1XMSINW5J3TjYAvfX", "lAZBJhdkNbwVj3bej5BuhjwbdV0nSp", "http://service.ap-southeast-5.maxcompute.aliyun.com/api"
	projectName, schemaName, tableName := "proj", "schema", "test_table"
	fullName := projectName + "." + schemaName + "." + tableName
	mcStore := resource.MaxCompute
	tnnt, _ := tenant.NewTenant(projectName, "ns")
	metadata := resource.Metadata{
		Version:     1,
		Description: "resource description",
		Labels:      map[string]string{"owner": "optimus"},
	}

	odpsInstance := odps.NewInstance(odps.NewOdps(account.NewAliyunAccount(accessID, accessKey), endpoint), projectName, "")

	normalTables := []*odps.Table{
		odps.NewTable(odps.NewOdps(account.NewAliyunAccount(accessID, accessKey), endpoint), projectName, schemaName, tableName),
	}

	t.Run("Create", func(t *testing.T) {
		t.Run("returns error when cannot convert spec", func(t *testing.T) {
			table := new(mockMaxComputeTable)
			schema := new(mockMaxComputeSchema)
			odpsIns := new(mockOdpsIns)
			tableHandle := maxcompute.NewTableHandle(odpsIns, schema, table)

			spec := map[string]any{"description": []string{"test create"}}
			res, err := resource.NewResource(fullName, maxcompute.KindTable, mcStore, tnnt, &metadata, spec)
			assert.Nil(t, err)

			err = tableHandle.Create(res)
			assert.NotNil(t, err)
			assert.ErrorContains(t, err, "not able to decode spec for "+fullName)
		})
		t.Run("returns error when table name is empty", func(t *testing.T) {
			table := new(mockMaxComputeTable)
			schema := new(mockMaxComputeSchema)
			odpsIns := new(mockOdpsIns)
			tableHandle := maxcompute.NewTableHandle(odpsIns, schema, table)

			spec := map[string]any{
				"description": "test create",
				"schema": []map[string]any{
					{
						"name": "customer_id",
						"type": "STRING",
					},
				},
			}
			res, err := resource.NewResource(projectName+"."+schemaName, maxcompute.KindTable, mcStore, tnnt, &metadata, spec)
			assert.Nil(t, err)

			err = tableHandle.Create(res)
			assert.NotNil(t, err)
			assert.ErrorContains(t, err, "invalid resource name: "+projectName+"."+schemaName)
		})
		t.Run("returns error when failed to create schema", func(t *testing.T) {
			table := new(mockMaxComputeTable)
			schema := new(mockMaxComputeSchema)
			schema.On("Create", schemaName, true, mock.Anything).Return(fmt.Errorf("error while creating schema on maxcompute"))
			defer schema.AssertExpectations(t)
			odpsIns := new(mockOdpsIns)
			odpsIns.On("CurrentSchemaName").Return(schemaName)
			defer odpsIns.AssertExpectations(t)
			tableHandle := maxcompute.NewTableHandle(odpsIns, schema, table)

			spec := map[string]any{
				"description": "test create",
				"schema": []map[string]any{
					{
						"name": "customer_id",
						"type": "STRING",
					},
				},
			}
			res, err := resource.NewResource(fullName, maxcompute.KindTable, mcStore, tnnt, &metadata, spec)
			assert.Nil(t, err)

			err = tableHandle.Create(res)
			assert.NotNil(t, err)
			assert.ErrorContains(t, err, "error while creating schema on maxcompute")
		})
		t.Run("returns error when use invalid table schema data type", func(t *testing.T) {
			table := new(mockMaxComputeTable)
			schema := new(mockMaxComputeSchema)
			schema.On("Create", schemaName, true, mock.Anything).Return(nil)
			defer schema.AssertExpectations(t)
			odpsIns := new(mockOdpsIns)
			odpsIns.On("CurrentSchemaName").Return(schemaName)
			defer odpsIns.AssertExpectations(t)
			tableHandle := maxcompute.NewTableHandle(odpsIns, schema, table)

			spec := map[string]any{
				"description": "test create",
				"schema": []map[string]any{
					{
						"name": "customer_id",
						"type": "STRING_ERROR",
					},
				},
				"partition": map[string]any{
					"field": []string{"customer_id"},
				},
			}
			res, err := resource.NewResource(fullName, maxcompute.KindTable, mcStore, tnnt, &metadata, spec)
			assert.Nil(t, err)

			err = tableHandle.Create(res)
			assert.NotNil(t, err)
			assert.ErrorContains(t, err, "failed to build table schema to create for "+fullName)
		})
		t.Run("returns error when table already present on maxcompute", func(t *testing.T) {
			existTableErr := errors.New("Table or view already exists - table or view proj.test_table is already defined")
			table := new(mockMaxComputeTable)
			table.On("Create", mock.Anything, false, emptyStringMap, emptyStringMap).Return(existTableErr)
			defer table.AssertExpectations(t)
			schema := new(mockMaxComputeSchema)
			schema.On("Create", schemaName, true, mock.Anything).Return(nil)
			defer schema.AssertExpectations(t)
			odpsIns := new(mockOdpsIns)
			odpsIns.On("CurrentSchemaName").Return(schemaName)
			defer odpsIns.AssertExpectations(t)
			tableHandle := maxcompute.NewTableHandle(odpsIns, schema, table)

			spec := map[string]any{
				"description": "test create",
				"schema": []map[string]any{
					{
						"name": "customer_id",
						"type": "STRING",
					},
				},
				"partition": map[string]any{
					"field": []string{"customer_id"},
				},
			}
			res, err := resource.NewResource(fullName, maxcompute.KindTable, mcStore, tnnt, &metadata, spec)
			assert.Nil(t, err)

			err = tableHandle.Create(res)
			assert.NotNil(t, err)
			assert.ErrorContains(t, err, "table already exists on maxcompute: "+fullName)
		})
		t.Run("returns error when table creation returns error", func(t *testing.T) {
			table := new(mockMaxComputeTable)
			table.On("Create", mock.Anything, false, emptyStringMap, emptyStringMap).Return(errors.New("some error"))
			defer table.AssertExpectations(t)
			schema := new(mockMaxComputeSchema)
			schema.On("Create", schemaName, true, mock.Anything).Return(nil)
			defer schema.AssertExpectations(t)
			odpsIns := new(mockOdpsIns)
			odpsIns.On("CurrentSchemaName").Return(schemaName)
			defer odpsIns.AssertExpectations(t)
			tableHandle := maxcompute.NewTableHandle(odpsIns, schema, table)

			spec := map[string]any{
				"description": "test create",
				"schema": []map[string]any{
					{
						"name": "customer_id",
						"type": "STRING",
					},
				},
				"partition": map[string]any{
					"field": []string{"customer_id"},
				},
			}
			res, err := resource.NewResource(fullName, maxcompute.KindTable, mcStore, tnnt, &metadata, spec)
			assert.Nil(t, err)

			err = tableHandle.Create(res)
			assert.NotNil(t, err)
			assert.ErrorContains(t, err, "error while creating table on maxcompute")
		})
		t.Run("return success when create the resource with partition", func(t *testing.T) {
			table := new(mockMaxComputeTable)
			table.On("Create", mock.Anything, false, emptyStringMap, emptyStringMap).Return(nil)
			defer table.AssertExpectations(t)
			schema := new(mockMaxComputeSchema)
			schema.On("Create", schemaName, true, mock.Anything).Return(nil)
			defer schema.AssertExpectations(t)
			odpsIns := new(mockOdpsIns)
			odpsIns.On("CurrentSchemaName").Return(schemaName)
			defer odpsIns.AssertExpectations(t)
			tableHandle := maxcompute.NewTableHandle(odpsIns, schema, table)

			spec := map[string]any{
				"description": "test create",
				"schema": []map[string]any{
					{
						"name": "customer_id",
						"type": "STRING",
					},
					{
						"name": "customer_name",
						"type": "STRING",
					},
					{
						"name": "product_name",
						"type": "STRING",
					},
				},
				"partition": map[string]any{
					"field": []string{"customer_id"},
				},
			}
			res, err := resource.NewResource(fullName, maxcompute.KindTable, mcStore, tnnt, &metadata, spec)
			assert.Nil(t, err)

			err = tableHandle.Create(res)
			assert.Nil(t, err)
		})
	})

	t.Run("Update", func(t *testing.T) {
		t.Run("returns error when table name is empty", func(t *testing.T) {
			table := new(mockMaxComputeTable)
			schema := new(mockMaxComputeSchema)
			odpsIns := new(mockOdpsIns)
			tableHandle := maxcompute.NewTableHandle(odpsIns, schema, table)

			spec := map[string]any{
				"description": "test create",
				"schema": []map[string]any{
					{
						"name": "customer_id",
						"type": "STRING",
					},
				},
			}
			res, err := resource.NewResource(projectName+"."+schemaName, maxcompute.KindTable, mcStore, tnnt, &metadata, spec)
			assert.Nil(t, err)

			err = tableHandle.Update(res)
			assert.NotNil(t, err)
			assert.ErrorContains(t, err, "invalid resource name: "+projectName+"."+schemaName)
		})
		t.Run("returns error when table is not found on maxcompute", func(t *testing.T) {
			table := new(mockMaxComputeTable)
			table.On("BatchLoadTables", mock.Anything).Return(normalTables, fmt.Errorf("table not found"))
			defer table.AssertExpectations(t)
			schema := new(mockMaxComputeSchema)
			odpsIns := new(mockOdpsIns)
			tableHandle := maxcompute.NewTableHandle(odpsIns, schema, table)

			spec := map[string]any{"description": []string{"test update"}}
			res, err := resource.NewResource(fullName, maxcompute.KindTable, mcStore, tnnt, &metadata, spec)
			assert.Nil(t, err)

			err = tableHandle.Update(res)
			assert.NotNil(t, err)
			assert.ErrorContains(t, err, "error while get table on maxcompute")
		})
		t.Run("returns error when cannot convert spec", func(t *testing.T) {
			table := new(mockMaxComputeTable)
			table.On("BatchLoadTables", mock.Anything).Return(normalTables, nil)
			defer table.AssertExpectations(t)
			schema := new(mockMaxComputeSchema)
			odpsIns := new(mockOdpsIns)
			tableHandle := maxcompute.NewTableHandle(odpsIns, schema, table)

			spec := map[string]any{"description": []string{"test update"}}
			res, err := resource.NewResource(fullName, maxcompute.KindTable, mcStore, tnnt, &metadata, spec)
			assert.Nil(t, err)

			err = tableHandle.Update(res)
			assert.NotNil(t, err)
			assert.ErrorContains(t, err, "not able to decode spec for "+fullName)
		})
		t.Run("returns error when use invalid table schema", func(t *testing.T) {
			table := new(mockMaxComputeTable)
			table.On("BatchLoadTables", mock.Anything).Return(normalTables, nil)
			defer table.AssertExpectations(t)
			schema := new(mockMaxComputeSchema)
			odpsIns := new(mockOdpsIns)
			tableHandle := maxcompute.NewTableHandle(odpsIns, schema, table)

			spec := map[string]any{
				"description": "test update",
				"schema": []map[string]any{
					{
						"name": "customer_id",
						"type": "STRING_ERROR",
					},
				},
			}
			res, err := resource.NewResource(fullName, maxcompute.KindTable, mcStore, tnnt, &metadata, spec)
			assert.Nil(t, err)

			err = tableHandle.Update(res)
			assert.NotNil(t, err)
			assert.ErrorContains(t, err, "failed to build table schema to update for "+fullName)
		})
		t.Run("return error when update the resource for new required column", func(t *testing.T) {
			table := new(mockMaxComputeTable)
			table.On("BatchLoadTables", mock.Anything).Return(normalTables, nil)
			defer table.AssertExpectations(t)
			schema := new(mockMaxComputeSchema)
			odpsIns := new(mockOdpsIns)
			tableHandle := maxcompute.NewTableHandle(odpsIns, schema, table)

			spec := map[string]any{
				"description": "test update",
				"schema": []map[string]any{
					{
						"required": true,
						"name":     "customer_id",
						"type":     "STRING",
					},
					{
						"name":        "customer_address",
						"type":        "STRUCT",
						"description": "customer address",
						"struct": []map[string]any{
							{
								"name":        "zipcode",
								"type":        "ARRAY",
								"description": "address zipcode",
								"array": map[string]any{
									"type": "STRUCT",
									"struct": []map[string]any{
										{
											"name": "inside_1",
											"type": "STRING",
										},
										{
											"name": "inside_2",
											"type": "STRING",
										},
									},
								},
							},
						},
					},
				},
				"lifecycle": 1,
			}
			res, err := resource.NewResource(fullName, maxcompute.KindTable, mcStore, tnnt, &metadata, spec)
			assert.Nil(t, err)

			err = tableHandle.Update(res)
			assert.NotNil(t, err)
			assert.ErrorContains(t, err, "invalid schema for table "+fullName)
		})
		t.Run("returns error when table update query is invalid", func(t *testing.T) {
			table := new(mockMaxComputeTable)
			table.On("BatchLoadTables", mock.Anything).Return(normalTables, nil)
			defer table.AssertExpectations(t)
			schema := new(mockMaxComputeSchema)
			odpsIns := new(mockOdpsIns)
			odpsIns.On("ExecSQlWithHints", mock.Anything, mock.Anything).Return(odpsInstance, fmt.Errorf("sql task is invalid"))
			defer odpsIns.AssertExpectations(t)
			tableHandle := maxcompute.NewTableHandle(odpsIns, schema, table)

			spec := map[string]any{
				"schema": []map[string]any{
					{
						"name":          "customer_id",
						"type":          "STRING",
						"description":   "customer test",
						"default_value": "customer_test",
					},
				},
			}
			res, err := resource.NewResource(fullName, maxcompute.KindTable, mcStore, tnnt, &metadata, spec)
			assert.Nil(t, err)

			err = tableHandle.Update(res)
			assert.NotNil(t, err)
			assert.ErrorContains(t, err, "failed to create sql task to update for "+fullName)
		})
		t.Run("returns error when view creation returns error", func(t *testing.T) {
			table := new(mockMaxComputeTable)
			table.On("BatchLoadTables", mock.Anything).Return(normalTables, nil)
			defer table.AssertExpectations(t)
			schema := new(mockMaxComputeSchema)
			odpsIns := new(mockOdpsIns)
			odpsIns.On("ExecSQlWithHints", mock.Anything, mock.Anything).Return(odpsInstance, nil)
			defer odpsIns.AssertExpectations(t)
			tableHandle := maxcompute.NewTableHandle(odpsIns, schema, table)

			spec := map[string]any{
				"schema": []map[string]any{
					{
						"name": "customer_id",
						"type": "STRING",
					},
				},
			}
			res, err := resource.NewResource(fullName, maxcompute.KindTable, mcStore, tnnt, &metadata, spec)
			assert.Nil(t, err)

			err = tableHandle.Update(res)
			assert.NotNil(t, err)
			assert.ErrorContains(t, err, "error while execute sql query on maxcompute")
		})
	})

	t.Run("Exists", func(t *testing.T) {
		t.Run("returns false when error in checking existing tables", func(t *testing.T) {
			table := new(mockMaxComputeTable)
			table.On("BatchLoadTables", mock.Anything).Return(normalTables, errors.New("error in get"))
			defer table.AssertExpectations(t)
			schema := new(mockMaxComputeSchema)
			odpsIns := new(mockOdpsIns)
			tableHandle := maxcompute.NewTableHandle(odpsIns, schema, table)

			exists := tableHandle.Exists(tableName)
			assert.False(t, exists)
		})
		t.Run("returns true when checking existing tables", func(t *testing.T) {
			table := new(mockMaxComputeTable)
			table.On("BatchLoadTables", mock.Anything).Return(normalTables, nil)
			defer table.AssertExpectations(t)
			schema := new(mockMaxComputeSchema)
			odpsIns := new(mockOdpsIns)
			tableHandle := maxcompute.NewTableHandle(odpsIns, schema, table)

			exists := tableHandle.Exists(tableName)
			assert.True(t, exists)
		})
	})
}
