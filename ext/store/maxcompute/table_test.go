package maxcompute_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/aliyun/aliyun-odps-go-sdk/odps"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/account"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/tableschema"
	"github.com/goto/optimus/core/resource"
	"github.com/goto/optimus/core/tenant"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

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

func (m *mockMaxComputeTable) BatchLoadTables(tableNames []string) ([]odps.Table, error) {
	args := m.Called(tableNames)
	return args.Get(0).([]odps.Table), args.Error(1)
}

type mockOdpsIns struct {
	mock.Mock
}

func (m *mockOdpsIns) ExecSQl(sql string) (*odps.Instance, error) {
	args := m.Called(sql)
	return args.Get(0).(*odps.Instance), args.Error(1)
}

func (m *mockOdpsIns) ExecSQlWithHints(sql string, hints map[string]string) (*odps.Instance, error) {
	args := m.Called(sql, hints)
	return args.Get(0).(*odps.Instance), args.Error(1)
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

	normalTables := []odps.Table{
		odps.NewTable(odps.NewOdps(account.NewAliyunAccount(accessID, accessKey), endpoint), projectName, tableName),
	}

	t.Run("Create", func(t *testing.T) {
		t.Run("returns error when cannot convert spec", func(t *testing.T) {
			table := new(mockMaxComputeTable)
			odpsIns := new(mockOdpsIns)
			tableHandle := maxcompute.NewTableHandle(odpsIns, table)

			spec := map[string]any{"description": []string{"test create"}}
			res, err := resource.NewResource(fullName, maxcompute.KindTable, mcStore, tnnt, &metadata, spec)
			assert.Nil(t, err)

			err = tableHandle.Create(res)
			assert.NotNil(t, err)
			assert.ErrorContains(t, err, "not able to decode spec for "+fullName)
		})
		t.Run("returns error when use invalid schema data type", func(t *testing.T) {
			table := new(mockMaxComputeTable)
			odpsIns := new(mockOdpsIns)
			tableHandle := maxcompute.NewTableHandle(odpsIns, table)

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
			odpsIns := new(mockOdpsIns)
			tableHandle := maxcompute.NewTableHandle(odpsIns, table)

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
			odpsIns := new(mockOdpsIns)
			tableHandle := maxcompute.NewTableHandle(odpsIns, table)

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
			odpsIns := new(mockOdpsIns)
			tableHandle := maxcompute.NewTableHandle(odpsIns, table)

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
		t.Run("returns error when table is not found on maxcompute", func(t *testing.T) {
			table := new(mockMaxComputeTable)
			table.On("BatchLoadTables", mock.Anything).Return([]odps.Table{}, fmt.Errorf("table not found"))
			defer table.AssertExpectations(t)
			odpsIns := new(mockOdpsIns)
			tableHandle := maxcompute.NewTableHandle(odpsIns, table)

			spec := map[string]any{"description": []string{"test update"}}
			res, err := resource.NewResource(fullName, maxcompute.KindTable, mcStore, tnnt, &metadata, spec)
			assert.Nil(t, err)

			err = tableHandle.Update(res)
			assert.NotNil(t, err)
			assert.ErrorContains(t, err, "error while get table on maxcompute")
		})
		t.Run("returns error when get table schema", func(t *testing.T) {
			table := new(mockMaxComputeTable)
			table.On("BatchLoadTables", mock.Anything).Return(normalTables, nil)
			defer table.AssertExpectations(t)
			odpsIns := new(mockOdpsIns)
			tableHandle := maxcompute.NewTableHandle(odpsIns, table)

			spec := map[string]any{"description": []string{"test update"}}
			res, err := resource.NewResource(fullName, maxcompute.KindTable, mcStore, tnnt, &metadata, spec)
			assert.Nil(t, err)

			err = tableHandle.Update(res)
			assert.NotNil(t, err)
			assert.ErrorContains(t, err, "failed to get old table schema to update for "+fullName)
		})
	})

	t.Run("Exists", func(t *testing.T) {
		t.Run("returns false when error in checking existing tables", func(t *testing.T) {
			table := new(mockMaxComputeTable)
			table.On("BatchLoadTables", mock.Anything).Return([]odps.Table{}, errors.New("error in get"))
			defer table.AssertExpectations(t)
			odpsIns := new(mockOdpsIns)
			tableHandle := maxcompute.NewTableHandle(odpsIns, table)

			exists := tableHandle.Exists(tableName)
			assert.False(t, exists)
		})
		t.Run("returns true when checking existing tables", func(t *testing.T) {
			table := new(mockMaxComputeTable)
			table.On("BatchLoadTables", mock.Anything).Return(normalTables, nil)
			defer table.AssertExpectations(t)
			odpsIns := new(mockOdpsIns)
			tableHandle := maxcompute.NewTableHandle(odpsIns, table)

			exists := tableHandle.Exists(tableName)
			assert.True(t, exists)
		})
	})
}
