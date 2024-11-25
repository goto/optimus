package maxcompute_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/aliyun/aliyun-odps-go-sdk/odps"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/account"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/goto/optimus/core/resource"
	"github.com/goto/optimus/core/tenant"
	"github.com/goto/optimus/ext/store/maxcompute"
)

func TestViewHandle(t *testing.T) {
	accessID, accessKey, endpoint := "LNRJ5tH1XMSINW5J3TjYAvfX", "lAZBJhdkNbwVj3bej5BuhjwbdV0nSp", "http://service.ap-southeast-5.maxcompute.aliyun.com/api"
	projectName, schemaName, tableName := "proj", "schema", "test_view"
	fullName := projectName + "." + schemaName + "." + tableName
	mcStore := resource.MaxCompute
	tnnt, _ := tenant.NewTenant(projectName, "ns")
	metadata := resource.Metadata{
		Version:     1,
		Description: "resource description",
		Labels:      map[string]string{"owner": "optimus"},
	}

	normalTables := []*odps.Table{
		odps.NewTable(odps.NewOdps(account.NewAliyunAccount(accessID, accessKey), endpoint), projectName, schemaName, tableName),
	}

	t.Run("Create", func(t *testing.T) {
		t.Run("returns error when cannot convert spec", func(t *testing.T) {
			table := new(mockMaxComputeTable)
			schema := new(mockMaxComputeSchema)
			odpsIns := new(mockOdpsIns)
			viewHandle := maxcompute.NewViewHandle(odpsIns, schema, table)

			spec := map[string]any{"description": []string{"test create"}}
			res, err := resource.NewResource(fullName, maxcompute.KindView, mcStore, tnnt, &metadata, spec)
			assert.Nil(t, err)

			err = viewHandle.Create(res)
			assert.NotNil(t, err)
			assert.ErrorContains(t, err, "not able to decode spec for "+fullName)
		})

		t.Run("returns error when failed to create schema", func(t *testing.T) {
			table := new(mockMaxComputeTable)
			schema := new(mockMaxComputeSchema)
			schema.On("Create", schemaName, true, mock.Anything).Return(fmt.Errorf("error while creating schema on maxcompute"))
			defer schema.AssertExpectations(t)
			odpsIns := new(mockOdpsIns)
			odpsIns.On("CurrentSchemaName").Return(schemaName)
			defer odpsIns.AssertExpectations(t)
			viewHandle := maxcompute.NewViewHandle(odpsIns, schema, table)

			spec := map[string]any{
				"name":        "test_view",
				"database":    "schema",
				"project":     "proj",
				"description": "test create",
				"view_query":  "select * from test_customer;",
			}
			res, err := resource.NewResource(fullName, maxcompute.KindView, mcStore, tnnt, &metadata, spec)
			assert.Nil(t, err)

			err = viewHandle.Create(res)
			assert.NotNil(t, err)
			assert.ErrorContains(t, err, "error while creating schema on maxcompute")
		})
		t.Run("returns error when view table is already exist", func(t *testing.T) {
			table := new(mockMaxComputeTable)
			table.On("CreateView", mock.Anything, false, false, false).Return(fmt.Errorf("Table or view already exists"))
			defer table.AssertExpectations(t)
			schema := new(mockMaxComputeSchema)
			schema.On("Create", schemaName, true, mock.Anything).Return(nil)
			defer schema.AssertExpectations(t)
			odpsIns := new(mockOdpsIns)
			odpsIns.On("CurrentSchemaName").Return(schemaName)
			defer odpsIns.AssertExpectations(t)
			viewHandle := maxcompute.NewViewHandle(odpsIns, schema, table)

			spec := map[string]any{
				"name":        "test_view",
				"database":    "schema",
				"project":     "proj",
				"description": "test create",
				"view_query":  "select * from test_customer;",
			}
			res, err := resource.NewResource(fullName, maxcompute.KindView, mcStore, tnnt, &metadata, spec)
			assert.Nil(t, err)

			err = viewHandle.Create(res)
			assert.NotNil(t, err)
			assert.ErrorContains(t, err, "view already exists on maxcompute: "+fullName)
		})
		t.Run("returns error when view creation returns error", func(t *testing.T) {
			table := new(mockMaxComputeTable)
			table.On("CreateView", mock.Anything, false, false, false).Return(fmt.Errorf("error while creating view"))
			defer table.AssertExpectations(t)
			schema := new(mockMaxComputeSchema)
			schema.On("Create", schemaName, true, mock.Anything).Return(nil)
			defer schema.AssertExpectations(t)
			odpsIns := new(mockOdpsIns)
			odpsIns.On("CurrentSchemaName").Return(schemaName)
			defer odpsIns.AssertExpectations(t)
			viewHandle := maxcompute.NewViewHandle(odpsIns, schema, table)

			spec := map[string]any{
				"name":        "test_view",
				"database":    "schema",
				"project":     "proj",
				"description": "test create",
				"view_query":  "select from test_customer;",
			}
			res, err := resource.NewResource(fullName, maxcompute.KindView, mcStore, tnnt, &metadata, spec)
			assert.Nil(t, err)

			err = viewHandle.Create(res)
			assert.NotNil(t, err)
			assert.ErrorContains(t, err, "failed to create view "+fullName)
		})
		t.Run("returns success when create view table", func(t *testing.T) {
			table := new(mockMaxComputeTable)
			table.On("CreateView", mock.Anything, false, false, false).Return(nil)
			defer table.AssertExpectations(t)
			schema := new(mockMaxComputeSchema)
			schema.On("Create", schemaName, true, mock.Anything).Return(nil)
			defer schema.AssertExpectations(t)
			odpsIns := new(mockOdpsIns)
			odpsIns.On("CurrentSchemaName").Return(schemaName)
			defer odpsIns.AssertExpectations(t)
			viewHandle := maxcompute.NewViewHandle(odpsIns, schema, table)

			spec := map[string]any{
				"name":        "test_view",
				"database":    "schema",
				"project":     "proj",
				"description": "test create",
				"view_query":  "select * from test_customer;",
			}
			res, err := resource.NewResource(fullName, maxcompute.KindView, mcStore, tnnt, &metadata, spec)
			assert.Nil(t, err)

			err = viewHandle.Create(res)
			assert.Nil(t, err)
		})
	})

	t.Run("Update", func(t *testing.T) {
		t.Run("returns error when view is not found", func(t *testing.T) {
			table := new(mockMaxComputeTable)
			table.On("BatchLoadTables", mock.Anything).Return(normalTables, fmt.Errorf("view is not found"))
			defer table.AssertExpectations(t)
			schema := new(mockMaxComputeSchema)
			odpsIns := new(mockOdpsIns)
			viewHandle := maxcompute.NewViewHandle(odpsIns, schema, table)

			spec := map[string]any{"description": "test update"}
			res, err := resource.NewResource(fullName, maxcompute.KindView, mcStore, tnnt, &metadata, spec)
			assert.Nil(t, err)

			err = viewHandle.Update(res)
			assert.NotNil(t, err)
			assert.ErrorContains(t, err, "error while get view on maxcompute")
		})
		t.Run("returns error when cannot convert spec", func(t *testing.T) {
			schema := new(mockMaxComputeSchema)
			odpsIns := new(mockOdpsIns)
			viewHandle := maxcompute.NewViewHandle(odpsIns, schema, nil)

			spec := map[string]any{"description": []string{"test update"}}
			res, err := resource.NewResource(fullName, maxcompute.KindView, mcStore, tnnt, &metadata, spec)
			assert.Nil(t, err)

			err = viewHandle.Update(res)
			assert.NotNil(t, err)
			assert.ErrorContains(t, err, "not able to decode spec for "+fullName)
		})
		t.Run("returns error when view update returns error", func(t *testing.T) {
			table := new(mockMaxComputeTable)
			table.On("BatchLoadTables", mock.Anything).Return(normalTables, nil)
			table.On("CreateView", mock.Anything, true, false, false).Return(fmt.Errorf("error while update view"))
			defer table.AssertExpectations(t)
			schema := new(mockMaxComputeSchema)
			odpsIns := new(mockOdpsIns)
			viewHandle := maxcompute.NewViewHandle(odpsIns, schema, table)

			spec := map[string]any{
				"name":        "test_view",
				"database":    "schema",
				"project":     "proj",
				"description": "test update",
				"view_query":  "select from test_customer;",
			}
			res, err := resource.NewResource(fullName, maxcompute.KindView, mcStore, tnnt, &metadata, spec)
			assert.Nil(t, err)

			err = viewHandle.Update(res)
			assert.NotNil(t, err)
			assert.ErrorContains(t, err, "failed to update view "+fullName)
		})
		t.Run("returns success when view update", func(t *testing.T) {
			table := new(mockMaxComputeTable)
			table.On("BatchLoadTables", mock.Anything).Return(normalTables, nil)
			table.On("CreateView", mock.Anything, true, false, false).Return(nil)
			defer table.AssertExpectations(t)
			schema := new(mockMaxComputeSchema)
			odpsIns := new(mockOdpsIns)
			viewHandle := maxcompute.NewViewHandle(odpsIns, schema, table)

			spec := map[string]any{
				"name":        "test_view",
				"database":    "schema",
				"project":     "proj",
				"description": "test update",
				"view_query":  "select * from test_customer",
			}
			res, err := resource.NewResource(fullName, maxcompute.KindView, mcStore, tnnt, &metadata, spec)
			assert.Nil(t, err)

			err = viewHandle.Update(res)
			assert.Nil(t, err)
		})
	})

	t.Run("Exists", func(t *testing.T) {
		t.Run("returns false when error in checking existing view", func(t *testing.T) {
			table := new(mockMaxComputeTable)
			table.On("BatchLoadTables", mock.Anything).Return(normalTables, errors.New("error in get"))
			table.On("BatchLoadTables", mock.Anything).Return(nil, errors.New("error in get"))
			defer table.AssertExpectations(t)
			schema := new(mockMaxComputeSchema)
			odpsIns := new(mockOdpsIns)
			viewHandle := maxcompute.NewViewHandle(odpsIns, schema, table)

			exists := viewHandle.Exists(tableName)
			assert.False(t, exists)
		})
		t.Run("returns true when checking existing tables", func(t *testing.T) {
			table := new(mockMaxComputeTable)
			v1 := odps.NewTable(nil, projectName, schemaName, tableName)
			table.On("BatchLoadTables", mock.Anything).Return([]*odps.Table{v1}, nil)
			defer table.AssertExpectations(t)
			schema := new(mockMaxComputeSchema)
			odpsIns := new(mockOdpsIns)
			viewHandle := maxcompute.NewViewHandle(odpsIns, schema, table)

			exists := viewHandle.Exists(tableName)
			assert.True(t, exists)
		})
	})
}
