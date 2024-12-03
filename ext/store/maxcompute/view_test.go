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

	odpsInstance := odps.NewInstance(odps.NewOdps(account.NewAliyunAccount(accessID, accessKey), endpoint), projectName, "")

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
		t.Run("returns error when view name is empty", func(t *testing.T) {
			table := new(mockMaxComputeTable)
			schema := new(mockMaxComputeSchema)
			odpsIns := new(mockOdpsIns)
			viewHandle := maxcompute.NewViewHandle(odpsIns, schema, table)

			spec := map[string]any{
				"description": "test create",
				"columns":     []string{"customer_id", "customer_name", "product_name"},
				"view_query":  "select * from test_customer;",
			}
			res, err := resource.NewResource(projectName+"."+schemaName, maxcompute.KindView, mcStore, tnnt, &metadata, spec)
			assert.Nil(t, err)

			err = viewHandle.Create(res)
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
			viewHandle := maxcompute.NewViewHandle(odpsIns, schema, table)

			spec := map[string]any{
				"description": "test create",
				"columns":     []string{"customer_id", "customer_name", "product_name"},
				"view_query":  "select * from test_customer;",
			}
			res, err := resource.NewResource(fullName, maxcompute.KindView, mcStore, tnnt, &metadata, spec)
			assert.Nil(t, err)

			err = viewHandle.Create(res)
			assert.NotNil(t, err)
			assert.ErrorContains(t, err, "error while creating schema on maxcompute")
		})
		t.Run("returns error when view query is invalid", func(t *testing.T) {
			table := new(mockMaxComputeTable)
			schema := new(mockMaxComputeSchema)
			schema.On("Create", schemaName, true, mock.Anything).Return(nil)
			defer schema.AssertExpectations(t)
			odpsIns := new(mockOdpsIns)
			odpsIns.On("CurrentSchemaName").Return(schemaName)
			odpsIns.On("ExecSQl", mock.Anything).Return(odpsInstance, fmt.Errorf("sql task is invalid"))
			defer odpsIns.AssertExpectations(t)
			viewHandle := maxcompute.NewViewHandle(odpsIns, schema, table)

			spec := map[string]any{
				"description": "test create",
				"columns":     []string{"customer_id", "customer_name", "product_name"},
				"view_query":  "select from test_customer;",
			}
			res, err := resource.NewResource(fullName, maxcompute.KindView, mcStore, tnnt, &metadata, spec)
			assert.Nil(t, err)

			err = viewHandle.Create(res)
			assert.NotNil(t, err)
			assert.ErrorContains(t, err, "failed to create sql task to create view "+fullName)
		})
		t.Run("returns error when view creation returns error", func(t *testing.T) {
			table := new(mockMaxComputeTable)
			schema := new(mockMaxComputeSchema)
			schema.On("Create", schemaName, true, mock.Anything).Return(nil)
			defer schema.AssertExpectations(t)
			odpsIns := new(mockOdpsIns)
			odpsIns.On("CurrentSchemaName").Return(schemaName)
			odpsIns.On("ExecSQl", mock.Anything).Return(odpsInstance, nil)
			defer odpsIns.AssertExpectations(t)
			viewHandle := maxcompute.NewViewHandle(odpsIns, schema, table)

			spec := map[string]any{
				"description": "test create",
				"view_query":  "select * from test_customer",
			}
			res, err := resource.NewResource(fullName, maxcompute.KindView, mcStore, tnnt, &metadata, spec)
			assert.Nil(t, err)

			err = viewHandle.Create(res)
			assert.NotNil(t, err)
			assert.ErrorContains(t, err, "failed to create view "+fullName)
		})
	})

	t.Run("Update", func(t *testing.T) {
		t.Run("returns error when view name is empty", func(t *testing.T) {
			table := new(mockMaxComputeTable)
			schema := new(mockMaxComputeSchema)
			odpsIns := new(mockOdpsIns)
			viewHandle := maxcompute.NewViewHandle(odpsIns, schema, table)

			spec := map[string]any{
				"description": "test update",
				"columns":     []string{"customer_id", "customer_name", "product_name"},
				"view_query":  "select * from test_customer;",
			}
			res, err := resource.NewResource(projectName+"."+schemaName, maxcompute.KindView, mcStore, tnnt, &metadata, spec)
			assert.Nil(t, err)

			err = viewHandle.Update(res)
			assert.NotNil(t, err)
			assert.ErrorContains(t, err, "invalid resource name: "+projectName+"."+schemaName)
		})
		t.Run("returns error when view is not found", func(t *testing.T) {
			table := new(mockMaxComputeTable)
			table.On("BatchLoadTables", mock.Anything).Return([]*odps.Table{}, fmt.Errorf("view is not found"))
			defer table.AssertExpectations(t)
			schema := new(mockMaxComputeSchema)
			odpsIns := new(mockOdpsIns)
			viewHandle := maxcompute.NewViewHandle(odpsIns, schema, table)

			spec := map[string]any{"description": []string{"test update"}}
			res, err := resource.NewResource(fullName, maxcompute.KindView, mcStore, tnnt, &metadata, spec)
			assert.Nil(t, err)

			err = viewHandle.Update(res)
			assert.NotNil(t, err)
			assert.ErrorContains(t, err, "error while get view on maxcompute")
		})
		t.Run("returns error when cannot convert spec", func(t *testing.T) {
			table := new(mockMaxComputeTable)
			table.On("BatchLoadTables", mock.Anything).Return([]*odps.Table{}, nil)
			defer table.AssertExpectations(t)
			schema := new(mockMaxComputeSchema)
			odpsIns := new(mockOdpsIns)
			viewHandle := maxcompute.NewViewHandle(odpsIns, schema, table)

			spec := map[string]any{"description": []string{"test update"}}
			res, err := resource.NewResource(fullName, maxcompute.KindView, mcStore, tnnt, &metadata, spec)
			assert.Nil(t, err)

			err = viewHandle.Update(res)
			assert.NotNil(t, err)
			assert.ErrorContains(t, err, "not able to decode spec for "+fullName)
		})
		t.Run("returns error when view query is invalid", func(t *testing.T) {
			table := new(mockMaxComputeTable)
			table.On("BatchLoadTables", mock.Anything).Return([]*odps.Table{}, nil)
			defer table.AssertExpectations(t)
			schema := new(mockMaxComputeSchema)
			odpsIns := new(mockOdpsIns)
			odpsIns.On("ExecSQl", mock.Anything).Return(odpsInstance, fmt.Errorf("sql task is invalid"))
			defer odpsIns.AssertExpectations(t)
			viewHandle := maxcompute.NewViewHandle(odpsIns, schema, table)

			spec := map[string]any{
				"description": "test update",
				"columns":     []string{"customer_id", "customer_name", "product_name"},
				"view_query":  "select * from test_customer;",
			}
			res, err := resource.NewResource(fullName, maxcompute.KindView, mcStore, tnnt, &metadata, spec)
			assert.Nil(t, err)

			err = viewHandle.Update(res)
			assert.NotNil(t, err)
			assert.ErrorContains(t, err, "failed to create sql task to update view "+fullName)
		})
		t.Run("returns error when view creation returns error", func(t *testing.T) {
			table := new(mockMaxComputeTable)
			table.On("BatchLoadTables", mock.Anything).Return([]*odps.Table{}, nil)
			defer table.AssertExpectations(t)
			schema := new(mockMaxComputeSchema)
			odpsIns := new(mockOdpsIns)
			odpsIns.On("ExecSQl", mock.Anything).Return(odpsInstance, nil)
			defer odpsIns.AssertExpectations(t)
			viewHandle := maxcompute.NewViewHandle(odpsIns, schema, table)

			spec := map[string]any{
				"description": "test update",
				"view_query":  "select * from test_customer",
			}
			res, err := resource.NewResource(fullName, maxcompute.KindView, mcStore, tnnt, &metadata, spec)
			assert.Nil(t, err)

			err = viewHandle.Update(res)
			assert.NotNil(t, err)
			assert.ErrorContains(t, err, "failed to update view "+fullName)
		})
	})

	t.Run("Exists", func(t *testing.T) {
		t.Run("returns false when error in checking existing view", func(t *testing.T) {
			table := new(mockMaxComputeTable)
			table.On("BatchLoadTables", mock.Anything).Return([]*odps.Table{}, errors.New("error in get"))
			defer table.AssertExpectations(t)
			schema := new(mockMaxComputeSchema)
			odpsIns := new(mockOdpsIns)
			viewHandle := maxcompute.NewViewHandle(odpsIns, schema, table)

			exists := viewHandle.Exists(tableName)
			assert.False(t, exists)
		})
		t.Run("returns true when checking existing tables", func(t *testing.T) {
			table := new(mockMaxComputeTable)
			table.On("BatchLoadTables", mock.Anything).Return(normalTables, nil)
			defer table.AssertExpectations(t)
			schema := new(mockMaxComputeSchema)
			odpsIns := new(mockOdpsIns)
			viewHandle := maxcompute.NewViewHandle(odpsIns, schema, table)

			exists := viewHandle.Exists(tableName)
			assert.True(t, exists)
		})
	})
}

func TestToViewSQL(t *testing.T) {
	type args struct {
		v *maxcompute.View
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "create_view",
			args: args{
				v: &maxcompute.View{
					Name:        "schema.test_view",
					Description: "Create Test View",
					Columns:     []string{"a", "b", "c"},
					ViewQuery:   "select a, b, c from t1",
				},
			},
			want: `create or replace view schema.test_view
    (a, b, c)  
    comment 'Create Test View'  
    as
	select a, b, c from t1;`,
			wantErr: nil,
		},
		{
			name: "create_view_missing_description",
			args: args{
				v: &maxcompute.View{
					Name:      "schema.test_view",
					Columns:   []string{"a", "b", "c"},
					ViewQuery: "select a, b, c from t1",
				},
			},
			want: `create or replace view schema.test_view
    (a, b, c)  
    as
	select a, b, c from t1;`,
			wantErr: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := maxcompute.ToViewSQL(tt.args.v)
			if tt.wantErr != nil && !tt.wantErr(t, err, fmt.Sprintf("ToViewSQL error in (%s)", tt.name)) {
				return
			}
			assert.Equalf(t, tt.want, got, "ToViewSQL(%v)", tt.args.v)
		})
	}
}
