package maxcompute_test

import (
	"context"
	"errors"
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
	serdePropertiesMap := map[string]string{
		maxcompute.UseQuoteSerde: "true",
	}
	tnnt, _ := tenant.NewTenant(projectName, "ns")
	metadata := resource.Metadata{
		Version:     1,
		Description: "resource description",
		Labels:      map[string]string{"owner": "optimus"},
	}
	spec := map[string]any{
		"name":        tableName,
		"database":    schemaName,
		"project":     projectName,
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

	project, err := tenant.NewProject(projectName, map[string]string{
		tenant.ProjectStoragePathKey: "",
		tenant.ProjectSchedulerHost:  "",
	}, map[string]string{})
	assert.Nil(t, err)
	namespace, err := tenant.NewNamespace(tnnt.NamespaceName().String(), project.Name(), map[string]string{}, map[string]string{})
	assert.Nil(t, err)
	secrets := tenant.PlainTextSecrets{}
	tenantWithDetails, err := tenant.NewTenantDetails(project, namespace, secrets)
	assert.Nil(t, err)

	t.Run("Create", func(t *testing.T) {
		t.Run("returns error when cannot convert spec", func(t *testing.T) {
			table := new(mockExternalTable)
			schema := new(mockMaxComputeSchema)
			odpsIns := new(mockOdpsIns)
			tenantDetailsGetter := new(mockTenantDetailsGetter)
			maskingPolicyHandle := new(mockTableMaskingPolicyHandle)
			tableHandle := maxcompute.NewExternalTableHandle(odpsIns, schema, table, tenantDetailsGetter, maskingPolicyHandle)

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

		t.Run("returns error when table already present on maxcompute", func(t *testing.T) {
			existTableErr := errors.New("Table or view already exists - table or view proj.test_table is already defined")
			table := new(mockExternalTable)

			table.On("CreateExternal", mock.Anything, false, serdePropertiesMap, emptyJars, emptyStringMap, emptyStringMap).Return(existTableErr)
			defer table.AssertExpectations(t)
			schema := new(mockMaxComputeSchema)

			defer schema.AssertExpectations(t)
			odpsIns := new(mockOdpsIns)
			odpsIns.On("SetCurrentSchemaName", mock.Anything)
			defer odpsIns.AssertExpectations(t)

			tenantDetailsGetter := new(mockTenantDetailsGetter)
			tenantDetailsGetter.On("GetDetails", mock.Anything, tnnt).Return(tenantWithDetails, nil)
			defer tenantDetailsGetter.AssertExpectations(t)

			tableHandle := maxcompute.NewExternalTableHandle(odpsIns, schema, table, tenantDetailsGetter, nil)

			res, err := resource.NewResource(fullName, maxcompute.KindExternalTable, mcStore, tnnt, &metadata, spec)
			assert.Nil(t, err)

			err = tableHandle.Create(res)
			assert.NotNil(t, err)
			assert.ErrorContains(t, err, "external table already exists on maxcompute: "+fullName)
		})
		t.Run("returns error when table creation returns error", func(t *testing.T) {
			table := new(mockExternalTable)
			table.On("CreateExternal", mock.Anything, false, serdePropertiesMap, emptyJars, emptyStringMap, emptyStringMap).Return(errors.New("some error"))
			defer table.AssertExpectations(t)
			schema := new(mockMaxComputeSchema)

			defer schema.AssertExpectations(t)
			odpsIns := new(mockOdpsIns)
			odpsIns.On("SetCurrentSchemaName", mock.Anything)
			defer odpsIns.AssertExpectations(t)

			tenantDetailsGetter := new(mockTenantDetailsGetter)
			tenantDetailsGetter.On("GetDetails", mock.Anything, tnnt).Return(tenantWithDetails, nil)
			defer tenantDetailsGetter.AssertExpectations(t)

			tableHandle := maxcompute.NewExternalTableHandle(odpsIns, schema, table, tenantDetailsGetter, nil)

			res, err := resource.NewResource(fullName, maxcompute.KindExternalTable, mcStore, tnnt, &metadata, spec)
			assert.Nil(t, err)

			err = tableHandle.Create(res)
			assert.NotNil(t, err)
			assert.ErrorContains(t, err, "error while creating external table on maxcompute")
		})
		t.Run("return success when create the external table with mask policy", func(t *testing.T) {
			table := new(mockExternalTable)
			table.On("CreateExternal", mock.Anything, false, serdePropertiesMap, emptyJars, emptyStringMap, emptyStringMap).Return(nil)
			defer table.AssertExpectations(t)
			schema := new(mockMaxComputeSchema)

			defer schema.AssertExpectations(t)
			odpsIns := new(mockOdpsIns)
			odpsIns.On("SetCurrentSchemaName", mock.Anything)
			defer odpsIns.AssertExpectations(t)
			maskingPolicyHandle := new(mockTableMaskingPolicyHandle)
			maskingPolicyHandle.On("Process", tableName, mock.Anything).Return(nil)

			tenantDetailsGetter := new(mockTenantDetailsGetter)
			tenantDetailsGetter.On("GetDetails", mock.Anything, tnnt).Return(tenantWithDetails, nil)
			defer tenantDetailsGetter.AssertExpectations(t)

			tableHandle := maxcompute.NewExternalTableHandle(odpsIns, schema, table, tenantDetailsGetter, maskingPolicyHandle)

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
			tableHandle := maxcompute.NewExternalTableHandle(odpsIns, schema, table, nil, nil)

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

			tableHandle := maxcompute.NewExternalTableHandle(odpsIns, schema, table, nil, nil)

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

func (m *mockExternalTable) Delete(tableName string, ifExists bool) error {
	args := m.Called(tableName, ifExists)
	return args.Error(0)
}

func (m *mockExternalTable) BatchLoadTables(tableNames []string) ([]*odps.Table, error) {
	args := m.Called(tableNames)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]*odps.Table), args.Error(1)
}

type mockTenantDetailsGetter struct {
	mock.Mock
}

func (m *mockTenantDetailsGetter) GetDetails(ctx context.Context, tnnt tenant.Tenant) (*tenant.WithDetails, error) {
	args := m.Called(ctx, tnnt)
	return args.Get(0).(*tenant.WithDetails), args.Error(1)
}
