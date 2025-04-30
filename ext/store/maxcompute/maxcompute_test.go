package maxcompute_test

import (
	"context"
	"errors"
	"testing"

	"github.com/goto/salt/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/goto/optimus/core/resource"
	"github.com/goto/optimus/core/tenant"
	"github.com/goto/optimus/ext/store/maxcompute"
)

func TestMaxComputeStore(t *testing.T) {
	ctx := context.Background()
	maxFileSize := 10
	maxFileCleanupSize := 15
	log := log.NewNoop()
	projectName, schemaName, tableName := "proj", "schema", "test_table"
	fullName := projectName + "." + schemaName + "." + tableName
	tnnt, _ := tenant.NewTenant(projectName, "ns")
	pts, _ := tenant.NewPlainTextSecret("secret_name", "secret_value")
	maskingPolicySecret, _ := tenant.NewPlainTextSecret("masking_policy_secret_name", "masking_policy_secret_value")
	store := resource.MaxCompute
	metadata := resource.Metadata{
		Version:     1,
		Description: "resource description",
		Labels:      map[string]string{"owner": "optimus"},
	}
	spec := map[string]any{
		"name":        tableName,
		"database":    schemaName,
		"project":     projectName,
		"description": "resource",
	}

	t.Run("Create", func(t *testing.T) {
		t.Run("returns error when secret is not provided", func(t *testing.T) {
			secretProvider := new(mockSecretProvider)
			secretProvider.On("GetSecret", mock.Anything, tnnt, "DATASTORE_MAXCOMPUTE").
				Return(nil, errors.New("not found secret"))
			defer secretProvider.AssertExpectations(t)

			clientProvider := new(mockClientProvider)
			mcStore := maxcompute.NewMaxComputeDataStore(log, secretProvider, clientProvider, nil, nil, maxFileSize, maxFileCleanupSize)

			res, err := resource.NewResource(fullName, maxcompute.KindTable, store, tnnt, &metadata, spec)
			assert.Nil(t, err)

			err = mcStore.Create(ctx, res)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "not found secret")
		})
		t.Run("returns error when not able to get client", func(t *testing.T) {
			secretProvider := new(mockSecretProvider)
			secretProvider.On("GetSecret", mock.Anything, tnnt, "DATASTORE_MAXCOMPUTE").
				Return(pts, nil)
			defer secretProvider.AssertExpectations(t)

			clientProvider := new(mockClientProvider)
			clientProvider.On("Get", pts.Value()).Return(nil, errors.New("error in client"))
			defer clientProvider.AssertExpectations(t)
			mcStore := maxcompute.NewMaxComputeDataStore(log, secretProvider, clientProvider, nil, nil, maxFileSize, maxFileCleanupSize)

			res, err := resource.NewResource(fullName, maxcompute.KindTable, store, tnnt, &metadata, spec)
			assert.Nil(t, err)

			err = mcStore.Create(ctx, res)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "error in client")
		})
		t.Run("returns error when schema name is empty", func(t *testing.T) {
			secretProvider := new(mockSecretProvider)
			secretProvider.On("GetSecret", mock.Anything, tnnt, "DATASTORE_MAXCOMPUTE").
				Return(pts, nil)
			defer secretProvider.AssertExpectations(t)

			client := new(mockClient)
			defer client.AssertExpectations(t)

			clientProvider := new(mockClientProvider)
			clientProvider.On("Get", pts.Value()).Return(client, nil)
			defer clientProvider.AssertExpectations(t)
			mcStore := maxcompute.NewMaxComputeDataStore(log, secretProvider, clientProvider, nil, nil, maxFileSize, maxFileCleanupSize)

			res, err := resource.NewResource(projectName, maxcompute.KindTable, store, tnnt, &metadata, spec)
			assert.Nil(t, err)

			err = mcStore.Create(ctx, res)
			assert.NotNil(t, err)
			assert.ErrorContains(t, err, "invalid schema name: "+projectName)
		})
		t.Run("returns error when kind is invalid", func(t *testing.T) {
			secretProvider := new(mockSecretProvider)
			secretProvider.On("GetSecret", mock.Anything, tnnt, "DATASTORE_MAXCOMPUTE").
				Return(pts, nil)
			defer secretProvider.AssertExpectations(t)

			client := new(mockClient)
			defer client.AssertExpectations(t)

			clientProvider := new(mockClientProvider)
			clientProvider.On("Get", pts.Value()).Return(client, nil)
			defer clientProvider.AssertExpectations(t)

			mcStore := maxcompute.NewMaxComputeDataStore(log, secretProvider, clientProvider, nil, nil, maxFileSize, maxFileCleanupSize)

			res, err := resource.NewResource(fullName, "unknown", store, tnnt, &metadata, spec)
			assert.Nil(t, err)

			err = mcStore.Create(ctx, res)
			assert.NotNil(t, err)
			assert.ErrorContains(t, err, "invalid kind for maxcompute resource unknown")
		})
		t.Run("return success when calls appropriate handler for table", func(t *testing.T) {
			secretProvider := new(mockSecretProvider)
			secretProvider.On("GetSecret", mock.Anything, tnnt, "DATASTORE_MAXCOMPUTE").
				Return(pts, nil)
			secretProvider.On("GetSecret", mock.Anything, tnnt, "DATASTORE_MAXCOMPUTE_MASK_POLICY").
				Return(maskingPolicySecret, nil)
			defer secretProvider.AssertExpectations(t)

			res, err := resource.NewResource(fullName, maxcompute.KindTable, store, tnnt, &metadata, spec)
			assert.Nil(t, err)

			tableHandle := new(mockTableResourceHandle)
			tableHandle.On("Create", res).Return(nil)
			defer tableHandle.AssertExpectations(t)

			maskingPolicyClient := new(mockClient)
			maskingPolicyHandle := new(mockTableMaskingPolicyHandle)
			maskingPolicyClient.On("TableMaskingPolicyHandleFrom", mock.Anything).Return(maskingPolicyHandle)
			defer maskingPolicyClient.AssertExpectations(t)

			client := new(mockClient)
			client.On("TableHandleFrom", mock.Anything, maskingPolicyHandle).Return(tableHandle)
			defer client.AssertExpectations(t)

			clientProvider := new(mockClientProvider)
			clientProvider.On("Get", pts.Value()).Return(client, nil)
			clientProvider.On("Get", maskingPolicySecret.Value()).Return(maskingPolicyClient, nil)
			defer clientProvider.AssertExpectations(t)

			mcStore := maxcompute.NewMaxComputeDataStore(log, secretProvider, clientProvider, nil, nil, maxFileSize, maxFileCleanupSize)
			err = mcStore.Create(ctx, res)
			assert.Nil(t, err)
		})
		t.Run("return success when calls appropriate handler for view", func(t *testing.T) {
			secretProvider := new(mockSecretProvider)
			secretProvider.On("GetSecret", mock.Anything, tnnt, "DATASTORE_MAXCOMPUTE").
				Return(pts, nil)
			defer secretProvider.AssertExpectations(t)

			res, err := resource.NewResource(fullName, maxcompute.KindView, store, tnnt, &metadata, spec)
			assert.Nil(t, err)

			tableHandle := new(mockTableResourceHandle)
			tableHandle.On("Create", res).Return(nil)
			defer tableHandle.AssertExpectations(t)

			client := new(mockClient)
			client.On("ViewHandleFrom", mock.Anything).Return(tableHandle)
			defer client.AssertExpectations(t)

			clientProvider := new(mockClientProvider)
			clientProvider.On("Get", pts.Value()).Return(client, nil)
			defer clientProvider.AssertExpectations(t)

			mcStore := maxcompute.NewMaxComputeDataStore(log, secretProvider, clientProvider, nil, nil, maxFileSize, maxFileCleanupSize)
			err = mcStore.Create(ctx, res)
			assert.Nil(t, err)
		})
	})
	t.Run("Update", func(t *testing.T) {
		t.Run("returns error when secret is not provided", func(t *testing.T) {
			secretProvider := new(mockSecretProvider)
			secretProvider.On("GetSecret", mock.Anything, tnnt, "DATASTORE_MAXCOMPUTE").
				Return(nil, errors.New("not found secret"))
			defer secretProvider.AssertExpectations(t)

			clientProvider := new(mockClientProvider)
			mcStore := maxcompute.NewMaxComputeDataStore(log, secretProvider, clientProvider, nil, nil, maxFileSize, maxFileCleanupSize)

			res, err := resource.NewResource(fullName, maxcompute.KindTable, store, tnnt, &metadata, spec)
			assert.Nil(t, err)

			err = mcStore.Update(ctx, res)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "not found secret")
		})
		t.Run("returns error when not able to get client", func(t *testing.T) {
			secretProvider := new(mockSecretProvider)
			secretProvider.On("GetSecret", mock.Anything, tnnt, "DATASTORE_MAXCOMPUTE").
				Return(pts, nil)
			defer secretProvider.AssertExpectations(t)

			clientProvider := new(mockClientProvider)
			clientProvider.On("Get", pts.Value()).Return(nil, errors.New("error in client"))
			defer clientProvider.AssertExpectations(t)
			mcStore := maxcompute.NewMaxComputeDataStore(log, secretProvider, clientProvider, nil, nil, maxFileSize, maxFileCleanupSize)

			res, err := resource.NewResource(fullName, maxcompute.KindTable, store, tnnt, &metadata, spec)
			assert.Nil(t, err)

			err = mcStore.Update(ctx, res)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "error in client")
		})
		t.Run("returns error when schema name is empty", func(t *testing.T) {
			secretProvider := new(mockSecretProvider)
			secretProvider.On("GetSecret", mock.Anything, tnnt, "DATASTORE_MAXCOMPUTE").
				Return(pts, nil)
			defer secretProvider.AssertExpectations(t)

			client := new(mockClient)
			defer client.AssertExpectations(t)

			clientProvider := new(mockClientProvider)
			clientProvider.On("Get", pts.Value()).Return(client, nil)
			defer clientProvider.AssertExpectations(t)
			mcStore := maxcompute.NewMaxComputeDataStore(log, secretProvider, clientProvider, nil, nil, maxFileSize, maxFileCleanupSize)

			res, err := resource.NewResource(projectName, maxcompute.KindTable, store, tnnt, &metadata, spec)
			assert.Nil(t, err)

			err = mcStore.Update(ctx, res)
			assert.NotNil(t, err)
			assert.ErrorContains(t, err, "invalid schema name: "+projectName)
		})
		t.Run("returns error when kind is invalid", func(t *testing.T) {
			secretProvider := new(mockSecretProvider)
			secretProvider.On("GetSecret", mock.Anything, tnnt, "DATASTORE_MAXCOMPUTE").
				Return(pts, nil)
			defer secretProvider.AssertExpectations(t)

			client := new(mockClient)
			defer client.AssertExpectations(t)

			clientProvider := new(mockClientProvider)
			clientProvider.On("Get", pts.Value()).Return(client, nil)
			defer clientProvider.AssertExpectations(t)

			mcStore := maxcompute.NewMaxComputeDataStore(log, secretProvider, clientProvider, nil, nil, maxFileSize, maxFileCleanupSize)

			res, err := resource.NewResource(fullName, "unknown", store, tnnt, &metadata, spec)
			assert.Nil(t, err)

			err = mcStore.Update(ctx, res)
			assert.NotNil(t, err)
			assert.ErrorContains(t, err, "invalid kind for maxcompute resource unknown")
		})
		t.Run("return success when calls appropriate handler for table", func(t *testing.T) {
			secretProvider := new(mockSecretProvider)
			secretProvider.On("GetSecret", mock.Anything, tnnt, "DATASTORE_MAXCOMPUTE").
				Return(pts, nil)
			secretProvider.On("GetSecret", mock.Anything, tnnt, "DATASTORE_MAXCOMPUTE_MASK_POLICY").
				Return(maskingPolicySecret, nil)
			defer secretProvider.AssertExpectations(t)

			res, err := resource.NewResource(fullName, maxcompute.KindTable, store, tnnt, &metadata, spec)
			assert.Nil(t, err)

			tableHandle := new(mockTableResourceHandle)
			tableHandle.On("Update", res).Return(nil)
			defer tableHandle.AssertExpectations(t)

			maskingPolicyClient := new(mockClient)
			maskingPolicyHandle := new(mockTableMaskingPolicyHandle)
			maskingPolicyClient.On("TableMaskingPolicyHandleFrom", mock.Anything).Return(maskingPolicyHandle)
			defer maskingPolicyClient.AssertExpectations(t)

			client := new(mockClient)
			client.On("TableHandleFrom", mock.Anything, maskingPolicyHandle).Return(tableHandle)
			defer client.AssertExpectations(t)

			clientProvider := new(mockClientProvider)
			clientProvider.On("Get", pts.Value()).Return(client, nil)
			clientProvider.On("Get", maskingPolicySecret.Value()).Return(maskingPolicyClient, nil)
			defer clientProvider.AssertExpectations(t)

			mcStore := maxcompute.NewMaxComputeDataStore(log, secretProvider, clientProvider, nil, nil, maxFileSize, maxFileCleanupSize)
			err = mcStore.Update(ctx, res)
			assert.Nil(t, err)
		})
		t.Run("return success when calls appropriate handler for view", func(t *testing.T) {
			secretProvider := new(mockSecretProvider)
			secretProvider.On("GetSecret", mock.Anything, tnnt, "DATASTORE_MAXCOMPUTE").
				Return(pts, nil)
			defer secretProvider.AssertExpectations(t)

			res, err := resource.NewResource(fullName, maxcompute.KindView, store, tnnt, &metadata, spec)
			assert.Nil(t, err)

			tableHandle := new(mockTableResourceHandle)
			tableHandle.On("Update", res).Return(nil)
			defer tableHandle.AssertExpectations(t)

			client := new(mockClient)
			client.On("ViewHandleFrom", mock.Anything).Return(tableHandle)
			defer client.AssertExpectations(t)

			clientProvider := new(mockClientProvider)
			clientProvider.On("Get", pts.Value()).Return(client, nil)
			defer clientProvider.AssertExpectations(t)

			mcStore := maxcompute.NewMaxComputeDataStore(log, secretProvider, clientProvider, nil, nil, maxFileSize, maxFileCleanupSize)
			err = mcStore.Update(ctx, res)
			assert.Nil(t, err)
		})
	})
	t.Run("Validate", func(t *testing.T) {
		invalidSpec := map[string]any{
			"description": map[string]any{"some": "desc"},
		}
		specWithoutValues := map[string]any{
			"name":     tableName,
			"database": schemaName,
			"project":  projectName,
			"a":        "b",
		}
		t.Run("returns error when resource kind is invalid", func(t *testing.T) {
			res, err := resource.NewResource(fullName, "unknown", store, tnnt, &metadata, invalidSpec)
			assert.Nil(t, err)

			mcStore := maxcompute.NewMaxComputeDataStore(log, nil, nil, nil, nil, maxFileSize, maxFileCleanupSize)
			err = mcStore.Validate(res)
			assert.NotNil(t, err)
			assert.ErrorContains(t, err, "unknown kind")
		})
		t.Run("for table", func(t *testing.T) {
			t.Run("returns error when cannot decode table", func(t *testing.T) {
				res, err := resource.NewResource(fullName, maxcompute.KindTable, store, tnnt, &metadata, invalidSpec)
				assert.Nil(t, err)
				assert.Equal(t, fullName, res.FullName())

				mcStore := maxcompute.NewMaxComputeDataStore(log, nil, nil, nil, nil, maxFileSize, maxFileCleanupSize)
				err = mcStore.Validate(res)
				assert.NotNil(t, err)
				assert.ErrorContains(t, err, "not able to decode spec for "+fullName)
			})
			t.Run("returns error when decode empty table schema", func(t *testing.T) {
				res, err := resource.NewResource(fullName, maxcompute.KindTable, store, tnnt, &metadata, specWithoutValues)
				assert.Nil(t, err)
				assert.Equal(t, fullName, res.FullName())

				mcStore := maxcompute.NewMaxComputeDataStore(log, nil, nil, nil, nil, maxFileSize, maxFileCleanupSize)
				err = mcStore.Validate(res)
				assert.NotNil(t, err)
				assert.ErrorContains(t, err, "empty schema for table "+fullName)
			})
		})
		t.Run("for view", func(t *testing.T) {
			t.Run("returns error when cannot decode view", func(t *testing.T) {
				res, err := resource.NewResource(fullName, maxcompute.KindView, store, tnnt, &metadata, invalidSpec)
				assert.Nil(t, err)
				assert.Equal(t, fullName, res.FullName())

				mcStore := maxcompute.NewMaxComputeDataStore(log, nil, nil, nil, nil, maxFileSize, maxFileCleanupSize)
				err = mcStore.Validate(res)
				assert.NotNil(t, err)
				assert.ErrorContains(t, err, "not able to decode spec for "+fullName)
			})
			t.Run("returns error when decode empty view schema", func(t *testing.T) {
				res, err := resource.NewResource(fullName, maxcompute.KindView, store, tnnt, &metadata, specWithoutValues)
				assert.Nil(t, err)
				assert.Equal(t, fullName, res.FullName())

				mcStore := maxcompute.NewMaxComputeDataStore(log, nil, nil, nil, nil, maxFileSize, maxFileCleanupSize)
				err = mcStore.Validate(res)
				assert.NotNil(t, err)
				assert.ErrorContains(t, err, "view query is empty for "+fullName)
			})
		})
	})
	t.Run("GetURN", func(t *testing.T) {
		spec := map[string]any{
			"description": "resource",
			"project":     projectName,
			"database":    schemaName,
			"name":        tableName,
		}
		t.Run("returns urn for resource", func(t *testing.T) {
			expectedURN, err := resource.ParseURN("maxcompute://" + projectName + "." + schemaName + "." + tableName)
			assert.NoError(t, err)

			res, err := resource.NewResource(projectName+"."+schemaName+"."+tableName, maxcompute.KindTable, store, tnnt, &metadata, spec)
			assert.NoError(t, err)

			mcStore := maxcompute.NewMaxComputeDataStore(log, nil, nil, nil, nil, maxFileSize, maxFileCleanupSize)
			actualURN, err := mcStore.GetURN(res)
			assert.NoError(t, err)
			assert.Equal(t, expectedURN, actualURN)
		})
	})
	t.Run("Exist", func(t *testing.T) {
		t.Run("returns false and error when store is not maxcompute", func(t *testing.T) {
			secretProvider := new(mockSecretProvider)
			defer secretProvider.AssertExpectations(t)

			clientProvider := new(mockClientProvider)
			defer clientProvider.AssertExpectations(t)

			mcStore := maxcompute.NewMaxComputeDataStore(log, secretProvider, clientProvider, nil, nil, maxFileSize, maxFileCleanupSize)

			urn, err := resource.NewURN("random_store", "project.table")
			assert.NoError(t, err)

			actualExist, actualError := mcStore.Exist(ctx, tnnt, urn)
			assert.False(t, actualExist)
			assert.ErrorContains(t, actualError, "expected store [maxcompute] but received [random_store]")
		})
		t.Run("returns false and error when secret is not provided", func(t *testing.T) {
			secretProvider := new(mockSecretProvider)
			secretProvider.On("GetSecret", mock.Anything, tnnt, "DATASTORE_MAXCOMPUTE").
				Return(nil, errors.New("not found secret"))
			defer secretProvider.AssertExpectations(t)

			clientProvider := new(mockClientProvider)
			defer clientProvider.AssertExpectations(t)

			mcStore := maxcompute.NewMaxComputeDataStore(log, secretProvider, clientProvider, nil, nil, maxFileSize, maxFileCleanupSize)

			urn, err := resource.NewURN("maxcompute", "project.schema.table")
			assert.NoError(t, err)

			actualExist, actualError := mcStore.Exist(ctx, tnnt, urn)
			assert.False(t, actualExist)
			assert.ErrorContains(t, actualError, "not found secret")
		})
		t.Run("returns false and error when not able to get client", func(t *testing.T) {
			secretProvider := new(mockSecretProvider)
			secretProvider.On("GetSecret", mock.Anything, tnnt, "DATASTORE_MAXCOMPUTE").Return(pts, nil)
			defer secretProvider.AssertExpectations(t)

			clientProvider := new(mockClientProvider)
			clientProvider.On("Get", pts.Value()).Return(nil, errors.New("error in client"))
			defer clientProvider.AssertExpectations(t)

			mcStore := maxcompute.NewMaxComputeDataStore(log, secretProvider, clientProvider, nil, nil, maxFileSize, maxFileCleanupSize)

			urn, err := resource.NewURN("maxcompute", "project.schema.table")
			assert.NoError(t, err)

			actualExist, actualError := mcStore.Exist(ctx, tnnt, urn)
			assert.False(t, actualExist)
			assert.ErrorContains(t, actualError, "error in client")
		})
		t.Run("returns error when schema name is empty", func(t *testing.T) {
			secretProvider := new(mockSecretProvider)
			secretProvider.On("GetSecret", mock.Anything, tnnt, "DATASTORE_MAXCOMPUTE").
				Return(pts, nil)
			defer secretProvider.AssertExpectations(t)

			client := new(mockClient)
			defer client.AssertExpectations(t)

			clientProvider := new(mockClientProvider)
			clientProvider.On("Get", pts.Value()).Return(client, nil)
			defer clientProvider.AssertExpectations(t)

			mcStore := maxcompute.NewMaxComputeDataStore(log, secretProvider, clientProvider, nil, nil, maxFileSize, maxFileCleanupSize)

			urn, err := resource.NewURN("maxcompute", projectName)
			assert.NoError(t, err)

			actualExist, actualError := mcStore.Exist(ctx, tnnt, urn)
			assert.False(t, actualExist)
			assert.ErrorContains(t, actualError, "invalid schema name: "+projectName)
		})
		t.Run("returns true and error when resource name is invalid", func(t *testing.T) {
			secretProvider := new(mockSecretProvider)
			secretProvider.On("GetSecret", mock.Anything, tnnt, "DATASTORE_MAXCOMPUTE").Return(pts, nil)
			defer secretProvider.AssertExpectations(t)

			client := new(mockClient)
			defer client.AssertExpectations(t)

			clientProvider := new(mockClientProvider)
			clientProvider.On("Get", pts.Value()).Return(client, nil)
			defer clientProvider.AssertExpectations(t)

			tableHandle := new(mockTableResourceHandle)
			defer tableHandle.AssertExpectations(t)

			mcStore := maxcompute.NewMaxComputeDataStore(log, secretProvider, clientProvider, nil, nil, maxFileSize, maxFileCleanupSize)

			urn, err := resource.NewURN("maxcompute", "project.table")
			assert.NoError(t, err)

			actualExist, actualError := mcStore.Exist(ctx, tnnt, urn)
			assert.True(t, actualExist)
			assert.ErrorContains(t, actualError, "invalid resource name: project.table")
		})
		t.Run("returns true and error when resource name is empty", func(t *testing.T) {
			secretProvider := new(mockSecretProvider)
			secretProvider.On("GetSecret", mock.Anything, tnnt, "DATASTORE_MAXCOMPUTE").Return(pts, nil)
			defer secretProvider.AssertExpectations(t)

			client := new(mockClient)
			defer client.AssertExpectations(t)

			clientProvider := new(mockClientProvider)
			clientProvider.On("Get", pts.Value()).Return(client, nil)
			defer clientProvider.AssertExpectations(t)

			tableHandle := new(mockTableResourceHandle)
			defer tableHandle.AssertExpectations(t)

			mcStore := maxcompute.NewMaxComputeDataStore(log, secretProvider, clientProvider, nil, nil, maxFileSize, maxFileCleanupSize)

			urn, err := resource.NewURN("maxcompute", "project.schema.")
			assert.NoError(t, err)

			actualExist, actualError := mcStore.Exist(ctx, tnnt, urn)
			assert.True(t, actualExist)
			assert.ErrorContains(t, actualError, "invalid resource name: project.schema.")
		})
		t.Run("returns true and nil when schema table resource does exist", func(t *testing.T) {
			secretProvider := new(mockSecretProvider)
			secretProvider.On("GetSecret", mock.Anything, tnnt, "DATASTORE_MAXCOMPUTE").Return(pts, nil)
			secretProvider.On("GetSecret", mock.Anything, tnnt, "DATASTORE_MAXCOMPUTE_MASK_POLICY").
				Return(maskingPolicySecret, nil).Maybe()
			defer secretProvider.AssertExpectations(t)

			client := new(mockClient)
			defer client.AssertExpectations(t)

			clientProvider := new(mockClientProvider)
			clientProvider.On("Get", pts.Value()).Return(client, nil)
			clientProvider.On("Get", maskingPolicySecret.Value()).Return(client, nil).Maybe()
			defer clientProvider.AssertExpectations(t)

			mpHandle := new(mockTableMaskingPolicyHandle)
			tableHandle := new(mockTableResourceHandle)
			viewHandle := new(mockTableResourceHandle)
			defer func() {
				tableHandle.AssertExpectations(t)
				viewHandle.AssertExpectations(t)
				mpHandle.AssertExpectations(t)
			}()

			mcStore := maxcompute.NewMaxComputeDataStore(log, secretProvider, clientProvider, nil, nil, maxFileSize, maxFileCleanupSize)

			urn, err := resource.NewURN("maxcompute", "project.schema.table")
			assert.NoError(t, err)

			client.On("TableMaskingPolicyHandleFrom", mock.Anything).Return(mpHandle).Maybe()
			client.On("ExternalTableHandleFrom", mock.Anything, mock.Anything, mpHandle).Return(viewHandle).Maybe()
			viewHandle.On("Exists", mock.Anything).Return(true).Maybe()
			client.On("TableHandleFrom", mock.Anything, mpHandle).Return(tableHandle).Maybe()
			tableHandle.On("Exists", mock.Anything).Return(true).Maybe()
			client.On("ViewHandleFrom", mock.Anything).Return(viewHandle).Maybe()
			viewHandle.On("Exists", mock.Anything).Return(true).Maybe()

			actualExist, actualError := mcStore.Exist(ctx, tnnt, urn)
			assert.True(t, actualExist)
			assert.NoError(t, actualError)
		})
		t.Run("returns false and nil when schema table resource does not exist", func(t *testing.T) {
			secretProvider := new(mockSecretProvider)
			secretProvider.On("GetSecret", mock.Anything, tnnt, "DATASTORE_MAXCOMPUTE").Return(pts, nil)
			secretProvider.On("GetSecret", mock.Anything, tnnt, "DATASTORE_MAXCOMPUTE_MASK_POLICY").
				Return(maskingPolicySecret, nil).Maybe()
			defer secretProvider.AssertExpectations(t)

			client := new(mockClient)
			defer client.AssertExpectations(t)

			clientProvider := new(mockClientProvider)
			clientProvider.On("Get", pts.Value()).Return(client, nil)
			clientProvider.On("Get", maskingPolicySecret.Value()).Return(client, nil).Maybe()
			defer clientProvider.AssertExpectations(t)

			mpHandle := new(mockTableMaskingPolicyHandle)
			tableHandle := new(mockTableResourceHandle)
			viewHandle := new(mockTableResourceHandle)
			defer func() {
				tableHandle.AssertExpectations(t)
				viewHandle.AssertExpectations(t)
				defer mpHandle.AssertExpectations(t)
			}()

			mcStore := maxcompute.NewMaxComputeDataStore(log, secretProvider, clientProvider, nil, nil, maxFileSize, maxFileCleanupSize)

			urn, err := resource.NewURN("maxcompute", "project.schema.table")
			assert.NoError(t, err)

			client.On("TableMaskingPolicyHandleFrom", mock.Anything).Return(mpHandle).Maybe()
			client.On("TableHandleFrom", mock.Anything, mpHandle).Return(tableHandle).Maybe()
			tableHandle.On("Exists", mock.Anything).Return(false).Maybe()
			client.On("ViewHandleFrom", mock.Anything).Return(viewHandle).Maybe()
			viewHandle.On("Exists", mock.Anything).Return(false).Maybe()
			client.On("ExternalTableHandleFrom", mock.Anything, mock.Anything, mpHandle).Return(viewHandle).Maybe()
			viewHandle.On("Exists", mock.Anything).Return(false).Maybe()

			actualExist, actualError := mcStore.Exist(ctx, tnnt, urn)
			assert.False(t, actualExist)
			assert.NoError(t, actualError)
		})
	})
}

type mockTableResourceHandle struct {
	mock.Mock
}

func (m *mockTableResourceHandle) Create(res *resource.Resource) error {
	args := m.Called(res)
	return args.Error(0)
}

func (m *mockTableResourceHandle) Update(res *resource.Resource) error {
	args := m.Called(res)
	return args.Error(0)
}

func (m *mockTableResourceHandle) Exists(tableName string) bool {
	args := m.Called(tableName)
	return args.Get(0).(bool)
}

type mockClient struct {
	mock.Mock
}

func (m *mockClient) TableHandleFrom(projectSchema maxcompute.ProjectSchema, maskingPolicyHandle maxcompute.TableMaskingPolicyHandle) maxcompute.TableResourceHandle {
	args := m.Called(projectSchema, maskingPolicyHandle)
	return args.Get(0).(maxcompute.TableResourceHandle)
}

func (m *mockClient) ViewHandleFrom(projectSchema maxcompute.ProjectSchema) maxcompute.TableResourceHandle {
	args := m.Called(projectSchema)
	return args.Get(0).(maxcompute.TableResourceHandle)
}

func (m *mockClient) ExternalTableHandleFrom(schema maxcompute.ProjectSchema, tenantDetailsGetter maxcompute.TenantDetailsGetter, maskingPolicyHandle maxcompute.TableMaskingPolicyHandle) maxcompute.TableResourceHandle {
	args := m.Called(schema, tenantDetailsGetter, maskingPolicyHandle)
	return args.Get(0).(maxcompute.TableResourceHandle)
}

func (m *mockClient) TableMaskingPolicyHandleFrom(schema maxcompute.ProjectSchema) maxcompute.TableMaskingPolicyHandle {
	args := m.Called(schema)
	return args.Get(0).(maxcompute.TableMaskingPolicyHandle)
}

type mockClientProvider struct {
	mock.Mock
}

func (m *mockClientProvider) Get(account string) (maxcompute.Client, error) {
	args := m.Called(account)
	if args.Get(0) != nil {
		return args.Get(0).(maxcompute.Client), args.Error(1)
	}
	return nil, args.Error(1)
}

type mockSecretProvider struct {
	mock.Mock
}

func (s *mockSecretProvider) GetSecret(ctx context.Context, ten tenant.Tenant, name string) (*tenant.PlainTextSecret, error) {
	args := s.Called(ctx, ten, name)
	var pts *tenant.PlainTextSecret
	if args.Get(0) != nil {
		pts = args.Get(0).(*tenant.PlainTextSecret)
	}
	return pts, args.Error(1)
}
