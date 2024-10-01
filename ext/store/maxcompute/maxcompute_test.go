package maxcompute_test

import (
	"context"
	"errors"
	"github.com/goto/optimus/core/resource"
	"github.com/goto/optimus/core/tenant"
	"github.com/goto/optimus/ext/store/maxcompute"
	"github.com/stretchr/testify/mock"
	"testing"

	"github.com/stretchr/testify/assert"
)

type mockTableResourceHandle struct {
	mock.Mock
}

func (m *mockTableResourceHandle) Create(res *resource.Resource) error {
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

func (m *mockClient) TableHandleFrom() maxcompute.TableResourceHandle {
	args := m.Called()
	return args.Get(0).(maxcompute.TableResourceHandle)
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

func TestMaxComputeStore(t *testing.T) {
	ctx := context.Background()
	tableName := "test_table"
	tnnt, _ := tenant.NewTenant("proj", "ns")
	pts, _ := tenant.NewPlainTextSecret("secret_name", "secret_value")
	store := resource.MaxCompute
	metadata := resource.Metadata{
		Version:     1,
		Description: "resource description",
		Labels:      map[string]string{"owner": "optimus"},
	}
	spec := map[string]any{"description": "resource"}

	t.Run("Create", func(t *testing.T) {
		t.Run("returns error when secret is not provided", func(t *testing.T) {
			secretProvider := new(mockSecretProvider)
			secretProvider.On("GetSecret", mock.Anything, tnnt, "DATASTORE_MAXCOMPUTE").
				Return(nil, errors.New("not found secret"))
			defer secretProvider.AssertExpectations(t)

			clientProvider := new(mockClientProvider)
			mcStore := maxcompute.NewMaxComputeDataStore(secretProvider, clientProvider)

			res, err := resource.NewResource(tableName, maxcompute.KindTable, store, tnnt, &metadata, spec)
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
			clientProvider.On("Get", "secret_value").Return(nil, errors.New("error in client"))
			defer clientProvider.AssertExpectations(t)
			mcStore := maxcompute.NewMaxComputeDataStore(secretProvider, clientProvider)

			res, err := resource.NewResource(tableName, maxcompute.KindTable, store, tnnt, &metadata, spec)
			assert.Nil(t, err)

			err = mcStore.Create(ctx, res)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "error in client")
		})
		t.Run("returns error when kind is invalid", func(t *testing.T) {
			secretProvider := new(mockSecretProvider)
			secretProvider.On("GetSecret", mock.Anything, tnnt, "DATASTORE_MAXCOMPUTE").
				Return(pts, nil)
			defer secretProvider.AssertExpectations(t)

			client := new(mockClient)
			defer client.AssertExpectations(t)

			clientProvider := new(mockClientProvider)
			clientProvider.On("Get", "secret_value").Return(client, nil)
			defer clientProvider.AssertExpectations(t)

			mcStore := maxcompute.NewMaxComputeDataStore(secretProvider, clientProvider)

			res, err := resource.NewResource(tableName, "unknown", store, tnnt, &metadata, spec)
			assert.Nil(t, err)

			err = mcStore.Create(ctx, res)
			assert.NotNil(t, err)
			assert.ErrorContains(t, err, "invalid kind for maxcompute resource unknown")
		})
		t.Run("return success when calls appropriate handler for table", func(t *testing.T) {
			secretProvider := new(mockSecretProvider)
			secretProvider.On("GetSecret", mock.Anything, tnnt, "DATASTORE_MAXCOMPUTE").
				Return(pts, nil)
			defer secretProvider.AssertExpectations(t)

			res, err := resource.NewResource(tableName, maxcompute.KindTable, store, tnnt, &metadata, spec)
			assert.Nil(t, err)

			tableHandle := new(mockTableResourceHandle)
			tableHandle.On("Create", res).Return(nil)
			defer tableHandle.AssertExpectations(t)

			client := new(mockClient)
			client.On("TableHandleFrom").Return(tableHandle)
			defer client.AssertExpectations(t)

			clientProvider := new(mockClientProvider)
			clientProvider.On("Get", "secret_value").Return(client, nil)
			defer clientProvider.AssertExpectations(t)

			mcStore := maxcompute.NewMaxComputeDataStore(secretProvider, clientProvider)
			err = mcStore.Create(ctx, res)
			assert.Nil(t, err)
		})
	})
	t.Run("Validate", func(t *testing.T) {
		invalidSpec := map[string]any{
			"description": map[string]any{"some": "desc"},
		}
		specWithoutValues := map[string]any{"a": "b"}
		t.Run("returns error when resource kind is invalid", func(t *testing.T) {
			res, err := resource.NewResource(tableName, "unknown", store, tnnt, &metadata, invalidSpec)
			assert.Nil(t, err)

			mcStore := maxcompute.NewMaxComputeDataStore(nil, nil)
			err = mcStore.Validate(res)
			assert.NotNil(t, err)
			assert.ErrorContains(t, err, "unknown kind")
		})
		t.Run("for table", func(t *testing.T) {
			t.Run("returns error when cannot decode table", func(t *testing.T) {
				res, err := resource.NewResource(tableName, maxcompute.KindTable, store, tnnt, &metadata, invalidSpec)
				assert.Nil(t, err)
				assert.Equal(t, tableName, res.FullName())

				mcStore := maxcompute.NewMaxComputeDataStore(nil, nil)
				err = mcStore.Validate(res)
				assert.NotNil(t, err)
				assert.ErrorContains(t, err, "not able to decode spec for "+tableName)
			})
			t.Run("returns error when decode empty table schema", func(t *testing.T) {
				res, err := resource.NewResource(tableName, maxcompute.KindTable, store, tnnt, &metadata, specWithoutValues)
				assert.Nil(t, err)
				assert.Equal(t, tableName, res.FullName())

				mcStore := maxcompute.NewMaxComputeDataStore(nil, nil)
				err = mcStore.Validate(res)
				assert.NotNil(t, err)
				assert.ErrorContains(t, err, "empty schema for table "+tableName)
			})
		})
	})
	t.Run("GetURN", func(t *testing.T) {
		t.Run("returns urn for resource", func(t *testing.T) {
			expectedURN, err := resource.NewURN(store.String(), tableName)
			assert.NoError(t, err)

			res, err := resource.NewResource(tableName, maxcompute.KindTable, store, tnnt, &metadata, spec)
			assert.NoError(t, err)

			mcStore := maxcompute.NewMaxComputeDataStore(nil, nil)
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

			mcStore := maxcompute.NewMaxComputeDataStore(secretProvider, clientProvider)

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

			mcStore := maxcompute.NewMaxComputeDataStore(secretProvider, clientProvider)

			urn, err := resource.NewURN("maxcompute", "project.table")
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
			clientProvider.On("Get", "secret_value").Return(nil, errors.New("error in client"))
			defer clientProvider.AssertExpectations(t)

			mcStore := maxcompute.NewMaxComputeDataStore(secretProvider, clientProvider)

			urn, err := resource.NewURN("maxcompute", "project.table")
			assert.NoError(t, err)

			actualExist, actualError := mcStore.Exist(ctx, tnnt, urn)
			assert.False(t, actualExist)
			assert.ErrorContains(t, actualError, "error in client")
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

			mcStore := maxcompute.NewMaxComputeDataStore(secretProvider, clientProvider)

			urn, err := resource.NewURN("maxcompute", "table")
			assert.NoError(t, err)

			client.On("TableHandleFrom").Return(tableHandle).Maybe()

			actualExist, actualError := mcStore.Exist(ctx, tnnt, urn)
			assert.True(t, actualExist)
			assert.ErrorContains(t, actualError, "invalid resource name: table")
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

			mcStore := maxcompute.NewMaxComputeDataStore(secretProvider, clientProvider)

			urn, err := resource.NewURN("maxcompute", "project.")
			assert.NoError(t, err)

			client.On("TableHandleFrom").Return(tableHandle).Maybe()

			actualExist, actualError := mcStore.Exist(ctx, tnnt, urn)
			assert.True(t, actualExist)
			assert.ErrorContains(t, actualError, "invalid resource name: project.")
		})
		t.Run("returns true and nil when schema table resource does exist", func(t *testing.T) {
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

			mcStore := maxcompute.NewMaxComputeDataStore(secretProvider, clientProvider)

			urn, err := resource.NewURN("maxcompute", "project.table")
			assert.NoError(t, err)

			client.On("TableHandleFrom").Return(tableHandle).Maybe()
			tableHandle.On("Exists", mock.Anything).Return(true)

			actualExist, actualError := mcStore.Exist(ctx, tnnt, urn)
			assert.True(t, actualExist)
			assert.NoError(t, actualError)
		})
		t.Run("returns false and nil when schema table resource does not exist", func(t *testing.T) {
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

			mcStore := maxcompute.NewMaxComputeDataStore(secretProvider, clientProvider)

			urn, err := resource.NewURN("maxcompute", "project.table")
			assert.NoError(t, err)

			client.On("TableHandleFrom").Return(tableHandle)
			tableHandle.On("Exists", mock.Anything).Return(false)

			actualExist, actualError := mcStore.Exist(ctx, tnnt, urn)
			assert.False(t, actualExist)
			assert.NoError(t, actualError)
		})
	})
}
