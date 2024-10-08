package bigquery_test

import (
	"context"
	"errors"
	"testing"
	"time"

	bq "cloud.google.com/go/bigquery"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/goto/optimus/core/resource"
	"github.com/goto/optimus/core/tenant"
	"github.com/goto/optimus/ext/store/bigquery"
)

func TestBigqueryStore(t *testing.T) {
	ctx := context.Background()
	tnnt, _ := tenant.NewTenant("proj", "ns")
	store := resource.Bigquery
	metadata := resource.Metadata{Description: "meta"}
	spec := map[string]any{"description": "resource"}
	ds := bigquery.Dataset{Project: "project", DatasetName: "dataset"}

	t.Run("Create", func(t *testing.T) {
		t.Run("returns error when secret is not provided", func(t *testing.T) {
			secretProvider := new(mockSecretProvider)
			secretProvider.On("GetSecret", mock.Anything, tnnt, "DATASTORE_BIGQUERY").
				Return(nil, errors.New("not found secret"))
			defer secretProvider.AssertExpectations(t)

			clientProvider := new(mockClientProvider)
			bqStore := bigquery.NewBigqueryDataStore(secretProvider, clientProvider)

			dataset, err := resource.NewResource("project.dataset", bigquery.KindDataset, store, tnnt, &metadata, spec)
			assert.Nil(t, err)

			err = bqStore.Create(ctx, dataset)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "not found secret")
		})
		t.Run("returns error when not able to get client", func(t *testing.T) {
			pts, _ := tenant.NewPlainTextSecret("secret_name", "secret_value")
			secretProvider := new(mockSecretProvider)
			secretProvider.On("GetSecret", mock.Anything, tnnt, "DATASTORE_BIGQUERY").
				Return(pts, nil)
			defer secretProvider.AssertExpectations(t)

			clientProvider := new(mockClientProvider)
			clientProvider.On("Get", mock.Anything, "secret_value").Return(nil, errors.New("error in client"))
			defer clientProvider.AssertExpectations(t)

			bqStore := bigquery.NewBigqueryDataStore(secretProvider, clientProvider)

			dataset, err := resource.NewResource("project.dataset", bigquery.KindDataset, store, tnnt, &metadata, spec)
			assert.Nil(t, err)

			err = bqStore.Create(ctx, dataset)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "error in client")
		})
		t.Run("returns error when kind is invalid", func(t *testing.T) {
			pts, _ := tenant.NewPlainTextSecret("secret_name", "secret_value")
			secretProvider := new(mockSecretProvider)
			secretProvider.On("GetSecret", mock.Anything, tnnt, "DATASTORE_BIGQUERY").
				Return(pts, nil)
			defer secretProvider.AssertExpectations(t)

			client := new(mockClient)
			client.On("Close").Return(nil)
			defer client.AssertExpectations(t)

			clientProvider := new(mockClientProvider)
			clientProvider.On("Get", mock.Anything, "secret_value").Return(client, nil)
			defer clientProvider.AssertExpectations(t)

			bqStore := bigquery.NewBigqueryDataStore(secretProvider, clientProvider)

			dataset, err := resource.NewResource("project.dataset.name", "unknown", store, tnnt, &metadata, spec)
			assert.Nil(t, err)

			err = bqStore.Create(ctx, dataset)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "invalid argument for entity BigqueryStore: invalid kind for bigquery resource unknown")
		})
		t.Run("calls appropriate handler for each dataset", func(t *testing.T) {
			pts, _ := tenant.NewPlainTextSecret("secret_name", "secret_value")
			secretProvider := new(mockSecretProvider)
			secretProvider.On("GetSecret", mock.Anything, tnnt, "DATASTORE_BIGQUERY").
				Return(pts, nil)
			defer secretProvider.AssertExpectations(t)

			dataset, err := resource.NewResource("project.dataset", bigquery.KindDataset, store, tnnt, &metadata, spec)
			assert.Nil(t, err)

			datasetHandle := new(mockTableResourceHandle)
			datasetHandle.On("Create", mock.Anything, dataset).Return(nil)
			defer datasetHandle.AssertExpectations(t)

			client := new(mockClient)
			client.On("Close").Return(nil)
			client.On("DatasetHandleFrom", ds).Return(datasetHandle)
			defer client.AssertExpectations(t)

			clientProvider := new(mockClientProvider)
			clientProvider.On("Get", mock.Anything, "secret_value").Return(client, nil)
			defer clientProvider.AssertExpectations(t)

			bqStore := bigquery.NewBigqueryDataStore(secretProvider, clientProvider)

			err = bqStore.Create(ctx, dataset)
			assert.Nil(t, err)
		})
		t.Run("calls appropriate handler for dataset", func(t *testing.T) {
			pts, _ := tenant.NewPlainTextSecret("secret_name", "secret_value")
			secretProvider := new(mockSecretProvider)
			secretProvider.On("GetSecret", mock.Anything, tnnt, "DATASTORE_BIGQUERY").
				Return(pts, nil)
			defer secretProvider.AssertExpectations(t)

			dataset, err := resource.NewResource("project.dataset", bigquery.KindDataset, store, tnnt, &metadata, spec)
			assert.Nil(t, err)

			datasetHandle := new(mockTableResourceHandle)
			datasetHandle.On("Create", mock.Anything, dataset).Return(nil)
			defer datasetHandle.AssertExpectations(t)

			client := new(mockClient)
			client.On("Close").Return(nil)

			client.On("DatasetHandleFrom", ds).Return(datasetHandle)
			defer client.AssertExpectations(t)

			clientProvider := new(mockClientProvider)
			clientProvider.On("Get", mock.Anything, "secret_value").Return(client, nil)
			defer clientProvider.AssertExpectations(t)

			bqStore := bigquery.NewBigqueryDataStore(secretProvider, clientProvider)

			err = bqStore.Create(ctx, dataset)
			assert.Nil(t, err)
		})
		t.Run("calls appropriate handler for table", func(t *testing.T) {
			pts, _ := tenant.NewPlainTextSecret("secret_name", "secret_value")
			secretProvider := new(mockSecretProvider)
			secretProvider.On("GetSecret", mock.Anything, tnnt, "DATASTORE_BIGQUERY").
				Return(pts, nil)
			defer secretProvider.AssertExpectations(t)

			table, err := resource.NewResource("project.dataset.table", bigquery.KindTable, store, tnnt, &metadata, spec)
			assert.Nil(t, err)

			tableHandle := new(mockTableResourceHandle)
			tableHandle.On("Create", mock.Anything, table).Return(nil)
			defer tableHandle.AssertExpectations(t)

			client := new(mockClient)
			client.On("Close").Return(nil)

			client.On("TableHandleFrom", ds, "table").Return(tableHandle)
			defer client.AssertExpectations(t)

			clientProvider := new(mockClientProvider)
			clientProvider.On("Get", mock.Anything, "secret_value").Return(client, nil)
			defer clientProvider.AssertExpectations(t)

			bqStore := bigquery.NewBigqueryDataStore(secretProvider, clientProvider)

			err = bqStore.Create(ctx, table)
			assert.Nil(t, err)
		})
		t.Run("calls appropriate handler for view", func(t *testing.T) {
			pts, _ := tenant.NewPlainTextSecret("secret_name", "secret_value")
			secretProvider := new(mockSecretProvider)
			secretProvider.On("GetSecret", mock.Anything, tnnt, "DATASTORE_BIGQUERY").
				Return(pts, nil)
			defer secretProvider.AssertExpectations(t)

			view, err := resource.NewResource("project.dataset.view", bigquery.KindView, store, tnnt, &metadata, spec)
			assert.Nil(t, err)

			viewHandle := new(mockTableResourceHandle)
			viewHandle.On("Create", mock.Anything, view).Return(nil)
			defer viewHandle.AssertExpectations(t)

			client := new(mockClient)
			client.On("Close").Return(nil)
			client.On("ViewHandleFrom", ds, "view").Return(viewHandle)
			defer client.AssertExpectations(t)

			clientProvider := new(mockClientProvider)
			clientProvider.On("Get", mock.Anything, "secret_value").Return(client, nil)
			defer clientProvider.AssertExpectations(t)

			bqStore := bigquery.NewBigqueryDataStore(secretProvider, clientProvider)

			err = bqStore.Create(ctx, view)
			assert.Nil(t, err)
		})
		t.Run("calls appropriate handler for each dataset", func(t *testing.T) {
			pts, _ := tenant.NewPlainTextSecret("secret_name", "secret_value")
			secretProvider := new(mockSecretProvider)
			secretProvider.On("GetSecret", mock.Anything, tnnt, "DATASTORE_BIGQUERY").
				Return(pts, nil)
			defer secretProvider.AssertExpectations(t)

			extTable, err := resource.NewResource("project.dataset.extTable", bigquery.KindExternalTable, store, tnnt, &metadata, spec)
			assert.Nil(t, err)

			extTableHandle := new(mockTableResourceHandle)
			extTableHandle.On("Create", mock.Anything, extTable).Return(nil)
			defer extTableHandle.AssertExpectations(t)

			client := new(mockClient)
			client.On("Close").Return(nil)
			client.On("ExternalTableHandleFrom", ds, "extTable").Return(extTableHandle)
			defer client.AssertExpectations(t)

			clientProvider := new(mockClientProvider)
			clientProvider.On("Get", mock.Anything, "secret_value").Return(client, nil)
			defer clientProvider.AssertExpectations(t)

			bqStore := bigquery.NewBigqueryDataStore(secretProvider, clientProvider)

			err = bqStore.Create(ctx, extTable)
			assert.Nil(t, err)
		})

		// v2 spec tests
		metadataV2 := resource.Metadata{
			Version: resource.ResourceSpecV2,
		}
		expectedDs := bigquery.Dataset{Project: "project-new", DatasetName: "dataset-new"}

		t.Run("calls appropriate handler for dataset with v2 spec", func(t *testing.T) {
			specV2 := map[string]any{
				"description": "resource",
				"project":     "project-new",
				"dataset":     "dataset-new",
			}

			pts, _ := tenant.NewPlainTextSecret("secret_name", "secret_value")
			secretProvider := new(mockSecretProvider)
			secretProvider.On("GetSecret", mock.Anything, tnnt, "DATASTORE_BIGQUERY").
				Return(pts, nil)
			defer secretProvider.AssertExpectations(t)

			datasetRes, err := resource.NewResource("project-dataset", bigquery.KindDataset, store, tnnt, &metadataV2, specV2)
			assert.Nil(t, err)

			datasetHandle := new(mockTableResourceHandle)
			datasetHandle.On("Create", mock.Anything, datasetRes).Return(nil)
			defer datasetHandle.AssertExpectations(t)

			client := new(mockClient)
			client.On("Close").Return(nil)
			client.On("DatasetHandleFrom", expectedDs).Return(datasetHandle)
			defer client.AssertExpectations(t)

			clientProvider := new(mockClientProvider)
			clientProvider.On("Get", mock.Anything, "secret_value").Return(client, nil)
			defer clientProvider.AssertExpectations(t)

			bqStore := bigquery.NewBigqueryDataStore(secretProvider, clientProvider)

			err = bqStore.Create(ctx, datasetRes)
			assert.Nil(t, err)
		})

		t.Run("calls appropriate handler for table with v2 spec", func(t *testing.T) {
			specV2 := map[string]any{
				"description": "resource",
				"project":     "project-new",
				"dataset":     "dataset-new",
				"name":        "table-new",
			}

			pts, _ := tenant.NewPlainTextSecret("secret_name", "secret_value")
			secretProvider := new(mockSecretProvider)
			secretProvider.On("GetSecret", mock.Anything, tnnt, "DATASTORE_BIGQUERY").
				Return(pts, nil)
			defer secretProvider.AssertExpectations(t)

			tableRes, err := resource.NewResource("project.dataset.table", bigquery.KindTable, store, tnnt, &metadataV2, specV2)
			assert.Nil(t, err)

			tableHandle := new(mockTableResourceHandle)
			tableHandle.On("Create", mock.Anything, tableRes).Return(nil)
			defer tableHandle.AssertExpectations(t)

			client := new(mockClient)
			client.On("Close").Return(nil)
			client.On("TableHandleFrom", expectedDs, "table-new").Return(tableHandle)
			defer client.AssertExpectations(t)

			clientProvider := new(mockClientProvider)
			clientProvider.On("Get", mock.Anything, "secret_value").Return(client, nil)
			defer clientProvider.AssertExpectations(t)

			bqStore := bigquery.NewBigqueryDataStore(secretProvider, clientProvider)

			err = bqStore.Create(ctx, tableRes)
			assert.Nil(t, err)
		})

		t.Run("calls appropriate handler for view with v2 spec", func(t *testing.T) {
			specV2 := map[string]any{
				"description": "resource",
				"project":     "project-new",
				"dataset":     "dataset-new",
				"name":        "view-new",
			}

			pts, _ := tenant.NewPlainTextSecret("secret_name", "secret_value")
			secretProvider := new(mockSecretProvider)
			secretProvider.On("GetSecret", mock.Anything, tnnt, "DATASTORE_BIGQUERY").
				Return(pts, nil)
			defer secretProvider.AssertExpectations(t)

			viewRes, err := resource.NewResource("project.dataset.table", bigquery.KindView, store, tnnt, &metadataV2, specV2)
			assert.NoError(t, err)

			viewHandle := new(mockTableResourceHandle)
			viewHandle.On("Create", mock.Anything, viewRes).Return(nil)
			defer viewHandle.AssertExpectations(t)

			client := new(mockClient)
			client.On("Close").Return(nil)
			client.On("ViewHandleFrom", expectedDs, "view-new").Return(viewHandle)
			defer client.AssertExpectations(t)

			clientProvider := new(mockClientProvider)
			clientProvider.On("Get", mock.Anything, "secret_value").Return(client, nil)
			defer clientProvider.AssertExpectations(t)

			bqStore := bigquery.NewBigqueryDataStore(secretProvider, clientProvider)

			err = bqStore.Create(ctx, viewRes)
			assert.Nil(t, err)
		})
	})
	t.Run("Update", func(t *testing.T) {
		t.Run("returns error when secret is not provided", func(t *testing.T) {
			secretProvider := new(mockSecretProvider)
			secretProvider.On("GetSecret", mock.Anything, tnnt, "DATASTORE_BIGQUERY").
				Return(nil, errors.New("not found secret"))
			defer secretProvider.AssertExpectations(t)

			clientProvider := new(mockClientProvider)
			bqStore := bigquery.NewBigqueryDataStore(secretProvider, clientProvider)

			dataset, err := resource.NewResource("project.dataset", bigquery.KindDataset, store, tnnt, &metadata, spec)
			assert.Nil(t, err)

			err = bqStore.Update(ctx, dataset)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "not found secret")
		})
		t.Run("returns error when not able to get client", func(t *testing.T) {
			pts, _ := tenant.NewPlainTextSecret("secret_name", "secret_value")
			secretProvider := new(mockSecretProvider)
			secretProvider.On("GetSecret", mock.Anything, tnnt, "DATASTORE_BIGQUERY").
				Return(pts, nil)
			defer secretProvider.AssertExpectations(t)

			clientProvider := new(mockClientProvider)
			clientProvider.On("Get", mock.Anything, "secret_value").Return(nil, errors.New("error in client"))
			defer clientProvider.AssertExpectations(t)

			bqStore := bigquery.NewBigqueryDataStore(secretProvider, clientProvider)

			dataset, err := resource.NewResource("project.dataset", bigquery.KindDataset, store, tnnt, &metadata, spec)
			assert.Nil(t, err)

			err = bqStore.Update(ctx, dataset)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "error in client")
		})
		t.Run("returns error when kind is invalid", func(t *testing.T) {
			pts, _ := tenant.NewPlainTextSecret("secret_name", "secret_value")
			secretProvider := new(mockSecretProvider)
			secretProvider.On("GetSecret", mock.Anything, tnnt, "DATASTORE_BIGQUERY").
				Return(pts, nil)
			defer secretProvider.AssertExpectations(t)
			client := new(mockClient)
			client.On("Close").Return(nil)
			defer client.AssertExpectations(t)

			clientProvider := new(mockClientProvider)
			clientProvider.On("Get", mock.Anything, "secret_value").Return(client, nil)
			defer clientProvider.AssertExpectations(t)

			bqStore := bigquery.NewBigqueryDataStore(secretProvider, clientProvider)

			dataset, err := resource.NewResource("project.dataset.name1", "unknown", store, tnnt, &metadata, spec)
			assert.Nil(t, err)

			err = bqStore.Update(ctx, dataset)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "invalid argument for entity BigqueryStore: invalid kind for bigquery resource unknown")
		})
		t.Run("calls appropriate handler for each dataset", func(t *testing.T) {
			pts, _ := tenant.NewPlainTextSecret("secret_name", "secret_value")
			secretProvider := new(mockSecretProvider)
			secretProvider.On("GetSecret", mock.Anything, tnnt, "DATASTORE_BIGQUERY").
				Return(pts, nil)
			defer secretProvider.AssertExpectations(t)

			dataset, err := resource.NewResource("project.dataset", bigquery.KindDataset, store, tnnt, &metadata, spec)
			assert.Nil(t, err)

			datasetHandle := new(mockTableResourceHandle)
			datasetHandle.On("Update", mock.Anything, dataset).Return(nil)
			defer datasetHandle.AssertExpectations(t)

			client := new(mockClient)
			client.On("Close").Return(nil)

			client.On("DatasetHandleFrom", ds).Return(datasetHandle)
			defer client.AssertExpectations(t)

			clientProvider := new(mockClientProvider)
			clientProvider.On("Get", mock.Anything, "secret_value").Return(client, nil)

			bqStore := bigquery.NewBigqueryDataStore(secretProvider, clientProvider)

			err = bqStore.Update(ctx, dataset)
			assert.Nil(t, err)
		})
		t.Run("calls appropriate handler for dataset", func(t *testing.T) {
			pts, _ := tenant.NewPlainTextSecret("secret_name", "secret_value")
			secretProvider := new(mockSecretProvider)
			secretProvider.On("GetSecret", mock.Anything, tnnt, "DATASTORE_BIGQUERY").
				Return(pts, nil)
			defer secretProvider.AssertExpectations(t)

			dataset, err := resource.NewResource("project.dataset", bigquery.KindDataset, store, tnnt, &metadata, spec)
			assert.Nil(t, err)

			datasetHandle := new(mockTableResourceHandle)
			datasetHandle.On("Update", mock.Anything, dataset).Return(nil)
			defer datasetHandle.AssertExpectations(t)

			client := new(mockClient)
			client.On("Close").Return(nil)

			client.On("DatasetHandleFrom", ds).Return(datasetHandle)
			defer client.AssertExpectations(t)

			clientProvider := new(mockClientProvider)
			clientProvider.On("Get", mock.Anything, "secret_value").Return(client, nil)

			bqStore := bigquery.NewBigqueryDataStore(secretProvider, clientProvider)

			err = bqStore.Update(ctx, dataset)
			assert.Nil(t, err)
		})
		t.Run("calls appropriate handler for table", func(t *testing.T) {
			pts, _ := tenant.NewPlainTextSecret("secret_name", "secret_value")
			secretProvider := new(mockSecretProvider)
			secretProvider.On("GetSecret", mock.Anything, tnnt, "DATASTORE_BIGQUERY").
				Return(pts, nil)
			defer secretProvider.AssertExpectations(t)

			table, err := resource.NewResource("project.dataset.table", bigquery.KindTable, store, tnnt, &metadata, spec)
			assert.Nil(t, err)

			tableHandle := new(mockTableResourceHandle)
			tableHandle.On("Update", mock.Anything, table).Return(nil)
			defer tableHandle.AssertExpectations(t)

			client := new(mockClient)
			client.On("Close").Return(nil)

			client.On("TableHandleFrom", ds, "table").Return(tableHandle)
			defer client.AssertExpectations(t)

			clientProvider := new(mockClientProvider)
			clientProvider.On("Get", mock.Anything, "secret_value").Return(client, nil)

			bqStore := bigquery.NewBigqueryDataStore(secretProvider, clientProvider)

			err = bqStore.Update(ctx, table)
			assert.Nil(t, err)
		})
		t.Run("calls appropriate handler for view", func(t *testing.T) {
			pts, _ := tenant.NewPlainTextSecret("secret_name", "secret_value")
			secretProvider := new(mockSecretProvider)
			secretProvider.On("GetSecret", mock.Anything, tnnt, "DATASTORE_BIGQUERY").
				Return(pts, nil)
			defer secretProvider.AssertExpectations(t)

			view, err := resource.NewResource("project.dataset.view", bigquery.KindView, store, tnnt, &metadata, spec)
			assert.Nil(t, err)

			viewHandle := new(mockTableResourceHandle)
			viewHandle.On("Update", mock.Anything, view).Return(nil)
			defer viewHandle.AssertExpectations(t)

			client := new(mockClient)
			client.On("Close").Return(nil)
			client.On("ViewHandleFrom", ds, "view").Return(viewHandle)
			defer client.AssertExpectations(t)

			clientProvider := new(mockClientProvider)
			clientProvider.On("Get", mock.Anything, "secret_value").Return(client, nil)
			defer clientProvider.AssertExpectations(t)

			bqStore := bigquery.NewBigqueryDataStore(secretProvider, clientProvider)

			err = bqStore.Update(ctx, view)
			assert.Nil(t, err)
		})
		t.Run("calls appropriate handler for each dataset", func(t *testing.T) {
			pts, _ := tenant.NewPlainTextSecret("secret_name", "secret_value")
			secretProvider := new(mockSecretProvider)
			secretProvider.On("GetSecret", mock.Anything, tnnt, "DATASTORE_BIGQUERY").
				Return(pts, nil)
			defer secretProvider.AssertExpectations(t)

			extTable, err := resource.NewResource("project.dataset.extTable", bigquery.KindExternalTable, store, tnnt, &metadata, spec)
			assert.Nil(t, err)

			extTableHandle := new(mockTableResourceHandle)
			extTableHandle.On("Update", mock.Anything, extTable).Return(nil)
			defer extTableHandle.AssertExpectations(t)

			client := new(mockClient)
			client.On("Close").Return(nil)
			client.On("ExternalTableHandleFrom", ds, "extTable").Return(extTableHandle)
			defer client.AssertExpectations(t)

			clientProvider := new(mockClientProvider)
			clientProvider.On("Get", mock.Anything, "secret_value").Return(client, nil)

			bqStore := bigquery.NewBigqueryDataStore(secretProvider, clientProvider)

			err = bqStore.Update(ctx, extTable)
			assert.Nil(t, err)
		})

		// v2 spec tests
		metadataV2 := resource.Metadata{
			Version: resource.ResourceSpecV2,
		}
		expectedDs := bigquery.Dataset{Project: "project-new", DatasetName: "dataset-new"}

		t.Run("calls appropriate handler for dataset with v2 spec", func(t *testing.T) {
			specV2 := map[string]any{
				"description": "resource",
				"project":     "project-new",
				"dataset":     "dataset-new",
			}

			pts, _ := tenant.NewPlainTextSecret("secret_name", "secret_value")
			secretProvider := new(mockSecretProvider)
			secretProvider.On("GetSecret", mock.Anything, tnnt, "DATASTORE_BIGQUERY").
				Return(pts, nil)
			defer secretProvider.AssertExpectations(t)

			datasetRes, err := resource.NewResource("project-dataset", bigquery.KindDataset, store, tnnt, &metadataV2, specV2)
			assert.Nil(t, err)

			datasetHandle := new(mockTableResourceHandle)
			datasetHandle.On("Update", mock.Anything, datasetRes).Return(nil)
			defer datasetHandle.AssertExpectations(t)

			client := new(mockClient)
			client.On("Close").Return(nil)
			client.On("DatasetHandleFrom", expectedDs).Return(datasetHandle)
			defer client.AssertExpectations(t)

			clientProvider := new(mockClientProvider)
			clientProvider.On("Get", mock.Anything, "secret_value").Return(client, nil)
			defer clientProvider.AssertExpectations(t)

			bqStore := bigquery.NewBigqueryDataStore(secretProvider, clientProvider)

			err = bqStore.Update(ctx, datasetRes)
			assert.Nil(t, err)
		})

		t.Run("calls appropriate handler for table with v2 spec", func(t *testing.T) {
			specV2 := map[string]any{
				"description": "resource",
				"project":     "project-new",
				"dataset":     "dataset-new",
				"name":        "table-new",
			}

			pts, _ := tenant.NewPlainTextSecret("secret_name", "secret_value")
			secretProvider := new(mockSecretProvider)
			secretProvider.On("GetSecret", mock.Anything, tnnt, "DATASTORE_BIGQUERY").
				Return(pts, nil)
			defer secretProvider.AssertExpectations(t)

			tableRes, err := resource.NewResource("project.dataset.table", bigquery.KindTable, store, tnnt, &metadataV2, specV2)
			assert.Nil(t, err)

			tableHandle := new(mockTableResourceHandle)
			tableHandle.On("Update", mock.Anything, tableRes).Return(nil)
			defer tableHandle.AssertExpectations(t)

			client := new(mockClient)
			client.On("Close").Return(nil)
			client.On("TableHandleFrom", expectedDs, "table-new").Return(tableHandle)
			defer client.AssertExpectations(t)

			clientProvider := new(mockClientProvider)
			clientProvider.On("Get", mock.Anything, "secret_value").Return(client, nil)
			defer clientProvider.AssertExpectations(t)

			bqStore := bigquery.NewBigqueryDataStore(secretProvider, clientProvider)

			err = bqStore.Update(ctx, tableRes)
			assert.Nil(t, err)
		})

		t.Run("calls appropriate handler for view with v2 spec", func(t *testing.T) {
			specV2 := map[string]any{
				"description": "resource",
				"project":     "project-new",
				"dataset":     "dataset-new",
				"name":        "view-new",
			}

			pts, _ := tenant.NewPlainTextSecret("secret_name", "secret_value")
			secretProvider := new(mockSecretProvider)
			secretProvider.On("GetSecret", mock.Anything, tnnt, "DATASTORE_BIGQUERY").
				Return(pts, nil)
			defer secretProvider.AssertExpectations(t)

			viewRes, err := resource.NewResource("project.dataset.table", bigquery.KindView, store, tnnt, &metadataV2, specV2)
			assert.NoError(t, err)

			viewHandle := new(mockTableResourceHandle)
			viewHandle.On("Update", mock.Anything, viewRes).Return(nil)
			defer viewHandle.AssertExpectations(t)

			client := new(mockClient)
			client.On("Close").Return(nil)
			client.On("ViewHandleFrom", expectedDs, "view-new").Return(viewHandle)
			defer client.AssertExpectations(t)

			clientProvider := new(mockClientProvider)
			clientProvider.On("Get", mock.Anything, "secret_value").Return(client, nil)
			defer clientProvider.AssertExpectations(t)

			bqStore := bigquery.NewBigqueryDataStore(secretProvider, clientProvider)

			err = bqStore.Update(ctx, viewRes)
			assert.Nil(t, err)
		})
	})
	t.Run("BatchUpdate", func(t *testing.T) {
		t.Run("returns no error when empty list", func(t *testing.T) {
			bqStore := bigquery.NewBigqueryDataStore(nil, nil)

			err := bqStore.BatchUpdate(ctx, []*resource.Resource{})
			assert.NoError(t, err)
		})
		t.Run("returns error when cannot get secret", func(t *testing.T) {
			secretProvider := new(mockSecretProvider)
			secretProvider.On("GetSecret", mock.Anything, tnnt, "DATASTORE_BIGQUERY").
				Return(nil, errors.New("not found secret"))
			defer secretProvider.AssertExpectations(t)

			clientProvider := new(mockClientProvider)
			bqStore := bigquery.NewBigqueryDataStore(secretProvider, clientProvider)

			dataset, err := resource.NewResource("project.dataset", bigquery.KindDataset, store, tnnt, &metadata, spec)
			assert.Nil(t, err)

			err = bqStore.BatchUpdate(ctx, []*resource.Resource{dataset})
			assert.NotNil(t, err)
			assert.EqualError(t, err, "not found secret")
		})
		t.Run("returns error when cannot create client", func(t *testing.T) {
			pts, _ := tenant.NewPlainTextSecret("secret_name", "secret_value")
			secretProvider := new(mockSecretProvider)
			secretProvider.On("GetSecret", mock.Anything, tnnt, "DATASTORE_BIGQUERY").
				Return(pts, nil)
			defer secretProvider.AssertExpectations(t)

			dataset, err := resource.NewResource("project.dataset", bigquery.KindDataset, store, tnnt, &metadata, spec)
			assert.Nil(t, err)
			updateDS := resource.FromExisting(dataset, resource.ReplaceStatus(resource.StatusToUpdate))

			clientProvider := new(mockClientProvider)
			clientProvider.On("Get", mock.Anything, "secret_value").
				Return(nil, errors.New("some error"))

			bqStore := bigquery.NewBigqueryDataStore(secretProvider, clientProvider)

			err = bqStore.BatchUpdate(ctx, []*resource.Resource{updateDS})
			assert.NotNil(t, err)
			assert.EqualError(t, err, "some error")
		})
		t.Run("returns error when one or more job fails", func(t *testing.T) {
			pts, _ := tenant.NewPlainTextSecret("secret_name", "secret_value")
			secretProvider := new(mockSecretProvider)
			secretProvider.On("GetSecret", mock.Anything, tnnt, "DATASTORE_BIGQUERY").
				Return(pts, nil)
			defer secretProvider.AssertExpectations(t)

			dataset, err := resource.NewResource("project.dataset", bigquery.KindDataset, store, tnnt, &metadata, spec)
			assert.Nil(t, err)
			updateDS := resource.FromExisting(dataset, resource.ReplaceStatus(resource.StatusToUpdate))

			datasetHandle := new(mockTableResourceHandle)
			datasetHandle.On("Update", mock.Anything, updateDS).Return(errors.New("failed to update"))
			defer datasetHandle.AssertExpectations(t)

			client := new(mockClient)
			client.On("DatasetHandleFrom", ds).Return(datasetHandle)
			defer client.AssertExpectations(t)

			clientProvider := new(mockClientProvider)
			clientProvider.On("Get", mock.Anything, "secret_value").Return(client, nil)

			bqStore := bigquery.NewBigqueryDataStore(secretProvider, clientProvider)

			err = bqStore.BatchUpdate(ctx, []*resource.Resource{updateDS})
			assert.NotNil(t, err)
			assert.EqualError(t, err, "failed to update")
		})
		t.Run("returns no error when successfully updates", func(t *testing.T) {
			pts, _ := tenant.NewPlainTextSecret("secret_name", "secret_value")
			secretProvider := new(mockSecretProvider)
			secretProvider.On("GetSecret", mock.Anything, tnnt, "DATASTORE_BIGQUERY").
				Return(pts, nil)
			defer secretProvider.AssertExpectations(t)

			dataset, err := resource.NewResource("project.dataset", bigquery.KindDataset, store, tnnt, &metadata, spec)
			assert.Nil(t, err)
			updateDS := resource.FromExisting(dataset, resource.ReplaceStatus(resource.StatusToUpdate))

			datasetHandle := new(mockTableResourceHandle)
			datasetHandle.On("Update", mock.Anything, updateDS).Return(nil)
			defer datasetHandle.AssertExpectations(t)

			client := new(mockClient)
			client.On("DatasetHandleFrom", ds).Return(datasetHandle)
			defer client.AssertExpectations(t)

			clientProvider := new(mockClientProvider)
			clientProvider.On("Get", mock.Anything, "secret_value").Return(client, nil)

			bqStore := bigquery.NewBigqueryDataStore(secretProvider, clientProvider)

			err = bqStore.BatchUpdate(ctx, []*resource.Resource{updateDS})
			assert.Nil(t, err)
		})

		t.Run("returns no error when successfully updates with delete resource", func(t *testing.T) {
			pts, _ := tenant.NewPlainTextSecret("secret_name", "secret_value")
			secretProvider := new(mockSecretProvider)
			secretProvider.On("GetSecret", mock.Anything, tnnt, "DATASTORE_BIGQUERY").
				Return(pts, nil)
			defer secretProvider.AssertExpectations(t)

			dataset, err := resource.NewResource("project.dataset", bigquery.KindDataset, store, tnnt, &metadata, spec)
			assert.Nil(t, err)
			deleteDS := resource.FromExisting(dataset, resource.ReplaceStatus(resource.StatusToDelete))

			datasetHandle := new(mockTableResourceHandle)
			defer datasetHandle.AssertExpectations(t)

			client := new(mockClient)
			client.On("DatasetHandleFrom", ds).Return(datasetHandle)
			defer client.AssertExpectations(t)

			clientProvider := new(mockClientProvider)
			clientProvider.On("Get", mock.Anything, "secret_value").Return(client, nil)

			bqStore := bigquery.NewBigqueryDataStore(secretProvider, clientProvider)

			err = bqStore.BatchUpdate(ctx, []*resource.Resource{deleteDS})
			assert.Nil(t, err)
			assert.EqualValues(t, deleteDS.Status(), resource.StatusDeleted)
		})
	})
	t.Run("Validate", func(t *testing.T) {
		invalidSpec := map[string]any{
			"description": map[string]any{"some": "desc"},
		}
		specWithoutValues := map[string]any{"a": "b"}
		t.Run("returns error for invalid resource", func(t *testing.T) {
			res := resource.Resource{}
			bqStore := bigquery.NewBigqueryDataStore(nil, nil)
			err := bqStore.Validate(&res)
			assert.NotNil(t, err)
			assert.ErrorContains(t, err, "invalid sections in name:")
		})
		t.Run("returns error for invalid resource kind", func(t *testing.T) {
			res, err := resource.NewResource("project.set.view_name1", "unknown", resource.Bigquery,
				tnnt, &resource.Metadata{}, invalidSpec)
			assert.NoError(t, err)

			bqStore := bigquery.NewBigqueryDataStore(nil, nil)
			err = bqStore.Validate(res)
			assert.NotNil(t, err)
			assert.ErrorContains(t, err, "unknown kind")
		})
		t.Run("for view", func(t *testing.T) {
			t.Run("returns error when cannot decode view spec", func(t *testing.T) {
				res, err := resource.NewResource("project.set.view_name1", bigquery.KindView, resource.Bigquery,
					tnnt, &resource.Metadata{}, invalidSpec)
				assert.Nil(t, err)

				assert.Equal(t, "project.set.view_name1", res.FullName())

				bqStore := bigquery.NewBigqueryDataStore(nil, nil)
				err = bqStore.Validate(res)
				assert.NotNil(t, err)
				assert.ErrorContains(t, err, "not able to decode spec for project.set.view_name1")
			})
			t.Run("returns error for validation failure", func(t *testing.T) {
				res, err := resource.NewResource("project.set.view_name1", bigquery.KindView, resource.Bigquery,
					tnnt, &resource.Metadata{}, specWithoutValues)
				assert.Nil(t, err)

				assert.Equal(t, "project.set.view_name1", res.FullName())
				bqStore := bigquery.NewBigqueryDataStore(nil, nil)
				err = bqStore.Validate(res)
				assert.NotNil(t, err)
				assert.ErrorContains(t, err, "view query is empty for project.set.view_name1")
			})
		})
		t.Run("for external_table", func(t *testing.T) {
			t.Run("returns error when cannot decode spec", func(t *testing.T) {
				res, err := resource.NewResource("project.set.external_name1", bigquery.KindExternalTable, resource.Bigquery,
					tnnt, &resource.Metadata{}, invalidSpec)
				assert.Nil(t, err)

				assert.Equal(t, "project.set.external_name1", res.FullName())
				bqStore := bigquery.NewBigqueryDataStore(nil, nil)
				err = bqStore.Validate(res)
				assert.NotNil(t, err)
				assert.ErrorContains(t, err, "not able to decode spec for project.set.external_name1")
			})
			t.Run("returns error when external_table spec is invalid", func(t *testing.T) {
				res, err := resource.NewResource("project.set.external_name1", bigquery.KindExternalTable, resource.Bigquery,
					tnnt, &resource.Metadata{}, specWithoutValues)
				assert.Nil(t, err)

				assert.Equal(t, "project.set.external_name1", res.FullName())
				bqStore := bigquery.NewBigqueryDataStore(nil, nil)
				err = bqStore.Validate(res)
				assert.NotNil(t, err)
				assert.ErrorContains(t, err, "empty external table source for project.set.external_name1")
			})
		})
		t.Run("for table", func(t *testing.T) {
			t.Run("returns error when cannot decode table", func(t *testing.T) {
				res, err := resource.NewResource("project.set.table_name1", bigquery.KindTable, resource.Bigquery,
					tnnt, &resource.Metadata{}, invalidSpec)
				assert.Nil(t, err)

				assert.Equal(t, "project.set.table_name1", res.FullName())
				bqStore := bigquery.NewBigqueryDataStore(nil, nil)
				err = bqStore.Validate(res)
				assert.NotNil(t, err)
				assert.ErrorContains(t, err, "not able to decode spec for project.set.table_name1")
			})
			t.Run("returns error when cannot decode table", func(t *testing.T) {
				res, err := resource.NewResource("project.set.table_name1", bigquery.KindTable, resource.Bigquery,
					tnnt, &resource.Metadata{}, specWithoutValues)
				assert.Nil(t, err)

				assert.Equal(t, "project.set.table_name1", res.FullName())
				bqStore := bigquery.NewBigqueryDataStore(nil, nil)
				err = bqStore.Validate(res)
				assert.NotNil(t, err)
				assert.ErrorContains(t, err, "empty schema for table project.set.table_name1")
			})
		})
		t.Run("for dataset", func(t *testing.T) {
			t.Run("returns error when cannot decode dataset", func(t *testing.T) {
				res, err := resource.NewResource("project.set_name1", bigquery.KindDataset, resource.Bigquery,
					tnnt, &resource.Metadata{}, invalidSpec)
				assert.Nil(t, err)
				bqStore := bigquery.NewBigqueryDataStore(nil, nil)
				err = bqStore.Validate(res)
				assert.NotNil(t, err)
				assert.ErrorContains(t, err, "not able to decode spec for project.set_name1")
			})
			t.Run("returns no error when validation passes", func(t *testing.T) {
				res, err := resource.NewResource("project.set_name1", bigquery.KindDataset, resource.Bigquery,
					tnnt, &resource.Metadata{}, specWithoutValues)
				assert.Nil(t, err)
				bqStore := bigquery.NewBigqueryDataStore(nil, nil)
				err = bqStore.Validate(res)
				assert.Nil(t, err)
			})
		})
	})
	t.Run("URNFor", func(t *testing.T) {
		t.Run("returns urn for resource", func(t *testing.T) {
			res, err := resource.NewResource("project.set.view_name1", "unknown", resource.Bigquery,
				tnnt, &resource.Metadata{}, spec)
			assert.NoError(t, err)

			expectedURN, err := resource.ParseURN("bigquery://project:set.view_name1")
			assert.NoError(t, err)

			bqStore := bigquery.NewBigqueryDataStore(nil, nil)
			actualURN, err := bqStore.GetURN(res)
			assert.NoError(t, err)
			assert.Equal(t, expectedURN, actualURN)
		})
	})
	t.Run("Backup", func(t *testing.T) {
		createdAt := time.Date(2022, 11, 18, 1, 0, 0, 0, time.UTC)
		backup, backupErr := resource.NewBackup(store, tnnt, []string{"p.d.t"}, "", createdAt, nil)
		assert.NoError(t, backupErr)

		t.Run("returns error when cannot get secret", func(t *testing.T) {
			secretProvider := new(mockSecretProvider)
			secretProvider.On("GetSecret", mock.Anything, tnnt, "DATASTORE_BIGQUERY").
				Return(nil, errors.New("not found secret"))
			defer secretProvider.AssertExpectations(t)

			clientProvider := new(mockClientProvider)
			bqStore := bigquery.NewBigqueryDataStore(secretProvider, clientProvider)

			dataset, err := resource.NewResource("project.dataset", bigquery.KindDataset, store, tnnt, &metadata, spec)
			assert.Nil(t, err)

			_, err = bqStore.Backup(ctx, backup, []*resource.Resource{dataset})
			assert.NotNil(t, err)
			assert.EqualError(t, err, "not found secret")
		})
		t.Run("returns error when cannot create client", func(t *testing.T) {
			pts, _ := tenant.NewPlainTextSecret("secret_name", "secret_value")
			secretProvider := new(mockSecretProvider)
			secretProvider.On("GetSecret", mock.Anything, tnnt, "DATASTORE_BIGQUERY").
				Return(pts, nil)
			defer secretProvider.AssertExpectations(t)

			clientProvider := new(mockClientProvider)
			clientProvider.On("Get", mock.Anything, "secret_value").Return(nil, errors.New("error in client"))
			defer clientProvider.AssertExpectations(t)

			bqStore := bigquery.NewBigqueryDataStore(secretProvider, clientProvider)

			dataset, err := resource.NewResource("project.dataset", bigquery.KindDataset, store, tnnt, &metadata, spec)
			assert.Nil(t, err)

			_, err = bqStore.Backup(ctx, backup, []*resource.Resource{dataset})
			assert.NotNil(t, err)
			assert.EqualError(t, err, "error in client")
		})
		t.Run("calls backup resources to backup the resources", func(t *testing.T) {
			pts, _ := tenant.NewPlainTextSecret("secret_name", "secret_value")
			secretProvider := new(mockSecretProvider)
			secretProvider.On("GetSecret", mock.Anything, tnnt, "DATASTORE_BIGQUERY").
				Return(pts, nil)
			defer secretProvider.AssertExpectations(t)

			client := new(mockClient)
			client.On("Close").Return(nil)
			defer client.AssertExpectations(t)

			clientProvider := new(mockClientProvider)
			clientProvider.On("Get", mock.Anything, "secret_value").Return(client, nil)
			defer clientProvider.AssertExpectations(t)

			bqStore := bigquery.NewBigqueryDataStore(secretProvider, clientProvider)

			dataset, err := resource.NewResource("project.dataset", bigquery.KindDataset, store, tnnt, &metadata, spec)
			assert.Nil(t, err)

			result, err := bqStore.Backup(ctx, backup, []*resource.Resource{dataset})
			assert.NoError(t, err)
			assert.Equal(t, 1, len(result.IgnoredResources))
		})
	})
	t.Run("Exist", func(t *testing.T) {
		t.Run("returns false and error if store is not bigquery", func(t *testing.T) {
			secretProvider := new(mockSecretProvider)
			defer secretProvider.AssertExpectations(t)

			extTableHandle := new(mockTableResourceHandle)
			defer extTableHandle.AssertExpectations(t)

			client := new(mockClient)
			defer client.AssertExpectations(t)

			clientProvider := new(mockClientProvider)
			defer clientProvider.AssertExpectations(t)

			bqStore := bigquery.NewBigqueryDataStore(secretProvider, clientProvider)

			urn, err := resource.NewURN("random_store", "project.dataset.table")
			assert.NoError(t, err)

			actualExist, actualError := bqStore.Exist(ctx, tnnt, urn)

			assert.False(t, actualExist)
			assert.ErrorContains(t, actualError, "expected store [bigquery] but received [random_store]")
		})

		t.Run("returns false and error if secret provider returned error", func(t *testing.T) {
			secretProvider := new(mockSecretProvider)
			defer secretProvider.AssertExpectations(t)

			extTableHandle := new(mockTableResourceHandle)
			defer extTableHandle.AssertExpectations(t)

			client := new(mockClient)
			defer client.AssertExpectations(t)

			clientProvider := new(mockClientProvider)
			defer clientProvider.AssertExpectations(t)

			bqStore := bigquery.NewBigqueryDataStore(secretProvider, clientProvider)

			urn, err := resource.NewURN("bigquery", "project.dataset.table")
			assert.NoError(t, err)

			secretProvider.On("GetSecret", mock.Anything, tnnt, "DATASTORE_BIGQUERY").Return(nil, errors.New("expected error for testing"))

			actualExist, actualError := bqStore.Exist(ctx, tnnt, urn)

			assert.False(t, actualExist)
			assert.ErrorContains(t, actualError, "expected error for testing")
		})

		t.Run("returns false and error if client provider returned error", func(t *testing.T) {
			secretProvider := new(mockSecretProvider)
			defer secretProvider.AssertExpectations(t)

			extTableHandle := new(mockTableResourceHandle)
			defer extTableHandle.AssertExpectations(t)

			client := new(mockClient)
			defer client.AssertExpectations(t)

			clientProvider := new(mockClientProvider)
			defer clientProvider.AssertExpectations(t)

			bqStore := bigquery.NewBigqueryDataStore(secretProvider, clientProvider)

			urn, err := resource.NewURN("bigquery", "project.dataset.table")
			assert.NoError(t, err)

			pts, _ := tenant.NewPlainTextSecret("secret_name", "secret_value")
			secretProvider.On("GetSecret", mock.Anything, tnnt, "DATASTORE_BIGQUERY").Return(pts, nil)

			clientProvider.On("Get", mock.Anything, pts.Value()).Return(nil, errors.New("expected error for testing"))

			actualExist, actualError := bqStore.Exist(ctx, tnnt, urn)

			assert.False(t, actualExist)
			assert.ErrorContains(t, actualError, "expected error for testing")
		})

		t.Run("returns false and error if dataset name is invalid", func(t *testing.T) {
			secretProvider := new(mockSecretProvider)
			defer secretProvider.AssertExpectations(t)

			extTableHandle := new(mockTableResourceHandle)
			defer extTableHandle.AssertExpectations(t)

			client := new(mockClient)
			defer client.AssertExpectations(t)

			clientProvider := new(mockClientProvider)
			defer clientProvider.AssertExpectations(t)

			bqStore := bigquery.NewBigqueryDataStore(secretProvider, clientProvider)

			urn, err := resource.NewURN("bigquery", "invalid_name")
			assert.NoError(t, err)

			pts, _ := tenant.NewPlainTextSecret("secret_name", "secret_value")
			secretProvider.On("GetSecret", mock.Anything, tnnt, "DATASTORE_BIGQUERY").Return(pts, nil)

			clientProvider.On("Get", mock.Anything, pts.Value()).Return(client, nil)

			client.On("Close").Return(nil)

			actualExist, actualError := bqStore.Exist(ctx, tnnt, urn)

			assert.False(t, actualExist)
			assert.ErrorContains(t, actualError, "invalid dataset name")
		})

		t.Run("returns true and nil if dataset exists", func(t *testing.T) {
			secretProvider := new(mockSecretProvider)
			defer secretProvider.AssertExpectations(t)

			extTableHandle := new(mockTableResourceHandle)
			defer extTableHandle.AssertExpectations(t)

			client := new(mockClient)
			defer client.AssertExpectations(t)

			clientProvider := new(mockClientProvider)
			defer clientProvider.AssertExpectations(t)

			handle := new(mockTableResourceHandle)
			defer handle.AssertExpectations(t)

			bqStore := bigquery.NewBigqueryDataStore(secretProvider, clientProvider)

			urn, err := resource.NewURN("bigquery", "project.dataset")
			assert.NoError(t, err)

			pts, _ := tenant.NewPlainTextSecret("secret_name", "secret_value")
			secretProvider.On("GetSecret", mock.Anything, tnnt, "DATASTORE_BIGQUERY").Return(pts, nil)

			clientProvider.On("Get", mock.Anything, pts.Value()).Return(client, nil)

			client.On("Close").Return(nil)

			handle.On("Exists", mock.Anything).Return(true)

			client.On("DatasetHandleFrom", mock.Anything).Return(handle)

			actualExist, actualError := bqStore.Exist(ctx, tnnt, urn)

			assert.True(t, actualExist)
			assert.NoError(t, actualError)
		})

		t.Run("returns true and nil if dataset exists and underlying resource does exist", func(t *testing.T) {
			secretProvider := new(mockSecretProvider)
			defer secretProvider.AssertExpectations(t)

			extTableHandle := new(mockTableResourceHandle)
			defer extTableHandle.AssertExpectations(t)

			client := new(mockClient)
			defer client.AssertExpectations(t)

			clientProvider := new(mockClientProvider)
			defer clientProvider.AssertExpectations(t)

			handle := new(mockTableResourceHandle)
			defer handle.AssertExpectations(t)

			bqStore := bigquery.NewBigqueryDataStore(secretProvider, clientProvider)

			urn, err := resource.NewURN("bigquery", "project.dataset.table")
			assert.NoError(t, err)

			pts, _ := tenant.NewPlainTextSecret("secret_name", "secret_value")
			secretProvider.On("GetSecret", mock.Anything, tnnt, "DATASTORE_BIGQUERY").Return(pts, nil)

			clientProvider.On("Get", mock.Anything, pts.Value()).Return(client, nil)

			client.On("Close").Return(nil)

			handle.On("Exists", mock.Anything).Return(true)

			client.On("DatasetHandleFrom", mock.Anything, mock.Anything).Return(handle)
			client.On("TableHandleFrom", mock.Anything, mock.Anything).Return(handle).Maybe()
			client.On("ExternalTableHandleFrom", mock.Anything, mock.Anything).Return(handle).Maybe()
			client.On("ViewHandleFrom", mock.Anything, mock.Anything).Return(handle).Maybe()
			client.On("RoutineHandleFrom", mock.Anything, mock.Anything).Return(handle).Maybe()
			client.On("ModelHandleFrom", mock.Anything, mock.Anything).Return(handle).Maybe()

			actualExist, actualError := bqStore.Exist(ctx, tnnt, urn)

			assert.True(t, actualExist)
			assert.NoError(t, actualError)
		})

		t.Run("returns true and nil if dataset exists and underlying routine does exist", func(t *testing.T) {
			secretProvider := new(mockSecretProvider)
			defer secretProvider.AssertExpectations(t)

			client := new(mockClient)
			defer client.AssertExpectations(t)

			clientProvider := new(mockClientProvider)
			defer clientProvider.AssertExpectations(t)

			dataSetHandle := new(mockTableResourceHandle)
			tableHandle := new(mockTableResourceHandle)
			externalTableHandle := new(mockTableResourceHandle)
			viewHandle := new(mockTableResourceHandle)
			routineHandle := new(mockTableResourceHandle)
			modelHandle := new(mockTableResourceHandle)
			defer func() {
				dataSetHandle.AssertExpectations(t)
				tableHandle.AssertExpectations(t)
				externalTableHandle.AssertExpectations(t)
				viewHandle.AssertExpectations(t)
				routineHandle.AssertExpectations(t)
				modelHandle.AssertExpectations(t)
			}()

			bqStore := bigquery.NewBigqueryDataStore(secretProvider, clientProvider)

			urn, err := resource.NewURN("bigquery", "project.dataset.routine")
			assert.NoError(t, err)

			pts, _ := tenant.NewPlainTextSecret("secret_name", "secret_value")
			secretProvider.On("GetSecret", mock.Anything, tnnt, "DATASTORE_BIGQUERY").Return(pts, nil)

			clientProvider.On("Get", mock.Anything, pts.Value()).Return(client, nil)

			client.On("Close").Return(nil)

			client.On("DatasetHandleFrom", mock.Anything, mock.Anything).Return(dataSetHandle)
			dataSetHandle.On("Exists", mock.Anything).Return(true)

			client.On("TableHandleFrom", mock.Anything, mock.Anything).Return(tableHandle).Maybe()
			tableHandle.On("Exists", mock.Anything).Return(false).Maybe()
			client.On("ExternalTableHandleFrom", mock.Anything, mock.Anything).Return(externalTableHandle).Maybe()
			externalTableHandle.On("Exists", mock.Anything).Return(false).Maybe()
			client.On("ViewHandleFrom", mock.Anything, mock.Anything).Return(viewHandle).Maybe()
			viewHandle.On("Exists", mock.Anything).Return(false).Maybe()
			client.On("RoutineHandleFrom", mock.Anything, mock.Anything).Return(routineHandle)
			routineHandle.On("Exists", mock.Anything).Return(true)
			client.On("ModelHandleFrom", mock.Anything, mock.Anything).Return(modelHandle).Maybe()
			modelHandle.On("Exists", mock.Anything).Return(false).Maybe()

			actualExist, actualError := bqStore.Exist(ctx, tnnt, urn)

			assert.True(t, actualExist)
			assert.NoError(t, actualError)
		})

		t.Run("returns true and nil if dataset exists and underlying model does exist", func(t *testing.T) {
			secretProvider := new(mockSecretProvider)
			defer secretProvider.AssertExpectations(t)

			client := new(mockClient)
			defer client.AssertExpectations(t)

			clientProvider := new(mockClientProvider)
			defer clientProvider.AssertExpectations(t)

			dataSetHandle := new(mockTableResourceHandle)
			tableHandle := new(mockTableResourceHandle)
			externalTableHandle := new(mockTableResourceHandle)
			viewHandle := new(mockTableResourceHandle)
			routineHandle := new(mockTableResourceHandle)
			modelHandle := new(mockTableResourceHandle)
			defer func() {
				dataSetHandle.AssertExpectations(t)
				tableHandle.AssertExpectations(t)
				externalTableHandle.AssertExpectations(t)
				viewHandle.AssertExpectations(t)
				routineHandle.AssertExpectations(t)
				modelHandle.AssertExpectations(t)
			}()

			bqStore := bigquery.NewBigqueryDataStore(secretProvider, clientProvider)

			urn, err := resource.NewURN("bigquery", "project.dataset.routine")
			assert.NoError(t, err)

			pts, _ := tenant.NewPlainTextSecret("secret_name", "secret_value")
			secretProvider.On("GetSecret", mock.Anything, tnnt, "DATASTORE_BIGQUERY").Return(pts, nil)

			clientProvider.On("Get", mock.Anything, pts.Value()).Return(client, nil)

			client.On("Close").Return(nil)

			client.On("DatasetHandleFrom", mock.Anything, mock.Anything).Return(dataSetHandle)
			dataSetHandle.On("Exists", mock.Anything).Return(true)

			client.On("TableHandleFrom", mock.Anything, mock.Anything).Return(tableHandle).Maybe()
			tableHandle.On("Exists", mock.Anything).Return(false).Maybe()
			client.On("ExternalTableHandleFrom", mock.Anything, mock.Anything).Return(externalTableHandle).Maybe()
			externalTableHandle.On("Exists", mock.Anything).Return(false).Maybe()
			client.On("ViewHandleFrom", mock.Anything, mock.Anything).Return(viewHandle).Maybe()
			viewHandle.On("Exists", mock.Anything).Return(false).Maybe()
			client.On("RoutineHandleFrom", mock.Anything, mock.Anything).Return(routineHandle).Maybe()
			routineHandle.On("Exists", mock.Anything).Return(false).Maybe()
			client.On("ModelHandleFrom", mock.Anything, mock.Anything).Return(modelHandle)
			modelHandle.On("Exists", mock.Anything).Return(true)

			actualExist, actualError := bqStore.Exist(ctx, tnnt, urn)

			assert.True(t, actualExist)
			assert.NoError(t, actualError)
		})

		t.Run("returns false and nil error if dataset exists and underlying routine does not exist", func(t *testing.T) {
			secretProvider := new(mockSecretProvider)
			defer secretProvider.AssertExpectations(t)

			client := new(mockClient)
			defer client.AssertExpectations(t)

			clientProvider := new(mockClientProvider)
			defer clientProvider.AssertExpectations(t)

			dataSetHandle := new(mockTableResourceHandle)
			tableHandle := new(mockTableResourceHandle)
			externalTableHandle := new(mockTableResourceHandle)
			viewHandle := new(mockTableResourceHandle)
			routineHandle := new(mockTableResourceHandle)
			modelHandle := new(mockTableResourceHandle)
			defer func() {
				dataSetHandle.AssertExpectations(t)
				tableHandle.AssertExpectations(t)
				externalTableHandle.AssertExpectations(t)
				viewHandle.AssertExpectations(t)
				routineHandle.AssertExpectations(t)
				modelHandle.AssertExpectations(t)
			}()

			bqStore := bigquery.NewBigqueryDataStore(secretProvider, clientProvider)

			urn, err := resource.NewURN("bigquery", "project.dataset.routine")
			assert.NoError(t, err)

			pts, _ := tenant.NewPlainTextSecret("secret_name", "secret_value")
			secretProvider.On("GetSecret", mock.Anything, tnnt, "DATASTORE_BIGQUERY").Return(pts, nil)

			clientProvider.On("Get", mock.Anything, pts.Value()).Return(client, nil)

			client.On("Close").Return(nil)

			client.On("DatasetHandleFrom", mock.Anything, mock.Anything).Return(dataSetHandle)
			dataSetHandle.On("Exists", mock.Anything).Return(true)

			client.On("TableHandleFrom", mock.Anything, mock.Anything).Return(tableHandle).Maybe()
			tableHandle.On("Exists", mock.Anything).Return(false).Maybe()
			client.On("ExternalTableHandleFrom", mock.Anything, mock.Anything).Return(externalTableHandle).Maybe()
			externalTableHandle.On("Exists", mock.Anything).Return(false).Maybe()
			client.On("ViewHandleFrom", mock.Anything, mock.Anything).Return(viewHandle).Maybe()
			viewHandle.On("Exists", mock.Anything).Return(false).Maybe()
			client.On("RoutineHandleFrom", mock.Anything, mock.Anything).Return(routineHandle)
			routineHandle.On("Exists", mock.Anything).Return(false)
			client.On("ModelHandleFrom", mock.Anything, mock.Anything).Return(modelHandle).Maybe()
			modelHandle.On("Exists", mock.Anything).Return(false).Maybe()

			actualExist, actualError := bqStore.Exist(ctx, tnnt, urn)

			assert.False(t, actualExist)
			assert.NoError(t, actualError)
		})

		t.Run("returns false and nil if dataset exists and underlying resource does not exist", func(t *testing.T) {
			secretProvider := new(mockSecretProvider)
			defer secretProvider.AssertExpectations(t)

			extTableHandle := new(mockTableResourceHandle)
			defer extTableHandle.AssertExpectations(t)

			client := new(mockClient)
			defer client.AssertExpectations(t)

			clientProvider := new(mockClientProvider)
			defer clientProvider.AssertExpectations(t)

			handle := new(mockTableResourceHandle)
			defer handle.AssertExpectations(t)

			bqStore := bigquery.NewBigqueryDataStore(secretProvider, clientProvider)

			urn, err := resource.NewURN("bigquery", "project.dataset.table")
			assert.NoError(t, err)

			pts, _ := tenant.NewPlainTextSecret("secret_name", "secret_value")
			secretProvider.On("GetSecret", mock.Anything, tnnt, "DATASTORE_BIGQUERY").Return(pts, nil)

			clientProvider.On("Get", mock.Anything, pts.Value()).Return(client, nil)

			client.On("Close").Return(nil)

			handle.On("Exists", mock.Anything).Return(true).Once()
			handle.On("Exists", mock.Anything).Return(false).Times(5)

			client.On("DatasetHandleFrom", mock.Anything, mock.Anything).Return(handle)
			client.On("TableHandleFrom", mock.Anything, mock.Anything).Return(handle)
			client.On("ExternalTableHandleFrom", mock.Anything, mock.Anything).Return(handle)
			client.On("ViewHandleFrom", mock.Anything, mock.Anything).Return(handle)
			client.On("RoutineHandleFrom", mock.Anything, mock.Anything).Return(handle)
			client.On("ModelHandleFrom", mock.Anything, mock.Anything).Return(handle)

			actualExist, actualError := bqStore.Exist(ctx, tnnt, urn)

			assert.False(t, actualExist)
			assert.NoError(t, actualError)
		})
	})
}

type mockClientProvider struct {
	mock.Mock
}

func (m *mockClientProvider) Get(ctx context.Context, account string) (bigquery.Client, error) {
	args := m.Called(ctx, account)
	if args.Get(0) != nil {
		return args.Get(0).(bigquery.Client), args.Error(1)
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

type mockClient struct {
	mock.Mock
}

func (m *mockClient) DatasetHandleFrom(ds bigquery.Dataset) bigquery.ResourceHandle {
	args := m.Called(ds)
	return args.Get(0).(bigquery.ResourceHandle)
}

func (m *mockClient) ExternalTableHandleFrom(ds bigquery.Dataset, name string) bigquery.ResourceHandle {
	args := m.Called(ds, name)
	return args.Get(0).(bigquery.ResourceHandle)
}

func (m *mockClient) TableHandleFrom(ds bigquery.Dataset, name string) bigquery.TableResourceHandle {
	args := m.Called(ds, name)
	return args.Get(0).(bigquery.TableResourceHandle)
}

func (m *mockClient) RoutineHandleFrom(ds bigquery.Dataset, name string) bigquery.ResourceHandle {
	args := m.Called(ds, name)
	return args.Get(0).(bigquery.ResourceHandle)
}

func (m *mockClient) ModelHandleFrom(ds bigquery.Dataset, name string) bigquery.ResourceHandle {
	args := m.Called(ds, name)
	return args.Get(0).(bigquery.ResourceHandle)
}

func (m *mockClient) ViewHandleFrom(ds bigquery.Dataset, name string) bigquery.ResourceHandle {
	args := m.Called(ds, name)
	return args.Get(0).(bigquery.ResourceHandle)
}

func (m *mockClient) BulkGetDDLView(ctx context.Context, pd bigquery.ProjectDataset, names []string) (map[bigquery.ResourceURN]string, error) {
	ret := m.Called(ctx, pd, names)

	var r0 map[bigquery.ResourceURN]string
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context, bigquery.ProjectDataset, []string) (map[bigquery.ResourceURN]string, error)); ok {
		return rf(ctx, pd, names)
	}
	if rf, ok := ret.Get(0).(func(context.Context, bigquery.ProjectDataset, []string) map[bigquery.ResourceURN]string); ok {
		r0 = rf(ctx, pd, names)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(map[bigquery.ResourceURN]string)
		}
	}

	if rf, ok := ret.Get(1).(func(context.Context, bigquery.ProjectDataset, []string) error); ok {
		r1 = rf(ctx, pd, names)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

func (m *mockClient) Close() error {
	args := m.Called()
	return args.Error(0)
}

type mockTableResourceHandle struct {
	mock.Mock
}

func (m *mockTableResourceHandle) Create(ctx context.Context, res *resource.Resource) error {
	args := m.Called(ctx, res)
	return args.Error(0)
}

func (m *mockTableResourceHandle) Update(ctx context.Context, res *resource.Resource) error {
	args := m.Called(ctx, res)
	return args.Error(0)
}

func (m *mockTableResourceHandle) Exists(ctx context.Context) bool {
	args := m.Called(ctx)
	return args.Get(0).(bool)
}

func (m *mockTableResourceHandle) GetBQTable() (*bq.Table, error) {
	args := m.Called()
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*bq.Table), args.Error(1)
}

func (m *mockTableResourceHandle) CopierFrom(destination bigquery.TableResourceHandle) (bigquery.TableCopier, error) {
	args := m.Called(destination)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(bigquery.TableCopier), args.Error(1)
}

func (m *mockTableResourceHandle) UpdateExpiry(ctx context.Context, name string, expiry time.Time) error {
	args := m.Called(ctx, name, expiry)
	return args.Error(0)
}
