package service_test

import (
	"context"
	"testing"
	"time"

	"github.com/goto/salt/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/goto/optimus/core/resource"
	"github.com/goto/optimus/core/resource/service"
	"github.com/goto/optimus/core/tenant"
	"github.com/goto/optimus/internal/errors"
)

func TestResourceManager(t *testing.T) {
	ctx := context.Background()
	tnnt, _ := tenant.NewTenant("proj", "ns")
	meta := &resource.Metadata{Description: "test resource"}
	var store resource.Store = "snowflake"

	t.Run("CreateResource", func(t *testing.T) {
		t.Run("return error when service not found for datastore", func(t *testing.T) {
			repo := new(mockRepo)
			logger := log.NewLogrus()
			statusRepo := new(mockStatusRepo)
			manager := service.NewResourceManager(repo, statusRepo, logger)

			spec := map[string]any{"description": "test spec"}
			res, err := resource.NewResource("proj.ds.name1", "table", store, tnnt, meta, spec)
			assert.Nil(t, err)

			err = manager.CreateResource(ctx, res)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "internal error for entity resource: datastore [snowflake] for resource [proj.ds.name1] is not found")
		})
		t.Run("return error when datastore return an error", func(t *testing.T) {
			spec := map[string]any{"description": "test spec"}
			res, err := resource.NewResource("proj.ds.name1", "table", store, tnnt, meta, spec)
			assert.Nil(t, err)
			createRequest := resource.FromExisting(res, resource.ReplaceStatus(resource.StatusToCreate))

			argMatcher := mock.MatchedBy(func(res []*resource.Resource) bool {
				if len(res) != 1 {
					return false
				}
				return res[0].Name() == createRequest.Name() && res[0].Status() == resource.StatusCreateFailure
			})
			repo := new(mockRepo)
			repo.On("UpdateStatus", ctx, argMatcher).Return(nil)
			logger := log.NewLogrus()
			statusRepo := new(mockStatusRepo)
			manager := service.NewResourceManager(repo, statusRepo, logger)

			storeService := NewDataStore(t)
			storeService.On("Create", ctx, createRequest).Return(errors.InternalError("resource", "error in create", nil))
			defer storeService.AssertExpectations(t)

			manager.RegisterDatastore(store, storeService)

			err = manager.CreateResource(ctx, createRequest)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "error in create resource:\n internal error for entity resource: error in create")
		})
		t.Run("return error when create fails and mark also fails", func(t *testing.T) {
			spec := map[string]any{"description": "test spec"}
			res, err := resource.NewResource("proj.ds.name1", "table", store, tnnt, meta, spec)
			assert.Nil(t, err)
			createRequest := resource.FromExisting(res, resource.ReplaceStatus(resource.StatusToCreate))

			argMatcher := mock.MatchedBy(func(res []*resource.Resource) bool {
				if len(res) != 1 {
					return false
				}
				return res[0].Name() == createRequest.Name() && res[0].Status() == resource.StatusCreateFailure
			})
			repo := new(mockRepo)
			repo.On("UpdateStatus", ctx, argMatcher).
				Return(errors.NotFound("resource", "error in update"))
			logger := log.NewLogrus()
			statusRepo := new(mockStatusRepo)
			manager := service.NewResourceManager(repo, statusRepo, logger)

			storeService := NewDataStore(t)
			storeService.On("Create", ctx, createRequest).Return(errors.InvalidArgument("res", "error in create"))
			defer storeService.AssertExpectations(t)

			manager.RegisterDatastore(store, storeService)

			err = manager.CreateResource(ctx, createRequest)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "error in create resource:\n invalid argument for entity res: error in "+
				"create:\n not found for entity resource: error in update")
		})
		t.Run("marks the create exist_in_store if already exists on datastore", func(t *testing.T) {
			spec := map[string]any{"description": "test spec"}
			res, err := resource.NewResource("proj.ds.name1", "table", store, tnnt, meta, spec)
			assert.Nil(t, err)
			createRequest := resource.FromExisting(res, resource.ReplaceStatus(resource.StatusToCreate))

			argMatcher := mock.MatchedBy(func(res []*resource.Resource) bool {
				if len(res) != 1 {
					return false
				}
				return res[0].Name() == createRequest.Name() && res[0].Status() == resource.StatusExistInStore
			})
			repo := new(mockRepo)
			repo.On("UpdateStatus", ctx, argMatcher).Return(nil)
			logger := log.NewLogrus()
			statusRepo := new(mockStatusRepo)
			manager := service.NewResourceManager(repo, statusRepo, logger)

			storeService := NewDataStore(t)
			storeService.On("Create", ctx, createRequest).Return(errors.AlreadyExists("resource", "error in create"))
			defer storeService.AssertExpectations(t)

			manager.RegisterDatastore(store, storeService)

			err = manager.CreateResource(ctx, createRequest)
			assert.NoError(t, err)
		})
		t.Run("creates the resource on the datastore", func(t *testing.T) {
			spec := map[string]any{"description": "test spec"}
			res, err := resource.NewResource("proj.ds.name1", "table", store, tnnt, meta, spec)
			assert.Nil(t, err)
			createRequest := resource.FromExisting(res, resource.ReplaceStatus(resource.StatusToCreate))

			argMatcher := mock.MatchedBy(func(res []*resource.Resource) bool {
				if len(res) != 1 {
					return false
				}
				return res[0].Name() == createRequest.Name() && res[0].Status() == resource.StatusSuccess
			})
			repo := new(mockRepo)
			repo.On("UpdateStatus", ctx, argMatcher).Return(nil)
			logger := log.NewLogrus()
			statusRepo := new(mockStatusRepo)
			manager := service.NewResourceManager(repo, statusRepo, logger)

			storeService := NewDataStore(t)
			storeService.On("Create", ctx, createRequest).Return(nil)
			defer storeService.AssertExpectations(t)

			manager.RegisterDatastore(store, storeService)

			err = manager.CreateResource(ctx, createRequest)
			assert.Nil(t, err)
		})
	})
	t.Run("UpdateResource", func(t *testing.T) {
		t.Run("return error when service not found for datastore", func(t *testing.T) {
			repo := new(mockRepo)
			logger := log.NewLogrus()
			statusRepo := new(mockStatusRepo)
			manager := service.NewResourceManager(repo, statusRepo, logger)

			spec := map[string]any{"description": "test spec"}
			res, err := resource.NewResource("proj.ds.name1", "table", store, tnnt, meta, spec)
			assert.Nil(t, err)

			err = manager.UpdateResource(ctx, res)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "internal error for entity resource: datastore [snowflake] for resource [proj.ds.name1] is not found")
		})
		t.Run("return error when datastore return an error", func(t *testing.T) {
			spec := map[string]any{"description": "test spec"}
			res, err := resource.NewResource("proj.ds.name1", "table", store, tnnt, meta, spec)
			assert.Nil(t, err)
			updateRequest := resource.FromExisting(res, resource.ReplaceStatus(resource.StatusToUpdate))

			argMatcher := mock.MatchedBy(func(res []*resource.Resource) bool {
				if len(res) != 1 {
					return false
				}
				return res[0].Name() == updateRequest.Name() && res[0].Status() == resource.StatusUpdateFailure
			})
			repo := new(mockRepo)
			statusRepo := new(mockStatusRepo)
			repo.On("UpdateStatus", ctx, argMatcher).Return(nil)
			logger := log.NewLogrus()
			manager := service.NewResourceManager(repo, statusRepo, logger)

			storeService := NewDataStore(t)
			storeService.On("Update", ctx, updateRequest).Return(errors.InternalError("resource", "error in update", nil))
			defer storeService.AssertExpectations(t)

			manager.RegisterDatastore(store, storeService)

			err = manager.UpdateResource(ctx, updateRequest)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "error in update resource:\n internal error for entity resource: error in update")
		})
		t.Run("return error when update fails and mark also fails", func(t *testing.T) {
			spec := map[string]any{"description": "test spec"}
			res, err := resource.NewResource("proj.ds.name1", "table", store, tnnt, meta, spec)
			assert.Nil(t, err)
			updateRequest := resource.FromExisting(res, resource.ReplaceStatus(resource.StatusToUpdate))

			argMatcher := mock.MatchedBy(func(res []*resource.Resource) bool {
				if len(res) != 1 {
					return false
				}
				return res[0].Name() == updateRequest.Name() && res[0].Status() == resource.StatusUpdateFailure
			})
			repo := new(mockRepo)
			repo.On("UpdateStatus", ctx, argMatcher).
				Return(errors.NotFound("resource", "error in update"))
			defer repo.AssertExpectations(t)
			logger := log.NewLogrus()
			statusRepo := new(mockStatusRepo)
			manager := service.NewResourceManager(repo, statusRepo, logger)

			storeService := NewDataStore(t)
			storeService.On("Update", ctx, updateRequest).Return(errors.InvalidArgument("res", "error in update"))
			defer storeService.AssertExpectations(t)

			manager.RegisterDatastore(store, storeService)

			err = manager.UpdateResource(ctx, updateRequest)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "error in update resource:\n invalid argument for entity res: error in "+
				"update:\n not found for entity resource: error in update")
		})
		t.Run("updates the resource on the datastore", func(t *testing.T) {
			spec := map[string]any{"description": "test spec"}
			res, err := resource.NewResource("proj.ds.name1", "table", store, tnnt, meta, spec)
			assert.Nil(t, err)
			updateRequest := resource.FromExisting(res, resource.ReplaceStatus(resource.StatusToUpdate))

			argMatcher := mock.MatchedBy(func(res []*resource.Resource) bool {
				if len(res) != 1 {
					return false
				}
				return res[0].Name() == updateRequest.Name() && res[0].Status() == resource.StatusSuccess
			})
			repo := new(mockRepo)
			repo.On("UpdateStatus", ctx, argMatcher).Return(nil)
			logger := log.NewLogrus()
			statusRepo := new(mockStatusRepo)
			manager := service.NewResourceManager(repo, statusRepo, logger)

			storeService := NewDataStore(t)
			storeService.On("Update", ctx, updateRequest).Return(nil)
			defer storeService.AssertExpectations(t)

			manager.RegisterDatastore(store, storeService)

			err = manager.UpdateResource(ctx, updateRequest)
			assert.Nil(t, err)
		})
	})
	t.Run("Validate", func(t *testing.T) {
		t.Run("return error when service not found for datastore", func(t *testing.T) {
			spec := map[string]any{"description": "test spec"}
			res, err := resource.NewResource("proj.ds.name1", "table", store, tnnt, meta, spec)
			assert.Nil(t, err)
			updateRequest := resource.FromExisting(res, resource.ReplaceStatus(resource.StatusToUpdate))

			repo := new(mockRepo)
			logger := log.NewLogrus()
			statusRepo := new(mockStatusRepo)
			manager := service.NewResourceManager(repo, statusRepo, logger)

			err = manager.Validate(updateRequest)
			assert.NotNil(t, err)
			assert.ErrorContains(t, err, "datastore [snowflake] for resource [proj.ds.name1] is not found")
		})
		t.Run("returns response from the datastore", func(t *testing.T) {
			spec := map[string]any{"description": "test spec"}
			res, err := resource.NewResource("proj.ds.name1", "table", store, tnnt, meta, spec)
			assert.Nil(t, err)
			updateRequest := resource.FromExisting(res, resource.ReplaceStatus(resource.StatusToUpdate))

			logger := log.NewLogrus()
			statusRepo := new(mockStatusRepo)
			manager := service.NewResourceManager(nil, statusRepo, logger)

			storeService := NewDataStore(t)
			storeService.On("Validate", updateRequest).Return(nil)
			defer storeService.AssertExpectations(t)

			manager.RegisterDatastore(store, storeService)

			err = manager.Validate(updateRequest)
			assert.NoError(t, err)
		})
	})
	t.Run("URN", func(t *testing.T) {
		t.Run("return error when service not found for datastore", func(t *testing.T) {
			spec := map[string]any{"description": "test spec"}
			res, err := resource.NewResource("proj.ds.name1", "table", store, tnnt, meta, spec)
			assert.Nil(t, err)
			updateRequest := resource.FromExisting(res, resource.ReplaceStatus(resource.StatusToUpdate))

			repo := new(mockRepo)
			logger := log.NewLogrus()
			statusRepo := new(mockStatusRepo)
			manager := service.NewResourceManager(repo, statusRepo, logger)

			_, err = manager.GetURN(updateRequest)
			assert.NotNil(t, err)
			assert.ErrorContains(t, err, "datastore [snowflake] for resource [proj.ds.name1] is not found")
		})
		t.Run("returns response from the datastore", func(t *testing.T) {
			spec := map[string]any{"description": "test spec"}
			res, err := resource.NewResource("proj.ds.name1", "table", store, tnnt, meta, spec)
			assert.Nil(t, err)
			updateRequest := resource.FromExisting(res, resource.ReplaceStatus(resource.StatusToUpdate))

			logger := log.NewLogrus()
			statusRepo := new(mockStatusRepo)
			manager := service.NewResourceManager(nil, statusRepo, logger)

			urn, err := resource.ParseURN("snowflake://db.schema.table")
			assert.NoError(t, err)

			storeService := NewDataStore(t)
			storeService.On("GetURN", updateRequest).Return(urn, nil)
			defer storeService.AssertExpectations(t)

			manager.RegisterDatastore(store, storeService)

			actualURN, err := manager.GetURN(updateRequest)
			assert.NoError(t, err)
			assert.Equal(t, urn, actualURN)
		})
	})
	t.Run("BatchUpdate", func(t *testing.T) {
		t.Run("return error when service not found for datastore", func(t *testing.T) {
			spec := map[string]any{"description": "test spec"}
			res, err := resource.NewResource("proj.ds.name1", "table", store, tnnt, meta, spec)
			assert.Nil(t, err)
			updateRequest := resource.FromExisting(res, resource.ReplaceStatus(resource.StatusToUpdate))

			repo := new(mockRepo)
			logger := log.NewLogrus()
			statusRepo := new(mockStatusRepo)
			manager := service.NewResourceManager(repo, statusRepo, logger)

			err = manager.BatchUpdate(ctx, store, []*resource.Resource{updateRequest})
			assert.NotNil(t, err)
			assert.EqualError(t, err, "invalid argument for entity resource: data store service not "+
				"found for snowflake")
		})
		t.Run("return error when error in both batchUpdate and update status", func(t *testing.T) {
			spec := map[string]any{"description": "test spec"}
			res, err := resource.NewResource("proj.ds.name1", "table", store, tnnt, meta, spec)
			assert.Nil(t, err)
			updateRequest := resource.FromExisting(res, resource.ReplaceStatus(resource.StatusToUpdate))
			batchReq := []*resource.Resource{updateRequest}

			argMatcher := mock.MatchedBy(func(res []*resource.Resource) bool {
				if len(res) != 1 {
					return false
				}
				return res[0].Name() == updateRequest.Name() && res[0].Status() == resource.StatusUpdateFailure
			})
			repo := new(mockRepo)
			me := errors.NewMultiError("error in batch")
			me.Append(errors.InternalError("resource", "enable to update in data store", nil))
			repo.On("UpdateStatus", ctx, argMatcher).Return(me)
			defer repo.AssertExpectations(t)

			logger := log.NewLogrus()
			statusRepo := new(mockStatusRepo)
			manager := service.NewResourceManager(repo, statusRepo, logger)

			matcher := mock.MatchedBy(func(res []*resource.Resource) bool {
				if res[0].Name() == updateRequest.Name() {
					res[0].MarkFailure()
					return true
				}
				return false
			})
			me2 := errors.NewMultiError("error in db update")
			me.Append(errors.InternalError("resource", "enable to update state in db", nil))
			storeService := NewDataStore(t)
			storeService.On("BatchUpdate", ctx, matcher).Return(me2)
			defer storeService.AssertExpectations(t)

			manager.RegisterDatastore(store, storeService)

			err = manager.BatchUpdate(ctx, store, batchReq)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "error in batch update:"+
				"\n internal error for entity resource: enable to update in data store:"+
				"\n internal error for entity resource: enable to update state in db")
		})
		t.Run("returns success when no error in updating the batch", func(t *testing.T) {
			spec := map[string]any{"description": "test spec"}
			res, err := resource.NewResource("proj.ds.name1", "table", store, tnnt, meta, spec)
			assert.Nil(t, err)
			updateRequest := resource.FromExisting(res, resource.ReplaceStatus(resource.StatusToUpdate))
			batchReq := []*resource.Resource{updateRequest}
			argMatcher := mock.MatchedBy(func(res []*resource.Resource) bool {
				if len(res) != 1 {
					return false
				}
				return res[0].Name() == updateRequest.Name() && res[0].Status() == resource.StatusSuccess
			})
			repo := new(mockRepo)
			repo.On("UpdateStatus", mock.Anything, argMatcher).Return(nil)
			logger := log.NewLogrus()
			statusRepo := new(mockStatusRepo)
			manager := service.NewResourceManager(repo, statusRepo, logger)

			matcher := mock.MatchedBy(func(res []*resource.Resource) bool {
				if res[0].Name() == updateRequest.Name() {
					res[0].MarkSuccess()
					return true
				}
				return false
			})
			storeService := NewDataStore(t)
			storeService.On("BatchUpdate", ctx, matcher).Return(nil)
			defer storeService.AssertExpectations(t)

			manager.RegisterDatastore(store, storeService)

			err = manager.BatchUpdate(ctx, store, batchReq)
			assert.Nil(t, err)
		})
	})
	t.Run("Backup", func(t *testing.T) {
		t.Run("return error when service not found for datastore", func(t *testing.T) {
			repo := new(mockRepo)
			logger := log.NewLogrus()
			statusRepo := new(mockStatusRepo)
			manager := service.NewResourceManager(repo, statusRepo, logger)

			spec := map[string]any{"description": "test spec"}
			res, err := resource.NewResource("proj.ds.name1", "table", store, tnnt, meta, spec)
			assert.Nil(t, err)

			createdAt := time.Date(2022, 11, 18, 1, 0, 0, 0, time.UTC)
			backup, err := resource.NewBackup(store, tnnt, []string{"p.d.t"}, "", createdAt, nil)
			assert.NoError(t, err)

			_, err = manager.Backup(ctx, backup, []*resource.Resource{res})
			assert.NotNil(t, err)
			assert.EqualError(t, err, "invalid argument for entity resource: data store service not found "+
				"for snowflake")
		})
		t.Run("runs backup in datastore", func(t *testing.T) {
			spec := map[string]any{"description": "test spec"}
			res, err := resource.NewResource("proj.ds.name1", "table", store, tnnt, meta, spec)
			assert.Nil(t, err)

			createdAt := time.Date(2022, 11, 18, 1, 0, 0, 0, time.UTC)
			backup, err := resource.NewBackup(store, tnnt, []string{"p.d.t"}, "", createdAt, nil)
			assert.NoError(t, err)

			logger := log.NewLogrus()
			statusRepo := new(mockStatusRepo)
			manager := service.NewResourceManager(nil, statusRepo, logger)

			storeService := NewDataStore(t)
			storeService.On("Backup", ctx, backup, []*resource.Resource{res}).Return(&resource.BackupResult{
				ResourceNames: []string{"proj.ds.name1"},
			}, nil)
			defer storeService.AssertExpectations(t)

			manager.RegisterDatastore(store, storeService)

			result, err := manager.Backup(ctx, backup, []*resource.Resource{res})
			assert.NoError(t, err)
			assert.Equal(t, "proj.ds.name1", result.ResourceNames[0])
		})
	})
	t.Run("SyncResource", func(t *testing.T) {
		t.Run("returns error when store name is invalid", func(t *testing.T) {
			repo := new(mockRepo)
			logger := log.NewLogrus()
			statusRepo := new(mockStatusRepo)
			manager := service.NewResourceManager(repo, statusRepo, logger)

			spec := map[string]any{"description": "test spec"}
			res, err := resource.NewResource("proj.ds.name1", "table", store, tnnt, meta, spec)
			assert.Nil(t, err)

			err = manager.SyncResource(ctx, res)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "internal error for entity resource: datastore [snowflake] for resource [proj.ds.name1] is not found")
		})
		t.Run("returns error when create fails", func(t *testing.T) {
			spec := map[string]any{"description": "test spec"}
			res, err := resource.NewResource("proj.ds.name1", "table", store, tnnt, meta, spec)
			assert.Nil(t, err)

			argMatcher := mock.MatchedBy(func(r []*resource.Resource) bool {
				if len(r) != 1 {
					return false
				}
				return r[0].Name() == res.Name()
			})
			repo := new(mockRepo)
			repo.On("UpdateStatus", ctx, argMatcher).Return(nil)
			logger := log.NewLogrus()
			statusRepo := new(mockStatusRepo)
			manager := service.NewResourceManager(repo, statusRepo, logger)

			storeService := NewDataStore(t)
			storeService.On("Create", ctx, res).Return(errors.InternalError("resource", "error in create", nil))
			defer storeService.AssertExpectations(t)

			manager.RegisterDatastore(store, storeService)

			err = manager.SyncResource(ctx, res)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "internal error for entity resource: unable to create on datastore: "+
				"internal error for entity resource: error in create")
		})
		t.Run("returns error when update fails", func(t *testing.T) {
			spec := map[string]any{"description": "test spec"}
			res, err := resource.NewResource("proj.ds.name1", "table", store, tnnt, meta, spec)
			assert.Nil(t, err)

			argMatcher := mock.MatchedBy(func(r []*resource.Resource) bool {
				if len(r) != 1 {
					return false
				}
				return r[0].Name() == res.Name()
			})
			repo := new(mockRepo)
			repo.On("UpdateStatus", ctx, argMatcher).Return(nil)
			logger := log.NewLogrus()
			statusRepo := new(mockStatusRepo)
			manager := service.NewResourceManager(repo, statusRepo, logger)

			storeService := NewDataStore(t)
			storeService.On("Create", ctx, res).Return(errors.AlreadyExists(resource.EntityResource, "table already exists"))
			storeService.On("Update", ctx, res).Return(errors.InternalError("resource", "error in update", nil))
			defer storeService.AssertExpectations(t)

			manager.RegisterDatastore(store, storeService)

			err = manager.SyncResource(ctx, res)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "internal error for entity resource: unable to update on datastore: "+
				"internal error for entity resource: error in update")
		})
		t.Run("returns error when fails to update in db", func(t *testing.T) {
			spec := map[string]any{"description": "test spec"}
			res, err := resource.NewResource("proj.ds.name1", "table", store, tnnt, meta, spec)
			assert.Nil(t, err)

			argMatcher := mock.MatchedBy(func(r []*resource.Resource) bool {
				if len(r) != 1 {
					return false
				}
				return r[0].Name() == res.Name()
			})
			repo := new(mockRepo)
			repo.On("UpdateStatus", ctx, argMatcher).Return(errors.InternalError(resource.EntityResource, "error", nil))
			logger := log.NewLogrus()
			statusRepo := new(mockStatusRepo)
			manager := service.NewResourceManager(repo, statusRepo, logger)

			storeService := NewDataStore(t)
			storeService.On("Create", ctx, res).Return(nil)
			defer storeService.AssertExpectations(t)

			manager.RegisterDatastore(store, storeService)

			err = manager.SyncResource(ctx, res)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "internal error for entity resource: unable to update status in database: "+
				"internal error for entity resource: error")
		})
		t.Run("returns success when successful", func(t *testing.T) {
			spec := map[string]any{"description": "test spec"}
			res, err := resource.NewResource("proj.ds.name1", "table", store, tnnt, meta, spec)
			assert.Nil(t, err)

			argMatcher := mock.MatchedBy(func(r []*resource.Resource) bool {
				if len(r) != 1 {
					return false
				}
				return r[0].Name() == res.Name()
			})
			repo := new(mockRepo)
			repo.On("UpdateStatus", ctx, argMatcher).Return(nil)
			logger := log.NewLogrus()
			statusRepo := new(mockStatusRepo)
			manager := service.NewResourceManager(repo, statusRepo, logger)

			storeService := NewDataStore(t)
			storeService.On("Create", ctx, res).Return(errors.AlreadyExists(resource.EntityResource, "table already exists"))
			storeService.On("Update", ctx, res).Return(nil)
			defer storeService.AssertExpectations(t)

			manager.RegisterDatastore(store, storeService)

			err = manager.SyncResource(ctx, res)
			assert.Nil(t, err)
		})
	})
}

type mockStatusRepo struct {
	mock.Mock
}

func (m *mockStatusRepo) Upsert(ctx context.Context, projectName tenant.ProjectName, entityType, identifier string, remarks map[string]string, success bool) error {
	args := m.Called(ctx, projectName, entityType, identifier, remarks, success)
	return args.Error(0)
}

func (m *mockStatusRepo) UpdateBulk(ctx context.Context, projectName tenant.ProjectName, entityType string, syncStatus []resource.SyncStatus) error {
	args := m.Called(ctx, projectName, entityType, syncStatus)
	return args.Error(0)
}

func (m *mockStatusRepo) GetLastUpdateTime(ctx context.Context, projectName tenant.ProjectName, entityType string, identifiers []string) (map[string]time.Time, error) {
	args := m.Called(ctx, projectName, entityType, identifiers)
	return args.Get(0).(map[string]time.Time), args.Error(1)
}

type mockRepo struct {
	mock.Mock
}

func (m *mockRepo) UpdateStatus(ctx context.Context, res ...*resource.Resource) error {
	args := m.Called(ctx, res)
	return args.Error(0)
}

// DataStore is an autogenerated mock type for the DataStore type
type DataStore struct {
	mock.Mock
}

// Backup provides a mock function with given fields: _a0, _a1, _a2
func (_m *DataStore) Backup(_a0 context.Context, _a1 *resource.Backup, _a2 []*resource.Resource) (*resource.BackupResult, error) {
	ret := _m.Called(_a0, _a1, _a2)

	if len(ret) == 0 {
		panic("no return value specified for Backup")
	}

	var r0 *resource.BackupResult
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context, *resource.Backup, []*resource.Resource) (*resource.BackupResult, error)); ok {
		return rf(_a0, _a1, _a2)
	}
	if rf, ok := ret.Get(0).(func(context.Context, *resource.Backup, []*resource.Resource) *resource.BackupResult); ok {
		r0 = rf(_a0, _a1, _a2)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*resource.BackupResult)
		}
	}

	if rf, ok := ret.Get(1).(func(context.Context, *resource.Backup, []*resource.Resource) error); ok {
		r1 = rf(_a0, _a1, _a2)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// BatchUpdate provides a mock function with given fields: _a0, _a1
func (_m *DataStore) BatchUpdate(_a0 context.Context, _a1 []*resource.Resource) error {
	ret := _m.Called(_a0, _a1)

	if len(ret) == 0 {
		panic("no return value specified for BatchUpdate")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, []*resource.Resource) error); ok {
		r0 = rf(_a0, _a1)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// Create provides a mock function with given fields: _a0, _a1
func (_m *DataStore) Create(_a0 context.Context, _a1 *resource.Resource) error {
	ret := _m.Called(_a0, _a1)

	if len(ret) == 0 {
		panic("no return value specified for Create")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, *resource.Resource) error); ok {
		r0 = rf(_a0, _a1)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// Exist provides a mock function with given fields: ctx, tnnt, urn
func (_m *DataStore) Exist(ctx context.Context, tnnt tenant.Tenant, urn resource.URN) (bool, error) {
	ret := _m.Called(ctx, tnnt, urn)

	if len(ret) == 0 {
		panic("no return value specified for Exist")
	}

	var r0 bool
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context, tenant.Tenant, resource.URN) (bool, error)); ok {
		return rf(ctx, tnnt, urn)
	}
	if rf, ok := ret.Get(0).(func(context.Context, tenant.Tenant, resource.URN) bool); ok {
		r0 = rf(ctx, tnnt, urn)
	} else {
		r0 = ret.Get(0).(bool)
	}

	if rf, ok := ret.Get(1).(func(context.Context, tenant.Tenant, resource.URN) error); ok {
		r1 = rf(ctx, tnnt, urn)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// GetURN provides a mock function with given fields: res
func (_m *DataStore) GetURN(res *resource.Resource) (resource.URN, error) {
	ret := _m.Called(res)

	if len(ret) == 0 {
		panic("no return value specified for GetURN")
	}

	var r0 resource.URN
	var r1 error
	if rf, ok := ret.Get(0).(func(*resource.Resource) (resource.URN, error)); ok {
		return rf(res)
	}
	if rf, ok := ret.Get(0).(func(*resource.Resource) resource.URN); ok {
		r0 = rf(res)
	} else {
		r0 = ret.Get(0).(resource.URN)
	}

	if rf, ok := ret.Get(1).(func(*resource.Resource) error); ok {
		r1 = rf(res)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// Update provides a mock function with given fields: _a0, _a1
func (_m *DataStore) Update(_a0 context.Context, _a1 *resource.Resource) error {
	ret := _m.Called(_a0, _a1)

	if len(ret) == 0 {
		panic("no return value specified for Update")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, *resource.Resource) error); ok {
		r0 = rf(_a0, _a1)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// Validate provides a mock function with given fields: _a0
func (_m *DataStore) Validate(_a0 *resource.Resource) error {
	ret := _m.Called(_a0)

	if len(ret) == 0 {
		panic("no return value specified for Validate")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(*resource.Resource) error); ok {
		r0 = rf(_a0)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// NewDataStore creates a new instance of DataStore. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewDataStore(t interface {
	mock.TestingT
	Cleanup(func())
},
) *DataStore {
	mock := &DataStore{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
