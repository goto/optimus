package service_test

import (
	"context"
	"errors"
	"testing"

	"github.com/goto/salt/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/goto/optimus/core/event/moderator"
	"github.com/goto/optimus/core/job"
	"github.com/goto/optimus/core/resource"
	"github.com/goto/optimus/core/resource/service"
	"github.com/goto/optimus/core/tenant"
	oErrors "github.com/goto/optimus/internal/errors"
	"github.com/goto/optimus/internal/writer"
)

func TestResourceService(t *testing.T) {
	ctx := context.Background()
	logger := log.NewLogrus()
	logWriter := writer.NewLogWriter(logger)
	tnnt, tenantErr := tenant.NewTenant("project_test", "namespace_tes")
	assert.NoError(t, tenantErr)
	meta := &resource.Metadata{
		Version:     1,
		Description: "test metadata",
		Labels:      map[string]string{"owner": "optimus"},
	}
	spec := map[string]any{
		"description": "test spec",
	}

	datasetURN, err := resource.ParseURN("bigquery://project:dataset")
	assert.NoError(t, err)
	tableURN, err := resource.ParseURN("bigquery://project:dataset.table")
	assert.NoError(t, err)

	t.Run("Create", func(t *testing.T) {
		t.Run("returns error if resource is invalid", func(t *testing.T) {
			invalid := &resource.Resource{}

			mgr := NewResourceManager(t)
			mgr.On("Validate", invalid).Return(errors.New("validation error"))

			rscService := service.NewResourceService(logger, nil, nil, mgr, nil, nil, nil)

			actualError := rscService.Create(ctx, invalid)
			assert.Error(t, actualError)
			assert.ErrorContains(t, actualError, "validation error")
		})
		t.Run("returns error cannot get resource urn", func(t *testing.T) {
			incoming, err := resource.NewResource("project.dataset", "dataset", resource.Bigquery, tnnt, meta, spec)
			assert.NoError(t, err)

			mgr := NewResourceManager(t)
			mgr.On("Validate", incoming).Return(nil)
			mgr.On("GetURN", incoming).Return(resource.ZeroURN(), errors.New("urn error"))

			rscService := service.NewResourceService(logger, nil, nil, mgr, nil, nil, nil)

			actualError := rscService.Create(ctx, incoming)
			assert.Error(t, actualError)
			assert.ErrorContains(t, actualError, "urn error")
		})
		t.Run("returns error if cannot update resource urn", func(t *testing.T) {
			incoming, err := resource.NewResource("project.dataset", "dataset", resource.Bigquery, tnnt, meta, spec)
			assert.NoError(t, err)
			err = incoming.UpdateURN(tableURN)
			assert.NoError(t, err)

			mgr := NewResourceManager(t)
			mgr.On("Validate", incoming).Return(nil)
			mgr.On("GetURN", incoming).Return(tableURN, nil)

			rscService := service.NewResourceService(logger, nil, nil, mgr, nil, nil, nil)

			actualError := rscService.Create(ctx, incoming)
			assert.Error(t, actualError)
			assert.ErrorContains(t, actualError, "urn already present")
		})

		onlyActive := false

		t.Run("returns error if unknown error is encountered when getting existing resource", func(t *testing.T) {
			incoming, err := resource.NewResource("project.dataset", "dataset", resource.Bigquery, tnnt, meta, spec)
			assert.NoError(t, err)

			mgr := NewResourceManager(t)
			mgr.On("Validate", incoming).Return(nil)
			mgr.On("GetURN", incoming).Return(tableURN, nil)

			repo := newResourceRepository(t)
			repo.On("ReadByFullName", ctx, tnnt, resource.Bigquery, incoming.FullName(), onlyActive).Return(nil, errors.New("unknown error"))

			rscService := service.NewResourceService(logger, repo, nil, mgr, nil, nil, nil)

			actualError := rscService.Create(ctx, incoming)
			assert.ErrorContains(t, actualError, "unknown error")
		})

		t.Run("resource does not exist in repository", func(t *testing.T) {
			t.Run("returns error if error is encountered when creating resource to repository", func(t *testing.T) {
				incoming, err := resource.NewResource("project.dataset", "dataset", resource.Bigquery, tnnt, meta, spec)
				assert.NoError(t, err)

				repo := newResourceRepository(t)
				repo.On("ReadByFullName", ctx, tnnt, resource.Bigquery, incoming.FullName(), onlyActive).Return(nil, oErrors.NotFound(resource.EntityResource, "resource not found"))
				repo.On("Create", ctx, mock.Anything).Return(errors.New("error creating resource"))

				mgr := NewResourceManager(t)
				mgr.On("Validate", incoming).Return(nil)
				mgr.On("GetURN", incoming).Return(tableURN, nil)

				rscService := service.NewResourceService(logger, repo, nil, mgr, nil, nil, nil)

				actualError := rscService.Create(ctx, incoming)
				assert.ErrorContains(t, actualError, "error creating resource")
			})
		})

		t.Run("resource already exists in repository", func(t *testing.T) {
			t.Run("returns no error if status is success or exists_in_store", func(t *testing.T) {
				existing, err := resource.NewResource("project.dataset", "dataset", resource.Bigquery, tnnt, meta, spec)
				assert.NoError(t, err)

				mgr := NewResourceManager(t)
				mgr.On("Validate", mock.Anything).Return(nil)
				mgr.On("GetURN", mock.Anything).Return(datasetURN, nil)

				statusToTest := []resource.Status{
					resource.StatusExistInStore,
					resource.StatusSuccess,
				}

				for _, status := range statusToTest {
					incoming, err := resource.NewResource("project.dataset", "dataset", resource.Bigquery, tnnt, meta, spec)
					assert.NoError(t, err)

					existingWithStatus := resource.FromExisting(existing, resource.ReplaceStatus(status))

					repo := newResourceRepository(t)
					repo.On("ReadByFullName", ctx, tnnt, resource.Bigquery, incoming.FullName(), onlyActive).Return(existingWithStatus, nil)

					rscService := service.NewResourceService(logger, repo, nil, mgr, nil, nil, nil)

					err = rscService.Create(ctx, incoming)
					assert.NoError(t, err)
					repo.AssertExpectations(t)
				}
			})
			t.Run("returns error if status is neither create_failure nor to_create", func(t *testing.T) {
				existing, err := resource.NewResource("project.dataset", "dataset", resource.Bigquery, tnnt, meta, spec)
				assert.NoError(t, err)

				mgr := NewResourceManager(t)
				mgr.On("Validate", mock.Anything).Return(nil)
				mgr.On("GetURN", mock.Anything).Return(datasetURN, nil)

				unacceptableStatuses := []resource.Status{
					resource.StatusUnknown,
					resource.StatusValidationFailure,
					resource.StatusValidationSuccess,
					resource.StatusToUpdate,
					resource.StatusSkipped,
					resource.StatusUpdateFailure,
				}

				for _, status := range unacceptableStatuses {
					incoming, err := resource.NewResource("project.dataset", "dataset", resource.Bigquery, tnnt, meta, spec)
					assert.NoError(t, err)

					existingWithStatus := resource.FromExisting(existing, resource.ReplaceStatus(status))

					repo := newResourceRepository(t)
					rscService := service.NewResourceService(logger, repo, nil, mgr, nil, nil, nil)

					repo.On("ReadByFullName", ctx, tnnt, resource.Bigquery, incoming.FullName(), onlyActive).Return(existingWithStatus, nil)

					actualError := rscService.Create(ctx, incoming)
					assert.ErrorContains(t, actualError, "since it already exists with status")
					repo.AssertExpectations(t)
				}
			})

			t.Run("returns error if error is encountered when updating to repository", func(t *testing.T) {
				incoming, err := resource.NewResource("project.dataset", "dataset", resource.Bigquery, tnnt, meta, spec)
				assert.NoError(t, err)
				existing, err := resource.NewResource("project.dataset", "dataset", resource.Bigquery, tnnt, meta, spec)
				assert.NoError(t, err)
				existing = resource.FromExisting(existing, resource.ReplaceStatus(resource.StatusCreateFailure))

				repo := newResourceRepository(t)
				repo.On("ReadByFullName", ctx, tnnt, resource.Bigquery, incoming.FullName(), onlyActive).Return(existing, nil)
				repo.On("Update", ctx, incoming).Return(errors.New("error updating resource"))

				mgr := NewResourceManager(t)
				mgr.On("Validate", incoming).Return(nil)
				mgr.On("GetURN", incoming).Return(tableURN, nil)

				rscService := service.NewResourceService(logger, repo, nil, mgr, nil, nil, nil)

				actualError := rscService.Create(ctx, incoming)
				assert.ErrorContains(t, actualError, "error updating resource")
			})
		})

		t.Run("returns error if error is encountered when creating to store", func(t *testing.T) {
			incoming, err := resource.NewResource("project.dataset", "dataset", resource.Bigquery, tnnt, meta, spec)
			assert.NoError(t, err)

			repo := newResourceRepository(t)
			repo.On("ReadByFullName", ctx, tnnt, resource.Bigquery, incoming.FullName(), onlyActive).Return(nil, oErrors.NotFound(resource.EntityResource, "resource not found"))
			repo.On("Create", ctx, incoming).Return(nil)

			mgr := NewResourceManager(t)
			mgr.On("Validate", incoming).Return(nil)
			mgr.On("GetURN", incoming).Return(tableURN, nil)
			mgr.On("CreateResource", ctx, incoming).Return(errors.New("error creating to store"))

			rscService := service.NewResourceService(logger, repo, nil, mgr, nil, nil, nil)

			actualError := rscService.Create(ctx, incoming)
			assert.ErrorContains(t, actualError, "error creating to store")
		})

		t.Run("returns nil if no error is encountered", func(t *testing.T) {
			incoming, err := resource.NewResource("project.dataset", "dataset", resource.Bigquery, tnnt, meta, spec)
			assert.NoError(t, err)

			repo := newResourceRepository(t)
			repo.On("ReadByFullName", ctx, tnnt, resource.Bigquery, incoming.FullName(), onlyActive).Return(nil, oErrors.NotFound(resource.EntityResource, "resource not found"))
			repo.On("Create", ctx, incoming).Return(nil)

			mgr := NewResourceManager(t)
			mgr.On("Validate", incoming).Return(nil)
			mgr.On("GetURN", incoming).Return(tableURN, nil)
			mgr.On("CreateResource", ctx, incoming).Return(nil)

			eventHandler := newEventHandler(t)
			alertmanager := new(mockAlertManager)
			alertmanager.On("SendResourceEvent", mock.Anything)

			rscService := service.NewResourceService(logger, repo, nil, mgr, eventHandler, nil, nil)

			actualError := rscService.Create(ctx, incoming)
			assert.NoError(t, actualError)
		})
	})

	t.Run("Update", func(t *testing.T) {
		onlyActive := true
		t.Run("returns error if resource is invalid", func(t *testing.T) {
			invalidResource := &resource.Resource{}

			mgr := NewResourceManager(t)
			mgr.On("Validate", invalidResource).Return(errors.New("validation error"))

			rscService := service.NewResourceService(logger, nil, nil, mgr, nil, nil, nil)

			actualError := rscService.Update(ctx, invalidResource, logWriter)
			assert.Error(t, actualError)
		})
		t.Run("returns error cannot get resource urn", func(t *testing.T) {
			incoming, err := resource.NewResource("project.dataset", "dataset", resource.Bigquery, tnnt, meta, spec)
			assert.NoError(t, err)

			mgr := NewResourceManager(t)
			mgr.On("Validate", incoming).Return(nil)
			mgr.On("GetURN", incoming).Return(resource.ZeroURN(), errors.New("urn error"))

			rscService := service.NewResourceService(logger, nil, nil, mgr, nil, nil, nil)

			actualError := rscService.Update(ctx, incoming, logWriter)
			assert.Error(t, actualError)
			assert.ErrorContains(t, actualError, "urn error")
		})
		t.Run("returns error if cannot update resource urn", func(t *testing.T) {
			incoming, err := resource.NewResource("project.dataset", "dataset", resource.Bigquery, tnnt, meta, spec)
			assert.NoError(t, err)
			urn, err := resource.ParseURN("bigquery://project:dataset.table")
			assert.NoError(t, err)
			err = incoming.UpdateURN(urn)
			assert.NoError(t, err)

			mgr := NewResourceManager(t)
			mgr.On("Validate", incoming).Return(nil)
			mgr.On("GetURN", incoming).Return(urn, nil)

			rscService := service.NewResourceService(logger, nil, nil, mgr, nil, nil, nil)

			actualError := rscService.Update(ctx, incoming, logWriter)
			assert.Error(t, actualError)
			assert.ErrorContains(t, actualError, "urn already present")
		})

		t.Run("returns error if error is encountered when getting from repo", func(t *testing.T) {
			fullName := "project.dataset"
			resourceToUpdate, err := resource.NewResource(fullName, "dataset", resource.Bigquery, tnnt, meta, spec)
			assert.NoError(t, err)

			repo := newResourceRepository(t)
			repo.On("ReadByFullName", ctx, tnnt, resource.Bigquery, fullName, onlyActive).Return(nil, errors.New("unknown error"))

			mgr := NewResourceManager(t)
			mgr.On("Validate", resourceToUpdate).Return(nil)
			mgr.On("GetURN", resourceToUpdate).Return(datasetURN, nil)

			rscService := service.NewResourceService(logger, repo, nil, mgr, nil, nil, nil)

			actualError := rscService.Update(ctx, resourceToUpdate, logWriter)
			assert.ErrorContains(t, actualError, "unknown error")
		})

		t.Run("returns error if status is not one of to_update, success, exist_in_store, or update_failure", func(t *testing.T) {
			existing, err := resource.NewResource("project.dataset", "dataset", resource.Bigquery, tnnt, meta, spec)
			assert.NoError(t, err)

			mgr := NewResourceManager(t)
			mgr.On("Validate", mock.Anything).Return(nil)
			mgr.On("GetURN", mock.Anything).Return(datasetURN, nil)

			repo := newResourceRepository(t)
			rscService := service.NewResourceService(logger, repo, nil, mgr, nil, nil, nil)

			unacceptableStatuses := []resource.Status{
				resource.StatusUnknown,
				resource.StatusValidationFailure,
				resource.StatusValidationSuccess,
				resource.StatusToCreate,
				resource.StatusSkipped,
				resource.StatusCreateFailure,
			}

			for _, status := range unacceptableStatuses {
				resourceToUpdate, err := resource.NewResource("project.dataset", "dataset", resource.Bigquery, tnnt, meta, spec)
				assert.NoError(t, err)

				existingWithStatus := resource.FromExisting(existing, resource.ReplaceStatus(status))

				repo.On("ReadByFullName", ctx, tnnt, resource.Bigquery, resourceToUpdate.FullName(), onlyActive).Return(existingWithStatus, nil)

				actualError := rscService.Update(ctx, resourceToUpdate, logWriter)
				assert.ErrorContains(t, actualError, "cannot update resource")
			}
		})

		t.Run("returns error if error is encountered when updating to repo", func(t *testing.T) {
			fullName := "project.dataset"
			resourceToUpdate, err := resource.NewResource(fullName, "dataset", resource.Bigquery, tnnt, meta, spec)
			assert.NoError(t, err)
			existingResource, err := resource.NewResource(fullName, "dataset", resource.Bigquery, tnnt, meta, spec)
			assert.NoError(t, err)
			existingResource = resource.FromExisting(existingResource, resource.ReplaceStatus(resource.StatusToUpdate))

			mgr := NewResourceManager(t)
			mgr.On("Validate", resourceToUpdate).Return(nil)
			mgr.On("GetURN", resourceToUpdate).Return(datasetURN, nil)

			repo := newResourceRepository(t)
			repo.On("ReadByFullName", ctx, tnnt, resource.Bigquery, fullName, onlyActive).Return(existingResource, nil)
			repo.On("Update", ctx, mock.Anything).Return(errors.New("unknown error"))

			rscService := service.NewResourceService(logger, repo, nil, mgr, nil, nil, nil)

			actualError := rscService.Update(ctx, resourceToUpdate, logWriter)
			assert.ErrorContains(t, actualError, "unknown error")
		})

		t.Run("returns error if error is encountered when updating to store", func(t *testing.T) {
			fullName := "project.dataset"
			resourceToUpdate, err := resource.NewResource(fullName, "dataset", resource.Bigquery, tnnt, meta, spec)
			assert.NoError(t, err)
			existingResource, err := resource.NewResource(fullName, "dataset", resource.Bigquery, tnnt, meta, spec)
			assert.NoError(t, err)
			existingResource = resource.FromExisting(existingResource, resource.ReplaceStatus(resource.StatusToUpdate))

			repo := newResourceRepository(t)
			repo.On("ReadByFullName", ctx, tnnt, resource.Bigquery, fullName, onlyActive).Return(existingResource, nil)
			repo.On("Update", ctx, mock.Anything).Return(nil)

			mgr := NewResourceManager(t)
			mgr.On("Validate", mock.Anything).Return(nil)
			mgr.On("GetURN", mock.Anything).Return(datasetURN, nil)
			mgr.On("UpdateResource", ctx, mock.Anything).Return(errors.New("unknown error"))

			rscService := service.NewResourceService(logger, repo, nil, mgr, nil, nil, nil)

			actualError := rscService.Update(ctx, resourceToUpdate, logWriter)
			assert.ErrorContains(t, actualError, "unknown error")
		})

		t.Run("returns error if encountered error when refreshing downstream", func(t *testing.T) {
			fullName := "project.dataset"
			incomingSpec := map[string]any{"view_query": "select 1;"}
			resourceToUpdate, err := resource.NewResource(fullName, "dataset", resource.Bigquery, tnnt, meta, incomingSpec)
			assert.NoError(t, err)
			existingSpec := map[string]any{"view_query": "select 2;"}
			existingResource, err := resource.NewResource(fullName, "dataset", resource.Bigquery, tnnt, meta, existingSpec)
			assert.NoError(t, err)
			existingResource = resource.FromExisting(existingResource, resource.ReplaceStatus(resource.StatusToUpdate))

			repo := newResourceRepository(t)
			repo.On("ReadByFullName", ctx, tnnt, resource.Bigquery, fullName, onlyActive).Return(existingResource, nil)
			repo.On("Update", ctx, mock.Anything).Return(nil)

			mgr := NewResourceManager(t)
			mgr.On("Validate", mock.Anything).Return(nil)
			mgr.On("GetURN", mock.Anything).Return(datasetURN, nil)
			mgr.On("UpdateResource", ctx, mock.Anything).Run(func(args mock.Arguments) {
				res, ok := args[1].(*resource.Resource)
				if ok {
					res.MarkSuccess()
				}
			}).Return(nil)

			eventHandler := newEventHandler(t)
			eventHandler.On("HandleEvent", mock.Anything)
			alertmanager := new(mockAlertManager)
			alertmanager.On("SendResourceEvent", mock.Anything)

			refresher := new(mockDownstreamRefresher)
			refresher.On("RefreshResourceDownstream", ctx, mock.Anything, logWriter).Return(errors.New("unknown error"))

			rscService := service.NewResourceService(logger, repo, refresher, mgr, eventHandler, nil, alertmanager)

			actualError := rscService.Update(ctx, resourceToUpdate, logWriter)
			assert.ErrorContains(t, actualError, "unknown error")
		})

		t.Run("returns nil if no error is encountered", func(t *testing.T) {
			fullName := "project.dataset"
			incomingSpec := map[string]any{"view_query": "select 1;"}
			resourceToUpdate, err := resource.NewResource(fullName, "dataset", resource.Bigquery, tnnt, meta, incomingSpec)
			assert.NoError(t, err)
			existingSpec := map[string]any{"view_query": "select 2;"}
			existingResource, err := resource.NewResource(fullName, "dataset", resource.Bigquery, tnnt, meta, existingSpec)
			assert.NoError(t, err)
			existingResource = resource.FromExisting(existingResource, resource.ReplaceStatus(resource.StatusToUpdate))

			repo := newResourceRepository(t)
			repo.On("ReadByFullName", ctx, tnnt, resource.Bigquery, fullName, onlyActive).Return(existingResource, nil)
			repo.On("Update", ctx, mock.Anything).Return(nil)

			mgr := NewResourceManager(t)
			mgr.On("Validate", mock.Anything).Return(nil)
			mgr.On("GetURN", mock.Anything).Return(datasetURN, nil)
			mgr.On("UpdateResource", ctx, mock.Anything).Return(nil)

			eventHandler := newEventHandler(t)
			alertmanager := new(mockAlertManager)
			alertmanager.On("SendResourceEvent", mock.Anything)

			refresher := new(mockDownstreamRefresher)
			refresher.On("RefreshResourceDownstream", ctx, mock.Anything, logWriter).Return(nil)

			rscService := service.NewResourceService(logger, repo, refresher, mgr, eventHandler, nil, nil)

			actualError := rscService.Update(ctx, resourceToUpdate, logWriter)
			assert.NoError(t, actualError)
		})
	})

	t.Run("Get", func(t *testing.T) {
		onlyActive := true
		t.Run("returns nil and error if resource name is empty", func(t *testing.T) {
			rscService := service.NewResourceService(logger, nil, nil, nil, nil, nil, nil)

			store := resource.Bigquery
			actualResource, actualError := rscService.Get(ctx, tnnt, store, "")
			assert.Nil(t, actualResource)
			assert.ErrorContains(t, actualError, "empty resource full name")
		})

		t.Run("returns nil and error if error is encountered when getting from repo", func(t *testing.T) {
			repo := newResourceRepository(t)
			fullName := "project.dataset"
			repo.On("ReadByFullName", ctx, tnnt, resource.Bigquery, fullName, onlyActive).Return(nil, errors.New("unknown error"))

			refresher := new(mockDownstreamRefresher)

			rscService := service.NewResourceService(logger, repo, refresher, nil, nil, nil, nil)

			actualResource, actualError := rscService.Get(ctx, tnnt, resource.Bigquery, fullName)
			assert.Nil(t, actualResource)
			assert.ErrorContains(t, actualError, "unknown error")
		})

		t.Run("returns resource and nil if no error is encountered", func(t *testing.T) {
			fullName := "project.dataset"
			existingResource, err := resource.NewResource(fullName, "dataset", resource.Bigquery, tnnt, meta, spec)
			assert.NoError(t, err)

			repo := newResourceRepository(t)
			repo.On("ReadByFullName", ctx, tnnt, resource.Bigquery, fullName, onlyActive).Return(existingResource, nil)

			refresher := new(mockDownstreamRefresher)

			rscService := service.NewResourceService(logger, repo, refresher, nil, nil, nil, nil)

			actualResource, actualError := rscService.Get(ctx, tnnt, resource.Bigquery, fullName)
			assert.EqualValues(t, existingResource, actualResource)
			assert.NoError(t, actualError)
		})
	})

	t.Run("GetAll", func(t *testing.T) {
		onlyActive := true
		t.Run("returns nil and error if error is encountered when getting all from repo", func(t *testing.T) {
			repo := newResourceRepository(t)
			repo.On("ReadAll", ctx, tnnt, resource.Bigquery, onlyActive).Return(nil, errors.New("unknown error"))

			refresher := new(mockDownstreamRefresher)

			rscService := service.NewResourceService(logger, repo, refresher, nil, nil, nil, nil)

			actualResources, actualError := rscService.GetAll(ctx, tnnt, resource.Bigquery)
			assert.Nil(t, actualResources)
			assert.ErrorContains(t, actualError, "unknown error")
		})

		t.Run("returns resources and nil if no error is encountered", func(t *testing.T) {
			existingResource, err := resource.NewResource("project.dataset", "dataset", resource.Bigquery, tnnt, meta, spec)
			assert.NoError(t, err)

			repo := newResourceRepository(t)
			repo.On("ReadAll", ctx, tnnt, resource.Bigquery, onlyActive).Return([]*resource.Resource{existingResource}, nil)

			refresher := new(mockDownstreamRefresher)

			rscService := service.NewResourceService(logger, repo, refresher, nil, nil, nil, nil)

			actualResources, actualError := rscService.GetAll(ctx, tnnt, resource.Bigquery)
			assert.EqualValues(t, []*resource.Resource{existingResource}, actualResources)
			assert.NoError(t, actualError)
		})
	})

	t.Run("Deploy", func(t *testing.T) {
		onlyActive := false
		viewSpec := map[string]any{
			"view_query": "select * from `proj.dataset.table`",
		}
		resourceWithStatus := func(name string, spec map[string]any, status resource.Status) *resource.Resource {
			existingResource, resErr := resource.NewResource(name, "view", resource.Bigquery, tnnt, meta, spec)
			assert.NoError(t, resErr)
			return resource.FromExisting(existingResource, resource.ReplaceStatus(status))
		}

		t.Run("returns error if one or more resources are invalid", func(t *testing.T) {
			invalidResourceToUpdate := &resource.Resource{}
			resourcesToUpdate := []*resource.Resource{invalidResourceToUpdate}

			repo := newResourceRepository(t)
			repo.On("ReadAll", ctx, tnnt, resource.Bigquery, onlyActive).Return([]*resource.Resource{}, nil)

			mgr := NewResourceManager(t)
			mgr.On("Validate", invalidResourceToUpdate).Return(errors.New("error validating"))

			rscService := service.NewResourceService(logger, repo, nil, mgr, nil, nil, nil)

			actualError := rscService.Deploy(ctx, tnnt, resource.Bigquery, resourcesToUpdate, logWriter)
			assert.Error(t, actualError)
			assert.ErrorContains(t, actualError, "error validating")
		})

		t.Run("skips resource when cannot get resource urn", func(t *testing.T) {
			incoming, err := resource.NewResource("project.dataset", "dataset", resource.Bigquery, tnnt, meta, spec)
			assert.NoError(t, err)

			mgr := NewResourceManager(t)
			mgr.On("Validate", incoming).Return(nil)
			mgr.On("GetURN", incoming).Return(resource.ZeroURN(), errors.New("urn error"))

			repo := newResourceRepository(t)
			repo.On("ReadAll", ctx, tnnt, resource.Bigquery, onlyActive).Return([]*resource.Resource{}, nil)

			rscService := service.NewResourceService(logger, repo, nil, mgr, nil, nil, nil)

			actualError := rscService.Deploy(ctx, tnnt, resource.Bigquery, []*resource.Resource{incoming}, logWriter)
			assert.Error(t, actualError)
			assert.ErrorContains(t, actualError, "urn error")
			assert.Equal(t, "unknown", incoming.Status().String())
		})

		t.Run("returns error if cannot update resource urn", func(t *testing.T) {
			incoming, err := resource.NewResource("project.dataset", "dataset", resource.Bigquery, tnnt, meta, spec)
			assert.NoError(t, err)
			urn, err := resource.ParseURN("bigquery://project:dataset.table")
			assert.NoError(t, err)
			err = incoming.UpdateURN(urn)
			assert.NoError(t, err)

			mgr := NewResourceManager(t)
			mgr.On("Validate", incoming).Return(nil)
			mgr.On("GetURN", incoming).Return(urn, nil)

			repo := newResourceRepository(t)
			repo.On("ReadAll", ctx, tnnt, resource.Bigquery, onlyActive).Return([]*resource.Resource{}, nil)

			rscService := service.NewResourceService(logger, repo, nil, mgr, nil, nil, nil)

			actualError := rscService.Deploy(ctx, tnnt, resource.Bigquery, []*resource.Resource{incoming}, logWriter)
			assert.Error(t, actualError)
			assert.ErrorContains(t, actualError, "urn already present for")
			assert.Equal(t, "unknown", incoming.Status().String())
		})

		t.Run("returns error if error is encountered when reading from repo", func(t *testing.T) {
			incomingResourceToUpdate := resourceWithStatus("project.dataset.table1", viewSpec, resource.StatusValidationSuccess)
			resourcesToUpdate := []*resource.Resource{incomingResourceToUpdate}

			repo := newResourceRepository(t)
			repo.On("ReadAll", ctx, tnnt, resource.Bigquery, onlyActive).Return(nil, errors.New("error while read all"))

			urn, err := resource.ParseURN("bigquery://project:dataset.table1")
			assert.NoError(t, err)

			mgr := NewResourceManager(t)
			mgr.On("Validate", incomingResourceToUpdate).Return(nil)
			mgr.On("GetURN", incomingResourceToUpdate).Return(urn, nil)

			rscService := service.NewResourceService(logger, repo, nil, mgr, nil, nil, nil)

			actualError := rscService.Deploy(ctx, tnnt, resource.Bigquery, resourcesToUpdate, logWriter)
			assert.ErrorContains(t, actualError, "error while read all")
		})

		t.Run("returns nil if there is no resource to create or modify", func(t *testing.T) {
			incomingResourceToUpdate, resErr := resource.NewResource("project.dataset.view1", "view", resource.Bigquery, tnnt, meta, viewSpec)
			assert.NoError(t, resErr)
			existing := resourceWithStatus("project.dataset.view1", viewSpec, resource.StatusSuccess)

			repo := newResourceRepository(t)
			repo.On("ReadAll", ctx, tnnt, resource.Bigquery, onlyActive).Return([]*resource.Resource{existing}, nil)

			urn, err := resource.ParseURN("bigquery://project:dataset.view1")
			assert.NoError(t, err)

			mgr := NewResourceManager(t)
			mgr.On("Validate", mock.Anything).Return(nil)
			mgr.On("GetURN", mock.Anything).Return(urn, nil)

			rscService := service.NewResourceService(logger, repo, nil, mgr, nil, nil, nil)

			actualError := rscService.Deploy(ctx, tnnt, resource.Bigquery, []*resource.Resource{incomingResourceToUpdate}, logWriter)
			assert.NoError(t, actualError)
		})

		t.Run("returns error if error is encountered when creating on repo", func(t *testing.T) {
			incomingMetadata := &resource.Metadata{
				Description: "incoming resource metadata",
			}
			fullName := "project.dataset"
			incomingResourceToUpdate, err := resource.NewResource(fullName, "dataset", resource.Bigquery, tnnt, incomingMetadata, spec)
			assert.NoError(t, err)

			repo := newResourceRepository(t)
			repo.On("ReadAll", ctx, tnnt, resource.Bigquery, onlyActive).Return([]*resource.Resource{}, nil)
			repo.On("Create", ctx, incomingResourceToUpdate).Return(errors.New("error in create"))

			mgr := NewResourceManager(t)
			mgr.On("Validate", mock.Anything).Return(nil)
			mgr.On("GetURN", mock.Anything).Return(datasetURN, nil)

			eventHandler := newEventHandler(t)
			alertmanager := new(mockAlertManager)
			alertmanager.On("SendResourceEvent", mock.Anything)

			rscService := service.NewResourceService(logger, repo, nil, mgr, eventHandler, nil, alertmanager)

			actualError := rscService.Deploy(ctx, tnnt, resource.Bigquery, []*resource.Resource{incomingResourceToUpdate}, logWriter)

			assert.ErrorContains(t, actualError, "error in create")
		})

		t.Run("returns error if error is encountered when updating on repo", func(t *testing.T) {
			fullName := "project.dataset.table1"
			incomingMetadata := &resource.Metadata{
				Description: "incoming resource metadata",
			}
			incomingResourceToUpdate, err := resource.NewResource(fullName, "view", resource.Bigquery, tnnt, incomingMetadata, viewSpec)
			assert.NoError(t, err)

			existing := resourceWithStatus(fullName, viewSpec, resource.StatusSuccess)

			repo := newResourceRepository(t)
			repo.On("ReadAll", ctx, tnnt, resource.Bigquery, onlyActive).Return([]*resource.Resource{existing}, nil)
			repo.On("Update", ctx, incomingResourceToUpdate).Return(errors.New("error in update"))

			urn, err := resource.ParseURN("bigquery://project:dataset.view1")
			assert.NoError(t, err)

			mgr := NewResourceManager(t)
			mgr.On("Validate", mock.Anything).Return(nil)
			mgr.On("GetURN", mock.Anything).Return(urn, nil)

			eventHandler := newEventHandler(t)
			alertmanager := new(mockAlertManager)
			alertmanager.On("SendResourceEvent", mock.Anything)
			rscService := service.NewResourceService(logger, repo, nil, mgr, eventHandler, nil, alertmanager)

			actualError := rscService.Deploy(ctx, tnnt, resource.Bigquery, []*resource.Resource{incomingResourceToUpdate}, logWriter)

			assert.ErrorContains(t, actualError, "error in update")
		})

		t.Run("returns error if error is encountered when updating as to_create on repo", func(t *testing.T) {
			fullName := "project.dataset.table1"
			incomingMetadata := &resource.Metadata{
				Description: "incoming resource metadata",
			}
			incomingResourceToUpdate, err := resource.NewResource(fullName, "view", resource.Bigquery, tnnt, incomingMetadata, viewSpec)
			assert.NoError(t, err)

			existing := resourceWithStatus(fullName, viewSpec, resource.StatusCreateFailure)

			repo := newResourceRepository(t)
			repo.On("ReadAll", ctx, tnnt, resource.Bigquery, onlyActive).Return([]*resource.Resource{existing}, nil)
			repo.On("Update", ctx, incomingResourceToUpdate).Return(nil)

			urn, err := resource.ParseURN("bigquery://project:dataset.view1")
			assert.NoError(t, err)

			mgr := NewResourceManager(t)
			mgr.On("Validate", mock.Anything).Return(nil)
			mgr.On("GetURN", mock.Anything).Return(urn, nil)
			mgr.On("BatchUpdate", ctx, resource.Bigquery, mock.Anything).Return(errors.New("unknown error"))

			eventHandler := newEventHandler(t)
			alertmanager := new(mockAlertManager)
			alertmanager.On("SendResourceEvent", mock.Anything)
			rscService := service.NewResourceService(logger, repo, nil, mgr, eventHandler, nil, alertmanager)

			actualError := rscService.Deploy(ctx, tnnt, resource.Bigquery, []*resource.Resource{incomingResourceToUpdate}, logWriter)
			assert.ErrorContains(t, actualError, "unknown error")
		})

		t.Run("returns error if error is encountered when batch updating to store", func(t *testing.T) {
			existingMetadata := &resource.Metadata{
				Description: "existing resource metadata",
			}
			fullName := "project.dataset"
			existingResource, err := resource.NewResource(fullName, "dataset", resource.Bigquery, tnnt, existingMetadata, spec)
			assert.NoError(t, err)
			incomingMetadata := &resource.Metadata{
				Description: "incoming resource metadata",
			}
			incomingResourceToUpdate, err := resource.NewResource(fullName, "dataset", resource.Bigquery, tnnt, incomingMetadata, spec)
			assert.NoError(t, err)

			repo := newResourceRepository(t)
			repo.On("ReadAll", ctx, tnnt, resource.Bigquery, onlyActive).Return([]*resource.Resource{existingResource}, nil)
			repo.On("Update", ctx, incomingResourceToUpdate).Return(nil)

			urn, err := resource.ParseURN("bigquery://project:dataset.view1")
			assert.NoError(t, err)

			mgr := NewResourceManager(t)
			mgr.On("Validate", mock.Anything).Return(nil)
			mgr.On("GetURN", mock.Anything).Return(urn, nil)
			mgr.On("BatchUpdate", ctx, resource.Bigquery, mock.Anything).Return(errors.New("unknown error"))

			eventHandler := newEventHandler(t)
			alertmanager := new(mockAlertManager)
			alertmanager.On("SendResourceEvent", mock.Anything)
			rscService := service.NewResourceService(logger, repo, nil, mgr, eventHandler, nil, alertmanager)

			actualError := rscService.Deploy(ctx, tnnt, resource.Bigquery, []*resource.Resource{incomingResourceToUpdate}, logWriter)
			assert.ErrorContains(t, actualError, "unknown error")
		})

		t.Run("returns nil if encountered error when refreshing downstream", func(t *testing.T) {
			existingToCreate := resourceWithStatus("project.dataset.view1", viewSpec, resource.StatusCreateFailure)
			existingToSkip := resourceWithStatus("project.dataset.view2", viewSpec, resource.StatusSuccess)
			existingToUpdate := resourceWithStatus("project.dataset.view3", viewSpec, resource.StatusUpdateFailure)

			updatedViewSpec := map[string]any{
				"view_query": "select 1;",
			}
			incomingToUpdate, err := resource.NewResource("project.dataset.view3", "view", resource.Bigquery, tnnt, meta, updatedViewSpec)
			assert.NoError(t, err)
			incomingToCreateExisting, resErr := resource.NewResource("project.dataset.view1", "view", resource.Bigquery, tnnt, meta, viewSpec)
			assert.NoError(t, resErr)
			incomingToSkip, resErr := resource.NewResource("project.dataset.view2", "view", resource.Bigquery, tnnt, meta, viewSpec)
			assert.NoError(t, resErr)
			incomingToCreate, resErr := resource.NewResource("project.dataset.view5", "view", resource.Bigquery, tnnt, meta, viewSpec)
			assert.NoError(t, resErr)

			repo := newResourceRepository(t)
			repo.On("ReadAll", ctx, tnnt, resource.Bigquery, onlyActive).Return([]*resource.Resource{existingToCreate, existingToSkip, existingToUpdate}, nil)
			repo.On("Create", ctx, incomingToCreate).Return(nil)
			repo.On("Update", ctx, incomingToUpdate).Return(nil)
			repo.On("Update", ctx, incomingToCreateExisting).Return(nil)

			urn, err := resource.ParseURN("bigquery://project:dataset.view1")
			assert.NoError(t, err)

			mgr := NewResourceManager(t)
			mgr.On("Validate", mock.Anything).Return(nil)
			mgr.On("GetURN", mock.Anything).Return(urn, nil)
			mgr.On("BatchUpdate", ctx, resource.Bigquery, []*resource.Resource{incomingToCreate, incomingToUpdate, incomingToCreateExisting}).Run(func(args mock.Arguments) {
				res := args.Get(2).([]*resource.Resource)
				for _, r := range res {
					r.MarkSuccess()
				}
			}).Return(nil)

			eventHandler := newEventHandler(t)
			alertmanager := new(mockAlertManager)
			alertmanager.On("SendResourceEvent", mock.Anything)
			argMatcher := mock.MatchedBy(func(ev moderator.Event) bool {
				return ev != nil
			})
			eventHandler.On("HandleEvent", argMatcher).Return().Times(3)

			refresher := new(mockDownstreamRefresher)
			refresher.On("RefreshResourceDownstream", ctx, mock.Anything, logWriter).Return(errors.New("unknown error"))

			rscService := service.NewResourceService(logger, repo, refresher, mgr, eventHandler, nil, alertmanager)

			incomings := []*resource.Resource{incomingToCreate, incomingToSkip, incomingToUpdate, incomingToCreateExisting}
			actualError := rscService.Deploy(ctx, tnnt, resource.Bigquery, incomings, logWriter)
			assert.ErrorContains(t, actualError, "unknown error")
		})

		t.Run("returns nil if no error is encountered", func(t *testing.T) {
			existingToCreate := resourceWithStatus("project.dataset.view1", viewSpec, resource.StatusCreateFailure)
			existingToSkip := resourceWithStatus("project.dataset.view2", viewSpec, resource.StatusSuccess)
			existingToUpdate := resourceWithStatus("project.dataset.view3", viewSpec, resource.StatusUpdateFailure)

			updatedViewSpec := map[string]any{
				"view_query": "select 1;",
			}
			incomingToUpdate, err := resource.NewResource("project.dataset.view3", "view", resource.Bigquery, tnnt, meta, updatedViewSpec)
			assert.NoError(t, err)
			incomingToCreateExisting, resErr := resource.NewResource("project.dataset.view1", "view", resource.Bigquery, tnnt, meta, viewSpec)
			assert.NoError(t, resErr)
			incomingToSkip, resErr := resource.NewResource("project.dataset.view2", "view", resource.Bigquery, tnnt, meta, viewSpec)
			assert.NoError(t, resErr)
			incomingToCreate, resErr := resource.NewResource("project.dataset.view5", "view", resource.Bigquery, tnnt, meta, viewSpec)
			assert.NoError(t, resErr)

			repo := newResourceRepository(t)
			repo.On("ReadAll", ctx, tnnt, resource.Bigquery, onlyActive).Return([]*resource.Resource{existingToCreate, existingToSkip, existingToUpdate}, nil)
			repo.On("Create", ctx, incomingToCreate).Return(nil)
			repo.On("Update", ctx, incomingToUpdate).Return(nil)
			repo.On("Update", ctx, incomingToCreateExisting).Return(nil)

			urn, err := resource.ParseURN("bigquery://project:dataset.view1")
			assert.NoError(t, err)

			mgr := NewResourceManager(t)
			mgr.On("Validate", mock.Anything).Return(nil)
			mgr.On("GetURN", mock.Anything).Return(urn, nil)
			mgr.On("BatchUpdate", ctx, resource.Bigquery, []*resource.Resource{incomingToCreate, incomingToUpdate, incomingToCreateExisting}).Run(func(args mock.Arguments) {
				res := args.Get(2).([]*resource.Resource)
				for _, r := range res {
					r.MarkSuccess()
				}
			}).Return(nil)

			eventHandler := newEventHandler(t)
			alertmanager := new(mockAlertManager)
			alertmanager.On("SendResourceEvent", mock.Anything)
			argMatcher := mock.MatchedBy(func(ev moderator.Event) bool {
				return ev != nil
			})
			eventHandler.On("HandleEvent", argMatcher).Return().Times(3)

			refresher := new(mockDownstreamRefresher)
			refresher.On("RefreshResourceDownstream", ctx, mock.Anything, logWriter).Return(nil)

			rscService := service.NewResourceService(logger, repo, refresher, mgr, eventHandler, nil, alertmanager)

			incomings := []*resource.Resource{incomingToCreate, incomingToSkip, incomingToUpdate, incomingToCreateExisting}
			actualError := rscService.Deploy(ctx, tnnt, resource.Bigquery, incomings, logWriter)
			assert.NoError(t, actualError)
		})

		t.Run("returns nil if no error is encountered with a recreation resource", func(t *testing.T) {
			existingToCreate := resourceWithStatus("project.dataset.view1", viewSpec, resource.StatusCreateFailure)
			existingToSkip := resourceWithStatus("project.dataset.view2", viewSpec, resource.StatusSuccess)
			existingToUpdate := resourceWithStatus("project.dataset.view3", viewSpec, resource.StatusUpdateFailure)
			existingToRecreate := resourceWithStatus("project.dataset.view4", viewSpec, resource.StatusDeleted)

			updatedViewSpec := map[string]any{
				"view_query": "select 1;",
			}
			incomingToUpdate, err := resource.NewResource("project.dataset.view3", "view", resource.Bigquery, tnnt, meta, updatedViewSpec)
			assert.NoError(t, err)
			incomingToCreateExisting, resErr := resource.NewResource("project.dataset.view1", "view", resource.Bigquery, tnnt, meta, viewSpec)
			assert.NoError(t, resErr)
			incomingToSkip, resErr := resource.NewResource("project.dataset.view2", "view", resource.Bigquery, tnnt, meta, viewSpec)
			assert.NoError(t, resErr)
			incomingToCreate, resErr := resource.NewResource("project.dataset.view5", "view", resource.Bigquery, tnnt, meta, viewSpec)
			assert.NoError(t, resErr)
			incomingToRecreate, resErr := resource.NewResource("project.dataset.view4", "view", resource.Bigquery, tnnt, meta, viewSpec)
			assert.NoError(t, resErr)

			repo := newResourceRepository(t)
			repo.On("ReadAll", ctx, tnnt, resource.Bigquery, onlyActive).Return([]*resource.Resource{existingToCreate, existingToSkip, existingToUpdate, existingToRecreate}, nil)
			repo.On("Create", ctx, incomingToCreate).Return(nil)
			repo.On("Update", ctx, incomingToUpdate).Return(nil)
			repo.On("Update", ctx, incomingToCreateExisting).Return(nil)
			repo.On("Update", ctx, incomingToRecreate).Return(nil)

			urn, err := resource.ParseURN("bigquery://project:dataset.view1")
			assert.NoError(t, err)

			mgr := NewResourceManager(t)
			mgr.On("Validate", mock.Anything).Return(nil)
			mgr.On("GetURN", mock.Anything).Return(urn, nil)
			mgr.On("BatchUpdate", ctx, resource.Bigquery, []*resource.Resource{incomingToCreate, incomingToUpdate, incomingToCreateExisting, incomingToRecreate}).
				Run(func(args mock.Arguments) {
					res := args.Get(2).([]*resource.Resource)
					for _, r := range res {
						if r.Status() == resource.StatusToDelete {
							r.MarkDeleted()
							continue
						}
						r.MarkSuccess()
					}
				}).
				Return(nil)

			eventHandler := newEventHandler(t)
			alertmanager := new(mockAlertManager)
			alertmanager.On("SendResourceEvent", mock.Anything)
			argMatcher := mock.MatchedBy(func(ev moderator.Event) bool {
				return ev != nil
			})
			eventHandler.On("HandleEvent", argMatcher).Return().Times(4)

			refresher := new(mockDownstreamRefresher)
			refresher.On("RefreshResourceDownstream", ctx, mock.Anything, logWriter).Return(nil)

			rscService := service.NewResourceService(logger, repo, refresher, mgr, eventHandler, nil, alertmanager)

			incomings := []*resource.Resource{incomingToCreate, incomingToSkip, incomingToUpdate, incomingToCreateExisting, incomingToRecreate}
			actualError := rscService.Deploy(ctx, tnnt, resource.Bigquery, incomings, logWriter)
			assert.NoError(t, actualError)
		})
	})

	t.Run("SyncResource", func(t *testing.T) {
		t.Run("returns error when get resources returns error", func(t *testing.T) {
			fullName := "project.dataset"
			repo := newResourceRepository(t)
			repo.On("GetResources", ctx, tnnt, resource.Bigquery, []string{fullName}).Return(nil, errors.New("unknown error"))

			mgr := NewResourceManager(t)

			rscService := service.NewResourceService(logger, repo, nil, mgr, nil, nil, nil)

			resp, actualError := rscService.SyncResources(ctx, tnnt, resource.Bigquery, []string{fullName})
			assert.ErrorContains(t, actualError, "unknown error")
			assert.Nil(t, resp)
		})
		t.Run("returns if no resources to sync", func(t *testing.T) {
			fullName := "project.dataset"
			repo := newResourceRepository(t)
			repo.On("GetResources", ctx, tnnt, resource.Bigquery, []string{fullName}).Return([]*resource.Resource{}, nil)

			mgr := NewResourceManager(t)

			rscService := service.NewResourceService(logger, repo, nil, mgr, nil, nil, nil)

			response, actualError := rscService.SyncResources(ctx, tnnt, resource.Bigquery, []string{fullName})
			assert.Nil(t, actualError)
			assert.Equal(t, fullName, response.IgnoredResources[0].Name)
			assert.Equal(t, "no resource found in namespace", response.IgnoredResources[0].Reason)
			assert.Equal(t, 0, len(response.ResourceNames))
		})
		t.Run("returns error while syncing resource", func(t *testing.T) {
			fullName := "project.dataset"
			incoming, err := resource.NewResource(fullName, "dataset", resource.Bigquery, tnnt, meta, spec)
			assert.NoError(t, err)

			repo := newResourceRepository(t)
			repo.On("GetResources", ctx, tnnt, resource.Bigquery, []string{fullName}).
				Return([]*resource.Resource{incoming}, nil)

			mgr := NewResourceManager(t)
			mgr.On("SyncResource", ctx, incoming).Return(errors.New("unable to create"))

			rscService := service.NewResourceService(logger, repo, nil, mgr, nil, nil, nil)

			response, actualError := rscService.SyncResources(ctx, tnnt, resource.Bigquery, []string{fullName})
			assert.Nil(t, actualError)
			assert.Equal(t, fullName, response.IgnoredResources[0].Name)
			assert.Equal(t, "unable to create", response.IgnoredResources[0].Reason)
			assert.Equal(t, 0, len(response.ResourceNames))
		})
		t.Run("syncs the resource successfully", func(t *testing.T) {
			fullName := "project.dataset"
			incoming, err := resource.NewResource(fullName, "dataset", resource.Bigquery, tnnt, meta, spec)
			assert.NoError(t, err)

			repo := newResourceRepository(t)
			repo.On("GetResources", ctx, tnnt, resource.Bigquery, []string{fullName}).
				Return([]*resource.Resource{incoming}, nil)

			mgr := NewResourceManager(t)
			mgr.On("SyncResource", ctx, incoming).Return(nil)

			rscService := service.NewResourceService(logger, repo, nil, mgr, nil, nil, nil)

			response, actualError := rscService.SyncResources(ctx, tnnt, resource.Bigquery, []string{fullName})
			assert.Nil(t, actualError)
			assert.Equal(t, fullName, response.ResourceNames[0])
			assert.Equal(t, 0, len(response.IgnoredResources))
		})
	})

	t.Run("Delete", func(t *testing.T) {
		onlyActive := true
		downstreamList := job.DownstreamList{job.NewDownstream("projA/JobA", tnnt.ProjectName(), tnnt.NamespaceName(), "taskA")}
		resourceName := "dataset.table_test"
		t.Run("success without downstream jobs", func(t *testing.T) {
			var (
				mockDepResolver = new(mockDownstreamResolver)
				repo            = newResourceRepository(t)
				rscService      = service.NewResourceService(logger, repo, nil, nil, nil, mockDepResolver, nil)
				req             = &resource.DeleteRequest{
					Tenant:    tnnt,
					Datastore: resource.Bigquery,
					FullName:  resourceName,
					Force:     false,
				}
				existingResource, _ = resource.NewResource(resourceName, "dataset", resource.Bigquery, tnnt, meta, spec)
			)
			defer mockDepResolver.AssertExpectations(t)

			repo.On("ReadByFullName", ctx, req.Tenant, req.Datastore, req.FullName, onlyActive).Return(existingResource, nil)
			mockDepResolver.On("GetDownstreamByResourceURN", ctx, existingResource.Tenant(), existingResource.URN()).
				Return(nil, nil)
			repo.On("Delete", ctx, existingResource).Return(nil)

			actual, err := rscService.Delete(ctx, req)
			assert.NoError(t, err)
			assert.NotNil(t, actual)
			assert.Empty(t, actual.DownstreamJobs)
		})

		t.Run("success with downstream jobs and force true", func(t *testing.T) {
			var (
				mockDepResolver = new(mockDownstreamResolver)
				repo            = newResourceRepository(t)
				rscService      = service.NewResourceService(logger, repo, nil, nil, nil, mockDepResolver, nil)
				req             = &resource.DeleteRequest{
					Tenant:    tnnt,
					Datastore: resource.Bigquery,
					FullName:  resourceName,
					Force:     true,
				}
				existingResource, _ = resource.NewResource(resourceName, "dataset", resource.Bigquery, tnnt, meta, spec)
			)
			defer mockDepResolver.AssertExpectations(t)

			repo.On("ReadByFullName", ctx, req.Tenant, req.Datastore, req.FullName, onlyActive).Return(existingResource, nil)
			mockDepResolver.On("GetDownstreamByResourceURN", ctx, existingResource.Tenant(), existingResource.URN()).
				Return(downstreamList, nil)
			repo.On("Delete", ctx, existingResource).Return(nil)

			actual, err := rscService.Delete(ctx, req)
			assert.NoError(t, err)
			assert.NotNil(t, actual)
			assert.NotEmpty(t, actual.DownstreamJobs)
			assert.Len(t, actual.DownstreamJobs, len(downstreamList))

			var downstreamSlice []*job.Downstream = downstreamList
			for i := range actual.DownstreamJobs {
				assert.EqualValues(t, actual.DownstreamJobs[i], downstreamSlice[i].FullName())
			}
		})

		t.Run("return error when delete", func(t *testing.T) {
			var (
				mockDepResolver = new(mockDownstreamResolver)
				repo            = newResourceRepository(t)
				rscService      = service.NewResourceService(logger, repo, nil, nil, nil, mockDepResolver, nil)
				req             = &resource.DeleteRequest{
					Tenant:    tnnt,
					Datastore: resource.Bigquery,
					FullName:  resourceName,
					Force:     false,
				}
				existingResource, _ = resource.NewResource(resourceName, "dataset", resource.Bigquery, tnnt, meta, spec)
			)
			defer mockDepResolver.AssertExpectations(t)

			repo.On("ReadByFullName", ctx, req.Tenant, req.Datastore, req.FullName, onlyActive).Return(existingResource, nil)
			mockDepResolver.On("GetDownstreamByResourceURN", ctx, existingResource.Tenant(), existingResource.URN()).
				Return(nil, nil)
			repo.On("Delete", ctx, existingResource).Return(context.DeadlineExceeded)

			actual, err := rscService.Delete(ctx, req)
			assert.Error(t, err)
			assert.Nil(t, actual)
		})

		t.Run("return error when downstream still exists", func(t *testing.T) {
			var (
				mockDepResolver = new(mockDownstreamResolver)
				repo            = newResourceRepository(t)
				rscService      = service.NewResourceService(logger, repo, nil, nil, nil, mockDepResolver, nil)
				req             = &resource.DeleteRequest{
					Tenant:    tnnt,
					Datastore: resource.Bigquery,
					FullName:  resourceName,
					Force:     false,
				}
				existingResource, _ = resource.NewResource(resourceName, "dataset", resource.Bigquery, tnnt, meta, spec)
			)
			defer mockDepResolver.AssertExpectations(t)

			repo.On("ReadByFullName", ctx, req.Tenant, req.Datastore, req.FullName, onlyActive).Return(existingResource, nil)
			mockDepResolver.On("GetDownstreamByResourceURN", ctx, existingResource.Tenant(), existingResource.URN()).
				Return(downstreamList, nil)

			actual, err := rscService.Delete(ctx, req)
			assert.Error(t, err)
			assert.ErrorContains(t, err, "failed precondition for entity resource: there are still resource using dataset.table_test, jobs: [project_test/projA/JobA]")
			assert.Nil(t, actual)
		})

		t.Run("return error when resolve downstream dependency", func(t *testing.T) {
			var (
				mockDepResolver = new(mockDownstreamResolver)
				repo            = newResourceRepository(t)
				rscService      = service.NewResourceService(logger, repo, nil, nil, nil, mockDepResolver, nil)
				req             = &resource.DeleteRequest{
					Tenant:    tnnt,
					Datastore: resource.Bigquery,
					FullName:  resourceName,
					Force:     false,
				}
				existingResource, _ = resource.NewResource(resourceName, "dataset", resource.Bigquery, tnnt, meta, spec)
			)
			defer mockDepResolver.AssertExpectations(t)

			repo.On("ReadByFullName", ctx, req.Tenant, req.Datastore, req.FullName, onlyActive).Return(existingResource, nil)
			mockDepResolver.On("GetDownstreamByResourceURN", ctx, existingResource.Tenant(), existingResource.URN()).
				Return(downstreamList, context.DeadlineExceeded)

			actual, err := rscService.Delete(ctx, req)
			assert.Error(t, err)
			assert.Nil(t, actual)
		})

		t.Run("return error when get resource", func(t *testing.T) {
			var (
				mockDepResolver = new(mockDownstreamResolver)
				repo            = newResourceRepository(t)
				rscService      = service.NewResourceService(logger, repo, nil, nil, nil, mockDepResolver, nil)
				req             = &resource.DeleteRequest{
					Tenant:    tnnt,
					Datastore: resource.Bigquery,
					FullName:  resourceName,
					Force:     false,
				}
			)
			defer mockDepResolver.AssertExpectations(t)

			repo.On("ReadByFullName", ctx, req.Tenant, req.Datastore, req.FullName, onlyActive).Return(nil, context.DeadlineExceeded)

			actual, err := rscService.Delete(ctx, req)
			assert.Error(t, err)
			assert.Nil(t, actual)
		})
	})
}

type mockResourceRepository struct {
	mock.Mock
}

func (m *mockResourceRepository) Create(ctx context.Context, res *resource.Resource) error {
	return m.Called(ctx, res).Error(0)
}

func (m *mockResourceRepository) ReadAll(ctx context.Context, tnnt tenant.Tenant, store resource.Store, onlyActive bool) ([]*resource.Resource, error) {
	args := m.Called(ctx, tnnt, store, onlyActive)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]*resource.Resource), args.Error(1)
}

func (m *mockResourceRepository) ReadByFullName(ctx context.Context, tnnt tenant.Tenant, store resource.Store, fullName string, onlyActive bool) (*resource.Resource, error) {
	args := m.Called(ctx, tnnt, store, fullName, onlyActive)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*resource.Resource), args.Error(1)
}

func (m *mockResourceRepository) Update(ctx context.Context, res *resource.Resource) error {
	return m.Called(ctx, res).Error(0)
}

func (m *mockResourceRepository) ChangeNamespace(ctx context.Context, res *resource.Resource, newTenant tenant.Tenant) error {
	return m.Called(ctx, res, newTenant).Error(0)
}

func (m *mockResourceRepository) GetResources(ctx context.Context, tnnt tenant.Tenant, store resource.Store, names []string) ([]*resource.Resource, error) {
	args := m.Called(ctx, tnnt, store, names)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]*resource.Resource), args.Error(1)
}

func (m *mockResourceRepository) Delete(ctx context.Context, res *resource.Resource) error {
	return m.Called(ctx, res).Error(0)
}

type mockConstructorTestingTNewResourceRepository interface {
	mock.TestingT
	Cleanup(func())
}

func newResourceRepository(t mockConstructorTestingTNewResourceRepository) *mockResourceRepository {
	mock := &mockResourceRepository{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}

type mockEventHandler struct {
	mock.Mock
}

func (m *mockEventHandler) HandleEvent(e moderator.Event) {
	m.Called(e)
}

type mockConstructorEventHandler interface {
	mock.TestingT
	Cleanup(func())
}

func newEventHandler(t mockConstructorEventHandler) *mockEventHandler {
	mock := &mockEventHandler{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}

type mockDownstreamRefresher struct {
	mock.Mock
}

func (m *mockDownstreamRefresher) RefreshResourceDownstream(ctx context.Context, resourceURNs []resource.URN, logWriter writer.LogWriter) error {
	return m.Called(ctx, resourceURNs, logWriter).Error(0)
}

// ResourceManager is an autogenerated mock type for the ResourceManager type
type ResourceManager struct {
	mock.Mock
}

// BatchUpdate provides a mock function with given fields: ctx, store, resources
func (_m *ResourceManager) BatchUpdate(ctx context.Context, store resource.Store, resources []*resource.Resource) error {
	ret := _m.Called(ctx, store, resources)

	if len(ret) == 0 {
		panic("no return value specified for BatchUpdate")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, resource.Store, []*resource.Resource) error); ok {
		r0 = rf(ctx, store, resources)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// CreateResource provides a mock function with given fields: ctx, res
func (_m *ResourceManager) CreateResource(ctx context.Context, res *resource.Resource) error {
	ret := _m.Called(ctx, res)

	if len(ret) == 0 {
		panic("no return value specified for CreateResource")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, *resource.Resource) error); ok {
		r0 = rf(ctx, res)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// Exist provides a mock function with given fields: ctx, tnnt, urn
func (_m *ResourceManager) Exist(ctx context.Context, tnnt tenant.Tenant, urn resource.URN) (bool, error) {
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
func (_m *ResourceManager) GetURN(res *resource.Resource) (resource.URN, error) {
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

// SyncResource provides a mock function with given fields: ctx, res
func (_m *ResourceManager) SyncResource(ctx context.Context, res *resource.Resource) error {
	ret := _m.Called(ctx, res)

	if len(ret) == 0 {
		panic("no return value specified for SyncResource")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, *resource.Resource) error); ok {
		r0 = rf(ctx, res)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// UpdateResource provides a mock function with given fields: ctx, res
func (_m *ResourceManager) UpdateResource(ctx context.Context, res *resource.Resource) error {
	ret := _m.Called(ctx, res)

	if len(ret) == 0 {
		panic("no return value specified for UpdateResource")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, *resource.Resource) error); ok {
		r0 = rf(ctx, res)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// Validate provides a mock function with given fields: res
func (_m *ResourceManager) Validate(res *resource.Resource) error {
	ret := _m.Called(res)

	if len(ret) == 0 {
		panic("no return value specified for Validate")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(*resource.Resource) error); ok {
		r0 = rf(res)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// NewResourceManager creates a new instance of ResourceManager. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewResourceManager(t interface {
	mock.TestingT
	Cleanup(func())
},
) *ResourceManager {
	mock := &ResourceManager{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}

type mockDownstreamResolver struct {
	mock.Mock
}

func (m *mockDownstreamResolver) GetDownstreamByResourceURN(ctx context.Context, tnnt tenant.Tenant, urn resource.URN) (job.DownstreamList, error) {
	args := m.Called(ctx, tnnt, urn)
	if args.Get(0) != nil {
		return args.Get(0).(job.DownstreamList), args.Error(1)
	}
	return nil, args.Error(1)
}

type mockAlertManager struct {
	mock.Mock
}

func (m *mockAlertManager) Close() error {
	args := m.Called()
	return args.Error(0)
}

func (m *mockAlertManager) SendResourceEvent(attr *resource.AlertAttrs) {
	m.Called(attr)
}
