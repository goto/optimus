package maxcompute_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/goto/optimus/core/resource"
	"github.com/goto/optimus/core/tenant"
	"github.com/goto/optimus/ext/store/maxcompute"
)

type mockMaxComputeSchemaInteractor struct {
	mock.Mock
}

func (m *mockMaxComputeSchemaInteractor) Exists() (bool, error) {
	args := m.Called()
	if args.Get(0) == nil {
		return false, args.Error(1)
	}
	return args.Bool(0), args.Error(1)
}

func TestSchemaHandle(t *testing.T) {
	projectName, schemaName := "proj", "schema"
	fullName := projectName + "." + schemaName
	mcStore := resource.MaxCompute
	tnnt, _ := tenant.NewTenant(projectName, "ns")
	metadata := resource.Metadata{
		Version:     1,
		Description: "resource description",
		Labels:      map[string]string{"owner": "optimus"},
	}

	t.Run("Create", func(t *testing.T) {
		t.Run("returns error when cannot convert spec", func(t *testing.T) {
			interactor := new(mockMaxComputeSchemaInteractor)
			schema := new(mockMaxComputeSchema)
			defer func() {
				interactor.AssertExpectations(t)
				schema.AssertExpectations(t)
			}()
			schemaHandle := maxcompute.NewSchemaHandle(schema, interactor)

			spec := map[string]any{"description": []string{"test create"}}
			res, err := resource.NewResource(fullName, maxcompute.KindSchema, mcStore, tnnt, &metadata, spec)
			assert.Nil(t, err)

			err = schemaHandle.Create(res)
			assert.Error(t, err)
			assert.ErrorContains(t, err, "not able to decode spec for "+fullName)
		})

		t.Run("returns error when failed to create schema", func(t *testing.T) {
			interactor := new(mockMaxComputeSchemaInteractor)
			schema := new(mockMaxComputeSchema)
			defer func() {
				interactor.AssertExpectations(t)
				schema.AssertExpectations(t)
			}()
			schemaHandle := maxcompute.NewSchemaHandle(schema, interactor)

			spec := map[string]any{"description": "test create"}
			res, err := resource.NewResource(fullName, maxcompute.KindSchema, mcStore, tnnt, &metadata, spec)
			assert.Nil(t, err)

			schema.On("Create", schemaName, true, spec["description"]).Return(context.DeadlineExceeded)

			err = schemaHandle.Create(res)
			assert.Error(t, err)
			assert.ErrorContains(t, err, "error while creating schema on maxcompute")
		})

		t.Run("returns success when create project schema", func(t *testing.T) {
			interactor := new(mockMaxComputeSchemaInteractor)
			schema := new(mockMaxComputeSchema)
			defer func() {
				interactor.AssertExpectations(t)
				schema.AssertExpectations(t)
			}()
			schemaHandle := maxcompute.NewSchemaHandle(schema, interactor)

			spec := map[string]any{"description": "test create"}
			res, err := resource.NewResource(fullName, maxcompute.KindSchema, mcStore, tnnt, &metadata, spec)
			assert.Nil(t, err)

			schema.On("Create", schemaName, true, spec["description"]).Return(nil)

			err = schemaHandle.Create(res)
			assert.NoError(t, err)
		})
	})

	t.Run("Update", func(t *testing.T) {
		t.Run("returns error when cannot convert spec", func(t *testing.T) {
			interactor := new(mockMaxComputeSchemaInteractor)
			schema := new(mockMaxComputeSchema)
			defer func() {
				interactor.AssertExpectations(t)
				schema.AssertExpectations(t)
			}()
			schemaHandle := maxcompute.NewSchemaHandle(schema, interactor)

			spec := map[string]any{"description": []string{"test create"}}
			res, err := resource.NewResource(fullName, maxcompute.KindSchema, mcStore, tnnt, &metadata, spec)
			assert.Nil(t, err)

			err = schemaHandle.Update(res)
			assert.Error(t, err)
			assert.ErrorContains(t, err, "not able to decode spec for "+fullName)
		})

		t.Run("returns error when failed to update schema", func(t *testing.T) {
			interactor := new(mockMaxComputeSchemaInteractor)
			schema := new(mockMaxComputeSchema)
			defer func() {
				interactor.AssertExpectations(t)
				schema.AssertExpectations(t)
			}()
			schemaHandle := maxcompute.NewSchemaHandle(schema, interactor)

			spec := map[string]any{"description": "test create"}
			res, err := resource.NewResource(fullName, maxcompute.KindSchema, mcStore, tnnt, &metadata, spec)
			assert.Nil(t, err)

			schema.On("Create", schemaName, false, spec["description"]).Return(context.DeadlineExceeded)

			err = schemaHandle.Update(res)
			assert.Error(t, err)
			assert.ErrorContains(t, err, "error while updating schema on maxcompute")
		})

		t.Run("returns success when create project schema", func(t *testing.T) {
			interactor := new(mockMaxComputeSchemaInteractor)
			schema := new(mockMaxComputeSchema)
			defer func() {
				interactor.AssertExpectations(t)
				schema.AssertExpectations(t)
			}()
			schemaHandle := maxcompute.NewSchemaHandle(schema, interactor)

			spec := map[string]any{"description": "test create"}
			res, err := resource.NewResource(fullName, maxcompute.KindSchema, mcStore, tnnt, &metadata, spec)
			assert.Nil(t, err)

			schema.On("Create", schemaName, false, spec["description"]).Return(nil)

			err = schemaHandle.Update(res)
			assert.NoError(t, err)
		})
	})

	t.Run("Exists", func(t *testing.T) {
		t.Run("returns false when error in checking existing schema", func(t *testing.T) {
			interactor := new(mockMaxComputeSchemaInteractor)
			schema := new(mockMaxComputeSchema)
			defer func() {
				interactor.AssertExpectations(t)
				schema.AssertExpectations(t)
			}()
			schemaHandle := maxcompute.NewSchemaHandle(schema, interactor)

			interactor.On("Exists").Return(false, context.DeadlineExceeded)

			exists := schemaHandle.Exists("")
			assert.False(t, exists)
		})

		t.Run("returns false when schema not found", func(t *testing.T) {
			interactor := new(mockMaxComputeSchemaInteractor)
			schema := new(mockMaxComputeSchema)
			defer func() {
				interactor.AssertExpectations(t)
				schema.AssertExpectations(t)
			}()
			schemaHandle := maxcompute.NewSchemaHandle(schema, interactor)

			interactor.On("Exists").Return(false, nil)

			exists := schemaHandle.Exists("")
			assert.False(t, exists)
		})

		t.Run("returns true when checking existing schema", func(t *testing.T) {
			interactor := new(mockMaxComputeSchemaInteractor)
			schema := new(mockMaxComputeSchema)
			defer func() {
				interactor.AssertExpectations(t)
				schema.AssertExpectations(t)
			}()
			schemaHandle := maxcompute.NewSchemaHandle(schema, interactor)

			interactor.On("Exists").Return(true, nil)

			exists := schemaHandle.Exists("")
			assert.True(t, exists)
		})
	})
}
