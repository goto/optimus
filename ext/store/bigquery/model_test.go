package bigquery_test

import (
	"context"
	"errors"
	"testing"

	"cloud.google.com/go/bigquery"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/goto/optimus/core/resource"
	"github.com/goto/optimus/core/tenant"
	storebigquery "github.com/goto/optimus/ext/store/bigquery"
)

func TestModelHandle(t *testing.T) {
	ctx := context.Background()
	bqStore := resource.Bigquery
	tnnt, _ := tenant.NewTenant("proj", "ns")
	metadata := resource.Metadata{
		Version:     1,
		Description: "resource description",
		Labels:      map[string]string{"owner": "optimus"},
	}
	spec := map[string]any{"description": []string{"a", "b"}}
	res, err := resource.NewResource("proj.dataset.view1", storebigquery.KindView, bqStore, tnnt, &metadata, spec, nil)
	assert.Nil(t, err)

	t.Run("Create", func(t *testing.T) {
		t.Run("return error, not supported", func(t *testing.T) {
			v := NewMockBigQueryModel(t)
			defer v.AssertExpectations(t)
			handle := storebigquery.NewModelHandle(v)

			err := handle.Create(ctx, res)
			assert.EqualError(t, err, "failed precondition for entity resource_model: create is not supported")
		})
	})

	t.Run("Update", func(t *testing.T) {
		t.Run("return error, not supported", func(t *testing.T) {
			v := NewMockBigQueryModel(t)
			defer v.AssertExpectations(t)
			handle := storebigquery.NewModelHandle(v)

			err := handle.Update(ctx, res)
			assert.EqualError(t, err, "failed precondition for entity resource_model: update is not supported")
		})
	})

	t.Run("Exists", func(t *testing.T) {
		t.Run("return true, model exists", func(t *testing.T) {
			v := NewMockBigQueryModel(t)
			defer v.AssertExpectations(t)
			handle := storebigquery.NewModelHandle(v)

			v.On("Metadata", ctx).Return(nil, nil)

			actual := handle.Exists(ctx)
			assert.True(t, actual)
		})

		t.Run("return false, model not exists", func(t *testing.T) {
			v := NewMockBigQueryModel(t)
			defer v.AssertExpectations(t)
			handle := storebigquery.NewModelHandle(v)

			v.On("Metadata", ctx).Return(nil, errors.New("some error"))

			actual := handle.Exists(ctx)
			assert.False(t, actual)
		})

		t.Run("return false, connection error", func(t *testing.T) {
			v := NewMockBigQueryModel(t)
			defer v.AssertExpectations(t)
			handle := storebigquery.NewModelHandle(v)

			v.On("Metadata", ctx).Return(nil, context.DeadlineExceeded)

			actual := handle.Exists(ctx)
			assert.False(t, actual)
		})

		t.Run("return false, BqModel is nil", func(t *testing.T) {
			v := NewMockBigQueryModel(t)
			defer v.AssertExpectations(t)
			handle := storebigquery.NewModelHandle(nil)

			actual := handle.Exists(ctx)
			assert.False(t, actual)
		})
	})
}

type mockBigQueryModel struct {
	mock.Mock
}

func (_m *mockBigQueryModel) Metadata(ctx context.Context) (*bigquery.ModelMetadata, error) {
	ret := _m.Called(ctx)

	if len(ret) == 0 {
		panic("no return value specified for Metadata")
	}

	var r0 *bigquery.ModelMetadata
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context) (*bigquery.ModelMetadata, error)); ok {
		return rf(ctx)
	}
	if rf, ok := ret.Get(0).(func(context.Context) *bigquery.ModelMetadata); ok {
		r0 = rf(ctx)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*bigquery.ModelMetadata)
		}
	}

	if rf, ok := ret.Get(1).(func(context.Context) error); ok {
		r1 = rf(ctx)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

func NewMockBigQueryModel(t interface {
	mock.TestingT
	Cleanup(func())
},
) *mockBigQueryModel {
	mock := &mockBigQueryModel{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
