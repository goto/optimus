package bigquery_test

import (
	"context"
	"errors"
	"testing"

	"cloud.google.com/go/bigquery"

	"github.com/goto/optimus/core/resource"
	"github.com/goto/optimus/core/tenant"
	storebigquery "github.com/goto/optimus/ext/store/bigquery"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestRoutineHandle(t *testing.T) {
	ctx := context.Background()
	bqStore := resource.Bigquery
	tnnt, _ := tenant.NewTenant("proj", "ns")
	metadata := resource.Metadata{
		Version:     1,
		Description: "resource description",
		Labels:      map[string]string{"owner": "optimus"},
	}
	spec := map[string]any{"description": []string{"a", "b"}}
	res, err := resource.NewResource("proj.dataset.view1", storebigquery.KindView, bqStore, tnnt, &metadata, spec)
	assert.Nil(t, err)

	t.Run("Create", func(t *testing.T) {
		t.Run("return error, not supported", func(t *testing.T) {
			v := new(mockBigQueryRoutine)
			handle := storebigquery.NewRoutineHandle(v)

			err := handle.Create(ctx, res)
			assert.EqualError(t, err, "failed precondition for entity routines: create is not supported")
		})
	})

	t.Run("Update", func(t *testing.T) {
		t.Run("return error, not supported", func(t *testing.T) {
			v := new(mockBigQueryRoutine)
			handle := storebigquery.NewRoutineHandle(v)

			err := handle.Update(ctx, res)
			assert.EqualError(t, err, "failed precondition for entity routines: update is not supported")
		})
	})

	t.Run("Exists", func(t *testing.T) {
		t.Run("return true, routine exists", func(t *testing.T) {
			v := new(mockBigQueryRoutine)
			handle := storebigquery.NewRoutineHandle(v)

			v.On("Metadata", ctx).Return(nil, nil)

			actual := handle.Exists(ctx)
			assert.True(t, actual)
		})

		t.Run("return false, routine not exists", func(t *testing.T) {
			v := new(mockBigQueryRoutine)
			handle := storebigquery.NewRoutineHandle(v)

			v.On("Metadata", ctx).Return(nil, errors.New("some error"))

			actual := handle.Exists(ctx)
			assert.False(t, actual)
		})

		t.Run("return false, connection error", func(t *testing.T) {
			v := new(mockBigQueryRoutine)
			handle := storebigquery.NewRoutineHandle(v)

			v.On("Metadata", ctx).Return(nil, context.DeadlineExceeded)

			actual := handle.Exists(ctx)
			assert.False(t, actual)
		})
	})
}

type mockBigQueryRoutine struct {
	mock.Mock
}

func (_m *mockBigQueryRoutine) Create(ctx context.Context, rm *bigquery.RoutineMetadata) error {
	ret := _m.Called(ctx, rm)

	if len(ret) == 0 {
		panic("no return value specified for Create")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, *bigquery.RoutineMetadata) error); ok {
		r0 = rf(ctx, rm)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

func (_m *mockBigQueryRoutine) Metadata(ctx context.Context) (*bigquery.RoutineMetadata, error) {
	ret := _m.Called(ctx)

	if len(ret) == 0 {
		panic("no return value specified for Metadata")
	}

	var r0 *bigquery.RoutineMetadata
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context) (*bigquery.RoutineMetadata, error)); ok {
		return rf(ctx)
	}
	if rf, ok := ret.Get(0).(func(context.Context) *bigquery.RoutineMetadata); ok {
		r0 = rf(ctx)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*bigquery.RoutineMetadata)
		}
	}

	if rf, ok := ret.Get(1).(func(context.Context) error); ok {
		r1 = rf(ctx)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

func (_m *mockBigQueryRoutine) Update(ctx context.Context, upd *bigquery.RoutineMetadataToUpdate, etag string) (*bigquery.RoutineMetadata, error) {
	ret := _m.Called(ctx, upd, etag)

	if len(ret) == 0 {
		panic("no return value specified for Update")
	}

	var r0 *bigquery.RoutineMetadata
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context, *bigquery.RoutineMetadataToUpdate, string) (*bigquery.RoutineMetadata, error)); ok {
		return rf(ctx, upd, etag)
	}
	if rf, ok := ret.Get(0).(func(context.Context, *bigquery.RoutineMetadataToUpdate, string) *bigquery.RoutineMetadata); ok {
		r0 = rf(ctx, upd, etag)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*bigquery.RoutineMetadata)
		}
	}

	if rf, ok := ret.Get(1).(func(context.Context, *bigquery.RoutineMetadataToUpdate, string) error); ok {
		r1 = rf(ctx, upd, etag)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

func NewBqRoutine(t interface {
	mock.TestingT
	Cleanup(func())
}) *mockBigQueryRoutine {
	mock := &mockBigQueryRoutine{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
