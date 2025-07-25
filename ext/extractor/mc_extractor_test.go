package extractor_test

import (
	"context"
	"errors"
	"testing"

	"github.com/goto/salt/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/goto/optimus/ext/extractor"
)

func TestMCExtractor(t *testing.T) {
	t.Run("should return error if client is nil", func(t *testing.T) {
		me, err := extractor.NewMCExtractor(nil, nil)
		assert.ErrorContains(t, err, "client is nil")
		assert.Nil(t, me)
	})
	t.Run("should return error if logger is nil", func(t *testing.T) {
		client := new(ViewGetter)
		defer client.AssertExpectations(t)
		me, err := extractor.NewMCExtractor(client, nil)
		assert.ErrorContains(t, err, "logger is nil")
		assert.Nil(t, me)
	})
	t.Run("should return error if get ddl is fail", func(t *testing.T) {
		client := new(ViewGetter)
		defer client.AssertExpectations(t)

		client.On("GetDDLView", mock.Anything, mock.Anything).Return("", errors.New("some error"))
		me, _ := extractor.NewMCExtractor(client, log.NewNoop())
		urnToDDL, err := me.Extract(context.Background(), []string{"project.schema.table_view"})
		assert.ErrorContains(t, err, "some error")
		assert.Empty(t, urnToDDL)
	})
	t.Run("should return ddl if get ddl is success", func(t *testing.T) {
		client := new(ViewGetter)
		defer client.AssertExpectations(t)

		client.On("GetDDLView", mock.Anything, mock.Anything).Return("select * from project.schema.table", nil)
		me, _ := extractor.NewMCExtractor(client, log.NewNoop())
		urnToDDL, err := me.Extract(context.Background(), []string{"project.schema.table_view"})
		assert.NoError(t, err)
		assert.Equal(t, map[string]string{"project.schema.table_view": "select * from project.schema.table"}, urnToDDL)
	})
}

// ViewGetter is an autogenerated mock type for the ViewGetter type
type ViewGetter struct {
	mock.Mock
}

// GetDDLView provides a mock function with given fields: ctx, table
func (_m *ViewGetter) GetDDLView(ctx context.Context, table string) (string, error) {
	ret := _m.Called(ctx, table)

	if len(ret) == 0 {
		panic("no return value specified for GetDDLView")
	}

	var r0 string
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context, string) (string, error)); ok {
		return rf(ctx, table)
	}
	if rf, ok := ret.Get(0).(func(context.Context, string) string); ok {
		r0 = rf(ctx, table)
	} else {
		r0 = ret.Get(0).(string)
	}

	if rf, ok := ret.Get(1).(func(context.Context, string) error); ok {
		r1 = rf(ctx, table)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// NewViewGetter creates a new instance of ViewGetter. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewViewGetter(t interface {
	mock.TestingT
	Cleanup(func())
},
) *ViewGetter {
	mock := &ViewGetter{}
	mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
