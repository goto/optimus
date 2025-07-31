package maxcompute_test

import (
	"errors"
	"testing"

	"github.com/aliyun/aliyun-odps-go-sdk/odps"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/account"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/goto/optimus/ext/store/maxcompute"
)

type mockMaxComputeFunction struct {
	mock.Mock
}

func (_m *mockMaxComputeFunction) Get(functionName string) (*odps.Function, error) {
	ret := _m.Called(functionName)

	ret0 := ret.Get(0)
	if ret0 == nil {
		return nil, ret.Error(1)
	}

	odpsFn, ok := ret0.(*odps.Function)
	if !ok {
		return nil, ret.Error(1)
	}

	return odpsFn, ret.Error(1)
}

func NewMockMaxComputeFunction(t interface {
	mock.TestingT
	Cleanup(func())
},
) *mockMaxComputeFunction {
	mock := &mockMaxComputeFunction{}
	mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}

func TestFunctionHandle(t *testing.T) {
	accessID, accessKey, endpoint := "LNRJ5tH1XMSINW5J3TjYAvfX", "lAZBJhdkNbwVj3bej5BuhjwbdV0nSp", "http://service.ap-southeast-5.maxcompute.aliyun.com/api"
	aliAccount := account.NewAliyunAccount(accessID, accessKey)
	odpsIns := odps.NewOdps(aliAccount, endpoint)
	functionName := "test_function"
	dummyOdpsFunction := &odps.Function{
		OdpsIns: odpsIns,
	}

	t.Run("Create", func(t *testing.T) {
		mcFunction := NewMockMaxComputeFunction(t)
		defer mcFunction.AssertExpectations(t)
		fh := maxcompute.NewFunctionHandle(mcFunction)

		err := fh.Create(nil)
		assert.Error(t, err)
		assert.ErrorContains(t, err, "create is not supported")
	})

	t.Run("Update", func(t *testing.T) {
		mcFunction := NewMockMaxComputeFunction(t)
		defer mcFunction.AssertExpectations(t)
		fh := maxcompute.NewFunctionHandle(mcFunction)

		err := fh.Update(nil)
		assert.Error(t, err)
		assert.ErrorContains(t, err, "update is not supported")
	})

	t.Run("Exists", func(t *testing.T) {
		t.Run("return false when function does not exist", func(t *testing.T) {
			mcFunction := NewMockMaxComputeFunction(t)
			defer mcFunction.AssertExpectations(t)
			fh := maxcompute.NewFunctionHandle(mcFunction)

			mcFunction.On("Get", functionName).Return(dummyOdpsFunction, nil)

			actual := fh.Exists(functionName)
			assert.False(t, actual)
		})

		t.Run("return false when function name is not set", func(t *testing.T) {
			mcFunction := NewMockMaxComputeFunction(t)
			defer mcFunction.AssertExpectations(t)
			fh := maxcompute.NewFunctionHandle(mcFunction)

			mcFunction.On("Get", "").
				Return(nil, errors.New("function name is not set"))

			actual := fh.Exists("")
			assert.False(t, actual)
		})
	})
}
