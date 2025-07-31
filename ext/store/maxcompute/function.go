package maxcompute

import (
	"github.com/aliyun/aliyun-odps-go-sdk/odps"

	"github.com/goto/optimus/core/resource"
	"github.com/goto/optimus/internal/errors"
)

type MCFunction interface {
	Get(functionName string) (*odps.Function, error)
}

type FunctionHandle struct {
	mcFunction MCFunction
}

func (*FunctionHandle) Create(_ *resource.Resource) error {
	return errors.FailedPrecondition(EntityFunction, "create is not supported")
}

func (*FunctionHandle) Update(_ *resource.Resource) error {
	return errors.FailedPrecondition(EntityFunction, "update is not supported")
}

func (fh *FunctionHandle) Exists(tableName string) bool {
	function, err := fh.mcFunction.Get(tableName)
	if err != nil {
		// this case will return when function name is not set
		return false
	}
	exists, _ := function.Exist()
	return exists
}

func NewFunctionHandle(mcFunction MCFunction) *FunctionHandle {
	return &FunctionHandle{
		mcFunction: mcFunction,
	}
}
