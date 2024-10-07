package maxcompute

import (
	"fmt"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/datatype"

	"github.com/goto/optimus/internal/errors"
)

type Decimal struct {
	Precision int32 `mapstructure:"precision"`
	Scale     int32 `mapstructure:"scale"`
}

func (d Decimal) Validate() error {
	if d.Scale > 18 || d.Scale < 0 {
		return errors.InvalidArgument(resourceSchema, fmt.Sprintf("decimal scale[%d] is not valid", d.Scale))
	}
	if d.Precision < 1 || d.Precision > 38 {
		return errors.InvalidArgument(resourceSchema, fmt.Sprintf("decimal precision[%d] is not valid", d.Precision))
	}
	return nil
}

type Char struct {
	Length int `mapstructure:"length"`
}

func (c Char) Validate() error {
	if c.Length > 255 {
		return errors.InvalidArgument(resourceSchema, fmt.Sprintf("char length[%d] is not valid", c.Length))
	}
	return nil
}

type VarChar struct {
	Length int `mapstructure:"length"`
}

func (v VarChar) Validate() error {
	if v.Length > 65535 || v.Length < 1 {
		return errors.InvalidArgument(resourceSchema, fmt.Sprintf("varchar length[%d] is not valid", v.Length))
	}
	return nil
}

func isStruct(dataType datatype.DataType) bool {
	_, ok := dataType.(datatype.StructType)
	if !ok {
		return false
	}

	return true
}

func isArrayStruct(dataType datatype.DataType) bool {
	cast, ok := dataType.(datatype.ArrayType)
	if !ok {
		return false
	}

	return isStruct(cast.ElementType)
}
