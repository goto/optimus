package maxcompute

import (
	"fmt"

	"github.com/aliyun/aliyun-odps-go-sdk/odps/datatype"

	"github.com/goto/optimus/internal/errors"
)

const (
	maxCharLength       = 255
	minDecimalPrecision = 1
	maxDecimalPrecision = 38
	minDecimalScale     = 0
	maxDecimalScale     = 18
	maxVarcharLength    = 65535
)

type Decimal struct {
	Precision int32 `mapstructure:"precision"`
	Scale     int32 `mapstructure:"scale"`
}

func (d Decimal) Validate() error {
	if d.Scale > maxDecimalScale || d.Scale < minDecimalScale {
		return errors.InvalidArgument(resourceSchema, fmt.Sprintf("decimal scale[%d] is not valid", d.Scale))
	}
	if d.Precision < 1 || d.Precision > maxDecimalPrecision {
		return errors.InvalidArgument(resourceSchema, fmt.Sprintf("decimal precision[%d] is not valid", d.Precision))
	}
	return nil
}

type Char struct {
	Length int `mapstructure:"length"`
}

func (c Char) Validate() error {
	if c.Length > maxCharLength {
		return errors.InvalidArgument(resourceSchema, fmt.Sprintf("char length[%d] is not valid", c.Length))
	}
	return nil
}

type VarChar struct {
	Length int `mapstructure:"length"`
}

func (v VarChar) Validate() error {
	if v.Length > maxVarcharLength || v.Length < 1 {
		return errors.InvalidArgument(resourceSchema, fmt.Sprintf("varchar length[%d] is not valid", v.Length))
	}
	return nil
}

func isStruct(dataType datatype.DataType) bool {
	_, ok := dataType.(datatype.StructType)
	return ok
}

func isArrayStruct(dataType datatype.DataType) bool {
	cast, ok := dataType.(datatype.ArrayType)
	if !ok {
		return false
	}

	return isStruct(cast.ElementType)
}
