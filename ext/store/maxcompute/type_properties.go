package maxcompute

import "github.com/goto/optimus/internal/errors"

type Decimal struct {
	Precision int32 `mapstructure:"precision"`
	Scale     int32 `mapstructure:"scale"`
}

func (d Decimal) Validate() error {
	if d.Scale > 18 || d.Scale < 0 {
		return errors.InvalidArgument(resourceSchema, "decimal scale is not valid")
	}
	if d.Precision < 1 || d.Precision > 38 {
		return errors.InvalidArgument(resourceSchema, "decimal precision is not valid")
	}
	return nil
}

type Char struct {
	Length int `mapstructure:"length"`
}

func (c Char) Validate() error {
	if c.Length > 255 {
		return errors.InvalidArgument(resourceSchema, "char length is not valid")
	}
	return nil
}

type VarChar struct {
	Length int `mapstructure:"length"`
}

func (v VarChar) Validate() error {
	if v.Length > 65535 || v.Length < 1 {
		return errors.InvalidArgument(resourceSchema, "varchar length is not valid")
	}
	return nil
}
