package maxcompute

import (
	"strings"

	"github.com/aliyun/aliyun-odps-go-sdk/odps/datatype"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/tableschema"

	"github.com/goto/optimus/internal/errors"
)

const (
	resourceSchema = "maxcompute_schema"
)

type Schema []Field

func (s Schema) Validate() error {
	for _, f := range s {
		err := f.Validate()
		if err != nil {
			return err
		}
	}
	return nil
}

type MapSchema struct {
	Key   Field `mapstructure:"key"`
	Value Field `mapstructure:"value"`
}

func (m *MapSchema) Validate() error {
	mu := errors.NewMultiError("map schema validation")
	mu.Append(m.Key.validateNode(false))
	mu.Append(m.Value.validateNode(false))
	return mu.ToErr()
}

type Field struct {
	Name        string `mapstructure:"name,omitempty"`
	Type        string `mapstructure:"type,omitempty"`
	Description string `mapstructure:"description,omitempty"`

	// First label should be the primary label and others as extended
	Labels []string `mapstructure:"labels,omitempty"`

	DefaultValue string `mapstructure:"default_value,omitempty"`
	Required     bool   `mapstructure:"required,omitempty"`

	Decimal      *Decimal   `mapstructure:"decimal,omitempty"`
	Char         *Char      `mapstructure:"char,omitempty"`
	VarChar      *VarChar   `mapstructure:"varchar,omitempty"`
	StructSchema []Field    `mapstructure:"struct,omitempty"`
	ArraySchema  *Field     `mapstructure:"array,omitempty"`
	MapSchema    *MapSchema `mapstructure:"map,omitempty"`
}

func (f *Field) Validate() error {
	return f.validateNode(true)
}

func (f *Field) ToMaxDataType() (datatype.DataType, error) {
	typeCode := datatype.TypeCodeFromStr(strings.ToUpper(f.Type))

	switch typeCode {
	case datatype.ARRAY:
		d2, err := f.ArraySchema.ToMaxDataType()
		if err != nil {
			return nil, err
		}
		return datatype.NewArrayType(d2), nil

	case datatype.STRUCT:
		fields := make([]datatype.DataType, len(f.StructSchema))
		for i, f1 := range f.StructSchema {
			field, err := f1.ToMaxDataType()
			if err != nil {
				return nil, err
			}
			fields[i] = field
		}
		return datatype.NewStructType(), nil

	case datatype.CHAR:
		return datatype.NewCharType(f.Char.Length), nil

	case datatype.VARCHAR:
		return datatype.NewVarcharType(f.VarChar.Length), nil

	case datatype.DECIMAL:
		return datatype.NewDecimalType(f.Decimal.Precision, f.Decimal.Scale), nil

	case datatype.JSON:
		return datatype.NewJsonType(), nil

	case datatype.MAP:
		keyType, err := f.MapSchema.Key.ToMaxDataType()
		if err != nil {
			return nil, err
		}
		valueType, err := f.MapSchema.Value.ToMaxDataType()
		if err != nil {
			return nil, err
		}
		return datatype.NewMapType(keyType, valueType), nil

	case datatype.TypeUnknown:
		return nil, errors.InvalidArgument(resourceSchema, "unknown data type: "+f.Type)
	default:
		return datatype.NewPrimitiveType(typeCode), nil
	}
}

func (f *Field) ToColumn() (tableschema.Column, error) {
	dataType, err := f.ToMaxDataType()
	if err != nil {
		return tableschema.Column{}, err
	}

	c1 := tableschema.Column{
		Name:           f.Name,
		Type:           dataType,
		Comment:        f.Description,
		ExtendedLabels: nil,
		IsNullable:     false,
	}

	if f.Required {
		c1.IsNullable = true
	}

	if f.DefaultValue != "" {
		c1.HasDefaultValue = true
		c1.DefaultValue = f.DefaultValue
	}

	if len(f.Labels) > 0 {
		c1.Label = f.Labels[0]
		c1.ExtendedLabels = f.Labels[1:]
	}

	return c1, nil
}

func (f *Field) validateNode(checkName bool) error {
	mu := errors.NewMultiError("field validation")
	if checkName && strings.TrimSpace(f.Name) == "" {
		mu.Append(errors.InvalidArgument(resourceSchema, "field name is empty"))
	}
	if strings.TrimSpace(f.Type) == "" {
		mu.Append(errors.InvalidArgument(resourceSchema, "field type is empty for "+f.Name))
	}

	if f.Decimal != nil {
		mu.Append(f.Decimal.Validate())
	}

	if f.Char != nil {
		mu.Append(f.Char.Validate())
	}

	if f.VarChar != nil {
		mu.Append(f.VarChar.Validate())
	}

	if len(f.StructSchema) != 0 {
		for _, rField := range f.StructSchema {
			mu.Append(rField.validateNode(true))
		}
	}

	if f.ArraySchema != nil {
		mu.Append(f.ArraySchema.validateNode(false))
	}

	if f.MapSchema != nil {
		mu.Append(f.MapSchema.Validate())
	}

	return mu.ToErr()
}
