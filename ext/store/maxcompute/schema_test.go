package maxcompute_test

import (
	"testing"

	"github.com/aliyun/aliyun-odps-go-sdk/odps/datatype"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/tableschema"

	"github.com/stretchr/testify/assert"

	"github.com/goto/optimus/ext/store/maxcompute"
)

func TestSchemaValidate(t *testing.T) {
	t.Run("returns error when schema field type is unknown", func(t *testing.T) {
		schema := maxcompute.Schema{
			{
				Name: "name",
				Type: "unknown",
			},
		}

		err := schema.Validate()
		assert.NotNil(t, err)
		assert.ErrorContains(t, err, "unknown field type")
	})
	t.Run("return success when schema is valid", func(t *testing.T) {
		schema := maxcompute.Schema{
			{
				Name: "name",
				Type: "string",
			},
			{
				Name: "age",
				Type: "int",
			},
		}

		err := schema.Validate()
		assert.Nil(t, err)
	})
}

func TestSchemaToMaxComputeColumn(t *testing.T) {
	t.Run("return error when schema column type is unknown", func(t *testing.T) {
		schema := maxcompute.Schema{
			{
				Name: "name",
				Type: "unknown",
			},
		}

		_, err := schema.ToMaxComputeColumns()
		assert.NotNil(t, err)
		assert.ErrorContains(t, err, "unknown data type")
	})
	t.Run("return error when schema column array type is invalid", func(t *testing.T) {
		schema := maxcompute.Schema{
			{
				Name: "name",
				Type: "array",
				ArraySchema: &maxcompute.Field{
					Type: "unknown",
				},
			},
		}

		_, err := schema.ToMaxComputeColumns()
		assert.NotNil(t, err)
		assert.ErrorContains(t, err, "unknown data type")
	})
	t.Run("return error when schema column struct type is invalid", func(t *testing.T) {
		schema := maxcompute.Schema{
			{
				Name: "name",
				Type: "struct",
				StructSchema: []maxcompute.Field{
					{
						Name: "test",
						Type: "unknown",
					},
				},
			},
		}

		_, err := schema.ToMaxComputeColumns()
		assert.NotNil(t, err)
		assert.ErrorContains(t, err, "unknown data type")
	})
	t.Run("return error when schema column map type is invalid", func(t *testing.T) {
		schema := maxcompute.Schema{
			{
				Name: "name",
				Type: "map",
				MapSchema: &maxcompute.MapSchema{
					Key: maxcompute.Field{
						Name: "test_key",
						Type: "string",
					},
					Value: maxcompute.Field{
						Name: "test_key",
						Type: "unknown",
					},
				},
			},
			{
				Name: "other",
				Type: "map",
				MapSchema: &maxcompute.MapSchema{
					Key: maxcompute.Field{
						Name: "test_key",
						Type: "unknown",
					},
					Value: maxcompute.Field{
						Name: "test_key",
						Type: "string",
					},
				},
			},
		}

		_, err := schema.ToMaxComputeColumns()
		assert.NotNil(t, err)
		assert.ErrorContains(t, err, "unknown data type")
	})
	t.Run("return success when schema column is valid", func(t *testing.T) {
		schema := maxcompute.Schema{
			{
				Name:         "name",
				Required:     true,
				DefaultValue: "test",
				Type:         "char",
				Char:         &maxcompute.Char{Length: 255},
				Labels:       []string{"owner", "member"},
			},
			{
				Name:    "introduction",
				Type:    "varchar",
				VarChar: &maxcompute.VarChar{Length: 300},
			},
			{
				Name: "age",
				Type: "int",
			},
			{
				Name:    "weight",
				Type:    "decimal",
				Decimal: &maxcompute.Decimal{Precision: 2, Scale: 1},
			},
			{
				Name: "friends",
				Type: "array",
				ArraySchema: &maxcompute.Field{
					Type: "string",
				},
			},
			{
				Name: "address",
				Type: "struct",
				StructSchema: []maxcompute.Field{
					{
						Name: "city",
						Type: "string",
					},
					{
						Name: "zip",
						Type: "string",
					},
				},
			},
			{
				Name: "other",
				Type: "map",
				MapSchema: &maxcompute.MapSchema{
					Key: maxcompute.Field{
						Type: "string",
					},
					Value: maxcompute.Field{
						Type: "string",
					},
				},
			},
			{
				Name: "data",
				Type: "json",
			},
		}
		expectedColumns := []tableschema.Column{
			{
				Name:            "name",
				DefaultValue:    "test",
				HasDefaultValue: true,
				Type:            datatype.CharType{Length: 255},
				Comment:         "",
				Label:           "owner",
				ExtendedLabels:  []string{"member"},
				IsNullable:      false,
			},
			{
				Name:           "introduction",
				Type:           datatype.VarcharType{Length: 300},
				Comment:        "",
				ExtendedLabels: nil,
				IsNullable:     true,
			},
			{
				Name:           "age",
				Type:           datatype.IntType,
				Comment:        "",
				ExtendedLabels: nil,
				IsNullable:     true,
			},
			{
				Name:           "weight",
				Type:           datatype.DecimalType{Precision: 2, Scale: 1},
				Comment:        "",
				ExtendedLabels: nil,
				IsNullable:     true,
			},
			{
				Name:           "friends",
				Type:           datatype.ArrayType{ElementType: datatype.StringType},
				Comment:        "",
				ExtendedLabels: nil,
				IsNullable:     true,
			},
			{
				Name: "address",
				Type: datatype.StructType{
					Fields: []datatype.StructFieldType{
						{
							Name: "city",
							Type: datatype.StringType,
						},
						{
							Name: "zip",
							Type: datatype.StringType,
						},
					},
				},
				Comment:        "",
				ExtendedLabels: nil,
				IsNullable:     true,
			},
			{
				Name:           "other",
				Type:           datatype.MapType{KeyType: datatype.StringType, ValueType: datatype.StringType},
				Comment:        "",
				ExtendedLabels: nil,
				IsNullable:     true,
			},
			{
				Name:           "data",
				Type:           datatype.JsonType{},
				Comment:        "",
				ExtendedLabels: nil,
				IsNullable:     true,
			},
		}

		columns, err := schema.ToMaxComputeColumns()
		assert.Nil(t, err)
		assert.Equal(t, expectedColumns, columns)
	})
}

func TestFieldValidate(t *testing.T) {
	t.Run("when invalid", func(t *testing.T) {
		t.Run("returns error when name is empty", func(t *testing.T) {
			f := maxcompute.Field{
				Name: "",
				Type: "string",
			}

			err := f.Validate()
			assert.NotNil(t, err)
			assert.ErrorContains(t, err, "field name is empty")
		})
		t.Run("returns error when type is unknown", func(t *testing.T) {
			f := maxcompute.Field{
				Name: "name",
				Type: "unknown",
			}

			err := f.Validate()
			assert.NotNil(t, err)
			assert.ErrorContains(t, err, "unknown field type")
		})
		t.Run("returns error when decimal type is empty", func(t *testing.T) {
			f := maxcompute.Field{
				Name:    "name",
				Type:    "decimal",
				Decimal: nil,
			}

			err := f.Validate()
			assert.NotNil(t, err)
			assert.ErrorContains(t, err, "field decimal is empty")
		})
		t.Run("returns error when decimal type is invalid", func(t *testing.T) {
			f := maxcompute.Field{
				Name:    "name",
				Type:    "decimal",
				Decimal: &maxcompute.Decimal{Precision: 10, Scale: 20},
			}

			err := f.Validate()
			assert.NotNil(t, err)
			assert.ErrorContains(t, err, "decimal scale[20] is not valid")
		})
		t.Run("returns error when char type is empty", func(t *testing.T) {
			f := maxcompute.Field{
				Name: "name",
				Type: "char",
				Char: nil,
			}

			err := f.Validate()
			assert.NotNil(t, err)
			assert.ErrorContains(t, err, "field char is empty")
		})
		t.Run("returns error when char type is invalid", func(t *testing.T) {
			f := maxcompute.Field{
				Name: "name",
				Type: "char",
				Char: &maxcompute.Char{Length: 300},
			}

			err := f.Validate()
			assert.NotNil(t, err)
			assert.ErrorContains(t, err, "char length[300] is not valid")
		})
		t.Run("returns error when varchar type is empty", func(t *testing.T) {
			f := maxcompute.Field{
				Name:    "name",
				Type:    "varchar",
				VarChar: nil,
			}

			err := f.Validate()
			assert.NotNil(t, err)
			assert.ErrorContains(t, err, "field varchar is empty")
		})
		t.Run("returns error when varchar type is invalid", func(t *testing.T) {
			f := maxcompute.Field{
				Name:    "name",
				Type:    "varchar",
				VarChar: &maxcompute.VarChar{Length: 65999},
			}

			err := f.Validate()
			assert.NotNil(t, err)
			assert.ErrorContains(t, err, "varchar length[65999] is not valid")
		})
		t.Run("returns error when struct type is empty", func(t *testing.T) {
			f := maxcompute.Field{
				Name:         "name",
				Type:         "struct",
				StructSchema: nil,
			}

			err := f.Validate()
			assert.NotNil(t, err)
			assert.ErrorContains(t, err, "struct schema is empty")
		})
		t.Run("returns error when struct type is invalid", func(t *testing.T) {
			f := maxcompute.Field{
				Name: "collection",
				Type: "struct",
				StructSchema: []maxcompute.Field{
					{
						Name: "store",
						Type: "string",
					},
					{
						Name: "product",
						Type: "unknown",
					},
				},
			}

			err := f.Validate()
			assert.NotNil(t, err)
			assert.ErrorContains(t, err, "unknown field type")
		})
		t.Run("returns error when array type is empty", func(t *testing.T) {
			f := maxcompute.Field{
				Name:        "name",
				Type:        "array",
				ArraySchema: nil,
			}

			err := f.Validate()
			assert.NotNil(t, err)
			assert.ErrorContains(t, err, "array schema is empty")
		})
		t.Run("returns error when array type is invalid", func(t *testing.T) {
			f := maxcompute.Field{
				Name: "names",
				Type: "array",
				ArraySchema: &maxcompute.Field{
					Type: "unknown",
				},
			}

			err := f.Validate()
			assert.NotNil(t, err)
			assert.ErrorContains(t, err, "unknown field type")
		})
		t.Run("returns error when map type is empty", func(t *testing.T) {
			f := maxcompute.Field{
				Name:      "name",
				Type:      "map",
				MapSchema: nil,
			}

			err := f.Validate()
			assert.NotNil(t, err)
			assert.ErrorContains(t, err, "map schema is empty")
		})
		t.Run("returns error when map type is invalid", func(t *testing.T) {
			f := maxcompute.Field{
				Name: "name",
				Type: "map",
				MapSchema: &maxcompute.MapSchema{
					Key: maxcompute.Field{
						Type: "string",
					},
					Value: maxcompute.Field{
						Type: "unknown",
					},
				},
			}

			err := f.Validate()
			assert.NotNil(t, err)
			assert.ErrorContains(t, err, "unknown field type")
		})
	})
	t.Run("return success when field schema is valid", func(t *testing.T) {
		f := maxcompute.Field{
			Name: "name",
			Type: "string",
		}

		err := f.Validate()
		assert.Nil(t, err)
	})
}
