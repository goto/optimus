package maxcompute_test

import (
	"fmt"
	"testing"

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
	emptyPartitionColumnName := map[string]struct{}{}
	t.Run("return error when schema column type is unknown", func(t *testing.T) {
		schema := maxcompute.Schema{
			{
				Name: "name",
				Type: "unknown",
			},
		}

		err := schema.ToMaxComputeColumns(emptyPartitionColumnName, nil, nil, "common")
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

		err := schema.ToMaxComputeColumns(emptyPartitionColumnName, nil, nil, "common")
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

		err := schema.ToMaxComputeColumns(emptyPartitionColumnName, nil, nil, "common")
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

		err := schema.ToMaxComputeColumns(emptyPartitionColumnName, nil, nil, "common")
		assert.NotNil(t, err)
		assert.ErrorContains(t, err, "unknown data type")
	})
	t.Run("return error when cluster column type is hash but not specify bucket", func(t *testing.T) {
		builder := tableschema.NewSchemaBuilder()
		schema := maxcompute.Schema{
			{
				Name:         "name",
				Required:     true,
				DefaultValue: "test",
				Type:         "char",
				Char:         &maxcompute.Char{Length: 255},
				Labels:       []string{"owner", "member"},
			},
		}

		clusterColumns := &maxcompute.Cluster{
			Using:  []string{"name", "age"},
			SortBy: []maxcompute.SortColumn{{Name: "name", Order: "asc"}},
		}

		err := schema.ToMaxComputeColumns(emptyPartitionColumnName, clusterColumns, builder, "common")
		assert.NotNil(t, err)
		assert.ErrorContains(t, err, "number of cluster buckets is needed for hash type clustering")
	})
	t.Run("return error when cluster column is not found in normal column", func(t *testing.T) {
		builder := tableschema.NewSchemaBuilder()
		schema := maxcompute.Schema{
			{
				Name:         "name",
				Required:     true,
				DefaultValue: "test",
				Type:         "char",
				Char:         &maxcompute.Char{Length: 255},
				Labels:       []string{"owner", "member"},
			},
		}

		invalidClusterColumn := "age"
		clusterColumns := &maxcompute.Cluster{
			Using:   []string{invalidClusterColumn},
			Buckets: 5,
		}

		err := schema.ToMaxComputeColumns(emptyPartitionColumnName, clusterColumns, builder, "common")
		assert.NotNil(t, err)
		assert.ErrorContains(t, err, fmt.Sprintf("cluster column %s not found in normal column", invalidClusterColumn))
	})
	t.Run("return error when sort column is not found in cluster column", func(t *testing.T) {
		builder := tableschema.NewSchemaBuilder()
		schema := maxcompute.Schema{
			{
				Name:         "name",
				Required:     true,
				DefaultValue: "test",
				Type:         "char",
				Char:         &maxcompute.Char{Length: 255},
				Labels:       []string{"owner", "member"},
			},
		}

		invalidSortClusterColumn := "age"
		clusterColumns := &maxcompute.Cluster{
			Using:   []string{"name"},
			SortBy:  []maxcompute.SortColumn{{Name: invalidSortClusterColumn, Order: "asc"}},
			Buckets: 5,
		}

		err := schema.ToMaxComputeColumns(emptyPartitionColumnName, clusterColumns, builder, "common")
		assert.NotNil(t, err)
		assert.ErrorContains(t, err, fmt.Sprintf("sort column %s not found in cluster column", invalidSortClusterColumn))
	})
	t.Run("return success when schema column is valid for common table", func(t *testing.T) {
		builder := tableschema.NewSchemaBuilder()
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
		partitionColumnName := map[string]struct{}{
			"data": {},
		}

		clusterColumns := &maxcompute.Cluster{
			Using:   []string{"name", "age"},
			SortBy:  []maxcompute.SortColumn{{Name: "name", Order: "asc"}},
			Buckets: 5,
		}

		err := schema.ToMaxComputeColumns(partitionColumnName, clusterColumns, builder, "common")
		assert.Nil(t, err)
	})
	t.Run("return success when schema column is valid for delta table", func(t *testing.T) {
		builder := tableschema.NewSchemaBuilder()
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
		partitionColumnName := map[string]struct{}{
			"data": {},
		}

		clusterColumns := &maxcompute.Cluster{
			Using:   []string{"name", "age"},
			SortBy:  []maxcompute.SortColumn{{Name: "name", Order: "asc"}},
			Buckets: 5,
		}

		err := schema.ToMaxComputeColumns(partitionColumnName, clusterColumns, builder, "delta")
		assert.Nil(t, err)
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
		t.Run("returns error when mask policy has invalid characters", func(t *testing.T) {
			f := maxcompute.Field{
				Name:       "name",
				Type:       "string",
				MaskPolicy: "mask_policy@@",
			}

			err := f.Validate()
			assert.NotNil(t, err)
			assert.ErrorContains(t, err, "mask policy contains invalid characters")
		})
		t.Run("returns error when unmask policy has invalid characters", func(t *testing.T) {
			f := maxcompute.Field{
				Name:         "name",
				Type:         "string",
				MaskPolicy:   "mask_policy",
				UnmaskPolicy: "unmask_policy@@",
			}

			err := f.Validate()
			assert.NotNil(t, err)
			assert.ErrorContains(t, err, "unmask policy contains invalid characters")
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
