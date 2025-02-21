package maxcompute

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/aliyun/aliyun-odps-go-sdk/odps/datatype"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/tableschema"
	"github.com/mitchellh/mapstructure"

	"github.com/goto/optimus/core/resource"
	"github.com/goto/optimus/internal/errors"
)

const (
	resourceSchema = "maxcompute_schema"

	KindTable         string = "table"
	KindView          string = "view"
	KindSchema        string = "schema"
	KindExternalTable string = "external_table"

	allowedColumnMaskPolicyPattern = `^[a-zA-Z0-9_-]+$`
)

type Schema []*Field

func (s Schema) Validate() error {
	for _, f := range s {
		err := f.Validate()
		if err != nil {
			return err
		}
	}
	return nil
}

func (s Schema) ToMaxComputeColumns(partitionColumn map[string]struct{}, clusterColumn *Cluster, schemaBuilder *tableschema.SchemaBuilder) error {
	mu := errors.NewMultiError("converting to max compute column")

	clusterColumnAllowed := map[string]struct{}{}
	for _, f := range s {
		column, err := f.ToColumn()
		if err != nil {
			mu.Append(err)
			continue
		}

		if _, ok := partitionColumn[f.Name]; ok {
			schemaBuilder.PartitionColumn(column)
		} else {
			schemaBuilder.Column(column)
			clusterColumnAllowed[column.Name] = struct{}{}
		}
	}

	if clusterColumn != nil && len(clusterColumn.Using) != 0 {
		if clusterColumn.Type == "" {
			clusterColumn.Type = tableschema.CLUSTER_TYPE.Hash
		}
		schemaBuilder.ClusterType(clusterColumn.Type)

		if clusterColumn.Type == tableschema.CLUSTER_TYPE.Hash {
			if clusterColumn.Buckets == 0 {
				mu.Append(errors.InvalidArgument(resourceSchema, "number of cluster buckets is needed for hash type clustering"))
				return mu.ToErr()
			}
			schemaBuilder.ClusterBucketNum(clusterColumn.Buckets)
		}

		sortClusterAllowed := map[string]struct{}{}
		for _, column := range clusterColumn.Using {
			if _, ok := clusterColumnAllowed[column]; !ok {
				mu.Append(errors.InvalidArgument(resourceSchema, fmt.Sprintf("cluster column %s not found in normal column", column)))
				return mu.ToErr()
			}
			sortClusterAllowed[column] = struct{}{}
		}
		schemaBuilder.ClusterColumns(clusterColumn.Using)

		if len(clusterColumn.SortBy) != 0 {
			var sortClusterColumn []tableschema.SortColumn
			for _, sortColumn := range clusterColumn.SortBy {
				if _, ok := sortClusterAllowed[sortColumn.Name]; !ok {
					mu.Append(errors.InvalidArgument(resourceSchema, fmt.Sprintf("sort column %s not found in cluster column", sortColumn.Name)))
					return mu.ToErr()
				}
				sortClusterColumn = append(sortClusterColumn, tableschema.SortColumn{Name: sortColumn.Name, Order: tableschema.SortOrder(sortColumn.Order)})
			}
			schemaBuilder.ClusterSortColumns(sortClusterColumn)
		}
	}

	return mu.ToErr()
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

	SourceTimeFormat string `mapstructure:"source_time_format,omitempty"`

	Decimal      *Decimal   `mapstructure:"decimal,omitempty"`
	Char         *Char      `mapstructure:"char,omitempty"`
	VarChar      *VarChar   `mapstructure:"varchar,omitempty"`
	StructSchema []Field    `mapstructure:"struct,omitempty"`
	ArraySchema  *Field     `mapstructure:"array,omitempty"`
	MapSchema    *MapSchema `mapstructure:"map,omitempty"`

	// masking policy fields
	MaskPolicy   string `mapstructure:"mask_policy,omitempty"`
	UnmaskPolicy string `mapstructure:"unmask_policy,omitempty"`
}

func (f *Field) Validate() error {
	return f.validateNode(true)
}

func (f *Field) toMaxDataType() (datatype.DataType, error) {
	typeCode := datatype.TypeCodeFromStr(strings.ToUpper(f.Type))

	switch typeCode {
	case datatype.ARRAY:
		d2, err := f.ArraySchema.toMaxDataType()
		if err != nil {
			return nil, err
		}
		return datatype.NewArrayType(d2), nil

	case datatype.STRUCT:
		fields := make([]datatype.StructFieldType, len(f.StructSchema))
		for i, f1 := range f.StructSchema {
			d1, err := f1.toMaxDataType()
			if err != nil {
				return nil, err
			}
			fields[i] = datatype.StructFieldType{
				Name: f1.Name,
				Type: d1,
			}
		}
		return datatype.NewStructType(fields...), nil

	case datatype.CHAR:
		return datatype.NewCharType(f.Char.Length), nil

	case datatype.VARCHAR:
		return datatype.NewVarcharType(f.VarChar.Length), nil

	case datatype.DECIMAL:
		return datatype.NewDecimalType(f.Decimal.Precision, f.Decimal.Scale), nil

	case datatype.JSON:
		return datatype.NewJsonType(), nil

	case datatype.MAP:
		keyType, err := f.MapSchema.Key.toMaxDataType()
		if err != nil {
			return nil, err
		}
		valueType, err := f.MapSchema.Value.toMaxDataType()
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
	dataType, err := f.toMaxDataType()
	if err != nil {
		return tableschema.Column{}, err
	}

	c1 := tableschema.Column{
		Name:           f.Name,
		Type:           dataType,
		Comment:        f.Description,
		ExtendedLabels: nil,
		IsNullable:     true,
	}

	if f.Required {
		c1.IsNullable = false
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

	typeCode := datatype.TypeCodeFromStr(strings.ToUpper(f.Type))
	if typeCode == datatype.TypeUnknown {
		mu.Append(errors.InvalidArgument(resourceSchema, "unknown field type for "+f.Name))
	}

	mu.Append(f.validateColumnMaskingPolicy())

	switch typeCode {
	case datatype.TypeUnknown:
		mu.Append(errors.InvalidArgument(resourceSchema, "unknown data type: "+f.Type))

	case datatype.DECIMAL:
		if f.Decimal == nil {
			mu.Append(errors.InvalidArgument(resourceSchema, "field decimal is empty"))
		} else {
			mu.Append(f.Decimal.Validate())
		}

	case datatype.CHAR:
		if f.Char == nil {
			mu.Append(errors.InvalidArgument(resourceSchema, "field char is empty"))
		} else {
			mu.Append(f.Char.Validate())
		}

	case datatype.VARCHAR:
		if f.VarChar == nil {
			mu.Append(errors.InvalidArgument(resourceSchema, "field varchar is empty"))
		} else {
			mu.Append(f.VarChar.Validate())
		}

	case datatype.STRUCT:
		if f.StructSchema == nil {
			mu.Append(errors.InvalidArgument(resourceSchema, "struct schema is empty"))
		} else {
			for _, rField := range f.StructSchema {
				mu.Append(rField.validateNode(true))
			}
		}

	case datatype.ARRAY:
		if f.ArraySchema == nil {
			mu.Append(errors.InvalidArgument(resourceSchema, "array schema is empty"))
		} else {
			mu.Append(f.ArraySchema.validateNode(false))
		}

	case datatype.MAP:
		if f.MapSchema == nil {
			mu.Append(errors.InvalidArgument(resourceSchema, "map schema is empty"))
		} else {
			mu.Append(f.MapSchema.Validate())
		}
	default:
		// other data types do not require special properties
	}

	return mu.ToErr()
}

func (f *Field) validateColumnMaskingPolicy() error {
	mu := errors.NewMultiError("mask policy validation")
	if f.MaskPolicy != "" {
		if matched, _ := regexp.MatchString(allowedColumnMaskPolicyPattern, f.MaskPolicy); !matched {
			mu.Append(errors.InvalidArgument(resourceSchema, "mask policy contains invalid characters"))
		}
	}

	if f.UnmaskPolicy != "" {
		if matched, _ := regexp.MatchString(allowedColumnMaskPolicyPattern, f.UnmaskPolicy); !matched {
			mu.Append(errors.InvalidArgument(resourceSchema, "unmask policy contains invalid characters"))
		}
	}

	return mu.ToErr()
}

func ConvertSpecTo[T any](res *resource.Resource) (*T, error) {
	var spec T
	if err := mapstructure.Decode(res.Spec(), &spec); err != nil {
		msg := fmt.Sprintf("%s: not able to decode spec for %s", err, res.FullName())
		return nil, errors.InvalidArgument(resource.EntityResource, msg)
	}
	return &spec, nil
}
