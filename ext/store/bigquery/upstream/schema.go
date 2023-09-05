package upstream

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"cloud.google.com/go/bigquery"
	bq "github.com/goto/optimus/ext/store/bigquery"
	"google.golang.org/api/iterator"
)

const wildCardSuffix = "*"

type SchemaType string

const (
	Unknown SchemaType = "UNKNOWN"

	BaseTable        SchemaType = "BASE TABLE"
	External         SchemaType = "EXTERNAL"
	MaterializedView SchemaType = "MATERIALIZED VIEW"
	Snapshot         SchemaType = "SNAPSHOT"
	View             SchemaType = "VIEW"
)

type Schema struct {
	Resource Resource
	Type     SchemaType
	DDL      string
}

func ReadSchemasUnderGroup(ctx context.Context, client bq.Client, group *ResourceGroup) ([]*Schema, error) {
	queryContent := buildQuery(group)

	queryStatement := client.Query(queryContent)

	rowIterator, err := queryStatement.Read(ctx)
	if err != nil {
		return nil, err
	}

	var schemas []*Schema
	var errorMessages []string

	for {
		var values []bigquery.Value
		if err := rowIterator.Next(&values); err != nil {
			if errors.Is(err, iterator.Done) {
				break
			}

			errorMessages = append(errorMessages, err.Error())
			continue
		}

		if len(values) == 0 {
			continue
		}

		sch, err := convertToSchema(values)
		if err != nil {
			errorMessages = append(errorMessages, err.Error())
			continue
		}

		schemas = append(schemas, sch)
	}

	if len(errorMessages) > 0 {
		err = fmt.Errorf("error encountered when reading reading schema: [%s]", strings.Join(errorMessages, ", "))
	}

	return schemas, err
}

func buildQuery(group *ResourceGroup) string {
	var nameQueries, prefixQueries []string
	for _, n := range group.Names {
		if strings.HasSuffix(n, wildCardSuffix) {
			prefix, _ := strings.CutSuffix(n, wildCardSuffix)
			prefixQuery := fmt.Sprintf("STARTS_WITH(table_name, '%s')", prefix)
			prefixQueries = append(prefixQueries, prefixQuery)
		} else {
			nameQuery := fmt.Sprintf("'%s'", n)
			nameQueries = append(nameQueries, nameQuery)
		}
	}

	names := strings.Join(nameQueries, ", ")
	prefixes := strings.Join(prefixQueries, " or\n")

	var whereClause string
	if len(nameQueries) > 0 && len(prefixQueries) > 0 {
		whereClause = fmt.Sprintf("WHERE table_name in (%s) or %s", names, prefixes)
	} else if len(nameQueries) > 0 {
		whereClause = fmt.Sprintf("WHERE table_name in (%s)", names)
	} else if len(prefixQueries) > 0 {
		whereClause = fmt.Sprintf("WHERE %s", prefixes)
	}

	return "SELECT table_catalog, table_schema, table_name, table_type, ddl\n" +
		fmt.Sprintf("FROM `%s.%s.INFORMATION_SCHEMA.TABLES`\n", group.Project, group.Dataset) +
		whereClause
}

func convertToSchema(values []bigquery.Value) (*Schema, error) {
	const expectedSchemaRowLen = 5

	if l := len(values); l != expectedSchemaRowLen {
		return nil, fmt.Errorf("unexpected number of row length: %d", l)
	}

	project, ok := values[0].(string)
	if !ok {
		return nil, errors.New("error casting project")
	}

	dataset, ok := values[1].(string)
	if !ok {
		return nil, errors.New("error casting dataset")
	}

	name, ok := values[2].(string)
	if !ok {
		return nil, errors.New("error casting name")
	}

	_type, ok := values[3].(string)
	if !ok {
		return nil, errors.New("error casting _type")
	}

	ddl, ok := values[4].(string)
	if !ok {
		return nil, errors.New("error casting ddl")
	}

	resource := Resource{
		Project: project,
		Dataset: dataset,
		Name:    name,
	}

	var schemaType SchemaType
	switch _type {
	case string(BaseTable):
		schemaType = BaseTable
	case string(External):
		schemaType = External
	case string(MaterializedView):
		schemaType = MaterializedView
	case string(Snapshot):
		schemaType = Snapshot
	case string(View):
		schemaType = View
	default:
		schemaType = Unknown
	}

	return &Schema{
		Resource: resource,
		Type:     schemaType,
		DDL:      ddl,
	}, nil
}

type Schemas []*Schema

func (s Schemas) SplitSchemasByType(target SchemaType) (Schemas, Schemas) {
	result := []*Schema{}
	rest := []*Schema{}
	for _, sch := range s {
		if sch.Type == target {
			result = append(result, sch)
		} else {
			rest = append(rest, sch)
		}
	}
	return result, rest
}

func (s Schemas) ToResources() []*Resource {
	output := make([]*Resource, len(s))
	for i, sch := range s {
		output[i] = &sch.Resource
	}
	return output
}
