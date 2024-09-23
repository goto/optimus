package bigquery

import (
	"context"
	"fmt"
	"strings"

	"cloud.google.com/go/bigquery"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"

	"github.com/goto/optimus/internal/errors"
)

type BqClientProvider struct{}

func NewClientProvider() *BqClientProvider {
	return &BqClientProvider{}
}

func (BqClientProvider) Get(ctx context.Context, account string) (Client, error) {
	return NewClient(ctx, account)
}

type BqClient struct {
	*bigquery.Client
}

func NewClient(ctx context.Context, svcAccount string) (*BqClient, error) {
	cred, err := google.CredentialsFromJSON(ctx, []byte(svcAccount), bigquery.Scope)
	if err != nil {
		return nil, errors.InternalError(store, "failed to read account", err)
	}

	c, err := bigquery.NewClient(ctx, cred.ProjectID, option.WithCredentials(cred))
	if err != nil {
		return nil, errors.InternalError(store, "failed to create BQ client", err)
	}

	return &BqClient{c}, nil
}

func (c *BqClient) DatasetHandleFrom(ds Dataset) ResourceHandle {
	dsHandle := c.DatasetInProject(ds.Project, ds.DatasetName)
	return NewDatasetHandle(dsHandle)
}

func (c *BqClient) TableHandleFrom(ds Dataset, name string) TableResourceHandle {
	t := c.DatasetInProject(ds.Project, ds.DatasetName).Table(name)
	return NewTableHandle(t)
}

func (c *BqClient) RoutineHandleFrom(ds Dataset, name string) ResourceHandle {
	t := c.DatasetInProject(ds.Project, ds.DatasetName).Routine(name)
	return NewRoutineHandle(t)
}

func (c *BqClient) ExternalTableHandleFrom(ds Dataset, name string) ResourceHandle {
	t := c.DatasetInProject(ds.Project, ds.DatasetName).Table(name)
	return NewExternalTableHandle(t)
}

func (c *BqClient) ModelHandleFrom(ds Dataset, name string) ResourceHandle {
	t := c.DatasetInProject(ds.Project, ds.DatasetName).Model(name)
	return NewModelHandle(t)
}

func (c *BqClient) BulkGetDDLView(ctx context.Context, pd ProjectDataset, names []string) (map[ResourceURN]string, error) {
	me := errors.NewMultiError("bulk get ddl view errors")
	urnToDDL := make(map[ResourceURN]string, len(names))
	for _, name := range names {
		resourceURN, err := NewResourceURN(pd.Project, pd.Dataset, name)
		if err != nil {
			me.Append(err)
			continue
		}
		urnToDDL[resourceURN] = ""
	}

	queryContent := buildGetDDLQuery(pd.Project, pd.Dataset, names...)
	queryStatement := c.Client.Query(queryContent)
	rowIterator, err := queryStatement.Read(ctx)
	if err != nil {
		return urnToDDL, err
	}

	for {
		var values []bigquery.Value
		if err := rowIterator.Next(&values); err != nil {
			if errors.Is(err, iterator.Done) {
				break
			}

			me.Append(err)
			continue
		}

		if len(values) == 0 {
			continue
		}

		name, ddl, err := getViewDDL(values)
		if err != nil {
			me.Append(err)
			continue
		}

		resourceURN, err := NewResourceURN(pd.Project, pd.Dataset, name)
		if err != nil {
			me.Append(err)
			continue
		}
		urnToDDL[resourceURN] = ddl
	}

	return urnToDDL, me.ToErr()
}

func (c *BqClient) ViewHandleFrom(ds Dataset, name string) ResourceHandle {
	t := c.DatasetInProject(ds.Project, ds.DatasetName).Table(name)
	return NewViewHandle(t)
}

func buildGetDDLQuery(project, dataset string, tables ...string) string {
	var nameQueries, prefixQueries []string
	const wildCardSuffix = "*"
	for _, n := range tables {
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
		fmt.Sprintf("FROM `%s.%s.INFORMATION_SCHEMA.TABLES`\n", project, dataset) +
		whereClause
}

func getViewDDL(values []bigquery.Value) (string, string, error) {
	const expectedSchemaRowLen = 5
	const viewType = "VIEW"

	if l := len(values); l != expectedSchemaRowLen {
		return "", "", fmt.Errorf("unexpected number of row length: %d", l)
	}

	name, ok := values[2].(string)
	if !ok {
		return "", "", fmt.Errorf("error casting name")
	}

	_type, ok := values[3].(string)
	if !ok {
		return "", "", fmt.Errorf("error casting _type")
	}

	ddl, ok := values[4].(string)
	if !ok {
		return "", "", fmt.Errorf("error casting ddl")
	}

	if _type == viewType {
		return name, ddl, nil
	}
	return name, "", nil
}
