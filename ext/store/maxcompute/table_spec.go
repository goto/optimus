package maxcompute

import (
	"fmt"

	"github.com/goto/optimus/internal/errors"
)

const (
	EntityTable = "resource_table"
)

/*
Table defines the spec for max-compute table
We are trying to keep the spec similar to bigquery to make maintenance easier
Cluster: we accept name of column for clustering, check Cluster for more details
Partition: we accept the name of columns for partitioning and provide in config
ExtraConfig:

	hints: hints to be provided to the table for creation, map<string, string>
	alias: alias for table to be passed to table, map<string, string>
*/
type Table struct {
	Name     string `mapstructure:"name,omitempty"`
	Project  string `mapstructure:"project,omitempty"`
	Database string `mapstructure:"database,omitempty"`

	Description string     `mapstructure:"description,omitempty"`
	Schema      Schema     `mapstructure:"schema,omitempty"`
	Cluster     *Cluster   `mapstructure:"cluster,omitempty"`
	Partition   *Partition `mapstructure:"partition,omitempty"`
	Lifecycle   int        `mapstructure:"lifecycle,omitempty"`
	Type        string     `mapstructure:"type,omitempty"`

	Hints       map[string]string      `mapstructure:"hints,omitempty"`
	ExtraConfig map[string]interface{} `mapstructure:",remain"`
}

func (t *Table) FullName() string {
	return fmt.Sprintf("%s.%s.%s", t.Project, t.Database, t.Name)
}

func (t *Table) Validate() error {
	if len(t.Schema) == 0 {
		return errors.InvalidArgument(EntityTable, "empty schema for table "+t.FullName())
	}

	if err := t.Schema.Validate(); err != nil {
		return errors.AddErrContext(err, EntityTable, "invalid schema for table "+t.FullName())
	}

	if t.Partition != nil {
		if len(t.Partition.Columns) == 0 {
			return errors.InvalidArgument(EntityTable, "invalid partition columns for table "+t.FullName())
		}
	}

	if t.Cluster != nil {
		if err := t.Cluster.Validate(); err != nil {
			return errors.AddErrContext(err, EntityTable, "invalid cluster for table "+t.FullName())
		}
	}

	return nil
}

/*
Cluster configuration
Using:	define the columns used for clustering

Type: type of clustering to use for table

	Hash:  https://www.alibabacloud.com/help/en/maxcompute/use-cases/range-clustering
	Range: https://www.alibabacloud.com/help/en/maxcompute/use-cases/hash-clustering

SortBy: columns to use for sorting
Buckets: buckets to fill data in
*/
type Cluster struct {
	Using   []string     `mapstructure:"using,omitempty"`
	Type    string       `mapstructure:"type,omitempty"`
	SortBy  []SortColumn `mapstructure:"sort_by,omitempty"`
	Buckets int          `mapstructure:"buckets,omitempty"`
}

type SortColumn struct {
	Name  string `mapstructure:"name"`
	Order string `mapstructure:"order,omitempty"`
}

func (c Cluster) Validate() error {
	if len(c.Using) == 0 {
		return errors.InvalidArgument(EntityTable, "cluster config is empty")
	}
	for _, clause := range c.Using {
		if clause == "" {
			return errors.InvalidArgument(EntityTable, "cluster config has invalid value")
		}
	}

	return nil
}

type Partition struct {
	Columns []string `mapstructure:"field"`
}
