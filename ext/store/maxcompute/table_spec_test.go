package maxcompute_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/goto/optimus/ext/store/maxcompute"
)

func TestRelationalTable(t *testing.T) {
	t.Run("when invalid", func(t *testing.T) {
		t.Run("returns validation error for empty schema", func(t *testing.T) {
			table := maxcompute.Table{
				Name:        "playground.characters",
				Schema:      nil,
				Cluster:     &maxcompute.Cluster{Using: []string{"tags"}},
				Partition:   &maxcompute.Partition{Columns: []string{"time"}},
				ExtraConfig: nil,
			}
			err := table.Validate()
			assert.NotNil(t, err)
			assert.ErrorContains(t, err, "empty schema for table playground.characters")
		})
		t.Run("returns validation error for invalid schema", func(t *testing.T) {
			table := maxcompute.Table{
				Name:        "playground.characters",
				Schema:      maxcompute.Schema{{Name: "", Type: "string"}},
				Cluster:     &maxcompute.Cluster{Using: []string{"tags"}},
				Partition:   &maxcompute.Partition{Columns: []string{"time"}},
				ExtraConfig: nil,
			}
			err := table.Validate()
			assert.NotNil(t, err)
			assert.ErrorContains(t, err, "invalid schema for table playground.characters")
		})
		t.Run("returns validation error for invalid cluster", func(t *testing.T) {
			table := maxcompute.Table{
				Name:        "playground.characters",
				Schema:      maxcompute.Schema{{Name: "id", Type: "string"}},
				Cluster:     &maxcompute.Cluster{Using: []string{}},
				Partition:   &maxcompute.Partition{Columns: []string{"time"}},
				ExtraConfig: nil,
			}
			err := table.Validate()
			assert.NotNil(t, err)
			assert.ErrorContains(t, err, "invalid cluster for table playground.characters")
		})
	})
	t.Run("returns no validation error when correct", func(t *testing.T) {
		table := maxcompute.Table{
			Name:        "playground.characters",
			Schema:      maxcompute.Schema{{Name: "id", Type: "string"}},
			Cluster:     &maxcompute.Cluster{Using: []string{"tags"}},
			Partition:   &maxcompute.Partition{Columns: []string{"time"}},
			ExtraConfig: nil,
		}
		err := table.Validate()
		assert.Nil(t, err)

		assert.Equal(t, "playground.characters", table.FullName())
	})
	t.Run("fails validation for empty field name in partition", func(t *testing.T) {
		table := maxcompute.Table{
			Name:        "playground.characters",
			Schema:      maxcompute.Schema{{Name: "id", Type: "string"}},
			Cluster:     &maxcompute.Cluster{Using: []string{"tags"}},
			Partition:   &maxcompute.Partition{Columns: []string{}},
			ExtraConfig: nil,
		}
		err := table.Validate()
		assert.NotNil(t, err)
		assert.ErrorContains(t, err, "invalid partition columns for table playground.characters")
	})
}

func TestTableClustering(t *testing.T) {
	t.Run("returns error when invalid", func(t *testing.T) {
		cluster := maxcompute.Cluster{Using: nil}

		err := cluster.Validate()
		assert.NotNil(t, err)
		assert.ErrorContains(t, err, "cluster config is empty")
	})
	t.Run("returns error when invalid value for cluster column", func(t *testing.T) {
		cluster := maxcompute.Cluster{Using: []string{""}}

		err := cluster.Validate()
		assert.NotNil(t, err)
		assert.ErrorContains(t, err, "cluster config has invalid value")
	})
	t.Run("no validation error when valid", func(t *testing.T) {
		cluster := maxcompute.Cluster{
			Using:   []string{"id"},
			Type:    "RANGE",
			SortBy:  nil,
			Buckets: 0,
		}
		assert.Nil(t, cluster.Validate())
	})
}
