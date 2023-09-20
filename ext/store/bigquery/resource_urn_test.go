package bigquery_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/goto/optimus/ext/store/bigquery"
)

func TestNewResource(t *testing.T) {
	t.Run("return error when project is empty", func(t *testing.T) {
		resourceURN, err := bigquery.NewResourceURN("", "dataset", "table")
		assert.ErrorContains(t, err, "project is empty")
		assert.Empty(t, resourceURN)
	})
	t.Run("return error when dataset is empty", func(t *testing.T) {
		resourceURN, err := bigquery.NewResourceURN("project", "", "table")
		assert.ErrorContains(t, err, "dataset is empty")
		assert.Empty(t, resourceURN)
	})
	t.Run("return error when name is empty", func(t *testing.T) {
		resourceURN, err := bigquery.NewResourceURN("project", "dataset", "")
		assert.ErrorContains(t, err, "name is empty")
		assert.Empty(t, resourceURN)
	})
	t.Run("return success", func(t *testing.T) {
		resourceURN, err := bigquery.NewResourceURN("project", "dataset", "table")
		assert.NoError(t, err)
		assert.NotNil(t, resourceURN)
	})
}

func TestURN(t *testing.T) {
	t.Run("should return correct urn bq format", func(t *testing.T) {
		resourceURN, err := bigquery.NewResourceURN("project", "dataset", "table")
		assert.NoError(t, err)
		assert.NotNil(t, resourceURN)
		assert.Equal(t, "bigquery://project:dataset.table", resourceURN.URN())
	})
}
