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

func TestFlattenUnique(t *testing.T) {
	t.Run("should return flattened upstream in the form of resource", func(t *testing.T) {
		resources := []*bigquery.ResourceURNWithUpstreams{
			{
				ResourceURN: newResourceURN("project_test_1", "dataset_test_1", "name_test_1"),
			},
			{
				ResourceURN: newResourceURN("project_test_2", "dataset_test_2", "name_test_2"),
				Upstreams: []*bigquery.ResourceURNWithUpstreams{
					{
						ResourceURN: newResourceURN("project_test_3", "dataset_test_3", "name_test_3"),
					},
					{
						ResourceURN: newResourceURN("project_test_4", "dataset_test_4", "name_test_4"),
					},
					{
						ResourceURN: newResourceURN("project_test_1", "dataset_test_1", "name_test_1"),
					},
					nil,
				},
			},
		}

		expectedResources := []*bigquery.ResourceURNWithUpstreams{
			{
				ResourceURN: newResourceURN("project_test_1", "dataset_test_1", "name_test_1"),
			},
			{
				ResourceURN: newResourceURN("project_test_2", "dataset_test_2", "name_test_2"),
			},
			{
				ResourceURN: newResourceURN("project_test_3", "dataset_test_3", "name_test_3"),
			},
			{
				ResourceURN: newResourceURN("project_test_4", "dataset_test_4", "name_test_4"),
			},
		}

		actualResources := bigquery.ResourceURNWithUpstreamsList(resources).FlattenUnique()

		assert.ElementsMatch(t, expectedResources, actualResources)
	})
}

func newResourceURN(project, dataset, name string) bigquery.ResourceURN {
	resourceURN, _ := bigquery.NewResourceURN(project, dataset, name)
	return resourceURN
}
