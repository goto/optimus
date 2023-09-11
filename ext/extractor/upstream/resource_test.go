package upstream_test

import (
	"testing"

	"github.com/goto/optimus/ext/extractor/upstream"
	"github.com/stretchr/testify/assert"
)

func TestFilterResources(t *testing.T) {
	input := []*upstream.Resource{
		{
			Project: "project_test",
			Dataset: "dataset_test",
			Name:    "name_1_test",
		},
		{
			Project: "project_test",
			Dataset: "dataset_test",
			Name:    "name_2_test",
		},
		{
			Project: "project_test",
			Dataset: "dataset_test",
			Name:    "name_3_test",
		},
	}
	exclude3 := input[2]

	expectedResult := input[:2]

	actualResult := upstream.Resources(input).GetWithoutResource(exclude3)

	assert.ElementsMatch(t, expectedResult, actualResult)
}

func TestUniqueFilterResources(t *testing.T) {
	input := []*upstream.Resource{
		{
			Project: "project_test",
			Dataset: "dataset_1_test",
			Name:    "name_1_test",
		},
		{
			Project: "project_test",
			Dataset: "dataset_1_test",
			Name:    "name_2_test",
		},
		{
			Project: "project_test",
			Dataset: "dataset_1_test",
			Name:    "name_1_test",
		},
		{
			Project: "project_test",
			Dataset: "dataset_1_test",
			Name:    "name_2_test",
		},
	}

	expectedResult := input[:2]

	actualResult := upstream.Resources(input).GetUnique()

	assert.ElementsMatch(t, expectedResult, actualResult)
}

func TestGroupResources(t *testing.T) {
	input := []*upstream.Resource{
		{
			Project: "project_test",
			Dataset: "dataset_1_test",
			Name:    "name_1_test",
		},
		{
			Project: "project_test",
			Dataset: "dataset_1_test",
			Name:    "name_2_test",
		},
		{
			Project: "project_test",
			Dataset: "dataset_2_test",
			Name:    "name_1_test",
		},
		{
			Project: "project_test",
			Dataset: "dataset_2_test",
			Name:    "name_2_test",
		},
	}

	expectedResult := []*upstream.ResourceGroup{
		{
			Project: "project_test",
			Dataset: "dataset_1_test",
			Names:   []string{"name_1_test", "name_2_test"},
		},
		{
			Project: "project_test",
			Dataset: "dataset_2_test",
			Names:   []string{"name_1_test", "name_2_test"},
		},
	}

	actualResult := upstream.Resources(input).GroupResources()

	assert.ElementsMatch(t, expectedResult, actualResult)
}

func TestFlattenUpstreams(t *testing.T) {
	t.Run("should return flattened upstream in the form of resource", func(t *testing.T) {
		upstreams := []*upstream.Resource{
			{
				Project: "project_test_1",
				Dataset: "dataset_test_1",
				Name:    "name_test_1",
			},
			{
				Project: "project_test_2",
				Dataset: "dataset_test_2",
				Name:    "name_test_2",
				Upstreams: []*upstream.Resource{
					{
						Project: "project_test_3",
						Dataset: "dataset_test_3",
						Name:    "name_test_3",
					},
					{
						Project: "project_test_4",
						Dataset: "dataset_test_4",
						Name:    "name_test_4",
					},
					nil,
				},
			},
		}

		expectedResources := []*upstream.Resource{
			{
				Project: "project_test_1",
				Dataset: "dataset_test_1",
				Name:    "name_test_1",
			},
			{
				Project: "project_test_2",
				Dataset: "dataset_test_2",
				Name:    "name_test_2",
			},
			{
				Project: "project_test_3",
				Dataset: "dataset_test_3",
				Name:    "name_test_3",
			},
			{
				Project: "project_test_4",
				Dataset: "dataset_test_4",
				Name:    "name_test_4",
			},
		}

		actualResources := upstream.Resources(upstreams).GetFlattened()

		assert.Equal(t, expectedResources, actualResources)
	})
}
