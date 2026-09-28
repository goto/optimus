package dag_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/goto/optimus/core/scheduler"
	"github.com/goto/optimus/ext/scheduler/airflow/dag"
)

func TestToResource(t *testing.T) {
	t.Run("returns nil when resource is nil", func(t *testing.T) {
		actual := dag.ToResource(nil)
		assert.Nil(t, actual)
	})

	t.Run("returns nil when request and limit are both nil", func(t *testing.T) {
		actual := dag.ToResource(&scheduler.Resource{})
		assert.Nil(t, actual)
	})

	t.Run("returns nil when request and limit are both fully empty", func(t *testing.T) {
		actual := dag.ToResource(&scheduler.Resource{
			Request: &scheduler.ResourceConfig{},
			Limit:   &scheduler.ResourceConfig{},
		})
		assert.Nil(t, actual)
	})

	t.Run("sets only request when only request is populated", func(t *testing.T) {
		actual := dag.ToResource(&scheduler.Resource{
			Request: &scheduler.ResourceConfig{CPU: "250m", Memory: "128Mi"},
		})
		assert.Equal(t, &dag.Resource{
			Request: &dag.ResourceConfig{CPU: "250m", Memory: "128Mi"},
		}, actual)
	})

	t.Run("sets only limit when only limit is populated with ephemeral storage", func(t *testing.T) {
		actual := dag.ToResource(&scheduler.Resource{
			Limit: &scheduler.ResourceConfig{EphemeralStorage: "5Gi"},
		})
		assert.Equal(t, &dag.Resource{
			Limit: &dag.ResourceConfig{EphemeralStorage: "5Gi"},
		}, actual)
	})

	t.Run("sets both request and limit including ephemeral storage when fully populated", func(t *testing.T) {
		actual := dag.ToResource(&scheduler.Resource{
			Request: &scheduler.ResourceConfig{CPU: "250m", Memory: "128Mi", EphemeralStorage: "1Gi"},
			Limit:   &scheduler.ResourceConfig{CPU: "500m", Memory: "256Mi", EphemeralStorage: "2Gi"},
		})
		assert.Equal(t, &dag.Resource{
			Request: &dag.ResourceConfig{CPU: "250m", Memory: "128Mi", EphemeralStorage: "1Gi"},
			Limit:   &dag.ResourceConfig{CPU: "500m", Memory: "256Mi", EphemeralStorage: "2Gi"},
		}, actual)
	})
}
