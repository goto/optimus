package resolver_test

import (
	"context"
	"testing"

	"github.com/goto/optimus/core/job/resolver"
	"github.com/stretchr/testify/assert"
)

func TestGenerateDestination(t *testing.T) {
	ctx := context.Background()
	t.Run("should properly generate a destination provided correct config inputs", func(t *testing.T) {
		configs := map[string]string{
			"PROJECT": "proj",
			"DATASET": "datas",
			"TABLE":   "tab",
		}
		destination, err := resolver.GenerateDestination(ctx, configs)
		assert.Nil(t, err)
		assert.Equal(t, "bigquery://proj:datas.tab", destination)
	})
	t.Run("should throw an error if any on of the config is missing to generate destination", func(t *testing.T) {
		configs := map[string]string{
			"PROJECT": "proj",
			"DATASET": "datas",
		}
		destination, err := resolver.GenerateDestination(ctx, configs)
		assert.NotNil(t, err)
		assert.Empty(t, destination)
	})
}
