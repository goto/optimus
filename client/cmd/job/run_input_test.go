package job_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/goto/optimus/client/cmd/job"
)

func TestConstructConfigEnvSourcingContent(t *testing.T) {
	t.Run("construct content with unsubstitude value", func(t *testing.T) {
		config := map[string]string{
			"EXAMPLE": "<no value>",
			"ANOTHER": "hello",
		}
		content, keys := job.ConstructConfigEnvSourcingContent(config)
		assert.Len(t, keys, 1)
		assert.Equal(t, `EXAMPLE='<no value>'
ANOTHER='hello'
`, content)
	})
	t.Run("construct content with single quote in it", func(t *testing.T) {
		config := map[string]string{
			"EXAMPLE": "value with 'single quote'",
			"ANOTHER": "hello",
		}
		content, keys := job.ConstructConfigEnvSourcingContent(config)
		assert.Len(t, keys, 0)
		assert.Equal(t, `EXAMPLE='value with '\''single quote'\'''
ANOTHER='hello'
`, content)
	})
}
