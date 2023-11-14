package label_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/goto/optimus/internal/lib/label"
)

func TestFromMap(t *testing.T) {
	t.Run("should return nil and error if incoming labels are nil", func(t *testing.T) {
		var incoming map[string]string

		actualLabels, actualError := label.FromMap(incoming)

		assert.Nil(t, actualLabels)
		assert.ErrorContains(t, actualError, "incoming map is nil")
	})

	t.Run("should return labels and nil if no error is encountered", func(t *testing.T) {
		incoming := map[string]string{
			"key1": "value1",
		}

		actualLabels, actualError := label.FromMap(incoming)

		assert.NotEmpty(t, actualLabels)
		assert.ErrorContains(t, actualError, "incoming map is nil")
	})
}
