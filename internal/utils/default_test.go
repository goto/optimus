package utils_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/goto/optimus/internal/utils"
)

func TestDefault(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		nonEmpty := utils.GetFirstNonEmpty("", "first", "second")
		assert.Equal(t, nonEmpty, "first")
	})
}
