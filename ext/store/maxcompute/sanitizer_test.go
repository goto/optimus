package maxcompute

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSanitizer(t *testing.T) {
	t.Run("returns safe keyword", func(t *testing.T) {
		testCases := []struct {
			input    string
			expected string
		}{
			{"select", "`select`"},
			{"from", "`from`"},
			{"case", "`case`"},
			{"customer_name", "customer_name"},
			{"other", "other"},
			{"table", "`table`"},
		}

		for _, tc := range testCases {
			result := SafeKeyword(tc.input)
			assert.Equal(t, tc.expected, result)
		}
	})
}
