package maxcompute_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/goto/optimus/ext/store/maxcompute"
)

func TestSanitizer(t *testing.T) {
	t.Run("returns quoted identifier for reserved keywords", func(t *testing.T) {
		testCases := []struct {
			input    string
			expected string
		}{
			{"select", "`select`"},
			{"from", "`from`"},
			{"case", "`case`"},
			{"table", "`table`"},
			{"SELECT", "`SELECT`"},
			{"From", "`From`"},
		}

		for _, tc := range testCases {
			result := maxcompute.QuoteIdentifier(tc.input)
			assert.Equal(t, tc.expected, result)
		}
	})

	t.Run("returns identifier unchanged when not a reserved keyword", func(t *testing.T) {
		testCases := []struct {
			input    string
			expected string
		}{
			{"customer_name", "customer_name"},
			{"other", "other"},
		}

		for _, tc := range testCases {
			result := maxcompute.QuoteIdentifier(tc.input)
			assert.Equal(t, tc.expected, result)
		}
	})
}
