package utils_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/goto/optimus/internal/utils"
)

func TestEscapeLikePattern(t *testing.T) {
	tests := []struct {
		name     string
		pattern  string
		expected string
	}{
		{
			name:     "keeps plain pattern as is",
			pattern:  "projectA.schemaA.",
			expected: "projectA.schemaA.",
		},
		{
			name:     "escapes percent",
			pattern:  "100%",
			expected: `100\%`,
		},
		{
			name:     "escapes underscore",
			pattern:  "my_table",
			expected: `my\_table`,
		},
		{
			name:     "escapes backslash",
			pattern:  `back\slash`,
			expected: `back\\slash`,
		},
		{
			name:     "escapes all wildcard characters",
			pattern:  `a_b%c\d`,
			expected: `a\_b\%c\\d`,
		},
		{
			name:     "handles empty string",
			pattern:  "",
			expected: "",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, utils.EscapeLikePattern(tc.pattern))
		})
	}
}
