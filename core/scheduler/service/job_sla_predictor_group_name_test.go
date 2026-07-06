package service // nolint: testpackage

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDeriveGroupName(t *testing.T) {
	assert.Equal(t, "default", deriveGroupName(nil))
	assert.Equal(t, "default", deriveGroupName(map[string]string{}))
	// keys are sorted for stability
	assert.Equal(t, "a=1, b=2", deriveGroupName(map[string]string{"b": "2", "a": "1"}))
}
