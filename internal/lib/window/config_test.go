package window_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/goto/optimus/internal/lib/window"
)

func TestWindowConfig(t *testing.T) {
	t.Run("Preset Window config", func(t *testing.T) {
		t.Run("returns error when preset is empty", func(t *testing.T) {
			_, err := window.NewPresetConfig("")
			assert.ErrorContains(t, err, "invalid window config preset")
		})
		t.Run("creates a config with preset", func(t *testing.T) {
			preset := "@yesterday"
			config, err := window.NewPresetConfig(preset)
			assert.NoError(t, err)
			assert.Equal(t, "yesterday", config.Preset)
			assert.Equal(t, "preset", string(config.Type()))
		})
	})
	t.Run("Custom Window config", func(t *testing.T) {
		t.Run("creates a custom window config version 3", func(t *testing.T) {
			config, err := window.NewConfig("1d", "-1h", "", "")

			assert.NoError(t, err)
			assert.Equal(t, "", config.Preset)
			sc := config.GetSimpleConfig()
			assert.Equal(t, "1d", sc.Size)
			assert.Equal(t, "-1h", sc.ShiftBy)
			assert.Equal(t, "", sc.TruncateTo)
			assert.Equal(t, "custom", string(config.Type()))
		})
	})
	t.Run("Incremental Window config", func(t *testing.T) {
		t.Run("creates a incremental window config", func(t *testing.T) {
			config := window.NewIncrementalConfig()
			assert.Equal(t, "", config.Preset)
			assert.Equal(t, "incremental", string(config.Type()))
		})
	})
}
