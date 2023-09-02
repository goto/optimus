package window_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/goto/optimus/internal/lib/window"
	"github.com/goto/optimus/internal/models"
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
			assert.Equal(t, "", config.GetSize())
			assert.Equal(t, "", config.GetOffset())
			assert.Equal(t, "", config.GetTruncateTo())
			assert.Equal(t, 0, config.GetVersion())
			assert.Equal(t, "preset", string(config.Type()))
		})
	})
	t.Run("Custom Window config", func(t *testing.T) {
		t.Run("creates a custom window config", func(t *testing.T) {
			w, err := models.NewWindow(2, "d", "0", "24h")
			assert.NoError(t, err)
			config := window.NewCustomConfig(w)
			assert.Equal(t, "", config.Preset)
			assert.Equal(t, "24h", config.GetSize())
			assert.Equal(t, "0", config.GetOffset())
			assert.Equal(t, "d", config.GetTruncateTo())
			assert.Equal(t, 2, config.GetVersion())
			assert.Equal(t, "custom", string(config.Type()))
		})
	})
	t.Run("Incremental Window config", func(t *testing.T) {
		t.Run("creates a incremental window config", func(t *testing.T) {
			config := window.NewIncrementalConfig()
			assert.Equal(t, "", config.Preset)
			assert.Equal(t, "", config.GetSize())
			assert.Equal(t, "", config.GetOffset())
			assert.Equal(t, "", config.GetTruncateTo())
			assert.Equal(t, 0, config.GetVersion())
			assert.Equal(t, "incremental", string(config.Type()))
		})
	})
}
