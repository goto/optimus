package window_test

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/goto/optimus/internal/lib/window"
	"github.com/goto/optimus/internal/models"
)

type Preset struct {
	Name   string
	config window.SimpleConfig
}

func (p Preset) Config() window.SimpleConfig {
	return p.config
}

func TestWindowFactory(t *testing.T) {
	t.Run("From", func(t *testing.T) {
		t.Run("creates window from incremental", func(t *testing.T) {
			config := window.NewIncrementalConfig()
			w, err := window.From[Preset](config, "0 0 1 * *", nil)
			assert.NoError(t, err)

			sept1 := time.Date(2023, 9, 1, 1, 0, 0, 0, time.UTC)
			interval, err := w.GetInterval(sept1)
			assert.NoError(t, err)
			assert.Equal(t, "2023-09-01T00:00:00Z", interval.Start().Format(time.RFC3339))
			assert.Equal(t, "2023-10-01T00:00:00Z", interval.End().Format(time.RFC3339))
		})
		t.Run("returns error with invalid preset", func(t *testing.T) {
			config, err := window.NewPresetConfig("yesterday")
			assert.NoError(t, err)

			_, err = window.From[Preset](config, "", func(name string) (Preset, error) {
				return Preset{}, errors.New("cannot get window")
			})
			assert.Error(t, err)
		})
		t.Run("creates window from preset", func(t *testing.T) {
			config, err := window.NewPresetConfig("yesterday")
			assert.NoError(t, err)

			w, err := window.From[Preset](config, "", func(name string) (Preset, error) {
				conf := window.SimpleConfig{
					Size:       "1d",
					Delay:      "",
					TruncateTo: "",
					Location:   "",
				}
				return Preset{
					Name:   "yesterday",
					config: conf,
				}, nil
			})
			assert.NoError(t, err)

			sept1 := time.Date(2023, 9, 1, 1, 0, 0, 0, time.UTC)
			interval, err := w.GetInterval(sept1)
			assert.NoError(t, err)
			assert.Equal(t, "2023-08-31T00:00:00Z", interval.Start().Format(time.RFC3339))
			assert.Equal(t, "2023-09-01T00:00:00Z", interval.End().Format(time.RFC3339))
		})
		t.Run("creates window from config", func(t *testing.T) {
			config, err := window.NewConfig("1d", "", "", "")
			assert.NoError(t, err)

			w, err := window.From[Preset](config, "", func(name string) (Preset, error) {
				return Preset{
					Name:   "yesterday",
					config: config.GetSimpleConfig(),
				}, nil
			})
			assert.NoError(t, err)

			sept1 := time.Date(2023, 9, 1, 1, 0, 0, 0, time.UTC)
			interval, err := w.GetInterval(sept1)
			assert.NoError(t, err)
			assert.Equal(t, "2023-08-31T00:00:00Z", interval.Start().Format(time.RFC3339))
			assert.Equal(t, "2023-09-01T00:00:00Z", interval.End().Format(time.RFC3339))
		})
		t.Run("creates window from base window", func(t *testing.T) {
			w1, err := models.NewWindow(2, "d", "0", "24h")
			assert.NoError(t, err)

			w, err := window.From[Preset](window.NewCustomConfig(w1), "", nil)
			assert.NoError(t, err)

			sept1 := time.Date(2023, 9, 1, 1, 0, 0, 0, time.UTC)
			interval, err := w.GetInterval(sept1)
			assert.NoError(t, err)
			assert.Equal(t, "2023-08-31T00:00:00Z", interval.Start().Format(time.RFC3339))
			assert.Equal(t, "2023-09-01T00:00:00Z", interval.End().Format(time.RFC3339))
		})
	})
}
