package window

import (
	"strings"

	"github.com/goto/optimus/internal/errors"
	"github.com/goto/optimus/internal/models"
)

const (
	Incremental Type = "incremental"
	Preset      Type = "preset"
	Custom      Type = "custom"
)

type Type string

type Config struct {
	windowType Type

	Preset string
	// kept for backward compatibility, will be changed to only v2 at some point
	Window models.Window
}

func NewPresetConfig(preset string) (Config, error) {
	presetName := strings.ToLower(strings.TrimPrefix(preset, "@"))
	if preset == "" {
		return Config{}, errors.InvalidArgument("Window", "invalid window config preset")
	}

	return Config{
		windowType: Preset,
		Preset:     presetName,
	}, nil
}

func NewCustomConfig(w models.Window) Config {
	return Config{
		windowType: Custom,
		Window:     w,
	}
}

func NewIncrementalConfig() Config {
	return Config{windowType: Incremental}
}

func (c Config) Type() Type {
	return c.windowType
}
