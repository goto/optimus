package window

import (
	"strings"
	"time"

	"github.com/goto/optimus/internal/errors"
	"github.com/goto/optimus/internal/lib/duration"
	"github.com/goto/optimus/internal/models"
)

const NewWindowVersion = 3

const (
	Incremental Type = "incremental"
	Preset      Type = "preset"
	Custom      Type = "custom"
)

type Type string

type SimpleConfig struct {
	Size       string
	Delay      string
	Location   string
	TruncateTo string // TODO: remove later if unused
}

type Config struct {
	windowType Type

	Preset string
	// kept for backward compatibility, will be removed later
	Window models.Window

	simple SimpleConfig
}

// Following functions are for backward compatibility

func (c Config) GetSize() string { // nolint: gocritic
	if c.Window == nil {
		return c.simple.Size
	}

	return c.Window.GetSize()
}

func (c Config) GetOffset() string { // nolint: gocritic
	if c.Window == nil {
		if c.simple.Delay == "" {
			return ""
		}
		if strings.HasPrefix(c.simple.Delay, "-") {
			return c.simple.Delay[1:]
		}
		return "-" + c.simple.Delay
	}

	return c.Window.GetOffset()
}

func (c Config) GetTruncateTo() string { // nolint: gocritic
	if c.Window == nil {
		return c.simple.TruncateTo
	}

	return c.Window.GetTruncateTo()
}

func (c Config) GetVersion() int {
	if c.Window == nil {
		return NewWindowVersion
	}

	return c.Window.GetVersion()
}

func (c Config) GetSimpleConfig() SimpleConfig {
	return c.simple
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

func NewSimpleConfig(size, delay, location, truncateTo string) (SimpleConfig, error) {
	validationErr := errors.NewMultiError("error in window config")

	err := duration.Validate(size)
	validationErr.Append(err)

	err = duration.Validate(delay)
	validationErr.Append(err)

	_, err = time.LoadLocation(location)
	validationErr.Append(err)

	if truncateTo != "" {
		_, err = duration.UnitFrom(truncateTo)
		validationErr.Append(err)
	}

	if len(validationErr.Errors) > 0 {
		return SimpleConfig{}, validationErr.ToErr()
	}

	return SimpleConfig{
		Size:       size,
		Delay:      delay,
		Location:   location,
		TruncateTo: truncateTo,
	}, nil
}

func NewConfig(size, delay, location, truncateTo string) (Config, error) {
	simpleConfig, err := NewSimpleConfig(size, delay, location, truncateTo)
	if err != nil {
		return Config{}, err
	}

	return Config{
		windowType: Custom,
		simple:     simpleConfig,
	}, nil
}

func NewCustomConfigWithTimezone(c SimpleConfig) Config {
	return Config{
		windowType: Custom,
		simple:     c,
	}
}

func NewIncrementalConfig() Config {
	return Config{windowType: Incremental}
}

func (c Config) Type() Type {
	return c.windowType
}
