package tenant

import (
	"strings"

	"github.com/goto/optimus/internal/models"
)

const presetWindowVersion = 2

type Preset struct {
	name        string
	description string

	window models.Window
}

func (p Preset) Name() string {
	return p.name
}

func (p Preset) Description() string {
	return p.description
}

func (p Preset) Window() models.Window {
	return p.window
}

func NewPreset(name, description, truncateTo, offset, size string) (Preset, error) {
	window, err := models.NewWindow(presetWindowVersion, truncateTo, offset, size)
	if err != nil {
		return Preset{}, err
	}

	err = window.Validate()
	if err != nil {
		return Preset{}, err
	}

	return Preset{
		name:        strings.ToLower(name),
		description: description,
		window:      window,
	}, nil
}
