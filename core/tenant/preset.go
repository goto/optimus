package tenant

import "github.com/goto/optimus/internal/models"

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
	window, err := models.NewWindow(2, truncateTo, offset, size)
	if err != nil {
		return Preset{}, err
	}

	return Preset{
		name:        name,
		description: description,
		window:      window,
	}, nil
}
