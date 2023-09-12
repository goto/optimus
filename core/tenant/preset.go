package tenant

import (
	"github.com/goto/optimus/internal/errors"
	"github.com/goto/optimus/internal/models"
)

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

func (p Preset) Equal(incoming Preset) bool {
	if p.name != incoming.name {
		return false
	}

	if p.description != incoming.description {
		return false
	}

	if p.window.GetTruncateTo() != incoming.window.GetTruncateTo() {
		return false
	}

	if p.window.GetOffset() != incoming.window.GetOffset() {
		return false
	}

	if p.window.GetSize() != incoming.window.GetSize() {
		return false
	}

	return true
}

func NewPreset(name, description, truncateTo, offset, size string) (Preset, error) {
	if name == "" {
		return Preset{}, errors.InvalidArgument(EntityProject, "name is empty")
	}

	if description == "" {
		return Preset{}, errors.InvalidArgument(EntityProject, "description is empty")
	}

	window, err := models.NewWindow(2, truncateTo, offset, size) //nolint:gomnd
	if err != nil {
		return Preset{}, err
	}

	if err := window.Validate(); err != nil {
		return Preset{}, err
	}

	return Preset{
		name:        name,
		description: description,
		window:      window,
	}, nil
}
