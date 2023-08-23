package tenant

import (
	"strings"

	"github.com/goto/optimus/internal/errors"
	"github.com/goto/optimus/internal/models"
)

const EntityPreset = "preset"

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
	cleanedName := strings.ToLower(strings.TrimSpace(name))
	if cleanedName == "" {
		return Preset{}, errors.InvalidArgument(EntityPreset, "cleaned preset name is empty")
	}

	cleanedDescription := strings.TrimSpace(description)
	if cleanedDescription == "" {
		return Preset{}, errors.InvalidArgument(EntityPreset, "cleaned preset description is empty")
	}

	window, err := models.NewWindow(2, truncateTo, offset, size)
	if err != nil {
		return Preset{}, err
	}

	return Preset{
		name:        cleanedName,
		description: cleanedDescription,
		window:      window,
	}, nil
}
