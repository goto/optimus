package tenant

import (
	"strings"

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
	cleanedName := strings.ToLower(strings.TrimSpace(name))
	if cleanedName == "" {
		return Preset{}, errors.InvalidArgument(EntityProject, "cleaned preset name is empty")
	}

	cleanedDescription := strings.TrimSpace(description)
	if cleanedDescription == "" {
		return Preset{}, errors.InvalidArgument(EntityProject, "cleaned preset description is empty")
	}

	window, err := models.NewWindow(2, truncateTo, offset, size)
	if err != nil {
		return Preset{}, err
	}

	if err := window.Validate(); err != nil {
		return Preset{}, err
	}

	return Preset{
		name:        cleanedName,
		description: cleanedDescription,
		window:      window,
	}, nil
}
