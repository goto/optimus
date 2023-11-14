package tenant

import (
	"github.com/goto/optimus/internal/errors"
	"github.com/goto/optimus/internal/lib/window"
)

type Preset struct {
	name        string
	description string

	config window.SimpleConfig
}

func (p Preset) Name() string {
	return p.name
}

func (p Preset) Description() string {
	return p.description
}

func (p Preset) Config() window.SimpleConfig {
	return p.config
}

func (p Preset) Equal(incoming Preset) bool {
	if p.name != incoming.name {
		return false
	}

	if p.description != incoming.description {
		return false
	}

	if p.config.Size != incoming.config.Size {
		return false
	}

	if p.config.Delay != incoming.config.Delay {
		return false
	}

	if p.config.Location != incoming.config.Location {
		return false
	}

	if p.config.TruncateTo != incoming.config.TruncateTo {
		return false
	}

	return true
}

func NewPreset(name, description, size, delay, location, truncateTo string) (Preset, error) {
	if name == "" {
		return Preset{}, errors.InvalidArgument(EntityProject, "name is empty")
	}

	if description == "" {
		return Preset{}, errors.InvalidArgument(EntityProject, "description is empty")
	}

	config, err := window.NewSimpleConfig(size, delay, location, truncateTo)
	if err != nil {
		return Preset{}, err
	}

	return Preset{
		name:        name,
		description: description,
		config:      config,
	}, nil
}

func NewPresetWithConfig(name, description string, conf window.SimpleConfig) Preset {
	return Preset{
		name:        name,
		description: description,
		config:      conf,
	}
}
