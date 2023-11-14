package window

import (
	"time"

	"github.com/goto/optimus/internal/lib/interval"
)

type WithConfig interface {
	Config() SimpleConfig
}

type Window interface {
	GetInterval(referenceTime time.Time) (interval.Interval, error)
}

func From[T WithConfig](config Config, schedule string, getter func(string) (T, error)) (Window, error) {
	if config.Type() == Incremental {
		return FromSchedule(schedule)
	}

	if config.Type() == Preset {
		preset, err := getter(config.Preset)
		if err != nil {
			return CustomWindow{}, err
		}
		return FromCustomConfig(preset.Config())
	}

	if config.Window == nil {
		return FromCustomConfig(config.simple)
	}

	return FromBaseWindow(config.Window), nil
}
