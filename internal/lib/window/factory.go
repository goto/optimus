package window

import "github.com/goto/optimus/internal/models"

type WithWindow interface {
	Window() models.Window
}

func From[T WithWindow](config Config, schedule string, getter func(string) (T, error)) (Window, error) {
	if config.Type() == Incremental {
		return FromSchedule(schedule)
	}

	if config.Type() == Preset {
		baseWindow, err := getter(config.Preset)
		if err != nil {
			return Window{}, err
		}
		return FromBaseWindow(baseWindow.Window()), nil
	}

	return FromBaseWindow(config.Window), nil
}
