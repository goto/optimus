package window

import (
	"time"

	"github.com/goto/optimus/internal/lib/interval"
	"github.com/goto/optimus/internal/models"
)

type OldWindow struct {
	window models.Window
}

func (w OldWindow) GetInterval(referenceTime time.Time) (interval.Interval, error) {
	endTime, err := w.window.GetEndTime(referenceTime)
	if err != nil {
		return interval.Interval{}, err
	}

	startTime, err := w.window.GetStartTime(referenceTime)
	if err != nil {
		return interval.Interval{}, err
	}

	return interval.NewInterval(startTime, endTime), nil
}

func FromBaseWindow(w models.Window) OldWindow {
	return OldWindow{window: w}
}
