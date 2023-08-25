package window

import (
	"time"

	"github.com/goto/optimus/internal/errors"
	"github.com/goto/optimus/internal/lib/cron"
	"github.com/goto/optimus/internal/models"
)

type Interval struct {
	Start time.Time
	End   time.Time
}

type Window struct {
	schedule *cron.ScheduleSpec
	window   models.Window
}

func (w Window) GetInterval(referenceTime time.Time) (Interval, error) {
	if w.schedule != nil {
		return Interval{
			Start: w.schedule.Prev(referenceTime),
			End:   w.schedule.Next(referenceTime),
		}, nil
	}

	startTime, err := w.window.GetStartTime(referenceTime)
	if err != nil {
		return Interval{}, err
	}

	endTime, err := w.window.GetEndTime(referenceTime)
	if err != nil {
		return Interval{}, err
	}

	return Interval{
		Start: startTime,
		End:   endTime,
	}, nil
}

func FromSchedule(schedule string) (Window, error) {
	jobCron, err := cron.ParseCronSchedule(schedule)
	if err != nil {
		return Window{}, errors.InternalError("Window", "unable to parse job cron interval", err)
	}

	return Window{schedule: jobCron}, nil
}

func FromBaseWindow(w models.Window) Window {
	return Window{window: w}
}
