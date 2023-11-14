package window

import (
	"time"

	"github.com/goto/optimus/internal/errors"
	"github.com/goto/optimus/internal/lib/cron"
	"github.com/goto/optimus/internal/lib/interval"
)

type IncrementalWindow struct {
	schedule *cron.ScheduleSpec
}

func (w IncrementalWindow) GetInterval(referenceTime time.Time) (interval.Interval, error) {
	s := w.schedule.Prev(referenceTime)
	end := w.schedule.Next(referenceTime)
	return interval.NewInterval(s, end), nil
}

func FromSchedule(schedule string) (IncrementalWindow, error) {
	jobCron, err := cron.ParseCronSchedule(schedule)
	if err != nil {
		return IncrementalWindow{}, errors.InternalError("Window", "unable to parse job cron interval", err)
	}

	return IncrementalWindow{schedule: jobCron}, nil
}
