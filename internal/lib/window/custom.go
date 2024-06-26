package window

import (
	"time"

	"github.com/goto/optimus/internal/errors"
	"github.com/goto/optimus/internal/lib/duration"
	"github.com/goto/optimus/internal/lib/interval"
)

var errNegativeSize = errors.InvalidArgument("window", "size can not be negative")

type CustomWindow struct {
	size    duration.Duration
	shiftBy duration.Duration

	timezone   *time.Location
	truncateTo string // Instead of empty, let's use None for the unit. makes it easy to understand
}

func (w CustomWindow) GetInterval(ref time.Time) (interval.Interval, error) {
	truncatedTime, err := w.alignToTimeUnit(ref)
	if err != nil {
		return interval.Interval{}, err
	}

	endTimeWithoutShifting := truncatedTime
	startTimeWithoutShifting := w.size.SubtractFrom(endTimeWithoutShifting)

	finalEndTime := w.shiftBy.AddFrom(endTimeWithoutShifting)
	finalStartTime := w.shiftBy.AddFrom(startTimeWithoutShifting)

	return interval.NewInterval(finalStartTime, finalEndTime), nil
}

func (w CustomWindow) GetEnd(ref time.Time) (time.Time, error) {
	truncatedTime, err := w.alignToTimeUnit(ref)
	if err != nil {
		return ref, err
	}
	return w.shiftBy.AddFrom(truncatedTime), nil
}

func (w CustomWindow) alignToTimeUnit(ref time.Time) (time.Time, error) {
	unit := w.size.GetUnit()
	if w.truncateTo != "" {
		var err error
		unit, err = duration.UnitFrom(w.truncateTo)
		if err != nil {
			return ref, err
		}
	}

	timeWithZone := ref.In(w.timezone)
	if unit == duration.None {
		return timeWithZone, nil
	}

	year, month, day := timeWithZone.Date()
	hour, minute, sec, nsec := 0, 0, 0, 0

	switch unit {
	case duration.Hour:
		hour = timeWithZone.Hour()

	case duration.Week:
		weekday := timeWithZone.Weekday()
		if weekday == 0 {
			weekday = 7 // moving sunday to end of week, monday as start of week
		}
		day -= int(weekday - time.Monday)

	case duration.Year:
		month = 1
		fallthrough

	case duration.Month:
		day = 1
	}

	return time.Date(year, month, day, hour, minute, sec, nsec, w.timezone), nil
}

// TODO: this function is not used anywhere at the moment, consider removing it
func NewCustomWindow(size, shiftBy duration.Duration, location *time.Location, truncateTo string) CustomWindow {
	return CustomWindow{
		size:       size,
		shiftBy:    shiftBy,
		timezone:   location,
		truncateTo: truncateTo,
	}
}

func FromCustomConfig(c SimpleConfig) (CustomWindow, error) {
	size, err := duration.From(c.Size)
	if err != nil {
		return CustomWindow{}, err
	}

	if size.GetCount() < 0 {
		return CustomWindow{}, errNegativeSize
	}

	shiftBy, err := duration.From(c.ShiftBy)
	if err != nil {
		return CustomWindow{}, err
	}

	loc, err := time.LoadLocation(c.Location)
	if err != nil {
		return CustomWindow{}, err
	}

	return CustomWindow{
		size:       size,
		shiftBy:    shiftBy,
		timezone:   loc,
		truncateTo: c.TruncateTo,
	}, nil
}
