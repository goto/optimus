package interval

import (
	"time"
)

type Interval struct {
	start time.Time
	end   time.Time
}

func (i Interval) Start() time.Time {
	return i.start
}

func (i Interval) End() time.Time {
	return i.end
}

func NewInterval(start, end time.Time) Interval {
	return Interval{
		start: start,
		end:   end,
	}
}

func (i Interval) Empty() bool {
	return i.start.IsZero() && i.end.IsZero()
}

// IsAfter compares 2 intervals if one is after the another.
// Interval 2 can start at the end of interval 1 or after
func (i Interval) IsAfter(i2 Interval) bool {
	return i.start.After(i2.end) || i.start.Equal(i2.end)
}

func (i Interval) Equal(i2 Interval) bool {
	return i.start.Equal(i2.start) && i.end.Equal(i2.end)
}
