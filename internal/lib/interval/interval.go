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
