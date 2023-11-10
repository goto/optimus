package duration

import (
	"time"
)

type Unit string

const (
	None  Unit = "None"
	Hour  Unit = "h"
	Day   Unit = "d"
	Week  Unit = "w"
	Month Unit = "M"
	Year  Unit = "y"
)

type Duration struct {
	count int
	unit  Unit
}

func (d Duration) SubtractFrom(t time.Time) time.Time {
	count := d.count * -1

	switch d.unit {
	case None:
		return t
	case Hour:
		return t.Add(time.Hour * time.Duration(count))
	case Day:
		return t.AddDate(0, 0, count)
	case Week:
		return t.AddDate(0, 0, count*7)
	case Month:
		return t.AddDate(0, count, 0)
	case Year:
		return t.AddDate(count, 0, 0)
	}

	return t
}

func (d Duration) GetUnit() Unit {
	return d.unit
}

func (d Duration) GetCount() int {
	return d.count
}

func NewDuration(count int, unit Unit) Duration {
	return Duration{
		count: count,
		unit:  unit,
	}
}
