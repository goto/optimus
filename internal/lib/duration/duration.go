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

const NumberOfDaysInWeek = 7

type Duration struct {
	count int
	unit  Unit
}

func (d Duration) AddFrom(t time.Time) time.Time {
	return addByCountUnit(t, d.count, d.unit)
}

func (d Duration) SubtractFrom(t time.Time) time.Time {
	count := d.count * -1

	return addByCountUnit(t, count, d.unit)
}

func addByCountUnit(t time.Time, count int, unit Unit) time.Time {
	switch unit {
	case None:
		return t
	case Hour:
		return t.Add(time.Hour * time.Duration(count))
	case Day:
		return t.AddDate(0, 0, count)
	case Week:
		return t.AddDate(0, 0, count*NumberOfDaysInWeek)
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
