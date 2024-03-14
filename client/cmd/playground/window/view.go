package window

import (
	"bytes"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/olekukonko/tablewriter"

	"github.com/goto/optimus/internal/lib/duration"
	"github.com/goto/optimus/internal/lib/interval"
	"github.com/goto/optimus/internal/lib/window"
)

type view struct {
	currentCursor cursorPointer

	sizeInput      string
	sizeUnit       duration.Unit
	delayInput     string
	delayUnit      duration.Unit
	truncateToUnit duration.Unit
	locationInput  string

	scheduleTime time.Time
}

// Render renders the view into a string format.
// This is the only method which should be called for `view` type
// from the caller outside `view`, emphasized with the first capitalized letter.
func (v *view) Render() string {
	var s strings.Builder
	s.WriteString(v.getInputSection())
	s.WriteString(v.getResultSection())
	s.WriteString("DOCUMENTATION:\n")
	s.WriteString("- https://goto.github.io/optimus/docs/concepts/intervals-and-windows")

	return s.String()
}

func (v *view) getInputSection() string {
	buff := new(bytes.Buffer)

	table := tablewriter.NewWriter(buff)
	table.SetAutoWrapText(false)
	table.SetBorder(false)
	table.SetRowLine(false)
	table.SetColumnSeparator("")

	inputTable := v.getInputTable()
	inputHint := v.getInputHint()

	table.Append([]string{"INPUT", "HINT"})
	table.Append([]string{inputTable, inputHint})

	table.Render()

	return buff.String()
}

func (v *view) getResultSection() string {
	buff := new(bytes.Buffer)

	table := tablewriter.NewWriter(buff)
	table.SetAutoWrapText(false)
	table.SetBorder(false)
	table.SetRowLine(false)
	table.SetColumnSeparator("")

	table.Append([]string{"RESULT"})
	table.Append([]string{v.getResultTable()})
	table.Render()

	return buff.String()
}

func (v *view) getResultTable() string {
	buff := new(bytes.Buffer)
	table := tablewriter.NewWriter(buff)

	interval, err := v.calculateInterval()
	if err != nil {
		table.SetHeader([]string{"ERROR"})
		table.Append([]string{err.Error()})
	} else {
		startRow := interval.Start().Format(time.RFC3339)
		endRow := interval.End().Format(time.RFC3339)

		table.SetHeader([]string{"Start Time", "End Time"})
		table.Append([]string{startRow, endRow})
	}

	table.Render()
	return buff.String()
}

func (v *view) getInputHint() string {
	var hint string
	switch v.currentCursor {
	case pointToSizeInput:
		hint = "empty or positive numeric value only"
	case pointToSizeUnit:
		hint = `valid values are:
- <empty> or None
- h: hour
- d: day
- w: week
- M: month
- y: year

<empty> or None are only valid if size value is empty

(shift+up) or (shift+w) to increment
(shift+down) or (shift+s) to decrement
`
	case pointToDelayInput:
		hint = `empty or numeric value only
negative is allowed`
	case pointToDelayUnit:
		hint = `valid values are:
- <empty> or None
- h: hour
- d: day
- w: week
- M: month
- y: year

<empty> or None are only valid if delay value is empty

(shift+up) or (shift+w) to increment
(shift+down) or (shift+s) to decrement
`
	case pointToTruncateToUnit:
		hint = `valid values are:
- <empty> or None
- h: hour
- d: day
- w: week
- M: month
- y: year

<empty> unit enforces truncation based on the size unit
None unit enforces no truncation

(shift+up) or (shift+w) to increment
(shift+down) or (shift+s) to decrement
`
	case pointToLocationInput:
		hint = `valid value is from IANA Time Zone database
lower case is encouraged`
	case pointToYear:
		hint = `year of the schedule time

(shift+up) or (shift+w) to increment
(shift+down) or (shift+s) to decrement
`
	case pointToMonth:
		hint = `month of the schedule time

(shift+up) or (shift+w) to increment
(shift+down) or (shift+s) to decrement
`
	case pointToDay:
		hint = `day of the schedule time

(shift+up) or (shift+w) to increment
(shift+down) or (shift+s) to decrement
`
	case pointToHour:
		hint = `hour of the schedule time

(shift+up) or (shift+w) to increment
(shift+down) or (shift+s) to decrement
`
	case pointToMinute:
		hint = `minute of the schedule time

(shift+up) or (shift+w) to increment
(shift+down) or (shift+s) to decrement
`
	}

	return hint
}

func (v *view) getInputTable() string {
	buff := new(bytes.Buffer)

	table := tablewriter.NewWriter(buff)
	table.SetAutoMergeCellsByColumnIndex([]int{0})
	table.SetRowLine(true)
	table.SetColumnAlignment([]int{tablewriter.ALIGN_LEFT, tablewriter.ALIGN_LEFT})

	table.Append([]string{
		"size",
		v.getSizeInput(),
	})
	table.Append([]string{
		"delay",
		v.getDelayInput(),
	})
	table.Append([]string{
		"truncate_to",
		v.getTruncateToInput(),
	})
	table.Append([]string{
		"location",
		v.getLocationInput(),
	})
	table.Append([]string{
		"job schedule",
		v.getScheduleTimeInput(),
	})
	table.Append([]string{
		"job schedule",
		v.scheduleTime.Weekday().String(),
	})

	table.Render()
	return buff.String() + "\n"
}

func (v *view) calculateInterval() (interval.Interval, error) {
	sizeDuration, err := v.getSizeDuration()
	if err != nil {
		return interval.Interval{}, err
	}

	delayDuration, err := v.getDelayDuration()
	if err != nil {
		return interval.Interval{}, err
	}

	location, err := time.LoadLocation(v.locationInput)
	if err != nil {
		return interval.Interval{}, errors.New("location is not recognized")
	}

	var truncate string
	if v.truncateToUnit != duration.None {
		truncate = string(v.truncateToUnit)
	}

	customWindow := window.NewCustomWindow(sizeDuration, delayDuration, location, truncate)

	return customWindow.GetInterval(v.scheduleTime)
}

func (v *view) getDelayDuration() (duration.Duration, error) {
	return v.getDuration(v.delayInput, v.delayUnit)
}

func (v *view) getSizeDuration() (duration.Duration, error) {
	sizeDuration, err := v.getDuration(v.sizeInput, v.sizeUnit)
	if err != nil {
		return duration.Duration{}, err
	}

	if sizeDuration.GetCount() < 0 {
		return duration.Duration{}, errors.New("size can not be negative")
	}

	return sizeDuration, nil
}

func (*view) getDuration(rawCount string, unit duration.Unit) (duration.Duration, error) {
	const maxDigit = 4
	if len(rawCount) > maxDigit {
		return duration.Duration{}, fmt.Errorf("maximum allowed digit is %d", maxDigit)
	}

	var count int
	if rawCount != "" {
		c, err := strconv.Atoi(rawCount)
		if err != nil {
			return duration.Duration{}, errors.New("value is not a valid integer")
		}

		count = c
	}

	if unit == "" {
		unit = duration.None
	}

	return duration.NewDuration(count, unit), nil
}

func (v *view) getSizeInput() string {
	sizeInput := v.getValueWithCursor(pointToSizeInput, v.sizeInput)
	sizeUnit := v.getValueWithCursor(pointToSizeUnit, string(v.sizeUnit))
	return sizeInput + " " + sizeUnit
}

func (v *view) getDelayInput() string {
	delayInput := v.getValueWithCursor(pointToDelayInput, v.delayInput)
	delayUnit := v.getValueWithCursor(pointToDelayUnit, string(v.delayUnit))
	return delayInput + " " + delayUnit
}

func (v *view) getTruncateToInput() string {
	return v.getValueWithCursor(pointToTruncateToUnit, string(v.truncateToUnit))
}

func (v *view) getLocationInput() string {
	return v.getValueWithCursor(pointToLocationInput, v.locationInput)
}

func (v *view) getScheduleTimeInput() string {
	year := v.getValueWithCursor(pointToYear, strconv.Itoa(v.scheduleTime.Year()))
	month := v.getValueWithCursor(pointToMonth, v.scheduleTime.Month().String())
	day := v.getValueWithCursor(pointToDay, strconv.Itoa(v.scheduleTime.Day()))
	hour := v.getValueWithCursor(pointToHour, strconv.Itoa(v.scheduleTime.Hour()))
	minute := v.getValueWithCursor(pointToMinute, strconv.Itoa(v.scheduleTime.Minute()))

	return year + " " + month + " " + day + " " + hour + ":" + minute + v.getUTC()
}

func (v *view) getValueWithCursor(targetCursor cursorPointer, value string) string {
	if v.currentCursor == targetCursor {
		return "[" + value + "]"
	}

	return value
}

func (v *view) getUTC() string {
	switch v.currentCursor {
	case pointToYear, pointToMonth, pointToDay, pointToHour, pointToMinute:
		return " UTC"
	}

	return " UTC  "
}
