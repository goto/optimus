package window

import (
	"bytes"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/charmbracelet/bubbles/textinput"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/olekukonko/tablewriter"

	"github.com/goto/optimus/internal/lib/duration"
	"github.com/goto/optimus/internal/lib/interval"
	"github.com/goto/optimus/internal/lib/window"
)

type model struct {
	currentCursor cursorPointer

	sizeInput      textinput.Model
	sizeUnit       duration.Unit
	delayInput     textinput.Model
	delayUnit      duration.Unit
	truncateToUnit duration.Unit
	locationInput  textinput.Model

	scheduleTime time.Time
}

func newModel() *model {
	sizeInput := textinput.New()
	sizeInput.SetValue("1")
	sizeUnit := duration.Day

	delayInput := textinput.New()
	delayInput.SetValue("1")
	delayUnit := duration.Day

	locationInput := textinput.New()
	locationInput.SetValue("UTC")

	truncateToUnit := duration.Day

	return &model{
		currentCursor:  pointToSizeInput,
		sizeInput:      sizeInput,
		sizeUnit:       sizeUnit,
		delayInput:     delayInput,
		delayUnit:      delayUnit,
		locationInput:  locationInput,
		truncateToUnit: truncateToUnit,
		scheduleTime:   time.Now().UTC(),
	}
}

func (*model) Init() tea.Cmd {
	// this method is to adhere to library contract
	return nil
}

func (m *model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	currMsg := reflect.TypeOf(msg)
	if currMsg.String() != "tea.KeyMsg" {
		return m, nil
	}

	msgStr := fmt.Sprintf("%s", msg)
	switch msgStr {
	case "ctrl+c", "q":
		return m, tea.Quit
	case "up":
		m.handleUp()
	case "down":
		m.handleDown()
	case "left":
		m.handleLeft()
	case "right":
		m.handleRight()
	case "shift+up", "W":
		m.handleIncrement()
	case "shift+down", "S":
		m.handleDecrement()
	default:
		m.handleInput(msg)
	}
	return m, nil
}

func (m *model) View() string {
	buff := new(bytes.Buffer)
	table := tablewriter.NewWriter(buff)
	table.SetAutoWrapText(false)
	table.SetBorder(false)
	table.SetRowLine(false)
	table.SetColumnSeparator("")
	table.Append([]string{m.generateWindowInputView(), m.generateWindowInputHintView()})
	table.Render()

	var s strings.Builder
	s.WriteString("INPUT")
	s.WriteString("\n")
	s.WriteString(buff.String())

	s.WriteString("RESULT")
	s.WriteString("\n")
	s.WriteString(m.generateWindowResultView())
	s.WriteString("\n")
	s.WriteString("DOCUMENTATION:\n")
	s.WriteString("- https://goto.github.io/optimus/docs/concepts/intervals-and-windows")
	return s.String()
}

func (m *model) generateWindowResultView() string {
	buff := new(bytes.Buffer)
	table := tablewriter.NewWriter(buff)

	interval, err := m.calculateInterval()
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

func (m *model) generateWindowInputHintView() string {
	var hint string
	switch m.currentCursor {
	case pointToSizeInput:
		hint = "empty or positive numeric value only\n"
	case pointToSizeUnit:
		hint = `valid values are:
- None or <empty>: only valid if size input is empty
- h: hour
- d: day
- w: week
- M: month
- y: year

press (shift+up) or (shift+w) to increment value
press (shift+down) or (shift+s) to decrement value
`
	case pointToDelayInput:
		hint = "empty or numeric value only (negative is allowed)"
	case pointToDelayUnit:
		hint = `valid values are:
- None or <empty>: only valid if delay input is empty
- h: hour
- d: day
- w: week
- M: month
- y: year

press (shift+up) or (shift+w) to increment value
press (shift+down) or (shift+s) to decrement value
`
	case pointToTruncateToUnit:
		hint = `valid values are:
- <empty>: truncation happens based on the size unit
- None: enforce no trancation
- h: hour
- d: day
- w: week
- M: month
- y: year

press (shift+up) or (shift+w) to increment value
press (shift+down) or (shift+s) to decrement value
`
	case pointToLocationInput:
		hint = `valid value is from IANA Time Zone database
lower case is encouraged`
	case pointToYear:
		hint = `year of the schedule time

press (shift+up) or (shift+w) to increment value
press (shift+down) or (shift+s) to decrement value
`
	case pointToMonth:
		hint = `month of the schedule time

press (shift+up) or (shift+w) to increment value
press (shift+down) or (shift+s) to decrement value
`
	case pointToDay:
		hint = `day of the schedule time

press (shift+up) or (shift+w) to increment value
press (shift+down) or (shift+s) to decrement value
`
	case pointToHour:
		hint = `hour of the schedule time

press (shift+up) or (shift+w) to increment value
press (shift+down) or (shift+s) to decrement value
`
	case pointToMinute:
		hint = `minute of the schedule time

press (shift+up) or (shift+w) to increment value
press (shift+down) or (shift+s) to decrement value
`
	}
	return "!hint!\n" + hint
}

func (m *model) generateWindowInputView() string {
	buff := new(bytes.Buffer)
	table := tablewriter.NewWriter(buff)
	table.SetAutoWrapText(false)
	table.SetRowLine(true)
	table.SetColumnAlignment([]int{tablewriter.ALIGN_LEFT, tablewriter.ALIGN_LEFT})
	table.Append([]string{
		"size",
		m.generateSizeView(),
	})
	table.Append([]string{
		"delay",
		m.generateDelayView(),
	})
	table.Append([]string{
		"truncate_to",
		m.generateTruncateToView(),
	})
	table.Append([]string{
		"location",
		m.generateLocationView(),
	})
	table.Append([]string{
		"job schedule",
		m.generateScheduleTimeView(),
	})
	table.Render()
	return buff.String()
}

func (m *model) calculateInterval() (interval.Interval, error) {
	sizeDuration, err := m.getSizeDuration()
	if err != nil {
		return interval.Interval{}, err
	}

	delayDuration, err := m.getDelayDuration()
	if err != nil {
		return interval.Interval{}, err
	}

	location, err := time.LoadLocation(m.locationInput.Value())
	if err != nil {
		return interval.Interval{}, errors.New("location is not recognized")
	}

	var truncate string
	if m.truncateToUnit != duration.None {
		truncate = string(m.truncateToUnit)
	}

	customWindow := window.NewCustomWindow(sizeDuration, delayDuration, location, truncate)

	return customWindow.GetInterval(m.scheduleTime)
}

func (m *model) getDelayDuration() (duration.Duration, error) {
	return m.getDuration(m.delayInput.Value(), m.delayUnit)
}

func (m *model) getSizeDuration() (duration.Duration, error) {
	sizeDuration, err := m.getDuration(m.sizeInput.Value(), m.sizeUnit)
	if err != nil {
		return duration.Duration{}, err
	}

	if sizeDuration.GetCount() < 0 {
		return duration.Duration{}, errors.New("size can not be negative")
	}

	return sizeDuration, nil
}

func (*model) getDuration(rawCount string, unit duration.Unit) (duration.Duration, error) {
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

func (m *model) generateSizeView() string {
	sizeInput := m.generateValueWithCursorPointerView(pointToSizeInput, m.sizeInput.Value())
	sizeUnit := m.generateValueWithCursorPointerView(pointToSizeUnit, string(m.sizeUnit))
	return sizeInput + " " + sizeUnit
}

func (m *model) generateDelayView() string {
	delayInput := m.generateValueWithCursorPointerView(pointToDelayInput, m.delayInput.Value())
	delayUnit := m.generateValueWithCursorPointerView(pointToDelayUnit, string(m.delayUnit))
	return delayInput + " " + delayUnit
}

func (m *model) generateTruncateToView() string {
	return m.generateValueWithCursorPointerView(pointToTruncateToUnit, string(m.truncateToUnit))
}

func (m *model) generateLocationView() string {
	return m.generateValueWithCursorPointerView(pointToLocationInput, m.locationInput.Value())
}

func (m *model) generateScheduleTimeView() string {
	year := m.generateValueWithCursorPointerView(pointToYear, strconv.Itoa(m.scheduleTime.Year()))
	month := m.generateValueWithCursorPointerView(pointToMonth, m.scheduleTime.Month().String())
	day := m.generateValueWithCursorPointerView(pointToDay, strconv.Itoa(m.scheduleTime.Day()))
	hour := m.generateValueWithCursorPointerView(pointToHour, strconv.Itoa(m.scheduleTime.Hour()))
	minute := m.generateValueWithCursorPointerView(pointToMinute, strconv.Itoa(m.scheduleTime.Minute()))

	return year + " " + month + " " + day + " " + hour + ":" + minute + " UTC | " + m.scheduleTime.Weekday().String()
}

func (m *model) generateValueWithCursorPointerView(targetCursor cursorPointer, value string) string {
	if m.currentCursor == targetCursor {
		var s strings.Builder
		s.WriteString("[")
		s.WriteString(value)
		s.WriteString("]")
		return s.String()
	}
	return value
}

func (m *model) handleInput(msg tea.Msg) {
	switch m.currentCursor {
	case pointToSizeInput:
		m.sizeInput, _ = m.sizeInput.Update(msg)
	case pointToDelayInput:
		m.delayInput, _ = m.delayInput.Update(msg)
	case pointToLocationInput:
		m.locationInput, _ = m.locationInput.Update(msg)
	}
}

func (m *model) handleDecrement() {
	switch m.currentCursor {
	case pointToSizeUnit:
		m.decrementUnit(&m.sizeUnit)
		m.truncateToUnit = m.sizeUnit
	case pointToDelayUnit:
		m.decrementUnit(&m.delayUnit)
	case pointToTruncateToUnit:
		m.decrementUnit(&m.truncateToUnit)
	default:
		m.decrementScheduleTime()
	}
}

func (m *model) decrementScheduleTime() {
	switch m.currentCursor {
	case pointToMinute:
		m.scheduleTime = m.scheduleTime.Add(-1 * time.Minute)
	case pointToHour:
		m.scheduleTime = m.scheduleTime.Add(-1 * time.Hour)
	case pointToDay:
		m.scheduleTime = m.scheduleTime.AddDate(0, 0, -1)
	case pointToMonth:
		m.scheduleTime = m.scheduleTime.AddDate(0, -1, 0)
	case pointToYear:
		m.scheduleTime = m.scheduleTime.AddDate(-1, 0, 0)
	}
}

func (*model) decrementUnit(unit *duration.Unit) {
	switch *unit {
	case duration.None:
		*unit = duration.Hour
	case duration.Hour:
		*unit = duration.Day
	case duration.Day:
		*unit = duration.Week
	case duration.Week:
		*unit = duration.Month
	case duration.Month:
		*unit = duration.Year
	case duration.Year:
		*unit = duration.None
	}
}

func (m *model) handleIncrement() {
	switch m.currentCursor {
	case pointToSizeUnit:
		m.incrementUnit(&m.sizeUnit)
		m.truncateToUnit = m.sizeUnit
	case pointToDelayUnit:
		m.incrementUnit(&m.delayUnit)
	case pointToTruncateToUnit:
		m.incrementUnit(&m.truncateToUnit)
	default:
		m.incrementScheduleTime()
	}
}

func (m *model) incrementScheduleTime() {
	switch m.currentCursor {
	case pointToMinute:
		m.scheduleTime = m.scheduleTime.Add(time.Minute)
	case pointToHour:
		m.scheduleTime = m.scheduleTime.Add(time.Hour)
	case pointToDay:
		m.scheduleTime = m.scheduleTime.AddDate(0, 0, 1)
	case pointToMonth:
		m.scheduleTime = m.scheduleTime.AddDate(0, 1, 0)
	case pointToYear:
		m.scheduleTime = m.scheduleTime.AddDate(1, 0, 0)
	}
}

func (*model) incrementUnit(unit *duration.Unit) {
	switch *unit {
	case duration.None:
		*unit = duration.Year
	case duration.Hour:
		*unit = duration.None
	case duration.Day:
		*unit = duration.Hour
	case duration.Week:
		*unit = duration.Day
	case duration.Month:
		*unit = duration.Week
	case duration.Year:
		*unit = duration.Month
	}
}

func (m *model) handleRight() {
	switch m.currentCursor {
	case pointToYear:
		m.currentCursor = pointToMonth
	case pointToMonth:
		m.currentCursor = pointToDay
	case pointToDay:
		m.currentCursor = pointToHour
	case pointToHour:
		m.currentCursor = pointToMinute
	case pointToMinute:
		m.currentCursor = pointToYear
	case pointToSizeInput:
		m.sizeInput.Blur()
		m.currentCursor = pointToSizeUnit
	case pointToDelayInput:
		m.delayInput.Blur()
		m.currentCursor = pointToDelayUnit
	}
}

func (m *model) handleLeft() {
	switch m.currentCursor {
	case pointToMinute:
		m.currentCursor = pointToHour
	case pointToHour:
		m.currentCursor = pointToDay
	case pointToDay:
		m.currentCursor = pointToMonth
	case pointToMonth:
		m.currentCursor = pointToYear
	case pointToYear:
		m.currentCursor = pointToMinute
	case pointToSizeUnit:
		m.sizeInput.Focus()
		m.currentCursor = pointToSizeInput
	case pointToDelayUnit:
		m.delayInput.Focus()
		m.currentCursor = pointToDelayInput
	}
}

func (m *model) handleDown() {
	switch m.currentCursor {
	case pointToSizeInput:
		m.sizeInput.Blur()
		m.delayInput.Focus()
		m.currentCursor = pointToDelayInput
	case pointToSizeUnit:
		m.currentCursor = pointToDelayUnit
	case pointToDelayInput, pointToDelayUnit:
		m.delayInput.Blur()
		m.currentCursor = pointToTruncateToUnit
	case pointToTruncateToUnit:
		m.locationInput.Focus()
		m.currentCursor = pointToLocationInput
	case pointToLocationInput:
		m.locationInput.Blur()
		m.currentCursor = pointToYear
	default:
		m.currentCursor = pointToSizeInput
		m.sizeInput.Focus()
	}
}

func (m *model) handleUp() {
	switch m.currentCursor {
	case pointToSizeInput, pointToSizeUnit:
		m.sizeInput.Blur()
		m.currentCursor = pointToYear
	case pointToDelayInput:
		m.delayInput.Blur()
		m.sizeInput.Focus()
		m.currentCursor = pointToSizeInput
	case pointToDelayUnit:
		m.currentCursor = pointToSizeUnit
	case pointToTruncateToUnit:
		m.delayInput.Focus()
		m.currentCursor = pointToDelayInput
	case pointToLocationInput:
		m.locationInput.Blur()
		m.currentCursor = pointToTruncateToUnit
	default:
		m.locationInput.Focus()
		m.currentCursor = pointToLocationInput
	}
}
