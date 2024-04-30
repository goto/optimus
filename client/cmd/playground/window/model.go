package window

import (
	"fmt"
	"reflect"
	"time"

	"github.com/charmbracelet/bubbles/textinput"
	tea "github.com/charmbracelet/bubbletea"

	"github.com/goto/optimus/internal/lib/duration"
)

type model struct {
	currentCursor cursorPointer

	sizeInput      textinput.Model
	sizeUnit       duration.Unit
	shfitByInput   textinput.Model
	shiftByUnit    duration.Unit
	truncateToUnit duration.Unit
	locationInput  textinput.Model

	scheduleTime time.Time
}

func newModel() *model {
	sizeInput := textinput.New()
	sizeInput.SetValue("1")
	sizeUnit := duration.Day

	shiftByInput := textinput.New()
	shiftByInput.SetValue("1")
	shiftByUnit := duration.Day

	locationInput := textinput.New()
	locationInput.SetValue("UTC")

	truncateToUnit := duration.Day

	return &model{
		currentCursor:  pointToSizeInput,
		sizeInput:      sizeInput,
		sizeUnit:       sizeUnit,
		shfitByInput:   shiftByInput,
		shiftByUnit:    shiftByUnit,
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
	view := view{
		currentCursor:  m.currentCursor,
		sizeInput:      m.sizeInput.Value(),
		sizeUnit:       m.sizeUnit,
		shiftByInput:   m.shfitByInput.Value(),
		shiftByUnit:    m.shiftByUnit,
		truncateToUnit: m.truncateToUnit,
		locationInput:  m.locationInput.Value(),

		scheduleTime: m.scheduleTime,
	}

	return view.Render()
}

func (m *model) handleInput(msg tea.Msg) {
	switch m.currentCursor {
	case pointToSizeInput:
		m.sizeInput, _ = m.sizeInput.Update(msg)
	case pointToShiftByInput:
		m.shfitByInput, _ = m.shfitByInput.Update(msg)
	case pointToLocationInput:
		m.locationInput, _ = m.locationInput.Update(msg)
	}
}

func (m *model) handleDecrement() {
	switch m.currentCursor {
	case pointToSizeUnit:
		m.decrementUnit(&m.sizeUnit)
		m.truncateToUnit = m.sizeUnit
	case pointToShiftByUnit:
		m.decrementUnit(&m.shiftByUnit)
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
	case pointToShiftByUnit:
		m.incrementUnit(&m.shiftByUnit)
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
	case pointToShiftByInput:
		m.shfitByInput.Blur()
		m.currentCursor = pointToShiftByUnit
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
	case pointToShiftByUnit:
		m.shfitByInput.Focus()
		m.currentCursor = pointToShiftByInput
	}
}

func (m *model) handleDown() {
	switch m.currentCursor {
	case pointToSizeInput:
		m.sizeInput.Blur()
		m.shfitByInput.Focus()
		m.currentCursor = pointToShiftByInput
	case pointToSizeUnit:
		m.currentCursor = pointToShiftByUnit
	case pointToShiftByInput, pointToShiftByUnit:
		m.shfitByInput.Blur()
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
	case pointToShiftByInput:
		m.shfitByInput.Blur()
		m.sizeInput.Focus()
		m.currentCursor = pointToSizeInput
	case pointToShiftByUnit:
		m.currentCursor = pointToSizeUnit
	case pointToTruncateToUnit:
		m.shfitByInput.Focus()
		m.currentCursor = pointToShiftByInput
	case pointToLocationInput:
		m.locationInput.Blur()
		m.currentCursor = pointToTruncateToUnit
	default:
		m.locationInput.Focus()
		m.currentCursor = pointToLocationInput
	}
}
