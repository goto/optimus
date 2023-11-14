package window_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/goto/optimus/internal/lib/duration"
	"github.com/goto/optimus/internal/lib/window"
)

func TestCustomWindow(t *testing.T) {
	istLoc, locErr := time.LoadLocation("Asia/Kolkata")
	assert.NoError(t, locErr)

	// Times given below are same, they are just in different locations
	refTimeUTC := time.Date(2023, time.November, 12, 10, 20, 50, 0, time.UTC)
	//refTimeUTC := time.Date(2023, time.November, 10, 10, 20, 50, 0, time.UTC)
	refTimeIST := time.Date(2023, time.November, 10, 15, 50, 50, 0, istLoc)

	zero := duration.NewDuration(0, duration.None)
	hour := duration.NewDuration(2, duration.Hour)
	day := duration.NewDuration(3, duration.Day)
	week := duration.NewDuration(1, duration.Week)
	month := duration.NewDuration(3, duration.Month)
	year := duration.NewDuration(1, duration.Year)

	t.Run("GetInterval", func(t *testing.T) {

	})
	t.Run("GetEnd without delay", func(t *testing.T) {
		tests := []struct {
			name   string
			window window.CustomWindow
			output time.Time
		}{
			{
				name:   "returns ref time in window format when none",
				window: window.NewCustomWindow(hour, zero, time.UTC, "None"),
				output: refTimeUTC,
			},
			{
				name:   "returns ref time in hour window",
				window: window.NewCustomWindow(hour, zero, time.UTC, ""),
				output: time.Date(2023, 11, 10, 10, 0, 0, 0, time.UTC),
			},
			{
				name:   "returns ref time in day window",
				window: window.NewCustomWindow(day, zero, time.UTC, ""),
				output: time.Date(2023, 11, 10, 0, 0, 0, 0, time.UTC),
			},
			{
				name:   "returns ref time in week window",
				window: window.NewCustomWindow(week, zero, time.UTC, ""),
				output: time.Date(2023, 11, 6, 0, 0, 0, 0, time.UTC),
			},
			{
				name:   "returns ref time in month window",
				window: window.NewCustomWindow(month, zero, time.UTC, ""),
				output: time.Date(2023, 11, 1, 0, 0, 0, 0, time.UTC),
			},
			{
				name:   "returns ref time in year window",
				window: window.NewCustomWindow(year, zero, time.UTC, ""),
				output: time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC),
			},
			{
				name:   "returns ref time in window format when none in IST",
				window: window.NewCustomWindow(hour, zero, istLoc, "None"),
				output: refTimeUTC.In(istLoc),
			},
			{
				name:   "returns ref time in IST hour window",
				window: window.NewCustomWindow(hour, zero, istLoc, ""),
				output: time.Date(2023, 11, 10, 15, 0, 0, 0, istLoc),
			},
			{
				name:   "returns ref time in IST day window",
				window: window.NewCustomWindow(day, zero, istLoc, ""),
				output: time.Date(2023, 11, 10, 0, 0, 0, 0, istLoc),
			},
			{
				name:   "returns ref time in IST week window",
				window: window.NewCustomWindow(week, zero, istLoc, ""),
				output: time.Date(2023, 11, 6, 0, 0, 0, 0, istLoc),
			},
			{
				name:   "returns ref time in IST month window",
				window: window.NewCustomWindow(month, zero, istLoc, ""),
				output: time.Date(2023, 11, 1, 0, 0, 0, 0, istLoc),
			},
			{
				name:   "returns ref time in IST year window",
				window: window.NewCustomWindow(year, zero, istLoc, ""),
				output: time.Date(2023, 1, 1, 0, 0, 0, 0, istLoc),
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				end, err := tt.window.GetEnd(refTimeUTC)
				assert.NoError(t, err)
				assert.Equal(t, tt.output, end)

				endIST, err := tt.window.GetEnd(refTimeIST)
				assert.NoError(t, err)
				assert.Equal(t, tt.output, endIST)
			})
		}
	})
	t.Run("FromCustomConfig", func(t *testing.T) {

	})
}
