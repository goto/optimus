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
	refTimeUTC := time.Date(2023, time.November, 10, 10, 20, 50, 0, time.UTC)
	refTimeIST := time.Date(2023, time.November, 10, 15, 50, 50, 0, istLoc)

	zero := duration.NewDuration(0, duration.None)
	hour := duration.NewDuration(2, duration.Hour)
	day := duration.NewDuration(3, duration.Day)
	week := duration.NewDuration(1, duration.Week)
	month := duration.NewDuration(3, duration.Month)
	year := duration.NewDuration(1, duration.Year)

	t.Run("GetInterval", func(t *testing.T) {
		t.Run("returns error when invalid config", func(t *testing.T) {
			w := window.NewCustomWindow(duration.NewDuration(1, duration.Day),
				duration.NewDuration(0, duration.None), time.UTC, "g")

			_, err := w.GetInterval(refTimeUTC)
			assert.Error(t, err)
			assert.ErrorContains(t, err, "invalid value for unit g, accepted values are [h,d,w,M,y]")
		})

		type values struct {
			ref   time.Time
			start time.Time
			end   time.Time
		}

		tests := []struct {
			name   string
			window window.CustomWindow
			values []values
		}{
			{
				name:   "returns interval with none(truncate_to)",
				window: window.NewCustomWindow(day, zero, time.UTC, "None"),
				values: []values{
					{
						ref:   refTimeIST,
						start: time.Date(2023, time.November, 7, 10, 20, 50, 0, time.UTC),
						end:   refTimeUTC,
					},
				},
			},
			{
				name:   "returns hours interval with days truncate_to",
				window: window.NewCustomWindow(duration.NewDuration(24, duration.Hour), zero, time.UTC, "d"),
				values: []values{
					{
						ref:   refTimeIST,
						start: time.Date(2023, time.November, 9, 0, 0, 0, 0, time.UTC),
						end:   time.Date(2023, time.November, 10, 0, 0, 0, 0, time.UTC),
					},
					{
						ref:   time.Date(2023, time.November, 9, 0, 0, 0, 0, time.UTC),
						start: time.Date(2023, time.November, 8, 0, 0, 0, 0, time.UTC),
						end:   time.Date(2023, time.November, 9, 0, 0, 0, 0, time.UTC),
					},
				},
			},
			{
				name:   "returns interval with hour",
				window: window.NewCustomWindow(hour, zero, time.UTC, ""),
				values: []values{
					{
						ref:   refTimeIST,
						start: time.Date(2023, time.November, 10, 8, 0, 0, 0, time.UTC),
						end:   time.Date(2023, time.November, 10, 10, 0, 0, 0, time.UTC),
					},
					{
						ref:   time.Date(2023, time.November, 9, 0, 0, 0, 0, time.UTC),
						start: time.Date(2023, time.November, 8, 22, 0, 0, 0, time.UTC),
						end:   time.Date(2023, time.November, 9, 0, 0, 0, 0, time.UTC),
					},
					{
						ref:   time.Date(2023, time.November, 9, 0, 0, 0, 0, istLoc),
						start: time.Date(2023, time.November, 8, 16, 0, 0, 0, time.UTC),
						end:   time.Date(2023, time.November, 8, 18, 0, 0, 0, time.UTC),
					},
				},
			},
			{
				name:   "returns interval with day",
				window: window.NewCustomWindow(day, zero, time.UTC, ""),
				values: []values{
					{
						ref:   refTimeIST,
						start: time.Date(2023, time.November, 7, 0, 0, 0, 0, time.UTC),
						end:   time.Date(2023, time.November, 10, 0, 0, 0, 0, time.UTC),
					},
					{
						ref:   time.Date(2023, time.November, 9, 0, 0, 0, 0, time.UTC),
						start: time.Date(2023, time.November, 6, 0, 0, 0, 0, time.UTC),
						end:   time.Date(2023, time.November, 9, 0, 0, 0, 0, time.UTC),
					},
					{
						ref:   time.Date(2023, time.November, 9, 0, 0, 2, 0, istLoc),
						start: time.Date(2023, time.November, 5, 0, 0, 0, 0, time.UTC),
						end:   time.Date(2023, time.November, 8, 0, 0, 0, 0, time.UTC),
					},
					{
						ref:   time.Date(2023, time.November, 8, 23, 59, 59, 0, time.UTC),
						start: time.Date(2023, time.November, 5, 0, 0, 0, 0, time.UTC),
						end:   time.Date(2023, time.November, 8, 0, 0, 0, 0, time.UTC),
					},
					{
						ref:   time.Date(2023, time.November, 1, 0, 0, 0, 0, time.UTC),
						start: time.Date(2023, time.October, 29, 0, 0, 0, 0, time.UTC),
						end:   time.Date(2023, time.November, 1, 0, 0, 0, 0, time.UTC),
					},
				},
			},
			{
				name:   "returns interval with week",
				window: window.NewCustomWindow(week, zero, time.UTC, ""),
				values: []values{
					{
						ref:   refTimeIST,
						start: time.Date(2023, time.October, 30, 0, 0, 0, 0, time.UTC),
						end:   time.Date(2023, time.November, 6, 0, 0, 0, 0, time.UTC),
					},
					{
						ref:   time.Date(2023, time.November, 13, 0, 0, 0, 0, time.UTC),
						start: time.Date(2023, time.November, 6, 0, 0, 0, 0, time.UTC),
						end:   time.Date(2023, time.November, 13, 0, 0, 0, 0, time.UTC),
					},
					{
						ref:   time.Date(2023, time.November, 8, 0, 0, 0, 0, time.UTC),
						start: time.Date(2023, time.October, 30, 0, 0, 0, 0, time.UTC),
						end:   time.Date(2023, time.November, 6, 0, 0, 0, 0, time.UTC),
					},
					{
						ref:   time.Date(2023, time.November, 12, 0, 0, 0, 0, time.UTC),
						start: time.Date(2023, time.October, 30, 0, 0, 0, 0, time.UTC),
						end:   time.Date(2023, time.November, 6, 0, 0, 0, 0, time.UTC),
					},
				},
			},
			{
				name:   "returns interval with month",
				window: window.NewCustomWindow(month, zero, time.UTC, ""),
				values: []values{
					{
						ref:   refTimeIST,
						start: time.Date(2023, time.August, 1, 0, 0, 0, 0, time.UTC),
						end:   time.Date(2023, time.November, 1, 0, 0, 0, 0, time.UTC),
					},
					{
						ref:   time.Date(2024, time.February, 1, 0, 0, 0, 0, time.UTC),
						start: time.Date(2023, time.November, 1, 0, 0, 0, 0, time.UTC),
						end:   time.Date(2024, time.February, 1, 0, 0, 0, 0, time.UTC),
					},
				},
			},
			{
				name:   "returns interval with year",
				window: window.NewCustomWindow(year, zero, time.UTC, ""),
				values: []values{
					{
						ref:   refTimeIST,
						start: time.Date(2022, time.January, 1, 0, 0, 0, 0, time.UTC),
						end:   time.Date(2023, time.January, 1, 0, 0, 0, 0, time.UTC),
					},
				},
			},
			{
				name:   "returns day interval with shift by hour",
				window: window.NewCustomWindow(day, hour, time.UTC, ""),
				values: []values{
					{
						ref:   refTimeIST,
						start: time.Date(2023, time.November, 7, 2, 0, 0, 0, time.UTC),
						end:   time.Date(2023, time.November, 10, 2, 0, 0, 0, time.UTC),
					},
					{
						ref:   time.Date(2023, time.November, 9, 0, 0, 0, 0, time.UTC),
						start: time.Date(2023, time.November, 6, 2, 0, 0, 0, time.UTC),
						end:   time.Date(2023, time.November, 9, 2, 0, 0, 0, time.UTC),
					},
				},
			},
			{
				name:   "returns week interval with shift by day",
				window: window.NewCustomWindow(week, day, time.UTC, ""),
				values: []values{
					{
						ref:   refTimeIST,
						start: time.Date(2023, time.November, 2, 0, 0, 0, 0, time.UTC),
						end:   time.Date(2023, time.November, 9, 0, 0, 0, 0, time.UTC),
					},
					{
						ref:   time.Date(2023, time.November, 13, 0, 0, 0, 0, time.UTC),
						start: time.Date(2023, time.November, 9, 0, 0, 0, 0, time.UTC),
						end:   time.Date(2023, time.November, 16, 0, 0, 0, 0, time.UTC),
					},
				},
			},
			{
				name:   "returns month interval with shift by day",
				window: window.NewCustomWindow(month, day, time.UTC, ""),
				values: []values{
					{
						ref:   refTimeIST,
						start: time.Date(2023, time.August, 4, 0, 0, 0, 0, time.UTC),
						end:   time.Date(2023, time.November, 4, 0, 0, 0, 0, time.UTC),
					},
					{
						ref:   time.Date(2023, time.January, 13, 0, 0, 0, 0, time.UTC),
						start: time.Date(2022, time.October, 4, 0, 0, 0, 0, time.UTC),
						end:   time.Date(2023, time.January, 4, 0, 0, 0, 0, time.UTC),
					},
				},
			},
			{
				name:   "returns days in month interval with shift by day",
				window: window.NewCustomWindow(duration.NewDuration(27, duration.Day), day, time.UTC, "M"),
				values: []values{
					{
						ref:   refTimeIST,
						start: time.Date(2023, time.October, 8, 0, 0, 0, 0, time.UTC),
						end:   time.Date(2023, time.November, 4, 0, 0, 0, 0, time.UTC),
					},
				},
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				for _, v := range tt.values {
					i, err := tt.window.GetInterval(v.ref)
					assert.NoError(t, err)
					assert.Equal(t, v.start, i.Start())
					assert.Equal(t, v.end, i.End())
				}
			})
		}
	})
	t.Run("GetEnd without shift by", func(t *testing.T) {
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
		t.Run("returns error when size is invalid", func(t *testing.T) {
			_, err := window.FromCustomConfig(window.SimpleConfig{
				Size:       "3g",
				ShiftBy:    "",
				Location:   "",
				TruncateTo: "",
			})
			assert.Error(t, err)
		})
		t.Run("returns error when size count is negative", func(t *testing.T) {
			_, err := window.FromCustomConfig(window.SimpleConfig{
				Size:       "-3h",
				ShiftBy:    "",
				Location:   "",
				TruncateTo: "",
			})
			assert.Error(t, err)
		})
		t.Run("returns error when shift by is invalid", func(t *testing.T) {
			_, err := window.FromCustomConfig(window.SimpleConfig{
				Size:       "1d",
				ShiftBy:    "2p",
				Location:   "",
				TruncateTo: "",
			})
			assert.Error(t, err)
		})
		t.Run("returns error when location is invalid", func(t *testing.T) {
			_, err := window.FromCustomConfig(window.SimpleConfig{
				Size:       "1d",
				ShiftBy:    "1h",
				Location:   "Unknown",
				TruncateTo: "",
			})
			assert.Error(t, err)
		})
	})
}
