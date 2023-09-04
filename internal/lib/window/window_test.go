package window_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/goto/optimus/internal/lib/window"
	"github.com/goto/optimus/internal/models"
)

func TestWindow(t *testing.T) {
	t.Run("FromSchedule", func(t *testing.T) {
		t.Run("returns error when schedule not valid", func(t *testing.T) {
			schedule := "* * * *"
			_, err := window.FromSchedule(schedule)
			assert.Error(t, err)
		})
		t.Run("returns interval when schedule valid", func(t *testing.T) {
			schedule := "0 0 1 * *"
			w, err := window.FromSchedule(schedule)
			assert.NoError(t, err)

			sept1 := time.Date(2023, 9, 1, 1, 0, 0, 0, time.UTC)
			interval, err := w.GetInterval(sept1)
			assert.NoError(t, err)
			assert.Equal(t, "2023-09-01T00:00:00Z", interval.Start.Format(time.RFC3339))
			assert.Equal(t, "2023-10-01T00:00:00Z", interval.End.Format(time.RFC3339))
		})
	})

	t.Run("FromBaseWindow", func(t *testing.T) {
		t.Run("returns error when unable to get end of window", func(t *testing.T) {
			w1, _ := models.NewWindow(2, "Z", "0", "24h")
			baseWindow := window.FromBaseWindow(w1)

			sept1 := time.Date(2023, 9, 1, 1, 0, 0, 0, time.UTC)
			_, err := baseWindow.GetInterval(sept1)
			assert.Error(t, err)
			assert.ErrorContains(t, err, "error validating truncate_to")
		})
		t.Run("returns error when unable to get start of window", func(t *testing.T) {
			w1, _ := models.NewWindow(2, "M", "0", "bM")
			baseWindow := window.FromBaseWindow(w1)

			sept1 := time.Date(2023, 9, 1, 1, 0, 0, 0, time.UTC)
			_, err := baseWindow.GetInterval(sept1)
			assert.Error(t, err)
			assert.ErrorContains(t, err, "error validating size")
		})
		t.Run("returns the interval for window", func(t *testing.T) {
			w1, _ := models.NewWindow(2, "d", "0", "24h")
			baseWindow := window.FromBaseWindow(w1)

			sept1 := time.Date(2023, 9, 1, 1, 0, 0, 0, time.UTC)
			interval, err := baseWindow.GetInterval(sept1)
			assert.NoError(t, err)
			assert.Equal(t, "2023-08-31T00:00:00Z", interval.Start.Format(time.RFC3339))
			assert.Equal(t, "2023-09-01T00:00:00Z", interval.End.Format(time.RFC3339))
		})
		t.Run("should return zero and error if error parsing schedule", func(t *testing.T) {
			schedule := "-1 * * * *"

			actualWindow, actualError := window.FromSchedule(schedule)

			assert.Zero(t, actualWindow)
			assert.Error(t, actualError)
			assert.ErrorContains(t, actualError, "unable to parse job cron interval")
		})
		t.Run("should return window and nil if no error is encountered", func(t *testing.T) {
			schedule := "0 * * * *"

			actualWindow, actualError := window.FromSchedule(schedule)

			assert.NotZero(t, actualWindow)
			assert.NoError(t, actualError)
		})
	})

	t.Run("GetInterval", func(t *testing.T) {
		const (
			version    = 2
			truncateTo = "d"
			offset     = "-24h"
			size       = "24h"
		)

		referenceTime := time.Now()

		t.Run("should return zero and error if error when getting start time", func(t *testing.T) {
			invalidSize := "24"

			baseWindow, err := models.NewWindow(version, truncateTo, offset, invalidSize)
			assert.NotNil(t, baseWindow)
			assert.NoError(t, err)

			w := window.FromBaseWindow(baseWindow)

			actualInterval, actualError := w.GetInterval(referenceTime)

			assert.Zero(t, actualInterval)
			assert.Error(t, actualError)
			assert.ErrorContains(t, actualError, "missing unit in duration")
		})

		t.Run("should return interval and nil if no error is encountered", func(t *testing.T) {
			baseWindow, err := models.NewWindow(version, truncateTo, offset, size)
			assert.NotNil(t, baseWindow)
			assert.NoError(t, err)

			w := window.FromBaseWindow(baseWindow)

			actualInterval, actualError := w.GetInterval(referenceTime)

			assert.NotZero(t, actualInterval)
			assert.NoError(t, actualError)
		})
	})
}
