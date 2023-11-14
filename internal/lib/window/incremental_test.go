package window_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/goto/optimus/internal/lib/window"
)

func TestIncrementalWindow(t *testing.T) {
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
			_, err = w.GetInterval(sept1)
			assert.NoError(t, err)
			//assert.Equal(t, "2023-09-01T00:00:00Z", interval.Start.Format(time.RFC3339))
			//assert.Equal(t, "2023-10-01T00:00:00Z", interval.End.Format(time.RFC3339))
		})
	})
}
