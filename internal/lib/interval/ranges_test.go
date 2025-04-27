package interval_test

import (
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/goto/optimus/internal/lib/duration"
	"github.com/goto/optimus/internal/lib/interval"
	"github.com/goto/optimus/internal/lib/window"
)

func TestRanges(t *testing.T) {
	scheduledAt1 := time.Date(2025, 3, 18, 8, 0, 0, 0, time.UTC)
	scheduledAt2 := time.Date(2025, 3, 18, 10, 0, 0, 0, time.UTC)

	day0 := duration.NewDuration(0, duration.Day)
	day1 := duration.NewDuration(1, duration.Day)
	day2 := duration.NewDuration(2, duration.Day)
	day7 := duration.NewDuration(7, duration.Day)
	week1 := duration.NewDuration(1, duration.Week)
	month1 := duration.NewDuration(1, duration.Month)

	yesterday := window.NewCustomWindow(day1, day0, time.UTC, "")
	last2Days := window.NewCustomWindow(day2, day0, time.UTC, "")
	odd2Days := window.NewCustomWindow(day2, day1, time.UTC, "")
	last6Days := window.NewCustomWindow(day7, day1, time.UTC, "")
	lastWeek := window.NewCustomWindow(week1, day0, time.UTC, "")
	lastMonth := window.NewCustomWindow(month1, day0, time.UTC, "")

	t.Run("works for single item range", func(t *testing.T) {
		windows := []window.CustomWindow{yesterday, last2Days, odd2Days, last6Days, lastWeek, lastMonth}
		for _, w1 := range windows {
			expected := createRange([]time.Time{scheduledAt1}, w1, "pending")
			actual := createRange([]time.Time{scheduledAt2}, w1, "success")

			updated := expected.UpdateDataFrom(actual)
			values := updated.Values()
			assert.Equal(t, "success", values[0])
		}
	})
	t.Run("works when actual runs are more", func(t *testing.T) {
		schedules := []time.Time{}
		for i := 1; i < 8; i++ {
			t1 := time.Date(2025, 3, 16+i, 8, 30, 0, 0, time.UTC)
			schedules = append(schedules, t1)
		}
		expected := createRange(schedules, yesterday, "pending")
		schedules = append(schedules,
			time.Date(2025, 3, 15, 10, 0, 0, 0, time.UTC),
			time.Date(2025, 3, 12, 10, 0, 0, 0, time.UTC),
			time.Date(2025, 3, 13, 10, 0, 0, 0, time.UTC),
			time.Date(2025, 3, 26, 10, 0, 0, 0, time.UTC),
		)
		slices.SortFunc(schedules, sortTime)
		actual := createRange(schedules, yesterday, "success")

		updated := expected.UpdateDataFrom(actual)
		values := updated.Values()
		assert.Equal(t, len(values), 7)
		assert.Equal(t, "success", values[0])
	})
	t.Run("works when expected runs are more", func(t *testing.T) {
		schedules := []time.Time{}
		for i := 1; i < 7; i++ {
			t1 := time.Date(2025, 3, 16+i, 8, 30, 0, 0, time.UTC)
			schedules = append(schedules, t1)
		}
		actual := createRange(schedules, yesterday, "success")
		schedules = append(schedules,
			time.Date(2025, 3, 23, 10, 0, 0, 0, time.UTC),
		)
		expected := createRange(schedules, yesterday, "pending")

		updated := expected.UpdateDataFrom(actual)
		values := updated.Values()
		assert.Equal(t, len(values), 7)
		assert.Equal(t, "success", values[0])
		assert.Equal(t, "pending", values[6])
	})
	t.Run("works when actual runs are less", func(t *testing.T) {
		schedules := []time.Time{}
		for i := 1; i < 5; i++ {
			t1 := time.Date(2025, 3, 16+i, 8, 30, 0, 0, time.UTC)
			schedules = append(schedules, t1)
		}
		actual := createRange(schedules, yesterday, "success")
		schedules = append(schedules,
			time.Date(2025, 3, 14, 10, 0, 0, 0, time.UTC),
			time.Date(2025, 3, 15, 10, 0, 0, 0, time.UTC),
			time.Date(2025, 3, 16, 10, 0, 0, 0, time.UTC),
		)
		slices.SortFunc(schedules, sortTime)
		expected := createRange(schedules, yesterday, "pending")

		updated := expected.UpdateDataFrom(actual)
		values := updated.Values()
		assert.Equal(t, len(values), 7)
		assert.Equal(t, "pending", values[0])
		assert.Equal(t, "pending", values[1])
		assert.Equal(t, "pending", values[2])
		assert.Equal(t, "success", values[3])
		assert.Equal(t, "success", values[6])
	})
	t.Run("works when actual runs are less with odd window", func(t *testing.T) {
		schedules := []time.Time{}
		for i := 1; i < 5; i++ {
			t1 := time.Date(2025, 3, 16+i, 8, 30, 0, 0, time.UTC)
			schedules = append(schedules, t1)
		}
		actual := createRange(schedules, odd2Days, "success")
		schedules = append(schedules,
			time.Date(2025, 3, 14, 10, 0, 0, 0, time.UTC),
			time.Date(2025, 3, 15, 10, 0, 0, 0, time.UTC),
			time.Date(2025, 3, 16, 10, 0, 0, 0, time.UTC),
		)
		slices.SortFunc(schedules, sortTime)
		expected := createRange(schedules, odd2Days, "pending")

		updated := expected.UpdateDataFrom(actual)
		values := updated.Values()
		assert.Equal(t, len(values), 7)
		assert.Equal(t, "pending", values[0])
		assert.Equal(t, "pending", values[1])
		assert.Equal(t, "pending", values[2])
		assert.Equal(t, "success", values[3])
		assert.Equal(t, "success", values[6])
	})
}

func createRange[V any](schedules []time.Time, cw window.CustomWindow, v V) interval.Range[V] {
	r1 := interval.Range[V]{}
	for _, s := range schedules {
		intr, err := cw.GetInterval(s)
		if err != nil {
			fmt.Println(err)
		} else {
			r1 = append(r1, interval.Data[V]{
				Data: v,
				In:   intr,
			})
		}
	}
	return r1
}

func sortTime(a, b time.Time) int {
	if a.Equal(b) {
		return 0
	}
	if a.After(b) {
		return 1
	} else {
		return -1
	}
}
