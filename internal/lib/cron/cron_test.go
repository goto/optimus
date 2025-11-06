package cron_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/goto/optimus/internal/lib/cron"
)

func TestScheduleSpec(t *testing.T) {
	t.Run("Prev", func(t *testing.T) {
		t.Run("with constant interval", func(t *testing.T) {
			scheduleSpec, err := cron.ParseCronSchedule("@midnight")
			assert.Nil(t, err)
			scheduleStartTime, _ := time.Parse(time.RFC3339, "2022-03-25T02:00:00+00:00")
			prevScheduleTime := scheduleSpec.Prev(scheduleStartTime)
			expectedTime, _ := time.Parse(time.RFC3339, "2022-03-25T00:00:00+00:00")
			assert.Equal(t, prevScheduleTime, expectedTime)
		})
		t.Run("with varying interval", func(t *testing.T) {
			// at 2 AM every month on 2,11,19,26
			scheduleSpec, err := cron.ParseCronSchedule("0 2 2,11,19,26 * *")
			assert.Nil(t, err)

			scheduleStartTime, _ := time.Parse(time.RFC3339, "2022-03-19T01:59:59+00:00")
			prevScheduleTime := scheduleSpec.Prev(scheduleStartTime)
			expectedTime, _ := time.Parse(time.RFC3339, "2022-03-11T02:00:00+00:00")
			assert.Equal(t, prevScheduleTime, expectedTime)
		})
		t.Run("with time falling on schedule time", func(t *testing.T) {
			scheduleSpec, err := cron.ParseCronSchedule("@monthly")
			assert.Nil(t, err)

			scheduleStartTime, _ := time.Parse(time.RFC3339, "2022-03-01T00:00:00+00:00")
			prevScheduleTime := scheduleSpec.Prev(scheduleStartTime)
			expectedTime, _ := time.Parse(time.RFC3339, "2022-02-01T00:00:00+00:00")
			assert.Equal(t, prevScheduleTime, expectedTime)
		})
	})
	t.Run("Next", func(t *testing.T) {
		t.Run("with constant interval", func(t *testing.T) {
			scheduleSpec, err := cron.ParseCronSchedule("@midnight")
			assert.Nil(t, err)
			scheduleStartTime, _ := time.Parse(time.RFC3339, "2022-03-25T02:00:00+00:00")
			prevScheduleTime := scheduleSpec.Next(scheduleStartTime)
			expectedTime, _ := time.Parse(time.RFC3339, "2022-03-26T00:00:00+00:00")
			assert.Equal(t, prevScheduleTime, expectedTime)
		})
		t.Run("with varying interval", func(t *testing.T) {
			// at 2 AM every month on 2,11,19,26
			scheduleSpec, err := cron.ParseCronSchedule("0 2 2,11,19,26 * *")
			assert.Nil(t, err)

			scheduleStartTime, _ := time.Parse(time.RFC3339, "2022-03-19T02:01:59+00:00")
			prevScheduleTime := scheduleSpec.Next(scheduleStartTime)
			expectedTime, _ := time.Parse(time.RFC3339, "2022-03-26T02:00:00+00:00")
			assert.Equal(t, prevScheduleTime, expectedTime)
		})
		t.Run("with current time falling on schedule time", func(t *testing.T) {
			scheduleSpec, err := cron.ParseCronSchedule("@monthly")
			assert.Nil(t, err)

			scheduleStartTime, _ := time.Parse(time.RFC3339, "2022-03-01T00:00:00+00:00")
			prevScheduleTime := scheduleSpec.Next(scheduleStartTime)
			expectedTime, _ := time.Parse(time.RFC3339, "2022-04-01T00:00:00+00:00")
			assert.Equal(t, prevScheduleTime, expectedTime)
		})
	})
	t.Run("IsSubDaily", func(t *testing.T) {
		t.Run("should return true for sub-daily schedules", func(t *testing.T) {
			testCases := []struct {
				name     string
				cronExpr string
			}{
				{"every hour", "0 * * * *"},
				{"every 30 minutes", "*/30 * * * *"},
				{"every 6 hours", "0 */6 * * *"},
				{"every 2 hours", "0 0-23/2 * * *"},
				{"every 15 minutes", "*/15 * * * *"},
				{"every 3 hours", "0 */3 * * *"},
			}

			for _, tc := range testCases {
				t.Run(tc.name, func(t *testing.T) {
					scheduleSpec, err := cron.ParseCronSchedule(tc.cronExpr)
					assert.Nil(t, err)
					assert.True(t, scheduleSpec.IsSubDaily())
				})
			}
		})
		t.Run("should return false for daily or longer schedules", func(t *testing.T) {
			testCases := []struct {
				name     string
				cronExpr string
			}{
				{"daily at 2 AM", "0 2 * * *"},
				{"daily at 12 AM", "0 0 * * *"},
				{"2 days a week", "0 0 * * 1,3"},
				{"15th day every month", "0 0 15 * *"},
				// nonstandard descriptors
				{"daily at midnight", "@daily"},
				{"weekly", "@weekly"},
				{"monthly", "@monthly"},
				{"yearly", "@yearly"},
			}

			for _, tc := range testCases {
				t.Run(tc.name, func(t *testing.T) {
					scheduleSpec, err := cron.ParseCronSchedule(tc.cronExpr)
					assert.Nil(t, err)
					assert.False(t, scheduleSpec.IsSubDaily())
				})
			}
		})
	})
}
