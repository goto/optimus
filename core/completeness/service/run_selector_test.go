package service_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/goto/optimus/core/completeness/service"
	"github.com/goto/optimus/internal/lib/cron"
)

func mustParseCron(t *testing.T, interval string) *cron.ScheduleSpec {
	t.Helper()
	s, err := cron.ParseCronSchedule(interval)
	require.NoError(t, err)
	return s
}

func atJKT(t *testing.T, s string) time.Time {
	t.Helper()
	parsed, err := time.ParseInLocation("2006-01-02 15:04:05", s, service.JKT)
	require.NoError(t, err)
	return parsed
}

func TestSelectScheduledAt(t *testing.T) {
	t.Run("sub-daily: mid-slot reports the previous settled run, not the in-flight one", func(t *testing.T) {
		hourly := mustParseCron(t, "0 * * * *")
		now := atJKT(t, "2026-09-02 02:30:00")

		got, hasSchedule := service.SelectScheduledAt(hourly, now)

		assert.True(t, hasSchedule)
		assert.Equal(t, atJKT(t, "2026-09-02 01:00:00"), got)
	})

	t.Run("sub-daily: exactly on the hour boundary follows cron.Prev's strict semantics", func(t *testing.T) {
		// cron.ScheduleSpec.Prev is strict (occurrence *before* the given instant, never
		// equal to it -- see internal/lib/cron/cron.go), consistent with how the rest of
		// this codebase already uses it (Schedule.GetPreviousSchedule etc.). At the exact
		// boundary this means the slot that "just fired" isn't counted as fired yet, so
		// double-Prev lands one slot earlier than the mid-slot case below. Real request
		// timestamps are never exactly on a cron boundary to the nanosecond, so this is a
		// documented edge case, not a behavior anyone is expected to rely on.
		hourly := mustParseCron(t, "0 * * * *")
		now := atJKT(t, "2026-09-02 02:00:00")

		got, hasSchedule := service.SelectScheduledAt(hourly, now)

		assert.True(t, hasSchedule)
		assert.Equal(t, atJKT(t, "2026-09-02 00:00:00"), got)
	})

	t.Run("sub-daily: every 6 hours behaves the same as hourly", func(t *testing.T) {
		every6h := mustParseCron(t, "0 */6 * * *")
		now := atJKT(t, "2026-09-02 13:15:00") // slots at 00,06,12,18

		got, hasSchedule := service.SelectScheduledAt(every6h, now)

		assert.True(t, hasSchedule)
		assert.Equal(t, atJKT(t, "2026-09-02 06:00:00"), got)
	})

	t.Run("daily: before today's scheduled time reports no schedule yet", func(t *testing.T) {
		daily1AM := mustParseCron(t, "0 1 * * *")
		now := atJKT(t, "2026-09-02 00:30:00")

		_, hasSchedule := service.SelectScheduledAt(daily1AM, now)

		assert.False(t, hasSchedule)
	})

	t.Run("daily: after today's scheduled time reports today's occurrence", func(t *testing.T) {
		daily1AM := mustParseCron(t, "0 1 * * *")
		now := atJKT(t, "2026-09-02 09:00:00")

		got, hasSchedule := service.SelectScheduledAt(daily1AM, now)

		assert.True(t, hasSchedule)
		assert.Equal(t, atJKT(t, "2026-09-02 01:00:00"), got)
	})

	t.Run("weekly: on the scheduled day, before scheduled time reports no schedule yet", func(t *testing.T) {
		// 2026-09-02 is a Wednesday.
		weeklyWed1AM := mustParseCron(t, "0 1 * * 3")
		now := atJKT(t, "2026-09-02 00:30:00")

		_, hasSchedule := service.SelectScheduledAt(weeklyWed1AM, now)

		assert.False(t, hasSchedule)
	})

	t.Run("weekly: on the scheduled day, after scheduled time reports today's occurrence", func(t *testing.T) {
		weeklyWed1AM := mustParseCron(t, "0 1 * * 3")
		now := atJKT(t, "2026-09-02 09:00:00")

		got, hasSchedule := service.SelectScheduledAt(weeklyWed1AM, now)

		assert.True(t, hasSchedule)
		assert.Equal(t, atJKT(t, "2026-09-02 01:00:00"), got)
	})

	t.Run("weekly: on a non-scheduled day falls back to the last occurrence", func(t *testing.T) {
		weeklyWed1AM := mustParseCron(t, "0 1 * * 3")
		now := atJKT(t, "2026-09-04 12:00:00") // Friday

		got, hasSchedule := service.SelectScheduledAt(weeklyWed1AM, now)

		assert.True(t, hasSchedule)
		assert.Equal(t, atJKT(t, "2026-09-02 01:00:00"), got) // last Wednesday
	})

	t.Run("irregular weekday-only: weekend falls back to Friday's occurrence like weekly+", func(t *testing.T) {
		weekdays9AM := mustParseCron(t, "0 9 * * 1-5")
		now := atJKT(t, "2026-09-05 12:00:00") // Saturday

		got, hasSchedule := service.SelectScheduledAt(weekdays9AM, now)

		assert.True(t, hasSchedule)
		assert.Equal(t, atJKT(t, "2026-09-04 09:00:00"), got) // Friday
	})

	t.Run("irregular weekday-only: on a weekday behaves like daily", func(t *testing.T) {
		weekdays9AM := mustParseCron(t, "0 9 * * 1-5")
		now := atJKT(t, "2026-09-04 08:00:00") // Friday, before 9 AM

		_, hasSchedule := service.SelectScheduledAt(weekdays9AM, now)

		assert.False(t, hasSchedule)
	})
}
