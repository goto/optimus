package scheduler_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/goto/optimus/core/scheduler"
)

func TestIsCustomBackfillRunID(t *testing.T) {
	t.Run("should detect dag run id created by a custom backfill", func(t *testing.T) {
		dagRunID := "custom-backfill_9f7b0d4e-6b3a-4c11-8f2e-1d2b3c4d5e6f__2026-10-19"
		assert.True(t, scheduler.IsCustomBackfillRunID(dagRunID))
	})

	t.Run("should not detect scheduled or manual runs as custom backfill", func(t *testing.T) {
		assert.False(t, scheduler.IsCustomBackfillRunID("scheduled__2026-10-19T10:00:00+00:00"))
		assert.False(t, scheduler.IsCustomBackfillRunID("manual__my-custom-run"))
		assert.False(t, scheduler.IsCustomBackfillRunID(""))
	})

	t.Run("should not detect ids that merely contain the prefix without the id separator", func(t *testing.T) {
		assert.False(t, scheduler.IsCustomBackfillRunID("custom-backfill"))
		assert.False(t, scheduler.IsCustomBackfillRunID("custom-backfill-something__2026-10-19"))
	})
}

func TestIsBackfillRun(t *testing.T) {
	customBackfillRunID := "custom-backfill_9f7b0d4e-6b3a-4c11-8f2e-1d2b3c4d5e6f__2026-10-19"
	scheduledRunID := "scheduled__2026-10-19T10:00:00+00:00"

	t.Run("should detect custom backfill runs regardless of the scheduled time", func(t *testing.T) {
		// custom backfills carry a future scheduled_at, which is exactly why the marker matters
		assert.True(t, scheduler.IsBackfillRun(customBackfillRunID, time.Now().Add(time.Hour), 12))
		assert.True(t, scheduler.IsBackfillRun(customBackfillRunID, time.Time{}, 12))
	})

	t.Run("should detect runs scheduled outside the lookback period", func(t *testing.T) {
		assert.True(t, scheduler.IsBackfillRun(scheduledRunID, time.Now().Add(-13*time.Hour), 12))
	})

	t.Run("should not detect recent runs as backfill", func(t *testing.T) {
		assert.False(t, scheduler.IsBackfillRun(scheduledRunID, time.Now().Add(-time.Hour), 12))
		assert.False(t, scheduler.IsBackfillRun(scheduledRunID, time.Now().Add(time.Hour), 12))
	})

	t.Run("should not treat a missing scheduled time as a signal", func(t *testing.T) {
		assert.False(t, scheduler.IsBackfillRun(scheduledRunID, time.Time{}, 12))
	})
}
