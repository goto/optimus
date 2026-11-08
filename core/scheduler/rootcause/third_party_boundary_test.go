//go:build unit_test

package rootcause_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/goto/optimus/core/scheduler/rootcause"
)

func TestThirdPartyDailyBoundary(t *testing.T) {
	ref := time.Date(2026, 9, 10, 1, 30, 0, 0, time.UTC)

	t.Run("before the start hour falls back to the previous day", func(t *testing.T) {
		got := rootcause.ThirdPartyDailyBoundary(ref, 18)
		assert.Equal(t, time.Date(2026, 9, 9, 18, 0, 0, 0, time.UTC), got)
	})

	t.Run("at or after the start hour stays on the same day", func(t *testing.T) {
		got := rootcause.ThirdPartyDailyBoundary(ref.Add(17*time.Hour), 18) // 18:30 UTC
		assert.Equal(t, time.Date(2026, 9, 10, 18, 0, 0, 0, time.UTC), got)
	})

	t.Run("hour zero is midnight of the same day", func(t *testing.T) {
		got := rootcause.ThirdPartyDailyBoundary(ref, 0)
		assert.Equal(t, time.Date(2026, 9, 10, 0, 0, 0, 0, time.UTC), got)
	})

	t.Run("out-of-range hours fall back to midnight", func(t *testing.T) {
		got := rootcause.ThirdPartyDailyBoundary(ref, 24)
		assert.Equal(t, time.Date(2026, 9, 10, 0, 0, 0, 0, time.UTC), got)
	})

	t.Run("normalizes non-UTC input to UTC", func(t *testing.T) {
		jkt := time.Date(2026, 9, 10, 8, 30, 0, 0, time.FixedZone("WIB", 7*3600)) // 01:30 UTC
		got := rootcause.ThirdPartyDailyBoundary(jkt, 18)
		assert.Equal(t, time.Date(2026, 9, 9, 18, 0, 0, 0, time.UTC), got)
	})
}
