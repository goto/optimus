//go:build unit_test

package scheduler_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/goto/optimus/core/scheduler"
)

func TestBreachAlertAggregator(t *testing.T) {
	scheduledAt := time.Date(2026, 9, 11, 5, 0, 0, 0, time.UTC)

	cause := func(name string, reason scheduler.RootCauseReason, at time.Time) *scheduler.JobState {
		return &scheduler.JobState{
			JobName: scheduler.JobName(name),
			JobRun:  scheduler.JobRunSummary{ScheduledAt: at},
			Reason:  reason,
			Status:  scheduler.SLABreachCauseRunningLate,
		}
	}

	t.Run("one root cause threatening several jobs is a single alert", func(t *testing.T) {
		agg := scheduler.NewBreachAlertAggregator()
		stg := cause("stg_payments", scheduler.ReasonRunningLong, scheduledAt)

		agg.Add("dwh-team", "dwh_orders", stg, "proj", "CRITICAL")
		agg.Add("dwh-team", "dwh_refunds", stg, "proj", "CRITICAL")

		alerts := agg.Build()

		assert.Len(t, alerts, 1)
		assert.Equal(t, []string{"dwh_orders", "dwh_refunds"}, alerts[0].ImpactedJobs)
		assert.Equal(t, "stg_payments", alerts[0].RootCauseJob)
	})

	t.Run("the same job twice does not duplicate in the body", func(t *testing.T) {
		agg := scheduler.NewBreachAlertAggregator()
		stg := cause("stg_payments", scheduler.ReasonRunningLong, scheduledAt)

		agg.Add("dwh-team", "dwh_orders", stg, "proj", "CRITICAL")
		agg.Add("dwh-team", "dwh_orders", stg, "proj", "CRITICAL")

		assert.Equal(t, []string{"dwh_orders"}, agg.Build()[0].ImpactedJobs)
	})

	t.Run("each impacted team gets its own alert for the same root cause", func(t *testing.T) {
		agg := scheduler.NewBreachAlertAggregator()
		stg := cause("stg_payments", scheduler.ReasonRunningLong, scheduledAt)

		agg.Add("dwh-team", "dwh_orders", stg, "proj", "CRITICAL")
		agg.Add("finance-team", "fin_ledger", stg, "proj", "CRITICAL")

		alerts := agg.Build()

		assert.Len(t, alerts, 2)
		assert.Equal(t, "dwh-team", alerts[0].Team)
		assert.Equal(t, "finance-team", alerts[1].Team)
	})

	t.Run("a different reason is a different alert", func(t *testing.T) {
		// reason is part of the dedup key: a job that was waiting and is now
		// overrunning is a new situation, not a repeat of the old one
		agg := scheduler.NewBreachAlertAggregator()

		agg.Add("dwh-team", "dwh_orders", cause("stg_payments", scheduler.ReasonThirdPartyDelay, scheduledAt), "proj", "CRITICAL")
		agg.Add("dwh-team", "dwh_orders", cause("stg_payments", scheduler.ReasonRunningLong, scheduledAt), "proj", "CRITICAL")

		assert.Len(t, agg.Build(), 2)
	})

	t.Run("the next run of the same job is a different alert", func(t *testing.T) {
		// scheduled_at in the key keeps consecutive runs from merging into one incident
		agg := scheduler.NewBreachAlertAggregator()

		agg.Add("dwh-team", "dwh_orders", cause("stg_payments", scheduler.ReasonRunningLong, scheduledAt), "proj", "CRITICAL")
		agg.Add("dwh-team", "dwh_orders", cause("stg_payments", scheduler.ReasonRunningLong, scheduledAt.Add(24*time.Hour)), "proj", "CRITICAL")

		assert.Len(t, agg.Build(), 2)
	})
}
