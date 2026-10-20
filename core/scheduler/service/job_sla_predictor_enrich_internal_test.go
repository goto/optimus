package service // nolint: testpackage

import (
	"testing"
	"time"

	"github.com/goto/salt/log"
	"github.com/stretchr/testify/assert"

	"github.com/goto/optimus/config"
	"github.com/goto/optimus/core/scheduler"
)

// enrichBreachAlerts is a pure mirror of the root cause's safe-start evidence: identification
// resolves the safe start exactly once (see effectiveSafeStart in core/scheduler/rootcause/
// delay.go), and the same value drove classification and the induced delay, so the alert shows
// exactly what the pipeline relied on — no second historical-start lookup happens here.
func TestEnrichBreachAlerts(t *testing.T) {
	referenceTime := time.Date(2026, 10, 13, 10, 0, 0, 0, time.UTC)
	scheduledAt := referenceTime.Add(-2 * time.Hour)
	reqConfig := JobSLAPredictorRequestConfig{ReferenceTime: referenceTime}

	t.Run("a running-long cause copies its already-classified safe-start evidence", func(t *testing.T) {
		safeStart := scheduledAt.Add(10 * time.Minute)
		startedAt := scheduledAt.Add(45 * time.Minute)
		induced := 90 * time.Minute
		svc := &JobSLAPredictorService{
			l:      log.NewNoop(),
			config: config.PotentialSLABreachConfig{},
		}
		alert := &scheduler.PotentialSLABreachAlert{
			RootCauseJob:         "job-C",
			Reason:               scheduler.ReasonRunningLong,
			RootCauseScheduledAt: &scheduledAt,
			Evidence: scheduler.RootCauseEvidence{
				LatestSafeStartAt: &safeStart,
				StartedAt:         &startedAt,
				InducedDelay:      induced,
			},
		}

		svc.enrichBreachAlerts([]*scheduler.PotentialSLABreachAlert{alert}, reqConfig)

		assert.Equal(t, &safeStart, alert.HistoricalStartTime)
		assert.Equal(t, &startedAt, alert.CurrentStartTime)
		assert.Equal(t, &induced, alert.TotalInducedDelay)
	})

	t.Run("a started-late cause is handled the same way", func(t *testing.T) {
		safeStart := scheduledAt.Add(10 * time.Minute)
		startedAt := scheduledAt.Add(50 * time.Minute)
		induced := 40 * time.Minute
		svc := &JobSLAPredictorService{
			l:      log.NewNoop(),
			config: config.PotentialSLABreachConfig{},
		}
		alert := &scheduler.PotentialSLABreachAlert{
			RootCauseJob:         "job-C",
			Reason:               scheduler.ReasonStartedLate,
			RootCauseScheduledAt: &scheduledAt,
			Evidence: scheduler.RootCauseEvidence{
				LatestSafeStartAt: &safeStart,
				StartedAt:         &startedAt,
				InducedDelay:      induced,
			},
		}

		svc.enrichBreachAlerts([]*scheduler.PotentialSLABreachAlert{alert}, reqConfig)

		assert.Equal(t, &safeStart, alert.HistoricalStartTime)
		assert.Equal(t, &startedAt, alert.CurrentStartTime)
		assert.Equal(t, &induced, alert.TotalInducedDelay)
	})

	t.Run("an unclassified not-started cause uses its evidence safe start, with the reference time as the current start", func(t *testing.T) {
		// identification resolved the safe start from the job's historical offset (see
		// explanationsFor's unclassified branch) and measured the still-waiting delay against it
		safeStart := scheduledAt.Add(10 * time.Minute)
		induced := referenceTime.Sub(safeStart)
		svc := &JobSLAPredictorService{
			l:      log.NewNoop(),
			config: config.PotentialSLABreachConfig{},
		}
		alert := &scheduler.PotentialSLABreachAlert{
			RootCauseJob:         "job-C",
			Reason:               scheduler.ReasonUnknown,
			Status:               scheduler.SLABreachCauseNotStarted,
			RootCauseScheduledAt: &scheduledAt,
			Evidence: scheduler.RootCauseEvidence{
				LatestSafeStartAt: &safeStart,
				InducedDelay:      induced,
			},
		}

		svc.enrichBreachAlerts([]*scheduler.PotentialSLABreachAlert{alert}, reqConfig)

		assert.Equal(t, &safeStart, alert.HistoricalStartTime)
		assert.Equal(t, &referenceTime, alert.CurrentStartTime)
		assert.Equal(t, &induced, alert.TotalInducedDelay)
	})

	t.Run("a started cause classified unknown still shows its safe-start evidence", func(t *testing.T) {
		// a STARTED_LATE demoted to UNKNOWN (no upstream explains it) keeps its genuine
		// safe-start evidence; the timing facts remain true regardless of the reason label
		safeStart := scheduledAt.Add(10 * time.Minute)
		startedAt := scheduledAt.Add(50 * time.Minute)
		induced := 40 * time.Minute
		svc := &JobSLAPredictorService{
			l:      log.NewNoop(),
			config: config.PotentialSLABreachConfig{},
		}
		alert := &scheduler.PotentialSLABreachAlert{
			RootCauseJob: "job-C",
			Reason:       scheduler.ReasonUnknown,
			Evidence: scheduler.RootCauseEvidence{
				LatestSafeStartAt: &safeStart,
				StartedAt:         &startedAt,
				InducedDelay:      induced,
			},
		}

		svc.enrichBreachAlerts([]*scheduler.PotentialSLABreachAlert{alert}, reqConfig)

		assert.Equal(t, &safeStart, alert.HistoricalStartTime)
		assert.Equal(t, &startedAt, alert.CurrentStartTime)
		assert.Equal(t, &induced, alert.TotalInducedDelay)
	})

	t.Run("causes without safe-start evidence stay untouched", func(t *testing.T) {
		for _, cause := range []*scheduler.PotentialSLABreachAlert{
			// third-party delay: the root cause job name is a sensor identity, and its delay
			// is measured against a delay-start hour, not a safe start
			{RootCauseJob: "wait_dex_orders", Reason: scheduler.ReasonThirdPartyDelay},
			// upstream failed: delay is measured from the failure time
			{RootCauseJob: "job-C", Reason: scheduler.ReasonUpstreamFailed},
			// unclassified not-started with no historical offset available
			{RootCauseJob: "job-C", Reason: scheduler.ReasonUnknown, Status: scheduler.SLABreachCauseNotStarted},
		} {
			svc := &JobSLAPredictorService{
				l:      log.NewNoop(),
				config: config.PotentialSLABreachConfig{},
			}

			svc.enrichBreachAlerts([]*scheduler.PotentialSLABreachAlert{cause}, reqConfig)

			assert.Nil(t, cause.HistoricalStartTime)
			assert.Nil(t, cause.CurrentStartTime)
			assert.Nil(t, cause.TotalInducedDelay)
		}
	})
}
