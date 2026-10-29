package alertmanager //nolint:testpackage

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/goto/optimus/core/scheduler"
)

func TestBuildRootCauseBreachPayload(t *testing.T) {
	scheduledAt := time.Date(2026, 10, 26, 1, 0, 0, 0, time.UTC)
	a := &AlertManager{endpoint: "http://alertmanager"}

	t.Run("runtime delay alert carries the root cause identity and evidence", func(t *testing.T) {
		payload := a.buildRootCauseBreachPayload(&scheduler.RootCauseBreachAlert{
			Team:                 "proj-team",
			Project:              "p_root",
			RootCauseProject:     "p_root",
			RootCauseJob:         "job_offender",
			RootCauseScheduledAt: &scheduledAt,
			Reason: scheduler.RootCauseReasonDetail{
				Reason:              scheduler.TierRuntimeDelay,
				Status:              scheduler.ReasonStatusActive,
				InducedDelaySeconds: 2400,
			},
			ImpactedJobs: []string{"p_down/job_a", "p_down/job_b", "p_down/job_c", "p_down/job_d"},
		})

		assert.Equal(t, OptimusRootCauseBreachTemplate, payload.Template)
		assert.Equal(t, AlertTypeRootCauseBreach, payload.AlertType)
		assert.Equal(t, "p_root", payload.Project)
		assert.Equal(t, "proj-team", payload.Labels[DefaultChannelLabel])
		assert.Equal(t, WarningSeverity, payload.Labels[SeverityLabel])
		assert.Equal(t, "proj-team", payload.Data["team"])
		assert.Equal(t, "job_offender", payload.Data["root_cause_job"])
		assert.Equal(t, scheduledAt.UTC().Format(time.RFC3339), payload.Data["root_cause_scheduled_at"])
		assert.Equal(t, "RUNTIME_DELAY", payload.Data["reason"])
		assert.Equal(t, "40m0s", payload.Data["induced_delay"])
		assert.Equal(t, 4, payload.Data["impacted_jobs_count"])
		assert.Equal(t, "p_down/job_a, p_down/job_b & p_down/job_c", payload.Data["impacted_jobs_preview"])
	})

	t.Run("scheduler delay alert omits the root cause identity for global dedup", func(t *testing.T) {
		payload := a.buildRootCauseBreachPayload(&scheduler.RootCauseBreachAlert{
			Team:             "platform-team",
			Project:          "p_root",
			RootCauseProject: "p_root",
			// RootCauseJob and RootCauseScheduledAt intentionally unset
			Reason: scheduler.RootCauseReasonDetail{
				Reason:              scheduler.TierSchedulerDelay,
				Status:              scheduler.ReasonStatusActive,
				InducedDelaySeconds: 600,
			},
			ImpactedJobs: []string{"p_down/job_a"},
		})

		assert.NotContains(t, payload.Data, "root_cause_job")
		assert.NotContains(t, payload.Data, "root_cause_scheduled_at")
		assert.Equal(t, "SCHEDULER_DELAY", payload.Data["reason"])
		assert.Equal(t, "10m0s", payload.Data["induced_delay"])
	})

	t.Run("third party delay alert carries sensor evidence", func(t *testing.T) {
		payload := a.buildRootCauseBreachPayload(&scheduler.RootCauseBreachAlert{
			Team:             "streaming-team",
			Project:          "p_raw",
			RootCauseProject: "p_raw",
			RootCauseJob:     "job_raw",
			Reason: scheduler.RootCauseReasonDetail{
				Reason:              scheduler.TierThirdPartyDelay,
				Status:              scheduler.ReasonStatusActive,
				InducedDelaySeconds: 1200,
				BlockedOnSensors:    []string{"wait_dex_event_delivery"},
				SourceType:          "dex",
			},
		})

		assert.Equal(t, []string{"wait_dex_event_delivery"}, payload.Data["blocked_on_sensors"])
		assert.Equal(t, "dex", payload.Data["source_type"])
		assert.Equal(t, "streaming-team", payload.Labels[DefaultChannelLabel])
	})
}
