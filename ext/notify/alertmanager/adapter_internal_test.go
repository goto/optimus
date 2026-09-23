package alertmanager // nolint: testpackage

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/goto/optimus/core/scheduler"
)

func TestBuildPotentialSLABreachPayload(t *testing.T) {
	am := &AlertManager{endpoint: "http://alertmanager"}
	scheduledAt := time.Date(2026, 9, 11, 5, 0, 0, 0, time.UTC)

	startedAt := time.Date(2026, 9, 11, 5, 30, 0, 0, time.UTC)
	expectedFinish := time.Date(2026, 9, 11, 6, 45, 0, 0, time.UTC)

	t.Run("carries the deduplication keys as top-level strings", func(t *testing.T) {
		// getDedupValues only resolves top-level string values. AM keys
		// potential_sla_breach on root_cause_job, reason, and labels.team.
		payload := am.buildPotentialSLABreachPayload(&scheduler.PotentialSLABreachAlert{
			Team:                 "dwh-team",
			Project:              "proj-1",
			RootCauseJob:         "stg_payments",
			RootCauseProject:     "proj-0",
			RootCauseScheduledAt: &scheduledAt,
			Reason:               scheduler.ReasonRunningLong,
			Status:               scheduler.SLABreachCauseRunningLate,
			Severity:             CriticalSeverity,
			ImpactedJobs:         []string{"dwh_orders", "dwh_refunds"},
		})

		assert.Equal(t, "stg_payments", payload.Data["root_cause_job"])
		assert.Equal(t, "proj-0", payload.Data["root_cause_project"])
		assert.Equal(t, "2026-09-11T05:00:00Z", payload.Data["root_cause_scheduled_at"])
		assert.Equal(t, "RUNNING_LONG", payload.Data["reason"])
		assert.Equal(t, "dwh-team", payload.Labels[DefaultChannelLabel])
		assert.Equal(t, 2, payload.Data["impacted_jobs_count"])
		assert.Equal(t, "dwh_orders & dwh_refunds", payload.Data["impacted_jobs_preview"])
		assert.NotContains(t, payload.Data, "project_dashboard")
		assert.NotContains(t, payload.Data, "root_cause_dashboard")

		for _, key := range []string{"root_cause_job", "root_cause_scheduled_at", "reason"} {
			_, isString := payload.Data[key].(string)
			assert.True(t, isString, "%s must be a string for deduplication to read it", key)
		}
	})

	t.Run("routes on its own severity and marks critical as production", func(t *testing.T) {
		payload := am.buildPotentialSLABreachPayload(&scheduler.PotentialSLABreachAlert{
			Team: "dwh-team", Project: "proj-1", RootCauseJob: "stg_payments",
			RootCauseScheduledAt: &scheduledAt, Severity: CriticalSeverity,
		})

		assert.Equal(t, OptimusPotentialSLABreachTemplate, payload.Template)
		assert.Equal(t, AlertTypePotentialSLABreach, payload.AlertType)
		assert.Equal(t, CriticalSeverity, payload.Labels[SeverityLabel])
		assert.Equal(t, "production", payload.Labels[EnvironmentLabel])
		assert.Equal(t, "proj-1", payload.Project)
	})

	t.Run("defaults severity to warning and leaves environment unset", func(t *testing.T) {
		payload := am.buildPotentialSLABreachPayload(&scheduler.PotentialSLABreachAlert{
			Team: "team-b", Project: "proj-1", RootCauseJob: "stg_payments",
			RootCauseScheduledAt: &scheduledAt,
		})

		assert.Equal(t, DefaultSeverity, payload.Labels[SeverityLabel])
		_, hasEnv := payload.Labels[EnvironmentLabel]
		assert.False(t, hasEnv)
	})

	t.Run("includes only the evidence that is set", func(t *testing.T) {
		payload := am.buildPotentialSLABreachPayload(&scheduler.PotentialSLABreachAlert{
			Team: "dwh-team", Project: "proj-1", RootCauseJob: "stg_payments",
			RootCauseScheduledAt: &scheduledAt,
			Reason:               scheduler.ReasonRunningLong,
			Evidence: scheduler.RootCauseEvidence{
				StartedAt:        &startedAt,
				ExpectedFinishAt: &expectedFinish,
				InducedDelay:     75 * time.Minute,
			},
		})

		assert.Equal(t, "2026-09-11T05:30:00Z", payload.Data["started_at"])
		assert.Equal(t, "2026-09-11T06:45:00Z", payload.Data["expected_finish_at"])
		assert.Equal(t, (75 * time.Minute).String(), payload.Data["induced_delay"])
		// unset evidence must be absent rather than a zero timestamp the template would render
		assert.NotContains(t, payload.Data, "latest_safe_start_at")
		assert.NotContains(t, payload.Data, "blocked_on_sensors")
		assert.NotContains(t, payload.Data, "source_type")
	})

	t.Run("third party delay links the waiter job and omits scheduled_at", func(t *testing.T) {
		payload := am.buildPotentialSLABreachPayload(&scheduler.PotentialSLABreachAlert{
			Team:         "dwh-team",
			Project:      "dwh-project",
			RootCauseJob: "wait_dex_orders",
			Reason:       scheduler.ReasonThirdPartyDelay,
			Evidence: scheduler.RootCauseEvidence{
				BlockedOnSensors: []string{"wait_dex_orders"},
				SourceType:       "dex",
			},
		})

		assert.Equal(t, "THIRD_PARTY_DELAY", payload.Data["reason"])
		assert.Equal(t, "dex", payload.Data["source_type"])
		assert.Equal(t, []string{"wait_dex_orders"}, payload.Data["blocked_on_sensors"])
		assert.Equal(t, "wait_dex_orders", payload.Data["root_cause_job"])
		assert.NotContains(t, payload.Data, "root_cause_scheduled_at")
		assert.NotContains(t, payload.Data, "induced_delay")
	})

	t.Run("impacted jobs preview names at most 3 jobs, joined with an ampersand", func(t *testing.T) {
		payload := am.buildPotentialSLABreachPayload(&scheduler.PotentialSLABreachAlert{
			Team: "dwh-team", Project: "proj-1", RootCauseJob: "stg_payments",
			ImpactedJobs: []string{"job_a", "job_b", "job_c", "job_d"},
		})

		assert.Equal(t, "job_a, job_b & job_c", payload.Data["impacted_jobs_preview"])
		assert.Equal(t, 4, payload.Data["impacted_jobs_count"])
	})
}
