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
		// no severity set → the rollout default WARNING, no production environment
		assert.Equal(t, WarningSeverity, payload.Labels[SeverityLabel])
		assert.NotContains(t, payload.Labels, EnvironmentLabel)
		// the offending run has not finished here
		assert.NotContains(t, payload.Data, "is_job_finished")
	})

	t.Run("a finished root cause marks the payload so the template can render it", func(t *testing.T) {
		payload := a.buildRootCauseBreachPayload(&scheduler.RootCauseBreachAlert{
			Team:             "proj-team",
			Project:          "p_root",
			RootCauseProject: "p_root",
			RootCauseJob:     "job_offender",
			Reason: scheduler.RootCauseReasonDetail{
				Reason:              scheduler.TierRuntimeDelay,
				Status:              scheduler.ReasonStatusActive,
				InducedDelaySeconds: 2400,
				IsJobFinished:       true,
			},
		})

		assert.Equal(t, true, payload.Data["is_job_finished"])
	})

	t.Run("a project-scoped roll-up shows the job's own delay next to the project total", func(t *testing.T) {
		payload := a.buildRootCauseBreachPayload(&scheduler.RootCauseBreachAlert{
			Team:             "proj-team",
			Project:          "p_a",
			RootCauseProject: "p_a",
			RootCauseJob:     "job_a2",
			Reason: scheduler.RootCauseReasonDetail{
				Reason:                     scheduler.TierRuntimeDelay,
				Status:                     scheduler.ReasonStatusActive,
				InducedDelaySeconds:        4800, // 80m project total, what crossed the threshold
				DelayScope:                 scheduler.DelayScopeProject,
				MaxNodeInducedDelaySeconds: 2700, // 45m, the representative job alone
			},
		})

		// the displayed delay is the job's own, not the roll-up
		assert.Equal(t, "45m0s", payload.Data["induced_delay"])
		assert.Equal(t, "project", payload.Data["induced_delay_scope"])
		assert.Equal(t, "1h20m0s", payload.Data["project_total_induced_delay"])
	})

	t.Run("a job-scoped reason keeps the measured delay with no roll-up keys", func(t *testing.T) {
		payload := a.buildRootCauseBreachPayload(&scheduler.RootCauseBreachAlert{
			Team:             "proj-team",
			Project:          "p_root",
			RootCauseProject: "p_root",
			RootCauseJob:     "job_offender",
			Reason: scheduler.RootCauseReasonDetail{
				Reason:              scheduler.TierRuntimeDelay,
				Status:              scheduler.ReasonStatusActive,
				InducedDelaySeconds: 2400,
				DelayScope:          scheduler.DelayScopeJob,
			},
		})

		assert.Equal(t, "40m0s", payload.Data["induced_delay"])
		assert.NotContains(t, payload.Data, "induced_delay_scope")
		assert.NotContains(t, payload.Data, "project_total_induced_delay")
	})

	t.Run("a legacy roll-up row without the max-node field falls back to the total", func(t *testing.T) {
		payload := a.buildRootCauseBreachPayload(&scheduler.RootCauseBreachAlert{
			Team:             "proj-team",
			Project:          "p_a",
			RootCauseProject: "p_a",
			RootCauseJob:     "job_a2",
			Reason: scheduler.RootCauseReasonDetail{
				Reason:              scheduler.TierRuntimeDelay,
				Status:              scheduler.ReasonStatusActive,
				InducedDelaySeconds: 4800,
			},
		})

		assert.Equal(t, "1h20m0s", payload.Data["induced_delay"])
		assert.NotContains(t, payload.Data, "project_total_induced_delay")
	})

	t.Run("severity follows the alert and CRITICAL marks the environment as production", func(t *testing.T) {
		payload := a.buildRootCauseBreachPayload(&scheduler.RootCauseBreachAlert{
			Team:             "streaming-team",
			Project:          "p_raw",
			RootCauseProject: "p_raw",
			RootCauseJob:     "job_raw",
			Severity:         "CRITICAL",
			Reason: scheduler.RootCauseReasonDetail{
				Reason:              scheduler.TierThirdPartyDelay,
				Status:              scheduler.ReasonStatusActive,
				InducedDelaySeconds: 1200,
				BlockedOnSensors:    []string{"wait_dex_event_delivery"},
				SourceType:          "dex",
			},
		})

		assert.Equal(t, CriticalSeverity, payload.Labels[SeverityLabel])
		assert.Equal(t, "production", payload.Labels[EnvironmentLabel])
	})

	t.Run("an unknown severity falls back to WARNING so rollout stays unpaged", func(t *testing.T) {
		payload := a.buildRootCauseBreachPayload(&scheduler.RootCauseBreachAlert{
			Team:             "streaming-team",
			Project:          "p_raw",
			RootCauseProject: "p_raw",
			RootCauseJob:     "job_raw",
			Severity:         "nonsense",
			Reason: scheduler.RootCauseReasonDetail{
				Reason: scheduler.TierThirdPartyDelay,
				Status: scheduler.ReasonStatusActive,
			},
		})

		assert.Equal(t, WarningSeverity, payload.Labels[SeverityLabel])
		assert.NotContains(t, payload.Labels, EnvironmentLabel)
	})
}
