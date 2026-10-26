package scheduler_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/goto/optimus/core/scheduler"
	"github.com/goto/optimus/core/tenant"
)

func incidentKey(project, job string, scheduledAt time.Time) scheduler.SLABreachIncidentKey {
	return scheduler.SLABreachIncidentKey{
		ProjectName: tenant.ProjectName(project),
		JobName:     scheduler.JobName(job),
		ScheduledAt: scheduledAt,
	}
}

func TestGroupDetections(t *testing.T) {
	baseTime := time.Date(2026, 10, 22, 1, 0, 0, 0, time.UTC)

	t.Run("groups detections from different tiers into one incident per key", func(t *testing.T) {
		key := incidentKey("p_proj", "job_root", baseTime)
		detections := []scheduler.RootCauseDetection{
			{
				IncidentKey: key,
				Reason: scheduler.RootCauseReasonDetail{
					Reason:              scheduler.TierRuntimeDelay,
					Status:              scheduler.ReasonStatusActive,
					InducedDelaySeconds: 40 * 60,
				},
				ImpactedJobs: []scheduler.ImpactedJob{
					{ProjectName: "p_down", JobName: "job_down", ScheduledAt: baseTime},
				},
			},
			{
				IncidentKey: key,
				Reason: scheduler.RootCauseReasonDetail{
					Reason:              scheduler.TierThirdPartyDelay,
					Status:              scheduler.ReasonStatusActive,
					InducedDelaySeconds: 25 * 60,
					BlockedOnSensors:    []string{"wait_dex_event_delivery"},
					SourceType:          "dex",
				},
			},
		}

		incidents := scheduler.GroupDetections(detections)

		assert.Len(t, incidents, 1)
		incident := incidents[0].Incident
		assert.Equal(t, key, incident.Key)
		assert.Len(t, incident.Details.Reasons, 2)
		assert.True(t, incident.HasActiveReason())
		assert.Equal(t, 40*60, incident.MaxInducedDelaySeconds)
		assert.Len(t, incidents[0].Impacts, 1)
		assert.Equal(t, scheduler.JobName("job_down"), incidents[0].Impacts[0].JobName)
	})

	t.Run("merges duplicate reasons keeping max induced delay and freshest evidence", func(t *testing.T) {
		key := incidentKey("p_proj", "job_root", baseTime)
		detections := []scheduler.RootCauseDetection{
			{
				IncidentKey: key,
				Reason: scheduler.RootCauseReasonDetail{
					Reason:              scheduler.TierRuntimeDelay,
					Status:              scheduler.ReasonStatusActive,
					InducedDelaySeconds: 31 * 60,
				},
			},
			{
				IncidentKey: key,
				Reason: scheduler.RootCauseReasonDetail{
					Reason:              scheduler.TierRuntimeDelay,
					Status:              scheduler.ReasonStatusActive,
					InducedDelaySeconds: 45 * 60,
				},
				ImpactedJobs: []scheduler.ImpactedJob{
					{ProjectName: "p_down", JobName: "job_a", ScheduledAt: baseTime},
				},
			},
		}

		incidents := scheduler.GroupDetections(detections)

		assert.Len(t, incidents, 1)
		reasons := incidents[0].Incident.Details.Reasons
		assert.Len(t, reasons, 1)
		assert.Equal(t, 45*60, reasons[0].InducedDelaySeconds)
		assert.Equal(t, 45*60, incidents[0].Incident.MaxInducedDelaySeconds)
		assert.Len(t, incidents[0].Impacts, 1)
	})

	t.Run("deduplicates impacted jobs across detections", func(t *testing.T) {
		key := incidentKey("p_proj", "job_root", baseTime)
		impacted := scheduler.ImpactedJob{ProjectName: "p_down", JobName: "job_a", ScheduledAt: baseTime}
		detections := []scheduler.RootCauseDetection{
			{IncidentKey: key, Reason: scheduler.RootCauseReasonDetail{Reason: scheduler.TierRuntimeDelay, Status: scheduler.ReasonStatusActive}, ImpactedJobs: []scheduler.ImpactedJob{impacted}},
			{IncidentKey: key, Reason: scheduler.RootCauseReasonDetail{Reason: scheduler.TierSchedulerDelay, Status: scheduler.ReasonStatusActive}, ImpactedJobs: []scheduler.ImpactedJob{impacted}},
		}

		incidents := scheduler.GroupDetections(detections)

		assert.Len(t, incidents, 1)
		assert.Len(t, incidents[0].Impacts, 1)
	})

	t.Run("max induced delay is a high-water mark including resolved reasons", func(t *testing.T) {
		key := incidentKey("p_proj", "job_root", baseTime)
		detections := []scheduler.RootCauseDetection{
			{
				IncidentKey: key,
				Reason: scheduler.RootCauseReasonDetail{
					Reason:              scheduler.TierRuntimeDelay,
					Status:              scheduler.ReasonStatusResolved,
					InducedDelaySeconds: 90 * 60,
				},
			},
			{
				IncidentKey: key,
				Reason: scheduler.RootCauseReasonDetail{
					Reason:              scheduler.TierThirdPartyDelay,
					Status:              scheduler.ReasonStatusActive,
					InducedDelaySeconds: 15 * 60,
				},
			},
		}

		incidents := scheduler.GroupDetections(detections)

		assert.Len(t, incidents, 1)
		// the resolved reason still counts toward the peak severity
		assert.Equal(t, 90*60, incidents[0].Incident.MaxInducedDelaySeconds)
	})

	t.Run("orders incidents deterministically by key", func(t *testing.T) {
		later := baseTime.Add(time.Hour)
		detections := []scheduler.RootCauseDetection{
			{IncidentKey: incidentKey("p_b", "job_x", baseTime), Reason: scheduler.RootCauseReasonDetail{Reason: scheduler.TierRuntimeDelay, Status: scheduler.ReasonStatusActive}},
			{IncidentKey: incidentKey("p_a", "job_z", later), Reason: scheduler.RootCauseReasonDetail{Reason: scheduler.TierRuntimeDelay, Status: scheduler.ReasonStatusActive}},
			{IncidentKey: incidentKey("p_a", "job_a", baseTime), Reason: scheduler.RootCauseReasonDetail{Reason: scheduler.TierRuntimeDelay, Status: scheduler.ReasonStatusActive}},
		}

		incidents := scheduler.GroupDetections(detections)

		assert.Len(t, incidents, 3)
		assert.Equal(t, "p_a", incidents[0].Incident.Key.ProjectName.String())
		assert.Equal(t, "job_a", incidents[0].Incident.Key.JobName.String())
		assert.Equal(t, "p_a", incidents[1].Incident.Key.ProjectName.String())
		assert.Equal(t, "job_z", incidents[1].Incident.Key.JobName.String())
		assert.Equal(t, "p_b", incidents[2].Incident.Key.ProjectName.String())
	})

	t.Run("empty detections yield no incidents", func(t *testing.T) {
		assert.Empty(t, scheduler.GroupDetections(nil))
	})
}

func TestSLABreachIncident_ActiveReasons(t *testing.T) {
	incident := &scheduler.SLABreachIncident{
		Details: scheduler.RootCauseDetails{
			Reasons: []scheduler.RootCauseReasonDetail{
				{Reason: scheduler.TierRuntimeDelay, Status: scheduler.ReasonStatusActive},
				{Reason: scheduler.TierThirdPartyDelay, Status: scheduler.ReasonStatusResolved},
			},
		},
	}

	active := incident.ActiveReasons()
	assert.Len(t, active, 1)
	assert.Equal(t, scheduler.TierRuntimeDelay, active[0].Reason)
	assert.True(t, incident.HasActiveReason())

	incident.Details.Reasons[0].Status = scheduler.ReasonStatusResolved
	assert.False(t, incident.HasActiveReason())
	assert.Empty(t, incident.ActiveReasons())
}
