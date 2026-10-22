package scheduler

import (
	"testing"
	"time"

	"github.com/goto/optimus/core/tenant"
	"github.com/stretchr/testify/assert"
)

func incidentKey(project, job string, scheduledAt time.Time) SLABreachIncidentKey {
	return SLABreachIncidentKey{
		ProjectName: tenant.ProjectName(project),
		JobName:     JobName(job),
		ScheduledAt: scheduledAt,
	}
}

func TestGroupDetections(t *testing.T) {
	baseTime := time.Date(2026, 10, 22, 1, 0, 0, 0, time.UTC)

	t.Run("groups detections from different tiers into one incident per key", func(t *testing.T) {
		key := incidentKey("p_proj", "job_root", baseTime)
		detections := []RootCauseDetection{
			{
				IncidentKey: key,
				Reason: RootCauseReasonDetail{
					Reason:              TierRuntimeDelay,
					Status:              ReasonStatusActive,
					InducedDelaySeconds: 40 * 60,
				},
				ImpactedJobs: []ImpactedJob{
					{ProjectName: "p_down", JobName: "job_down", ScheduledAt: baseTime},
				},
			},
			{
				IncidentKey: key,
				Reason: RootCauseReasonDetail{
					Reason:              TierThirdPartyDelay,
					Status:              ReasonStatusActive,
					InducedDelaySeconds: 25 * 60,
					BlockedOnSensors:    []string{"wait_dex_event_delivery"},
					SourceType:          "dex",
				},
			},
		}

		incidents := GroupDetections(detections)

		assert.Len(t, incidents, 1)
		incident := incidents[0].Incident
		assert.Equal(t, key, incident.Key)
		assert.Len(t, incident.Details.Reasons, 2)
		assert.True(t, incident.HasActiveReason())
		assert.Equal(t, 40*60, incident.MaxInducedDelaySeconds)
		assert.Len(t, incidents[0].Impacts, 1)
		assert.Equal(t, JobName("job_down"), incidents[0].Impacts[0].JobName)
	})

	t.Run("merges duplicate reasons keeping max induced delay and freshest evidence", func(t *testing.T) {
		key := incidentKey("p_proj", "job_root", baseTime)
		detections := []RootCauseDetection{
			{
				IncidentKey: key,
				Reason: RootCauseReasonDetail{
					Reason:              TierRuntimeDelay,
					Status:              ReasonStatusActive,
					InducedDelaySeconds: 31 * 60,
				},
			},
			{
				IncidentKey: key,
				Reason: RootCauseReasonDetail{
					Reason:              TierRuntimeDelay,
					Status:              ReasonStatusActive,
					InducedDelaySeconds: 45 * 60,
				},
				ImpactedJobs: []ImpactedJob{
					{ProjectName: "p_down", JobName: "job_a", ScheduledAt: baseTime},
				},
			},
		}

		incidents := GroupDetections(detections)

		assert.Len(t, incidents, 1)
		reasons := incidents[0].Incident.Details.Reasons
		assert.Len(t, reasons, 1)
		assert.Equal(t, 45*60, reasons[0].InducedDelaySeconds)
		assert.Equal(t, 45*60, incidents[0].Incident.MaxInducedDelaySeconds)
		assert.Len(t, incidents[0].Impacts, 1)
	})

	t.Run("deduplicates impacted jobs across detections", func(t *testing.T) {
		key := incidentKey("p_proj", "job_root", baseTime)
		impacted := ImpactedJob{ProjectName: "p_down", JobName: "job_a", ScheduledAt: baseTime}
		detections := []RootCauseDetection{
			{IncidentKey: key, Reason: RootCauseReasonDetail{Reason: TierRuntimeDelay, Status: ReasonStatusActive}, ImpactedJobs: []ImpactedJob{impacted}},
			{IncidentKey: key, Reason: RootCauseReasonDetail{Reason: TierSchedulerDelay, Status: ReasonStatusActive}, ImpactedJobs: []ImpactedJob{impacted}},
		}

		incidents := GroupDetections(detections)

		assert.Len(t, incidents, 1)
		assert.Len(t, incidents[0].Impacts, 1)
	})

	t.Run("max induced delay is a high-water mark including resolved reasons", func(t *testing.T) {
		key := incidentKey("p_proj", "job_root", baseTime)
		detections := []RootCauseDetection{
			{
				IncidentKey: key,
				Reason: RootCauseReasonDetail{
					Reason:              TierRuntimeDelay,
					Status:              ReasonStatusResolved,
					InducedDelaySeconds: 90 * 60,
				},
			},
			{
				IncidentKey: key,
				Reason: RootCauseReasonDetail{
					Reason:              TierThirdPartyDelay,
					Status:              ReasonStatusActive,
					InducedDelaySeconds: 15 * 60,
				},
			},
		}

		incidents := GroupDetections(detections)

		assert.Len(t, incidents, 1)
		// the resolved reason still counts toward the peak severity
		assert.Equal(t, 90*60, incidents[0].Incident.MaxInducedDelaySeconds)
	})

	t.Run("orders incidents deterministically by key", func(t *testing.T) {
		later := baseTime.Add(time.Hour)
		detections := []RootCauseDetection{
			{IncidentKey: incidentKey("p_b", "job_x", baseTime), Reason: RootCauseReasonDetail{Reason: TierRuntimeDelay, Status: ReasonStatusActive}},
			{IncidentKey: incidentKey("p_a", "job_z", later), Reason: RootCauseReasonDetail{Reason: TierRuntimeDelay, Status: ReasonStatusActive}},
			{IncidentKey: incidentKey("p_a", "job_a", baseTime), Reason: RootCauseReasonDetail{Reason: TierRuntimeDelay, Status: ReasonStatusActive}},
		}

		incidents := GroupDetections(detections)

		assert.Len(t, incidents, 3)
		assert.Equal(t, "p_a", incidents[0].Incident.Key.ProjectName.String())
		assert.Equal(t, "job_a", incidents[0].Incident.Key.JobName.String())
		assert.Equal(t, "p_a", incidents[1].Incident.Key.ProjectName.String())
		assert.Equal(t, "job_z", incidents[1].Incident.Key.JobName.String())
		assert.Equal(t, "p_b", incidents[2].Incident.Key.ProjectName.String())
	})

	t.Run("empty detections yield no incidents", func(t *testing.T) {
		assert.Empty(t, GroupDetections(nil))
	})
}

func TestSLABreachIncident_ActiveReasons(t *testing.T) {
	incident := &SLABreachIncident{
		Details: RootCauseDetails{
			Reasons: []RootCauseReasonDetail{
				{Reason: TierRuntimeDelay, Status: ReasonStatusActive},
				{Reason: TierThirdPartyDelay, Status: ReasonStatusResolved},
			},
		},
	}

	active := incident.ActiveReasons()
	assert.Len(t, active, 1)
	assert.Equal(t, TierRuntimeDelay, active[0].Reason)
	assert.True(t, incident.HasActiveReason())

	incident.Details.Reasons[0].Status = ReasonStatusResolved
	assert.False(t, incident.HasActiveReason())
	assert.Empty(t, incident.ActiveReasons())
}
