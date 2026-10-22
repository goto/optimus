//go:build !unit_test

package scheduler_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"

	"github.com/goto/optimus/core/scheduler"
	"github.com/goto/optimus/core/tenant"
	postgres "github.com/goto/optimus/internal/store/postgres/scheduler"
)

func TestPostgresSLABreachIncidentRepository(t *testing.T) {
	ctx := context.Background()

	projectName := tenant.ProjectName("p_rootcause")
	jobName := scheduler.JobName("job_offender")
	scheduledAt := time.Date(2026, 10, 22, 1, 0, 0, 0, time.UTC)

	impactedA := scheduler.ImpactedJob{ProjectName: "p_down", JobName: "job_a", ScheduledAt: scheduledAt.Add(time.Hour)}
	impactedB := scheduler.ImpactedJob{ProjectName: "p_down", JobName: "job_b", ScheduledAt: scheduledAt.Add(2 * time.Hour)}

	newIncident := func() *scheduler.SLABreachIncident {
		return &scheduler.SLABreachIncident{
			Key: scheduler.SLABreachIncidentKey{ProjectName: projectName, JobName: jobName, ScheduledAt: scheduledAt},
			Details: scheduler.RootCauseDetails{
				Reasons: []scheduler.RootCauseReasonDetail{
					{
						Reason:              scheduler.TierRuntimeDelay,
						Status:              scheduler.ReasonStatusActive,
						InducedDelaySeconds: 2400,
					},
				},
			},
			MaxInducedDelaySeconds: 2400,
		}
	}

	t.Run("UpsertIncident", func(t *testing.T) {
		t.Run("inserts a new incident with impacts", func(t *testing.T) {
			db := dbSetup()
			repo := postgres.NewSLABreachIncidentRepository(db)
			incident := newIncident()

			err := repo.UpsertIncident(ctx, incident, []scheduler.ImpactedJob{impactedA, impactedB})
			assert.NoError(t, err)
			assert.NotEqual(t, uuid.Nil, incident.ID, "id should be assigned")
			assert.False(t, incident.UpdatedAt.IsZero())
		})

		t.Run("replaces details and impacts on conflict without duplicating rows", func(t *testing.T) {
			db := dbSetup()
			repo := postgres.NewSLABreachIncidentRepository(db)

			incident := newIncident()
			assert.NoError(t, repo.UpsertIncident(ctx, incident, []scheduler.ImpactedJob{impactedA, impactedB}))
			firstUpdatedAt := incident.UpdatedAt

			// same entity, new full truth: a second reason is added and one impact is dropped
			incident = newIncident()
			incident.Details.Reasons = append(incident.Details.Reasons, scheduler.RootCauseReasonDetail{
				Reason:              scheduler.TierThirdPartyDelay,
				Status:              scheduler.ReasonStatusActive,
				InducedDelaySeconds: 900,
				BlockedOnSensors:    []string{"wait_dex_event_delivery"},
				SourceType:          "dex",
			})
			incident.MaxInducedDelaySeconds = 2400
			assert.NoError(t, repo.UpsertIncident(ctx, incident, []scheduler.ImpactedJob{impactedA}))
			assert.True(t, incident.UpdatedAt.After(firstUpdatedAt))

			active, err := repo.GetActiveIncidents(ctx, time.Now().UTC().Add(-time.Minute))
			assert.NoError(t, err)
			assert.Len(t, active, 1)
			assert.Len(t, active[0].Incident.Details.Reasons, 2, "both reasons are kept in the replaced payload")
			assert.Len(t, active[0].Impacts, 1, "stale impact rows are removed, not appended")
			assert.Equal(t, "job_a", active[0].Impacts[0].JobName.String())
		})
	})

	t.Run("GetActiveIncidents", func(t *testing.T) {
		t.Run("returns only incidents refreshed since the given time with an active reason", func(t *testing.T) {
			db := dbSetup()
			repo := postgres.NewSLABreachIncidentRepository(db)

			active := newIncident()
			assert.NoError(t, repo.UpsertIncident(ctx, active, []scheduler.ImpactedJob{impactedA}))

			// an incident with only resolved reasons must not come back
			resolved := newIncident()
			resolved.Key = scheduler.SLABreachIncidentKey{ProjectName: projectName, JobName: "job_resolved", ScheduledAt: scheduledAt}
			resolved.Details.Reasons[0].Status = scheduler.ReasonStatusResolved
			assert.NoError(t, repo.UpsertIncident(ctx, resolved, nil))

			activeIncidents, err := repo.GetActiveIncidents(ctx, time.Now().UTC().Add(-time.Minute))
			assert.NoError(t, err)
			assert.Len(t, activeIncidents, 1)
			assert.Equal(t, jobName, activeIncidents[0].Incident.Key.JobName)
			assert.Len(t, activeIncidents[0].Impacts, 1)

			// pushed outside the freshness window, the incident is treated as stale
			stale, err := repo.GetActiveIncidents(ctx, time.Now().UTC().Add(time.Minute))
			assert.NoError(t, err)
			assert.Empty(t, stale)
		})
	})
}
