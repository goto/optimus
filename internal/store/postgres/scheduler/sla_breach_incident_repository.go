package scheduler

import (
	"context"
	"encoding/json"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/goto/optimus/core/scheduler"
	"github.com/goto/optimus/core/tenant"
	"github.com/goto/optimus/internal/errors"
)

type SLABreachIncidentRepository struct {
	db *pgxpool.Pool
}

func NewSLABreachIncidentRepository(db *pgxpool.Pool) *SLABreachIncidentRepository {
	return &SLABreachIncidentRepository{db: db}
}

// UpsertIncident writes the incident's details (replace-on-conflict, the payload is already
// fully merged in the application layer) and replaces its impact rows, in one transaction.
func (r *SLABreachIncidentRepository) UpsertIncident(ctx context.Context, incident *scheduler.SLABreachIncident, impactedJobs []scheduler.ImpactedJob) error {
	details, err := json.Marshal(incident.Details)
	if err != nil {
		return errors.Wrap(scheduler.EntitySLABreachIncident, "error marshaling incident details", err)
	}

	tx, err := r.db.Begin(ctx)
	if err != nil {
		return errors.Wrap(scheduler.EntitySLABreachIncident, "error beginning transaction for incident upsert", err)
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()

	upsertQuery := `
		INSERT INTO sla_breach_root_cause (project_name, job_name, scheduled_at, details, max_induced_delay_seconds, updated_at)
		VALUES ($1, $2, $3, $4, $5, NOW())
		ON CONFLICT (project_name, job_name, scheduled_at)
		DO UPDATE SET details = EXCLUDED.details,
			max_induced_delay_seconds = EXCLUDED.max_induced_delay_seconds,
			updated_at = NOW()
		RETURNING id, created_at, updated_at`
	var id uuid.UUID
	var createdAt, updatedAt time.Time
	if err := tx.QueryRow(ctx, upsertQuery,
		incident.Key.ProjectName.String(), incident.Key.JobName.String(), incident.Key.ScheduledAt,
		details, incident.MaxInducedDelaySeconds,
	).Scan(&id, &createdAt, &updatedAt); err != nil {
		return errors.Wrap(scheduler.EntitySLABreachIncident, "error upserting SLA breach incident", err)
	}

	// replace the blast radius: the new impacts are this tick's full set
	if _, err := tx.Exec(ctx, `DELETE FROM sla_breach_impact WHERE root_cause_id = $1`, id); err != nil {
		return errors.Wrap(scheduler.EntitySLABreachIncident, "error clearing SLA breach impact rows", err)
	}
	for _, impacted := range impactedJobs {
		insertImpactQuery := `
			INSERT INTO sla_breach_impact (root_cause_id, impacted_project, impacted_job, scheduled_at)
			VALUES ($1, $2, $3, $4)
			ON CONFLICT DO NOTHING`
		if _, err := tx.Exec(ctx, insertImpactQuery,
			id, impacted.ProjectName.String(), impacted.JobName.String(), impacted.ScheduledAt,
		); err != nil {
			return errors.Wrap(scheduler.EntitySLABreachIncident, "error inserting SLA breach impact row", err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return errors.Wrap(scheduler.EntitySLABreachIncident, "error committing incident upsert", err)
	}

	incident.ID = id
	incident.CreatedAt = createdAt
	incident.UpdatedAt = updatedAt
	return nil
}

// GetActiveIncidents returns incidents refreshed at or after `since` that still carry at
// least one active reason, each with its current impact rows. The subscriber treats anything
// older as resolved/stale.
func (r *SLABreachIncidentRepository) GetActiveIncidents(ctx context.Context, since time.Time) ([]*scheduler.SLABreachIncidentWithImpacts, error) {
	query := `
		SELECT rc.id, rc.project_name, rc.job_name, rc.scheduled_at, rc.details,
			rc.max_induced_delay_seconds, rc.created_at, rc.updated_at,
			i.impacted_project, i.impacted_job, i.scheduled_at
		FROM sla_breach_root_cause rc
		LEFT JOIN sla_breach_impact i ON i.root_cause_id = rc.id
		WHERE rc.updated_at >= $1
			AND rc.details @> '{"reasons": [{"status": "active"}]}'::jsonb
		ORDER BY rc.project_name, rc.job_name, rc.scheduled_at`

	rows, err := r.db.Query(ctx, query, since)
	if err != nil {
		return nil, errors.Wrap(scheduler.EntitySLABreachIncident, "error querying active SLA breach incidents", err)
	}
	defer rows.Close()

	resultsByID := map[uuid.UUID]*scheduler.SLABreachIncidentWithImpacts{}
	var results []*scheduler.SLABreachIncidentWithImpacts
	for rows.Next() {
		var id uuid.UUID
		var projectName, jobName string
		var scheduledAt, createdAt, updatedAt time.Time
		var rawDetails []byte
		var maxInducedDelaySeconds int
		var impactedProject, impactedJob *string
		var impactedScheduledAt *time.Time

		if err := rows.Scan(&id, &projectName, &jobName, &scheduledAt, &rawDetails,
			&maxInducedDelaySeconds, &createdAt, &updatedAt,
			&impactedProject, &impactedJob, &impactedScheduledAt); err != nil {
			return nil, errors.Wrap(scheduler.EntitySLABreachIncident, "error scanning SLA breach incident row", err)
		}

		result, ok := resultsByID[id]
		if !ok {
			details := scheduler.RootCauseDetails{}
			if err := json.Unmarshal(rawDetails, &details); err != nil {
				return nil, errors.Wrap(scheduler.EntitySLABreachIncident, "error unmarshaling incident details", err)
			}
			key, err := newIncidentKey(projectName, jobName, scheduledAt)
			if err != nil {
				return nil, err
			}
			result = &scheduler.SLABreachIncidentWithImpacts{
				Incident: &scheduler.SLABreachIncident{
					ID:                     id,
					Key:                    key,
					Details:                details,
					MaxInducedDelaySeconds: maxInducedDelaySeconds,
					CreatedAt:              createdAt,
					UpdatedAt:              updatedAt,
				},
			}
			resultsByID[id] = result
			results = append(results, result)
		}

		if impactedProject != nil && impactedJob != nil && impactedScheduledAt != nil {
			impactedKey, err := newIncidentKey(*impactedProject, *impactedJob, *impactedScheduledAt)
			if err != nil {
				return nil, err
			}

			result.Impacts = append(result.Impacts, scheduler.ImpactedJob(impactedKey))
		}
	}
	if err := rows.Err(); err != nil {
		return nil, errors.Wrap(scheduler.EntitySLABreachIncident, "error reading SLA breach incident rows", err)
	}
	return results, nil
}

func newIncidentKey(projectName, jobName string, scheduledAt time.Time) (scheduler.SLABreachIncidentKey, error) {
	project, err := tenant.ProjectNameFrom(projectName)
	if err != nil {
		return scheduler.SLABreachIncidentKey{}, errors.Wrap(scheduler.EntitySLABreachIncident, "error adapting project name", err)
	}
	job, err := scheduler.JobNameFrom(jobName)
	if err != nil {
		return scheduler.SLABreachIncidentKey{}, errors.Wrap(scheduler.EntitySLABreachIncident, "error adapting job name", err)
	}
	return scheduler.SLABreachIncidentKey{ProjectName: project, JobName: job, ScheduledAt: scheduledAt}, nil
}

var _ scheduler.SLABreachIncidentRepository = (*SLABreachIncidentRepository)(nil)
