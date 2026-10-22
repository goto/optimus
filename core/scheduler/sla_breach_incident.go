package scheduler

import (
	"context"
	"sort"
	"time"

	"github.com/google/uuid"
	"github.com/goto/optimus/core/tenant"
)

// SLABreachTier names one detection tier of the root-cause SLA breach engine. Tiers
// are evaluated independently, so one incident can hold several reasons at once.
type SLABreachTier string

const (
	// TierRuntimeDelay: task/hook runtime along the bottleneck path overran the historical
	// baseline beyond the configured threshold.
	TierRuntimeDelay SLABreachTier = "RUNTIME_DELAY"
	// TierSchedulerDelay: the scheduler or queue took too long to start the run once its
	// gating upstreams had finished.
	TierSchedulerDelay SLABreachTier = "SCHEDULER_DELAY"
	// TierThirdPartyDelay: the run is stuck (or was stuck) waiting on a third-party
	// raw-layer sensor, e.g. wait_dex_*.
	TierThirdPartyDelay SLABreachTier = "THIRD_PARTY_DELAY"
	// TierFailedUpstream: an upstream run, of any scheduled date, has failed.
	TierFailedUpstream SLABreachTier = "FAILED_UPSTREAM"
	// TierWaitingUpstream: the run is stuck waiting although every resolved upstream is
	// successful or missing entirely.
	TierWaitingUpstream SLABreachTier = "WAITING_UPSTREAM"
	// TierAggressiveSLA: the downstream job breached its SLA with no upstream cause
	// detected; the SLA is configured too aggressively.
	TierAggressiveSLA SLABreachTier = "AGGRESSIVE_SLA"
)

// ReasonStatus is the lifecycle state of a single reason within an incident. The engine
// keeps both active and resolved reasons in the details payload so reasons can be re-upserted
// as they appear and disappear from the lineage graph.
type ReasonStatus string

const EntitySLABreachIncident = "slaBreachIncident"

const (
	ReasonStatusActive   ReasonStatus = "active"
	ReasonStatusResolved ReasonStatus = "resolved"
)

// RootCauseReasonDetail is one reason with its evidence.
type RootCauseReasonDetail struct {
	Reason SLABreachTier `json:"reason"`
	Status ReasonStatus  `json:"status"`
	// InducedDelaySeconds is how far this reason overran its baseline, in seconds.
	InducedDelaySeconds int      `json:"induced_delay_seconds"`
	BlockedOnSensors    []string `json:"blocked_on_sensors,omitempty"`
	SourceType          string   `json:"source_type,omitempty"`
}

// RootCauseDetails is the JSONB payload stored on an incident row.
type RootCauseDetails struct {
	Reasons []RootCauseReasonDetail `json:"reasons"`
}

// SLABreachIncidentKey identifies one offending entity. It matches the unique constraint on
// sla_breach_root_cause (project_name, job_name, scheduled_at). THIRD_PARTY_DELAY incidents
// use a daily boundary as ScheduledAt, since sensors have no native run of their own.
type SLABreachIncidentKey struct {
	ProjectName tenant.ProjectName
	JobName     JobName
	ScheduledAt time.Time
}

// SLABreachIncident is the publisher state: one platform degradation, keyed by the offending
// entity, carrying every known reason and refreshed (updated_at bumped) as long as the
// engine keeps observing it in the lineage graph.
type SLABreachIncident struct {
	ID      uuid.UUID
	Key     SLABreachIncidentKey
	Details RootCauseDetails
	// MaxInducedDelaySeconds is the largest induced delay among the active reasons.
	MaxInducedDelaySeconds int
	CreatedAt              time.Time
	UpdatedAt              time.Time
}

// ActiveReasons returns the reasons currently observed as blocking the lineage.
func (i *SLABreachIncident) ActiveReasons() []RootCauseReasonDetail {
	active := make([]RootCauseReasonDetail, 0, len(i.Details.Reasons))
	for _, reason := range i.Details.Reasons {
		if reason.Status == ReasonStatusActive {
			active = append(active, reason)
		}
	}
	return active
}

// HasActiveReason reports whether the incident still has at least one active reason, i.e.
// whether it is worth alerting on.
func (i *SLABreachIncident) HasActiveReason() bool {
	for _, reason := range i.Details.Reasons {
		if reason.Status == ReasonStatusActive {
			return true
		}
	}
	return false
}

// ImpactedJob is one downstream job suffering because of an incident.
type ImpactedJob struct {
	ProjectName tenant.ProjectName
	JobName     JobName
	ScheduledAt time.Time
}

// RootCauseDetection is one tier match observed for an incident key during a single
// evaluation tick, together with the downstream jobs it was observed to impact.
type RootCauseDetection struct {
	IncidentKey  SLABreachIncidentKey
	Reason       RootCauseReasonDetail
	ImpactedJobs []ImpactedJob
}

// GroupDetections assembles the detections of one evaluation tick into incidents, one per
// unique incident key. Reasons are merged per incident (deduplicated by reason, keeping the
// max induced delay and the freshest evidence), and impacted jobs are deduplicated.
//
// This is the application-layer merge: the fully assembled details payload written by the
// upsert is the complete current truth of each incident, so the database write can be a
// plain replace without losing reasons detected by another tier or target in the same tick.
func GroupDetections(detections []RootCauseDetection) []*SLABreachIncidentWithImpacts {
	byKey := map[SLABreachIncidentKey]*SLABreachIncident{}
	impactsByIncident := map[*SLABreachIncident][]ImpactedJob{}

	for _, detection := range detections {
		incident, ok := byKey[detection.IncidentKey]
		if !ok {
			incident = &SLABreachIncident{Key: detection.IncidentKey}
			byKey[detection.IncidentKey] = incident
		}

		merged := false
		for j := range incident.Details.Reasons {
			if incident.Details.Reasons[j].Reason != detection.Reason.Reason {
				continue
			}
			slot := &incident.Details.Reasons[j]
			if detection.Reason.InducedDelaySeconds > slot.InducedDelaySeconds {
				slot.InducedDelaySeconds = detection.Reason.InducedDelaySeconds
			}
			if len(detection.Reason.BlockedOnSensors) > 0 {
				slot.BlockedOnSensors = detection.Reason.BlockedOnSensors
			}
			if detection.Reason.SourceType != "" {
				slot.SourceType = detection.Reason.SourceType
			}
			// a fresh observation of the tier means it is currently blocking
			slot.Status = detection.Reason.Status
			merged = true
			break
		}
		if !merged {
			incident.Details.Reasons = append(incident.Details.Reasons, detection.Reason)
		}

		for _, impacted := range detection.ImpactedJobs {
			duplicate := false
			for _, existing := range impactsByIncident[incident] {
				if existing == impacted {
					duplicate = true
					break
				}
			}
			if !duplicate {
				impactsByIncident[incident] = append(impactsByIncident[incident], impacted)
			}
		}
	}

	incidents := make([]*SLABreachIncidentWithImpacts, 0, len(byKey))
	for _, incident := range byKey {
		incident.MaxInducedDelaySeconds = maxActiveInducedDelaySeconds(incident.Details)
		incidents = append(incidents, &SLABreachIncidentWithImpacts{
			Incident: incident,
			Impacts:  impactsByIncident[incident],
		})
	}
	sort.Slice(incidents, func(i, j int) bool {
		return incidentKeyLess(incidents[i].Incident.Key, incidents[j].Incident.Key)
	})
	return incidents
}

func maxActiveInducedDelaySeconds(details RootCauseDetails) int {
	maxSeconds := 0
	for _, reason := range details.Reasons {
		if reason.Status != ReasonStatusActive {
			continue
		}
		if reason.InducedDelaySeconds > maxSeconds {
			maxSeconds = reason.InducedDelaySeconds
		}
	}
	return maxSeconds
}

func incidentKeyLess(a, b SLABreachIncidentKey) bool {
	if a.ProjectName != b.ProjectName {
		return a.ProjectName < b.ProjectName
	}
	if a.JobName != b.JobName {
		return a.JobName < b.JobName
	}
	return a.ScheduledAt.Before(b.ScheduledAt)
}

// SLABreachIncidentWithImpacts pairs an incident with its current blast radius.
type SLABreachIncidentWithImpacts struct {
	Incident *SLABreachIncident
	Impacts  []ImpactedJob
}

// SLABreachIncidentRepository persists incidents and their blast radius. The publisher
// upserts; the refactored subscriber reads.
type SLABreachIncidentRepository interface {
	// UpsertIncident writes the incident's fully assembled details and replaces its impact
	// rows in one transaction, keyed by the incident's unique (project, job, scheduled_at).
	// A fresh payload always represents the complete current truth of the incident.
	UpsertIncident(ctx context.Context, incident *SLABreachIncident, impactedJobs []ImpactedJob) error
	// GetActiveIncidents returns incidents refreshed at or after `since` that still carry at
	// least one active reason, each with its current impact rows.
	GetActiveIncidents(ctx context.Context, since time.Time) ([]*SLABreachIncidentWithImpacts, error)
}
