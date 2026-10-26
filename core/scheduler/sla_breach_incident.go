package scheduler

import (
	"context"
	"sort"
	"time"

	"github.com/google/uuid"

	"github.com/goto/optimus/core/tenant"
)

// SLABreachTier are evaluated independently, so one incident can hold several reasons at once.
type SLABreachTier string

const (
	// TierRuntimeDelay task/hook runtime along the bottleneck path overran the historical
	// baseline beyond the configured threshold.
	TierRuntimeDelay    SLABreachTier = "RUNTIME_DELAY"
	TierSchedulerDelay  SLABreachTier = "SCHEDULER_DELAY"
	TierThirdPartyDelay SLABreachTier = "THIRD_PARTY_DELAY"
	TierFailedUpstream  SLABreachTier = "FAILED_UPSTREAM"

	// TierWaitingUpstream the run is stuck waiting although every resolved upstream is
	// successful or missing entirely.
	TierWaitingUpstream SLABreachTier = "WAITING_UPSTREAM"

	// TierAggressiveSLA the downstream job breached its SLA with no upstream cause
	// detected; the SLA is configured too aggressively.
	TierAggressiveSLA SLABreachTier = "AGGRESSIVE_SLA"
)

type ReasonStatus string

const EntitySLABreachIncident = "slaBreachIncident"

const (
	ReasonStatusActive   ReasonStatus = "active"
	ReasonStatusResolved ReasonStatus = "resolved"
)

type RootCauseReasonDetail struct {
	Reason              SLABreachTier `json:"reason"`
	Status              ReasonStatus  `json:"status"`
	InducedDelaySeconds int           `json:"induced_delay_seconds"`
	BlockedOnSensors    []string      `json:"blocked_on_sensors,omitempty"`
	SourceType          string        `json:"source_type,omitempty"`
}

type RootCauseDetails struct {
	Reasons []RootCauseReasonDetail `json:"reasons"`
}

// SLABreachIncidentKey identifies one offending job. It matches the unique constraint on
// sla_breach_root_cause (project_name, job_name, scheduled_at). THIRD_PARTY_DELAY incidents
// use a daily boundary as ScheduledAt, since sensors have no native run of their own.
type SLABreachIncidentKey struct {
	ProjectName tenant.ProjectName
	JobName     JobName
	ScheduledAt time.Time
}

// SLABreachIncident is the publisher state.
type SLABreachIncident struct {
	ID                     uuid.UUID
	Key                    SLABreachIncidentKey
	Details                RootCauseDetails
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

func (i *SLABreachIncident) HasActiveReason() bool {
	for _, reason := range i.Details.Reasons {
		if reason.Status == ReasonStatusActive {
			return true
		}
	}
	return false
}

type ImpactedJob struct {
	ProjectName tenant.ProjectName
	JobName     JobName
	ScheduledAt time.Time
}

type RootCauseDetection struct {
	IncidentKey  SLABreachIncidentKey
	Reason       RootCauseReasonDetail
	ImpactedJobs []ImpactedJob
}

// GroupDetections assembles the detections of one evaluation tick into incidents, one per
// unique incident key. Reasons are merged per incident (deduplicated by reason, keeping the
// max induced delay and the freshest evidence), and impacted jobs are deduplicated.
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
		incident.MaxInducedDelaySeconds = maxInducedDelaySeconds(incident.Details)
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

func maxInducedDelaySeconds(details RootCauseDetails) int {
	maxSeconds := 0
	for _, reason := range details.Reasons {
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

type SLABreachIncidentWithImpacts struct {
	Incident *SLABreachIncident
	Impacts  []ImpactedJob
}

// SLABreachIncidentRepository persists incidents and their blast radius.
// The publisher upserts; the refactored subscriber reads.
type SLABreachIncidentRepository interface {
	UpsertIncident(ctx context.Context, incident *SLABreachIncident, impactedJobs []ImpactedJob) error
	GetActiveIncidents(ctx context.Context, since time.Time) ([]*SLABreachIncidentWithImpacts, error)
}
