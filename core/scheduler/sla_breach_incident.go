package scheduler

import (
	"context"
	"errors"
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

// ErrSLABreachIncidentNotFound is returned by the incident repository when no incident
// exists for the given key — the publisher's first write of an incident, for example.
// The caller decides whether that is the expected first-write path or something to log.
var ErrSLABreachIncidentNotFound = errors.New("sla breach incident not found")

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
	// IsJobFinished records whether the offending run had already finished when this
	// reason was observed. The alert router uses it to send exactly one notification for
	// a finished cause instead of hourly repeats. Absent (false) on older rows, which
	// correctly reads as "was still running when last seen".
	IsJobFinished bool `json:"is_job_finished,omitempty"`
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

// MergeResolvedReasons merges the freshly observed incident with the previously stored
// one so reasons that disappeared from the lineage are marked resolved instead of lost:
//
//   - every reason present in the fresh observation keeps its fresh form (always active —
//     a fresh observation of the tier means it is blocking right now), including one that
//     was previously resolved and has come back;
//   - every reason that exists only in the previous payload is kept, with its status
//     flipped to resolved and its last induced delay and evidence preserved;
//   - MaxInducedDelaySeconds is recomputed over the merged set, so the high-water mark
//     survives resolution.
//
// The input incidents are not modified; nil previous returns an unchanged copy of fresh.
func MergeResolvedReasons(fresh, previous *SLABreachIncident) *SLABreachIncident {
	merged := &SLABreachIncident{
		ID:        fresh.ID,
		Key:       fresh.Key,
		Details:   RootCauseDetails{Reasons: append([]RootCauseReasonDetail(nil), fresh.Details.Reasons...)},
		CreatedAt: fresh.CreatedAt,
		UpdatedAt: fresh.UpdatedAt,
	}
	if previous == nil {
		merged.MaxInducedDelaySeconds = maxInducedDelaySeconds(merged.Details)
		return merged
	}

	freshReasons := map[SLABreachTier]bool{}
	for _, reason := range merged.Details.Reasons {
		freshReasons[reason.Reason] = true
	}
	for _, previousReason := range previous.Details.Reasons {
		if freshReasons[previousReason.Reason] {
			continue
		}
		resolved := previousReason
		resolved.Status = ReasonStatusResolved
		merged.Details.Reasons = append(merged.Details.Reasons, resolved)
	}

	merged.MaxInducedDelaySeconds = maxInducedDelaySeconds(merged.Details)
	return merged
}

// SLABreachIncidentRepository persists incidents and their blast radius.
// The publisher upserts; the refactored subscriber reads.
type SLABreachIncidentRepository interface {
	UpsertIncident(ctx context.Context, incident *SLABreachIncident, impactedJobs []ImpactedJob) error
	GetActiveIncidents(ctx context.Context, since time.Time) ([]*SLABreachIncidentWithImpacts, error)
	// GetIncidentByKey returns the currently stored incident for the key, or
	// ErrSLABreachIncidentNotFound when none exists yet. The publisher reads it to flip
	// disappeared reasons to resolved.
	GetIncidentByKey(ctx context.Context, key SLABreachIncidentKey) (*SLABreachIncident, error)
}

// RootCauseBreachAlert is one alert for a single active reason of an incident, routed to
// the team that can act on it.
type RootCauseBreachAlert struct {
	Team             string
	Project          string
	RootCauseProject string
	RootCauseJob     string
	// RootCauseScheduledAt is the offending run's schedule; THIRD_PARTY_DELAY incidents
	// carry the daily sensor boundary instead, since sensors have no native run.
	RootCauseScheduledAt *time.Time
	Reason               RootCauseReasonDetail
	ImpactedJobs         []string
	Severity             string
}
