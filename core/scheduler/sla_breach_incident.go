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

const (
	DelayScopeJob     = "job"
	DelayScopeProject = "project"
)

type RootCauseReasonDetail struct {
	Reason SLABreachTier `json:"reason"`
	Status ReasonStatus  `json:"status"`
	// InducedDelaySeconds is the delay that counts toward thresholds and suppression
	InducedDelaySeconds int      `json:"induced_delay_seconds"`
	BlockedOnSensors    []string `json:"blocked_on_sensors,omitempty"`
	SourceType          string   `json:"source_type,omitempty"`
	// IsJobFinished records whether the offending run had already finished when this
	// reason was observed. The alert router uses it to send exactly one notification for
	// a finished cause instead of hourly repeats. Absent (false) on older rows, which
	// correctly reads as "was still running when last seen".
	IsJobFinished bool   `json:"is_job_finished,omitempty"`
	DelayScope    string `json:"delay_scope,omitempty"`
	// MaxNodeInducedDelaySeconds is the representative job's own induced delay for a
	// project-scoped runtime reason
	MaxNodeInducedDelaySeconds int `json:"max_node_induced_delay_seconds,omitempty"`
	// Evidence records how InducedDelaySeconds was calculated.
	// Optional and absent on older rows; never sent to alerting.
	Evidence *ReasonEvidence `json:"evidence,omitempty"`
}

// Evidence rule names: which detector produced the reason and under which condition.
const (
	EvidenceRuleRuntimeJobOverThreshold        = "runtime_job_over_threshold"
	EvidenceRuleRuntimeProjectSumOverThreshold = "runtime_project_sum_over_threshold"
	EvidenceRuleSchedulerPathOverBudget        = "scheduler_path_over_budget"
	EvidenceRuleThirdPartyWaitOverThreshold    = "third_party_wait_over_threshold"
	EvidenceRuleWaitingUpstreamPastSafeStart   = "waiting_upstream_past_safe_start"
	EvidenceRuleAggressiveSLANoCause           = "aggressive_sla_no_cause"
)

const (
	// MaxEvidenceComponents caps the calculation component list stored per reason;
	// components beyond the cap are dropped but counted in CalculationComponentsTotal.
	// The list is sorted largest first, so the cap keeps the biggest contributors.
	MaxEvidenceComponents = 20
)

// Source markers for evidence times: which candidate supplied a start or end.
const (
	EvidenceSourceActualEnd       = "actual_end"
	EvidenceSourceReferenceTime   = "reference_time"
	EvidenceSourceWaitEnd         = "wait_end"
	EvidenceSourceScheduledAt     = "scheduled_at"
	EvidenceSourceUpstreamEnd     = "upstream_end"
	EvidenceSourceDailyBoundary   = "daily_boundary"
	EvidenceSourceSensorWaitStart = "sensor_wait_start"
)

// ReasonEvidence is the calculation record of one reason's induced delay.
// Exactly one reason section is filled — the one matching the rule — and the
// other sections stay nil. FAILED_UPSTREAM has no rule of its own and carries
// no section.
type ReasonEvidence struct {
	ReferenceTime time.Time `json:"reference_time"`
	// Rule names the detector that produced the reason and the condition it fired on.
	Rule string `json:"rule,omitempty"`
	// ThresholdSeconds is the minimum the measured delay must exceed before the
	// reason fires. For the runtime reasons it is the configured minimum delay;
	// for the scheduler reason it is the measured path's allowance: the
	// per-depth minutes scaled by the number of measured nodes.
	ThresholdSeconds int `json:"threshold_seconds,omitempty"`

	Runtime         *RuntimeEvidence         `json:"runtime,omitempty"`
	Scheduler       *SchedulerEvidence       `json:"scheduler,omitempty"`
	ThirdParty      *ThirdPartyEvidence      `json:"third_party,omitempty"`
	WaitingUpstream *WaitingUpstreamEvidence `json:"waiting_upstream,omitempty"`
	AggressiveSLA   *AggressiveSLAEvidence   `json:"aggressive_sla,omitempty"`
}

// ComponentBase is the part every component kind shares. It is embedded, so its
// fields appear flat inside each component object in the stored JSON.
type ComponentBase struct {
	Project string `json:"project"`
	Job     string `json:"job"`
	// InducedSeconds is this job's contribution to the reason's induced delay.
	InducedSeconds int        `json:"induced_seconds"`
	ScheduledAt    *time.Time `json:"scheduled_at,omitempty"`
}

// evidenceSort exposes the shared keys CapComponents orders components by.
type evidenceSort interface {
	evidenceSortKey() (inducedSeconds int, project, job string)
}

func (c ComponentBase) evidenceSortKey() (int, string, string) {
	return c.InducedSeconds, c.Project, c.Job
}

// RuntimeComponent is one job's runtime measurement: the observed runtime with
// its start/end sources and the baseline it was compared against.
type RuntimeComponent struct {
	ComponentBase
	TaskStart *time.Time `json:"task_start,omitempty"`
	End       *time.Time `json:"end,omitempty"`
	// EndSource marks what End is: the run's actual end, or the reference time
	// while the run was still going.
	EndSource       string `json:"end_source,omitempty"`
	ObservedSeconds *int   `json:"observed_seconds,omitempty"`
	BaselineSeconds *int   `json:"baseline_seconds,omitempty"`
}

// SchedulerComponent is one measured node's scheduler-owned wait: which upstream
// end (or the node's own schedule) it was measured from, and when the task started.
type SchedulerComponent struct {
	ComponentBase
	TaskStart *time.Time `json:"task_start,omitempty"`
	// UpstreamJob and UpstreamEnd identify the latest-finished upstream whose end
	// the wait was measured from; empty when the node had no finished upstream.
	UpstreamJob string     `json:"upstream_job,omitempty"`
	UpstreamEnd *time.Time `json:"upstream_end,omitempty"`
	// WaitFrom is when the node could have started; WaitFromSource marks which
	// candidate supplied it (its own schedule or the upstream end).
	WaitFrom       *time.Time `json:"wait_from,omitempty"`
	WaitFromSource string     `json:"wait_from_source,omitempty"`
}

// RuntimeEvidence backs the runtime reasons: a single job over the threshold, or
// a project whose summed induced delay crossed it.
type RuntimeEvidence struct {
	// MaxNode names the project's max-delay job for a project-scoped reason.
	MaxNode                    string             `json:"max_node,omitempty"`
	CalculationComponentsTotal int                `json:"calculation_components_total,omitempty"`
	CalculationComponents      []RuntimeComponent `json:"calculation_components,omitempty"`
	// SkippedNoBaseline lists the project's jobs that could not be measured
	// because they have no usable baseline.
	SkippedNoBaseline []string `json:"skipped_no_baseline,omitempty"`
}

// SchedulerEvidence backs the scheduler reason: the summed scheduling waits of
// the measured path.
type SchedulerEvidence struct {
	CalculationComponentsTotal int                  `json:"calculation_components_total,omitempty"`
	CalculationComponents      []SchedulerComponent `json:"calculation_components,omitempty"`
}

// ThirdPartyEvidence backs the third-party reason: a single sensor wait, so
// there is no component list.
type ThirdPartyEvidence struct {
	Sensor   string    `json:"sensor,omitempty"`
	Boundary time.Time `json:"boundary,omitempty"`
	// WaitFrom is when the sensor could have started; WaitFromSource marks which
	// candidate supplied it (the sensor's wait start or the daily boundary).
	WaitFrom       time.Time `json:"wait_from,omitempty"`
	WaitFromSource string    `json:"wait_from_source,omitempty"`
	End            time.Time `json:"end,omitempty"`
	// EndSource marks what End is: the wait's end, or the reference time while
	// the sensor was still waiting.
	EndSource string `json:"end_source,omitempty"`
}

// WaitingUpstreamEvidence backs the waiting-upstream reason: the facts behind a
// stuck node. Its induced delay is the node's own overdue time past LatestSafeStart.
type WaitingUpstreamEvidence struct {
	// NodeState is the stuck node's own state: waiting on its upstream, or an
	// expected run that was never created.
	NodeState                string     `json:"node_state,omitempty"`
	InferredSLA              *time.Time `json:"inferred_sla,omitempty"`
	EstimatedDurationSeconds *int       `json:"estimated_duration_seconds,omitempty"`
	LatestSafeStart          *time.Time `json:"latest_safe_start,omitempty"`
	// MissingUpstreamRuns names the upstreams (project/job) whose expected run was
	// never created. Empty means every upstream succeeded and the node still did
	// not start, pointing at a scheduling or trigger gap; non-empty names the runs
	// to create or backfill.
	MissingUpstreamRuns []string `json:"missing_upstream_runs,omitempty"`
}

// AggressiveSLAEvidence backs the aggressive-SLA reason: the target is past its
// SLA with no cause explaining it, so the facts show the deadline it missed and
// how the run went against its baseline.
type AggressiveSLAEvidence struct {
	// TargetSLATime is the impacted target's own SLA deadline
	// (scheduled_at plus its SLA duration) — the time it breached.
	TargetSLATime time.Time `json:"target_sla_time,omitempty"`
	// End is when the run ended; while it is still running or has not started,
	// it is the reference time instead.
	End       *time.Time `json:"end,omitempty"`
	EndSource string     `json:"end_source,omitempty"`
	// LateBySeconds is how late the run is past the target SLA: the actual end
	// minus the deadline, or the reference time minus the deadline while the
	// run has not finished.
	LateBySeconds   int        `json:"late_by_seconds,omitempty"`
	TaskStart       *time.Time `json:"task_start,omitempty"`
	BaselineSeconds *int       `json:"baseline_seconds,omitempty"`
}

func TimePtr(t time.Time) *time.Time { return &[]time.Time{t.UTC()}[0] }

func IntPtr(v int) *int { return &v }

// CapComponents sorts components by induced delay (largest first, ties by project
// then job) and truncates the list to MaxEvidenceComponents, reporting the pre-cap
// count. It works for any component kind sharing ComponentBase.
func CapComponents[T evidenceSort](components []T) ([]T, int) {
	total := len(components)
	sort.SliceStable(components, func(i, j int) bool {
		aInduced, aProject, aJob := components[i].evidenceSortKey()
		bInduced, bProject, bJob := components[j].evidenceSortKey()
		if aInduced != bInduced {
			return aInduced > bInduced
		}
		if aProject != bProject {
			return aProject < bProject
		}
		return aJob < bJob
	})
	if total > MaxEvidenceComponents {
		components = components[:MaxEvidenceComponents]
	}
	return components, total
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
				winner := detection.Reason
				slot.InducedDelaySeconds = winner.InducedDelaySeconds
				slot.IsJobFinished = winner.IsJobFinished
				slot.DelayScope = winner.DelayScope
				slot.MaxNodeInducedDelaySeconds = winner.MaxNodeInducedDelaySeconds
				slot.Evidence = winner.Evidence
			} else if detection.Reason.InducedDelaySeconds == slot.InducedDelaySeconds && slot.Evidence == nil {
				// equal numbers: keep the first evidence when the slot has one,
				// else adopt the incoming one
				slot.Evidence = detection.Reason.Evidence
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
