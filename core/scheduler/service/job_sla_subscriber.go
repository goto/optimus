package service

import (
	"context"
	"time"

	"github.com/goto/optimus/core/scheduler"
	"github.com/goto/optimus/core/tenant"
	"github.com/goto/optimus/internal/errors"
)

// incidentFreshnessWindow is how far back the subscriber looks for refreshed incidents;
// anything older is stale because the publisher keeps bumping updated_at while a root
// cause still blocks the lineage.
const incidentFreshnessWindow = 24 * time.Hour

// rootCauseTenant reconstructs a tenant for the incident's project. The incident schema
// stores only the project name, and the namespace is never queried on this path — the "-"
// placeholder exists purely so the root-cause project name renders in alerts and responses.
func rootCauseTenant(projectName tenant.ProjectName) tenant.Tenant {
	tnnt, err := tenant.NewTenant(projectName.String(), "-")
	if err != nil {
		return tenant.Tenant{}
	}
	return tnnt
}

// mapReason translates a stored incident reason onto the reason vocabulary of the
// potential-SLA breach alerts; tiers without a finer-grained legacy equivalent stay UNKNOWN.
func mapReason(reason scheduler.SLABreachTier) scheduler.RootCauseReason {
	switch reason {
	case scheduler.TierRuntimeDelay:
		return scheduler.ReasonRunningLong
	case scheduler.TierThirdPartyDelay:
		return scheduler.ReasonThirdPartyDelay
	case scheduler.TierFailedUpstream:
		return scheduler.ReasonUpstreamFailed
	default:
		return scheduler.ReasonUnknown
	}
}

// mapStatus classifies the cause state for the alert payload.
func mapStatus(reason scheduler.SLABreachTier) scheduler.SLABreachCause {
	if reason == scheduler.TierRuntimeDelay {
		return scheduler.SLABreachCauseRunningLate
	}
	return scheduler.SLABreachCauseNotStarted
}

func (s *JobSLAPredictorService) activeIncidents(ctx context.Context, reqConfig JobSLAPredictorRequestConfig) ([]*scheduler.SLABreachIncidentWithImpacts, error) {
	if s.incidentRepo == nil {
		return nil, errors.InvalidArgument(scheduler.EntitySLABreachIncident, "subscriber mode requires the SLA breach incident repository")
	}
	return s.incidentRepo.GetActiveIncidents(ctx, reqConfig.ReferenceTime.Add(-incidentFreshnessWindow))
}

// identifyFromIncidents is the subscriber path of the Potential SLA Breach job: instead of
// recomputing lineage it reads the incidents the root-cause publisher persists and sends
// the impacted teams a final warning. Repetition is suppressed by the alert_logs
// deduplication for potential_sla_breach, not by the sla_predictor store.
func (s *JobSLAPredictorService) identifyFromIncidents(ctx context.Context, combos []scheduler.SLABreachCombo, reqConfig JobSLAPredictorRequestConfig) (map[string]*scheduler.TargetBreach, error) {
	emptyResponse := map[string]*scheduler.TargetBreach{}

	incidents, err := s.activeIncidents(ctx, reqConfig)
	if err != nil {
		return emptyResponse, err
	}

	scope := map[tenant.ProjectName]bool{}
	for _, combo := range combos {
		scope[combo.ProjectName] = true
	}
	skipJobNames := map[scheduler.JobName]bool{}
	for _, skipJobName := range reqConfig.SkipJobNames {
		skipJobNames[scheduler.JobName(skipJobName)] = true
	}

	agg := scheduler.NewBreachAlertAggregator()
	response := map[string]*scheduler.TargetBreach{}
	teamCache := map[tenant.Tenant]string{}
	detailsCache := map[scheduler.JobName]*scheduler.JobWithDetails{}

	for _, incident := range incidents {
		for _, reason := range incident.Incident.ActiveReasons() {
			rootCause := subscriberRootCause(incident, reason)
			for _, impacted := range incident.Impacts {
				if len(scope) > 0 && !scope[impacted.ProjectName] {
					continue
				}
				if skipJobNames[impacted.JobName] {
					continue
				}

				details, ok := s.impactedJobDetails(ctx, impacted, detailsCache)
				if !ok {
					continue
				}
				slaDuration, err := details.SLADuration()
				if err != nil || slaDuration == 0 {
					s.l.Debug("impacted job has no SLA configured, skipping final warning", "job", impacted.JobName.String())
					continue
				}
				if details.Job == nil {
					s.l.Warn("impacted job has no tenant information, skipping final warning", "job", impacted.JobName.String())
					continue
				}
				team := s.resolveTeam(ctx, details.Job.Tenant, teamCache)
				if team == "" {
					continue
				}

				impactedName := impacted.ProjectName.String() + "/" + impacted.JobName.String()
				agg.Add(team, impactedName, rootCause, impacted.ProjectName.String(), reqConfig.Severity)

				// the response carries the impacted run's own SLA deadline
				state := *rootCause
				inferredSLA := impacted.ScheduledAt.Add(time.Duration(slaDuration) * time.Second)
				state.InferredSLA = &inferredSLA

				key := impacted.ProjectName.String() + "/" + impacted.JobName.String()
				breach, ok := response[key]
				if !ok {
					breach = &scheduler.TargetBreach{
						TargetProject: impacted.ProjectName.String(),
						TargetJobName: impacted.JobName,
						Upstreams:     map[scheduler.JobName]*scheduler.JobState{},
					}
					response[key] = breach
				}
				breach.Upstreams[rootCause.JobName] = &state
			}
		}
	}

	if reqConfig.EnableAlert {
		for _, alert := range agg.Build() {
			s.potentialSLANotifier.SendPotentialSLABreach(alert)
		}
	}
	return response, nil
}

// subscriberRootCause renders the incident's offending entity as a cause state for the
// shared aggregator and response mapping.
func subscriberRootCause(incident *scheduler.SLABreachIncidentWithImpacts, reason scheduler.RootCauseReasonDetail) *scheduler.JobState {
	return &scheduler.JobState{
		JobName: incident.Incident.Key.JobName,
		Tenant:  rootCauseTenant(incident.Incident.Key.ProjectName),
		JobRun: scheduler.JobRunSummary{
			JobName:     incident.Incident.Key.JobName,
			ScheduledAt: incident.Incident.Key.ScheduledAt,
		},
		Status: mapStatus(reason.Reason),
		Reason: mapReason(reason.Reason),
		Evidence: scheduler.RootCauseEvidence{
			InducedDelay:     time.Duration(reason.InducedDelaySeconds) * time.Second,
			BlockedOnSensors: reason.BlockedOnSensors,
			SourceType:       reason.SourceType,
		},
	}
}

// impactedJobDetails fetches the impacted job once per tick: its tenant resolves the
// alerting team and its SLA config the deadline for the response.
func (s *JobSLAPredictorService) impactedJobDetails(ctx context.Context, impacted scheduler.ImpactedJob, cache map[scheduler.JobName]*scheduler.JobWithDetails) (*scheduler.JobWithDetails, bool) {
	if details, cached := cache[impacted.JobName]; cached {
		return details, details != nil
	}

	jobs, err := s.jobDetailsGetter.GetJobs(ctx, impacted.ProjectName, []string{impacted.JobName.String()})
	if err != nil {
		s.l.Error("failed to fetch impacted job for final warning, skipping", "job", impacted.JobName.String(), "error", err)
		cache[impacted.JobName] = nil
		return nil, false
	}
	if len(jobs) == 0 {
		s.l.Warn("impacted job no longer exists, skipping final warning", "job", impacted.JobName.String())
		cache[impacted.JobName] = nil
		return nil, false
	}

	cache[impacted.JobName] = jobs[0]
	return jobs[0], true
}
