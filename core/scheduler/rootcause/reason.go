package rootcause

import (
	"strings"
	"time"

	"github.com/goto/optimus/core/scheduler"
)

func sensorPrefixFor(thirdPartyType string) string {
	return "wait_" + thirdPartyType + "_"
}

// Candidate is one root cause job plus the run facts needed to explain it.
type Candidate struct {
	State                       *scheduler.JobState
	PendingSensors              []string
	ReferenceTime               time.Time
	ThirdPartyDelayStartHourUTC int
	ThirdPartyDelayThreshold    time.Duration
}

func (c Candidate) thirdPartyDelayExceedsThreshold() bool {
	if c.State == nil {
		return false
	}
	return thirdPartyDelayExceedsThreshold(*c.State, c.ReferenceTime, c.ThirdPartyDelayStartHourUTC, c.ThirdPartyDelayThreshold)
}

// ReasonDetector is the extension point: reasons are added by appending a detector
// rather than by editing a switch. Every match is scored; the max induced delay wins.
type ReasonDetector interface {
	Name() string
	Detect(c Candidate) (scheduler.RootCauseReason, scheduler.RootCauseEvidence, bool)
}

// DefaultDetectors is the set of explanations considered for each candidate.
// A job can match more than one (e.g. STARTED_LATE and RUNNING_LONG); identification
// keeps the match with the largest induced delay.
// thirdPartyTypes comes from the server's upstream_resolvers. Empty means the
// deployment has no third-party sensors, so THIRD_PARTY_DELAY never fires.
func DefaultDetectors(thirdPartyTypes []string) []ReasonDetector {
	return []ReasonDetector{
		NewResolvedThirdPartySensorDetector(thirdPartyTypes),
		RunningLongDetector{},
		StartedLateDetector{},
		NewThirdPartySensorDetector(thirdPartyTypes),
	}
}

func thirdPartyPrefixes(thirdPartyTypes []string) map[string]string {
	typeByPrefix := make(map[string]string, len(thirdPartyTypes))
	for _, thirdPartyType := range thirdPartyTypes {
		if thirdPartyType == "" {
			continue
		}
		typeByPrefix[sensorPrefixFor(thirdPartyType)] = thirdPartyType
	}
	return typeByPrefix
}

type RunningLongDetector struct{}

func (RunningLongDetector) Name() string { return "running_long" }

func (RunningLongDetector) Detect(c Candidate) (scheduler.RootCauseReason, scheduler.RootCauseEvidence, bool) {
	start := c.State.JobRun.TaskStartTime
	if start == nil || c.State.EstimatedDuration == nil {
		return "", scheduler.RootCauseEvidence{}, false
	}
	elapsed := c.ReferenceTime.Sub(*start)
	// a run that finished late is still a root cause
	if end := c.State.JobRun.GetActualEndTime(); end != nil {
		elapsed = end.Sub(*start)
	}
	if elapsed <= *c.State.EstimatedDuration {
		return "", scheduler.RootCauseEvidence{}, false
	}
	return scheduler.ReasonRunningLong, evidenceForStarted(c), true
}

type StartedLateDetector struct{}

func (StartedLateDetector) Name() string { return "started_late" }

// A deadline derived from the job's own history cannot be missed by a job that both
// started on time and ran at that speed, so a normal-speed breach means a late start.
func (StartedLateDetector) Detect(c Candidate) (scheduler.RootCauseReason, scheduler.RootCauseEvidence, bool) {
	start := c.State.JobRun.TaskStartTime
	if start == nil || c.State.EstimatedDuration == nil || c.State.InferredSLA == nil {
		return "", scheduler.RootCauseEvidence{}, false
	}
	if !start.After(c.State.InferredSLA.Add(-*c.State.EstimatedDuration)) {
		return "", scheduler.RootCauseEvidence{}, false
	}
	return scheduler.ReasonStartedLate, evidenceForStarted(c), true
}

// ResolvedThirdPartySensorDetector catches a finished third-party wait whose induced delay
// exceeds the configured seconds threshold. wait_dex is typically a seconds-long API
// completeness check, so a wait at or under the threshold is treated as normal gating
// and must not steal STARTED_LATE / RUNNING_LONG.
type ResolvedThirdPartySensorDetector struct {
	typeByPrefix map[string]string
}

func NewResolvedThirdPartySensorDetector(thirdPartyTypes []string) ResolvedThirdPartySensorDetector {
	return ResolvedThirdPartySensorDetector{typeByPrefix: thirdPartyPrefixes(thirdPartyTypes)}
}

func (ResolvedThirdPartySensorDetector) Name() string { return "resolved_third_party_delay" }

func (d ResolvedThirdPartySensorDetector) Detect(c Candidate) (scheduler.RootCauseReason, scheduler.RootCauseEvidence, bool) {
	sensorName := c.State.JobRun.SensorName
	if sensorName == nil || c.State.JobRun.WaitStartTime == nil || c.State.JobRun.WaitEndTime == nil {
		return "", scheduler.RootCauseEvidence{}, false
	}
	if !c.thirdPartyDelayExceedsThreshold() {
		return "", scheduler.RootCauseEvidence{}, false
	}
	for prefix, thirdPartyType := range d.typeByPrefix {
		if strings.HasPrefix(*sensorName, prefix) {
			return scheduler.ReasonThirdPartyDelay, scheduler.RootCauseEvidence{
				BlockedOnSensors: []string{*sensorName},
				SourceType:       thirdPartyType,
			}, true
		}
	}
	return "", scheduler.RootCauseEvidence{}, false
}

// ThirdPartySensorDetector only ever sees a sensor that is still pending.
type ThirdPartySensorDetector struct {
	typeByPrefix map[string]string
}

func NewThirdPartySensorDetector(thirdPartyTypes []string) ThirdPartySensorDetector {
	return ThirdPartySensorDetector{typeByPrefix: thirdPartyPrefixes(thirdPartyTypes)}
}

func (ThirdPartySensorDetector) Name() string { return "third_party_delay" }

// A pending third-party sensor is itself the signal that the source data has not
// landed, so no call out to the third party is needed to reach this verdict.
func (d ThirdPartySensorDetector) Detect(c Candidate) (scheduler.RootCauseReason, scheduler.RootCauseEvidence, bool) {
	if c.State.JobRun.TaskStartTime != nil {
		return "", scheduler.RootCauseEvidence{}, false
	}
	blocked := make([]string, 0, len(c.PendingSensors))
	sourceType := ""
	for _, sensor := range c.PendingSensors {
		for prefix, thirdPartyType := range d.typeByPrefix {
			if strings.HasPrefix(sensor, prefix) {
				blocked = append(blocked, sensor)
				sourceType = thirdPartyType
				break
			}
		}
	}
	if len(blocked) == 0 {
		return "", scheduler.RootCauseEvidence{}, false
	}
	if !c.thirdPartyDelayExceedsThreshold() {
		return "", scheduler.RootCauseEvidence{}, false
	}
	return scheduler.ReasonThirdPartyDelay, scheduler.RootCauseEvidence{BlockedOnSensors: blocked, SourceType: sourceType}, true
}

func evidenceForStarted(c Candidate) scheduler.RootCauseEvidence {
	evidence := scheduler.RootCauseEvidence{StartedAt: c.State.JobRun.TaskStartTime}
	if c.State.EstimatedDuration != nil {
		finish := c.State.JobRun.TaskStartTime.Add(*c.State.EstimatedDuration)
		evidence.ExpectedFinishAt = &finish
		if c.State.InferredSLA != nil {
			safeStart := c.State.InferredSLA.Add(-*c.State.EstimatedDuration)
			evidence.LatestSafeStartAt = &safeStart
		}
	}
	return evidence
}

type classified struct {
	Reason   scheduler.RootCauseReason
	Evidence scheduler.RootCauseEvidence
}

func classifyAll(detectors []ReasonDetector, c Candidate) []classified {
	matches := make([]classified, 0, len(detectors))
	for _, detector := range detectors {
		if reason, evidence, ok := detector.Detect(c); ok {
			matches = append(matches, classified{Reason: reason, Evidence: evidence})
		}
	}
	return matches
}
