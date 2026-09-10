package rootcause

import (
	"strings"
	"time"

	"github.com/goto/optimus/core/scheduler"
)

// DexSensorPrefix is how the DAG template names third-party sensor tasks:
// wait_<type>_<identifier>. Optimus stores that task_id verbatim in sensor_run.name.
const DexSensorPrefix = "wait_dex_"

// Candidate is one root cause job plus the run facts needed to explain it.
type Candidate struct {
	State          *scheduler.JobState
	PendingSensors []string
	ReferenceTime  time.Time
}

// ReasonDetector is the extension point: reasons are added by appending a detector
// rather than by editing a switch. The chain is ordered and the first match wins.
type ReasonDetector interface {
	Name() string
	Detect(c Candidate) (scheduler.RootCauseReason, scheduler.RootCauseEvidence, bool)
}

// DefaultDetectors returns the chain in precedence order. Overrunning beats starting
// late, because a job doing both is best described by the part still growing.
func DefaultDetectors(sensorPrefix string) []ReasonDetector {
	if sensorPrefix == "" {
		sensorPrefix = DexSensorPrefix
	}
	return []ReasonDetector{
		RunningLongDetector{},
		StartedLateDetector{},
		RawDataDelayDetector{SensorPrefix: sensorPrefix},
	}
}

type RunningLongDetector struct{}

func (RunningLongDetector) Name() string { return "running_long" }

func (RunningLongDetector) Detect(c Candidate) (scheduler.RootCauseReason, scheduler.RootCauseEvidence, bool) {
	start := c.State.JobRun.TaskStartTime
	if start == nil || c.State.EstimatedDuration == nil {
		return "", scheduler.RootCauseEvidence{}, false
	}
	if c.ReferenceTime.Sub(*start) <= *c.State.EstimatedDuration {
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

type RawDataDelayDetector struct {
	SensorPrefix string
}

func (RawDataDelayDetector) Name() string { return "raw_data_delay" }

// A pending third-party sensor is itself the signal that the raw data has not landed,
// so no call out to the third party is needed to reach this verdict.
func (d RawDataDelayDetector) Detect(c Candidate) (scheduler.RootCauseReason, scheduler.RootCauseEvidence, bool) {
	if c.State.JobRun.TaskStartTime != nil {
		return "", scheduler.RootCauseEvidence{}, false
	}
	blocked := make([]string, 0, len(c.PendingSensors))
	for _, sensor := range c.PendingSensors {
		if strings.HasPrefix(sensor, d.SensorPrefix) {
			blocked = append(blocked, sensor)
		}
	}
	if len(blocked) == 0 {
		return "", scheduler.RootCauseEvidence{}, false
	}
	return scheduler.ReasonRawDataDelay, scheduler.RootCauseEvidence{BlockedOnSensors: blocked}, true
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

func classify(detectors []ReasonDetector, c Candidate) (scheduler.RootCauseReason, scheduler.RootCauseEvidence) {
	for _, detector := range detectors {
		if reason, evidence, ok := detector.Detect(c); ok {
			return reason, evidence
		}
	}
	return scheduler.ReasonUnknown, scheduler.RootCauseEvidence{}
}
