//go:build unit_test

package rootcause_test

import (
	"context"
	"testing"
	"time"

	"github.com/goto/salt/log"
	"github.com/stretchr/testify/assert"

	"github.com/goto/optimus/core/scheduler"
	"github.com/goto/optimus/core/scheduler/rootcause"
	"github.com/goto/optimus/core/tenant"
)

type fakeScheduledChangeGetter struct{}

func (fakeScheduledChangeGetter) GetRecentScheduleChange(context.Context, scheduler.JobName, tenant.Tenant, time.Time) (string, error) {
	return "", nil
}

type fakePendingSensorGetter struct {
	byKey map[string][]string
}

func (f fakePendingSensorGetter) GetPendingSensors(_ context.Context, keys []rootcause.JobRunKey) (map[string][]string, error) {
	out := make(map[string][]string, len(keys))
	for _, k := range keys {
		if sensors, ok := f.byKey[k.String()]; ok {
			out[k.String()] = sensors
		}
	}
	return out, nil
}

// TestIdentifier_EscalatesStartedLate covers root-cause identification for a job that
// breaches its own inferred SLA only because of a gap absorbed by an upstream that never
// breaches its own (looser) threshold - the case the SLA-threshold-only algorithm misses,
// since it only checks whether a job's *direct* upstream also breached, not whether that
// upstream's own execution was itself abnormal.
func TestIdentifier_EscalatesStartedLate(t *testing.T) {
	tnnt, err := tenant.NewTenant("project-a", "team-a")
	assert.NoError(t, err)

	scheduledAt := time.Date(2026, 9, 10, 10, 0, 0, 0, time.UTC)
	targetSLA := scheduledAt.Add(30 * time.Minute)
	dur := func(d time.Duration) *time.Duration { return &d }
	at := func(m int) *time.Time {
		v := scheduledAt.Add(time.Duration(m) * time.Minute)
		return &v
	}

	damper := scheduler.DamperFactor{Method: scheduler.DamperFactorMethodConstant, BaseValue: 1.0}

	newIdentifier := func(pendingSensors map[string][]string) *rootcause.Identifier {
		var sensorGetter rootcause.PendingSensorGetter
		if pendingSensors != nil {
			sensorGetter = fakePendingSensorGetter{byKey: pendingSensors}
		}
		return rootcause.NewIdentifier(log.NewNoop(), fakeScheduledChangeGetter{}, sensorGetter, []string{"dex"})
	}

	buildLineage := func(upstreamRun *scheduler.JobRunSummary, targetRun *scheduler.JobRunSummary) *scheduler.JobLineageSummary {
		upstream := &scheduler.JobLineageSummary{
			JobName:   "job-B",
			IsEnabled: true,
			Tenant:    tnnt,
			JobRuns:   map[scheduler.JobName]*scheduler.JobRunSummary{"job-A": upstreamRun},
		}
		return &scheduler.JobLineageSummary{
			JobName:   "job-A",
			IsEnabled: true,
			Tenant:    tnnt,
			Upstreams: []*scheduler.JobLineageSummary{upstream},
			JobRuns:   map[scheduler.JobName]*scheduler.JobRunSummary{"job-A": targetRun},
		}
	}

	jobDurations := map[scheduler.JobName]*time.Duration{
		"job-A": dur(10 * time.Minute),
		"job-B": dur(5 * time.Minute),
	}

	t.Run("upstream running long but under its own threshold escalates identity to it", func(t *testing.T) {
		// job-B: estimate 5m, own inferred SLA +20m; ran 0->17m (well past its own estimate,
		// but still finishes before its own +20m deadline, so the threshold-only algorithm
		// never flags it).
		upstreamRun := &scheduler.JobRunSummary{
			ScheduledAt:   scheduledAt,
			TaskStartTime: at(0),
			TaskEndTime:   at(17),
			JobEndTime:    at(17),
		}
		// job-A starts 5m after job-B finishes (an unmodeled trigger/poke gap), landing after
		// its own safe-start (+20m) even though it hasn't run long itself.
		targetRun := &scheduler.JobRunSummary{
			ScheduledAt:   scheduledAt,
			TaskStartTime: at(22),
		}
		lineage := buildLineage(upstreamRun, targetRun)

		causes, _ := newIdentifier(nil).Identify(context.Background(), lineage, jobDurations, &targetSLA, nil, damper, scheduledAt.Add(32*time.Minute))

		assert.Len(t, causes, 1)
		cause, ok := causes["job-B"]
		assert.True(t, ok, "root cause should have escalated to job-B, got: %+v", causes)
		assert.Equal(t, scheduler.ReasonRunningLong, cause.Reason)
		assert.Equal(t, scheduler.JobName("job-B"), cause.JobName)
	})

	t.Run("upstream clean under its own threshold leaves the original job as the cause", func(t *testing.T) {
		// job-B finishes comfortably within both its own estimate and its own deadline.
		upstreamRun := &scheduler.JobRunSummary{
			ScheduledAt:   scheduledAt,
			TaskStartTime: at(0),
			TaskEndTime:   at(5),
			JobEndTime:    at(5),
		}
		targetRun := &scheduler.JobRunSummary{
			ScheduledAt:   scheduledAt,
			TaskStartTime: at(22),
		}
		lineage := buildLineage(upstreamRun, targetRun)

		causes, _ := newIdentifier(nil).Identify(context.Background(), lineage, jobDurations, &targetSLA, nil, damper, scheduledAt.Add(32*time.Minute))

		assert.Len(t, causes, 1)
		cause, ok := causes["job-A"]
		assert.True(t, ok, "root cause should stay job-A when the upstream is clean, got: %+v", causes)
		assert.Equal(t, scheduler.ReasonStartedLate, cause.Reason)
	})

	t.Run("upstream blocked on a configured third-party sensor escalates identity to it", func(t *testing.T) {
		// job-B has not started at all, and is blocked on a pending dex sensor.
		upstreamRun := &scheduler.JobRunSummary{ScheduledAt: scheduledAt}
		targetRun := &scheduler.JobRunSummary{
			ScheduledAt:   scheduledAt,
			TaskStartTime: at(22),
		}
		lineage := buildLineage(upstreamRun, targetRun)

		key := rootcause.JobRunKey{Project: tnnt.ProjectName(), JobName: "job-B", ScheduledAt: scheduledAt}
		identifier := newIdentifier(map[string][]string{key.String(): {"wait_dex_some-topic"}})

		causes, _ := identifier.Identify(context.Background(), lineage, jobDurations, &targetSLA, nil, damper, scheduledAt.Add(32*time.Minute))

		assert.Len(t, causes, 1)
		cause, ok := causes["job-B"]
		assert.True(t, ok, "root cause should have escalated to job-B, got: %+v", causes)
		assert.Equal(t, scheduler.ReasonThirdPartyDelay, cause.Reason)
		assert.Equal(t, []string{"wait_dex_some-topic"}, cause.Evidence.BlockedOnSensors)
		assert.Equal(t, "dex", cause.Evidence.SourceType)
	})
}
