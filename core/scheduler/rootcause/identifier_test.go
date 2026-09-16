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
		return rootcause.NewIdentifier(log.NewNoop(), fakeScheduledChangeGetter{}, sensorGetter, []string{"dex"}, rootcause.IdentifierConfig{})
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

	t.Run("started late and running long on the same leaf keeps the larger delay", func(t *testing.T) {
		// job-A started 15m after its safe start (10:20) and has already overrun its 10m
		// estimate by 8m at 10:40. job-B finished by 10:20 so it is not an SLA leaf, but
		// escalate still scores it as RUNNING_LONG (15m overrun vs 5m estimate) which beats
		// both of job-A's own delays.
		upstreamRun := &scheduler.JobRunSummary{
			ScheduledAt:   scheduledAt,
			TaskStartTime: at(0),
			TaskEndTime:   at(20),
			JobEndTime:    at(20),
		}
		targetRun := &scheduler.JobRunSummary{
			ScheduledAt:   scheduledAt,
			TaskStartTime: at(22),
		}
		lineage := buildLineage(upstreamRun, targetRun)

		causes, _ := newIdentifier(nil).Identify(context.Background(), lineage, jobDurations, &targetSLA, nil, damper, scheduledAt.Add(40*time.Minute))

		assert.Len(t, causes, 1)
		cause, ok := causes["job-B"]
		assert.True(t, ok, "max delay should be job-B's overrun, got: %+v", causes)
		assert.Equal(t, scheduler.ReasonRunningLong, cause.Reason)
		assert.Equal(t, 15*time.Minute, cause.Evidence.InducedDelay)
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
		cause, ok := causes["wait_dex_some-topic"]
		assert.True(t, ok, "root cause identity should be the dex sensor, got: %+v", causes)
		assert.Equal(t, scheduler.ReasonThirdPartyDelay, cause.Reason)
		assert.Equal(t, scheduler.JobName("wait_dex_some-topic"), cause.JobName)
		assert.Equal(t, scheduler.JobName("job-B"), cause.JobRun.JobName)
		assert.Equal(t, []string{"wait_dex_some-topic"}, cause.Evidence.BlockedOnSensors)
		assert.Equal(t, "dex", cause.Evidence.SourceType)
	})

	t.Run("upstream wait above the delay threshold is attributed to the sensor", func(t *testing.T) {
		sensor := "wait_dex_orders"
		upstreamRun := &scheduler.JobRunSummary{
			ScheduledAt:   scheduledAt,
			SensorName:    &sensor,
			WaitStartTime: at(0),
			WaitEndTime:   at(20),
			TaskStartTime: at(20),
			TaskEndTime:   at(25),
			JobEndTime:    at(25),
		}
		targetRun := &scheduler.JobRunSummary{
			ScheduledAt:   scheduledAt,
			TaskStartTime: at(26),
		}
		lineage := buildLineage(upstreamRun, targetRun)
		identifier := rootcause.NewIdentifier(log.NewNoop(), fakeScheduledChangeGetter{}, nil, []string{"dex"}, rootcause.IdentifierConfig{})

		causes, _ := identifier.Identify(context.Background(), lineage, jobDurations, &targetSLA, nil, damper, scheduledAt.Add(32*time.Minute))

		assert.Len(t, causes, 1)
		cause, ok := causes[scheduler.JobName(sensor)]
		assert.True(t, ok, "root cause identity should be the dex sensor, got: %+v", causes)
		assert.Equal(t, scheduler.ReasonThirdPartyDelay, cause.Reason)
	})

	t.Run("max induced delay wins when two independent leaves exist", func(t *testing.T) {
		// job-B overran its estimate by 12m; job-C is still waiting on DEX since 10:00.
		// At 10:32 the third-party wait (32m after 10:00, counting from 00:00 UTC) beats B.
		runningLong := &scheduler.JobRunSummary{
			ScheduledAt:   scheduledAt,
			TaskStartTime: at(0),
			TaskEndTime:   at(25),
			JobEndTime:    at(25),
		}
		blocked := &scheduler.JobRunSummary{
			ScheduledAt:   scheduledAt,
			WaitStartTime: at(0),
		}
		targetRun := &scheduler.JobRunSummary{
			ScheduledAt:   scheduledAt,
			TaskStartTime: at(22),
		}
		jobC := &scheduler.JobLineageSummary{
			JobName:   "job-C",
			IsEnabled: true,
			Tenant:    tnnt,
			JobRuns:   map[scheduler.JobName]*scheduler.JobRunSummary{"job-A": blocked},
		}
		jobB := &scheduler.JobLineageSummary{
			JobName:   "job-B",
			IsEnabled: true,
			Tenant:    tnnt,
			JobRuns:   map[scheduler.JobName]*scheduler.JobRunSummary{"job-A": runningLong},
		}
		lineage := &scheduler.JobLineageSummary{
			JobName:   "job-A",
			IsEnabled: true,
			Tenant:    tnnt,
			Upstreams: []*scheduler.JobLineageSummary{jobB, jobC},
			JobRuns:   map[scheduler.JobName]*scheduler.JobRunSummary{"job-A": targetRun},
		}
		durations := map[scheduler.JobName]*time.Duration{
			"job-A": dur(10 * time.Minute),
			"job-B": dur(5 * time.Minute),
			"job-C": dur(5 * time.Minute),
		}
		key := rootcause.JobRunKey{Project: tnnt.ProjectName(), JobName: "job-C", ScheduledAt: scheduledAt}
		identifier := newIdentifier(map[string][]string{key.String(): {"wait_dex_orders"}})

		causes, _ := identifier.Identify(context.Background(), lineage, durations, &targetSLA, nil, damper, scheduledAt.Add(32*time.Minute))

		assert.Len(t, causes, 1)
		cause, ok := causes["wait_dex_orders"]
		assert.True(t, ok, "max-delay cause should be the dex sensor, got: %+v", causes)
		assert.Equal(t, scheduler.ReasonThirdPartyDelay, cause.Reason)
		assert.Greater(t, cause.Evidence.InducedDelay, 12*time.Minute)
	})

	t.Run("max delay below min_root_cause_delay keeps unknown reason", func(t *testing.T) {
		// job-B overran its 5m estimate by 12m; 12m is below a 13m floor.
		upstreamRun := &scheduler.JobRunSummary{
			ScheduledAt:   scheduledAt,
			TaskStartTime: at(0),
			TaskEndTime:   at(17),
			JobEndTime:    at(17),
		}
		targetRun := &scheduler.JobRunSummary{
			ScheduledAt:   scheduledAt,
			TaskStartTime: at(22),
		}
		lineage := buildLineage(upstreamRun, targetRun)
		identifier := rootcause.NewIdentifier(log.NewNoop(), fakeScheduledChangeGetter{}, nil, []string{"dex"}, rootcause.IdentifierConfig{
			MinRootCauseDelaySeconds: 13 * 60,
		})

		causes, paths := identifier.Identify(context.Background(), lineage, jobDurations, &targetSLA, nil, damper, scheduledAt.Add(32*time.Minute))

		assert.Len(t, causes, 1)
		cause, ok := causes["job-B"]
		assert.True(t, ok, "root cause identity should stay job-B, got: %+v", causes)
		assert.Equal(t, scheduler.ReasonUnknown, cause.Reason)
		assert.Equal(t, 12*time.Minute, cause.Evidence.InducedDelay)
		assert.Equal(t, scheduler.ReasonUnknown, paths["job-B"][len(paths["job-B"])-1].Reason)
	})

	t.Run("max delay at min_root_cause_delay keeps classified reason", func(t *testing.T) {
		upstreamRun := &scheduler.JobRunSummary{
			ScheduledAt:   scheduledAt,
			TaskStartTime: at(0),
			TaskEndTime:   at(17),
			JobEndTime:    at(17),
		}
		targetRun := &scheduler.JobRunSummary{
			ScheduledAt:   scheduledAt,
			TaskStartTime: at(22),
		}
		lineage := buildLineage(upstreamRun, targetRun)
		identifier := rootcause.NewIdentifier(log.NewNoop(), fakeScheduledChangeGetter{}, nil, []string{"dex"}, rootcause.IdentifierConfig{
			MinRootCauseDelaySeconds: 12 * 60,
		})

		causes, _ := identifier.Identify(context.Background(), lineage, jobDurations, &targetSLA, nil, damper, scheduledAt.Add(32*time.Minute))

		assert.Len(t, causes, 1)
		cause, ok := causes["job-B"]
		assert.True(t, ok, "root cause identity should stay job-B, got: %+v", causes)
		assert.Equal(t, scheduler.ReasonRunningLong, cause.Reason)
		assert.Equal(t, 12*time.Minute, cause.Evidence.InducedDelay)
	})
}
