package service

import (
	"context"
	"testing"
	"time"

	"github.com/goto/salt/log"
	"github.com/stretchr/testify/assert"

	"github.com/goto/optimus/core/scheduler"
	"github.com/goto/optimus/core/tenant"
)

// internal test file: the potential-breach annotation needs the injectable clock on the
// unexported service field for deterministic assertions.

type stubLineageBuilder struct {
	trees map[*scheduler.JobSchedule]*scheduler.JobLineageSummary
}

func (s stubLineageBuilder) BuildLineage(context.Context, []*scheduler.JobSchedule, int) (map[*scheduler.JobSchedule]*scheduler.JobLineageSummary, error) {
	return s.trees, nil
}

type stubDurationRepo struct {
	byJob map[scheduler.JobName]time.Duration
}

func (s stubDurationRepo) GetPercentileDurationByJobNames(_ context.Context, jobNames []scheduler.JobName, _ map[string][]string, _ time.Time, _, _ int) (map[scheduler.JobName]*time.Duration, error) {
	out := map[scheduler.JobName]*time.Duration{}
	for _, jobName := range jobNames {
		if duration, ok := s.byJob[jobName]; ok {
			duration := duration
			out[jobName] = &duration
		}
	}
	return out, nil
}

func potentialBreachFixture(now time.Time, upstreamTaskStart, upstreamTaskEnd *time.Time) (*JobLineageService, *scheduler.JobSchedule) {
	tnnt, _ := tenant.NewTenant("p_root", "ns1")
	targetSchedule := &scheduler.JobSchedule{JobName: "job_target", ScheduledAt: now.Add(-100 * time.Minute)}

	rcPtr := func(t time.Time) *time.Time { return &t }
	targetRun := &scheduler.JobRunSummary{
		JobName:       "job_target",
		ScheduledAt:   now.Add(-100 * time.Minute), // 09:00
		SLATime:       rcPtr(now.Add(20 * time.Minute)),
		JobStatus:     "running",
		TaskStartTime: rcPtr(now.Add(-100 * time.Minute)),
	}
	upstreamRun := &scheduler.JobRunSummary{
		JobName:       "job_upstream",
		ScheduledAt:   now.Add(-120 * time.Minute), // 08:00
		SLATime:       rcPtr(now.Add(-60 * time.Minute)),
		JobStatus:     "success",
		TaskStartTime: upstreamTaskStart,
		TaskEndTime:   upstreamTaskEnd,
	}

	upstreamTree := &scheduler.JobLineageSummary{
		JobName: "job_upstream", Tenant: tnnt, IsEnabled: true,
		JobRuns: map[scheduler.JobName]*scheduler.JobRunSummary{"job_upstream": upstreamRun},
	}
	tree := &scheduler.JobLineageSummary{
		JobName: "job_target", Tenant: tnnt, IsEnabled: true,
		JobRuns:   map[scheduler.JobName]*scheduler.JobRunSummary{"job_target": targetRun},
		Upstreams: []*scheduler.JobLineageSummary{upstreamTree},
	}
	upstreamTree.JobRuns["job_target"] = upstreamRun

	svc := &JobLineageService{
		l:                            log.NewNoop(),
		lineageBuilder:               stubLineageBuilder{trees: map[*scheduler.JobSchedule]*scheduler.JobLineageSummary{targetSchedule: tree}},
		durationEstimator:            stubDurationRepo{byJob: map[scheduler.JobName]time.Duration{"job_target": 30 * time.Minute, "job_upstream": 20 * time.Minute}},
		maxLineageDepth:              25,
		lineageWindowHours:           24,
		historicalDurationLastNRuns:  7,
		historicalDurationPercentile: 95,
		potentialBreachEnabled:       true,
		damperFactor: scheduler.DamperFactor{
			BaseValue: 1.0, Alpha: 1.0, MinValue: 0,
			Method: scheduler.DamperFactorMethodExponential,
		},
		now: func() time.Time { return now },
	}
	return svc, targetSchedule
}

// expected inferred deadline for the upstream with a 1.0 damper:
// target SLA (now+20m) minus the target's own 30m estimate = now-10m
var upstreamInferredDeadlineOffset = -10 * time.Minute

func TestJobLineageService_PotentialBreachAnnotation(t *testing.T) {
	referenceTime := time.Date(2026, 11, 5, 10, 40, 0, 0, time.UTC)

	t.Run("an upstream still running past its inferred deadline is a potential breach cause", func(t *testing.T) {
		started := referenceTime.Add(-100 * time.Minute) // 09:00, still going
		svc, targetSchedule := potentialBreachFixture(referenceTime, &started, nil)

		lineages, err := svc.GetJobExecutionSummary(context.Background(), []*scheduler.JobSchedule{targetSchedule}, scheduler.LineageSummaryOptions{})
		assert.NoError(t, err)
		assert.Len(t, lineages, 1)

		lineage := lineages[0]
		assert.True(t, lineage.IsPotentiallyBreaching, "the target's deadline has not passed but the chain already endangers it")

		byJob := map[scheduler.JobName]*scheduler.JobExecutionSummary{}
		for _, node := range lineage.JobRuns {
			byJob[node.JobName] = node
		}
		upstream := byJob["job_upstream"]
		assert.NotNil(t, upstream.InferredSLA)
		assert.Equal(t, referenceTime.Add(upstreamInferredDeadlineOffset), *upstream.InferredSLA)
		assert.True(t, upstream.IsPotentialBreachCause)
		assert.False(t, byJob["job_target"].IsPotentialBreachCause, "the target itself is within its deadline")
	})

	t.Run("an upstream that finished before its inferred deadline is not a cause", func(t *testing.T) {
		started := referenceTime.Add(-100 * time.Minute)
		finished := referenceTime.Add(-50 * time.Minute) // ends at now-50m, before the now-10m deadline
		svc, targetSchedule := potentialBreachFixture(referenceTime, &started, &finished)

		lineages, err := svc.GetJobExecutionSummary(context.Background(), []*scheduler.JobSchedule{targetSchedule}, scheduler.LineageSummaryOptions{})
		assert.NoError(t, err)

		lineage := lineages[0]
		assert.False(t, lineage.IsPotentiallyBreaching)
		for _, node := range lineage.JobRuns {
			assert.False(t, node.IsPotentialBreachCause, node.JobName)
		}
	})

	t.Run("an upstream run missing entirely is a potential cause once past its latest start", func(t *testing.T) {
		svc, targetSchedule := potentialBreachFixture(referenceTime, nil, nil) // upstream never started

		lineages, err := svc.GetJobExecutionSummary(context.Background(), []*scheduler.JobSchedule{targetSchedule}, scheduler.LineageSummaryOptions{})
		assert.NoError(t, err)

		lineage := lineages[0]
		assert.True(t, lineage.IsPotentiallyBreaching)
		for _, node := range lineage.JobRuns {
			if node.JobName == "job_upstream" {
				assert.True(t, node.IsPotentialBreachCause, "it should have started by now-30m")
			}
		}
	})

	t.Run("a target past its own SLA is actual, not potential", func(t *testing.T) {
		started := referenceTime.Add(-100 * time.Minute)
		svc, targetSchedule := potentialBreachFixture(referenceTime, &started, nil)

		// push the reference time past the target's SLA deadline (now+20m)
		late := referenceTime.Add(30 * time.Minute)
		svc.now = func() time.Time { return late }

		lineages, err := svc.GetJobExecutionSummary(context.Background(), []*scheduler.JobSchedule{targetSchedule}, scheduler.LineageSummaryOptions{})
		assert.NoError(t, err)

		lineage := lineages[0]
		assert.False(t, lineage.IsPotentiallyBreaching, "an actual breach is the reactive path's business")
	})

	t.Run("prediction disabled leaves the lineage untouched", func(t *testing.T) {
		started := referenceTime.Add(-100 * time.Minute)
		svc, targetSchedule := potentialBreachFixture(referenceTime, &started, nil)
		svc.potentialBreachEnabled = false

		lineages, err := svc.GetJobExecutionSummary(context.Background(), []*scheduler.JobSchedule{targetSchedule}, scheduler.LineageSummaryOptions{})
		assert.NoError(t, err)

		lineage := lineages[0]
		assert.False(t, lineage.IsPotentiallyBreaching)
		for _, node := range lineage.JobRuns {
			assert.Nil(t, node.InferredSLA)
			assert.False(t, node.IsPotentialBreachCause)
		}
	})
}
