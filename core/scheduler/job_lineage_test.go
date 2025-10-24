package scheduler_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/goto/optimus/core/scheduler"
	"github.com/goto/optimus/core/tenant"
	"github.com/goto/optimus/internal/lib/window"
)

func TestJobLineageSummary_Flatten(t *testing.T) {
	projName := tenant.ProjectName("proj")
	namespaceName := tenant.ProjectName("ns1")
	tnnt, _ := tenant.NewTenant(projName.String(), namespaceName.String())

	baseTime := time.Date(2023, 1, 1, 10, 0, 0, 0, time.UTC)
	windowConfig, _ := window.NewPresetConfig("yesterday")

	t.Run("should handle nested upstream lineage", func(t *testing.T) {
		upstreamLevel2A := createJobLineage("upstream_level2_a", tnnt, &windowConfig, nil, createJobRun(baseTime, baseTime.Add(0), baseTime.Add(10*time.Minute)))
		upstreamLevel2B := createJobLineage("upstream_level2_b", tnnt, &windowConfig, nil, createJobRun(baseTime, baseTime.Add(0), baseTime.Add(8*time.Minute)))
		upstream1 := createJobLineage("upstream1", tnnt, &windowConfig, []*scheduler.JobLineageSummary{upstreamLevel2A}, createJobRun(baseTime, baseTime.Add(0), baseTime.Add(15*time.Minute)))
		upstream2 := createJobLineage("upstream2", tnnt, &windowConfig, []*scheduler.JobLineageSummary{upstreamLevel2B}, createJobRun(baseTime, baseTime.Add(0), baseTime.Add(25*time.Minute)))
		jobLineage := createJobLineage("job1", tnnt, &windowConfig, []*scheduler.JobLineageSummary{upstream1, upstream2}, createJobRun(baseTime, baseTime.Add(0), baseTime.Add(30*time.Minute)))

		maxDepth := 5
		jobRunSums := jobLineage.Flatten(maxDepth)
		assert.Len(t, jobRunSums, 5)

		jobNames := make(map[scheduler.JobName]bool)
		for _, summary := range jobRunSums {
			jobNames[summary.JobName] = true
		}
		assert.True(t, jobNames[scheduler.JobName("upstream_level2_a")])
		assert.True(t, jobNames[scheduler.JobName("upstream_level2_b")])
		assert.True(t, jobNames[scheduler.JobName("upstream1")])
		assert.True(t, jobNames[scheduler.JobName("upstream2")])
		assert.True(t, jobNames[scheduler.JobName("job1")])
	})

	t.Run("should handle flatten limited to max depth", func(t *testing.T) {
		level4 := createJobLineage("level4", tnnt, &windowConfig, nil, createJobRun(baseTime, baseTime.Add(0), baseTime.Add(10*time.Minute)))
		level3 := createJobLineage("level3", tnnt, &windowConfig, []*scheduler.JobLineageSummary{level4}, createJobRun(baseTime, baseTime.Add(0), baseTime.Add(20*time.Minute)))
		level2 := createJobLineage("level2", tnnt, &windowConfig, []*scheduler.JobLineageSummary{level3}, createJobRun(baseTime, baseTime.Add(0), baseTime.Add(30*time.Minute)))
		level1 := createJobLineage("level1", tnnt, &windowConfig, []*scheduler.JobLineageSummary{level2}, createJobRun(baseTime, baseTime.Add(0), baseTime.Add(40*time.Minute)))
		root := createJobLineage("root", tnnt, &windowConfig, []*scheduler.JobLineageSummary{level1}, createJobRun(baseTime, baseTime.Add(0), baseTime.Add(50*time.Minute)))

		maxDepth := 2
		jobRunSums := root.Flatten(maxDepth)
		assert.Len(t, jobRunSums, 2)

		assert.Equal(t, jobRunSums[0].JobName, scheduler.JobName("root"))
		assert.Equal(t, jobRunSums[1].JobName, scheduler.JobName("level1"))
	})
}

func createJobLineage(name string, tnnt tenant.Tenant, windowConfig *window.Config, upstreams []*scheduler.JobLineageSummary, runSummary *scheduler.JobRunSummary) *scheduler.JobLineageSummary {
	return &scheduler.JobLineageSummary{
		JobName:          scheduler.JobName(name),
		Tenant:           tnnt,
		ScheduleInterval: "0 8 * * *",
		Window:           windowConfig,
		SLA:              scheduler.SLAConfig{Duration: time.Hour},
		Upstreams:        upstreams,
		JobRuns: map[string]*scheduler.JobRunSummary{
			runSummary.ScheduledAt.UTC().Format(time.RFC3339): runSummary,
		},
	}
}

func createJobRun(baseTime, startTime, endTime time.Time) *scheduler.JobRunSummary {
	return &scheduler.JobRunSummary{
		ScheduledAt:  baseTime,
		JobStartTime: &startTime,
		JobEndTime:   &endTime,
	}
}

func TestJobLineageSummary_PruneLineage(t *testing.T) {
	projName := tenant.ProjectName("proj")
	namespaceName := tenant.ProjectName("ns1")
	tnnt, _ := tenant.NewTenant(projName.String(), namespaceName.String())

	baseTime := time.Date(2023, 1, 1, 10, 0, 0, 0, time.UTC)
	windowConfig, _ := window.NewPresetConfig("yesterday")

	t.Run("should handle nested lineage with pruning", func(t *testing.T) {
		level3A := createJobLineage("level3a", tnnt, &windowConfig, nil, createJobRun(baseTime, baseTime.Add(0), baseTime.Add(5*time.Minute)))
		level3B := createJobLineage("level3b", tnnt, &windowConfig, nil, createJobRun(baseTime, baseTime.Add(0), baseTime.Add(15*time.Minute)))
		level3C := createJobLineage("level3c", tnnt, &windowConfig, nil, createJobRun(baseTime, baseTime.Add(0), baseTime.Add(25*time.Minute)))

		level2A := createJobLineage("level2a", tnnt, &windowConfig, []*scheduler.JobLineageSummary{level3A, level3B}, createJobRun(baseTime, baseTime.Add(0), baseTime.Add(50*time.Minute)))
		level2B := createJobLineage("level2b", tnnt, &windowConfig, []*scheduler.JobLineageSummary{level3C}, createJobRun(baseTime, baseTime.Add(0), baseTime.Add(45*time.Minute)))
		level2C := createJobLineage("level2c", tnnt, &windowConfig, nil, createJobRun(baseTime, baseTime.Add(0), baseTime.Add(40*time.Minute)))

		rootJob := createJobLineage("root", tnnt, &windowConfig, []*scheduler.JobLineageSummary{level2A, level2B, level2C}, createJobRun(baseTime, baseTime.Add(0), baseTime.Add(50*time.Minute)))

		maxUpstreamsPerLevel := 1
		maxDepth := 5
		pruned := rootJob.PruneLineage(maxUpstreamsPerLevel, maxDepth)

		assert.Equal(t, scheduler.JobName("root"), pruned.JobName)
		assert.Len(t, pruned.Upstreams, 1)

		// check for level 1 upstream
		assert.Equal(t, scheduler.JobName("level2a"), pruned.Upstreams[0].JobName)

		level2aNode := pruned.Upstreams[0]
		assert.Len(t, level2aNode.Upstreams, 1)
		assert.Equal(t, scheduler.JobName("level3b"), level2aNode.Upstreams[0].JobName)
	})

	t.Run("should handle pruned lineage by depth", func(t *testing.T) {
		level4 := createJobLineage("level4", tnnt, &windowConfig, nil, createJobRun(baseTime, baseTime.Add(0), baseTime.Add(10*time.Minute)))
		level3 := createJobLineage("level3", tnnt, &windowConfig, []*scheduler.JobLineageSummary{level4}, createJobRun(baseTime, baseTime.Add(0), baseTime.Add(20*time.Minute)))
		level2 := createJobLineage("level2", tnnt, &windowConfig, []*scheduler.JobLineageSummary{level3}, createJobRun(baseTime, baseTime.Add(0), baseTime.Add(30*time.Minute)))
		level1 := createJobLineage("level1", tnnt, &windowConfig, []*scheduler.JobLineageSummary{level2}, createJobRun(baseTime, baseTime.Add(0), baseTime.Add(40*time.Minute)))
		root := createJobLineage("root", tnnt, &windowConfig, []*scheduler.JobLineageSummary{level1}, createJobRun(baseTime, baseTime.Add(0), baseTime.Add(50*time.Minute)))

		maxUpstreamsPerLevel := 5
		maxDepth := 2
		pruned := root.PruneLineage(maxUpstreamsPerLevel, maxDepth)

		assert.Equal(t, scheduler.JobName("root"), pruned.JobName)
		assert.Len(t, pruned.Upstreams, 1)
		assert.Equal(t, scheduler.JobName("level1"), pruned.Upstreams[0].JobName)
		assert.Len(t, pruned.Upstreams[0].Upstreams, 1)
		assert.Equal(t, scheduler.JobName("level2"), pruned.Upstreams[0].Upstreams[0].JobName)
		assert.Len(t, pruned.Upstreams[0].Upstreams[0].Upstreams, 0)
	})

	t.Run("should handle empty upstreams", func(t *testing.T) {
		jobLineage := createJobLineage("job1", tnnt, &windowConfig, nil, createJobRun(baseTime, baseTime.Add(0), baseTime.Add(30*time.Minute)))

		maxUpstreamsPerLevel := 5
		maxDepth := 5
		pruned := jobLineage.PruneLineage(maxUpstreamsPerLevel, maxDepth)

		assert.Equal(t, scheduler.JobName("job1"), pruned.JobName)
		assert.Len(t, pruned.Upstreams, 0)
	})

	t.Run("should handle circular references", func(t *testing.T) {
		upstream1 := createJobLineage("upstream1", tnnt, &windowConfig, nil, createJobRun(baseTime, baseTime.Add(0), baseTime.Add(10*time.Minute)))
		upstream2 := createJobLineage("upstream2", tnnt, &windowConfig, []*scheduler.JobLineageSummary{upstream1}, createJobRun(baseTime, baseTime.Add(0), baseTime.Add(20*time.Minute)))

		upstream1.Upstreams = []*scheduler.JobLineageSummary{upstream2}

		root := createJobLineage("root", tnnt, &windowConfig, []*scheduler.JobLineageSummary{upstream1}, createJobRun(baseTime, baseTime.Add(0), baseTime.Add(30*time.Minute)))

		maxUpstreamsPerLevel := 5
		maxDepth := 5
		pruned := root.PruneLineage(maxUpstreamsPerLevel, maxDepth)

		assert.Equal(t, scheduler.JobName("root"), pruned.JobName)
		assert.Len(t, pruned.Upstreams, 1)
		assert.Equal(t, scheduler.JobName("upstream1"), pruned.Upstreams[0].JobName)
	})
}
