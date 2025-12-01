package scheduler_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/goto/optimus/core/scheduler"
	"github.com/goto/optimus/core/tenant"
	"github.com/goto/optimus/internal/lib/window"
)

type downstreamJobNameAndRunPair struct {
	JobName   scheduler.JobName
	JobRunSum *scheduler.JobRunSummary
}

func createJobLineage(name string, tnnt tenant.Tenant, windowConfig *window.Config, upstreams []*scheduler.JobLineageSummary, runPairs ...downstreamJobNameAndRunPair) *scheduler.JobLineageSummary {
	summary := &scheduler.JobLineageSummary{
		JobName:          scheduler.JobName(name),
		Tenant:           tnnt,
		ScheduleInterval: "0 8 * * *",
		Window:           windowConfig,
		SLA:              scheduler.SLAConfig{Duration: time.Hour},
		Upstreams:        upstreams,
		JobRuns:          map[scheduler.JobName]*scheduler.JobRunSummary{},
	}

	for _, pair := range runPairs {
		summary.JobRuns[pair.JobName] = pair.JobRunSum
	}

	return summary
}

func createJobRunPair(baseTime, startTime, endTime time.Time, jobName string) downstreamJobNameAndRunPair {
	return downstreamJobNameAndRunPair{
		JobName:   scheduler.JobName(jobName),
		JobRunSum: createJobRun(baseTime, startTime, endTime),
	}
}

func createJobRun(baseTime, startTime, endTime time.Time) *scheduler.JobRunSummary {
	return &scheduler.JobRunSummary{
		ScheduledAt:  baseTime,
		JobStartTime: &startTime,
		JobEndTime:   &endTime,
		HookEndTime:  &endTime,
		TaskEndTime:  &endTime,
	}
}

func TestJobLineageSummary_Flatten(t *testing.T) {
	projName := tenant.ProjectName("proj")
	namespaceName := tenant.ProjectName("ns1")
	tnnt, _ := tenant.NewTenant(projName.String(), namespaceName.String())

	baseTime := time.Date(2023, 1, 1, 10, 0, 0, 0, time.UTC)
	windowConfig, _ := window.NewPresetConfig("yesterday")

	t.Run("should handle nested upstream lineage", func(t *testing.T) {
		upstreamLevel2A := createJobLineage("upstream_level2_a", tnnt, &windowConfig, nil, createJobRunPair(baseTime, baseTime.Add(0), baseTime.Add(10*time.Minute), "upstream1"))
		upstreamLevel2B := createJobLineage("upstream_level2_b", tnnt, &windowConfig, nil, createJobRunPair(baseTime, baseTime.Add(0), baseTime.Add(8*time.Minute), "upstream2"))
		upstream1 := createJobLineage("upstream1", tnnt, &windowConfig, []*scheduler.JobLineageSummary{upstreamLevel2A}, createJobRunPair(baseTime, baseTime.Add(0), baseTime.Add(15*time.Minute), "job1"))
		upstream2 := createJobLineage("upstream2", tnnt, &windowConfig, []*scheduler.JobLineageSummary{upstreamLevel2B}, createJobRunPair(baseTime, baseTime.Add(0), baseTime.Add(25*time.Minute), "job1"))
		jobLineage := createJobLineage("job1", tnnt, &windowConfig, []*scheduler.JobLineageSummary{upstream1, upstream2}, createJobRunPair(baseTime, baseTime.Add(0), baseTime.Add(30*time.Minute), "job1"))
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
		level4 := createJobLineage("level4", tnnt, &windowConfig, nil, createJobRunPair(baseTime, baseTime.Add(0), baseTime.Add(10*time.Minute), "level3"))
		level3 := createJobLineage("level3", tnnt, &windowConfig, []*scheduler.JobLineageSummary{level4}, createJobRunPair(baseTime, baseTime.Add(0), baseTime.Add(20*time.Minute), "level2"))
		level2 := createJobLineage("level2", tnnt, &windowConfig, []*scheduler.JobLineageSummary{level3}, createJobRunPair(baseTime, baseTime.Add(0), baseTime.Add(30*time.Minute), "level1"))
		level1 := createJobLineage("level1", tnnt, &windowConfig, []*scheduler.JobLineageSummary{level2}, createJobRunPair(baseTime, baseTime.Add(0), baseTime.Add(40*time.Minute), "root"))
		root := createJobLineage("root", tnnt, &windowConfig, []*scheduler.JobLineageSummary{level1}, createJobRunPair(baseTime, baseTime.Add(0), baseTime.Add(50*time.Minute), "root"))
		maxDepth := 2
		jobRunSums := root.Flatten(maxDepth)
		assert.Len(t, jobRunSums, 2)

		assert.Equal(t, jobRunSums[0].JobName, scheduler.JobName("root"))
		assert.Equal(t, jobRunSums[1].JobName, scheduler.JobName("level1"))
	})

	// scenario:
	// 			 root
	//        /    \
	//     l1A      l1B
	//       |   	  /
	//    	l2		 |
	//      	\		/
	//     			l3
	//      		|
	//     			l4
	t.Run("should generate unique paths for each upstream for shared upstreams", func(t *testing.T) {
		level4 := createJobLineage("level4", tnnt, &windowConfig, nil, createJobRunPair(baseTime, baseTime.Add(0), baseTime.Add(10*time.Minute), "level3"))
		// both level3 runs point to the same scheduled at
		level3 := createJobLineage("level3", tnnt, &windowConfig, []*scheduler.JobLineageSummary{level4},
			createJobRunPair(baseTime, baseTime.Add(0), baseTime.Add(20*time.Minute), "level2"),
			createJobRunPair(baseTime, baseTime.Add(0), baseTime.Add(20*time.Minute), "level1B"),
		)
		level2 := createJobLineage("level2", tnnt, &windowConfig, []*scheduler.JobLineageSummary{level3}, createJobRunPair(baseTime, baseTime.Add(0), baseTime.Add(25*time.Minute), "level1A"))
		level1A := createJobLineage("level1A", tnnt, &windowConfig, []*scheduler.JobLineageSummary{level2}, createJobRunPair(baseTime, baseTime.Add(0), baseTime.Add(50*time.Minute), "root"))
		level1B := createJobLineage("level1B", tnnt, &windowConfig, []*scheduler.JobLineageSummary{level3}, createJobRunPair(baseTime, baseTime.Add(0), baseTime.Add(40*time.Minute), "root"))
		root := createJobLineage("root", tnnt, &windowConfig, []*scheduler.JobLineageSummary{level1A, level1B}, createJobRunPair(baseTime, baseTime.Add(0), baseTime.Add(60*time.Minute), "root"))

		maxDepth := 5
		flattened := root.Flatten(maxDepth)

		assert.Equal(t, 8, len(flattened))
		jobNames := make(map[scheduler.JobName][]int)
		for _, summary := range flattened {
			jobNames[summary.JobName] = append(jobNames[summary.JobName], summary.Level)
		}
		assert.ElementsMatch(t, []int{0}, jobNames[scheduler.JobName("root")])
		assert.ElementsMatch(t, []int{1}, jobNames[scheduler.JobName("level1A")])
		assert.ElementsMatch(t, []int{1}, jobNames[scheduler.JobName("level1B")])
		assert.ElementsMatch(t, []int{2}, jobNames[scheduler.JobName("level2")])
		// upstream level3 should appear in two different paths: level 3 from level1A and level 2 from level1B
		assert.ElementsMatch(t, []int{2, 3}, jobNames[scheduler.JobName("level3")])
		// upstream level4 should appear in two different paths: level 4 from level1A and level 3 from level1B
		assert.ElementsMatch(t, []int{3, 4}, jobNames[scheduler.JobName("level4")])
	})
}

func TestJobLineageSummary_PruneLineage(t *testing.T) {
	projName := tenant.ProjectName("proj")
	namespaceName := tenant.ProjectName("ns1")
	tnnt, _ := tenant.NewTenant(projName.String(), namespaceName.String())

	baseTime := time.Date(2023, 1, 1, 10, 0, 0, 0, time.UTC)
	windowConfig, _ := window.NewPresetConfig("yesterday")

	t.Run("should handle nested lineage with pruning", func(t *testing.T) {
		level3A := createJobLineage("level3a", tnnt, &windowConfig, nil, createJobRunPair(baseTime, baseTime.Add(0), baseTime.Add(5*time.Minute), "level2a"))
		level3B := createJobLineage("level3b", tnnt, &windowConfig, nil, createJobRunPair(baseTime, baseTime.Add(0), baseTime.Add(15*time.Minute), "level2a"))
		level3C := createJobLineage("level3c", tnnt, &windowConfig, nil, createJobRunPair(baseTime, baseTime.Add(0), baseTime.Add(25*time.Minute), "level2b"))

		level2A := createJobLineage("level2a", tnnt, &windowConfig, []*scheduler.JobLineageSummary{level3A, level3B}, createJobRunPair(baseTime, baseTime.Add(0), baseTime.Add(50*time.Minute), "root"))
		level2B := createJobLineage("level2b", tnnt, &windowConfig, []*scheduler.JobLineageSummary{level3C}, createJobRunPair(baseTime, baseTime.Add(0), baseTime.Add(45*time.Minute), "root"))
		level2C := createJobLineage("level2c", tnnt, &windowConfig, nil, createJobRunPair(baseTime, baseTime.Add(0), baseTime.Add(40*time.Minute), "root"))

		rootJob := createJobLineage("root", tnnt, &windowConfig, []*scheduler.JobLineageSummary{level2A, level2B, level2C}, createJobRunPair(baseTime, baseTime.Add(0), baseTime.Add(50*time.Minute), "root"))

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
		level4 := createJobLineage("level4", tnnt, &windowConfig, nil, createJobRunPair(baseTime, baseTime.Add(0), baseTime.Add(10*time.Minute), "level3"))
		level3 := createJobLineage("level3", tnnt, &windowConfig, []*scheduler.JobLineageSummary{level4}, createJobRunPair(baseTime, baseTime.Add(0), baseTime.Add(20*time.Minute), "level2"))
		level2 := createJobLineage("level2", tnnt, &windowConfig, []*scheduler.JobLineageSummary{level3}, createJobRunPair(baseTime, baseTime.Add(0), baseTime.Add(30*time.Minute), "level1"))
		level1 := createJobLineage("level1", tnnt, &windowConfig, []*scheduler.JobLineageSummary{level2}, createJobRunPair(baseTime, baseTime.Add(0), baseTime.Add(40*time.Minute), "root"))
		root := createJobLineage("root", tnnt, &windowConfig, []*scheduler.JobLineageSummary{level1}, createJobRunPair(baseTime, baseTime.Add(0), baseTime.Add(50*time.Minute), "root"))
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
		jobLineage := createJobLineage("job1", tnnt, &windowConfig, nil, createJobRunPair(baseTime, baseTime.Add(0), baseTime.Add(30*time.Minute), "job1"))

		maxUpstreamsPerLevel := 5
		maxDepth := 5
		pruned := jobLineage.PruneLineage(maxUpstreamsPerLevel, maxDepth)

		assert.Equal(t, scheduler.JobName("job1"), pruned.JobName)
		assert.Len(t, pruned.Upstreams, 0)
	})

	t.Run("should handle circular references", func(t *testing.T) {
		upstream1 := createJobLineage("upstream1", tnnt, &windowConfig, nil,
			createJobRunPair(baseTime, baseTime.Add(0), baseTime.Add(10*time.Minute), "root"),
			createJobRunPair(baseTime, baseTime.Add(0), baseTime.Add(10*time.Minute), "upstream2"),
		)
		upstream2 := createJobLineage("upstream2", tnnt, &windowConfig, []*scheduler.JobLineageSummary{upstream1}, createJobRunPair(baseTime, baseTime.Add(0), baseTime.Add(20*time.Minute), "upstream1"))

		upstream1.Upstreams = []*scheduler.JobLineageSummary{upstream2}

		root := createJobLineage("root", tnnt, &windowConfig, []*scheduler.JobLineageSummary{upstream1}, createJobRunPair(baseTime, baseTime.Add(0), baseTime.Add(30*time.Minute), "root"))

		maxUpstreamsPerLevel := 5
		maxDepth := 5
		pruned := root.PruneLineage(maxUpstreamsPerLevel, maxDepth)

		assert.Equal(t, scheduler.JobName("root"), pruned.JobName)
		assert.Len(t, pruned.Upstreams, 1)
		assert.Equal(t, scheduler.JobName("upstream1"), pruned.Upstreams[0].JobName)
	})

	// scenario:
	// 			 root
	//        /    \
	//     l1A      l1B
	//       |   	  /
	//    	l2		 |
	//      	\		/
	//     			l3
	//      		|
	//     			l4
	// with maxUpstreamsPerLevel = 1,
	// case: if l1A is slowest, result should be root -> l1A -> l2 -> l3 -> l4
	// if l1B is slowest, result should be root -> l1B -> l3 -> l4
	t.Run("handle occurrence of same upstream from different paths", func(t *testing.T) {
		t.Run("should pick upstream from slowest path - l1A", func(t *testing.T) {
			level4 := createJobLineage("level4", tnnt, &windowConfig, nil, createJobRunPair(baseTime, baseTime.Add(0), baseTime.Add(10*time.Minute), "level3"))
			level3 := createJobLineage("level3", tnnt, &windowConfig, []*scheduler.JobLineageSummary{level4},
				createJobRunPair(baseTime, baseTime.Add(0), baseTime.Add(20*time.Minute), "level2"),
				createJobRunPair(baseTime, baseTime.Add(0), baseTime.Add(20*time.Minute), "level1B"),
			)
			level2 := createJobLineage("level2", tnnt, &windowConfig, []*scheduler.JobLineageSummary{level3}, createJobRunPair(baseTime, baseTime.Add(0), baseTime.Add(30*time.Minute), "level1A"))
			level1A := createJobLineage("level1A", tnnt, &windowConfig, []*scheduler.JobLineageSummary{level2}, createJobRunPair(baseTime, baseTime.Add(0), baseTime.Add(45*time.Minute), "root"))
			level1B := createJobLineage("level1B", tnnt, &windowConfig, []*scheduler.JobLineageSummary{level3}, createJobRunPair(baseTime, baseTime.Add(0), baseTime.Add(40*time.Minute), "root"))
			root := createJobLineage("root", tnnt, &windowConfig, []*scheduler.JobLineageSummary{level1A, level1B}, createJobRunPair(baseTime, baseTime.Add(0), baseTime.Add(50*time.Minute), "root"))

			maxUpstreamsPerLevel := 1
			maxDepth := 5
			pruned := root.PruneLineage(maxUpstreamsPerLevel, maxDepth)

			assert.Equal(t, scheduler.JobName("root"), pruned.JobName)
			assert.Len(t, pruned.Upstreams, 1)
			assert.Equal(t, scheduler.JobName("level1A"), pruned.Upstreams[0].JobName)
			assert.Len(t, pruned.Upstreams[0].Upstreams, 1)
			assert.Equal(t, scheduler.JobName("level2"), pruned.Upstreams[0].Upstreams[0].JobName)
			assert.Len(t, pruned.Upstreams[0].Upstreams[0].Upstreams, 1)
			assert.Equal(t, scheduler.JobName("level3"), pruned.Upstreams[0].Upstreams[0].Upstreams[0].JobName)
			assert.Len(t, pruned.Upstreams[0].Upstreams[0].Upstreams[0].Upstreams, 1)
			assert.Equal(t, scheduler.JobName("level4"), pruned.Upstreams[0].Upstreams[0].Upstreams[0].Upstreams[0].JobName)
		})

		t.Run("should pick upstream from slowest path - l1B", func(t *testing.T) {
			level4 := createJobLineage("level4", tnnt, &windowConfig, nil, createJobRunPair(baseTime, baseTime.Add(0), baseTime.Add(10*time.Minute), "level3"))
			level3 := createJobLineage("level3", tnnt, &windowConfig, []*scheduler.JobLineageSummary{level4},
				createJobRunPair(baseTime, baseTime.Add(0), baseTime.Add(20*time.Minute), "level2"),
				createJobRunPair(baseTime, baseTime.Add(0), baseTime.Add(20*time.Minute), "level1B"),
			)
			level2 := createJobLineage("level2", tnnt, &windowConfig, []*scheduler.JobLineageSummary{level3}, createJobRunPair(baseTime, baseTime.Add(0), baseTime.Add(25*time.Minute), "level1A"))
			level1A := createJobLineage("level1A", tnnt, &windowConfig, []*scheduler.JobLineageSummary{level2}, createJobRunPair(baseTime, baseTime.Add(0), baseTime.Add(30*time.Minute), "root"))
			level1B := createJobLineage("level1B", tnnt, &windowConfig, []*scheduler.JobLineageSummary{level3}, createJobRunPair(baseTime, baseTime.Add(0), baseTime.Add(50*time.Minute), "root"))
			root := createJobLineage("root", tnnt, &windowConfig, []*scheduler.JobLineageSummary{level1A, level1B}, createJobRunPair(baseTime, baseTime.Add(0), baseTime.Add(60*time.Minute), "root"))

			maxUpstreamsPerLevel := 1
			maxDepth := 5
			pruned := root.PruneLineage(maxUpstreamsPerLevel, maxDepth)

			assert.Equal(t, scheduler.JobName("root"), pruned.JobName)
			assert.Len(t, pruned.Upstreams, 1)
			assert.Equal(t, scheduler.JobName("level1B"), pruned.Upstreams[0].JobName)
			assert.Len(t, pruned.Upstreams[0].Upstreams, 1)
			assert.Equal(t, scheduler.JobName("level3"), pruned.Upstreams[0].Upstreams[0].JobName)
			assert.Len(t, pruned.Upstreams[0].Upstreams[0].Upstreams, 1)
			assert.Equal(t, scheduler.JobName("level4"), pruned.Upstreams[0].Upstreams[0].Upstreams[0].JobName)
		})

		t.Run("should generate path from all upstreams if there are shared paths", func(t *testing.T) {
			level4 := createJobLineage("level4", tnnt, &windowConfig, nil, createJobRunPair(baseTime, baseTime.Add(0), baseTime.Add(10*time.Minute), "level3"))
			level3 := createJobLineage("level3", tnnt, &windowConfig, []*scheduler.JobLineageSummary{level4},
				createJobRunPair(baseTime, baseTime.Add(0), baseTime.Add(20*time.Minute), "level2"),
				createJobRunPair(baseTime, baseTime.Add(0), baseTime.Add(20*time.Minute), "level1B"),
			)
			level2 := createJobLineage("level2", tnnt, &windowConfig, []*scheduler.JobLineageSummary{level3}, createJobRunPair(baseTime, baseTime.Add(0), baseTime.Add(25*time.Minute), "level1A"))
			level1A := createJobLineage("level1A", tnnt, &windowConfig, []*scheduler.JobLineageSummary{level2}, createJobRunPair(baseTime, baseTime.Add(0), baseTime.Add(50*time.Minute), "root"))
			level1B := createJobLineage("level1B", tnnt, &windowConfig, []*scheduler.JobLineageSummary{level3}, createJobRunPair(baseTime, baseTime.Add(0), baseTime.Add(40*time.Minute), "root"))
			root := createJobLineage("root", tnnt, &windowConfig, []*scheduler.JobLineageSummary{level1A, level1B}, createJobRunPair(baseTime, baseTime.Add(0), baseTime.Add(60*time.Minute), "root"))

			maxUpstreamsPerLevel := 10
			maxDepth := 5
			pruned := root.PruneLineage(maxUpstreamsPerLevel, maxDepth)

			assert.Equal(t, scheduler.JobName("root"), pruned.JobName)
			assert.Len(t, pruned.Upstreams, 2)
			assert.Equal(t, scheduler.JobName("level1A"), pruned.Upstreams[0].JobName)
			assert.Equal(t, scheduler.JobName("level1B"), pruned.Upstreams[1].JobName)

			assert.Len(t, pruned.Upstreams[0].Upstreams, 1)
			assert.Equal(t, scheduler.JobName("level2"), pruned.Upstreams[0].Upstreams[0].JobName)
			assert.Len(t, pruned.Upstreams[0].Upstreams[0].Upstreams, 1)
			assert.Equal(t, scheduler.JobName("level3"), pruned.Upstreams[0].Upstreams[0].Upstreams[0].JobName)
			assert.Len(t, pruned.Upstreams[0].Upstreams[0].Upstreams[0].Upstreams, 1)
			assert.Equal(t, scheduler.JobName("level4"), pruned.Upstreams[0].Upstreams[0].Upstreams[0].Upstreams[0].JobName)

			assert.Len(t, pruned.Upstreams[1].Upstreams, 1)
			assert.Equal(t, scheduler.JobName("level3"), pruned.Upstreams[1].Upstreams[0].JobName)
			assert.Len(t, pruned.Upstreams[1].Upstreams[0].Upstreams, 1)
			assert.Equal(t, scheduler.JobName("level4"), pruned.Upstreams[1].Upstreams[0].Upstreams[0].JobName)
		})
	})
}
