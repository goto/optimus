package scheduler_test

import (
	"fmt"
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

// nolint:unparam
func createJobRunPair(downstreamJobName, jobName string, baseTime, startTime, endTime time.Time) downstreamJobNameAndRunPair {
	return downstreamJobNameAndRunPair{
		JobName:   scheduler.JobName(downstreamJobName),
		JobRunSum: createJobRun(jobName, baseTime, startTime, endTime),
	}
}

func createJobRun(jobName string, baseTime, startTime, endTime time.Time) *scheduler.JobRunSummary {
	return &scheduler.JobRunSummary{
		JobName:       scheduler.JobName(jobName),
		ScheduledAt:   baseTime,
		JobStartTime:  &startTime,
		JobEndTime:    &endTime,
		TaskStartTime: &startTime,
		TaskEndTime:   &endTime,
		HookEndTime:   &endTime,
	}
}

func TestJobLineageSummary_GenerateLineageExecutionSummary(t *testing.T) {
	projName := tenant.ProjectName("proj")
	namespaceName := tenant.ProjectName("ns1")
	tnnt, _ := tenant.NewTenant(projName.String(), namespaceName.String())

	baseTime := time.Date(2023, 1, 1, 10, 0, 0, 0, time.UTC)
	windowConfig, _ := window.NewPresetConfig("yesterday")

	t.Run("should return nil for nil job lineage", func(t *testing.T) {
		var jobLineage *scheduler.JobLineageSummary
		result := jobLineage.GenerateLineageExecutionSummary(scheduler.LineageWalkOptions{MaxNodes: 10, MaxDepth: 5})
		assert.Nil(t, result)
	})

	t.Run("should generate execution summary for single job", func(t *testing.T) {
		startTime := baseTime.Add(10 * time.Minute)
		endTime := baseTime.Add(20 * time.Minute)
		jobLineage := createJobLineage("root", tnnt, &windowConfig, nil,
			createJobRunPair("root", "root", baseTime, startTime, endTime))

		result := jobLineage.GenerateLineageExecutionSummary(scheduler.LineageWalkOptions{MaxNodes: 10, MaxDepth: 5})

		assert.NotNil(t, result)
		assert.Equal(t, scheduler.JobName("root"), result.JobName)
		assert.Equal(t, baseTime, result.ScheduledAt)
		assert.Len(t, result.JobRuns, 1)
		assert.Equal(t, int64(0), result.ExecutionSummary.TotalScheduledWayTooLateSeconds)
		assert.Equal(t, int64(600), result.ExecutionSummary.TotalSystemSchedulingDelaySeconds)
	})

	t.Run("should calculate system scheduling delay correctly", func(t *testing.T) {
		upstreamStartTime := baseTime.Add(5 * time.Minute)
		upstreamEndTime := baseTime.Add(15 * time.Minute)
		rootStartTime := baseTime.Add(20 * time.Minute)
		rootEndTime := baseTime.Add(30 * time.Minute)

		upstream := createJobLineage("upstream", tnnt, &windowConfig, nil,
			createJobRunPair("root", "upstream", baseTime, upstreamStartTime, upstreamEndTime))
		jobLineage := createJobLineage("root", tnnt, &windowConfig, []*scheduler.JobLineageSummary{upstream},
			createJobRunPair("root", "root", baseTime, rootStartTime, rootEndTime))

		result := jobLineage.GenerateLineageExecutionSummary(scheduler.LineageWalkOptions{MaxNodes: 10, MaxDepth: 5})

		assert.Equal(t, int64(600), result.ExecutionSummary.TotalSystemSchedulingDelaySeconds)
		assert.Equal(t, int64(300), result.ExecutionSummary.AverageSystemSchedulingDelaySeconds)
	})

	t.Run("should calculate scheduled way too late correctly", func(t *testing.T) {
		// add 5 minutes delay to upstream start time
		upstreamStartTime := baseTime.Add(5 * time.Minute)
		upstreamEndTime := baseTime.Add(25 * time.Minute)

		// upstream start at baseTime, root scheduled at baseTime + 30min
		// so the downstream is scheduled 5 mins too late from upstream end time
		baseRootTime := baseTime.Add(30 * time.Minute)
		// add 1 minute delay to root start time
		rootStartTime := baseRootTime.Add(1 * time.Minute)
		rootEndTime := baseRootTime.Add(45 * time.Minute)

		upstream := createJobLineage("upstream", tnnt, &windowConfig, nil,
			createJobRunPair("root", "upstream", baseTime, upstreamStartTime, upstreamEndTime))
		jobLineage := createJobLineage("root", tnnt, &windowConfig, []*scheduler.JobLineageSummary{upstream},
			createJobRunPair("root", "root", baseRootTime, rootStartTime, rootEndTime))

		result := jobLineage.GenerateLineageExecutionSummary(scheduler.LineageWalkOptions{MaxNodes: 10, MaxDepth: 5})

		// 5 minutes from last upstream end time - root scheduled time
		assert.Equal(t, int64(300), result.ExecutionSummary.TotalScheduledWayTooLateSeconds)
		// 5 minutes from upstream + 1 minute from root = 6 minutes total scheduled way too late
		assert.Equal(t, int64(360), result.ExecutionSummary.TotalSystemSchedulingDelaySeconds)

		assert.Equal(t, scheduler.JobName("root"), result.ExecutionSummary.LargestScheduledWayTooLateJob.JobName)
		assert.Equal(t, scheduler.JobName("upstream"), result.ExecutionSummary.LargestScheduledWayTooLateJob.UpstreamJobName)
	})

	t.Run("should calculate task and hook durations", func(t *testing.T) {
		taskStart := baseTime.Add(10 * time.Minute)
		taskEnd := baseTime.Add(20 * time.Minute)
		hookStart := baseTime.Add(20 * time.Minute)
		hookEnd := baseTime.Add(25 * time.Minute)

		jobRun := &scheduler.JobRunSummary{
			JobName:       scheduler.JobName("root"),
			ScheduledAt:   baseTime,
			TaskStartTime: &taskStart,
			TaskEndTime:   &taskEnd,
			HookStartTime: &hookStart,
			HookEndTime:   &hookEnd,
		}

		jobLineage := &scheduler.JobLineageSummary{
			JobName:          scheduler.JobName("root"),
			Tenant:           tnnt,
			ScheduleInterval: "0 8 * * *",
			Window:           &windowConfig,
			SLA:              scheduler.SLAConfig{Duration: time.Hour},
			JobRuns:          map[scheduler.JobName]*scheduler.JobRunSummary{scheduler.JobName("root"): jobRun},
		}

		result := jobLineage.GenerateLineageExecutionSummary(scheduler.LineageWalkOptions{MaxNodes: 10, MaxDepth: 5})

		assert.Len(t, result.ExecutionSummary.TopLongestTaskDurationJobs, 1)
		assert.Len(t, result.ExecutionSummary.TopLongestHookDurationJobs, 1)
		assert.Equal(t, 10*time.Minute, result.ExecutionSummary.TopLongestTaskDurationJobs[0].TaskDuration)
		assert.Equal(t, 5*time.Minute, result.ExecutionSummary.TopLongestHookDurationJobs[0].TaskDuration)
	})

	t.Run("should limit top longest jobs to 3", func(t *testing.T) {
		var upstreams []*scheduler.JobLineageSummary
		jobRuns := make(map[scheduler.JobName]*scheduler.JobRunSummary)

		for i := 1; i <= 5; i++ {
			jobName := scheduler.JobName(fmt.Sprintf("job%d", i))
			taskStart := baseTime.Add(time.Duration(i) * time.Minute)
			taskEnd := baseTime.Add(time.Duration(i*10) * time.Minute)

			jobRun := &scheduler.JobRunSummary{
				JobName:       jobName,
				ScheduledAt:   baseTime,
				TaskStartTime: &taskStart,
				TaskEndTime:   &taskEnd,
			}
			jobRuns[scheduler.JobName("root")] = jobRun

			upstream := &scheduler.JobLineageSummary{
				JobName: jobName,
				JobRuns: map[scheduler.JobName]*scheduler.JobRunSummary{scheduler.JobName("root"): jobRun},
			}
			upstreams = append(upstreams, upstream)
		}

		rootRun := &scheduler.JobRunSummary{
			JobName:     scheduler.JobName("root"),
			ScheduledAt: baseTime,
		}
		jobRuns[scheduler.JobName("root")] = rootRun

		jobLineage := &scheduler.JobLineageSummary{
			JobName:   scheduler.JobName("root"),
			Upstreams: upstreams,
			JobRuns:   jobRuns,
		}

		result := jobLineage.GenerateLineageExecutionSummary(scheduler.LineageWalkOptions{MaxNodes: 10, MaxDepth: 5})

		assert.LessOrEqual(t, len(result.ExecutionSummary.TopLongestTaskDurationJobs), 3)
	})

	t.Run("should handle jobs without completed runs", func(t *testing.T) {
		incompleteRun := &scheduler.JobRunSummary{
			JobName:       scheduler.JobName("incomplete"),
			ScheduledAt:   baseTime,
			TaskStartTime: nil,
			TaskEndTime:   nil,
		}

		upstream := &scheduler.JobLineageSummary{
			JobName: scheduler.JobName("incomplete"),
			JobRuns: map[scheduler.JobName]*scheduler.JobRunSummary{scheduler.JobName("root"): incompleteRun},
		}

		rootRun := &scheduler.JobRunSummary{
			JobName:     scheduler.JobName("root"),
			ScheduledAt: baseTime,
		}

		jobLineage := &scheduler.JobLineageSummary{
			JobName:   scheduler.JobName("root"),
			Upstreams: []*scheduler.JobLineageSummary{upstream},
			JobRuns:   map[scheduler.JobName]*scheduler.JobRunSummary{scheduler.JobName("root"): rootRun},
		}

		result := jobLineage.GenerateLineageExecutionSummary(scheduler.LineageWalkOptions{MaxNodes: 10, MaxDepth: 5})

		assert.NotNil(t, result)
		assert.Empty(t, result.ExecutionSummary.TopLongestTaskDurationJobs)
	})
}

func createRunningJobRun(jobName string, scheduledAt, startTime time.Time) *scheduler.JobRunSummary {
	return &scheduler.JobRunSummary{
		JobName:       scheduler.JobName(jobName),
		ScheduledAt:   scheduledAt,
		JobStatus:     scheduler.StateRunning.String(),
		JobStartTime:  &startTime,
		TaskStartTime: &startTime,
	}
}

func createRunningJobRunPair(downstreamJobName, jobName string, scheduledAt, startTime time.Time) downstreamJobNameAndRunPair {
	return downstreamJobNameAndRunPair{
		JobName:   scheduler.JobName(downstreamJobName),
		JobRunSum: createRunningJobRun(jobName, scheduledAt, startTime),
	}
}

func findNode(nodes []*scheduler.JobExecutionSummary, jobName string) *scheduler.JobExecutionSummary {
	for _, node := range nodes {
		if node.JobName == scheduler.JobName(jobName) {
			return node
		}
	}
	return nil
}

func countNodes(nodes []*scheduler.JobExecutionSummary, jobName string) int {
	count := 0
	for _, node := range nodes {
		if node.JobName == scheduler.JobName(jobName) {
			count++
		}
	}
	return count
}

func TestJobLineageSummary_GetLineageNodes(t *testing.T) {
	projName := tenant.ProjectName("proj")
	namespaceName := tenant.ProjectName("ns1")
	tnnt, _ := tenant.NewTenant(projName.String(), namespaceName.String())

	baseTime := time.Date(2023, 1, 1, 10, 0, 0, 0, time.UTC)
	windowConfig, _ := window.NewPresetConfig("yesterday")

	t.Run("should return an empty result for a nil lineage", func(t *testing.T) {
		var jobLineage *scheduler.JobLineageSummary

		result := jobLineage.GetLineageNodes(scheduler.LineageWalkOptions{MaxNodes: 0, MaxDepth: 5})

		assert.Empty(t, result.Nodes)
		assert.False(t, result.Truncated)
	})

	t.Run("should return single job when no upstreams", func(t *testing.T) {
		jobLineage := createJobLineage("root", tnnt, &windowConfig, nil,
			createJobRunPair("root", "root", baseTime, baseTime.Add(10*time.Minute), baseTime.Add(20*time.Minute)))

		result := jobLineage.GetLineageNodes(scheduler.LineageWalkOptions{MaxNodes: 0, MaxDepth: 5})

		assert.Len(t, result.Nodes, 1)
		assert.Equal(t, scheduler.JobName("root"), result.Nodes[0].JobName)
		assert.Equal(t, 0, result.Nodes[0].Level)
		assert.Equal(t, 1, result.TotalNodes)
	})

	t.Run("should return every upstream on a level rather than trimming to a ranked few", func(t *testing.T) {
		var upstreams []*scheduler.JobLineageSummary
		for i := 1; i <= 5; i++ {
			upstream := createJobLineage(fmt.Sprintf("upstream%d", i), tnnt, &windowConfig, nil,
				createJobRunPair("root", fmt.Sprintf("upstream%d", i), baseTime, baseTime.Add(time.Duration(i)*time.Minute), baseTime.Add(time.Duration(i+10)*time.Minute)))
			upstreams = append(upstreams, upstream)
		}

		jobLineage := createJobLineage("root", tnnt, &windowConfig, upstreams,
			createJobRunPair("root", "root", baseTime, baseTime.Add(30*time.Minute), baseTime.Add(40*time.Minute)))

		result := jobLineage.GetLineageNodes(scheduler.LineageWalkOptions{MaxNodes: 0, MaxDepth: 5})

		level1Count := 0
		for _, node := range result.Nodes {
			if node.Level == 1 {
				level1Count++
			}
		}
		assert.Equal(t, 5, level1Count)
	})

	t.Run("should respect max depth", func(t *testing.T) {
		level3 := createJobLineage("level3", tnnt, &windowConfig, nil,
			createJobRunPair("level2", "level3", baseTime, baseTime.Add(5*time.Minute), baseTime.Add(10*time.Minute)))
		level2 := createJobLineage("level2", tnnt, &windowConfig, []*scheduler.JobLineageSummary{level3},
			createJobRunPair("level1", "level2", baseTime, baseTime.Add(15*time.Minute), baseTime.Add(20*time.Minute)))
		level1 := createJobLineage("level1", tnnt, &windowConfig, []*scheduler.JobLineageSummary{level2},
			createJobRunPair("root", "level1", baseTime, baseTime.Add(25*time.Minute), baseTime.Add(30*time.Minute)))
		root := createJobLineage("root", tnnt, &windowConfig, []*scheduler.JobLineageSummary{level1},
			createJobRunPair("root", "root", baseTime, baseTime.Add(35*time.Minute), baseTime.Add(40*time.Minute)))

		result := root.GetLineageNodes(scheduler.LineageWalkOptions{MaxNodes: 0, MaxDepth: 2})

		maxLevel := 0
		for _, node := range result.Nodes {
			if node.Level > maxLevel {
				maxLevel = node.Level
			}
		}
		assert.Equal(t, 2, maxLevel)
		assert.Nil(t, findNode(result.Nodes, "level3"))
	})

	t.Run("should walk the whole lineage when no node budget is given", func(t *testing.T) {
		upstream := createJobLineage("upstream", tnnt, &windowConfig, nil,
			createJobRunPair("root", "upstream", baseTime, baseTime.Add(5*time.Minute), baseTime.Add(10*time.Minute)))
		jobLineage := createJobLineage("root", tnnt, &windowConfig, []*scheduler.JobLineageSummary{upstream},
			createJobRunPair("root", "root", baseTime, baseTime.Add(15*time.Minute), baseTime.Add(20*time.Minute)))

		result := jobLineage.GetLineageNodes(scheduler.LineageWalkOptions{MaxNodes: 0, MaxDepth: 5})

		assert.Len(t, result.Nodes, 2)
		assert.False(t, result.Truncated)
	})

	t.Run("should stop at the node budget and flag the result as truncated", func(t *testing.T) {
		var upstreams []*scheduler.JobLineageSummary
		for i := 1; i <= 5; i++ {
			upstream := createJobLineage(fmt.Sprintf("upstream%d", i), tnnt, &windowConfig, nil,
				createJobRunPair("root", fmt.Sprintf("upstream%d", i), baseTime, baseTime.Add(time.Duration(i)*time.Minute), baseTime.Add(time.Duration(i+10)*time.Minute)))
			upstreams = append(upstreams, upstream)
		}
		jobLineage := createJobLineage("root", tnnt, &windowConfig, upstreams,
			createJobRunPair("root", "root", baseTime, baseTime.Add(30*time.Minute), baseTime.Add(40*time.Minute)))

		result := jobLineage.GetLineageNodes(scheduler.LineageWalkOptions{MaxNodes: 3, MaxDepth: 5})

		assert.Len(t, result.Nodes, 3)
		assert.Equal(t, 3, result.TotalNodes)
		assert.True(t, result.Truncated)
	})

	t.Run("should keep the nodes closest to the target when the budget cuts the walk short", func(t *testing.T) {
		// breadth-first order means a budget drops the deepest runs, so a caller asking for a
		// smaller retrospective view gets the levels nearest the target rather than an
		// arbitrary slice
		level3 := createJobLineage("level3", tnnt, &windowConfig, nil,
			createJobRunPair("level2", "level3", baseTime, baseTime.Add(5*time.Minute), baseTime.Add(10*time.Minute)))
		level2 := createJobLineage("level2", tnnt, &windowConfig, []*scheduler.JobLineageSummary{level3},
			createJobRunPair("level1", "level2", baseTime, baseTime.Add(15*time.Minute), baseTime.Add(20*time.Minute)))
		level1 := createJobLineage("level1", tnnt, &windowConfig, []*scheduler.JobLineageSummary{level2},
			createJobRunPair("root", "level1", baseTime, baseTime.Add(25*time.Minute), baseTime.Add(30*time.Minute)))
		root := createJobLineage("root", tnnt, &windowConfig, []*scheduler.JobLineageSummary{level1},
			createJobRunPair("root", "root", baseTime, baseTime.Add(35*time.Minute), baseTime.Add(40*time.Minute)))

		result := root.GetLineageNodes(scheduler.LineageWalkOptions{MaxNodes: 2, MaxDepth: 10})

		assert.Len(t, result.Nodes, 2)
		assert.True(t, result.Truncated)
		assert.NotNil(t, findNode(result.Nodes, "root"))
		assert.NotNil(t, findNode(result.Nodes, "level1"))
		assert.Nil(t, findNode(result.Nodes, "level2"))
		assert.Nil(t, findNode(result.Nodes, "level3"))
	})

	t.Run("should skip upstreams without job runs", func(t *testing.T) {
		upstreamWithRun := createJobLineage("with_run", tnnt, &windowConfig, nil,
			createJobRunPair("root", "with_run", baseTime, baseTime.Add(5*time.Minute), baseTime.Add(10*time.Minute)))

		upstreamWithoutRun := &scheduler.JobLineageSummary{
			JobName: scheduler.JobName("without_run"),
			JobRuns: map[scheduler.JobName]*scheduler.JobRunSummary{},
		}

		jobLineage := createJobLineage("root", tnnt, &windowConfig,
			[]*scheduler.JobLineageSummary{upstreamWithRun, upstreamWithoutRun},
			createJobRunPair("root", "root", baseTime, baseTime.Add(15*time.Minute), baseTime.Add(20*time.Minute)))

		result := jobLineage.GetLineageNodes(scheduler.LineageWalkOptions{MaxNodes: 0, MaxDepth: 5})

		assert.Len(t, result.Nodes, 2)
		assert.Nil(t, findNode(result.Nodes, "without_run"))
	})

	t.Run("should look up a run beyond the first level by its immediate parent, not the tree root", func(t *testing.T) {
		// chain: root -> level1 -> level2. level2's run is keyed by "level1" (its true immediate
		// downstream), not "root". Only correct immediate-parent threading through the traversal
		// can resolve it - looking it up via the tree root ("root") would find nothing.
		level2 := createJobLineage("level2", tnnt, &windowConfig, nil,
			createJobRunPair("level1", "level2", baseTime, baseTime.Add(1*time.Minute), baseTime.Add(5*time.Minute)))
		level1 := createJobLineage("level1", tnnt, &windowConfig, []*scheduler.JobLineageSummary{level2},
			createJobRunPair("root", "level1", baseTime, baseTime.Add(10*time.Minute), baseTime.Add(15*time.Minute)))
		root := createJobLineage("root", tnnt, &windowConfig, []*scheduler.JobLineageSummary{level1},
			createJobRunPair("root", "root", baseTime, baseTime.Add(20*time.Minute), baseTime.Add(25*time.Minute)))

		result := root.GetLineageNodes(scheduler.LineageWalkOptions{MaxNodes: 0, MaxDepth: 5})

		level2Node := findNode(result.Nodes, "level2")
		if assert.NotNil(t, level2Node, "level2 should be found via its immediate parent's key") {
			assert.Equal(t, baseTime.Add(1*time.Minute), *level2Node.JobRunSummary.TaskStartTime)
			assert.Equal(t, 2, level2Node.Level)
		}
	})

	t.Run("should include an upstream that has not finished yet", func(t *testing.T) {
		running := createJobLineage("running", tnnt, &windowConfig, nil,
			createRunningJobRunPair("root", "running", baseTime, baseTime.Add(5*time.Minute)))
		root := createJobLineage("root", tnnt, &windowConfig, []*scheduler.JobLineageSummary{running},
			createRunningJobRunPair("root", "root", baseTime, baseTime.Add(20*time.Minute)))

		result := root.GetLineageNodes(scheduler.LineageWalkOptions{MaxNodes: 0, MaxDepth: 5})

		runningNode := findNode(result.Nodes, "running")
		if assert.NotNil(t, runningNode, "an unfinished upstream must not be dropped") {
			assert.Equal(t, scheduler.StateRunning, runningNode.State)
		}
	})

	t.Run("should keep walking past an unfinished upstream into its own upstreams", func(t *testing.T) {
		// the upstream of an in-flight job is exactly what an operator needs to see, so an
		// unfinished node must not cut its branch short
		grandparent := createJobLineage("grandparent", tnnt, &windowConfig, nil,
			createJobRunPair("running", "grandparent", baseTime, baseTime.Add(1*time.Minute), baseTime.Add(3*time.Minute)))
		running := createJobLineage("running", tnnt, &windowConfig, []*scheduler.JobLineageSummary{grandparent},
			createRunningJobRunPair("root", "running", baseTime, baseTime.Add(5*time.Minute)))
		root := createJobLineage("root", tnnt, &windowConfig, []*scheduler.JobLineageSummary{running},
			createRunningJobRunPair("root", "root", baseTime, baseTime.Add(20*time.Minute)))

		result := root.GetLineageNodes(scheduler.LineageWalkOptions{MaxNodes: 0, MaxDepth: 5})

		grandparentNode := findNode(result.Nodes, "grandparent")
		if assert.NotNil(t, grandparentNode, "upstreams of an unfinished job must still be reachable") {
			assert.Equal(t, 2, grandparentNode.Level)
		}
	})

	t.Run("should keep walking when every upstream of a job is unfinished", func(t *testing.T) {
		grandparent := createJobLineage("grandparent", tnnt, &windowConfig, nil,
			createRunningJobRunPair("runningA", "grandparent", baseTime, baseTime.Add(1*time.Minute)))
		runningA := createJobLineage("runningA", tnnt, &windowConfig, []*scheduler.JobLineageSummary{grandparent},
			createRunningJobRunPair("root", "runningA", baseTime, baseTime.Add(5*time.Minute)))
		runningB := createJobLineage("runningB", tnnt, &windowConfig, nil,
			createRunningJobRunPair("root", "runningB", baseTime, baseTime.Add(6*time.Minute)))
		root := createJobLineage("root", tnnt, &windowConfig, []*scheduler.JobLineageSummary{runningA, runningB},
			createRunningJobRunPair("root", "root", baseTime, baseTime.Add(20*time.Minute)))

		result := root.GetLineageNodes(scheduler.LineageWalkOptions{MaxNodes: 0, MaxDepth: 5})

		assert.Len(t, result.Nodes, 4)
		assert.NotNil(t, findNode(result.Nodes, "grandparent"))
	})

	t.Run("should return a shared upstream once with an edge to each downstream", func(t *testing.T) {
		// topology: root -> {B, C}, both B and C -> D, and both paths resolve to the same run of
		// D. D must appear once, carrying both downstreams, rather than once per path.
		sharedRun := createJobRun("D", baseTime.Add(-1*time.Hour), baseTime.Add(-55*time.Minute), baseTime.Add(-50*time.Minute))
		d := &scheduler.JobLineageSummary{
			JobName:   scheduler.JobName("D"),
			Tenant:    tnnt,
			SLA:       scheduler.SLAConfig{Duration: time.Hour},
			IsEnabled: true,
			JobRuns: map[scheduler.JobName]*scheduler.JobRunSummary{
				scheduler.JobName("B"): sharedRun,
				scheduler.JobName("C"): sharedRun,
			},
		}
		b := createJobLineage("B", tnnt, &windowConfig, []*scheduler.JobLineageSummary{d},
			createJobRunPair("root", "B", baseTime, baseTime.Add(5*time.Minute), baseTime.Add(10*time.Minute)))
		c := createJobLineage("C", tnnt, &windowConfig, []*scheduler.JobLineageSummary{d},
			createJobRunPair("root", "C", baseTime, baseTime.Add(1*time.Minute), baseTime.Add(2*time.Minute)))
		root := createJobLineage("root", tnnt, &windowConfig, []*scheduler.JobLineageSummary{b, c},
			createJobRunPair("root", "root", baseTime, baseTime.Add(20*time.Minute), baseTime.Add(25*time.Minute)))

		result := root.GetLineageNodes(scheduler.LineageWalkOptions{MaxNodes: 0, MaxDepth: 5})

		assert.Equal(t, 1, countNodes(result.Nodes, "D"), "a shared run must be deduplicated")
		dNode := findNode(result.Nodes, "D")
		if assert.NotNil(t, dNode) {
			assert.Len(t, dNode.DownstreamRefs, 2)
			downstreams := []scheduler.JobName{dNode.DownstreamRefs[0].JobName, dNode.DownstreamRefs[1].JobName}
			assert.Contains(t, downstreams, scheduler.JobName("B"))
			assert.Contains(t, downstreams, scheduler.JobName("C"))
		}
	})

	t.Run("should deduplicate a shared run whose schedules carry different locations", func(t *testing.T) {
		// Go compares time.Time by wall clock, monotonic reading and location, so the same
		// instant in two locations is two different map keys unless the key normalises to UTC
		jakarta := time.FixedZone("WIB", 7*60*60)
		sharedSchedule := baseTime.Add(-1 * time.Hour)

		runViaB := createJobRun("D", sharedSchedule, sharedSchedule.Add(5*time.Minute), sharedSchedule.Add(10*time.Minute))
		runViaC := createJobRun("D", sharedSchedule.In(jakarta), sharedSchedule.Add(5*time.Minute), sharedSchedule.Add(10*time.Minute))

		d := &scheduler.JobLineageSummary{
			JobName: scheduler.JobName("D"),
			Tenant:  tnnt,
			JobRuns: map[scheduler.JobName]*scheduler.JobRunSummary{
				scheduler.JobName("B"): runViaB,
				scheduler.JobName("C"): runViaC,
			},
		}
		b := createJobLineage("B", tnnt, &windowConfig, []*scheduler.JobLineageSummary{d},
			createJobRunPair("root", "B", baseTime, baseTime.Add(5*time.Minute), baseTime.Add(10*time.Minute)))
		c := createJobLineage("C", tnnt, &windowConfig, []*scheduler.JobLineageSummary{d},
			createJobRunPair("root", "C", baseTime, baseTime.Add(1*time.Minute), baseTime.Add(2*time.Minute)))
		root := createJobLineage("root", tnnt, &windowConfig, []*scheduler.JobLineageSummary{b, c},
			createJobRunPair("root", "root", baseTime, baseTime.Add(20*time.Minute), baseTime.Add(25*time.Minute)))

		result := root.GetLineageNodes(scheduler.LineageWalkOptions{MaxNodes: 0, MaxDepth: 5})

		assert.Equal(t, 1, countNodes(result.Nodes, "D"), "the same instant in another location is the same run")
	})

	t.Run("should keep a shared upstream's runs apart when the paths resolve to different schedules", func(t *testing.T) {
		// same topology, but B and C pull different runs of D. Those are distinct runs, so they
		// must stay distinct nodes - deduplicating on job name alone would lose one of them.
		d := createJobLineage("D", tnnt, &windowConfig, nil,
			createJobRunPair("B", "D", baseTime.Add(-1*time.Hour), baseTime.Add(-55*time.Minute), baseTime.Add(-50*time.Minute)),
			createJobRunPair("C", "D", baseTime.Add(-2*time.Hour), baseTime.Add(-115*time.Minute), baseTime.Add(-110*time.Minute)),
		)
		b := createJobLineage("B", tnnt, &windowConfig, []*scheduler.JobLineageSummary{d},
			createJobRunPair("root", "B", baseTime, baseTime.Add(5*time.Minute), baseTime.Add(10*time.Minute)))
		c := createJobLineage("C", tnnt, &windowConfig, []*scheduler.JobLineageSummary{d},
			createJobRunPair("root", "C", baseTime, baseTime.Add(1*time.Minute), baseTime.Add(2*time.Minute)))
		root := createJobLineage("root", tnnt, &windowConfig, []*scheduler.JobLineageSummary{b, c},
			createJobRunPair("root", "root", baseTime, baseTime.Add(20*time.Minute), baseTime.Add(25*time.Minute)))

		result := root.GetLineageNodes(scheduler.LineageWalkOptions{MaxNodes: 0, MaxDepth: 5})

		assert.Equal(t, 2, countNodes(result.Nodes, "D"), "distinct runs of a shared job are distinct nodes")

		var schedules []time.Time
		for _, node := range result.Nodes {
			if node.JobName == "D" {
				schedules = append(schedules, node.JobRunSummary.ScheduledAt)
				assert.Len(t, node.DownstreamRefs, 1)
			}
		}
		assert.Contains(t, schedules, baseTime.Add(-1*time.Hour))
		assert.Contains(t, schedules, baseTime.Add(-2*time.Hour))
	})

	t.Run("should assign the shortest level to a job reachable by paths of different lengths", func(t *testing.T) {
		// root -> shared directly, and root -> mid -> shared. The direct edge is shorter, and
		// breadth-first order must settle the level at 1 rather than 2.
		sharedRun := createJobRun("shared", baseTime, baseTime.Add(1*time.Minute), baseTime.Add(2*time.Minute))
		shared := &scheduler.JobLineageSummary{
			JobName: scheduler.JobName("shared"),
			Tenant:  tnnt,
			JobRuns: map[scheduler.JobName]*scheduler.JobRunSummary{
				scheduler.JobName("root"): sharedRun,
				scheduler.JobName("mid"):  sharedRun,
			},
		}
		mid := createJobLineage("mid", tnnt, &windowConfig, []*scheduler.JobLineageSummary{shared},
			createJobRunPair("root", "mid", baseTime, baseTime.Add(5*time.Minute), baseTime.Add(8*time.Minute)))
		root := createJobLineage("root", tnnt, &windowConfig, []*scheduler.JobLineageSummary{mid, shared},
			createJobRunPair("root", "root", baseTime, baseTime.Add(20*time.Minute), baseTime.Add(25*time.Minute)))

		result := root.GetLineageNodes(scheduler.LineageWalkOptions{MaxNodes: 0, MaxDepth: 5})

		sharedNode := findNode(result.Nodes, "shared")
		if assert.NotNil(t, sharedNode) {
			assert.Equal(t, 1, sharedNode.Level, "level should be the shortest distance from the target")
			assert.Len(t, sharedNode.DownstreamRefs, 2)
		}
	})

	t.Run("should mark only the unfinished runs whose own upstreams have all finished", func(t *testing.T) {
		// finished <- blocking(running) <- blocked(running) : the lineage is waiting on
		// "blocking", while "blocked" is itself waiting on it
		blocking := createJobLineage("blocking", tnnt, &windowConfig, nil,
			createRunningJobRunPair("blocked", "blocking", baseTime, baseTime.Add(2*time.Minute)))
		blocked := createJobLineage("blocked", tnnt, &windowConfig, []*scheduler.JobLineageSummary{blocking},
			createRunningJobRunPair("root", "blocked", baseTime, baseTime.Add(5*time.Minute)))
		finished := createJobLineage("finished", tnnt, &windowConfig, nil,
			createJobRunPair("root", "finished", baseTime, baseTime.Add(1*time.Minute), baseTime.Add(4*time.Minute)))
		root := createJobLineage("root", tnnt, &windowConfig, []*scheduler.JobLineageSummary{blocked, finished},
			createRunningJobRunPair("root", "root", baseTime, baseTime.Add(20*time.Minute)))

		result := root.GetLineageNodes(scheduler.LineageWalkOptions{MaxNodes: 0, MaxDepth: 5})

		assert.True(t, findNode(result.Nodes, "blocking").IsBlocking, "its upstreams have all finished")
		assert.False(t, findNode(result.Nodes, "blocked").IsBlocking, "it is waiting on an unfinished upstream")
		assert.False(t, findNode(result.Nodes, "finished").IsBlocking, "a finished run blocks nothing")
		assert.False(t, findNode(result.Nodes, "root").IsBlocking, "the target is waiting on its upstream")
	})

	t.Run("should mark a leaf that has not started as blocking", func(t *testing.T) {
		notStarted := &scheduler.JobLineageSummary{
			JobName: scheduler.JobName("not_started"),
			Tenant:  tnnt,
			JobRuns: map[scheduler.JobName]*scheduler.JobRunSummary{
				scheduler.JobName("root"): {
					JobName:     scheduler.JobName("not_started"),
					ScheduledAt: baseTime,
				},
			},
		}
		root := createJobLineage("root", tnnt, &windowConfig, []*scheduler.JobLineageSummary{notStarted},
			createRunningJobRunPair("root", "root", baseTime, baseTime.Add(20*time.Minute)))

		result := root.GetLineageNodes(scheduler.LineageWalkOptions{MaxNodes: 0, MaxDepth: 5})

		notStartedNode := findNode(result.Nodes, "not_started")
		if assert.NotNil(t, notStartedNode) {
			assert.Equal(t, scheduler.StateNotScheduled, notStartedNode.State)
			assert.True(t, notStartedNode.IsBlocking)
		}
	})

	t.Run("should order nodes by level, then by the state most worth attention", func(t *testing.T) {
		succeeded := createJobLineage("succeeded", tnnt, &windowConfig, nil,
			createJobRunPair("root", "succeeded", baseTime, baseTime.Add(1*time.Minute), baseTime.Add(4*time.Minute)))
		running := createJobLineage("running", tnnt, &windowConfig, nil,
			createRunningJobRunPair("root", "running", baseTime, baseTime.Add(5*time.Minute)))
		failedRun := createJobRun("failed", baseTime, baseTime.Add(2*time.Minute), baseTime.Add(3*time.Minute))
		failedRun.JobStatus = scheduler.StateFailed.String()
		failed := createJobLineage("failed", tnnt, &windowConfig, nil,
			downstreamJobNameAndRunPair{JobName: scheduler.JobName("root"), JobRunSum: failedRun})
		root := createJobLineage("root", tnnt, &windowConfig,
			[]*scheduler.JobLineageSummary{succeeded, running, failed},
			createRunningJobRunPair("root", "root", baseTime, baseTime.Add(20*time.Minute)))

		result := root.GetLineageNodes(scheduler.LineageWalkOptions{MaxNodes: 0, MaxDepth: 5})

		assert.Equal(t, scheduler.JobName("root"), result.Nodes[0].JobName, "the target comes first")
		assert.Equal(t, scheduler.JobName("failed"), result.Nodes[1].JobName)
		assert.Equal(t, scheduler.JobName("running"), result.Nodes[2].JobName)
		assert.Equal(t, scheduler.JobName("succeeded"), result.Nodes[3].JobName)
	})
}

func TestJobLineageSummary_GetLineageNodes_TopUpstreamsPerJob(t *testing.T) {
	projName := tenant.ProjectName("proj")
	namespaceName := tenant.ProjectName("ns1")
	tnnt, _ := tenant.NewTenant(projName.String(), namespaceName.String())

	baseTime := time.Date(2023, 1, 1, 10, 0, 0, 0, time.UTC)
	windowConfig, _ := window.NewPresetConfig("yesterday")

	// finishedUpstream builds an upstream of parent that ended endOffset after baseTime
	finishedUpstream := func(name, parent string, endOffset time.Duration, upstreams []*scheduler.JobLineageSummary) *scheduler.JobLineageSummary {
		return createJobLineage(name, tnnt, &windowConfig, upstreams,
			createJobRunPair(parent, name, baseTime, baseTime.Add(1*time.Minute), baseTime.Add(endOffset)))
	}

	t.Run("should keep only the latest finishing upstreams of each job", func(t *testing.T) {
		var upstreams []*scheduler.JobLineageSummary
		for i := 1; i <= 5; i++ {
			upstreams = append(upstreams, finishedUpstream(fmt.Sprintf("upstream%d", i), "root", time.Duration(i)*time.Minute, nil))
		}
		root := createJobLineage("root", tnnt, &windowConfig, upstreams,
			createJobRunPair("root", "root", baseTime, baseTime.Add(30*time.Minute), baseTime.Add(40*time.Minute)))

		result := root.GetLineageNodes(scheduler.LineageWalkOptions{TopUpstreamsPerJob: 2})

		assert.Len(t, result.Nodes, 3) // root plus its two latest finishing upstreams
		assert.NotNil(t, findNode(result.Nodes, "upstream5"))
		assert.NotNil(t, findNode(result.Nodes, "upstream4"))
		assert.Nil(t, findNode(result.Nodes, "upstream3"))
	})

	t.Run("should trace every kept upstream all the way to the root upstream", func(t *testing.T) {
		// the old per-level cap only ever expanded the single best candidate, so branches below
		// the runner-up were lost. Each kept upstream must be followed to the top.
		deepA := finishedUpstream("deepA", "keptA", 2*time.Minute, nil)
		deepB := finishedUpstream("deepB", "keptB", 3*time.Minute, nil)
		keptA := finishedUpstream("keptA", "root", 20*time.Minute, []*scheduler.JobLineageSummary{deepA})
		keptB := finishedUpstream("keptB", "root", 19*time.Minute, []*scheduler.JobLineageSummary{deepB})
		dropped := finishedUpstream("dropped", "root", 5*time.Minute, nil)
		root := createJobLineage("root", tnnt, &windowConfig,
			[]*scheduler.JobLineageSummary{keptA, keptB, dropped},
			createJobRunPair("root", "root", baseTime, baseTime.Add(30*time.Minute), baseTime.Add(40*time.Minute)))

		result := root.GetLineageNodes(scheduler.LineageWalkOptions{TopUpstreamsPerJob: 2})

		assert.NotNil(t, findNode(result.Nodes, "deepA"), "the top upstream's branch is traced")
		assert.NotNil(t, findNode(result.Nodes, "deepB"), "the runner-up's branch is traced too")
		assert.Nil(t, findNode(result.Nodes, "dropped"))
	})

	t.Run("should keep an upstream outside one job's top n when another job reaches it", func(t *testing.T) {
		// the cap bounds expansion, not membership
		shared := createJobLineage("shared", tnnt, &windowConfig, nil,
			createJobRunPair("low", "shared", baseTime, baseTime.Add(1*time.Minute), baseTime.Add(2*time.Minute)),
			createJobRunPair("root", "shared", baseTime, baseTime.Add(1*time.Minute), baseTime.Add(2*time.Minute)))
		high := finishedUpstream("high", "root", 25*time.Minute, nil)
		low := createJobLineage("low", tnnt, &windowConfig, []*scheduler.JobLineageSummary{shared},
			createJobRunPair("root", "low", baseTime, baseTime.Add(1*time.Minute), baseTime.Add(24*time.Minute)))
		root := createJobLineage("root", tnnt, &windowConfig,
			[]*scheduler.JobLineageSummary{high, low, shared},
			createJobRunPair("root", "root", baseTime, baseTime.Add(30*time.Minute), baseTime.Add(40*time.Minute)))

		result := root.GetLineageNodes(scheduler.LineageWalkOptions{TopUpstreamsPerJob: 2})

		// root's top 2 are high and low, so shared is not followed as a direct upstream of
		// root - but low is followed, and shared is reached through it
		sharedNode := findNode(result.Nodes, "shared")
		if assert.NotNil(t, sharedNode, "the cap bounds expansion, not membership") {
			assert.Equal(t, 2, sharedNode.Level, "reached via low rather than directly from root")
			assert.Len(t, sharedNode.DownstreamRefs, 1)
			assert.Equal(t, scheduler.JobName("low"), sharedNode.DownstreamRefs[0].JobName)
		}
	})

	t.Run("should rank unfinished upstreams last since they have no finish time", func(t *testing.T) {
		running := createJobLineage("running", tnnt, &windowConfig, nil,
			createRunningJobRunPair("root", "running", baseTime, baseTime.Add(1*time.Minute)))
		finished := finishedUpstream("finished", "root", 5*time.Minute, nil)
		root := createJobLineage("root", tnnt, &windowConfig,
			[]*scheduler.JobLineageSummary{running, finished},
			createJobRunPair("root", "root", baseTime, baseTime.Add(30*time.Minute), baseTime.Add(40*time.Minute)))

		result := root.GetLineageNodes(scheduler.LineageWalkOptions{TopUpstreamsPerJob: 1})

		assert.NotNil(t, findNode(result.Nodes, "finished"))
		assert.Nil(t, findNode(result.Nodes, "running"),
			"documented trade-off: this mode is for completed lineages only")
	})

	t.Run("should walk every upstream when the cap is zero", func(t *testing.T) {
		var upstreams []*scheduler.JobLineageSummary
		for i := 1; i <= 5; i++ {
			upstreams = append(upstreams, finishedUpstream(fmt.Sprintf("upstream%d", i), "root", time.Duration(i)*time.Minute, nil))
		}
		root := createJobLineage("root", tnnt, &windowConfig, upstreams,
			createJobRunPair("root", "root", baseTime, baseTime.Add(30*time.Minute), baseTime.Add(40*time.Minute)))

		result := root.GetLineageNodes(scheduler.LineageWalkOptions{})

		assert.Len(t, result.Nodes, 6)
	})
}

func TestLineageWalkResult_GatingPath(t *testing.T) {
	projName := tenant.ProjectName("proj")
	namespaceName := tenant.ProjectName("ns1")
	tnnt, _ := tenant.NewTenant(projName.String(), namespaceName.String())

	baseTime := time.Date(2023, 1, 1, 10, 0, 0, 0, time.UTC)
	windowConfig, _ := window.NewPresetConfig("yesterday")

	pathNames := func(path []*scheduler.JobExecutionSummary) []string {
		names := make([]string, 0, len(path))
		for _, node := range path {
			names = append(names, node.JobName.String())
		}
		return names
	}

	t.Run("should follow real edges rather than pairing the latest finisher of each level", func(t *testing.T) {
		// root -> {A, B}; A -> C, B -> D. B finishes last at level 1 and C last at level 2, but
		// C is on A's branch. Pairing by level would report C as the run that gated B, which
		// never waited on it.
		c := createJobLineage("C", tnnt, &windowConfig, nil,
			createJobRunPair("A", "C", baseTime, baseTime.Add(1*time.Minute), baseTime.Add(9*time.Minute)))
		d := createJobLineage("D", tnnt, &windowConfig, nil,
			createJobRunPair("B", "D", baseTime, baseTime.Add(1*time.Minute), baseTime.Add(4*time.Minute)))
		a := createJobLineage("A", tnnt, &windowConfig, []*scheduler.JobLineageSummary{c},
			createJobRunPair("root", "A", baseTime, baseTime.Add(10*time.Minute), baseTime.Add(12*time.Minute)))
		b := createJobLineage("B", tnnt, &windowConfig, []*scheduler.JobLineageSummary{d},
			createJobRunPair("root", "B", baseTime, baseTime.Add(10*time.Minute), baseTime.Add(20*time.Minute)))
		root := createJobLineage("root", tnnt, &windowConfig, []*scheduler.JobLineageSummary{a, b},
			createJobRunPair("root", "root", baseTime, baseTime.Add(25*time.Minute), baseTime.Add(30*time.Minute)))

		path := root.GetLineageNodes(scheduler.LineageWalkOptions{MaxNodes: 0, MaxDepth: 5}).GatingPath()

		assert.Equal(t, []string{"root", "B", "D"}, pathNames(path),
			"the chain must stay on B's branch once B is chosen")
	})

	t.Run("should stop at the run the lineage is currently waiting on", func(t *testing.T) {
		blocking := createJobLineage("blocking", tnnt, &windowConfig, nil,
			createRunningJobRunPair("root", "blocking", baseTime, baseTime.Add(2*time.Minute)))
		root := createJobLineage("root", tnnt, &windowConfig, []*scheduler.JobLineageSummary{blocking},
			createRunningJobRunPair("root", "root", baseTime, baseTime.Add(20*time.Minute)))

		path := root.GetLineageNodes(scheduler.LineageWalkOptions{MaxNodes: 0, MaxDepth: 5}).GatingPath()

		assert.Equal(t, []string{"root"}, pathNames(path),
			"an unfinished upstream ends the chain rather than being walked through")
	})

	t.Run("should include an unfinished target as the head of the chain", func(t *testing.T) {
		finished := createJobLineage("finished", tnnt, &windowConfig, nil,
			createJobRunPair("root", "finished", baseTime, baseTime.Add(1*time.Minute), baseTime.Add(5*time.Minute)))
		root := createJobLineage("root", tnnt, &windowConfig, []*scheduler.JobLineageSummary{finished},
			createRunningJobRunPair("root", "root", baseTime, baseTime.Add(20*time.Minute)))

		path := root.GetLineageNodes(scheduler.LineageWalkOptions{MaxNodes: 0, MaxDepth: 5}).GatingPath()

		assert.Equal(t, []string{"root", "finished"}, pathNames(path))
	})

	t.Run("should break ties on job name so the chain does not vary between requests", func(t *testing.T) {
		sameEnd := baseTime.Add(5 * time.Minute)
		zebra := createJobLineage("zebra", tnnt, &windowConfig, nil,
			createJobRunPair("root", "zebra", baseTime, baseTime.Add(1*time.Minute), sameEnd))
		alpha := createJobLineage("alpha", tnnt, &windowConfig, nil,
			createJobRunPair("root", "alpha", baseTime, baseTime.Add(1*time.Minute), sameEnd))
		root := createJobLineage("root", tnnt, &windowConfig, []*scheduler.JobLineageSummary{zebra, alpha},
			createJobRunPair("root", "root", baseTime, baseTime.Add(25*time.Minute), baseTime.Add(30*time.Minute)))

		for i := 0; i < 5; i++ {
			path := root.GetLineageNodes(scheduler.LineageWalkOptions{MaxNodes: 0, MaxDepth: 5}).GatingPath()
			assert.Equal(t, []string{"root", "alpha"}, pathNames(path))
		}
	})

	t.Run("should return an empty path for an empty walk", func(t *testing.T) {
		var jobLineage *scheduler.JobLineageSummary

		assert.Nil(t, jobLineage.GetLineageNodes(scheduler.LineageWalkOptions{MaxNodes: 0, MaxDepth: 5}).GatingPath())
	})
}

func TestJobRunSummary_IsFinished(t *testing.T) {
	baseTime := time.Date(2024, 1, 1, 10, 0, 0, 0, time.UTC)
	taskStart := baseTime.Add(5 * time.Minute)
	taskEnd := baseTime.Add(20 * time.Minute)
	hookStart := baseTime.Add(21 * time.Minute)
	hookEnd := baseTime.Add(30 * time.Minute)

	t.Run("should report not finished for a nil run", func(t *testing.T) {
		var run *scheduler.JobRunSummary
		assert.False(t, run.IsFinished())
	})

	t.Run("should report not finished when the run has no recorded times", func(t *testing.T) {
		run := &scheduler.JobRunSummary{ScheduledAt: baseTime}
		assert.False(t, run.IsFinished())
	})

	t.Run("should report not finished while the task is still running", func(t *testing.T) {
		run := &scheduler.JobRunSummary{ScheduledAt: baseTime, TaskStartTime: &taskStart}
		assert.False(t, run.IsFinished())
	})

	t.Run("should report finished when the task ended and the run has no hooks", func(t *testing.T) {
		run := &scheduler.JobRunSummary{ScheduledAt: baseTime, TaskStartTime: &taskStart, TaskEndTime: &taskEnd}
		assert.True(t, run.IsFinished())
	})

	t.Run("should report not finished when a hook is still running despite a populated task end", func(t *testing.T) {
		// hook_end_time is NULL while any hook is unfinished, so GetActualEndTime falls back
		// to the task end and would otherwise make the run look complete
		run := &scheduler.JobRunSummary{
			ScheduledAt:   baseTime,
			TaskStartTime: &taskStart,
			TaskEndTime:   &taskEnd,
			HookStartTime: &hookStart,
		}
		assert.Nil(t, run.HookEndTime)
		assert.NotNil(t, run.GetActualEndTime())
		assert.False(t, run.IsFinished())
	})

	t.Run("should report finished once every hook has ended", func(t *testing.T) {
		run := &scheduler.JobRunSummary{
			ScheduledAt:   baseTime,
			TaskStartTime: &taskStart,
			TaskEndTime:   &taskEnd,
			HookStartTime: &hookStart,
			HookEndTime:   &hookEnd,
		}
		assert.True(t, run.IsFinished())
	})

	t.Run("should report finished when only the hook end is known", func(t *testing.T) {
		run := &scheduler.JobRunSummary{ScheduledAt: baseTime, HookEndTime: &hookEnd}
		assert.True(t, run.IsFinished())
	})
}

func TestJobRunSummary_GetState(t *testing.T) {
	baseTime := time.Date(2024, 1, 1, 10, 0, 0, 0, time.UTC)
	sensorStart := baseTime.Add(1 * time.Minute)
	taskStart := baseTime.Add(5 * time.Minute)
	taskEnd := baseTime.Add(20 * time.Minute)
	hookStart := baseTime.Add(21 * time.Minute)
	hookEnd := baseTime.Add(30 * time.Minute)

	t.Run("should report not scheduled for a nil run", func(t *testing.T) {
		var run *scheduler.JobRunSummary
		assert.Equal(t, scheduler.StateNotScheduled, run.GetState())
	})

	t.Run("should report not scheduled for a run the lineage expected but which has no job run row", func(t *testing.T) {
		run := &scheduler.JobRunSummary{ScheduledAt: baseTime}
		assert.Equal(t, scheduler.StateNotScheduled, run.GetState())
	})

	t.Run("should report not scheduled when the run exists but nothing has started", func(t *testing.T) {
		run := &scheduler.JobRunSummary{ScheduledAt: baseTime, JobStatus: scheduler.StateQueued.String()}
		assert.Equal(t, scheduler.StateNotScheduled, run.GetState())
	})

	t.Run("should report waiting upstream while only the sensor has started", func(t *testing.T) {
		run := &scheduler.JobRunSummary{
			ScheduledAt:   baseTime,
			JobStatus:     scheduler.StateRunning.String(),
			WaitStartTime: &sensorStart,
		}
		assert.Equal(t, scheduler.StateWaitUpstream, run.GetState())
	})

	t.Run("should report running once the task has started", func(t *testing.T) {
		run := &scheduler.JobRunSummary{
			ScheduledAt:   baseTime,
			JobStatus:     scheduler.StateRunning.String(),
			WaitStartTime: &sensorStart,
			TaskStartTime: &taskStart,
		}
		assert.Equal(t, scheduler.StateRunning, run.GetState())
	})

	t.Run("should report running while a retry is in flight and the task end is unknown", func(t *testing.T) {
		run := &scheduler.JobRunSummary{
			ScheduledAt:   baseTime,
			JobStatus:     scheduler.StateUpForRetry.String(),
			TaskStartTime: &taskStart,
		}
		assert.Equal(t, scheduler.StateRunning, run.GetState())
	})

	t.Run("should report success when the run finished successfully", func(t *testing.T) {
		run := &scheduler.JobRunSummary{
			ScheduledAt:   baseTime,
			JobStatus:     scheduler.StateSuccess.String(),
			TaskStartTime: &taskStart,
			TaskEndTime:   &taskEnd,
			HookStartTime: &hookStart,
			HookEndTime:   &hookEnd,
		}
		assert.Equal(t, scheduler.StateSuccess, run.GetState())
	})

	t.Run("should report failed when the run finished unsuccessfully", func(t *testing.T) {
		run := &scheduler.JobRunSummary{
			ScheduledAt:   baseTime,
			JobStatus:     scheduler.StateFailed.String(),
			TaskStartTime: &taskStart,
			TaskEndTime:   &taskEnd,
		}
		assert.Equal(t, scheduler.StateFailed, run.GetState())
	})

	t.Run("should report running when the status is terminal but a hook is still executing", func(t *testing.T) {
		run := &scheduler.JobRunSummary{
			ScheduledAt:   baseTime,
			JobStatus:     scheduler.StateFailed.String(),
			TaskStartTime: &taskStart,
			TaskEndTime:   &taskEnd,
			HookStartTime: &hookStart,
		}
		assert.Equal(t, scheduler.StateRunning, run.GetState())
	})

	t.Run("should report success when the timestamps show completion before the status catches up", func(t *testing.T) {
		run := &scheduler.JobRunSummary{
			ScheduledAt:   baseTime,
			JobStatus:     scheduler.StateRunning.String(),
			TaskStartTime: &taskStart,
			TaskEndTime:   &taskEnd,
		}
		assert.Equal(t, scheduler.StateSuccess, run.GetState())
	})

	t.Run("should report success when the run completed under an unrecognised status", func(t *testing.T) {
		run := &scheduler.JobRunSummary{
			ScheduledAt:   baseTime,
			JobStatus:     "some_unknown_status",
			TaskStartTime: &taskStart,
			TaskEndTime:   &taskEnd,
		}
		assert.Equal(t, scheduler.StateSuccess, run.GetState())
	})

	t.Run("should report running when an unrecognised status is still in flight", func(t *testing.T) {
		run := &scheduler.JobRunSummary{
			ScheduledAt:   baseTime,
			JobStatus:     "some_unknown_status",
			TaskStartTime: &taskStart,
		}
		assert.Equal(t, scheduler.StateRunning, run.GetState())
	})
}
