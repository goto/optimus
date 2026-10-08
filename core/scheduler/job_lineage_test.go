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

type downstreamRunPair struct {
	Identifier scheduler.JobRunIdentifier
	JobRunSum  *scheduler.JobRunSummary
}

func createJobLineage(name string, tnnt tenant.Tenant, windowConfig *window.Config, upstreams []*scheduler.JobLineageSummary, runPairs ...downstreamRunPair) *scheduler.JobLineageSummary {
	node := scheduler.NewJobLineageSummary(scheduler.JobName(name), tnnt, "0 8 * * *", scheduler.SLAConfig{Duration: time.Hour}, windowConfig, true)

	for _, upstream := range upstreams {
		node.AddUpstream(upstream)
	}

	for _, pair := range runPairs {
		node.RecordRun(pair.Identifier, pair.JobRunSum)
	}

	return node
}

// createJobRunPair builds one entry of a job's run set: the run of this job (jobName, scheduled
// at baseTime) relevant to the specific run of its immediate downstream identified by
// (downstreamJobName, downstreamScheduledAt).
// nolint:unparam
func createJobRunPair(downstreamJobName string, downstreamScheduledAt time.Time, jobName string, baseTime, startTime, endTime time.Time) downstreamRunPair {
	return downstreamRunPair{
		Identifier: scheduler.JobRunIdentifier{JobName: scheduler.JobName(downstreamJobName), ScheduledAt: downstreamScheduledAt},
		JobRunSum:  createJobRun(jobName, baseTime, startTime, endTime),
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
		result := jobLineage.GenerateLineageExecutionSummary(10, 5)
		assert.Nil(t, result)
	})

	t.Run("should generate execution summary for single job", func(t *testing.T) {
		startTime := baseTime.Add(10 * time.Minute)
		endTime := baseTime.Add(20 * time.Minute)
		jobLineage := createJobLineage("root", tnnt, &windowConfig, nil,
			createJobRunPair("root", baseTime, "root", baseTime, startTime, endTime))

		result := jobLineage.GenerateLineageExecutionSummary(10, 5)

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
			createJobRunPair("root", baseTime, "upstream", baseTime, upstreamStartTime, upstreamEndTime))
		jobLineage := createJobLineage("root", tnnt, &windowConfig, []*scheduler.JobLineageSummary{upstream},
			createJobRunPair("root", baseTime, "root", baseTime, rootStartTime, rootEndTime))

		result := jobLineage.GenerateLineageExecutionSummary(10, 5)

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
			createJobRunPair("root", baseRootTime, "upstream", baseTime, upstreamStartTime, upstreamEndTime))
		jobLineage := createJobLineage("root", tnnt, &windowConfig, []*scheduler.JobLineageSummary{upstream},
			createJobRunPair("root", baseRootTime, "root", baseRootTime, rootStartTime, rootEndTime))

		result := jobLineage.GenerateLineageExecutionSummary(10, 5)

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

		jobLineage := scheduler.NewJobLineageSummary("root", tnnt, "0 8 * * *", scheduler.SLAConfig{Duration: time.Hour}, &windowConfig, true)
		jobLineage.RecordOwnRun(jobRun)

		result := jobLineage.GenerateLineageExecutionSummary(10, 5)

		assert.Len(t, result.ExecutionSummary.TopLongestTaskDurationJobs, 1)
		assert.Len(t, result.ExecutionSummary.TopLongestHookDurationJobs, 1)
		assert.Equal(t, 10*time.Minute, result.ExecutionSummary.TopLongestTaskDurationJobs[0].TaskDuration)
		assert.Equal(t, 5*time.Minute, result.ExecutionSummary.TopLongestHookDurationJobs[0].TaskDuration)
	})

	t.Run("should limit top longest jobs to 3", func(t *testing.T) {
		jobLineage := scheduler.NewJobLineageSummary("root", tenant.Tenant{}, "", scheduler.SLAConfig{}, nil, true)

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

			upstream := scheduler.NewJobLineageSummary(jobName, tenant.Tenant{}, "", scheduler.SLAConfig{}, nil, true)
			upstream.RecordRun(scheduler.JobRunIdentifier{JobName: "root", ScheduledAt: baseTime}, jobRun)
			jobLineage.AddUpstream(upstream)
		}

		rootRun := &scheduler.JobRunSummary{
			JobName:     scheduler.JobName("root"),
			ScheduledAt: baseTime,
		}
		jobLineage.RecordOwnRun(rootRun)

		result := jobLineage.GenerateLineageExecutionSummary(10, 5)

		assert.LessOrEqual(t, len(result.ExecutionSummary.TopLongestTaskDurationJobs), 3)
	})

	t.Run("should handle jobs without completed runs", func(t *testing.T) {
		incompleteRun := &scheduler.JobRunSummary{
			JobName:       scheduler.JobName("incomplete"),
			ScheduledAt:   baseTime,
			TaskStartTime: nil,
			TaskEndTime:   nil,
		}

		upstream := scheduler.NewJobLineageSummary("incomplete", tenant.Tenant{}, "", scheduler.SLAConfig{}, nil, true)
		upstream.RecordRun(scheduler.JobRunIdentifier{JobName: "root", ScheduledAt: baseTime}, incompleteRun)

		rootRun := &scheduler.JobRunSummary{
			JobName:     scheduler.JobName("root"),
			ScheduledAt: baseTime,
		}

		jobLineage := scheduler.NewJobLineageSummary("root", tenant.Tenant{}, "", scheduler.SLAConfig{}, nil, true)
		jobLineage.AddUpstream(upstream)
		jobLineage.RecordOwnRun(rootRun)

		result := jobLineage.GenerateLineageExecutionSummary(10, 5)

		assert.NotNil(t, result)
		assert.Empty(t, result.ExecutionSummary.TopLongestTaskDurationJobs)
	})
}

func TestJobLineageSummary_GetFlattenedSummaries(t *testing.T) {
	projName := tenant.ProjectName("proj")
	namespaceName := tenant.ProjectName("ns1")
	tnnt, _ := tenant.NewTenant(projName.String(), namespaceName.String())

	baseTime := time.Date(2023, 1, 1, 10, 0, 0, 0, time.UTC)
	windowConfig, _ := window.NewPresetConfig("yesterday")

	t.Run("should return single job when no upstreams", func(t *testing.T) {
		jobLineage := createJobLineage("root", tnnt, &windowConfig, nil,
			createJobRunPair("root", baseTime, "root", baseTime, baseTime.Add(10*time.Minute), baseTime.Add(20*time.Minute)))

		summaries := jobLineage.GetLineageNodes(scheduler.LineageWalkOptions{MaxNodes: 1000, MaxDepth: 5}).Nodes

		assert.Len(t, summaries, 1)
		assert.Equal(t, scheduler.JobName("root"), summaries[0].JobName)
		assert.Equal(t, 0, summaries[0].Level)
	})

	t.Run("should respect max upstreams per level", func(t *testing.T) {
		var upstreams []*scheduler.JobLineageSummary
		for i := 1; i <= 5; i++ {
			upstream := createJobLineage(fmt.Sprintf("upstream%d", i), tnnt, &windowConfig, nil,
				createJobRunPair("root", baseTime, fmt.Sprintf("upstream%d", i), baseTime, baseTime.Add(time.Duration(i)*time.Minute), baseTime.Add(time.Duration(i+10)*time.Minute)))
			upstreams = append(upstreams, upstream)
		}

		jobLineage := createJobLineage("root", tnnt, &windowConfig, upstreams,
			createJobRunPair("root", baseTime, "root", baseTime, baseTime.Add(30*time.Minute), baseTime.Add(40*time.Minute)))

		summaries := jobLineage.GetLineageNodes(scheduler.LineageWalkOptions{MaxNodes: 1000, MaxDepth: 5}).Nodes

		level1Count := 0
		for _, summary := range summaries {
			if summary.Level == 1 {
				level1Count++
			}
		}
		assert.LessOrEqual(t, level1Count, 3)
	})

	t.Run("should respect max depth", func(t *testing.T) {
		level3 := createJobLineage("level3", tnnt, &windowConfig, nil,
			createJobRunPair("level2", baseTime, "level3", baseTime, baseTime.Add(5*time.Minute), baseTime.Add(10*time.Minute)))
		level2 := createJobLineage("level2", tnnt, &windowConfig, []*scheduler.JobLineageSummary{level3},
			createJobRunPair("level1", baseTime, "level2", baseTime, baseTime.Add(15*time.Minute), baseTime.Add(20*time.Minute)))
		level1 := createJobLineage("level1", tnnt, &windowConfig, []*scheduler.JobLineageSummary{level2},
			createJobRunPair("root", baseTime, "level1", baseTime, baseTime.Add(25*time.Minute), baseTime.Add(30*time.Minute)))
		root := createJobLineage("root", tnnt, &windowConfig, []*scheduler.JobLineageSummary{level1},
			createJobRunPair("root", baseTime, "root", baseTime, baseTime.Add(35*time.Minute), baseTime.Add(40*time.Minute)))

		summaries := root.GetLineageNodes(scheduler.LineageWalkOptions{MaxNodes: 1000, MaxDepth: 2}).Nodes

		maxLevel := 0
		for _, summary := range summaries {
			if summary.Level > maxLevel {
				maxLevel = summary.Level
			}
		}
		assert.LessOrEqual(t, maxLevel, 2)
	})

	t.Run("should use default max upstreams when zero provided", func(t *testing.T) {
		upstream := createJobLineage("upstream", tnnt, &windowConfig, nil,
			createJobRunPair("root", baseTime, "upstream", baseTime, baseTime.Add(5*time.Minute), baseTime.Add(10*time.Minute)))
		jobLineage := createJobLineage("root", tnnt, &windowConfig, []*scheduler.JobLineageSummary{upstream},
			createJobRunPair("root", baseTime, "root", baseTime, baseTime.Add(15*time.Minute), baseTime.Add(20*time.Minute)))

		summaries := jobLineage.GetLineageNodes(scheduler.LineageWalkOptions{MaxNodes: 1000, MaxDepth: 5}).Nodes

		assert.Len(t, summaries, 2)
	})

	t.Run("should sort upstreams by end time and scheduled time", func(t *testing.T) {
		upstream1 := createJobLineage("upstream1", tnnt, &windowConfig, nil,
			createJobRunPair("root", baseTime, "upstream1", baseTime, baseTime.Add(5*time.Minute), baseTime.Add(10*time.Minute)))
		upstream2 := createJobLineage("upstream2", tnnt, &windowConfig, nil,
			createJobRunPair("root", baseTime, "upstream2", baseTime, baseTime.Add(7*time.Minute), baseTime.Add(15*time.Minute)))
		upstream3 := createJobLineage("upstream3", tnnt, &windowConfig, nil,
			createJobRunPair("root", baseTime, "upstream3", baseTime, baseTime.Add(3*time.Minute), baseTime.Add(8*time.Minute)))

		jobLineage := createJobLineage("root", tnnt, &windowConfig,
			[]*scheduler.JobLineageSummary{upstream1, upstream2, upstream3},
			createJobRunPair("root", baseTime, "root", baseTime, baseTime.Add(20*time.Minute), baseTime.Add(30*time.Minute)))

		summaries := jobLineage.GetLineageNodes(scheduler.LineageWalkOptions{MaxNodes: 1000, MaxDepth: 5}).Nodes

		level1Jobs := make([]string, 0)
		for _, summary := range summaries {
			if summary.Level == 1 {
				level1Jobs = append(level1Jobs, summary.JobName.String())
			}
		}
		assert.Len(t, level1Jobs, 2)
	})

	t.Run("should skip upstreams without job runs", func(t *testing.T) {
		upstreamWithRun := createJobLineage("with_run", tnnt, &windowConfig, nil,
			createJobRunPair("root", baseTime, "with_run", baseTime, baseTime.Add(5*time.Minute), baseTime.Add(10*time.Minute)))

		upstreamWithoutRun := scheduler.NewJobLineageSummary("without_run", tenant.Tenant{}, "", scheduler.SLAConfig{}, nil, true)

		jobLineage := createJobLineage("root", tnnt, &windowConfig,
			[]*scheduler.JobLineageSummary{upstreamWithRun, upstreamWithoutRun},
			createJobRunPair("root", baseTime, "root", baseTime, baseTime.Add(15*time.Minute), baseTime.Add(20*time.Minute)))
		summaries := jobLineage.GetLineageNodes(scheduler.LineageWalkOptions{MaxNodes: 1000, MaxDepth: 5}).Nodes

		level1Count := 0
		for _, summary := range summaries {
			if summary.Level == 1 {
				level1Count++
			}
		}
		assert.Equal(t, 1, level1Count)
	})

	t.Run("should look up a run beyond the first level by its immediate parent, not the tree root", func(t *testing.T) {
		// chain: root -> level1 -> level2. level2's run is keyed by "level1" (its true immediate
		// downstream), not "root". Only correct immediate-parent threading through the traversal
		// can resolve it - looking it up via the tree root ("root") would find nothing.
		level2 := createJobLineage("level2", tnnt, &windowConfig, nil,
			createJobRunPair("level1", baseTime, "level2", baseTime, baseTime.Add(1*time.Minute), baseTime.Add(5*time.Minute)))
		level1 := createJobLineage("level1", tnnt, &windowConfig, []*scheduler.JobLineageSummary{level2},
			createJobRunPair("root", baseTime, "level1", baseTime, baseTime.Add(10*time.Minute), baseTime.Add(15*time.Minute)))
		root := createJobLineage("root", tnnt, &windowConfig, []*scheduler.JobLineageSummary{level1},
			createJobRunPair("root", baseTime, "root", baseTime, baseTime.Add(20*time.Minute), baseTime.Add(25*time.Minute)))

		summaries := root.GetLineageNodes(scheduler.LineageWalkOptions{MaxNodes: 1000, MaxDepth: 5}).Nodes

		var foundLevel2 bool
		for _, summary := range summaries {
			if summary.JobName == "level2" {
				foundLevel2 = true
				assert.Equal(t, baseTime.Add(1*time.Minute), *summary.JobRunSummary.TaskStartTime)
			}
		}
		assert.True(t, foundLevel2, "level2 should be found via its immediate parent's key")
	})

	t.Run("diamond shared upstream is resolved with the run belonging to the traversed path", func(t *testing.T) {
		// topology: root -> {B, C}, both B and C -> D. D is the same shared node, but carries two
		// distinct runs - one per downstream path. B is crafted to rank ahead of C so the
		// traversal continues through B into D; D's run must then be the one keyed by B's
		// specific run, not C's or the tree root.
		d := createJobLineage("D", tnnt, &windowConfig, nil,
			createJobRunPair("B", baseTime, "D", baseTime.Add(-1*time.Hour), baseTime.Add(-55*time.Minute), baseTime.Add(-50*time.Minute)),
			createJobRunPair("C", baseTime, "D", baseTime.Add(-2*time.Hour), baseTime.Add(-115*time.Minute), baseTime.Add(-110*time.Minute)),
		)
		b := createJobLineage("B", tnnt, &windowConfig, []*scheduler.JobLineageSummary{d},
			createJobRunPair("root", baseTime, "B", baseTime, baseTime.Add(5*time.Minute), baseTime.Add(10*time.Minute)))
		c := createJobLineage("C", tnnt, &windowConfig, []*scheduler.JobLineageSummary{d},
			createJobRunPair("root", baseTime, "C", baseTime, baseTime.Add(1*time.Minute), baseTime.Add(2*time.Minute)))
		root := createJobLineage("root", tnnt, &windowConfig, []*scheduler.JobLineageSummary{b, c},
			createJobRunPair("root", baseTime, "root", baseTime, baseTime.Add(20*time.Minute), baseTime.Add(25*time.Minute)))

		summaries := root.GetLineageNodes(scheduler.LineageWalkOptions{MaxNodes: 1000, MaxDepth: 5}).Nodes

		var dSummary *scheduler.JobExecutionSummary
		for _, summary := range summaries {
			if summary.JobName == "D" {
				dSummary = summary
			}
		}

		if assert.NotNil(t, dSummary, "D should be reachable via B, the higher-ranked path") {
			assert.Equal(t, baseTime.Add(-1*time.Hour), dSummary.JobRunSummary.ScheduledAt)
			assert.Equal(t, "B", dSummary.DownstreamPathName)
		}
	})
}
