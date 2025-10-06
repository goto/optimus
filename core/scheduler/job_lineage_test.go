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

	createJobRun := func(jobName scheduler.JobName, startOffset, endOffset time.Duration) *scheduler.JobRunSummary {
		startTime := baseTime.Add(startOffset)
		endTime := baseTime.Add(endOffset)
		return &scheduler.JobRunSummary{
			JobName:      jobName,
			ScheduledAt:  baseTime,
			JobStartTime: &startTime,
			JobEndTime:   &endTime,
		}
	}

	t.Run("should handle nested upstream lineage", func(t *testing.T) {
		upstreamLevel2A := &scheduler.JobLineageSummary{
			JobName:          scheduler.JobName("upstream_level2_a"),
			Tenant:           tnnt,
			ScheduleInterval: "0 6 * * *",
			Window:           &windowConfig,
			SLA:              scheduler.SLAConfig{Duration: time.Hour},
			JobRuns: map[string]*scheduler.JobRunSummary{
				"run1": createJobRun(scheduler.JobName("upstream_level2_a"), 0, 10*time.Minute),
			},
		}

		upstreamLevel2B := &scheduler.JobLineageSummary{
			JobName:          scheduler.JobName("upstream_level2_b"),
			Tenant:           tnnt,
			ScheduleInterval: "0 7 * * *",
			Window:           &windowConfig,
			SLA:              scheduler.SLAConfig{Duration: time.Hour},
			JobRuns: map[string]*scheduler.JobRunSummary{
				"run1": createJobRun(scheduler.JobName("upstream_level2_b"), 0, 8*time.Minute),
			},
		}

		upstream1 := &scheduler.JobLineageSummary{
			JobName:          scheduler.JobName("upstream1"),
			Tenant:           tnnt,
			ScheduleInterval: "0 8 * * *",
			SLA:              scheduler.SLAConfig{Duration: time.Hour},
			Window:           &windowConfig,
			Upstreams:        []*scheduler.JobLineageSummary{upstreamLevel2A},
			JobRuns: map[string]*scheduler.JobRunSummary{
				"run1": createJobRun(scheduler.JobName("upstream1"), 0, 15*time.Minute),
			},
		}

		upstream2 := &scheduler.JobLineageSummary{
			JobName:          scheduler.JobName("upstream2"),
			Tenant:           tnnt,
			ScheduleInterval: "0 9 * * *",
			SLA:              scheduler.SLAConfig{Duration: time.Hour},
			Window:           &windowConfig,
			Upstreams:        []*scheduler.JobLineageSummary{upstreamLevel2B},
			JobRuns: map[string]*scheduler.JobRunSummary{
				"run1": createJobRun(scheduler.JobName("upstream2"), 0, 25*time.Minute),
			},
		}

		jobLineage := &scheduler.JobLineageSummary{
			JobName:          scheduler.JobName("job1"),
			Tenant:           tnnt,
			ScheduleInterval: "0 10 * * *",
			SLA:              scheduler.SLAConfig{Duration: 2 * time.Hour},
			Window:           &windowConfig,
			Upstreams:        []*scheduler.JobLineageSummary{upstream1, upstream2},
			JobRuns: map[string]*scheduler.JobRunSummary{
				"run1": createJobRun(scheduler.JobName("job1"), 0, 30*time.Minute),
			},
		}

		jobRunSums := jobLineage.Flatten()
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
}
