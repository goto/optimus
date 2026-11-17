package service

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/goto/salt/log"

	"github.com/goto/optimus/config"
	"github.com/goto/optimus/core/scheduler"
	"github.com/goto/optimus/core/tenant"
	"github.com/goto/optimus/internal/lib/window"
)

type stubJobRepository struct {
	JobRepository

	upstreams    *scheduler.Upstreams
	upstreamsErr error
	detailsByJob map[string]*scheduler.JobWithDetails
}

func (s *stubJobRepository) GetUpstreamsOfJob(_ context.Context, _ tenant.ProjectName, _ scheduler.JobName) (*scheduler.Upstreams, error) {
	return s.upstreams, s.upstreamsErr
}

func (s *stubJobRepository) GetJobDetails(_ context.Context, projectName tenant.ProjectName, jobName scheduler.JobName) (*scheduler.JobWithDetails, error) {
	details, ok := s.detailsByJob[jobName.String()+"@"+projectName.String()]
	if !ok {
		return nil, context.DeadlineExceeded
	}
	return details, nil
}

type stubProjectGetter struct {
	project *tenant.Project
}

func (s stubProjectGetter) Get(_ context.Context, _ tenant.ProjectName) (*tenant.Project, error) {
	return s.project, nil
}

func TestRecordTaskStartDelaySyncFilters(t *testing.T) {
	tnnt, err := tenant.NewTenant("proj", "ns1")
	assert.NoError(t, err)
	logger := log.NewNoop()
	service := NewJobRunService(logger, nil, &stubJobRunRepository{}, nil, nil, nil, nil, nil, nil, nil, nil, config.FeaturesConfig{}, nil, nil)

	newEvent := func(eventType scheduler.JobEventType, runID string, tryNumber int) *scheduler.Event {
		event := &scheduler.Event{
			JobName: scheduler.JobName("job_a"),
			Tenant:  tnnt,
			Type:    eventType,
		}
		if runID != "" {
			event.EventContext = &scheduler.EventContext{
				DagRun:              scheduler.DagRun{RunID: runID},
				OperatorRunInstance: scheduler.OperatorRunInstance{TryNumber: tryNumber},
			}
		}
		return event
	}

	t.Run("ignores non task start events", func(t *testing.T) {
		before := skippedDelayCount(t, "missing_run_id")
		service.recordTaskStartDelay(newEvent(scheduler.HookStartEvent, "", 1))
		assert.Equal(t, before, skippedDelayCount(t, "missing_run_id"))
	})

	t.Run("skips when run id is missing", func(t *testing.T) {
		before := skippedDelayCount(t, "missing_run_id")
		service.recordTaskStartDelay(newEvent(scheduler.TaskStartEvent, "", 1))
		assert.Equal(t, before+1, skippedDelayCount(t, "missing_run_id"))
	})

	t.Run("skips non regular runs", func(t *testing.T) {
		before := skippedDelayCount(t, "non_regular_run")
		service.recordTaskStartDelay(newEvent(scheduler.TaskStartEvent, "backfill_x", 1))
		assert.Equal(t, before+1, skippedDelayCount(t, "non_regular_run"))
	})

	t.Run("skips retry attempts", func(t *testing.T) {
		before := skippedDelayCount(t, "retry_attempt")
		service.recordTaskStartDelay(newEvent(scheduler.TaskStartEvent, "scheduled__2026-11-17T05:00:00+00:00", 2))
		assert.Equal(t, before+1, skippedDelayCount(t, "retry_attempt"))
	})

	t.Run("skips when all sampling slots are busy", func(t *testing.T) {
		// fill every slot, expect the skip, then release
		for i := 0; i < schedulerDelayMaxConcurrent; i++ {
			schedulerDelaySlots <- struct{}{}
		}
		before := skippedDelayCount(t, "overloaded")
		service.recordTaskStartDelay(newEvent(scheduler.TaskStartEvent, "scheduled__2026-11-17T05:00:00+00:00", 1))
		assert.Equal(t, before+1, skippedDelayCount(t, "overloaded"))
		for i := 0; i < schedulerDelayMaxConcurrent; i++ {
			<-schedulerDelaySlots
		}
	})
}

func TestObserveTaskStartDelay(t *testing.T) {
	ctx := context.Background()
	logger := log.NewNoop()

	tnnt, err := tenant.NewTenant("proj", "ns1")
	assert.NoError(t, err)
	upstreamTenant, err := tenant.NewTenant("proj", "ns1") // same host, same tenant in this test
	assert.NoError(t, err)

	jobName := scheduler.JobName("job_down")
	upstreamJobName := scheduler.JobName("job_up")

	scheduledAt := time.Date(2026, 11, 17, 5, 0, 0, 0, time.UTC)
	eventTime := scheduledAt.Add(20 * time.Minute)
	sensorEnd := scheduledAt.Add(15 * time.Minute)
	upstreamEnd := scheduledAt.Add(10 * time.Minute)

	preset, err := tenant.NewPreset("yesterday", "preset for test", "1d", "1d", "", "")
	assert.NoError(t, err)
	project, err := tenant.NewProject("proj", map[string]string{
		"STORAGE_PATH":   "file://",
		"SCHEDULER_HOST": "http://scheduler",
	}, map[string]string{})
	assert.NoError(t, err)
	project.SetPresets(map[string]tenant.Preset{"yesterday": preset})
	windowCfg, err := window.NewPresetConfig("yesterday")
	assert.NoError(t, err)

	newEvent := func() *scheduler.Event {
		return &scheduler.Event{
			JobName:        jobName,
			Tenant:         tnnt,
			Type:           scheduler.TaskStartEvent,
			EventTime:      eventTime,
			JobScheduledAt: scheduledAt,
			EventContext: &scheduler.EventContext{
				DagRun:              scheduler.DagRun{RunID: "scheduled__2026-11-17T05:00:00+00:00"},
				OperatorRunInstance: scheduler.OperatorRunInstance{TryNumber: 1},
			},
		}
	}

	downstreamDetails := &scheduler.JobWithDetails{
		Name: jobName,
		Job: &scheduler.Job{
			Name:         jobName,
			Tenant:       tnnt,
			WindowConfig: windowCfg,
		},
		Schedule: &scheduler.Schedule{Interval: "0 5 * * *"},
	}
	upstreamDetails := &scheduler.JobWithDetails{
		Name: upstreamJobName,
		Job: &scheduler.Job{
			Name:         upstreamJobName,
			Tenant:       upstreamTenant,
			WindowConfig: windowCfg,
		},
		Schedule: &scheduler.Schedule{Interval: "0 5 * * *"},
	}

	newService := func(repo *stubJobRunRepository, jobRepo *stubJobRepository) *JobRunService {
		return NewJobRunService(logger, jobRepo, repo, nil, nil, nil, nil, nil, nil, nil, stubProjectGetter{project: project}, config.FeaturesConfig{}, nil, nil)
	}

	observeDelta := func(t *testing.T, phase, gate string) uint64 {
		t.Helper()
		return observedDelayCount(t, phase, gate)
	}

	t.Run("records detection and handoff with an upstream gate", func(t *testing.T) {
		repo := &stubJobRunRepository{
			summaries: []*scheduler.JobRunSummary{{
				JobName:     jobName,
				ScheduledAt: scheduledAt,
				WaitEndTime: &sensorEnd,
			}},
		}
		repo.withUpstreamRuns([]*scheduler.JobRun{{
			JobName: upstreamJobName,
			State:   scheduler.StateSuccess,
			EndTime: &upstreamEnd,
		}})
		jobRepo := &stubJobRepository{
			upstreams: &scheduler.Upstreams{
				UpstreamJobs: []*scheduler.JobUpstream{{
					JobName: upstreamJobName.String(),
					Tenant:  upstreamTenant,
				}},
			},
			detailsByJob: map[string]*scheduler.JobWithDetails{
				jobName.String() + "@proj":         downstreamDetails,
				upstreamJobName.String() + "@proj": upstreamDetails,
			},
		}
		service := newService(repo, jobRepo)

		beforeD := observeDelta(t, SchedulerDelayPhaseDetection, string(scheduler.DelayGateUpstream))
		beforeH := observeDelta(t, SchedulerDelayPhaseHandoff, string(scheduler.DelayGateUpstream))
		beforeT := observeDelta(t, SchedulerDelayPhaseTotal, string(scheduler.DelayGateUpstream))

		service.observeTaskStartDelay(ctx, newEvent())

		assert.Equal(t, beforeD+1, observeDelta(t, SchedulerDelayPhaseDetection, string(scheduler.DelayGateUpstream)))
		assert.Equal(t, beforeH+1, observeDelta(t, SchedulerDelayPhaseHandoff, string(scheduler.DelayGateUpstream)))
		assert.Equal(t, beforeT+1, observeDelta(t, SchedulerDelayPhaseTotal, string(scheduler.DelayGateUpstream)))
		// no queued_dttm yet, so scheduling/pickup stay silent
		assert.Equal(t, uint64(0), observeDelta(t, SchedulerDelayPhaseScheduling, string(scheduler.DelayGateUpstream)))
		assert.Equal(t, uint64(0), observeDelta(t, SchedulerDelayPhasePickup, string(scheduler.DelayGateUpstream)))
	})

	t.Run("labels cross host upstreams as external_host", func(t *testing.T) {
		repo := &stubJobRunRepository{
			summaries: []*scheduler.JobRunSummary{{
				JobName:     jobName,
				ScheduledAt: scheduledAt,
				WaitEndTime: &sensorEnd,
			}},
		}
		jobRepo := &stubJobRepository{
			upstreams: &scheduler.Upstreams{
				UpstreamJobs: []*scheduler.JobUpstream{{
					JobName: upstreamJobName.String(),
					Tenant:  upstreamTenant,
					Host:    "https://other-optimus.example.com",
				}},
			},
		}
		service := newService(repo, jobRepo)

		before := observeDelta(t, SchedulerDelayPhaseHandoff, string(scheduler.DelayGateExternalHost))
		service.observeTaskStartDelay(ctx, newEvent())

		assert.Equal(t, before+1, observeDelta(t, SchedulerDelayPhaseHandoff, string(scheduler.DelayGateExternalHost)))
		assert.Equal(t, uint64(0), observeDelta(t, SchedulerDelayPhaseDetection, string(scheduler.DelayGateExternalHost)))
	})

	t.Run("labels third party gates as external", func(t *testing.T) {
		repo := &stubJobRunRepository{
			summaries: []*scheduler.JobRunSummary{{
				JobName:     jobName,
				ScheduledAt: scheduledAt,
				WaitEndTime: &sensorEnd,
			}},
		}
		jobRepo := &stubJobRepository{
			upstreams: &scheduler.Upstreams{
				ThirdParty: []*scheduler.ThirdPartyUpstream{{Type: "dex", Identifier: "dex_wait"}},
			},
		}
		service := newService(repo, jobRepo)

		before := observeDelta(t, SchedulerDelayPhaseHandoff, string(scheduler.DelayGateExternal))
		service.observeTaskStartDelay(ctx, newEvent())

		assert.Equal(t, before+1, observeDelta(t, SchedulerDelayPhaseHandoff, string(scheduler.DelayGateExternal)))
	})

	t.Run("no upstream at all is gate none", func(t *testing.T) {
		repo := &stubJobRunRepository{
			summaries: []*scheduler.JobRunSummary{{
				JobName:     jobName,
				ScheduledAt: scheduledAt,
				WaitEndTime: &sensorEnd,
			}},
		}
		jobRepo := &stubJobRepository{upstreams: &scheduler.Upstreams{}}
		service := newService(repo, jobRepo)

		before := observeDelta(t, SchedulerDelayPhaseDetection, string(scheduler.DelayGateNone))
		service.observeTaskStartDelay(ctx, newEvent())

		assert.Equal(t, before+1, observeDelta(t, SchedulerDelayPhaseDetection, string(scheduler.DelayGateNone)))
	})

	t.Run("upstream gate with no successful run is skipped", func(t *testing.T) {
		repo := &stubJobRunRepository{
			summaries: []*scheduler.JobRunSummary{{
				JobName:     jobName,
				ScheduledAt: scheduledAt,
				WaitEndTime: &sensorEnd,
			}},
		}
		repo.withUpstreamRuns(nil) // nothing finished in the window
		jobRepo := &stubJobRepository{
			upstreams: &scheduler.Upstreams{
				UpstreamJobs: []*scheduler.JobUpstream{{
					JobName: upstreamJobName.String(),
					Tenant:  upstreamTenant,
				}},
			},
			detailsByJob: map[string]*scheduler.JobWithDetails{
				jobName.String() + "@proj":         downstreamDetails,
				upstreamJobName.String() + "@proj": upstreamDetails,
			},
		}
		service := newService(repo, jobRepo)

		before := skippedDelayCount(t, "missing_gate_end")
		service.observeTaskStartDelay(ctx, newEvent())

		assert.Equal(t, before+1, skippedDelayCount(t, "missing_gate_end"))
	})

	t.Run("summary lookup failure is counted", func(t *testing.T) {
		repo := &stubJobRunRepository{err: context.DeadlineExceeded}
		jobRepo := &stubJobRepository{upstreams: &scheduler.Upstreams{}}
		service := newService(repo, jobRepo)

		before := skippedDelayCount(t, "summary_lookup_failed")
		service.observeTaskStartDelay(ctx, newEvent())

		assert.Equal(t, before+1, skippedDelayCount(t, "summary_lookup_failed"))
	})

	t.Run("run without sensor measures one handoff", func(t *testing.T) {
		repo := &stubJobRunRepository{
			summaries: []*scheduler.JobRunSummary{{
				JobName:     jobName,
				ScheduledAt: scheduledAt,
				// no WaitEndTime: no sensor rows
			}},
		}
		jobRepo := &stubJobRepository{upstreams: &scheduler.Upstreams{}}
		service := newService(repo, jobRepo)

		before := observeDelta(t, SchedulerDelayPhaseHandoff, string(scheduler.DelayGateNone))
		service.observeTaskStartDelay(ctx, newEvent())

		assert.Equal(t, before+1, observeDelta(t, SchedulerDelayPhaseHandoff, string(scheduler.DelayGateNone)))
	})
}
