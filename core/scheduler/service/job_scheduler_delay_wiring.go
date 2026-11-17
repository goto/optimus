package service

import (
	"context"
	"strings"
	"time"

	"github.com/goto/optimus/core/scheduler"
	"github.com/goto/optimus/internal/errors"
)

// This file wires the scheduler delay phases (other than dag_close, see
// recordDagCloseDelay) into the task start event path.
//
// The work runs in the background: the Airflow worker blocks on this callback,
// so measuring must never delay it. A bounded set of slots keeps a burst of
// task starts from stampeding the database, and every failure path only counts
// a skip -- a measurement problem must never fail the event.

const (
	// schedulerDelaySampleTimeout bounds one background sample computation.
	schedulerDelaySampleTimeout = 5 * time.Second
	// schedulerDelayMaxConcurrent bounds how many samples compute at once.
	// At a few queries per sample this keeps the extra database load marginal
	// even during a burst of task starts.
	schedulerDelayMaxConcurrent = 8
	// schedulerDelayMaxUpstreams caps the per-sample upstream lookups. Jobs
	// with more same-host upstreams than this are dropped and counted rather
	// than hammering the database; the average job has a handful of upstreams.
	schedulerDelayMaxUpstreams = 25
)

// schedulerDelaySlots is the concurrency gate for background samples.
var schedulerDelaySlots = make(chan struct{}, schedulerDelayMaxConcurrent)

// recordTaskStartDelay kicks off the phase measurement for one task start.
// The cheap checks (event type, regular run, first attempt) run synchronously
// so their skip counts are exact and no goroutine is wasted; everything that
// touches the database runs in the background.
func (s *JobRunService) recordTaskStartDelay(event *scheduler.Event) {
	if event.Type != scheduler.TaskStartEvent {
		return
	}

	runID := event.DagRunID()
	switch {
	case runID == "":
		skipSchedulerDelay("missing_run_id")
		return
	case !strings.HasPrefix(runID, scheduler.RunIDScheduledPrefix):
		skipSchedulerDelay("non_regular_run")
		return
	case event.EventContext != nil && event.EventContext.OperatorRunInstance.TryNumber > 1:
		// Retries start outside the normal schedule wait, so their delay says
		// nothing about scheduling. TryNumber comes from the callback payload's
		// "attempt" field.
		skipSchedulerDelay("retry_attempt")
		return
	}

	select {
	case schedulerDelaySlots <- struct{}{}:
	default:
		// All slots busy: dropping one sample is better than queueing a burst
		// that would only be observed minutes late.
		skipSchedulerDelay("overloaded")
		return
	}

	go func() { //nolint: contextcheck // a fresh context is deliberate: the event context dies with the handler
		defer func() {
			<-schedulerDelaySlots
			// A background sampler must never take the process down; log and
			// move on if anything unexpected panics.
			if r := recover(); r != nil {
				s.l.Error("panic in scheduler delay sampling, job [%s]: %v", event.JobName, r)
			}
		}()
		ctx, cancel := context.WithTimeout(context.Background(), schedulerDelaySampleTimeout)
		defer cancel()
		s.observeTaskStartDelay(ctx, event)
	}()
}

// observeTaskStartDelay gathers the timestamps of one sample from the
// database, derives the phases and records them. It is always called in the
// background with a bounded context.
func (s *JobRunService) observeTaskStartDelay(ctx context.Context, event *scheduler.Event) {
	summaries, err := s.repo.GetRunSummaryByIdentifiers(ctx, []scheduler.JobRunIdentifier{{
		JobName:     event.JobName,
		ScheduledAt: event.JobScheduledAt,
	}})
	if err != nil {
		s.l.Error("error getting run summary for scheduler delay, job [%s]: %s", event.JobName, err)
		skipSchedulerDelay("summary_lookup_failed")
		return
	}
	if len(summaries) == 0 {
		skipSchedulerDelay("no_run_summary")
		return
	}

	input := scheduler.DelayInput{
		ScheduledAt:   event.JobScheduledAt,
		TaskStart:     event.EventTime,
		LastSensorEnd: summaries[0].WaitEndTime, // nil when the run has no sensor rows
	}

	gate, gateEnd, skipReason := s.classifyGate(ctx, event)
	if skipReason != "" {
		skipSchedulerDelay(skipReason)
		return
	}
	input.Gate = gate
	input.GateEnd = gateEnd

	phases, skip := scheduler.ComputeSchedulerDelayPhases(input)
	if skip != scheduler.DelaySkipOK {
		skipSchedulerDelay(string(skip))
		return
	}

	project := event.Tenant.ProjectName().String()
	namespace := event.Tenant.NamespaceName().String()
	gateLabel := string(phases.Gate)
	observe := func(phase string, d *time.Duration) {
		if d != nil {
			observeSchedulerDelay(project, namespace, phase, gateLabel, *d)
		}
	}
	observe(SchedulerDelayPhaseDetection, phases.Detection)
	observe(SchedulerDelayPhaseScheduling, phases.Scheduling)
	observe(SchedulerDelayPhasePickup, phases.Pickup)
	observe(SchedulerDelayPhaseHandoff, phases.Handoff)
	observeSchedulerDelay(project, namespace, SchedulerDelayPhaseTotal, gateLabel, phases.Total)
}

// classifyGate decides what the run was waiting on and, for Optimus upstreams
// on this host, when the gate opened. Priority when several gate kinds exist:
// a same-host upstream is preferred because it is the only one whose end time
// we can measure; otherwise the sample is labelled by what is left.
func (s *JobRunService) classifyGate(ctx context.Context, event *scheduler.Event) (scheduler.DelayGate, *time.Time, string) {
	upstreams, err := s.jobRepo.GetUpstreamsOfJob(ctx, event.Tenant.ProjectName(), event.JobName)
	if err != nil {
		s.l.Error("error getting upstreams for scheduler delay, job [%s]: %s", event.JobName, err)
		return "", nil, "upstream_lookup_failed"
	}

	var sameHost []*scheduler.JobUpstream
	crossHost := false
	for _, up := range upstreams.UpstreamJobs {
		// External marks another Optimus host; a set Host says the same thing.
		// Both mean we cannot read the upstream's run end from this database.
		if up.External || up.Host != "" {
			crossHost = true
			continue
		}
		sameHost = append(sameHost, up)
	}

	switch {
	case len(sameHost) > schedulerDelayMaxUpstreams:
		return "", nil, "too_many_upstreams"
	case len(sameHost) > 0:
		gateEnd, err := s.latestUpstreamRunEnd(ctx, event, sameHost)
		if err != nil {
			s.l.Error("error resolving upstream run end for scheduler delay, job [%s]: %s", event.JobName, err)
			return "", nil, "upstream_run_lookup_failed"
		}
		// gateEnd may still be nil when no upstream run in the window has
		// finished; ComputeSchedulerDelayPhases reports that as a skip.
		return scheduler.DelayGateUpstream, gateEnd, ""
	case crossHost:
		return scheduler.DelayGateExternalHost, nil, ""
	case len(upstreams.ThirdParty) > 0:
		return scheduler.DelayGateExternal, nil, ""
	default:
		return scheduler.DelayGateNone, nil, ""
	}
}

// latestUpstreamRunEnd returns the DAG-level end time of the latest successful
// upstream run inside the window the downstream sensor waits on -- the same
// window logic the sensor itself applies, computed server-side. It returns a
// nil time (with no error) when no upstream run in the window has finished.
func (s *JobRunService) latestUpstreamRunEnd(ctx context.Context, event *scheduler.Event, upstreams []*scheduler.JobUpstream) (*time.Time, error) {
	sourceProject, err := s.projectGetter.Get(ctx, event.Tenant.ProjectName())
	if err != nil {
		return nil, err
	}
	jobDetails, err := s.jobRepo.GetJobDetails(ctx, event.Tenant.ProjectName(), event.JobName)
	if err != nil {
		return nil, err
	}
	if jobDetails.Schedule == nil || jobDetails.Job == nil {
		return nil, errors.InvalidArgument(scheduler.EntityJobRun, "job schedule is missing for scheduler delay, job "+event.JobName.String())
	}

	var latest *time.Time
	for _, up := range upstreams {
		upDetails, err := s.jobRepo.GetJobDetails(ctx, up.Tenant.ProjectName(), scheduler.JobName(up.JobName))
		if err != nil {
			s.l.Warn("error getting upstream details for scheduler delay, upstream [%s]: %s", up.JobName, err)
			continue
		}
		if upDetails.Schedule == nil {
			continue
		}

		// All upstream schedule times the downstream's window covers -- the
		// sensor waits for every one of them to succeed.
		times, err := s.GetExpectedRunSchedules(ctx, sourceProject,
			jobDetails.Schedule.Interval, jobDetails.Job.WindowConfig,
			upDetails.Schedule.Interval, event.JobScheduledAt)
		if err != nil {
			s.l.Warn("error computing expected upstream schedules for scheduler delay, upstream [%s]: %s", up.JobName, err)
			continue
		}
		runs, err := s.repo.GetByScheduledTimes(ctx, up.Tenant, scheduler.JobName(up.JobName), times)
		if err != nil {
			s.l.Warn("error getting upstream runs for scheduler delay, upstream [%s]: %s", up.JobName, err)
			continue
		}

		// The gate opens when the LAST of the awaited runs finishes, so take
		// the maximum DAG-level end time across successful runs.
		for _, run := range runs {
			if run.State != scheduler.StateSuccess || run.EndTime == nil {
				continue
			}
			if latest == nil || run.EndTime.After(*latest) {
				latest = run.EndTime
			}
		}
	}
	return latest, nil
}
