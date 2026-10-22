package service

import (
	"context"
	"time"

	"github.com/goto/salt/log"

	"github.com/goto/optimus/core/scheduler"
	"github.com/goto/optimus/core/tenant"
)

// This file holds the shared target-resolution pipeline used by SLA evaluation flows:
// resolving which jobs to evaluate (by names and/or labels), which of their runs fall in the
// evaluation window, and which SLA each target carries. The Potential SLA Breach subscriber
// uses it today; the root-cause SLA breach publisher reuses the same pipeline so both entry
// points evaluate identical targets.

type JobDetailsGetter interface {
	GetJobs(ctx context.Context, projectName tenant.ProjectName, jobs []string) ([]*scheduler.JobWithDetails, error)
	GetJobsByLabels(ctx context.Context, projectName tenant.ProjectName, labels map[string]string) ([]*scheduler.JobWithDetails, error)
	// GetJobsByLabelsMultiValue is like GetJobsByLabels but each key may match any of several
	// values (OR'd within a key, AND'd across keys).
	GetJobsByLabelsMultiValue(ctx context.Context, projectName tenant.ProjectName, labels map[string][]string) ([]*scheduler.JobWithDetails, error)
}

// getJobWithDetails fetches jobs matching the given names and/or labels within one project,
// merging both sources. Label values are single-valued (AND across keys).
func getJobWithDetails(ctx context.Context, l log.Logger, jobDetailsGetter JobDetailsGetter, projectName tenant.ProjectName, jobNames []scheduler.JobName, labels map[string]string) ([]*scheduler.JobWithDetails, error) {
	filteredJobsByName := map[scheduler.JobName]*scheduler.JobWithDetails{}
	filteredJobByLabel := map[scheduler.JobName]*scheduler.JobWithDetails{}
	filteredJobMerged := map[scheduler.JobName]*scheduler.JobWithDetails{}

	if len(jobNames) > 0 {
		jobNameStr := []string{}
		for _, jn := range jobNames {
			jobNameStr = append(jobNameStr, string(jn))
		}
		jobsWithDetails, err := jobDetailsGetter.GetJobs(ctx, projectName, jobNameStr)
		if err != nil {
			if jobsWithDetails == nil {
				return nil, err
			}
			l.Error("[getJobWithDetails] encountered non-blocking error when fetching jobs by names: %s", err.Error())
		}
		for _, job := range jobsWithDetails {
			filteredJobsByName[job.Name] = job
			filteredJobMerged[job.Name] = job
		}
		l.Info("[getJobWithDetails] fetched jobs by names", "count", len(filteredJobsByName))
		l.Info("[getJobWithDetails] jobs fetched by names", "jobs", filteredJobsByName)
	}

	if len(labels) > 0 {
		jobsWithDetails, err := jobDetailsGetter.GetJobsByLabels(ctx, projectName, labels)
		if err != nil {
			if jobsWithDetails == nil {
				return nil, err
			}
			l.Error("[getJobWithDetails] encountered non-blocking error when fetching jobs by labels: %s", err.Error())
		}
		for _, job := range jobsWithDetails {
			filteredJobByLabel[job.Name] = job
			filteredJobMerged[job.Name] = job
		}
		l.Info("[getJobWithDetails] fetched jobs by labels", "count", len(filteredJobByLabel))
		l.Info("[getJobWithDetails] jobs fetched by labels", "jobs", filteredJobByLabel)
	}

	filteredJobSchedules := []*scheduler.JobWithDetails{}
	for _, job := range filteredJobMerged {
		filteredJobSchedules = append(filteredJobSchedules, job)
	}
	l.Info("[getJobWithDetails] total jobs fetched after merging by names and labels", "count", len(filteredJobSchedules))

	return filteredJobSchedules, nil
}

// getJobWithDetailsMultiValue is like getJobWithDetails but each label key may match any of
// several values (OR'd within a key, AND'd across keys), so a project-wide evaluation can
// select jobs with broader filters instead of a strict target list.
func getJobWithDetailsMultiValue(ctx context.Context, l log.Logger, jobDetailsGetter JobDetailsGetter, projectName tenant.ProjectName, jobNames []scheduler.JobName, labels map[string][]string) ([]*scheduler.JobWithDetails, error) {
	filteredJobMerged := map[scheduler.JobName]*scheduler.JobWithDetails{}

	if len(jobNames) > 0 {
		jobNameStr := []string{}
		for _, jn := range jobNames {
			jobNameStr = append(jobNameStr, string(jn))
		}
		jobsWithDetails, err := jobDetailsGetter.GetJobs(ctx, projectName, jobNameStr)
		if err != nil {
			if jobsWithDetails == nil {
				return nil, err
			}
			l.Error("[getJobWithDetailsMultiValue] encountered non-blocking error when fetching jobs by names: %s", err.Error())
		}
		for _, job := range jobsWithDetails {
			filteredJobMerged[job.Name] = job
		}
		l.Info("[getJobWithDetailsMultiValue] fetched jobs by names", "count", len(filteredJobMerged))
	}

	if len(labels) > 0 {
		jobsWithDetails, err := jobDetailsGetter.GetJobsByLabelsMultiValue(ctx, projectName, labels)
		if err != nil {
			if jobsWithDetails == nil {
				return nil, err
			}
			l.Error("[getJobWithDetailsMultiValue] encountered non-blocking error when fetching jobs by labels: %s", err.Error())
		}
		for _, job := range jobsWithDetails {
			filteredJobMerged[job.Name] = job
		}
		l.Info("[getJobWithDetailsMultiValue] fetched jobs by labels", "count", len(filteredJobMerged))
	}

	filteredJobs := make([]*scheduler.JobWithDetails, 0, len(filteredJobMerged))
	for _, job := range filteredJobMerged {
		filteredJobs = append(filteredJobs, job)
	}
	l.Info("[getJobWithDetailsMultiValue] total jobs fetched after merging by names and labels", "count", len(filteredJobs))

	return filteredJobs, nil
}

// getTargetedSLA resolves the SLA deadline for every job that has one in the given window.
// Jobs without a schedule, SLA duration or resolvable scheduled_at are skipped (and logged),
// as they cannot be evaluated against a deadline.
func getTargetedSLA(l log.Logger, jobs []*scheduler.JobWithDetails, jobSchedules map[scheduler.JobName]*scheduler.JobSchedule) map[scheduler.JobName]*time.Time {
	targetedSLAByJobName := make(map[scheduler.JobName]*time.Time)
	l.Info("getting targeted SLAs for jobs", "count", len(jobs))
	for _, job := range jobs {
		if job.Schedule == nil {
			l.Warn("job does not have schedule, skipping SLA prediction", "job", job.Name)
			continue
		}
		slaDuration, err := job.SLADuration()
		if err != nil {
			l.Warn("failed to get SLA duration for job", "job", job.Name, "error", err)
			continue
		}
		if slaDuration == 0 {
			l.Warn("SLA duration is not set for job, skipping SLA prediction", "job", job.Name)
			continue
		}
		schedule, ok := jobSchedules[job.Name]
		if !ok {
			l.Warn("failed to get scheduled at for job", "job", job.Name)
			continue
		}
		sla := schedule.ScheduledAt.Add(time.Duration(slaDuration) * time.Second)
		targetedSLAByJobName[job.Name] = &sla
	}
	l.Info("total targeted SLAs found", "count", len(targetedSLAByJobName))
	// jobs not having targeted SLA will be skipped
	jobsSkipped := []string{}
	for _, job := range jobs {
		if _, ok := targetedSLAByJobName[job.Name]; !ok {
			jobsSkipped = append(jobsSkipped, job.Name.String())
		}
	}
	if len(jobsSkipped) > 0 {
		l.Info("jobs skipped due to no targeted SLA found", "jobs", jobsSkipped)
	}

	return targetedSLAByJobName
}

// getJobSchedules resolves, per job, the next scheduled run within scheduleRangeInHours of
// the reference time (falling back to the previous one). Jobs with no run in range are
// skipped, as there is nothing to evaluate.
func getJobSchedules(l log.Logger, jobs []*scheduler.JobWithDetails, scheduleRangeInHours time.Duration, referenceTime time.Time) map[scheduler.JobName]*scheduler.JobSchedule {
	jobSchedules := make(map[scheduler.JobName]*scheduler.JobSchedule)
	l.Info("jobs to get schedules for", "count", len(jobs))
	for _, job := range jobs {
		if job.Schedule == nil {
			continue
		}
		nextScheduledAt, err := job.Schedule.GetNextSchedule(referenceTime)
		if err != nil {
			l.Warn("failed to get scheduled at for job, skipping SLA prediction", "job", job.Name, "error", err)
			continue
		}

		prevScheduledAt, err := job.Schedule.GetPreviousSchedule(referenceTime)
		if err != nil {
			l.Warn("failed to get previous scheduled at for job, skipping SLA prediction", "job", job.Name, "error", err)
			continue
		}

		var scheduledAt time.Time
		if nextScheduledAt.Sub(referenceTime).Milliseconds() < scheduleRangeInHours.Milliseconds() {
			l.Debug("using next scheduled at for job within schedule range", "job", job.Name)
			scheduledAt = nextScheduledAt
		} else if referenceTime.Sub(prevScheduledAt).Milliseconds() < scheduleRangeInHours.Milliseconds() {
			l.Debug("using previous scheduled at for job within schedule range", "job", job.Name)
			scheduledAt = prevScheduledAt
		}

		if scheduledAt.IsZero() {
			l.Warn("no scheduled at found for job in the next schedule range, skipping SLA prediction", "job", job.Name)
			continue
		}

		jobSchedules[job.Name] = &scheduler.JobSchedule{
			JobName:     job.Name,
			ScheduledAt: scheduledAt,
		}
	}
	l.Info("total job schedules found", "count", len(jobSchedules))
	// jobs not having schedule within the range will be skipped
	jobsSkipped := []string{}
	for _, job := range jobs {
		if _, ok := jobSchedules[job.Name]; !ok {
			jobsSkipped = append(jobsSkipped, job.Name.String())
		}
	}
	if len(jobsSkipped) > 0 {
		l.Info("jobs skipped due to no schedule within the range", "jobs", jobsSkipped)
	}
	return jobSchedules
}

// collectJobNames gathers every job reachable from the given lineage summaries (the targets
// plus all their upstreams), deduplicated. Used to batch duration/start-offset lookups for
// the whole evaluated graph.
func collectJobNames(jobsWithLineage map[scheduler.JobName]*scheduler.JobLineageSummary) []scheduler.JobName {
	jobNamesMap := map[scheduler.JobName]bool{}
	stack := []*scheduler.JobLineageSummary{}
	for _, job := range jobsWithLineage {
		stack = append(stack, job)
	}
	// BFS to traverse all jobs
	for len(stack) > 0 {
		job := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if _, ok := jobNamesMap[job.JobName]; ok {
			continue
		}
		jobNamesMap[job.JobName] = true
		stack = append(stack, job.Upstreams...)
	}
	jobNames := make([]scheduler.JobName, 0, len(jobNamesMap))
	for jobName := range jobNamesMap {
		jobNames = append(jobNames, jobName)
	}
	return jobNames
}
