package v1beta1

import (
	"fmt"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/goto/optimus/core/scheduler"
	"github.com/goto/optimus/core/scheduler/service"
	"github.com/goto/optimus/core/tenant"
	"github.com/goto/optimus/internal/errors"
	pb "github.com/goto/optimus/protos/gotocompany/optimus/core/v1beta1"
)

// buildIdentifySLABreachInputs resolves an IdentifyPotentialSLABreachRequest into
// the combos (cross product of projects x label groups) to evaluate and the
// shared predictor config. job_names only applies in legacy single-project mode,
// where target selection isn't ambiguous.
func buildIdentifySLABreachInputs(req *pb.IdentifyPotentialSLABreachRequest) ([]scheduler.SLABreachCombo, service.JobSLAPredictorRequestConfig, error) {
	// resolve projects: prefer repeated project_names, fall back to path project_name
	projectNameStrs := req.GetProjectNames()
	if len(projectNameStrs) == 0 && req.GetProjectName() != "" {
		projectNameStrs = []string{req.GetProjectName()}
	}
	if len(projectNameStrs) == 0 {
		return nil, service.JobSLAPredictorRequestConfig{}, errors.InvalidArgument(scheduler.EntityJobRun, "no project provided")
	}
	projectNames := make([]tenant.ProjectName, 0, len(projectNameStrs))
	for _, pn := range projectNameStrs {
		projectName, err := tenant.ProjectNameFrom(pn)
		if err != nil {
			return nil, service.JobSLAPredictorRequestConfig{}, fmt.Errorf("failure in adapting project name [%s]: %w", pn, err)
		}
		projectNames = append(projectNames, projectName)
	}

	jobNames := []scheduler.JobName{}
	for _, jn := range req.GetJobNames() {
		jobName, err := scheduler.JobNameFrom(jn)
		if err != nil {
			return nil, service.JobSLAPredictorRequestConfig{}, fmt.Errorf("failure in adapting job name [%s]: %w", jn, err)
		}
		jobNames = append(jobNames, jobName)
	}

	// resolve label groups: prefer repeated label_groups, fall back to job_labels.
	// severity is a single request-level value (req.GetSeverity()), not per-group.
	type labelGroup struct {
		labels map[string]string
		name   string
	}
	labelGroups := make([]labelGroup, 0, len(req.GetLabelGroups()))
	for _, g := range req.GetLabelGroups() {
		labelGroups = append(labelGroups, labelGroup{labels: g.GetJobLabels(), name: g.GetName()})
	}
	legacyMode := len(labelGroups) == 0
	if legacyMode {
		labelGroups = append(labelGroups, labelGroup{labels: req.GetJobLabels()})
	}

	// consider jobs with next schedule within next and before scheduleRangeInHours hours
	scheduleRangeInHours := time.Duration(req.GetScheduledRangeInHours()) * time.Hour
	referenceTime := time.Now().UTC()
	if req.GetReferenceTime() != nil && req.GetReferenceTime().IsValid() {
		referenceTime = req.GetReferenceTime().AsTime().UTC()
	}
	reqConfig := service.JobSLAPredictorRequestConfig{
		ReferenceTime:        referenceTime,
		ScheduleRangeInHours: scheduleRangeInHours,
		SkipJobNames:         req.GetSkipJobNames(),
		EnableAlert:          req.GetAlertOnBreach(),
		EnableDeduplication:  req.GetEnableDeduplication(),
		Severity:             req.GetSeverity(),
		DamperCoeff:          float64(req.GetDamperCoeff()),
	}

	// build combos = projects x label groups.
	combos := make([]scheduler.SLABreachCombo, 0, len(projectNames)*len(labelGroups))
	for _, projectName := range projectNames {
		for _, g := range labelGroups {
			combo := scheduler.SLABreachCombo{
				ProjectName: projectName,
				Labels:      g.labels,
				GroupName:   g.name,
			}
			if legacyMode && len(projectNames) == 1 {
				combo.JobNames = jobNames
			}
			combos = append(combos, combo)
		}
	}

	return combos, reqConfig, nil
}

func fromJobRunLineageSummaryRequest(req *pb.GetJobRunLineageSummaryRequest) ([]*scheduler.JobSchedule, error) {
	targetJobSchedules := make([]*scheduler.JobSchedule, 0, len(req.GetTargetJobs()))

	for _, jobReq := range req.GetTargetJobs() {
		if _, err := scheduler.JobNameFrom(jobReq.GetJobName()); err != nil {
			return nil, fmt.Errorf("failure in parsing job name [%s]: %w", jobReq.GetJobName(), err)
		}

		if err := jobReq.GetScheduledAt().CheckValid(); err != nil {
			return nil, fmt.Errorf("failure in parsing scheduled at for job [%s]: %w", jobReq.GetJobName(), err)
		}

		targetJobSchedules = append(targetJobSchedules, &scheduler.JobSchedule{
			JobName:     scheduler.JobName(jobReq.GetJobName()),
			ScheduledAt: jobReq.GetScheduledAt().AsTime(),
		})
	}

	return targetJobSchedules, nil
}

func toJobRunLineageSummaryResponse(jobRunLineages []*scheduler.JobRunLineage) *pb.GetJobRunLineageSummaryResponse {
	var pbJobRunLineages []*pb.JobRunLineageSummary
	for _, lineage := range jobRunLineages {
		var pbJobRuns []*pb.JobExecutionSummary
		for _, run := range lineage.JobRuns {
			var jobStartTime *timestamppb.Timestamp
			if run.JobRunSummary.JobStartTime != nil {
				jobStartTime = timestamppb.New(*run.JobRunSummary.JobStartTime)
			}

			var jobEndTime *timestamppb.Timestamp
			if run.JobRunSummary.JobEndTime != nil {
				jobEndTime = timestamppb.New(*run.JobRunSummary.JobEndTime)
			}

			var hookStartTime *timestamppb.Timestamp
			if run.JobRunSummary.HookStartTime != nil {
				hookStartTime = timestamppb.New(*run.JobRunSummary.HookStartTime)
			}

			var hookEndTime *timestamppb.Timestamp
			if run.JobRunSummary.HookEndTime != nil {
				hookEndTime = timestamppb.New(*run.JobRunSummary.HookEndTime)
			}

			var waitStartTime *timestamppb.Timestamp
			if run.JobRunSummary.WaitStartTime != nil {
				waitStartTime = timestamppb.New(*run.JobRunSummary.WaitStartTime)
			}

			var waitEndTime *timestamppb.Timestamp
			if run.JobRunSummary.WaitEndTime != nil {
				waitEndTime = timestamppb.New(*run.JobRunSummary.WaitEndTime)
			}

			var taskStartTime *timestamppb.Timestamp
			if run.JobRunSummary.TaskStartTime != nil {
				taskStartTime = timestamppb.New(*run.JobRunSummary.TaskStartTime)
			}

			var taskEndTime *timestamppb.Timestamp
			if run.JobRunSummary.TaskEndTime != nil {
				taskEndTime = timestamppb.New(*run.JobRunSummary.TaskEndTime)
			}

			var slaTime *timestamppb.Timestamp
			if run.JobRunSummary.SLATime != nil {
				slaTime = timestamppb.New(*run.JobRunSummary.SLATime)
			}

			var sensorName string
			if run.JobRunSummary.SensorName != nil {
				sensorName = *run.JobRunSummary.SensorName
			}

			var taskName string
			if run.JobRunSummary.TaskName != nil {
				taskName = *run.JobRunSummary.TaskName
			}

			var hookName string
			if run.JobRunSummary.HookName != nil {
				hookName = *run.JobRunSummary.HookName
			}

			pbJobRun := &pb.JobExecutionSummary{
				JobName: run.JobName.String(),
				Sla: &pb.SLAConfig{
					Duration: durationpb.New(run.SLA.Duration),
				},
				JobRunSummary: &pb.JobRunSummary{
					ScheduledAt:   timestamppb.New(run.JobRunSummary.ScheduledAt),
					SlaTime:       slaTime,
					JobStartTime:  jobStartTime,
					JobEndTime:    jobEndTime,
					HookStartTime: hookStartTime,
					HookEndTime:   hookEndTime,
					WaitStartTime: waitStartTime,
					WaitEndTime:   waitEndTime,
					TaskStartTime: taskStartTime,
					TaskEndTime:   taskEndTime,
					SensorName:    sensorName,
					TaskName:      taskName,
					HookName:      hookName,
				},
				Level:              int32(run.Level),
				DownstreamPathName: run.DownstreamPathName,
			}
			if run.DelaySummary != nil {
				pbJobRun.DelaySummary = &pb.JobRunDelaySummary{
					ScheduledWayTooLateSeconds:   int32(run.DelaySummary.ScheduledWayTooLateSeconds),
					SystemSchedulingDelaySeconds: int32(run.DelaySummary.SystemSchedulingDelaySeconds),
				}
			}
			pbJobRun.HistoricalDurationTrend = &pb.HistoricalDurationTrend{
				TaskDurationSeconds: int32(run.HistoricalSummary.TaskDuration.Seconds()),
				HookDurationSeconds: int32(run.HistoricalSummary.HookDuration.Seconds()),
			}

			pbJobRuns = append(pbJobRuns, pbJobRun)
		}

		topLongestTaskDurationJobs := make([]*pb.JobWithTaskDuration, 0, len(lineage.ExecutionSummary.TopLongestTaskDurationJobs))
		for _, jobWithDuration := range lineage.ExecutionSummary.TopLongestTaskDurationJobs {
			topLongestTaskDurationJobs = append(topLongestTaskDurationJobs, &pb.JobWithTaskDuration{
				JobName:         jobWithDuration.JobName.String(),
				DurationSeconds: int32(jobWithDuration.TaskDuration.Seconds()),
				Level:           int32(jobWithDuration.Level),
			})
		}

		topLongestHookDurationJobs := make([]*pb.JobWithTaskDuration, 0, len(lineage.ExecutionSummary.TopLongestHookDurationJobs))
		for _, jobWithDuration := range lineage.ExecutionSummary.TopLongestHookDurationJobs {
			topLongestHookDurationJobs = append(topLongestHookDurationJobs, &pb.JobWithTaskDuration{
				JobName:         jobWithDuration.JobName.String(),
				DurationSeconds: int32(jobWithDuration.TaskDuration.Seconds()),
				Level:           int32(jobWithDuration.Level),
			})
		}

		pbJobRunLineages = append(pbJobRunLineages, &pb.JobRunLineageSummary{
			JobName:     lineage.JobName.String(),
			ScheduledAt: timestamppb.New(lineage.JobRuns[0].JobRunSummary.ScheduledAt),
			JobRuns:     pbJobRuns,
			ExecutionSummary: &pb.LineageExecutionSummary{
				TotalScheduledWayTooLateSeconds:     int32(lineage.ExecutionSummary.TotalScheduledWayTooLateSeconds),
				TotalSystemSchedulingDelaySeconds:   int32(lineage.ExecutionSummary.TotalSystemSchedulingDelaySeconds),
				AverageSystemSchedulingDelaySeconds: int32(lineage.ExecutionSummary.AverageSystemSchedulingDelaySeconds),
				TotalLineageDelaySeconds:            int32(lineage.ExecutionSummary.TotalLineageDelaySeconds),
				TotalLineageDurationSeconds:         int32(lineage.ExecutionSummary.TotalLineageDurationSeconds),
				LargestScheduledWayTooLateJob: &pb.LineageDelaySummary{
					JobName:             lineage.ExecutionSummary.LargestScheduledWayTooLateJob.JobName.String(),
					DelayDuration:       int32(lineage.ExecutionSummary.LargestScheduledWayTooLateJob.DelayDuration),
					ScheduledAt:         timestamppb.New(lineage.ExecutionSummary.LargestScheduledWayTooLateJob.ScheduledAt),
					UpstreamJobName:     lineage.ExecutionSummary.LargestScheduledWayTooLateJob.UpstreamJobName.String(),
					UpstreamScheduledAt: timestamppb.New(lineage.ExecutionSummary.LargestScheduledWayTooLateJob.UpstreamScheduledAt),
				},
				LargestSystemSchedulingDelayJob: &pb.LineageDelaySummary{
					JobName:             lineage.ExecutionSummary.LargestSystemSchedulingDelayJob.JobName.String(),
					DelayDuration:       int32(lineage.ExecutionSummary.LargestSystemSchedulingDelayJob.DelayDuration),
					ScheduledAt:         timestamppb.New(lineage.ExecutionSummary.LargestSystemSchedulingDelayJob.ScheduledAt),
					UpstreamJobName:     lineage.ExecutionSummary.LargestSystemSchedulingDelayJob.UpstreamJobName.String(),
					UpstreamScheduledAt: timestamppb.New(lineage.ExecutionSummary.LargestSystemSchedulingDelayJob.UpstreamScheduledAt),
				},
				TopLongestTaskDurationJobs: topLongestTaskDurationJobs,
				TopLongestHookDurationJobs: topLongestHookDurationJobs,
			},
		})
	}

	return &pb.GetJobRunLineageSummaryResponse{
		Jobs: pbJobRunLineages,
	}
}
