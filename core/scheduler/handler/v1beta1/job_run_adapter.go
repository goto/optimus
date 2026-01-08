package v1beta1

import (
	"fmt"

	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/goto/optimus/core/scheduler"
	pb "github.com/goto/optimus/protos/gotocompany/optimus/core/v1beta1"
)

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

			pbJobRuns = append(pbJobRuns, &pb.JobExecutionSummary{
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
				},
				Level:              int32(run.Level),
				DownstreamPathName: run.DownstreamPathName,
				DelaySummary: &pb.JobRunDelaySummary{
					ScheduledWayTooLateSeconds:   int32(run.DelaySummary.ScheduledWayTooLateSeconds),
					SystemSchedulingDelaySeconds: int32(run.DelaySummary.SystemSchedulingDelaySeconds),
				},
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
			},
		})
	}

	return &pb.GetJobRunLineageSummaryResponse{
		Jobs: pbJobRunLineages,
	}
}
