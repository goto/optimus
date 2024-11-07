package v1beta1

import (
	"context"
	"time"

	"github.com/goto/salt/log"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/goto/optimus/core/scheduler"
	"github.com/goto/optimus/core/tenant"
	"github.com/goto/optimus/internal/errors"
	"github.com/goto/optimus/internal/lib/interval"
	"github.com/goto/optimus/internal/utils/filter"
	pb "github.com/goto/optimus/protos/gotocompany/optimus/core/v1beta1"
)

type JobRunService interface {
	JobRunInput(context.Context, tenant.ProjectName, scheduler.JobName, scheduler.RunConfig) (*scheduler.ExecutorInput, error)
	UpdateJobState(context.Context, *scheduler.Event) error
	GetJobRunsByFilter(ctx context.Context, projectName tenant.ProjectName, jobName scheduler.JobName, filters ...filter.FilterOpt) ([]*scheduler.JobRun, error)
	GetJobRuns(ctx context.Context, projectName tenant.ProjectName, jobName scheduler.JobName, criteria *scheduler.JobRunsCriteria) ([]*scheduler.JobRunStatus, error)
	UploadToScheduler(ctx context.Context, projectName tenant.ProjectName) error
	GetInterval(ctx context.Context, projectName tenant.ProjectName, jobName scheduler.JobName, referenceTime time.Time) (interval.Interval, error)
}

type Notifier interface {
	Push(ctx context.Context, event *scheduler.Event) error
	Webhook(ctx context.Context, event *scheduler.Event) error
	Relay(ctx context.Context, event *scheduler.Event) error
}

type JobRunHandler struct {
	l        log.Logger
	service  JobRunService
	notifier Notifier

	pb.UnimplementedJobRunServiceServer
}

func (h JobRunHandler) JobRunInput(ctx context.Context, req *pb.JobRunInputRequest) (*pb.JobRunInputResponse, error) {
	projectName, err := tenant.ProjectNameFrom(req.GetProjectName())
	if err != nil {
		h.l.Error("error adapting project name [%s]: %s", req.GetProjectName(), err)
		return nil, errors.GRPCErr(err, "unable to get job run input for "+req.GetJobName())
	}

	jobName, err := scheduler.JobNameFrom(req.GetJobName())
	if err != nil {
		h.l.Error("error adapting job name [%s]: %s", req.GetJobName(), err)
		return nil, errors.GRPCErr(err, "unable to get job run input for "+req.GetJobName())
	}

	executor, err := scheduler.ExecutorFromEnum(req.InstanceName, req.InstanceType.String())
	if err != nil {
		h.l.Error("error adapting executor: %s", err)
		return nil, errors.GRPCErr(err, "unable to get job run input for "+req.GetJobName())
	}

	err = req.ScheduledAt.CheckValid()
	if err != nil {
		h.l.Error("invalid scheduled at: %s", err)
		return nil, errors.GRPCErr(errors.InvalidArgument(scheduler.EntityJobRun, "invalid scheduled_at"), "unable to get job run input for "+req.GetJobName())
	}

	runConfig, err := scheduler.RunConfigFrom(executor, req.ScheduledAt.AsTime(), req.JobrunId)
	if err != nil {
		h.l.Error("error adapting run config: %s", err)
		return nil, errors.GRPCErr(err, "unable to get job run input for "+req.GetJobName())
	}

	input, err := h.service.JobRunInput(ctx, projectName, jobName, runConfig)
	if err != nil {
		h.l.Error("error getting job run input: %s", err)
		return nil, errors.GRPCErr(err, "unable to get job run input for "+req.GetJobName())
	}

	return &pb.JobRunInputResponse{
		Envs:    input.Configs,
		Files:   input.Files,
		Secrets: input.Secrets,
	}, nil
}

// GetJobRuns gets job runs from optimus DB based on the criteria
func (h JobRunHandler) GetJobRuns(ctx context.Context, req *pb.GetJobRunsRequest) (*pb.GetJobRunsResponse, error) {
	projectName, err := tenant.ProjectNameFrom(req.GetProjectName())
	if err != nil {
		h.l.Error("error adapting project name [%s]: %s", req.GetProjectName(), err)
		return nil, errors.GRPCErr(err, "unable to get job run for "+req.GetJobName())
	}

	jobName, err := scheduler.JobNameFrom(req.GetJobName())
	if err != nil {
		h.l.Error("error adapting job name [%s]: %s", req.GetJobName(), err)
		return nil, errors.GRPCErr(err, "unable to get job run for "+req.GetJobName())
	}

	if len(req.GetState()) > 0 {
		_, err := scheduler.StateFromString(req.GetState())
		if err != nil {
			h.l.Error("error adapting job run state [%s]: %s", req.GetState(), err)
			return nil, errors.GRPCErr(err, "invalid job run state: "+req.GetState())
		}
	}

	var jobRuns []*scheduler.JobRun
	jobRuns, err = h.service.GetJobRunsByFilter(ctx, projectName, jobName,
		filter.WithString(filter.RunState, req.GetState()),
		filter.WithTime(filter.StartDate, req.GetSince().AsTime()),
		filter.WithTime(filter.EndDate, req.GetUntil().AsTime()),
	)
	if err != nil {
		h.l.Error("error getting job runs: %s", err)
		return nil, errors.GRPCErr(err, "unable to get job run for "+req.GetJobName())
	}

	var runs []*pb.JobRunWithDetail
	for _, run := range jobRuns {
		jobRunWithDetail := pb.JobRunWithDetail{
			State:       run.State.String(),
			ScheduledAt: timestamppb.New(run.ScheduledAt),
			StartTime:   timestamppb.New(run.StartTime),
		}
		if run.EndTime != nil {
			jobRunWithDetail.EndTime = timestamppb.New(*run.EndTime)
		}
		runs = append(runs, &jobRunWithDetail)
	}
	return &pb.GetJobRunsResponse{JobRuns: runs}, nil
}

// JobRun gets the job runs from scheduler based on the criteria
func (h JobRunHandler) JobRun(ctx context.Context, req *pb.JobRunRequest) (*pb.JobRunResponse, error) {
	projectName, err := tenant.ProjectNameFrom(req.GetProjectName())
	if err != nil {
		h.l.Error("error adapting project name [%s]: %s", req.GetProjectName(), err)
		return nil, errors.GRPCErr(err, "unable to get job run for "+req.GetJobName())
	}

	jobName, err := scheduler.JobNameFrom(req.GetJobName())
	if err != nil {
		h.l.Error("error adapting job name [%s]: %s", req.GetJobName(), err)
		return nil, errors.GRPCErr(err, "unable to get job run for "+req.GetJobName())
	}

	criteria, err := buildCriteriaForJobRun(req)
	if err != nil {
		h.l.Error("error building job run criteria: %s", err)
		return nil, errors.GRPCErr(err, "unable to get job run for "+req.GetJobName())
	}

	var jobRuns []*scheduler.JobRunStatus
	jobRuns, err = h.service.GetJobRuns(ctx, projectName, jobName, criteria)
	if err != nil {
		h.l.Error("error getting job runs: %s", err)
		return nil, errors.GRPCErr(err, "unable to get job run for "+req.GetJobName())
	}

	var runs []*pb.JobRun
	for _, run := range jobRuns {
		ts := timestamppb.New(run.ScheduledAt)
		runs = append(runs, &pb.JobRun{
			State:       run.State.String(),
			ScheduledAt: ts,
		})
	}
	return &pb.JobRunResponse{JobRuns: runs}, nil
}

func buildCriteriaForJobRun(req *pb.JobRunRequest) (*scheduler.JobRunsCriteria, error) {
	if !req.GetStartDate().IsValid() && !req.GetEndDate().IsValid() {
		return &scheduler.JobRunsCriteria{
			Name:        req.GetJobName(),
			OnlyLastRun: true,
		}, nil
	}
	if !req.GetStartDate().IsValid() {
		return nil, errors.InvalidArgument(scheduler.EntityJobRun, "empty start date is given")
	}
	if !req.GetEndDate().IsValid() {
		return nil, errors.InvalidArgument(scheduler.EntityJobRun, "empty end date is given")
	}
	return &scheduler.JobRunsCriteria{
		Name:      req.GetJobName(),
		StartDate: req.GetStartDate().AsTime(),
		EndDate:   req.GetEndDate().AsTime(),
		Filter:    req.GetFilter(),
	}, nil
}

func (h JobRunHandler) UploadToScheduler(_ context.Context, req *pb.UploadToSchedulerRequest) (*pb.UploadToSchedulerResponse, error) {
	projectName, err := tenant.ProjectNameFrom(req.GetProjectName())
	if err != nil {
		h.l.Error("error adapting project name [%s]: %s", req.GetProjectName(), err)
		return nil, errors.GRPCErr(err, "unable to get projectName")
	}
	go func() {
		err = h.service.UploadToScheduler(context.Background(), projectName)
		if err != nil {
			h.l.Error("Finished upload to scheduler with error: %s", err)
		}
	}()
	return &pb.UploadToSchedulerResponse{}, nil
}

// RegisterJobEvent TODO: check in jaeger if this api takes time, then we can make this async
func (h JobRunHandler) RegisterJobEvent(ctx context.Context, req *pb.RegisterJobEventRequest) (*pb.RegisterJobEventResponse, error) {
	tnnt, err := tenant.NewTenant(req.GetProjectName(), req.GetNamespaceName())
	if err != nil {
		h.l.Error("invalid tenant information request project [%s] namespace [%s]: %s", req.GetProjectName(), req.GetNamespaceName(), err)
		return nil, errors.GRPCErr(err, "unable to get tenant")
	}

	jobName, err := scheduler.JobNameFrom(req.GetJobName())
	if err != nil {
		h.l.Error("error adapting job name [%s]: %s", jobName, err)
		return nil, errors.GRPCErr(err, "unable to get job name"+req.GetJobName())
	}

	event, err := scheduler.EventFrom(req.GetEvent().Type.String(), req.GetEvent().Value.AsMap(), jobName, tnnt)
	if err != nil {
		h.l.Error("error adapting event: %s", err)
		return nil, errors.GRPCErr(err, "unable to parse event")
	}
	me := errors.NewMultiError("errors in RegisterJobEvent")

	err = h.service.UpdateJobState(ctx, event)
	if err != nil {
		h.l.Error("error updating job run state for Job: %s, Project: %s, eventType: %s, schedule_at: %s, err: %s", jobName, tnnt.ProjectName(), event.Type, event.JobScheduledAt.String(), err.Error())
		me.Append(errors.AddErrContext(err, scheduler.EntityJobRun, "scheduler could not update job run state"))
	}

	err = h.notifier.Relay(ctx, event)
	me.Append(err)

	err = h.notifier.Webhook(ctx, event)
	me.Append(err)

	err = h.notifier.Push(ctx, event)
	me.Append(err)

	return &pb.RegisterJobEventResponse{}, me.ToErr()
}

// GetInterval gets interval on specific job given reference time.
func (h JobRunHandler) GetInterval(ctx context.Context, req *pb.GetIntervalRequest) (*pb.GetIntervalResponse, error) {
	projectName, err := tenant.ProjectNameFrom(req.GetProjectName())
	if err != nil {
		h.l.Error("error adapting project name [%s]: %v", req.GetProjectName(), err)
		return nil, errors.GRPCErr(err, "unable to adapt project name")
	}

	jobName, err := scheduler.JobNameFrom(req.GetJobName())
	if err != nil {
		h.l.Error("error adapting job name [%s]: %v", req.GetJobName(), err)
		return nil, errors.GRPCErr(err, "unable to adapt job name")
	}

	if err := req.ReferenceTime.CheckValid(); err != nil {
		h.l.Error("invalid reference time for interval at: %s", err)
		return nil, errors.GRPCErr(errors.InvalidArgument(scheduler.EntityJobRun, "invalid reference time"), "unable to get interval for "+jobName.String())
	}

	interval, err := h.service.GetInterval(ctx, projectName, jobName, req.ReferenceTime.AsTime())
	if err != nil {
		h.l.Error("error getting interval for job [%s] under project [%s]: %v", jobName, projectName, err)
		return nil, errors.GRPCErr(err, "error getting interval for job "+jobName.String())
	}

	return &pb.GetIntervalResponse{
		StartTime: timestamppb.New(interval.Start()),
		EndTime:   timestamppb.New(interval.End()),
	}, nil
}

func NewJobRunHandler(l log.Logger, service JobRunService, notifier Notifier) *JobRunHandler {
	return &JobRunHandler{
		l:        l,
		service:  service,
		notifier: notifier,
	}
}
