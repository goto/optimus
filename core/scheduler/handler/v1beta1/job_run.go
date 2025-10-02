package v1beta1

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/goto/salt/log"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/goto/optimus/core/scheduler"
	"github.com/goto/optimus/core/scheduler/service"
	"github.com/goto/optimus/core/tenant"
	"github.com/goto/optimus/internal/errors"
	"github.com/goto/optimus/internal/lib/interval"
	"github.com/goto/optimus/internal/utils/filter"
	pb "github.com/goto/optimus/protos/gotocompany/optimus/core/v1beta1"
)

type JobSLAPredictorService interface {
	IdentifySLABreaches(ctx context.Context, projectName tenant.ProjectName, nextScheduleRangeInHours time.Duration, jobNames []scheduler.JobName, labels map[string]string) (map[scheduler.JobName]map[scheduler.JobName]*service.JobState, error)
}

type JobRunService interface {
	JobRunInput(context.Context, tenant.ProjectName, scheduler.JobName, scheduler.RunConfig) (*scheduler.ExecutorInput, error)
	UpdateJobState(context.Context, *scheduler.Event) error
	GetJobRunsByFilter(ctx context.Context, projectName tenant.ProjectName, jobName scheduler.JobName, filters ...filter.FilterOpt) ([]*scheduler.JobRun, error)
	GetJobRuns(ctx context.Context, projectName tenant.ProjectName, jobName scheduler.JobName, criteria *scheduler.JobRunsCriteria) ([]*scheduler.JobRunStatus, string, error)
	UploadToScheduler(ctx context.Context, projectName tenant.ProjectName) error
	GetInterval(ctx context.Context, projectName tenant.ProjectName, jobName scheduler.JobName, referenceTime time.Time) (interval.Interval, error)
}

type JobLineageService interface {
	GetJobExecutionSummary(ctx context.Context, jobSchedules []*scheduler.JobSchedule, numberOfUpstreamPerLevel int) ([]*scheduler.JobRunLineage, error)
}

type SchedulerService interface {
	CreateSchedulerRole(ctx context.Context, t tenant.Tenant, roleName string) error
	GetRolePermissions(ctx context.Context, t tenant.Tenant, roleName string) ([]string, error)
}

type Notifier interface {
	Push(ctx context.Context, event *scheduler.Event) error
	Webhook(ctx context.Context, event *scheduler.Event) error
	Relay(ctx context.Context, event *scheduler.Event) error
}

type JobRunHandler struct {
	l                      log.Logger
	service                JobRunService
	schedulerService       SchedulerService
	notifier               Notifier
	jobLineageService      JobLineageService
	jobSLAPredictorService JobSLAPredictorService

	pb.UnimplementedJobRunServiceServer
}

func (h JobRunHandler) GetSchedulerRole(ctx context.Context, req *pb.GetSchedulerRoleRequest) (*pb.GetSchedulerRoleResponse, error) {
	tnnt, err := tenant.NewTenant(req.GetProjectName(), req.GetNamespaceName())
	if err != nil {
		h.l.Error("invalid tenant information request project [%s] namespace [%s]: %s", req.GetProjectName(), req.GetNamespaceName(), err)
		return nil, errors.GRPCErr(err, "unable to get tenant")
	}
	roleName := req.GetRoleName()
	if roleName == "" {
		return nil, errors.GRPCErr(errors.InvalidArgument("scheduler", "roleName name is empty"), "")
	}

	permissions, err := h.schedulerService.GetRolePermissions(ctx, tnnt, roleName)
	if err != nil {
		return &pb.GetSchedulerRoleResponse{}, errors.GRPCErr(err, "unable to get role")
	}
	return &pb.GetSchedulerRoleResponse{
		Permissions: permissions,
	}, nil
}

func (h JobRunHandler) CreateSchedulerRole(ctx context.Context, req *pb.CreateSchedulerRoleRequest) (*pb.CreateSchedulerRoleResponse, error) {
	tnnt, err := tenant.NewTenant(req.GetProjectName(), req.GetNamespaceName())
	if err != nil {
		h.l.Error("invalid tenant information request project [%s] namespace [%s]: %s", req.GetProjectName(), req.GetNamespaceName(), err)
		return nil, errors.GRPCErr(err, "unable to get tenant")
	}
	roleName := req.GetRoleName()
	if roleName == "" {
		return nil, errors.GRPCErr(errors.InvalidArgument("scheduler", "roleName name is empty"), "")
	}

	err = h.schedulerService.CreateSchedulerRole(ctx, tnnt, roleName)
	if err != nil {
		if strings.Contains(err.Error(), "409") {
			err = errors.FailedPrecondition("Scheduler", fmt.Sprintf("unable to create role:[%s], err:[%s]", req.GetRoleName(), err.Error()))
		}
		return &pb.CreateSchedulerRoleResponse{}, errors.GRPCErr(err, "unable to register role")
	}
	return &pb.CreateSchedulerRoleResponse{}, nil
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
	var msg string
	jobRuns, msg, err = h.service.GetJobRuns(ctx, projectName, jobName, criteria)
	if err != nil {
		h.l.Error("error getting job runs: %s", err)
		return nil, errors.GRPCErr(err, "unable to get job run for "+req.GetJobName())
	}

	h.l.Debug("JobRuns[%d] for %s", len(jobRuns), req.GetJobName())
	h.l.Debug("JobRuns %v", jobRuns)
	var runs []*pb.JobRun
	for _, run := range jobRuns {
		ts := timestamppb.New(run.ScheduledAt)
		runs = append(runs, &pb.JobRun{
			State:       run.State.String(),
			ScheduledAt: ts,
		})
	}
	return &pb.JobRunResponse{
		JobRuns: runs,
		Message: msg,
	}, nil
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
	go func() { //nolint: contextcheck
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

	if me.ToErr() != nil {
		h.l.Error("error handling RegisterJobEvent event: %s, err: %s", event, me.ToErr())
		return &pb.RegisterJobEventResponse{}, errors.GRPCErr(me.ToErr(), "error in RegisterJobEvent handler")
	}
	return &pb.RegisterJobEventResponse{}, nil
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

// IdentifyPotentialSLABreach predicts potential SLA breaches for the given job targets at their targeted SLA time
func (h JobRunHandler) IdentifyPotentialSLABreach(ctx context.Context, req *pb.IdentifyPotentialSLABreachRequest) (*pb.IdentifyPotentialSLABreachResponse, error) {
	// get job schedules by names and labels
	labels := map[string]string{} // TODO: use map[string]string instead

	response := pb.IdentifyPotentialSLABreachResponse{}

	jobNames := []scheduler.JobName{}
	for _, jn := range req.GetJobNames() {
		jobName, err := scheduler.JobNameFrom(jn)
		if err != nil {
			h.l.Error("error adapting job name [%s]: %v", jn, err)
			return nil, errors.GRPCErr(err, "unable to adapt job name")
		}
		jobNames = append(jobNames, jobName)
	}

	projectName, err := tenant.ProjectNameFrom(req.GetProjectName())
	if err != nil {
		h.l.Error("error adapting project name [%s]: %v", req.GetProjectName(), err)
		return nil, errors.GRPCErr(err, "unable to adapt project name")
	}
	// consider jobs with next schedule within next nextScheduleRangeInHours hours
	nextScheduleRangeInHours := time.Duration(req.GetNextScheduledRangeInHours()) * time.Hour
	jobBreaches, err := h.jobSLAPredictorService.IdentifySLABreaches(ctx, projectName, nextScheduleRangeInHours, jobNames, labels)
	if err != nil {
		h.l.Error("error identifying potential SLA breaches: %v", err)
		return nil, errors.GRPCErr(err, "unable to identify potential SLA breaches")
	}

	jobs := make(map[string]*pb.UpStreamBreachedJobs)
	for jobNameTarget, upstreamJobStates := range jobBreaches {
		upstreamStates := []*pb.UpstreamJobStatus{}
		for upstreamJobName, upstreamJobState := range upstreamJobStates {
			if upstreamJobState == nil || upstreamJobState.InferredSLA == nil {
				continue
			}
			upstreamStates = append(upstreamStates, &pb.UpstreamJobStatus{
				ProjectName:     upstreamJobState.Tenant.ProjectName().String(),
				JobName:         upstreamJobName.String(),
				InferredSlaTime: timestamppb.New(*upstreamJobState.InferredSLA),
				RelativeLevel:   int32(upstreamJobState.RelativeLevel),
				Status:          string(upstreamJobState.Status),
			})
		}
		if len(upstreamStates) == 0 {
			continue
		}
		jobs[jobNameTarget.String()] = &pb.UpStreamBreachedJobs{
			UpstreamJobs: upstreamStates,
		}
	}

	if len(jobs) > 0 {
		response.Jobs = jobs
	}

	if req.AlertOnBreach && len(response.Jobs) > 0 {
		// TODO: alert if there are potential SLA breaches and alerting is enabled
	}

	return &response, nil
}

func getLevel(currentJob *scheduler.JobLineageSummary, job *scheduler.JobLineageSummary, memo map[*scheduler.JobLineageSummary]int32, currentLevel int32) int32 {
	if currentJob.JobName == job.JobName {
		return currentLevel
	}
	if memo == nil {
		memo = make(map[*scheduler.JobLineageSummary]int32)
	}
	if level, ok := memo[currentJob]; ok {
		return level
	}
	maxLevel := int32(-1)
	for _, up := range currentJob.Upstreams {
		level := getLevel(up, job, memo, currentLevel+1)
		if level > maxLevel {
			maxLevel = level
		}
	}
	memo[currentJob] = maxLevel
	return maxLevel
}

func (h JobRunHandler) GetJobRunLineageSummary(ctx context.Context, req *pb.GetJobRunLineageSummaryRequest) (*pb.GetJobRunLineageSummaryResponse, error) {
	targetJobSchedules, err := fromJobRunLineageSummaryRequest(req)
	if err != nil {
		h.l.Error("error parsing job schedules from request: %s", err)
		return nil, errors.GRPCErr(err, "unable to parse job schedules from request")
	}
	jobRunLineages, err := h.jobLineageService.GetJobExecutionSummary(ctx, targetJobSchedules, int(req.GetNumberOfUpstreamPerLevel()))
	if err != nil {
		h.l.Error("error getting job run lineage summary: %s", err)
		return nil, errors.GRPCErr(err, "unable to get job run lineage summary")
	}

	return toJobRunLineageSummaryResponse(jobRunLineages), nil
}

func NewJobRunHandler(
	l log.Logger,
	service JobRunService,
	notifier Notifier,
	schedulerService SchedulerService,
	jobLineageService JobLineageService,
	jobSLAPredictorService JobSLAPredictorService,
) *JobRunHandler {
	return &JobRunHandler{
		l:                      l,
		service:                service,
		notifier:               notifier,
		schedulerService:       schedulerService,
		jobLineageService:      jobLineageService,
		jobSLAPredictorService: jobSLAPredictorService,
	}
}
