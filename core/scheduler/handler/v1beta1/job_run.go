package v1beta1

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/goto/salt/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/goto/optimus/config"
	"github.com/goto/optimus/core/job"
	"github.com/goto/optimus/core/resource"
	"github.com/goto/optimus/core/scheduler"
	"github.com/goto/optimus/core/scheduler/service"
	"github.com/goto/optimus/core/tenant"
	"github.com/goto/optimus/internal/errors"
	"github.com/goto/optimus/internal/lib/interval"
	"github.com/goto/optimus/internal/utils/filter"
	pb "github.com/goto/optimus/protos/gotocompany/optimus/core/v1beta1"
)

var dexAPIResponse = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "dex_api_response",
}, []string{"project", "job", "resource_urn", "status", "run_type", "scheduled_date"})

const (
	sensorStatusErr        = "error"
	sensorStatusIncomplete = "incomplete"
	sensorStatusComplete   = "complete"

	sensorRunTypeScheduled = "scheduled"
	sensorRunTypeBackfill  = "backfill"

	backfillLookbackPeriodInHours = 24 * time.Hour
)

type JobSLAPredictorService interface {
	IdentifySLABreaches(ctx context.Context, projectName tenant.ProjectName, jobNames []scheduler.JobName, labels map[string]string, reqConfig service.JobSLAPredictorRequestConfig) (map[scheduler.JobName]map[scheduler.JobName]*service.JobState, error)
	IdentifySLABreachesBatch(ctx context.Context, combos []service.SLABreachCombo, reqConfig service.JobSLAPredictorRequestConfig) (map[string]*service.TargetBreach, error)
}

type JobExpectatorService interface {
	GenerateExpectedFinishTimes(ctx context.Context, projectName tenant.ProjectName, jobNames []scheduler.JobName, labels map[string]string, referenceTime time.Time, scheduleRangeInHours time.Duration) (map[scheduler.JobSchedule]service.FinishTimeDetail, error)
}

type JobRunService interface {
	JobRunInput(context.Context, tenant.ProjectName, scheduler.JobName, scheduler.RunConfig) (*scheduler.ExecutorInput, error)
	UpdateJobState(context.Context, *scheduler.Event) error
	GetJobRunsByFilter(ctx context.Context, projectName tenant.ProjectName, jobName scheduler.JobName, filters ...filter.FilterOpt) ([]*scheduler.JobRun, error)
	GetJobRuns(ctx context.Context, projectName tenant.ProjectName, jobName scheduler.JobName, criteria *scheduler.JobRunsCriteria) ([]*scheduler.JobRunStatus, string, error)
	UploadToScheduler(ctx context.Context, projectName tenant.ProjectName) error
	GetInterval(ctx context.Context, projectName tenant.ProjectName, jobName scheduler.JobName, referenceTime time.Time) (interval.Interval, error)

	GetReplayRunByScheduledAt(ctx context.Context, projectName tenant.ProjectName, jobName scheduler.JobName, scheduledAt time.Time) (*scheduler.ReplayWithRun, error)
}

type JobLineageService interface {
	GetJobExecutionSummary(ctx context.Context, jobSchedules []*scheduler.JobSchedule, numberOfUpstreamPerLevel int) ([]*scheduler.JobRunLineage, error)
}

type ThirdPartySensorService interface {
	GetClient(upstreamResolverType config.UpstreamResolverType) (service.ThirdPartyClient, error)
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
	l                       log.Logger
	service                 JobRunService
	schedulerService        SchedulerService
	notifier                Notifier
	jobLineageService       JobLineageService
	jobSLAPredictorService  JobSLAPredictorService
	thirdPartySensorService ThirdPartySensorService
	jobExpectatorService    JobExpectatorService

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

	runConfig, err := scheduler.RunConfigFrom(executor, req.ScheduledAt.AsTime(), req.JobrunId, req.DagRunId)
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

func (h JobRunHandler) GetDexSensorStatus(ctx context.Context, resourceURN resource.URN, startTime, endTime time.Time) (*pb.DexSensorResponse, error) {
	client, err := h.thirdPartySensorService.GetClient(config.DexUpstreamResolver)
	if err != nil {
		h.l.Error("error getting third party sensor client: %s", err)
		return nil, errors.GRPCErr(err, "unable to get third party sensor client")
	}

	isComplete, response, err := client.IsComplete(ctx, resourceURN, startTime, endTime)
	if err != nil {
		h.l.Error("error checking data completeness from third party sensor: %s", err)
		return nil, errors.GRPCErr(err, "unable to check data completeness from third party sensor")
	}

	stats, ok := response.(*scheduler.DataCompletenessStatus)
	if !ok {
		h.l.Warn("error asserting response type: %s", err)
	}

	dataCompleteness := make([]*pb.DataCompleteness, len(stats.DataCompletenessByDate))
	for i, dateStat := range stats.DataCompletenessByDate {
		dataCompleteness[i] = &pb.DataCompleteness{
			Date:       timestamppb.New(dateStat.Date),
			IsComplete: dateStat.IsComplete,
		}
	}

	return &pb.DexSensorResponse{
		IsComplete: isComplete,
		Log:        dataCompleteness,
	}, nil
}

func getJakartaTimeZone() *time.Location {
	jakartaLoc, _ := time.LoadLocation("Asia/Jakarta")
	return jakartaLoc
}

func getJakartaMidnightTime() time.Time {
	jakartaLoc := getJakartaTimeZone()
	now := time.Now().In(jakartaLoc)
	return time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, jakartaLoc)
}

// GetThirdPartySensorStatus gets third party sensor status
func (h JobRunHandler) GetThirdPartySensorStatus(ctx context.Context, req *pb.GetThirdPartySensorRequest) (*pb.GetThirdPartySensorResponse, error) {
	projectName, err := tenant.ProjectNameFrom(req.GetProjectName())
	if err != nil {
		h.l.Error("error adapting project name [%s]: %s", req.GetProjectName(), err)
		return nil, errors.GRPCErr(err, "unable to get third party sensor status for "+req.GetJobName())
	}

	jobName, err := scheduler.JobNameFrom(req.GetJobName())
	if err != nil {
		h.l.Error("error adapting job name [%s]: %s", req.GetJobName(), err)
		return nil, errors.GRPCErr(err, "unable to get third party sensor status for "+req.GetJobName())
	}

	// calculate startTime and endTime from request scheduledAt
	intervalResp, err := h.GetInterval(ctx, &pb.GetIntervalRequest{
		ProjectName:   req.GetProjectName(),
		JobName:       req.GetJobName(),
		ReferenceTime: req.GetScheduledAt(),
	})
	if err != nil {
		h.l.Error("error getting interval for job [%s:%s]: %s", projectName, jobName, err)
		return nil, errors.GRPCErr(err, "unable to get third party sensor status for "+req.GetJobName())
	}

	runType := sensorRunTypeScheduled
	if time.Since(req.GetScheduledAt().AsTime()) > backfillLookbackPeriodInHours {
		// consider attempts to get sensor status for a scheduled time in the past beyond the lookback period as backfill
		runType = sensorRunTypeBackfill
	}

	// TODO reevaluate this logic in the future, but ideally replayed runs should already be covered on the case above
	// replayWithRun, _ := h.service.GetReplayRunByScheduledAt(ctx, projectName, jobName, req.GetScheduledAt().AsTime())
	// if replayWithRun != nil {
	// 	runType = sensorRunTypeBackfill
	// }

	startTime := intervalResp.GetStartTime().AsTime().In(getJakartaTimeZone())
	endTime := intervalResp.GetEndTime().AsTime().In(getJakartaTimeZone())
	logicalEndTime := getJakartaMidnightTime().Add(-1 * time.Minute)
	if endTime.After(logicalEndTime) {
		h.l.Info(fmt.Sprintf("resetting the third party sensor window end time to "+
			"logical jakarta based end time: %s, job: %s", logicalEndTime.Format(time.RFC3339), jobName))
		endTime = logicalEndTime
	}
	if startTime.After(endTime.Add(-24 * time.Hour)) {
		startTime = endTime.Add(-24 * time.Hour)
	}

	thirdPartyType := req.GetThirdPartyType()
	if thirdPartyType == job.ThirdPartyTypeDex {
		dexSensorReq := req.GetDexSensorRequest()
		if dexSensorReq == nil {
			h.l.Error("error getting dex sensor request")
			return nil, errors.GRPCErr(errors.InvalidArgument("thirdPartySensor", "unable to get third party sensor status"), " job:  "+req.GetJobName())
		}
		resourceURN, err := resource.ParseURN(dexSensorReq.GetResourceUrn())
		if err != nil {
			h.l.Error("error parsing resource urn [%s]: %s", dexSensorReq.GetResourceUrn(), err)
			return nil, errors.GRPCErr(err, "unable to get third party sensor status for "+req.GetJobName())
		}
		resp, err := h.GetDexSensorStatus(ctx, resourceURN, startTime, endTime)
		if err != nil {
			h.l.Error(fmt.Sprintf("error getting third party sensor status for project: %s, job: %s, resourceURN: %s, err: %s", string(projectName), string(jobName), resourceURN.String(), err.Error()))
			dexAPIResponse.WithLabelValues(string(projectName), string(jobName), resourceURN.String(), sensorStatusErr, runType, req.GetScheduledAt().AsTime().Format(time.RFC3339)).Inc()
			return nil, err
		}
		if resp != nil && !resp.IsComplete {
			completenessLog := fmt.Sprintf("request interval start: %s, end, %s, ",
				startTime.In(getJakartaTimeZone()).Format(time.RFC3339),
				endTime.In(getJakartaTimeZone()).Format(time.RFC3339))

			for _, completeness := range resp.Log {
				completenessLog += fmt.Sprintf("[ date: %s, isComplete: %t ],",
					completeness.Date.AsTime().In(getJakartaTimeZone()).Format(time.RFC3339), completeness.IsComplete)
			}
			h.l.Error(fmt.Sprintf("error getting third party sensor status for project: %s, job: %s, resourceURN: %s, log: %v", string(projectName), string(jobName), resourceURN.String(), completenessLog))
			dexAPIResponse.WithLabelValues(string(projectName), string(jobName), resourceURN.String(), sensorStatusIncomplete, runType, req.GetScheduledAt().AsTime().Format(time.RFC3339)).Inc()
		} else {
			dexAPIResponse.WithLabelValues(string(projectName), string(jobName), resourceURN.String(), sensorStatusComplete, runType, req.GetScheduledAt().AsTime().Format(time.RFC3339)).Inc()
		}

		return &pb.GetThirdPartySensorResponse{
			Payload: &pb.GetThirdPartySensorResponse_DexSensorResponse{
				DexSensorResponse: resp,
			},
		}, nil
	}
	err = errors.NewError(
		errors.ErrInvalidArgument,
		scheduler.EntityThirdPartySensor,
		fmt.Sprintf("invalid third party type: %s,for job [%s:%s]", thirdPartyType, projectName, jobName))
	return nil, errors.GRPCErr(err, "unable to process third party sensor ")
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

// IdentifyPotentialSLABreach predicts potential SLA breaches for the given job targets at their targeted SLA time.
// It accepts multiple projects and multiple label groups in a single request, evaluates the cross product, and
// consolidates alerting into one message per team.
func (h JobRunHandler) IdentifyPotentialSLABreach(ctx context.Context, req *pb.IdentifyPotentialSLABreachRequest) (*pb.IdentifyPotentialSLABreachResponse, error) {
	response := pb.IdentifyPotentialSLABreachResponse{}

	// resolve projects: prefer repeated project_names, fall back to path project_name
	projectNameStrs := req.GetProjectNames()
	if len(projectNameStrs) == 0 && req.GetProjectName() != "" {
		projectNameStrs = []string{req.GetProjectName()}
	}
	if len(projectNameStrs) == 0 {
		err := errors.InvalidArgument(scheduler.EntityJobRun, "no project provided")
		h.l.Error("error identifying potential SLA breaches: %v", err)
		return nil, errors.GRPCErr(err, "unable to identify potential SLA breaches")
	}
	projectNames := make([]tenant.ProjectName, 0, len(projectNameStrs))
	for _, pn := range projectNameStrs {
		projectName, err := tenant.ProjectNameFrom(pn)
		if err != nil {
			h.l.Error("error adapting project name [%s]: %v", pn, err)
			return nil, errors.GRPCErr(err, "unable to adapt project name")
		}
		projectNames = append(projectNames, projectName)
	}

	jobNames := []scheduler.JobName{}
	for _, jn := range req.GetJobNames() {
		jobName, err := scheduler.JobNameFrom(jn)
		if err != nil {
			h.l.Error("error adapting job name [%s]: %v", jn, err)
			return nil, errors.GRPCErr(err, "unable to adapt job name")
		}
		jobNames = append(jobNames, jobName)
	}

	// resolve label groups: prefer repeated label_groups, fall back to job_labels + request severity
	type labelGroup struct {
		labels   map[string]string
		severity string
		name     string
	}
	labelGroups := make([]labelGroup, 0, len(req.GetLabelGroups()))
	for _, g := range req.GetLabelGroups() {
		labelGroups = append(labelGroups, labelGroup{labels: g.GetJobLabels(), severity: g.GetSeverity(), name: g.GetName()})
	}
	legacyMode := len(labelGroups) == 0
	if legacyMode {
		labelGroups = append(labelGroups, labelGroup{labels: req.GetJobLabels(), severity: req.GetSeverity()})
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

	// build combos = projects x label groups. job_names only applies in legacy
	// single-project mode where target selection isn't ambiguous.
	combos := make([]service.SLABreachCombo, 0, len(projectNames)*len(labelGroups))
	for _, projectName := range projectNames {
		for _, g := range labelGroups {
			combo := service.SLABreachCombo{
				ProjectName: projectName,
				Labels:      g.labels,
				GroupName:   g.name,
				Severity:    g.severity,
			}
			if legacyMode && len(projectNames) == 1 {
				combo.JobNames = jobNames
			}
			combos = append(combos, combo)
		}
	}

	targetBreaches, err := h.jobSLAPredictorService.IdentifySLABreachesBatch(ctx, combos, reqConfig)
	if err != nil {
		h.l.Error("error identifying potential SLA breaches: %v", err)
		return nil, errors.GRPCErr(err, "unable to identify potential SLA breaches")
	}

	jobs := make(map[string]*pb.UpstreamJobsStatus)
	for key, breach := range targetBreaches {
		upstreamStatus := []*pb.UpstreamJobStatus{}
		for upstreamJobName, upstreamJobState := range breach.Upstreams {
			if upstreamJobState == nil || upstreamJobState.InferredSLA == nil {
				continue
			}
			upstreamStatus = append(upstreamStatus, &pb.UpstreamJobStatus{
				ProjectName:     upstreamJobState.Tenant.ProjectName().String(),
				JobName:         upstreamJobName.String(),
				ScheduledAt:     timestamppb.New(upstreamJobState.JobRun.ScheduledAt),
				InferredSlaTime: timestamppb.New(*upstreamJobState.InferredSLA),
				RelativeLevel:   int32(upstreamJobState.RelativeLevel),
				Status:          string(upstreamJobState.Status),
			})
		}
		if len(upstreamStatus) == 0 {
			continue
		}
		jobs[key] = &pb.UpstreamJobsStatus{
			JobsStatus:        upstreamStatus,
			TargetProjectName: breach.TargetProject,
			TargetJobName:     breach.TargetJobName.String(),
		}
	}

	if len(jobs) > 0 {
		response.Jobs = jobs
	}

	return &response, nil
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

// GenerateExpectedFinishTime generates expected finish time for jobs based on their schedule in the given range
func (h JobRunHandler) GenerateExpectedFinishTime(ctx context.Context, req *pb.GenerateExpectedFinishTimeRequest) (*pb.GenerateExpectedFinishTimeResponse, error) {
	projectName, err := tenant.ProjectNameFrom(req.GetProjectName())
	if err != nil {
		h.l.Error(fmt.Sprintf("error adapting project name [%s]: %s", req.GetProjectName(), err.Error()))
		return nil, errors.GRPCErr(err, "unable to adapt project name")
	}

	jobNames := []scheduler.JobName{}
	for _, jn := range req.GetJobNames() {
		jobName, err := scheduler.JobNameFrom(jn)
		if err != nil {
			h.l.Error(fmt.Sprintf("error adapting job name [%s]: %s", jn, err.Error()))
			return nil, errors.GRPCErr(err, "unable to adapt job name")
		}
		jobNames = append(jobNames, jobName)
	}

	referenceTime := time.Now().UTC()
	if req.GetReferenceTime() != nil && req.GetReferenceTime().IsValid() {
		referenceTime = req.GetReferenceTime().AsTime().UTC()
	}
	scheduleRangeInHours := time.Duration(req.GetScheduledRangeInHours()) * time.Hour

	jobsWithFinishTime, err := h.jobExpectatorService.GenerateExpectedFinishTimes(ctx, projectName, jobNames, req.GetJobLabels(), referenceTime, scheduleRangeInHours)
	if err != nil {
		h.l.Error(fmt.Sprintf("error generating expected finish times: %s", err.Error()))
		return nil, errors.GRPCErr(err, "unable to generate expected finish times")
	}

	response := &pb.GenerateExpectedFinishTimeResponse{
		InprogressJobs: make(map[string]*pb.FinishTimeDetailResponse),
		FinishedJobs:   make(map[string]*pb.FinishTimeDetailResponse),
	}
	for jobSchedule, jobWithFinishTime := range jobsWithFinishTime {
		finishTimeDetail := &pb.FinishTimeDetailResponse{
			ScheduledAt: timestamppb.New(jobSchedule.ScheduledAt),
		}

		switch jobWithFinishTime.Status {
		case service.FinishTimeStatusFinished:
			finishTimeDetail.FinishTime = &pb.FinishTimeDetailResponse_ActualFinishTime{ActualFinishTime: timestamppb.New(jobWithFinishTime.FinishTime)}
			response.FinishedJobs[jobSchedule.JobName.String()] = finishTimeDetail
		case service.FinishTimeStatusInprogress:
			finishTimeDetail.FinishTime = &pb.FinishTimeDetailResponse_ExpectedFinishTime{ExpectedFinishTime: timestamppb.New(jobWithFinishTime.FinishTime)}
			response.InprogressJobs[jobSchedule.JobName.String()] = finishTimeDetail
		}
	}

	return response, nil
}

func NewJobRunHandler(
	l log.Logger,
	service JobRunService,
	notifier Notifier,
	schedulerService SchedulerService,
	jobLineageService JobLineageService,
	jobSLAPredictorService JobSLAPredictorService,
	thirdPartySensorService ThirdPartySensorService,
	jobExpectatorService JobExpectatorService,
) *JobRunHandler {
	return &JobRunHandler{
		l:                       l,
		service:                 service,
		notifier:                notifier,
		schedulerService:        schedulerService,
		jobLineageService:       jobLineageService,
		jobSLAPredictorService:  jobSLAPredictorService,
		thirdPartySensorService: thirdPartySensorService,
		jobExpectatorService:    jobExpectatorService,
	}
}
