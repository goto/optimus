package v1beta1

import (
	"fmt"
	"sort"
	"strings"

	"github.com/google/uuid"
	"github.com/goto/salt/log"
	"golang.org/x/net/context"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/goto/optimus/core/scheduler"
	"github.com/goto/optimus/core/tenant"
	"github.com/goto/optimus/internal/errors"
	"github.com/goto/optimus/internal/models"
	"github.com/goto/optimus/internal/utils"
	"github.com/goto/optimus/internal/utils/filter"
	pb "github.com/goto/optimus/protos/gotocompany/optimus/core/v1beta1"
)

type BackfillService interface {
	CreateBackfill(ctx context.Context, backfillReq *scheduler.Backfill) (backfillID uuid.UUID, dagRunID string, err error)
	BackfillDryRun(ctx context.Context, backfillReq *scheduler.Backfill) (jobRunInput *scheduler.ExecutorInput, err error)
	GetBackfills(ctx context.Context, projectName tenant.ProjectName, filters ...filter.FilterOpt) ([]*scheduler.Backfill, error)
	GetBackfillByID(ctx context.Context, backfillID uuid.UUID) (*scheduler.Backfill, error)
	CancelBackfill(ctx context.Context, backfillID uuid.UUID, canceledBy string) error
}

type backfillRequest interface {
	GetProjectName() string
	GetNamespaceName() string
	GetJobName() string
	GetDataStartTime() *timestamppb.Timestamp
	GetDataEndTime() *timestamppb.Timestamp
	GetJobConfig() string
	GetOverriddenAssets() map[string]string
	GetDescription() string
	GetCategory() string
	GetApprovalId() string
	GetUserId() string
}

type BackfillHandler struct {
	l       log.Logger
	service BackfillService

	pb.UnimplementedBackfillServiceServer
}

func newBackfilllRequest(l log.Logger, req backfillRequest) (*scheduler.Backfill, error) {
	replayTenant, err := tenant.NewTenant(req.GetProjectName(), req.GetNamespaceName())
	if err != nil {
		l.Error("invalid tenant information request project [%s] namespace [%s]: %s", req.GetProjectName(), req.GetNamespaceName(), err)
		return nil, errors.GRPCErr(err, "unable to start replay for "+req.GetJobName())
	}

	jobName, err := scheduler.JobNameFrom(req.GetJobName())
	if err != nil {
		l.Error("error adapting job name [%s]: %s", req.GetJobName(), err)
		return nil, errors.GRPCErr(err, "unable to start replay for "+req.GetJobName())
	}

	if err = req.GetDataStartTime().CheckValid(); err != nil {
		l.Error("error validating start time: %s", err)
		return nil, errors.GRPCErr(errors.InvalidArgument(scheduler.EntityJobRun, "invalid start_time"), "unable to start replay for "+req.GetJobName())
	}

	if err = req.GetDataEndTime().CheckValid(); err != nil {
		l.Error("error validating end time: %s", err)
		return nil, errors.GRPCErr(errors.InvalidArgument(scheduler.EntityJobRun, "invalid end_time"), "unable to start end for "+req.GetJobName())
	}

	if strings.TrimSpace(req.GetApprovalId()) == "" {
		err := fmt.Errorf("approval ID cannot be empty")
		l.Error(err.Error())
		return nil, errors.GRPCErr(errors.InvalidArgument(scheduler.EntityReplay, err.Error()), "unable to start replay for "+req.GetJobName())
	}

	if strings.TrimSpace(req.GetUserId()) == "" {
		err := fmt.Errorf("user ID cannot be empty")
		l.Error(err.Error())
		return nil, errors.GRPCErr(errors.InvalidArgument(scheduler.EntityReplay, err.Error()), "unable to start replay for "+req.GetJobName())
	}

	jobConfig := make(map[string]string)
	if req.GetJobConfig() != "" {
		jobConfig, err = parseJobConfig(req.GetJobConfig())
		if err != nil {
			return nil, err
		}
	}

	allowedReplayCategories := utils.ListToMap(models.ReplayCategories)

	if !utils.Contains(allowedReplayCategories, req.GetCategory()) {
		err = fmt.Errorf("invalid category: %s, allowed categories are: DQ_FIX, BACKFILL, OTHERS", req.GetCategory())
		l.Error(err.Error())
		return nil, errors.GRPCErr(errors.InvalidArgument(scheduler.EntityReplay, err.Error()), "unable to start replay for "+req.GetJobName())
	}

	if strings.TrimSpace(req.GetDescription()) == "" {
		err = fmt.Errorf("description cannot be empty")
		l.Error(err.Error())
		return nil, errors.GRPCErr(errors.InvalidArgument(scheduler.EntityReplay, err.Error()), "unable to start replay for "+req.GetJobName())
	}

	backfillConfig := scheduler.NewBackfillConfig(
		req.GetDataStartTime().AsTime(),
		req.GetDataEndTime().AsTime(),
		jobConfig,
		req.GetOverriddenAssets(),
		req.GetDescription(),
		req.GetCategory(),
		req.GetApprovalId(),
		req.GetUserId(),
	)

	backfillReq := scheduler.NewBackfillRequest(jobName, replayTenant, backfillConfig, scheduler.BackfillStateCreated, "create backfill request")
	return backfillReq, nil
}

func (h BackfillHandler) CreateBackfill(ctx context.Context, req *pb.CreateBackfillRequest) (*pb.CreateBackfillResponse, error) {
	backfillReq, err := newBackfilllRequest(h.l, req)
	if err != nil {
		return nil, err
	}

	backfillID, dagRunID, err := h.service.CreateBackfill(ctx, backfillReq)
	if err != nil {
		h.l.Error("error creating backfill for job [%s]: %s", req.GetJobName(), err)
		return nil, errors.GRPCErr(err, "unable to start backfill for "+req.GetJobName())
	}

	return &pb.CreateBackfillResponse{
		Id:       backfillID.String(),
		DagRunId: dagRunID,
	}, nil
}

func (h BackfillHandler) BackfillPreview(ctx context.Context, req *pb.BackfillPreviewRequest) (*pb.BackfillPreviewResponse, error) {
	backfillReq, err := newBackfilllRequest(h.l, req)
	if err != nil {
		return nil, err
	}

	jobRunInput, err := h.service.BackfillDryRun(ctx, backfillReq)
	if err != nil {
		h.l.Error("error creating backfill for job [%s]: %s", req.GetJobName(), err)
		return nil, errors.GRPCErr(err, "unable to run backfill dry run for "+req.GetJobName())
	}

	return &pb.BackfillPreviewResponse{
		CompiledAssets: &pb.BackfillCompiledAssets{
			Envs:  jobRunInput.Configs,
			Files: jobRunInput.Files,
		},
	}, nil
}

func (h BackfillHandler) CancelBackfill(ctx context.Context, req *pb.CancelBackfillRequest) (*pb.CancelBackfillResponse, error) {
	if strings.TrimSpace(req.GetCanceledBy()) == "" {
		err := errors.InvalidArgument(scheduler.EntityBackfill, "canceled_by cannot be empty")
		return nil, errors.GRPCErr(err, "unable to cancel backfill ")
	}

	id, err := uuid.Parse(req.GetBackfillId())
	if err != nil {
		h.l.Error("error parsing backfill id [%s]: %s", req.GetBackfillId(), err)
		err = errors.InvalidArgument(scheduler.EntityBackfill, err.Error())
		return nil, errors.GRPCErr(err, "unable to cancel backfill "+req.GetBackfillId())
	}

	if err := h.service.CancelBackfill(ctx, id, req.GetCanceledBy()); err != nil {
		h.l.Error("error cancelling backfill [%s]: %s", id.String(), err)
		return nil, errors.GRPCErr(err, "unable to cancel backfill "+req.GetBackfillId())
	}

	backfill, err := h.service.GetBackfillByID(ctx, id)
	if err != nil {
		h.l.Error("error getting backfill details [%s]: %s", id.String(), err)
		return nil, errors.GRPCErr(err, "unable to get backfill details "+req.GetBackfillId())
	}

	return &pb.CancelBackfillResponse{
		Backfill: backfillToProto(backfill),
	}, nil
}

func NewBackfilllHandler(l log.Logger, service BackfillService) *BackfillHandler {
	return &BackfillHandler{l: l, service: service}
}

func (h BackfillHandler) GetBackfills(ctx context.Context, req *pb.GetBackfillsRequest) (*pb.GetBackfillsResponse, error) {
	projectName, err := tenant.ProjectNameFrom(req.GetProjectName())
	if err != nil {
		h.l.Error("error adapting project name [%s]: %s", req.GetProjectName(), err)
		return nil, errors.GRPCErr(err, "unable to get backfill for project "+req.GetProjectName())
	}

	backfills, err := h.service.GetBackfills(ctx, projectName,
		filter.WithString(filter.SchedulerRunID, req.GetSchedulerRunId()),
		filter.WithString(filter.BackfillID, req.GetBackfillId()),
		filter.WithStringArray(filter.JobNames, req.GetJobNames()),
		filter.WithString(filter.BackfillStatus, req.GetStatus()),
		filter.WithString(filter.ApprovalID, req.GetApprovalId()),
		filter.WithString(filter.UserID, req.GetUserId()),
	)
	if err != nil {
		h.l.Error(fmt.Sprintf("error getting backfills for req: %+v, err: %s", req, err.Error()))
		return nil, errors.GRPCErr(err, "unable to get backfills with filter")
	}

	specs := make([]*pb.BackfillSpec, len(backfills))
	for i, b := range backfills {
		specs[i] = backfillToProto(b)
	}

	return &pb.GetBackfillsResponse{Backfills: specs}, nil
}

func backfillToProto(b *scheduler.Backfill) *pb.BackfillSpec {
	return &pb.BackfillSpec{
		Id:               b.ID().String(),
		SchedulerRunId:   b.SchedulerRunID,
		ProjectName:      b.Tenant().ProjectName().String(),
		NamespaceName:    b.Tenant().NamespaceName().String(),
		JobName:          b.JobName().String(),
		Description:      b.Config().Description,
		DataStartTime:    timestamppb.New(b.Config().Dstart),
		DataEndTime:      timestamppb.New(b.Config().Dend),
		OverriddenAssets: b.Config().Assets,
		StartTime:        timestamppb.New(b.CreatedAt()),
		EndTime:          timestamppb.New(b.UpdatedAt()),
		JobConfig:        formatJobConfig(b.Config().JobConfig),
		Status:           b.State().String(),
		Message:          b.Message(),
		CreatedAt:        timestamppb.New(b.CreatedAt()),
		UpdatedAt:        timestamppb.New(b.UpdatedAt()),
		Category:         b.Config().Category,
		ApprovalId:       b.Config().ApprovalID,
		UserId:           b.Config().UserID,
	}
}

func formatJobConfig(configs map[string]string) string {
	if len(configs) == 0 {
		return ""
	}
	keys := make([]string, 0, len(configs))
	for key := range configs {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		parts = append(parts, fmt.Sprintf("%s=%s", key, configs[key]))
	}
	return strings.Join(parts, ",")
}
