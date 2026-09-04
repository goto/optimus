package v1beta1

import (
	"context"

	"github.com/goto/salt/log"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/goto/optimus/core/completeness/service"
	"github.com/goto/optimus/internal/errors"
	pb "github.com/goto/optimus/protos/gotocompany/optimus/core/v1beta1"
)

// CompletenessService is the port this handler depends on; satisfied by
// core/completeness/service.Service.
type CompletenessService interface {
	CheckQueryCompleteness(ctx context.Context, datastoreName, query string) (*service.Result, error)
}

type CompletenessHandler struct {
	l       log.Logger
	service CompletenessService

	pb.UnimplementedCompletenessServiceServer
}

func NewCompletenessHandler(completenessService CompletenessService, logger log.Logger) *CompletenessHandler {
	return &CompletenessHandler{
		service: completenessService,
		l:       logger,
	}
}

func (h *CompletenessHandler) CheckQueryCompleteness(ctx context.Context, req *pb.CheckQueryCompletenessRequest) (*pb.CheckQueryCompletenessResponse, error) {
	result, err := h.service.CheckQueryCompleteness(ctx, req.GetDatastoreName(), req.GetQuery())
	if err != nil {
		return nil, errors.GRPCErr(err, "failed to check query completeness")
	}

	return &pb.CheckQueryCompletenessResponse{
		OverallStatus:   toProtoOverallStatus(result.OverallStatus),
		UnmanagedTables: toProtoUnmanagedTables(result.UnmanagedTables),
		ManagedTables:   toProtoManagedTables(result.ManagedTables),
	}, nil
}

func toProtoOverallStatus(s service.OverallStatus) pb.OverallStatus {
	if s == service.OverallStatusComplete {
		return pb.OverallStatus_OVERALL_STATUS_COMPLETE
	}
	return pb.OverallStatus_OVERALL_STATUS_NOT_COMPLETE
}

func toProtoUnmanagedTables(tables []service.UnmanagedTable) []*pb.UnmanagedTable {
	out := make([]*pb.UnmanagedTable, 0, len(tables))
	for _, t := range tables {
		out = append(out, &pb.UnmanagedTable{
			TableName:    t.TableName,
			ManagedByDex: t.ManagedByDex,
		})
	}
	return out
}

func toProtoManagedTables(tables []service.ManagedTable) []*pb.ManagedTable {
	out := make([]*pb.ManagedTable, 0, len(tables))
	for _, t := range tables {
		out = append(out, &pb.ManagedTable{
			TableName:        t.TableName,
			OptimusProject:   t.OptimusProject,
			OptimusNamespace: t.OptimusNamespace,
			JobName:          t.JobName,
			Run:              toProtoJobRun(t.Run),
			IsActive:         t.IsActive,
		})
	}
	return out
}

func toProtoJobRun(run *service.RunStatus) *pb.CompletenessJobRun {
	if run == nil {
		return nil
	}

	jobRun := &pb.CompletenessJobRun{
		State:       string(run.State),
		ScheduledAt: timestamppb.New(run.ScheduledAt),
		StartTime:   timestamppb.New(run.StartTime),
	}
	if run.EndTime != nil {
		jobRun.EndTime = timestamppb.New(*run.EndTime)
	}
	return jobRun
}
