package v1beta1_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/goto/salt/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/goto/optimus/core/scheduler"
	"github.com/goto/optimus/core/scheduler/handler/v1beta1"
	"github.com/goto/optimus/core/tenant"
	"github.com/goto/optimus/internal/utils/filter"
	pb "github.com/goto/optimus/protos/gotocompany/optimus/core/v1beta1"
)

func TestBackfillHandler(t *testing.T) {
	logger := log.NewNoop()
	ctx := context.Background()
	projectName := "a-data-proj"
	namespaceName := "a-namespace"
	category := "BACKFILL"
	approvalID, userID := "approval_id", "user_id"
	jobName, _ := scheduler.JobNameFrom("a-job-name")
	startTime := timestamppb.New(time.Date(2023, 0o1, 0o1, 13, 0, 0, 0, time.UTC))
	endTime := timestamppb.New(time.Date(2023, 0o1, 0o2, 13, 0, 0, 0, time.UTC))
	jobConfigStr := "EXECUTION_PROJECT=example_project,ANOTHER_CONFIG=example_value"
	jobConfig := map[string]string{"EXECUTION_PROJECT": "example_project", "ANOTHER_CONFIG": "example_value"}
	description := "sample backfill"
	backfillID := uuid.New()
	updateTime := time.Now()

	validBackfill := func() *scheduler.Backfill {
		tnnt, _ := tenant.NewTenant(projectName, namespaceName)
		cfg := scheduler.NewBackfillConfig(
			startTime.AsTime(), endTime.AsTime(),
			jobConfig, map[string]string{},
			description, category, approvalID, userID,
		)
		return scheduler.NewBackfill(backfillID, jobName, tnnt, cfg, scheduler.BackfillStateCreated, time.Now(), updateTime, "")
	}

	t.Run("CreateBackfill", func(t *testing.T) {
		t.Run("returns error when tenant is invalid", func(t *testing.T) {
			service := new(mockBackfillService)
			handler := v1beta1.NewBackfilllHandler(logger, service)

			req := &pb.CreateBackfillRequest{
				JobName:       jobName.String(),
				NamespaceName: namespaceName,
				DataStartTime: startTime,
				DataEndTime:   endTime,
				Description:   description,
				Category:      category,
				ApprovalId:    approvalID,
				UserId:        userID,
			}

			result, err := handler.CreateBackfill(ctx, req)
			assert.Error(t, err)
			assert.Nil(t, result)
		})

		t.Run("returns error when job name is empty", func(t *testing.T) {
			service := new(mockBackfillService)
			handler := v1beta1.NewBackfilllHandler(logger, service)

			req := &pb.CreateBackfillRequest{
				ProjectName:   projectName,
				NamespaceName: namespaceName,
				DataStartTime: startTime,
				DataEndTime:   endTime,
				Description:   description,
				Category:      category,
				ApprovalId:    approvalID,
				UserId:        userID,
			}

			result, err := handler.CreateBackfill(ctx, req)
			assert.Error(t, err)
			assert.Nil(t, result)
		})

		t.Run("returns error when start time is nil", func(t *testing.T) {
			service := new(mockBackfillService)
			handler := v1beta1.NewBackfilllHandler(logger, service)

			req := &pb.CreateBackfillRequest{
				ProjectName:   projectName,
				NamespaceName: namespaceName,
				JobName:       jobName.String(),
				DataEndTime:   endTime,
				Description:   description,
				Category:      category,
				ApprovalId:    approvalID,
				UserId:        userID,
			}

			result, err := handler.CreateBackfill(ctx, req)
			assert.Error(t, err)
			assert.Nil(t, result)
		})

		t.Run("returns error when end time is invalid", func(t *testing.T) {
			service := new(mockBackfillService)
			handler := v1beta1.NewBackfilllHandler(logger, service)

			req := &pb.CreateBackfillRequest{
				ProjectName:   projectName,
				NamespaceName: namespaceName,
				JobName:       jobName.String(),
				DataStartTime: startTime,
				DataEndTime:   timestamppb.New(time.Date(-1, 13, 2, 13, 0, 0, 0, time.UTC)),
				Description:   description,
				Category:      category,
				ApprovalId:    approvalID,
				UserId:        userID,
			}

			result, err := handler.CreateBackfill(ctx, req)
			assert.Error(t, err)
			assert.Nil(t, result)
		})

		t.Run("returns error when approval ID is empty", func(t *testing.T) {
			service := new(mockBackfillService)
			handler := v1beta1.NewBackfilllHandler(logger, service)

			req := &pb.CreateBackfillRequest{
				ProjectName:   projectName,
				NamespaceName: namespaceName,
				JobName:       jobName.String(),
				DataStartTime: startTime,
				DataEndTime:   endTime,
				Description:   description,
				Category:      category,
				UserId:        userID,
			}

			result, err := handler.CreateBackfill(ctx, req)
			assert.Error(t, err)
			assert.Nil(t, result)
		})

		t.Run("returns error when user ID is empty", func(t *testing.T) {
			service := new(mockBackfillService)
			handler := v1beta1.NewBackfilllHandler(logger, service)

			req := &pb.CreateBackfillRequest{
				ProjectName:   projectName,
				NamespaceName: namespaceName,
				JobName:       jobName.String(),
				DataStartTime: startTime,
				DataEndTime:   endTime,
				Description:   description,
				Category:      category,
				ApprovalId:    approvalID,
			}

			result, err := handler.CreateBackfill(ctx, req)
			assert.Error(t, err)
			assert.Nil(t, result)
		})

		t.Run("returns error when category is invalid", func(t *testing.T) {
			service := new(mockBackfillService)
			handler := v1beta1.NewBackfilllHandler(logger, service)

			req := &pb.CreateBackfillRequest{
				ProjectName:   projectName,
				NamespaceName: namespaceName,
				JobName:       jobName.String(),
				DataStartTime: startTime,
				DataEndTime:   endTime,
				Description:   description,
				Category:      "INVALID_CATEGORY",
				ApprovalId:    approvalID,
				UserId:        userID,
			}

			result, err := handler.CreateBackfill(ctx, req)
			assert.Error(t, err)
			assert.Nil(t, result)
		})

		t.Run("returns error when description is empty", func(t *testing.T) {
			service := new(mockBackfillService)
			handler := v1beta1.NewBackfilllHandler(logger, service)

			req := &pb.CreateBackfillRequest{
				ProjectName:   projectName,
				NamespaceName: namespaceName,
				JobName:       jobName.String(),
				DataStartTime: startTime,
				DataEndTime:   endTime,
				Category:      category,
				ApprovalId:    approvalID,
				UserId:        userID,
			}

			result, err := handler.CreateBackfill(ctx, req)
			assert.Error(t, err)
			assert.Nil(t, result)
		})

		t.Run("returns error when job config is malformed", func(t *testing.T) {
			service := new(mockBackfillService)
			handler := v1beta1.NewBackfilllHandler(logger, service)

			req := &pb.CreateBackfillRequest{
				ProjectName:   projectName,
				NamespaceName: namespaceName,
				JobName:       jobName.String(),
				DataStartTime: startTime,
				DataEndTime:   endTime,
				Description:   description,
				Category:      category,
				ApprovalId:    approvalID,
				UserId:        userID,
				JobConfig:     "BADFORMAT",
			}

			result, err := handler.CreateBackfill(ctx, req)
			assert.Error(t, err)
			assert.Nil(t, result)
		})

		t.Run("returns error when service returns error", func(t *testing.T) {
			service := new(mockBackfillService)
			defer service.AssertExpectations(t)
			handler := v1beta1.NewBackfilllHandler(logger, service)

			req := &pb.CreateBackfillRequest{
				ProjectName:   projectName,
				NamespaceName: namespaceName,
				JobName:       jobName.String(),
				DataStartTime: startTime,
				DataEndTime:   endTime,
				JobConfig:     jobConfigStr,
				Description:   description,
				Category:      category,
				ApprovalId:    approvalID,
				UserId:        userID,
			}

			service.On("CreateBackfill", ctx, mock.AnythingOfType("*scheduler.Backfill")).
				Return(uuid.Nil, "", errors.New("internal error"))

			result, err := handler.CreateBackfill(ctx, req)
			assert.ErrorContains(t, err, "internal error")
			assert.Nil(t, result)
		})

		t.Run("returns backfill ID and dag run ID when successful", func(t *testing.T) {
			service := new(mockBackfillService)
			defer service.AssertExpectations(t)
			handler := v1beta1.NewBackfilllHandler(logger, service)

			req := &pb.CreateBackfillRequest{
				ProjectName:   projectName,
				NamespaceName: namespaceName,
				JobName:       jobName.String(),
				DataStartTime: startTime,
				DataEndTime:   endTime,
				JobConfig:     jobConfigStr,
				Description:   description,
				Category:      category,
				ApprovalId:    approvalID,
				UserId:        userID,
			}
			dagRunID := "dag-run-123"

			service.On("CreateBackfill", ctx, mock.AnythingOfType("*scheduler.Backfill")).
				Return(backfillID, dagRunID, nil)

			result, err := handler.CreateBackfill(ctx, req)
			assert.NoError(t, err)
			assert.NotNil(t, result)
			assert.Equal(t, backfillID.String(), result.Id)
			assert.Equal(t, dagRunID, result.DagRunId)
		})

		t.Run("returns backfill ID when successful without job config", func(t *testing.T) {
			service := new(mockBackfillService)
			defer service.AssertExpectations(t)
			handler := v1beta1.NewBackfilllHandler(logger, service)

			req := &pb.CreateBackfillRequest{
				ProjectName:   projectName,
				NamespaceName: namespaceName,
				JobName:       jobName.String(),
				DataStartTime: startTime,
				DataEndTime:   endTime,
				Description:   description,
				Category:      category,
				ApprovalId:    approvalID,
				UserId:        userID,
			}
			dagRunID := "dag-run-456"

			service.On("CreateBackfill", ctx, mock.AnythingOfType("*scheduler.Backfill")).
				Return(backfillID, dagRunID, nil)

			result, err := handler.CreateBackfill(ctx, req)
			assert.NoError(t, err)
			assert.NotNil(t, result)
			assert.Equal(t, backfillID.String(), result.Id)
		})
	})

	t.Run("BackfillDryRun", func(t *testing.T) {
		t.Run("returns error when tenant is invalid", func(t *testing.T) {
			service := new(mockBackfillService)
			handler := v1beta1.NewBackfilllHandler(logger, service)

			req := &pb.BackfillPreviewRequest{
				JobName:       jobName.String(),
				NamespaceName: namespaceName,
				DataStartTime: startTime,
				DataEndTime:   endTime,
				Description:   description,
				Category:      category,
				ApprovalId:    approvalID,
				UserId:        userID,
			}

			result, err := handler.BackfillPreview(ctx, req)
			assert.Error(t, err)
			assert.Nil(t, result)
		})

		t.Run("returns error when service returns error", func(t *testing.T) {
			service := new(mockBackfillService)
			defer service.AssertExpectations(t)
			handler := v1beta1.NewBackfilllHandler(logger, service)

			req := &pb.BackfillPreviewRequest{
				ProjectName:   projectName,
				NamespaceName: namespaceName,
				JobName:       jobName.String(),
				DataStartTime: startTime,
				DataEndTime:   endTime,
				JobConfig:     jobConfigStr,
				Description:   description,
				Category:      category,
				ApprovalId:    approvalID,
				UserId:        userID,
			}

			service.On("BackfillDryRun", ctx, mock.AnythingOfType("*scheduler.Backfill")).
				Return(nil, errors.New("internal error"))

			result, err := handler.BackfillPreview(ctx, req)
			assert.ErrorContains(t, err, "internal error")
			assert.Nil(t, result)
		})

		t.Run("returns compiled assets when successful", func(t *testing.T) {
			service := new(mockBackfillService)
			defer service.AssertExpectations(t)
			handler := v1beta1.NewBackfilllHandler(logger, service)

			req := &pb.BackfillPreviewRequest{
				ProjectName:   projectName,
				NamespaceName: namespaceName,
				JobName:       jobName.String(),
				DataStartTime: startTime,
				DataEndTime:   endTime,
				JobConfig:     jobConfigStr,
				Description:   description,
				Category:      category,
				ApprovalId:    approvalID,
				UserId:        userID,
			}
			executorInput := &scheduler.ExecutorInput{
				Configs: scheduler.ConfigMap{"KEY": "VALUE"},
				Files:   scheduler.ConfigMap{"query.sql": "SELECT 1"},
				Secrets: scheduler.ConfigMap{"SECRET_KEY": "secret"},
			}

			service.On("BackfillDryRun", ctx, mock.AnythingOfType("*scheduler.Backfill")).
				Return(executorInput, nil)

			result, err := handler.BackfillPreview(ctx, req)
			assert.NoError(t, err)
			assert.NotNil(t, result)
			assert.NotNil(t, result.CompiledAssets)
			assert.Equal(t, map[string]string(executorInput.Configs), result.CompiledAssets.Envs)
			assert.Equal(t, map[string]string(executorInput.Files), result.CompiledAssets.Files)
		})
	})

	t.Run("CancelBackfill", func(t *testing.T) {
		t.Run("returns error when canceled_by is empty", func(t *testing.T) {
			service := new(mockBackfillService)
			handler := v1beta1.NewBackfilllHandler(logger, service)

			req := &pb.CancelBackfillRequest{
				BackfillId: backfillID.String(),
				CanceledBy: "",
			}

			result, err := handler.CancelBackfill(ctx, req)
			assert.ErrorContains(t, err, "canceled_by cannot be empty")
			assert.Nil(t, result)
		})

		t.Run("returns error when backfill ID is not a valid UUID", func(t *testing.T) {
			service := new(mockBackfillService)
			handler := v1beta1.NewBackfilllHandler(logger, service)

			req := &pb.CancelBackfillRequest{
				BackfillId: "not-a-uuid",
				CanceledBy: "user-abc",
			}

			result, err := handler.CancelBackfill(ctx, req)
			assert.Error(t, err)
			assert.Nil(t, result)
		})

		t.Run("returns error when service cancel fails", func(t *testing.T) {
			service := new(mockBackfillService)
			defer service.AssertExpectations(t)
			handler := v1beta1.NewBackfilllHandler(logger, service)

			req := &pb.CancelBackfillRequest{
				BackfillId: backfillID.String(),
				CanceledBy: "user-abc",
			}

			service.On("CancelBackfill", ctx, backfillID, "user-abc").
				Return(errors.New("cancel error"))

			result, err := handler.CancelBackfill(ctx, req)
			assert.ErrorContains(t, err, "cancel error")
			assert.Nil(t, result)
		})

		t.Run("returns error when get backfill by ID fails after cancel", func(t *testing.T) {
			service := new(mockBackfillService)
			defer service.AssertExpectations(t)
			handler := v1beta1.NewBackfilllHandler(logger, service)

			req := &pb.CancelBackfillRequest{
				BackfillId: backfillID.String(),
				CanceledBy: "user-abc",
			}

			service.On("CancelBackfill", ctx, backfillID, "user-abc").Return(nil)
			service.On("GetBackfillByID", ctx, backfillID).Return(nil, errors.New("not found"))

			result, err := handler.CancelBackfill(ctx, req)
			assert.ErrorContains(t, err, "not found")
			assert.Nil(t, result)
		})

		t.Run("returns cancelled backfill when successful", func(t *testing.T) {
			service := new(mockBackfillService)
			defer service.AssertExpectations(t)
			handler := v1beta1.NewBackfilllHandler(logger, service)

			req := &pb.CancelBackfillRequest{
				BackfillId: backfillID.String(),
				CanceledBy: "user-abc",
			}

			bf := validBackfill()
			service.On("CancelBackfill", ctx, backfillID, "user-abc").Return(nil)
			service.On("GetBackfillByID", ctx, backfillID).Return(bf, nil)

			result, err := handler.CancelBackfill(ctx, req)
			assert.NoError(t, err)
			assert.NotNil(t, result)
			assert.NotNil(t, result.Backfill)
			assert.Equal(t, backfillID.String(), result.Backfill.Id)
		})
	})

	t.Run("GetBackfill", func(t *testing.T) {
		t.Run("returns error when project name is empty", func(t *testing.T) {
			service := new(mockBackfillService)
			handler := v1beta1.NewBackfilllHandler(logger, service)

			req := &pb.GetBackfillRequest{
				ProjectName: "",
			}

			result, err := handler.GetBackfill(ctx, req)
			assert.Error(t, err)
			assert.Nil(t, result)
		})

		t.Run("returns error when service returns error", func(t *testing.T) {
			service := new(mockBackfillService)
			defer service.AssertExpectations(t)
			handler := v1beta1.NewBackfilllHandler(logger, service)

			req := &pb.GetBackfillRequest{
				ProjectName: projectName,
			}

			service.On("GetBackfills", ctx, tenant.ProjectName(projectName), mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
				Return(nil, errors.New("internal error"))

			result, err := handler.GetBackfill(ctx, req)
			assert.ErrorContains(t, err, "internal error")
			assert.Nil(t, result)
		})

		t.Run("returns empty list when no backfills found", func(t *testing.T) {
			service := new(mockBackfillService)
			defer service.AssertExpectations(t)
			handler := v1beta1.NewBackfilllHandler(logger, service)

			req := &pb.GetBackfillRequest{
				ProjectName: projectName,
			}

			service.On("GetBackfills", ctx, tenant.ProjectName(projectName), mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
				Return([]*scheduler.Backfill{}, nil)

			result, err := handler.GetBackfill(ctx, req)
			assert.NoError(t, err)
			assert.Empty(t, result.Backfills)
		})

		t.Run("returns backfill list when successful", func(t *testing.T) {
			service := new(mockBackfillService)
			defer service.AssertExpectations(t)
			handler := v1beta1.NewBackfilllHandler(logger, service)

			req := &pb.GetBackfillRequest{
				ProjectName: projectName,
				JobNames:    []string{jobName.String()},
				Status:      "created",
				ApprovalId:  approvalID,
				UserId:      userID,
			}

			bf := validBackfill()
			service.On("GetBackfills", ctx, tenant.ProjectName(projectName), mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
				Return([]*scheduler.Backfill{bf}, nil)

			result, err := handler.GetBackfill(ctx, req)
			assert.NoError(t, err)
			assert.Len(t, result.Backfills, 1)
			assert.Equal(t, backfillID.String(), result.Backfills[0].Id)
			assert.Equal(t, projectName, result.Backfills[0].ProjectName)
			assert.Equal(t, jobName.String(), result.Backfills[0].JobName)
		})
	})
}

type mockBackfillService struct {
	mock.Mock
}

func (_m *mockBackfillService) CreateBackfill(ctx context.Context, backfillReq *scheduler.Backfill) (uuid.UUID, string, error) {
	args := _m.Called(ctx, backfillReq)
	return args.Get(0).(uuid.UUID), args.Get(1).(string), args.Error(2)
}

func (_m *mockBackfillService) BackfillDryRun(ctx context.Context, backfillReq *scheduler.Backfill) (*scheduler.ExecutorInput, error) {
	args := _m.Called(ctx, backfillReq)

	var r0 *scheduler.ExecutorInput
	if rf, ok := args.Get(0).(func(context.Context, *scheduler.Backfill) *scheduler.ExecutorInput); ok {
		r0 = rf(ctx, backfillReq)
	} else {
		if args.Get(0) != nil {
			r0 = args.Get(0).(*scheduler.ExecutorInput)
		}
	}

	var r1 error
	if rf, ok := args.Get(1).(func(context.Context, *scheduler.Backfill) error); ok {
		r1 = rf(ctx, backfillReq)
	} else {
		r1 = args.Error(1)
	}

	return r0, r1
}

func (_m *mockBackfillService) GetBackfills(ctx context.Context, projectName tenant.ProjectName, filters ...filter.FilterOpt) ([]*scheduler.Backfill, error) {
	args := []interface{}{ctx, projectName}
	for _, f := range filters {
		args = append(args, f)
	}
	ret := _m.Called(args...)

	var r0 []*scheduler.Backfill
	if rf, ok := ret.Get(0).(func(context.Context, tenant.ProjectName, ...filter.FilterOpt) []*scheduler.Backfill); ok {
		r0 = rf(ctx, projectName, filters...)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]*scheduler.Backfill)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, tenant.ProjectName, ...filter.FilterOpt) error); ok {
		r1 = rf(ctx, projectName, filters...)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

func (_m *mockBackfillService) GetBackfillByID(ctx context.Context, backfillID uuid.UUID) (*scheduler.Backfill, error) {
	ret := _m.Called(ctx, backfillID)

	var r0 *scheduler.Backfill
	if rf, ok := ret.Get(0).(func(context.Context, uuid.UUID) *scheduler.Backfill); ok {
		r0 = rf(ctx, backfillID)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*scheduler.Backfill)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, uuid.UUID) error); ok {
		r1 = rf(ctx, backfillID)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

func (_m *mockBackfillService) CancelBackfill(ctx context.Context, backfillID uuid.UUID, canceledBy string) error {
	ret := _m.Called(ctx, backfillID, canceledBy)

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, uuid.UUID, string) error); ok {
		r0 = rf(ctx, backfillID, canceledBy)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}
