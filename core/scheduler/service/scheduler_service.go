package service

import (
	"context"

	"github.com/goto/optimus/core/tenant"
)

type SchedulerService struct {
	scheduler Scheduler
}

func (s *SchedulerService) CreateSchedulerRole(ctx context.Context, t tenant.Tenant, roleName string) error {
	return s.scheduler.AddRole(ctx, t, roleName, true)
}

func (s *SchedulerService) GetRolePermissions(ctx context.Context, t tenant.Tenant, roleName string) ([]string, error) {
	return s.scheduler.GetRolePermissions(ctx, t, roleName)
}

func NewSchedulerService(scheduler Scheduler) *SchedulerService {
	return &SchedulerService{
		scheduler: scheduler,
	}
}
