package scheduler

import (
	"context"

	"github.com/goto/optimus/core/tenant"
	"github.com/goto/optimus/ext/scheduler/airflowx/client"
	"github.com/goto/optimus/ext/scheduler/airflowx/compiler"
	"github.com/goto/optimus/ext/scheduler/filesystem"
)

// Helper, will be removed
type SchedulerFactory interface {
	New(ctx context.Context, tnnt tenant.Tenant) *AirflowScheduler
}

type TenantDetailsGetter interface {
	GetDetails(ctx context.Context, jobTenant tenant.Tenant) (*tenant.WithDetails, error)
}

type schedulerFactory struct {
	tenantDetailsGetter TenantDetailsGetter
}

func NewSchedulerFactory(tenantDetailsGetter TenantDetailsGetter) SchedulerFactory {
	return &schedulerFactory{
		tenantDetailsGetter: tenantDetailsGetter,
	}
}

func (s *schedulerFactory) New(ctx context.Context, tnnt tenant.Tenant) *AirflowScheduler {
	tnntWithDetails, _ := s.tenantDetailsGetter.GetDetails(ctx, tnnt)
	schedulerName, _ := tnntWithDetails.GetConfig("scheduler_name")
	schedulerVersion, _ := tnntWithDetails.GetConfig("scheduler_version")

	switch {
	case schedulerName == "airflow" && schedulerVersion == "2.1.4":
		// TODO: instantiate airflow 2.1.4 specific bucket, compiler, and client
		return s.newAirflow214()
	case schedulerName == "airflow" && schedulerVersion == "2.4.3":
		// TODO: instantiate airflow 2.4.3 specific bucket, compiler, and client
		return s.newAirflow243()
	}
	return &AirflowScheduler{}
}

func (s *schedulerFactory) newAirflow214() *AirflowScheduler {
	return &AirflowScheduler{
		fsFactory: &filesystem.Factory{},
		compiler:  compiler.NewDAGCompiler214(),
		client:    client.NewAirflowClientV2(),
	}
}

func (s *schedulerFactory) newAirflow243() *AirflowScheduler {
	return &AirflowScheduler{
		fsFactory: &filesystem.Factory{},
		compiler:  compiler.NewDAGCompiler243(),
		client:    client.NewAirflowClientV2(),
	}
}
