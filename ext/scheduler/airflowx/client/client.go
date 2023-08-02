package client

import (
	"context"
	"time"

	"github.com/goto/optimus/core/scheduler"
	"github.com/goto/optimus/core/tenant"
	"github.com/goto/optimus/internal/lib/cron"
)

type AirflowClient interface {
	UpdateJobsState(ctx context.Context, tnnt tenant.Tenant, jobNames []scheduler.JobName, state string) error
	GetJobRuns(ctx context.Context, tnnt tenant.Tenant, jobQuery *scheduler.JobRunsCriteria, jobCron *cron.ScheduleSpec) ([]*scheduler.JobRunStatus, error)
	Clear(ctx context.Context, t tenant.Tenant, jobName scheduler.JobName, executionTime time.Time) error
	ClearBatch(ctx context.Context, tnnt tenant.Tenant, jobName scheduler.JobName, startExecutionTime, endExecutionTime time.Time) error
	CreateRun(ctx context.Context, tnnt tenant.Tenant, jobName scheduler.JobName, executionTime time.Time, dagRunIDPrefix string) error
}
