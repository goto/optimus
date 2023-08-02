package scheduler

import (
	"context"
	"time"

	"github.com/goto/optimus/core/job"
	"github.com/goto/optimus/core/scheduler"
	"github.com/goto/optimus/core/tenant"
	"github.com/goto/optimus/ext/scheduler/filesystem"
	"github.com/goto/optimus/internal/lib/cron"
)

// Helper, will be removed
var _ Airflow = (*AirflowScheduler)(nil)

type AirflowScheduler struct {
	fsFactory *filesystem.Factory
	compiler  SchedulerCompiler
	client    SchedulerClient
}

func (*AirflowScheduler) DeployJobs(ctx context.Context, tenant tenant.Tenant, jobs []*scheduler.JobWithDetails) error {
	// TODO: implement here
	return nil
}

func (*AirflowScheduler) ListJobs(ctx context.Context, t tenant.Tenant) ([]string, error) {
	// TODO: implement here
	return nil, nil
}

func (*AirflowScheduler) DeleteJobs(ctx context.Context, t tenant.Tenant, jobNames []string) error {
	// TODO: implement here
	return nil
}

func (*AirflowScheduler) UpdateJobState(ctx context.Context, tnnt tenant.Tenant, jobNames []job.Name, state string) error {
	// TODO: implement here
	return nil
}

func (*AirflowScheduler) GetJobRuns(ctx context.Context, tnnt tenant.Tenant, jobQuery *scheduler.JobRunsCriteria, jobCron *cron.ScheduleSpec) ([]*scheduler.JobRunStatus, error) {
	// TODO: implement here
	return nil, nil
}

func (*AirflowScheduler) Clear(ctx context.Context, t tenant.Tenant, jobName scheduler.JobName, executionTime time.Time) error {
	// TODO: implement here
	return nil
}

func (*AirflowScheduler) ClearBatch(ctx context.Context, tnnt tenant.Tenant, jobName scheduler.JobName, startExecutionTime, endExecutionTime time.Time) error {
	// TODO: implement here
	return nil
}

func (*AirflowScheduler) CreateRun(ctx context.Context, tnnt tenant.Tenant, jobName scheduler.JobName, executionTime time.Time, dagRunIDPrefix string) error {
	// TODO: implement here
	return nil
}
