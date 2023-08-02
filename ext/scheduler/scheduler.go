package scheduler

import (
	"context"
	"time"

	"github.com/goto/optimus/core/job"
	"github.com/goto/optimus/core/scheduler"
	"github.com/goto/optimus/core/tenant"
	"github.com/goto/optimus/ext/scheduler/airflowx/client"
	"github.com/goto/optimus/ext/scheduler/airflowx/compiler"
	"github.com/goto/optimus/ext/scheduler/filesystem"
	"github.com/goto/optimus/internal/lib/cron"
)

// Helper, will be removed
var _ SchedulerFS = (filesystem.SchedulerFS)(nil)
var _ SchedulerCompiler = (compiler.AirflowCompiler)(nil)
var _ SchedulerClient = (client.AirflowClient)(nil)

// Helper, will be removed
type SchedulerFS interface {
	Write(ctx context.Context, path string, data []byte) error
	List(dirPath string) []string
	Delete(ctx context.Context, path string) error
}

// Helper, will be removed
type SchedulerCompiler interface {
	Compile(ctx context.Context, jobWithDetails *scheduler.JobWithDetails) ([]byte, error)
}

// Helper, will be removed
type SchedulerClient interface {
	UpdateJobsState(ctx context.Context, tnnt tenant.Tenant, jobNames []scheduler.JobName, state string) error
	GetJobRuns(ctx context.Context, tnnt tenant.Tenant, jobQuery *scheduler.JobRunsCriteria, jobCron *cron.ScheduleSpec) ([]*scheduler.JobRunStatus, error)
	Clear(ctx context.Context, t tenant.Tenant, jobName scheduler.JobName, executionTime time.Time) error
	ClearBatch(ctx context.Context, tnnt tenant.Tenant, jobName scheduler.JobName, startExecutionTime, endExecutionTime time.Time) error
	CreateRun(ctx context.Context, tnnt tenant.Tenant, jobName scheduler.JobName, executionTime time.Time, dagRunIDPrefix string) error
}

// Helper, will be removed
type Airflow interface {
	// Compile and transfer the compiled job to defined location
	DeployJobs(ctx context.Context, tenant tenant.Tenant, jobs []*scheduler.JobWithDetails) error
	// Show the jobs from the scheduler filesystem (bucket)
	ListJobs(ctx context.Context, t tenant.Tenant) ([]string, error)
	// Delete jobs from the scheduler based on jobNames
	DeleteJobs(ctx context.Context, t tenant.Tenant, jobNames []string) error
	// Update the job state for pausing / unpausing dag
	UpdateJobState(ctx context.Context, tnnt tenant.Tenant, jobNames []job.Name, state string) error
	// Get job runs from airflow given job runs criteria
	GetJobRuns(ctx context.Context, tnnt tenant.Tenant, jobQuery *scheduler.JobRunsCriteria, jobCron *cron.ScheduleSpec) ([]*scheduler.JobRunStatus, error)
	// Clear dag on scheduler based on job name and execution time
	Clear(ctx context.Context, t tenant.Tenant, jobName scheduler.JobName, executionTime time.Time) error
	// Clear dag in batch mode
	ClearBatch(ctx context.Context, tnnt tenant.Tenant, jobName scheduler.JobName, startExecutionTime, endExecutionTime time.Time) error
	// Create run on scheduler
	CreateRun(ctx context.Context, tnnt tenant.Tenant, jobName scheduler.JobName, executionTime time.Time, dagRunIDPrefix string) error
}
