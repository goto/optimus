package service

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/goto/salt/log"

	"github.com/goto/optimus/core/job"
	"github.com/goto/optimus/core/tenant"
	"github.com/goto/optimus/internal/errors"
)

type JobWorker struct {
	logger               log.Logger
	repo                 JobRepository
	jobDeploymentService JobDeploymentService
}

func NewJobWorker(logger log.Logger, repo JobRepository, jobDeploymentService JobDeploymentService) *JobWorker {
	return &JobWorker{
		logger:               logger,
		repo:                 repo,
		jobDeploymentService: jobDeploymentService,
	}
}

func groupByTenant(jobs []*job.Job) map[tenant.Tenant][]*job.Job {
	grouped := make(map[tenant.Tenant][]*job.Job)
	for _, j := range jobs {
		tnnt := j.Tenant()
		grouped[tnnt] = append(grouped[tnnt], j)
	}
	return grouped
}

func (w *JobWorker) SyncJobStatusByTenant(ctx context.Context, tnnt tenant.Tenant, jobs []*job.Job) error {
	jobSchedulerStates, err := w.jobDeploymentService.GetJobSchedulerState(ctx, tnnt)
	if err != nil {
		return err
	}
	var toDisable, toEnable job.Jobs
	for _, j := range jobs {
		if isPaused, ok := jobSchedulerStates[j.FullName()]; ok {
			if j.IsDisabled() != isPaused {
				if isPaused {
					toDisable = append(toDisable, j)
				} else {
					toEnable = append(toEnable, j)
				}
			}
		}
	}
	multierror := errors.NewMultiError("SyncJobStatusByTenant")

	if len(toDisable) > 0 {
		w.logger.Info(fmt.Sprintf("[SyncJobStatus] Job Status changed to disabled on scheduler jobs: %s", strings.Join(toDisable.GetJobNamesSring(), ", ")))
		err = w.jobDeploymentService.UpdateJobScheduleState(ctx, tnnt, toDisable.GetJobNames(), job.DISABLED)
		multierror.Append(err)
	}
	if len(toEnable) > 0 {
		w.logger.Info(fmt.Sprintf("[SyncJobStatus] Job Status changed to enabled on scheduler jobs: %s", strings.Join(toEnable.GetJobNamesSring(), ", ")))
		err = w.jobDeploymentService.UpdateJobScheduleState(ctx, tnnt, toEnable.GetJobNames(), job.ENABLED)
		multierror.Append(err)
	}
	return multierror.ToErr()
}

func (w *JobWorker) SyncJobStatus(ctx context.Context, statusSyncInterval int64) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			time.Sleep(time.Duration(statusSyncInterval) * time.Minute)
		}

		start := time.Now()
		allJobs, err := w.repo.GetAll(ctx)
		if err != nil {
			w.logger.Error(fmt.Sprintf("[SyncJobStatus] failed to get all jobs, err:%s", err.Error()))
		}
		jobsByTenant := groupByTenant(allJobs)

		for tnnt, jobs := range jobsByTenant {
			err = w.SyncJobStatusByTenant(ctx, tnnt, jobs)
			w.logger.Error(fmt.Sprintf("[SyncJobStatus] SyncJobStatusByTenant failed for tenant: %s, err:%s", tnnt, err.Error()))
		}

		w.logger.Info(fmt.Sprintf("[SyncJobStatus] finished syncing jobs status, Total Time: %s", time.Since(start).String()))
	}
}
