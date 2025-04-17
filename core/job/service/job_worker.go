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

func groupByTenant(jobs job.Jobs) map[tenant.Tenant]job.Jobs {
	grouped := make(map[tenant.Tenant]job.Jobs)
	for _, j := range jobs {
		tnnt := j.Tenant()
		grouped[tnnt] = append(grouped[tnnt], j)
	}
	return grouped
}

func groupByProject(jobs []*job.Job) map[tenant.ProjectName][]*job.Job {
	grouped := make(map[tenant.ProjectName][]*job.Job)
	for _, j := range jobs {
		tnnt := j.Tenant()
		grouped[tnnt.ProjectName()] = append(grouped[tnnt.ProjectName()], j)
	}
	return grouped
}

func (w *JobWorker) UpdateStateInStore(ctx context.Context, jobs job.Jobs, state job.State) error {
	multierror := errors.NewMultiError("UpdateStateInStore")
	grouped := groupByTenant(jobs)
	for t, js := range grouped {
		if len(js) == 0 {
			continue
		}
		multierror.Append(w.repo.UpdateState(ctx, t, js.GetJobNames(), state, fmt.Sprintf("state modified by 'scheduler state sync worker' at time: %s", time.Now().String())))
	}
	return multierror.ToErr()
}

func (w *JobWorker) SyncJobStatusByProject(ctx context.Context, projectName tenant.ProjectName, jobs []*job.Job) error {
	start := time.Now()
	jobSchedulerStates, err := w.jobDeploymentService.GetJobSchedulerState(ctx, projectName)
	if err != nil {
		w.logger.Info(fmt.Sprintf("[SyncJobStatus] Project: %s, failed fetch jobs status from scheduler, time taken: %s", projectName.String(), time.Since(start).String()))
		return err
	}
	w.logger.Info(fmt.Sprintf("[SyncJobStatus] Project: %s, fetched jobs status from scheduler, time taken: %s", projectName.String(), time.Since(start).String()))
	var toDisable, toEnable job.Jobs
	for _, j := range jobs {
		if isPaused, ok := jobSchedulerStates[j.GetName()]; ok {
			if j.IsDisabled() != isPaused {
				if isPaused {
					toDisable = append(toDisable, j)
				} else {
					toEnable = append(toEnable, j)
				}
			}
		}
	}
	multierror := errors.NewMultiError("SyncJobStatusByProject")

	if len(toDisable) > 0 {
		w.logger.Info(fmt.Sprintf("[SyncJobStatus] Job Status changed to disabled on scheduler jobs: %s", strings.Join(toDisable.GetJobNamesSring(), ", ")))
		err = w.UpdateStateInStore(ctx, toDisable, job.DISABLED)
		multierror.Append(err)
	}
	if len(toEnable) > 0 {
		w.logger.Info(fmt.Sprintf("[SyncJobStatus] Job Status changed to enabled on scheduler jobs: %s", strings.Join(toEnable.GetJobNamesSring(), ", ")))
		err = w.UpdateStateInStore(ctx, toEnable, job.ENABLED)
		multierror.Append(err)
	}
	return multierror.ToErr()
}

func (w *JobWorker) SyncJobStatus(ctx context.Context, statusSyncInterval int) {
	w.logger.Info(fmt.Sprintf("[SyncJobStatus] Starting worker with sync interval: %d", statusSyncInterval))
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
		jobsByProject := groupByProject(allJobs)

		for projectName, jobs := range jobsByProject {
			err = w.SyncJobStatusByProject(ctx, projectName, jobs)
			if err != nil {
				w.logger.Error(fmt.Sprintf("[SyncJobStatus] SyncJobStatusByProject failed for Project: %s, err:%s", projectName, err.Error()))
			}
		}

		w.logger.Info(fmt.Sprintf("[SyncJobStatus] finished syncing jobs status, Total Time: %s", time.Since(start).String()))
	}
}
