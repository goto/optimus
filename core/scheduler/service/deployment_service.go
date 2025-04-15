package service

import (
	"context"

	"go.opentelemetry.io/otel"

	"github.com/goto/optimus/core/job"
	"github.com/goto/optimus/core/scheduler"
	"github.com/goto/optimus/core/tenant"
	"github.com/goto/optimus/internal/errors"
)

func (s *JobRunService) UploadToScheduler(ctx context.Context, projectName tenant.ProjectName) error {
	spanCtx, span := otel.Tracer("optimus").Start(ctx, "UploadToScheduler")
	defer span.End()

	me := errors.NewMultiError("errorInUploadToScheduler")
	allJobsWithDetails, err := s.jobRepo.GetAll(spanCtx, projectName)
	me.Append(err)
	if allJobsWithDetails == nil {
		return me.ToErr()
	}
	span.AddEvent("got all the jobs to upload")

	err = s.priorityResolver.Resolve(spanCtx, allJobsWithDetails)
	if err != nil {
		s.l.Error("error resolving priority: %s", err)
		me.Append(err)
		return me.ToErr()
	}
	span.AddEvent("done with priority resolution")

	jobGroupByTenant := scheduler.GroupJobsByTenant(allJobsWithDetails)
	for t, jobs := range jobGroupByTenant {
		span.AddEvent("uploading job specs")
		if err = s.deployJobsPerNamespace(spanCtx, t, jobs); err == nil {
			s.l.Info("[success] namespace: %s, project: %s, deployed", t.NamespaceName().String(), t.ProjectName().String())
		}
		me.Append(err)

		span.AddEvent("uploading job metrics")
	}
	return me.ToErr()
}

func (s *JobRunService) deployJobsPerNamespace(ctx context.Context, t tenant.Tenant, jobs []*scheduler.JobWithDetails) error {
	err := s.scheduler.DeployJobs(ctx, t, jobs)
	if err != nil {
		s.l.Error("error deploying jobs under project [%s] namespace [%s]: %s", t.ProjectName().String(), t.NamespaceName().String(), err)
		return err
	}
	return s.cleanPerNamespace(ctx, t, jobs)
}

func (s *JobRunService) cleanPerNamespace(ctx context.Context, t tenant.Tenant, jobs []*scheduler.JobWithDetails) error {
	// get all stored job names
	schedulerJobNames, err := s.scheduler.ListJobs(ctx, t)
	if err != nil {
		s.l.Error("error listing jobs under project [%s] namespace [%s]: %s", t.ProjectName().String(), t.NamespaceName().String(), err)
		return err
	}
	jobNamesMap := make(map[string]struct{})
	for _, job := range jobs {
		jobNamesMap[job.Name.String()] = struct{}{}
	}
	var jobsToDelete []string

	for _, schedulerJobName := range schedulerJobNames {
		if _, ok := jobNamesMap[schedulerJobName]; !ok {
			jobsToDelete = append(jobsToDelete, schedulerJobName)
		}
	}
	return s.scheduler.DeleteJobs(ctx, t, jobsToDelete)
}

func (s *JobRunService) UpdateJobScheduleState(ctx context.Context, tnnt tenant.Tenant, jobName []job.Name, state job.State) error {
	return s.scheduler.UpdateJobState(ctx, tnnt, jobName, state.String())
}

func (s *JobRunService) GetJobSchedulerState(ctx context.Context, tnnt tenant.Tenant) (map[string]bool, error) {
	return s.scheduler.GetJobState(ctx, tnnt)
}

func (s *JobRunService) UploadJobs(ctx context.Context, tnnt tenant.Tenant, toUpdate, toDelete []string) (err error) {
	me := errors.NewMultiError("errorInUploadJobs")

	if len(toUpdate) > 0 {
		deployedJobs, err := s.resolveAndDeployJobs(ctx, tnnt, toUpdate)
		if len(deployedJobs) > 0 && err == nil {
			s.l.Info("[success] namespace: %s, project: %s, deployed %d jobs", tnnt.NamespaceName().String(),
				tnnt.ProjectName().String(), len(deployedJobs))
		}
		if len(deployedJobs) > 0 && err != nil {
			s.l.Error("[failure] namespace: %s, project: %s, deployed %d jobs with failure: %s", tnnt.NamespaceName().String(),
				tnnt.ProjectName().String(), len(deployedJobs), err.Error())
		}
		me.Append(err)
	}

	if len(toDelete) > 0 {
		if err = s.scheduler.DeleteJobs(ctx, tnnt, toDelete); err == nil {
			s.l.Info("deleted %s jobs on project: %s", len(toDelete), tnnt.ProjectName())
		}
		me.Append(err)
	}

	return me.ToErr()
}

func (s *JobRunService) resolveAndDeployJobs(ctx context.Context, tnnt tenant.Tenant, toUpdate []string) ([]*scheduler.JobWithDetails, error) {
	me := errors.NewMultiError("errorInResolveAndDeployJobs")
	allJobsWithDetails, err := s.jobRepo.GetJobs(ctx, tnnt.ProjectName(), toUpdate)
	if err != nil {
		if allJobsWithDetails == nil {
			return nil, err
		}
		me.Append(err)
	}

	if err = s.priorityResolver.Resolve(ctx, allJobsWithDetails); err != nil {
		s.l.Error("error priority resolving jobs: %s", err)
		me.Append(err)
		return nil, me.ToErr()
	}

	if err = s.scheduler.DeployJobs(ctx, tnnt, allJobsWithDetails); err != nil {
		me.Append(err)
	}
	return allJobsWithDetails, me.ToErr()
}
