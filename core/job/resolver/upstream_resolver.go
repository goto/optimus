package resolver

import (
	"context"
	"fmt"

	"github.com/goto/optimus/core/job"
	"github.com/goto/optimus/core/resource"
	"github.com/goto/optimus/core/tenant"
	"github.com/goto/optimus/internal/errors"
	"github.com/goto/optimus/internal/writer"
)

const (
	ConcurrentTicketPerSec = 50
	ConcurrentLimit        = 100
)

type UpstreamResolver struct {
	jobRepository            JobRepository
	externalUpstreamResolver ExternalUpstreamResolver
	internalUpstreamResolver InternalUpstreamResolver
}

func NewUpstreamResolver(jobRepository JobRepository, externalUpstreamResolver ExternalUpstreamResolver, internalUpstreamResolver InternalUpstreamResolver) *UpstreamResolver {
	return &UpstreamResolver{jobRepository: jobRepository, externalUpstreamResolver: externalUpstreamResolver, internalUpstreamResolver: internalUpstreamResolver}
}

type ExternalUpstreamResolver interface {
	Resolve(ctx context.Context, jobWithUpstream *job.WithUpstream, lw writer.LogWriter) (*job.WithUpstream, error)
	BulkResolve(context.Context, []*job.WithUpstream, writer.LogWriter) ([]*job.WithUpstream, error)
}

type InternalUpstreamResolver interface {
	Resolve(context.Context, *job.WithUpstream) (*job.WithUpstream, error)
	BulkResolve(context.Context, tenant.ProjectName, []*job.WithUpstream) ([]*job.WithUpstream, error)
}

type JobRepository interface {
	ResolveUpstreams(ctx context.Context, projectName tenant.ProjectName, jobNames []job.Name) (map[job.Name][]*job.Upstream, error)

	GetAllEnabledByResourceDestination(ctx context.Context, resourceDestination resource.URN) ([]*job.Job, error)
	GetEnabledJobByName(ctx context.Context, projectName tenant.ProjectName, jobName job.Name) (*job.Job, error)
}

func (u UpstreamResolver) CheckStaticResolvable(ctx context.Context, tnnt tenant.Tenant, incomingJobs []*job.Job, logWriter writer.LogWriter) error {
	me := errors.NewMultiError("check static resolvable incomingJobs errors")
	incomingJobNameMap := make(map[job.Name]*job.Job)
	for _, incomingJob := range incomingJobs {
		incomingJobNameMap[incomingJob.Spec().Name()] = incomingJob
	}

	jobsWithUnresolvedStaticUpstream, err := job.Jobs(incomingJobs).GetJobsWithUnresolvedStaticUpstreams()
	me.Append(err)

	jobsWithResolvedStaticInternalUpstreams, err := u.internalUpstreamResolver.BulkResolve(ctx, tnnt.ProjectName(), jobsWithUnresolvedStaticUpstream)
	me.Append(err)
	jobsWithResolvedStaticExternalUpstreams, err := u.externalUpstreamResolver.BulkResolve(ctx, jobsWithResolvedStaticInternalUpstreams, logWriter)
	me.Append(err)

	me.Append(checkForUnresolvedStaticUpstreams(tnnt, incomingJobNameMap, jobsWithResolvedStaticExternalUpstreams, logWriter))

	return me.ToErr()
}

func (u UpstreamResolver) BulkResolve(ctx context.Context, projectName tenant.ProjectName, jobs []*job.Job, logWriter writer.LogWriter) ([]*job.WithUpstream, error) {
	me := errors.NewMultiError("bulk resolve jobs errors")

	jobsWithUnresolvedUpstream, err := job.Jobs(jobs).GetJobsWithUnresolvedUpstreams()
	if err != nil {
		errorMsg := fmt.Sprintf("[%s] %s", jobs[0].Tenant().NamespaceName().String(), err.Error())
		logWriter.Write(writer.LogLevelError, errorMsg)
		me.Append(err)
	}

	jobsWithResolvedInternalUpstreams, err := u.internalUpstreamResolver.BulkResolve(ctx, projectName, jobsWithUnresolvedUpstream)
	if err != nil {
		errorMsg := fmt.Sprintf("unable to resolve upstream: %s", err.Error())
		logWriter.Write(writer.LogLevelError, errorMsg)
		me.Append(errors.NewError(errors.ErrInternalError, job.EntityJob, errorMsg))
		return nil, me.ToErr()
	}

	jobsWithResolvedExternalUpstreams, err := u.externalUpstreamResolver.BulkResolve(ctx, jobsWithResolvedInternalUpstreams, logWriter)
	me.Append(err)

	me.Append(u.getUnresolvedUpstreamsErrors(jobsWithResolvedExternalUpstreams, logWriter))

	return jobsWithResolvedExternalUpstreams, me.ToErr()
}

func (u UpstreamResolver) Resolve(ctx context.Context, subjectJob *job.Job, logWriter writer.LogWriter) ([]*job.Upstream, error) {
	me := errors.NewMultiError("upstream resolution errors")

	jobWithUnresolvedUpstream, err := subjectJob.GetJobWithUnresolvedUpstream()
	me.Append(err)

	jobWithInternalUpstream, err := u.internalUpstreamResolver.Resolve(ctx, jobWithUnresolvedUpstream)
	me.Append(err)

	jobWithInternalExternalUpstream, err := u.externalUpstreamResolver.Resolve(ctx, jobWithInternalUpstream, logWriter)
	me.Append(err)

	return jobWithInternalExternalUpstream.Upstreams(), me.ToErr()
}

func (UpstreamResolver) getUnresolvedUpstreamsErrors(jobsWithUpstreams []*job.WithUpstream, logWriter writer.LogWriter) error {
	me := errors.NewMultiError("unresolved upstreams errors")
	for _, jobWithUpstreams := range jobsWithUpstreams {
		for _, unresolvedUpstream := range jobWithUpstreams.GetUnresolvedUpstreams() {
			if unresolvedUpstream.Type() == job.UpstreamTypeStatic {
				errMsg := fmt.Sprintf("[%s] found unknown upstream for job %s: %s", jobWithUpstreams.Job().Tenant().NamespaceName().String(), jobWithUpstreams.Name().String(), unresolvedUpstream.FullName())
				logWriter.Write(writer.LogLevelError, errMsg)
				me.Append(errors.NewError(errors.ErrNotFound, job.EntityJob, errMsg))
			}
		}
	}
	return me.ToErr()
}

func checkForUnresolvedStaticUpstreams(tnnt tenant.Tenant, incomingJobNameMap map[job.Name]*job.Job, jobs []*job.WithUpstream, logWriter writer.LogWriter) error {
	me := errors.NewMultiError("check for unresolved static upstreams errors")
	for _, jobObj := range jobs {
		for _, upstream := range jobObj.Upstreams() {
			if upstream.Type() == job.UpstreamTypeInferred {
				continue
			}
			if upstream.State() == job.UpstreamStateResolved {
				continue
			}
			if _, ok := incomingJobNameMap[upstream.Name()]; ok {
				logWriter.Write(writer.LogLevelInfo, fmt.Sprintf("[%s] %s dependency: %s, for job: %s, is part of the incoming jobs themselves", tnnt.NamespaceName(), upstream.Type(), upstream.Name(), jobObj.GetName()))
				continue
			}
			me.Append(errors.NewError(errors.ErrInvalidState, job.EntityJob, fmt.Sprintf("could not resolve for %s upstream: %s, for job: %s", upstream.Type(), upstream.FullName(), jobObj.GetName())))
		}
	}
	return me.ToErr()
}
