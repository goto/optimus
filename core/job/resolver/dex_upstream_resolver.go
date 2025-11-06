package resolver

import (
	"context"
	"fmt"

	"github.com/goto/salt/log"
	"github.com/kushsharma/parallel"

	"github.com/goto/optimus/config"
	"github.com/goto/optimus/core/job"
	"github.com/goto/optimus/core/scheduler/service"
	"github.com/goto/optimus/core/tenant"
	"github.com/goto/optimus/internal/errors"
	"github.com/goto/optimus/internal/utils"
	"github.com/goto/optimus/internal/writer"
)

type dexUpstreamResolver struct {
	l                   log.Logger
	dexClient           service.ThirdPartyClient
	tenantDetailsGetter TenantDetailsGetter
}

func NewDexUpstreamResolver(l log.Logger, client service.ThirdPartyClient, tenantDetailsGetter TenantDetailsGetter) *dexUpstreamResolver {
	return &dexUpstreamResolver{
		l:                   l,
		dexClient:           client,
		tenantDetailsGetter: tenantDetailsGetter,
	}
}

func (u *dexUpstreamResolver) BulkResolve(ctx context.Context, jobsWithUpstream []*job.WithUpstream, lw writer.LogWriter) ([]*job.WithUpstream, error) {
	me := errors.NewMultiError("dex 3rd party upstream bulk resolution errors")

	var jobsWithAllUpstream []*job.WithUpstream
	runner := parallel.NewRunner(parallel.WithTicket(ConcurrentTicketPerSec), parallel.WithLimit(ConcurrentLimit))
	for _, jobWithUpstream := range jobsWithUpstream {
		runner.Add(func(currentJobWithUpstream *job.WithUpstream, lw writer.LogWriter) func() (interface{}, error) {
			return func() (interface{}, error) {
				return u.Resolve(ctx, currentJobWithUpstream, lw)
			}
		}(jobWithUpstream, lw))
	}

	for _, result := range runner.Run() {
		if result.Val != nil {
			specVal := result.Val.(*job.WithUpstream)
			jobsWithAllUpstream = append(jobsWithAllUpstream, specVal)
		}
		me.Append(result.Err)
	}

	return jobsWithAllUpstream, me.ToErr()
}

func (u *dexUpstreamResolver) Resolve(ctx context.Context, jobWithUpstream *job.WithUpstream, lw writer.LogWriter) (*job.WithUpstream, error) {
	details, err := u.tenantDetailsGetter.GetDetails(ctx, jobWithUpstream.Job().Tenant())
	if err != nil {
		return jobWithUpstream, fmt.Errorf("failed to get tenant details for tenant %s: %w", jobWithUpstream.Job().Tenant().String(), err)
	}
	if val, err := details.GetConfig(tenant.ProjectDexThirdPartySensor); err != nil {
		return jobWithUpstream, fmt.Errorf("failed to get dex 3rd party sensor config for tenant %s: %w", jobWithUpstream.Job().Tenant().String(), err)
	} else if !utils.ConvertToBoolean(val) {
		// skip DEX upstream resolution if dex 3rd party sensor is not enabled for the tenant
		return jobWithUpstream, nil
	}

	me := errors.NewMultiError(fmt.Sprintf("[%s] dex 3rd upstream resolution errors for job %s", jobWithUpstream.Job().Tenant().NamespaceName().String(), jobWithUpstream.Name().String()))
	unresolvedUpstreams := []*job.Upstream{}
	thirdPartyUpstreams := []*job.ThirdPartyUpstream{}
	for _, unresolvedUpstream := range jobWithUpstream.GetUnresolvedUpstreams() {
		// segregate DEX managed upstreams and non-DEX managed upstreams
		if isDEXManaged, err := u.dexClient.IsManaged(ctx, unresolvedUpstream.Resource()); err != nil {
			err = fmt.Errorf("failed to check dex management for resource urn %s: %w", unresolvedUpstream.Resource().String(), err)
			me.Append(err)
		} else if isDEXManaged {
			cfg := map[string]string{}
			cfg["resource_urn"] = unresolvedUpstream.Resource().String()
			resolvedUpstream := job.NewThirdPartyUpstream(config.DexUpstreamResolver.String(), unresolvedUpstream.Resource().GetName(), cfg)
			thirdPartyUpstreams = append(thirdPartyUpstreams, resolvedUpstream)
		} else {
			unresolvedUpstreams = append(unresolvedUpstreams, unresolvedUpstream)
		}
	}

	upstreams := []*job.Upstream{}
	upstreams = append(upstreams, jobWithUpstream.GetResolvedUpstreams()...)
	upstreams = append(upstreams, unresolvedUpstreams...)

	if len(me.Errors) > 0 {
		lw.Write(writer.LogLevelError, me.ToErr().Error())
	}

	return job.NewWithUpstreamAndThirdPartyUpstreams(jobWithUpstream.Job(), upstreams, thirdPartyUpstreams), me.ToErr()
}
