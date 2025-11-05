package resolver

import (
	"context"
	"fmt"

	"github.com/goto/salt/log"

	"github.com/goto/optimus/config"
	"github.com/goto/optimus/core/job"
	"github.com/goto/optimus/core/scheduler/service"
	"github.com/goto/optimus/core/tenant"
	"github.com/goto/optimus/internal/errors"
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

func (u *dexUpstreamResolver) BulkResolve(ctx context.Context, jobsWithUpstreams []*job.WithUpstream, lw writer.LogWriter) ([]*job.WithUpstream, error) {
	jobsWithUpstreamsResolved := []*job.WithUpstream{}
	for _, jobWithUpstream := range jobsWithUpstreams {
		jobWithUpstreamsResolved, err := u.Resolve(ctx, jobWithUpstream, lw)
		if err != nil {
			return nil, err
		}
		jobsWithUpstreamsResolved = append(jobsWithUpstreamsResolved, jobWithUpstreamsResolved)
	}

	return jobsWithUpstreamsResolved, nil
}

func (u *dexUpstreamResolver) Resolve(ctx context.Context, jobWithUpstream *job.WithUpstream, lw writer.LogWriter) (*job.WithUpstream, error) {
	details, err := u.tenantDetailsGetter.GetDetails(ctx, jobWithUpstream.Job().Tenant())
	if err != nil {
		return nil, fmt.Errorf("failed to get tenant details for tenant %s: %w", jobWithUpstream.Job().Tenant().String(), err)
	}
	if val, err := details.GetConfig(tenant.ProjectDexThirdPartySensor); err != nil {
		return nil, fmt.Errorf("failed to get dex 3rd party sensor config for tenant %s: %w", jobWithUpstream.Job().Tenant().String(), err)
	} else if val != "true" {
		// skip DEX upstream resolution if dex 3rd party sensor is not enabled for the tenant
		return jobWithUpstream, nil
	}

	me := errors.NewMultiError(fmt.Sprintf("[%s] dex 3rd upstream resolution errors for job %s", jobWithUpstream.Job().Tenant().NamespaceName().String(), jobWithUpstream.Name().String()))
	upstreams := []*job.Upstream{}
	thirdPartyUpstreams := []*job.ThirdPartyUpstream{}
	for _, unresolvedUpstream := range jobWithUpstream.GetUnresolvedUpstreams() {
		// segregate DEX managed upstreams and non-DEX managed upstreams
		if isDEXManaged, err := u.dexClient.IsManaged(ctx, unresolvedUpstream.Resource()); err != nil {
			me.Append(err)
		} else if isDEXManaged {
			cfg := map[string]string{}
			cfg["resource_urn"] = unresolvedUpstream.Resource().String()
			resolvedUpstream := job.NewThirdPartyUpstream(config.DexUpstreamResolver.String(), unresolvedUpstream.Resource().GetName(), cfg)
			thirdPartyUpstreams = append(thirdPartyUpstreams, resolvedUpstream)
		} else {
			upstreams = append(upstreams, unresolvedUpstream)
		}
	}
	if len(me.Errors) > 0 {
		lw.Write(writer.LogLevelError, me.ToErr().Error())
	}
	return job.NewWithUpstreamAndThirdPartyUpstreams(jobWithUpstream.Job(), upstreams, thirdPartyUpstreams), me.ToErr()
}
