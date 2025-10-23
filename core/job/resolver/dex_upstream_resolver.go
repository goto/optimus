package resolver

import (
	"context"
	"fmt"
	"strings"

	"github.com/goto/optimus/core/job"
	"github.com/goto/optimus/core/resource"
	"github.com/goto/optimus/internal/errors"
	"github.com/goto/optimus/internal/writer"
)

type dexUpstreamResolver struct{}

func NewDexUpstreamResolver(_ map[string]interface{}) *dexUpstreamResolver {
	return &dexUpstreamResolver{}
}

func (u *dexUpstreamResolver) BulkResolve(ctx context.Context, jobsWithUpstreams []*job.WithUpstream, lw writer.LogWriter) ([]*job.WithUpstream, error) {
	// TODO: implement DEX upstream resolver by calling DEX api
	// 1. get unresolved upstreams
	// 2. call DEX api to check whether the upstream is managed by DEX
	// 3. if yes, get the resolved upstream from DEX api response and set it to job's upstreams
	// 4. mark upstream_3rd_party_type as DEX
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
	me := errors.NewMultiError(fmt.Sprintf("[%s] dex 3rd upstream resolution errors for job %s", jobWithUpstream.Job().Tenant().NamespaceName().String(), jobWithUpstream.Name().String()))
	upstreams := []*job.Upstream{}
	thirdPartyUpstreams := []*job.ThirdPartyUpstream{}
	for _, unresolvedUpstream := range jobWithUpstream.GetUnresolvedUpstreams() {
		// segregate DEX managed upstreams and non-DEX managed upstreams
		if isDEXManaged, err := u.isDEXManagedUpstream(ctx, unresolvedUpstream.Resource()); err != nil {
			me.Append(err)
		} else if isDEXManaged {
			config := map[string]string{}
			config["resource_urn"] = unresolvedUpstream.Resource().String()
			resolvedUpstream := job.NewThirdPartyUpstream("dex", unresolvedUpstream.Resource().GetName(), config) // TODO: set resolved third party type as constant
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

func (*dexUpstreamResolver) isDEXManagedUpstream(_ context.Context, resourceURN resource.URN) (bool, error) { //nolint:unparam
	// now, only resolved if resource_urn contains _raw
	return strings.Contains(resourceURN.String(), "_raw"), nil
}
