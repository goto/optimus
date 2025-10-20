package resolver

import (
	"context"
	"strings"

	"github.com/goto/optimus/core/job"
	"github.com/goto/optimus/core/resource"
	"github.com/goto/optimus/internal/writer"
)

type dexUpstreamResolver struct {
}

func NewDexUpstreamResolver() *dexUpstreamResolver {
	return &dexUpstreamResolver{}
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
	upstreams := []*job.Upstream{}
	for _, unresolvedUpstream := range jobWithUpstream.GetUnresolvedUpstreams() {
		if u.isDEXManagedUpstream(ctx, unresolvedUpstream.Resource()) {
			resolvedUpstream := job.NewUpstreamResolvedThirdParty(unresolvedUpstream, "DEX") // TODO: set resolved third party type as constant
			upstreams = append(upstreams, resolvedUpstream)
		} else {
			upstreams = append(upstreams, unresolvedUpstream)
		}
	}
	return job.NewWithUpstream(jobWithUpstream.Job(), upstreams), nil
}

func (u *dexUpstreamResolver) isDEXManagedUpstream(_ context.Context, resourceURN resource.URN) bool {
	// TODO: implement DEX upstream resolver by calling DEX api
	// 1. get unresolved upstreams
	// 2. call DEX api to check whether the upstream is managed by DEX
	// 3. if yes, get the resolved upstream from DEX api response and set it to job's upstreams
	// 4. mark upstream_3rd_party_type as DEX

	// now, only resolved if resource_urn contains _raw
	return strings.Contains(string(resourceURN.String()), "_raw")
}
