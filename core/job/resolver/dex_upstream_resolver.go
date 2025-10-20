package resolver

import (
	"context"

	"github.com/goto/optimus/core/job"
	"github.com/goto/optimus/internal/writer"
)

type dexUpstreamResolver struct {
}

func NewDexUpstreamResolver() *dexUpstreamResolver {
	return &dexUpstreamResolver{}
}

func (h *dexUpstreamResolver) BulkResolve(ctx context.Context, jobsWithUpstreams []*job.WithUpstream, lw writer.LogWriter) ([]*job.WithUpstream, error) {
	// TODO: implement DEX upstream resolver by calling DEX api
	// 1. get unresolved upstreams
	// 2. call DEX api to check whether the upstream is managed by DEX
	// 3. if yes, get the resolved upstream from DEX api response and set it to job's upstreams
	// 4. mark upstream_3rd_party_type as DEX
	return jobsWithUpstreams, nil
}
