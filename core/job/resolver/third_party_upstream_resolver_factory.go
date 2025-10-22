package resolver

import (
	"context"

	"github.com/goto/optimus/config"
	"github.com/goto/optimus/core/job"
	"github.com/goto/optimus/internal/writer"
)

type ThirdPartyUpstreamResolver interface {
	BulkResolve(ctx context.Context, jobsWithUpstreams []*job.WithUpstream, lw writer.LogWriter) ([]*job.WithUpstream, error)
	Resolve(ctx context.Context, jobWithUpstream *job.WithUpstream, lw writer.LogWriter) (*job.WithUpstream, error)
}

func NewThirdPartyUpstreamResolvers(upstreamResolvers ...config.UpstreamResolver) []ThirdPartyUpstreamResolver {
	var resolvers []ThirdPartyUpstreamResolver
	for _, upstreamResolver := range upstreamResolvers {
		switch upstreamResolver.Type {
		case config.DexUpstreamResolver:
			// config can be accessed via upstreamResolver.Config if needed in the future
			resolvers = append(resolvers, NewDexUpstreamResolver(upstreamResolver.Config))
		}
	}
	return resolvers
}
