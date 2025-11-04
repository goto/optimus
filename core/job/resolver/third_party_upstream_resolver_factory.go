package resolver

import (
	"context"

	"github.com/goto/optimus/config"
	"github.com/goto/optimus/core/job"
	"github.com/goto/optimus/ext/dex"
	"github.com/goto/optimus/internal/writer"
	"github.com/goto/salt/log"
)

type ThirdPartyUpstreamResolver interface {
	BulkResolve(ctx context.Context, jobsWithUpstreams []*job.WithUpstream, lw writer.LogWriter) ([]*job.WithUpstream, error)
	Resolve(ctx context.Context, jobWithUpstream *job.WithUpstream, lw writer.LogWriter) (*job.WithUpstream, error)
}

func NewThirdPartyUpstreamResolvers(l log.Logger, upstreamResolvers ...config.UpstreamResolver) ([]ThirdPartyUpstreamResolver, error) {
	var resolvers []ThirdPartyUpstreamResolver
	for _, upstreamResolver := range upstreamResolvers {
		switch upstreamResolver.Type { //nolint:revive
		case config.DexUpstreamResolver:
			clientConfig, err := upstreamResolver.GetDexClientConfig()
			if err != nil {
				return nil, err
			}
			dexClient, err := dex.NewDexClient(l, clientConfig)
			if err != nil {
				return nil, err
			}
			resolvers = append(resolvers, NewDexUpstreamResolver(dexClient))
		}
	}
	return resolvers, nil
}
