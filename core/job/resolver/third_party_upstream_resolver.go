package resolver

import (
	"context"

	"github.com/goto/salt/log"

	"github.com/goto/optimus/config"
	"github.com/goto/optimus/core/job"
	"github.com/goto/optimus/core/scheduler/service"
	"github.com/goto/optimus/core/tenant"
	"github.com/goto/optimus/internal/writer"
)

type TenantDetailsGetter interface {
	GetDetails(ctx context.Context, jobTenant tenant.Tenant) (*tenant.WithDetails, error)
}

type ThirdPartyUpstreamResolver interface {
	BulkResolve(ctx context.Context, jobsWithUpstreams []*job.WithUpstream, lw writer.LogWriter) ([]*job.WithUpstream, error)
	Resolve(ctx context.Context, jobWithUpstream *job.WithUpstream, lw writer.LogWriter) (*job.WithUpstream, error)
}

func NewThirdPartyUpstreamResolvers(l log.Logger, tenantDetailsGetter TenantDetailsGetter, upstreamResolvers ...config.UpstreamResolver) ([]ThirdPartyUpstreamResolver, error) {
	sensorClients := service.NewSensorService(l, upstreamResolvers...)

	var resolvers []ThirdPartyUpstreamResolver
	for _, upstreamResolver := range upstreamResolvers {
		switch upstreamResolver.Type { //nolint:revive
		case config.DexUpstreamResolver:
			client, err := sensorClients.GetClient(upstreamResolver.Type)
			if err != nil {
				return nil, err
			}
			resolvers = append(resolvers, NewDexUpstreamResolver(l, client, tenantDetailsGetter))
		}
	}
	return resolvers, nil
}
