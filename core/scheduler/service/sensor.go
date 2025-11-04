package service

import (
	"context"
	"fmt"
	"time"

	"github.com/goto/optimus/config"
	"github.com/goto/optimus/core/resource"
	"github.com/goto/optimus/ext/dex"
	"github.com/goto/salt/log"
)

// ThirdPartyClient defines the interface that any third-party service client must implement
type ThirdPartyClient interface {
	// IsManaged checks if the given identifier is managed by the third-party service.
	IsManaged(ctx context.Context, resourceURN resource.URN) (bool, error)
	// IsComplete checks if the data for the given identifier between dateFrom and dateTo is complete.
	IsComplete(ctx context.Context, resourceURN resource.URN, dateFrom, dateTo time.Time) (bool, error)
}

type clients struct {
	logger            log.Logger
	upstreamResolvers map[config.UpstreamResolverType]config.UpstreamResolver
}

func NewSensorService(l log.Logger, upstreamResolvers ...config.UpstreamResolver) *clients {
	c := &clients{
		logger: l,
	}
	c.upstreamResolvers = make(map[config.UpstreamResolverType]config.UpstreamResolver)
	for _, ur := range upstreamResolvers {
		c.upstreamResolvers[ur.Type] = ur
	}
	return c
}

func (c *clients) GetClient(upstreamResolverType config.UpstreamResolverType) (ThirdPartyClient, error) {
	upstreamResolver, ok := c.upstreamResolvers[upstreamResolverType]
	if !ok {
		return nil, fmt.Errorf("upstream resolver of type %s not found", upstreamResolverType)
	}
	switch upstreamResolver.Type {
	case config.DexUpstreamResolver:
		return c.getDexClient(&upstreamResolver)
	default:
		return nil, fmt.Errorf("unsupported upstream resolver type: %s", upstreamResolver.Type)
	}
}

func (c *clients) getDexClient(upstreamResolver *config.UpstreamResolver) (ThirdPartyClient, error) {
	clientConfig, err := upstreamResolver.GetDexClientConfig()
	if err != nil {
		return nil, err
	}
	return dex.NewDexClient(c.logger, clientConfig)
}
