package dex // nolint: testpackage

import (
	"context"
	"testing"
	"time"

	"github.com/goto/salt/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/goto/optimus/config"
)

func TestTableStatsEndpoint(t *testing.T) {
	t.Run("defaults to v1 when flag is unset", func(t *testing.T) {
		assert.Equal(t, tableStatsEndpointV1, tableStatsEndpoint(&config.DexClientConfig{}))
	})
	t.Run("uses v2 when flag is true", func(t *testing.T) {
		assert.Equal(t, tableStatsEndpointV2, tableStatsEndpoint(&config.DexClientConfig{UseV2Endpoint: true}))
	})
	t.Run("uses v1 when flag is false", func(t *testing.T) {
		assert.Equal(t, tableStatsEndpointV1, tableStatsEndpoint(&config.DexClientConfig{UseV2Endpoint: false}))
	})
}

func TestConstructGetTableStatsRequest(t *testing.T) {
	ctx := context.Background()
	start := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)
	end := start.Add(24 * time.Hour)

	logger := log.NewLogrus()
	t.Run("builds v1 path by default", func(t *testing.T) {
		client := &Client{l: logger, config: &config.DexClientConfig{Host: "http://dex.example.io"}}
		req, err := client.constructGetTableStatsRequest(ctx, "maxcompute", "proj.schema.table", start, end)
		require.NoError(t, err)
		assert.Equal(t, "/dex/tables/maxcompute/proj.schema.table/stats", req.URL.Path)
	})
	t.Run("builds v1 path when use_v2_endpoint is false", func(t *testing.T) {
		client := &Client{l: logger, config: &config.DexClientConfig{
			Host:          "http://dex.example.io",
			UseV2Endpoint: false,
		}}
		req, err := client.constructGetTableStatsRequest(ctx, "maxcompute", "proj.schema.table", start, end)
		require.NoError(t, err)
		assert.Equal(t, "/dex/tables/maxcompute/proj.schema.table/stats", req.URL.Path)
	})
}
