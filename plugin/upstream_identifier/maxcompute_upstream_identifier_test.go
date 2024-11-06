package upstreamidentifier_test

import (
	"context"
	"testing"

	"github.com/goto/salt/log"
	"github.com/stretchr/testify/assert"

	upstreamidentifier "github.com/goto/optimus/plugin/upstream_identifier"
)

func TestNewMaxcomputeUpstreamIdentifier(t *testing.T) {
	logger := log.NewNoop()
	parserFunc := func(string) []string { return nil }
	evaluatorFunc := func(map[string]string) string { return "" }
	extractFunc := func(ctx context.Context, resources []string) (map[string]string, error) {
		mp := make(map[string]string)
		for _, resource := range resources {
			mp[resource] = ""
		}
		return mp, nil
	}
	t.Run("return error when logger is nil", func(t *testing.T) {
		upstreamIdentifier, err := upstreamidentifier.NewMaxcomputeUpstreamIdentifier(nil, parserFunc, extractFunc, evaluatorFunc)
		assert.Error(t, err)
		assert.Nil(t, upstreamIdentifier)
	})
	t.Run("return error when parserFunc is nil", func(t *testing.T) {
		upstreamIdentifier, err := upstreamidentifier.NewMaxcomputeUpstreamIdentifier(logger, nil, extractFunc, evaluatorFunc)
		assert.Error(t, err)
		assert.Nil(t, upstreamIdentifier)
	})
	t.Run("return error when extractorFunc is nil", func(t *testing.T) {
		upstreamIdentifier, err := upstreamidentifier.NewMaxcomputeUpstreamIdentifier(logger, parserFunc, nil, evaluatorFunc)
		assert.Error(t, err)
		assert.Nil(t, upstreamIdentifier)
	})
	t.Run("return error when no evaluators", func(t *testing.T) {
		upstreamIdentifier, err := upstreamidentifier.NewMaxcomputeUpstreamIdentifier(logger, parserFunc, extractFunc)
		assert.Error(t, err)
		assert.Nil(t, upstreamIdentifier)
	})
	t.Run("return error when evaluatorFuncs is nil", func(t *testing.T) {
		upstreamIdentifier, err := upstreamidentifier.NewMaxcomputeUpstreamIdentifier(logger, parserFunc, extractFunc, nil)
		assert.Error(t, err)
		assert.Nil(t, upstreamIdentifier)
	})
	t.Run("return success", func(t *testing.T) {
		upstreamIdentifier, err := upstreamidentifier.NewMaxcomputeUpstreamIdentifier(logger, parserFunc, extractFunc, evaluatorFunc)
		assert.NoError(t, err)
		assert.NotNil(t, upstreamIdentifier)
	})
}

func TestMaxcomputeUpstreamIdentifier_IdentifyResources(t *testing.T) {
	ctx := context.Background()
	logger := log.NewNoop()
	assets := map[string]string{
		"./query.sql":      "select 1 from project1.schema1.name1",
		"./query_view.sql": "select 1 from project1.schema1.nameview1",
	}
	resourceToDDL := map[string]string{
		"project1.schema1.nameview1": "select 1 from project1.schema1.name1",
	}
	// TODO: adding failure test cases
	t.Run("return success", func(t *testing.T) {
		parserFunc := func(string) []string { return []string{"project1.schema1.name1"} }
		evaluatorFunc := func(map[string]string) string { return "./query.sql" }
		extractFunc := func(ctx context.Context, resources []string) (map[string]string, error) {
			mp := make(map[string]string)
			for _, resource := range resources {
				mp[resource] = ""
			}
			return mp, nil
		}
		upstreamIdentifier, err := upstreamidentifier.NewMaxcomputeUpstreamIdentifier(logger, parserFunc, extractFunc, evaluatorFunc)
		assert.NoError(t, err)
		assert.NotNil(t, upstreamIdentifier)
		resourceURNs, err := upstreamIdentifier.IdentifyResources(ctx, assets)
		assert.NoError(t, err)
		assert.Len(t, resourceURNs, 1)
		assert.Equal(t, "maxcompute://project1.schema1.name1", resourceURNs[0].String())
	})
	t.Run("return success on view", func(t *testing.T) {
		parserFunc := func(string) []string { return []string{"project1.schema1.name1"} }
		evaluatorFunc := func(map[string]string) string { return "./query.sql" }
		extractFunc := func(ctx context.Context, resources []string) (map[string]string, error) {
			mp := make(map[string]string)
			for _, resource := range resources {
				mp[resource] = resourceToDDL[resource]
			}
			return mp, nil
		}
		upstreamIdentifier, err := upstreamidentifier.NewMaxcomputeUpstreamIdentifier(logger, parserFunc, extractFunc, evaluatorFunc)
		assert.NoError(t, err)
		assert.NotNil(t, upstreamIdentifier)
		resourceURNs, err := upstreamIdentifier.IdentifyResources(ctx, assets)
		assert.NoError(t, err)
		assert.Len(t, resourceURNs, 1)
		assert.Equal(t, "maxcompute://project1.schema1.name1", resourceURNs[0].String())
	})
}
