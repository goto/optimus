package upstreamidentifier_test

import (
	"testing"

	"github.com/goto/salt/log"
	"github.com/stretchr/testify/assert"

	upstreamidentifier "github.com/goto/optimus/plugin/upstream_generator"
)

func TestNewUpstreamIdentifierFactory(t *testing.T) {
	t.Run("return error when logger is nil", func(t *testing.T) {
		upstreamIdentifierFactory, err := upstreamidentifier.NewUpstreamIdentifierFactory(nil)
		assert.Error(t, err)
		assert.Nil(t, upstreamIdentifierFactory)
	})
	t.Run("return success", func(t *testing.T) {
		logger := log.NewNoop()
		upstreamIdentifierFactory, err := upstreamidentifier.NewUpstreamIdentifierFactory(logger)
		assert.NoError(t, err)
		assert.NotNil(t, upstreamIdentifierFactory)
	})
}
