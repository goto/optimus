package upstreamgenerator_test

import (
	"testing"

	"github.com/goto/salt/log"
	"github.com/stretchr/testify/assert"

	upstreamgenerator "github.com/goto/optimus/plugin/upstream_generator"
)

func TestNewUpstreamGeneratorFactory(t *testing.T) {
	t.Run("return error when logger is nil", func(t *testing.T) {
		upstreamGeneratorFactory, err := upstreamgenerator.NewUpstreamGeneratorFactory(nil)
		assert.Error(t, err)
		assert.Nil(t, upstreamGeneratorFactory)
	})
	t.Run("return success", func(t *testing.T) {
		logger := log.NewNoop()
		upstreamGeneratorFactory, err := upstreamgenerator.NewUpstreamGeneratorFactory(logger)
		assert.NoError(t, err)
		assert.NotNil(t, upstreamGeneratorFactory)
	})
}
