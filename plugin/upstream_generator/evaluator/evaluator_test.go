package evaluator_test

import (
	"testing"

	"github.com/goto/optimus/plugin/upstream_generator/evaluator"
	"github.com/goto/salt/log"
	"github.com/stretchr/testify/assert"
)

func TestNewEvaluatorFactory(t *testing.T) {
	t.Run("should return error if logger is nil", func(t *testing.T) {
		e, err := evaluator.NewEvaluatorFactory(nil)
		assert.Error(t, err)
		assert.Nil(t, e)
	})
	t.Run("should success", func(t *testing.T) {
		logger := log.NewNoop()
		e, err := evaluator.NewEvaluatorFactory(logger)
		assert.NoError(t, err)
		assert.NotNil(t, e)
	})
}

func TestGetFileEvaluator(t *testing.T) {
	logger := log.NewNoop()
	e, err := evaluator.NewEvaluatorFactory(logger)
	assert.NoError(t, err)
	assert.NotNil(t, e)
	t.Run("should return error when filepath is empty", func(t *testing.T) {
		fileEvaluator, err := e.GetFileEvaluator("")
		assert.Error(t, err)
		assert.Nil(t, fileEvaluator)
	})
	t.Run("should return file evaluator", func(t *testing.T) {
		fileEvaluator, err := e.GetFileEvaluator("./query.sql")
		assert.NoError(t, err)
		assert.NotNil(t, fileEvaluator)
	})
	t.Run("Evaluate", func(t *testing.T) {
		fileEvaluator, err := e.GetFileEvaluator("./query.sql")
		assert.NoError(t, err)
		assert.NotNil(t, fileEvaluator)
		t.Run("should return empty when asset doesn't have targeted filepath", func(t *testing.T) {
			assets := map[string]string{
				"not_target.sql": "select 1",
			}
			rawResource := fileEvaluator.Evaluate(assets)
			assert.Empty(t, rawResource)
		})
		t.Run("should return correct content when asset has targeted filepath", func(t *testing.T) {
			assets := map[string]string{
				"query.sql": "select 1",
			}
			rawResource := fileEvaluator.Evaluate(assets)
			assert.NotEmpty(t, rawResource)
		})
	})
}
