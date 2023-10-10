package evaluator_test

import (
	"os"
	"testing"

	"github.com/goto/salt/log"
	"github.com/stretchr/testify/assert"

	"github.com/goto/optimus/plugin/upstream_generator/evaluator"
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

func TestGetYamlPathEvaluator(t *testing.T) {
	logger := log.NewNoop()
	e, err := evaluator.NewEvaluatorFactory(logger)
	assert.NoError(t, err)
	assert.NotNil(t, e)
	t.Run("should return error when filepath is empty", func(t *testing.T) {
		selector := "$.query"
		yamlpathEvaluator, err := e.GetYamlPathEvaluator("", selector)
		assert.Error(t, err)
		assert.Nil(t, yamlpathEvaluator)
	})
	t.Run("should return error when filepath is empty", func(t *testing.T) {
		filepath := "./config.yaml"
		yamlpathEvaluator, err := e.GetYamlPathEvaluator(filepath, "")
		assert.Error(t, err)
		assert.Nil(t, yamlpathEvaluator)
	})
	t.Run("should return error when selector is not valid", func(t *testing.T) {
		filepath := "./config.yaml"
		selector := "$.[quer"
		yamlpathEvaluator, err := e.GetYamlPathEvaluator(filepath, selector)
		assert.Error(t, err)
		assert.ErrorContains(t, err, "instantiate yamlpath error")
		assert.Nil(t, yamlpathEvaluator)
	})
	t.Run("Evaluate", func(t *testing.T) {
		raw, err := os.ReadFile("./tests/yaml_asset.yaml")
		assert.NoError(t, err)
		assets := map[string]string{
			"config.yaml": string(raw),
		}
		t.Run("should return empty when asset doesn't have targeted filepath", func(t *testing.T) {
			filepath := "./config.yaml"
			selector := "$.query"
			yamlpathEvaluator, err := e.GetYamlPathEvaluator(filepath, selector)
			assert.NoError(t, err)
			assert.NotNil(t, yamlpathEvaluator)

			assets := map[string]string{
				"not_target.yaml": "query: select 1",
			}
			rawResource := yamlpathEvaluator.Evaluate(assets)
			assert.Empty(t, rawResource)
		})
		t.Run("should return empty when yaml content is malformed", func(t *testing.T) {
			filepath := "./config.yaml"
			selector := "$.query"
			yamlpathEvaluator, err := e.GetYamlPathEvaluator(filepath, selector)
			assert.NoError(t, err)
			assert.NotNil(t, yamlpathEvaluator)

			assets := map[string]string{
				"config.yaml": "%%malformed:s",
			}
			rawResource := yamlpathEvaluator.Evaluate(assets)
			assert.Empty(t, rawResource)
		})
		t.Run("should return empty when couldn't find targeted value", func(t *testing.T) {
			filepath := "./config.yaml"
			selector := "$.not_target_query"
			yamlpathEvaluator, err := e.GetYamlPathEvaluator(filepath, selector)
			assert.NoError(t, err)
			assert.NotNil(t, yamlpathEvaluator)

			rawResource := yamlpathEvaluator.Evaluate(assets)
			assert.Empty(t, rawResource)
		})
		t.Run("should return correct content when could find targeted value", func(t *testing.T) {
			filepath := "./config.yaml"
			selector := "$.query"
			yamlpathEvaluator, err := e.GetYamlPathEvaluator(filepath, selector)
			assert.NoError(t, err)
			assert.NotNil(t, yamlpathEvaluator)

			rawResource := yamlpathEvaluator.Evaluate(assets)
			assert.NotEmpty(t, rawResource)
			assert.Equal(t, "SELECT * FROM `project1.dataset1.table1`", rawResource)
		})
		t.Run("should return correct content when could find targeted value for nested selector", func(t *testing.T) {
			filepath := "./config.yaml"
			selector := "$.nested.query"
			yamlpathEvaluator, err := e.GetYamlPathEvaluator(filepath, selector)
			assert.NoError(t, err)
			assert.NotNil(t, yamlpathEvaluator)

			rawResource := yamlpathEvaluator.Evaluate(assets)
			assert.NotEmpty(t, rawResource)
			assert.Equal(t, "SELECT * FROM `project2.dataset2.table2`", rawResource)
		})
	})
}
