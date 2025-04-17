package evaluator_test

import (
	"os"
	"testing"

	"github.com/goto/salt/log"
	"github.com/stretchr/testify/assert"

	"github.com/goto/optimus/plugin/upstream_identifier/evaluator"
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
			rawResource := fileEvaluator.Evaluate(assets, map[string]string{})
			assert.Empty(t, rawResource)
		})
		t.Run("should return correct content when asset has targeted filepath", func(t *testing.T) {
			assets := map[string]string{
				"query.sql": "select 1",
			}
			rawResource := fileEvaluator.Evaluate(assets, map[string]string{})
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
		rawQuery1, err := os.ReadFile("./tests/query1.sql")
		assert.NoError(t, err)
		rawQuery2, err := os.ReadFile("./tests/query2.sql")
		assert.NoError(t, err)
		assets := map[string]string{
			"config.yaml": string(raw),
			"query1.sql":  string(rawQuery1),
			"query2.sql":  string(rawQuery2),
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
			rawResource := yamlpathEvaluator.Evaluate(assets, map[string]string{})
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
			rawResource := yamlpathEvaluator.Evaluate(assets, map[string]string{})
			assert.Empty(t, rawResource)
		})
		t.Run("should return empty when couldn't find targeted value", func(t *testing.T) {
			filepath := "./config.yaml"
			selector := "$.not_target_query"
			yamlpathEvaluator, err := e.GetYamlPathEvaluator(filepath, selector)
			assert.NoError(t, err)
			assert.NotNil(t, yamlpathEvaluator)

			rawResource := yamlpathEvaluator.Evaluate(assets, map[string]string{})
			assert.Empty(t, rawResource)
		})
		t.Run("should return correct content when could find targeted value", func(t *testing.T) {
			filepath := "./config.yaml"
			selector := "$.query"
			yamlpathEvaluator, err := e.GetYamlPathEvaluator(filepath, selector)
			assert.NoError(t, err)
			assert.NotNil(t, yamlpathEvaluator)

			rawResource := yamlpathEvaluator.Evaluate(assets, map[string]string{})
			assert.NotEmpty(t, rawResource)
			assert.Equal(t, "SELECT * FROM `project1.dataset1.table1`", rawResource)
		})
		t.Run("should return correct content when could find targeted value for nested selector", func(t *testing.T) {
			filepath := "./config.yaml"
			selector := "$.nested.query"
			yamlpathEvaluator, err := e.GetYamlPathEvaluator(filepath, selector)
			assert.NoError(t, err)
			assert.NotNil(t, yamlpathEvaluator)

			rawResource := yamlpathEvaluator.Evaluate(assets, map[string]string{})
			assert.NotEmpty(t, rawResource)
			assert.Equal(t, "SELECT * FROM `project2.dataset2.table2`", rawResource)
		})
		t.Run("should return correct multiple content when could find targeted value for multiple nested selector", func(t *testing.T) {
			filepath := "./config.yaml"
			selector := "$.multiple[*].query"
			yamlpathEvaluator, err := e.GetYamlPathEvaluator(filepath, selector)
			assert.NoError(t, err)
			assert.NotNil(t, yamlpathEvaluator)

			rawResource := yamlpathEvaluator.Evaluate(assets, map[string]string{})
			assert.NotEmpty(t, rawResource)
			assert.Equal(t, "SELECT * FROM `project3.dataset3.table3`\nSELECT * FROM `project4.dataset4.table4`", rawResource)
		})
		t.Run("should return correct content when the value is filepath", func(t *testing.T) {
			filepath := "./config.yaml"
			selector := "$.multiple_files[*].sql_path"
			yamlpathEvaluator, err := e.GetYamlPathEvaluator(filepath, selector)
			assert.NoError(t, err)
			assert.NotNil(t, yamlpathEvaluator)

			rawResource := yamlpathEvaluator.Evaluate(assets, map[string]string{})
			assert.NotEmpty(t, rawResource)
			assert.Equal(t, "SELECT * FROM `project5.dataset5.table5`\nSELECT * FROM `project6.dataset6.table6`", rawResource)
		})
	})
}

func TestEnvEvaluator(t *testing.T) {
	logger := log.NewNoop()
	e, err := evaluator.NewEvaluatorFactory(logger)
	assert.NoError(t, err)
	assert.NotNil(t, e)
	t.Run("should return error when env is empty", func(t *testing.T) {
		t.Skip()
		envEvaluator, err := e.GetEnvEvaluator("")
		assert.Error(t, err)
		assert.Nil(t, envEvaluator)
	})
	t.Run("should return env evaluator", func(t *testing.T) {
		t.Skip()
		envEvaluator, err := e.GetEnvEvaluator("MC__QUERY")
		assert.NoError(t, err)
		assert.NotNil(t, envEvaluator)
	})
	t.Run("Evaluate", func(t *testing.T) {
		config := map[string]string{
			"MC__QUERY":           "SELECT * FROM `project1.dataset1.table1`",
			"MC__QUERY_FILE_PATH": "/data/in/query.sql",
		}
		t.Run("should return query defined in the env", func(t *testing.T) {
			envEvaluator, err := e.GetEnvEvaluator("MC__QUERY")
			assert.NoError(t, err)
			assert.NotNil(t, envEvaluator)
			assets := map[string]string{}
			rawResource := envEvaluator.Evaluate(assets, config)
			assert.NotEmpty(t, rawResource)
			assert.Equal(t, "SELECT * FROM `project1.dataset1.table1`", rawResource)
		})
		t.Run("should return query from asset", func(t *testing.T) {
			envEvaluator, err := e.GetEnvEvaluator("MC__QUERY_FILE_PATH")
			assert.NoError(t, err)
			assert.NotNil(t, envEvaluator)
			assets := map[string]string{
				"query.sql": "select 1",
			}
			rawResource := envEvaluator.Evaluate(assets, config)
			assert.NotEmpty(t, rawResource)
			assert.Equal(t, "select 1", rawResource)
		})
	})
}
