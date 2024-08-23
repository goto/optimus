package dag_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/goto/optimus/ext/scheduler/airflow/dag"
)

func TestNewTemplates(t *testing.T) {
	t.Run("return templates if no error", func(t *testing.T) {
		templates, err := dag.NewTemplates()
		assert.NotNil(t, templates)
		assert.NoError(t, err)
		assert.Len(t, templates, 4)
	})
}

func TestTemplatesGet(t *testing.T) {
	t.Run("return correct template given complete semver", func(t *testing.T) {
		templates, err := dag.NewTemplates()
		assert.NoError(t, err)

		tmpl := templates.GetTemplate("2.4.3")
		assert.Equal(t, "optimus_dag_v2.4_compiler", tmpl.Name())
	})
	t.Run("return supported template given a backward-compatible version", func(t *testing.T) {
		templates, err := dag.NewTemplates()
		assert.NoError(t, err)

		tmpl := templates.GetTemplate("2.2.0")
		assert.Equal(t, "optimus_dag_v2.1_compiler", tmpl.Name())
	})
	t.Run("return default version if version is not supported", func(t *testing.T) {
		templates, err := dag.NewTemplates()
		assert.NoError(t, err)

		tmpl := templates.GetTemplate("2.0.0")
		assert.Equal(t, "optimus_dag_v2.1_compiler", tmpl.Name())
	})
}
