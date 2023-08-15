package dag_test

import (
	_ "embed"
	"os"
	"path/filepath"
	"testing"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"

	"github.com/goto/optimus/ext/scheduler/airflow/dag"
)

//go:embed template/dag.2.1.py.tmpl
var dagTemplate21 []byte

//go:embed template/dag.2.4.py.tmpl
var dagTemplate24 []byte

func TestNewTemplateFactory(t *testing.T) {
	t.Run("return error if fail to parse the template file", func(t *testing.T) {
		fs := createTemplateDummies("template")
		f, _ := fs.Create(filepath.Join("template", "dag.2.2.py.tmpl")) // corrupted
		f.WriteString(`{{ tt }}`)
		tmplFac, err := dag.NewTemplateFactory(fs, "template")
		assert.Nil(t, tmplFac)
		assert.Error(t, err)
		assert.ErrorContains(t, err, "unable to parse")
	})
	t.Run("return error if default version is not exist", func(t *testing.T) {
		fs := createTemplateDummies("template")
		fs.Remove(filepath.Join("template", "dag.2.1.py.tmpl")) // deleted
		tmplFac, err := dag.NewTemplateFactory(fs, "template")
		assert.Nil(t, tmplFac)
		assert.Error(t, err)
		assert.Error(t, err, "template default v2.1 is not exist")
	})
	t.Run("return template factory if no error", func(t *testing.T) {
		fs := createTemplateDummies("template")
		tmplFac, err := dag.NewTemplateFactory(fs, "template")
		assert.NotNil(t, tmplFac)
		assert.NoError(t, err)
	})
}

func TestTemplateFactoryNew(t *testing.T) {
	t.Run("return correct template given complete semver", func(t *testing.T) {
		fs := createTemplateDummies("template")
		tmplFac, err := dag.NewTemplateFactory(fs, "template")
		assert.NoError(t, err)

		tmpl := tmplFac.New("2.4.1")
		assert.Equal(t, "optimus_dag_v2.4_compiler", tmpl.Name())
	})
}

func createTemplateDummies(dir string) afero.Fs {
	fs := afero.NewMemMapFs()
	fs.Mkdir(dir, os.ModeDir)
	f, _ := fs.Create(filepath.Join(dir, "dag.2.1.py.tmpl"))
	f.Write(dagTemplate21)
	f, _ = fs.Create(filepath.Join(dir, "dag.2.4.py.tmpl"))
	f.Write(dagTemplate24)
	return fs
}
