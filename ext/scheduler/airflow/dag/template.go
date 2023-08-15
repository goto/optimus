package dag

import (
	_ "embed"
	"fmt"
	"io/fs"
	"path/filepath"
	"regexp"
	"strings"
	"text/template"

	"github.com/spf13/afero"

	"github.com/goto/optimus/internal/errors"
)

const defaultVersion = "2.1"

type TemplateFactory interface {
	New(airflowVersion string) *template.Template
}

type templateFactory struct {
	templates map[string]*template.Template
}

func NewTemplateFactory(aferoFs afero.Fs, templateDir string) (TemplateFactory, error) {
	templates := map[string]*template.Template{}
	re := regexp.MustCompile(`dag\.(\d.\d)\.py\.tmpl`)
	err := afero.Walk(aferoFs, templateDir, func(path string, d fs.FileInfo, err error) error {
		if d.IsDir() {
			return nil
		}
		fileName := filepath.Base(path)
		if re.MatchString(fileName) {
			version := strings.TrimSuffix(strings.TrimPrefix(fileName, "dag."), ".py.tmpl")
			rawTemplate, err := afero.ReadFile(aferoFs, path)
			if err != nil {
				return errors.InternalError(EntitySchedulerAirflow, fmt.Sprintf("dag template v%s is fail to load", version), err)
			}
			tmpl, err := template.New(fmt.Sprintf("optimus_dag_v%s_compiler", version)).Funcs(OptimusFuncMap()).Parse(string(rawTemplate))
			if err != nil {
				return errors.InternalError(EntitySchedulerAirflow, fmt.Sprintf("unable to parse scheduler dag template v%s", version), err)
			}
			templates[version] = tmpl
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if _, ok := templates[defaultVersion]; !ok {
		return nil, errors.InternalError(EntitySchedulerAirflow, fmt.Sprintf("template default v%s is not exist", defaultVersion), nil)
	}
	return &templateFactory{
		templates: templates,
	}, nil
}

func (f *templateFactory) New(airflowVersion string) *template.Template {
	version := strings.Join(strings.Split(airflowVersion, ".")[:2], ".")
	if tmpl, ok := f.templates[version]; ok {
		return tmpl
	} else {
		return f.templates[version]
	}
}
