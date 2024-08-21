package dag

import (
	"embed"
	"fmt"
	"io/fs"
	"path/filepath"
	"regexp"
	"strings"
	"text/template"

	"github.com/goto/optimus/internal/errors"
)

const (
	defaultVersion = "2.1"

	maxSemverPartLength = 2
)

// this map stores mapping of the provided scheduler minor versions
// to the DAG template versions available
var schedulerToTemplateVersionMap = map[string]string{
	"2.1":  "2.1",
	"2.2":  "2.1",
	"2.3":  "2.1",
	"2.4":  "2.4",
	"2.5":  "2.4",
	"2.6":  "2.6",
	"2.7":  "2.6",
	"2.8":  "2.6",
	"2.9":  "2.9",
	"2.10": "2.9",
}

//go:embed template
var templateFS embed.FS

type templates map[string]*template.Template

func NewTemplates() (templates, error) {
	templates := map[string]*template.Template{}
	re := regexp.MustCompile(`dag\.(\d.\d)\.py\.tmpl`)
	err := fs.WalkDir(templateFS, ".", func(path string, d fs.DirEntry, err error) error {
		if d.IsDir() {
			return nil
		}

		fileName := filepath.Base(path)
		if re.MatchString(fileName) {
			version := strings.TrimSuffix(strings.TrimPrefix(fileName, "dag."), ".py.tmpl")
			rawTemplate, err := fs.ReadFile(templateFS, path)
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
	return templates, nil
}

func (t templates) GetTemplate(airflowVersion string) *template.Template {
	// only take the major.minor version part of the given airflow version
	minorVersion := getMajorMinorVersion(airflowVersion)
	dagVersion := schedulerToTemplateVersionMap[minorVersion]
	if tmpl, ok := t[dagVersion]; ok {
		return tmpl
	}
	return t[defaultVersion]
}

func getMajorMinorVersion(version string) string {
	versionParts := strings.Split(version, ".")
	if len(versionParts) > maxSemverPartLength {
		versionParts = versionParts[:maxSemverPartLength]
	}

	return strings.Join(versionParts, ".")
}
