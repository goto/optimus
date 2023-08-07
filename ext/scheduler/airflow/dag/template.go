package dag

import (
	_ "embed"
	"text/template"

	"github.com/goto/optimus/internal/errors"
)

//go:embed dag.2.1.py.tmpl
var dagTemplate214 []byte

//go:embed dag.2.4.py.tmpl
var dagTemplate243 []byte

type TemplateFactory interface {
	New(airflowVersion string) *template.Template
}

type templateFactory struct {
	tmpl21 *template.Template
	tmpl24 *template.Template
}

func NewTemplateFactory() (TemplateFactory, error) {
	if len(dagTemplate214) == 0 {
		return nil, errors.InternalError("SchedulerAirflow", "dag template v2.1.4 is empty", nil)
	}

	if len(dagTemplate243) == 0 {
		return nil, errors.InternalError("SchedulerAirflow", "dag template v2.4.3 is empty", nil)
	}

	tmpl214, err := template.New("optimus_dag_compiler").Funcs(OptimusFuncMap()).Parse(string(dagTemplate214))
	if err != nil {
		return nil, errors.InternalError(EntitySchedulerAirflow, "unable to parse scheduler dag template", err)
	}
	tmpl243, err := template.New("optimus_dag_compiler").Funcs(OptimusFuncMap()).Parse(string(dagTemplate243))
	if err != nil {
		return nil, errors.InternalError(EntitySchedulerAirflow, "unable to parse scheduler dag template", err)
	}
	return &templateFactory{
		tmpl21: tmpl214,
		tmpl24: tmpl243,
	}, nil
}

func (f *templateFactory) New(airflowVersion string) *template.Template {
	switch airflowVersion {
	case "2.1.4":
		return f.tmpl21
	case "2.4.3":
		return f.tmpl24
	}
	return f.tmpl21 // fallback
}
