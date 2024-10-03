package maxcompute

import (
	"bytes"
	"context"
	"strings"
	"text/template"

	"github.com/aliyun/aliyun-odps-go-sdk/odps"

	"github.com/goto/optimus/core/resource"
	"github.com/goto/optimus/internal/errors"
)

const (
	viewCreate = "view_create"
)

type McSqlExecutor interface {
	ExecSql(taskName, sql string) (*odps.Instance, error)
}

type ViewHandle struct {
	mcView McSqlExecutor
}

func (v ViewHandle) Create(_ context.Context, res *resource.Resource) error {
	view, err := ConvertSpecTo[View](res)
	if err != nil {
		return err
	}

	sql, err := ToViewSQL(view)
	if err != nil {
		return err
	}

	inst, err := v.mcView.ExecSql(viewCreate, sql)
	if err != nil {
		return errors.InternalError(EntityView, "failed to create view "+res.FullName(), err)
	}

	err = inst.WaitForSuccess()
	if err != nil {
		// TODO: check the type of error
		return errors.InternalError(EntityView, "failed to create view "+res.FullName(), err)
	}

	return nil
}

func NewViewHandle(view McSqlExecutor) *ViewHandle {
	return &ViewHandle{mcView: view}
}

func ToViewSQL(v *View) (string, error) {
	var fns = template.FuncMap{
		"join": func(sep string, s []string) string {
			return strings.Join(s, sep)
		},
	}

	tplStr := `create or replace view if not exists {{ .Name.String }}
    ({{ join ", " .Columns }}) {{ if .Description }} 
    comment '{{ .Description}}' {{ end }} 
    as
	{{ .ViewQuery}}
;`

	tpl, err := template.New("DDL_UPSERT_VIEW").Funcs(fns).Parse(tplStr)
	if err != nil {
		return "", err
	}

	var out bytes.Buffer
	err = tpl.Execute(&out, v)
	if err != nil {
		return "", err
	}

	return out.String(), nil
}
