package maxcompute

import (
	"bytes"
	"strings"
	"text/template"

	"github.com/aliyun/aliyun-odps-go-sdk/odps"

	"github.com/goto/optimus/core/resource"
	"github.com/goto/optimus/internal/errors"
)

type ViewSqlExecutor interface {
	ExecSQl(sql string) (*odps.Instance, error)
}

type ViewHandle struct {
	mcView ViewSqlExecutor
}

func (v ViewHandle) Create(res *resource.Resource) error {
	view, err := ConvertSpecTo[View](res)
	if err != nil {
		return err
	}
	view.Name = res.Name()

	sql, err := ToViewSQL(view)
	if err != nil {
		return err
	}

	inst, err := v.mcView.ExecSQl(sql)
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

func (v ViewHandle) Update(res *resource.Resource) error {
	//TODO implement me
	panic("implement me")
}

func (v ViewHandle) Exists(tableName string) bool {
	//TODO implement me
	panic("implement me")
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
	{{ .ViewQuery}};`

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

func NewViewHandle(view ViewSqlExecutor) *ViewHandle {
	return &ViewHandle{mcView: view}
}
