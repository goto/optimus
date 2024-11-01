package maxcompute

import (
	"bytes"
	"strings"
	"text/template"

	"github.com/aliyun/aliyun-odps-go-sdk/odps"

	"github.com/goto/optimus/core/resource"
	"github.com/goto/optimus/internal/errors"
)

type ViewSQLExecutor interface {
	ExecSQl(sql string, hints ...map[string]string) (*odps.Instance, error)
}

type ViewTable interface {
	BatchLoadTables(tableNames []string) ([]*odps.Table, error)
}

type ViewHandle struct {
	viewSQLExecutor ViewSQLExecutor
	viewTable       ViewTable
}

func (v ViewHandle) Create(res *resource.Resource) error {
	view, err := ConvertSpecTo[View](res)
	if err != nil {
		return err
	}

	projectSchema, viewName, err := getCompleteComponentName(res)
	if err != nil {
		return err
	}

	view.Name, err = resource.NameFrom(projectSchema.Schema + "." + viewName.String())
	if err != nil {
		return err
	}

	sql, err := ToViewSQL(view)
	if err != nil {
		return errors.AddErrContext(err, EntityView, "failed to build view sql query to create view "+res.FullName())
	}

	inst, err := v.viewSQLExecutor.ExecSQl(sql)
	if err != nil {
		return errors.AddErrContext(err, EntityView, "failed to create sql task to create view "+res.FullName())
	}

	if err = inst.WaitForSuccess(); err != nil {
		if strings.Contains(err.Error(), "Table or view already exists") {
			return errors.AlreadyExists(EntityView, "view already exists on maxcompute: "+res.FullName())
		}
		return errors.InternalError(EntityView, "failed to create view "+res.FullName(), err)
	}

	return nil
}

func (v ViewHandle) Update(res *resource.Resource) error {
	projectSchema, viewName, err := getCompleteComponentName(res)
	if err != nil {
		return err
	}

	_, err = v.viewTable.BatchLoadTables([]string{viewName.String()})
	if err != nil {
		return errors.InternalError(EntityView, "error while get view on maxcompute", err)
	}

	view, err := ConvertSpecTo[View](res)
	if err != nil {
		return err
	}

	view.Name, err = resource.NameFrom(projectSchema.Schema + "." + viewName.String())
	if err != nil {
		return err
	}

	sql, err := ToViewSQL(view)
	if err != nil {
		return errors.AddErrContext(err, EntityView, "failed to build view sql query to update view "+res.FullName())
	}

	inst, err := v.viewSQLExecutor.ExecSQl(sql)
	if err != nil {
		return errors.AddErrContext(err, EntityView, "failed to create sql task to update view "+res.FullName())
	}

	if err = inst.WaitForSuccess(); err != nil {
		return errors.InternalError(EntityView, "failed to update view "+res.FullName(), err)
	}

	return nil
}

func (v ViewHandle) Exists(tableName string) bool {
	_, err := v.viewTable.BatchLoadTables([]string{tableName})
	return err == nil
}

func ToViewSQL(v *View) (string, error) {
	fns := template.FuncMap{
		"join": func(sep string, s []string) string {
			return strings.Join(s, sep)
		},
	}

	tplStr := `create or replace view {{ .Name.String }}
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

func NewViewHandle(viewSQLExecutor ViewSQLExecutor, view ViewTable) *ViewHandle {
	return &ViewHandle{viewSQLExecutor: viewSQLExecutor, viewTable: view}
}
