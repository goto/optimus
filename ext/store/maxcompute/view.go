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
	CurrentSchemaName() string
}

type ViewSchema interface {
	Create(schemaName string, createIfNotExists bool, comment string) error
}

type ViewTable interface {
	BatchLoadTables(tableNames []string) ([]*odps.Table, error)
}

type ViewHandle struct {
	viewSQLExecutor ViewSQLExecutor
	viewSchema      ViewSchema
	viewTable       ViewTable
}

func (v ViewHandle) Create(res *resource.Resource) error {
	view, err := ConvertSpecTo[View](res)
	if err != nil {
		return err
	}

	if err := v.viewSchema.Create(v.viewSQLExecutor.CurrentSchemaName(), true, ""); err != nil {
		return errors.InternalError(EntitySchema, "error while creating schema on maxcompute", err)
	}

	sql, err := ToViewSQL(view)
	if err != nil {
		return errors.AddErrContext(err, EntityView, "failed to build view sql query to create view "+view.FullName())
	}

	inst, err := v.viewSQLExecutor.ExecSQl(sql)
	if err != nil {
		return errors.AddErrContext(err, EntityView, "failed to create sql task to create view "+view.FullName())
	}

	if err = inst.WaitForSuccess(); err != nil {
		if strings.Contains(err.Error(), "Table or view already exists") {
			return errors.AlreadyExists(EntityView, "view already exists on maxcompute: "+view.FullName())
		}
		return errors.InternalError(EntityView, "failed to create view "+view.FullName(), err)
	}

	return nil
}

func (v ViewHandle) Update(res *resource.Resource) error {
	view, err := ConvertSpecTo[View](res)
	if err != nil {
		return err
	}

	_, err = v.viewTable.BatchLoadTables([]string{view.Name})
	if err != nil {
		return errors.InternalError(EntityView, "error while get view on maxcompute", err)
	}

	sql, err := ToViewSQL(view)
	if err != nil {
		return errors.AddErrContext(err, EntityView, "failed to build view sql query to update view "+view.FullName())
	}

	inst, err := v.viewSQLExecutor.ExecSQl(sql)
	if err != nil {
		return errors.AddErrContext(err, EntityView, "failed to create sql task to update view "+view.FullName())
	}

	if err = inst.WaitForSuccess(); err != nil {
		return errors.InternalError(EntityView, "failed to update view "+view.FullName(), err)
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

	tplStr := `create or replace view {{ .Database }}.{{ .Name }} {{ if .Columns }}
	({{ join ", " .Columns }}) {{ end }} {{ if .Description }} 
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

func NewViewHandle(viewSQLExecutor ViewSQLExecutor, viewSchema ViewSchema, viewTable ViewTable) *ViewHandle {
	return &ViewHandle{viewSQLExecutor: viewSQLExecutor, viewSchema: viewSchema, viewTable: viewTable}
}
