package maxcompute

import (
	"github.com/aliyun/aliyun-odps-go-sdk/odps"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/tableschema"
	"strings"

	"github.com/goto/optimus/core/resource"
	"github.com/goto/optimus/internal/errors"
)

type ViewSQLExecutor interface {
	CurrentSchemaName() string
}

type ViewSchema interface {
	Create(schemaName string, createIfNotExists bool, comment string) error
}

type ViewTable interface {
	CreateView(schema tableschema.TableSchema, orReplace, createIfNotExists, buildDeferred bool) error
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

	_, view.Name, err = getCompleteComponentName(res)
	if err != nil {
		return err
	}

	if err := v.viewSchema.Create(v.viewSQLExecutor.CurrentSchemaName(), true, ""); err != nil {
		return errors.InternalError(EntitySchema, "error while creating schema on maxcompute", err)
	}

	viewSchema := buildViewSchema(view)
	if err := v.viewTable.CreateView(viewSchema, false, false, false); err != nil {
		if strings.Contains(err.Error(), "Table or view already exists") {
			return errors.AlreadyExists(EntityView, "view already exists on maxcompute: "+res.FullName())
		}
		return errors.InternalError(EntityView, "failed to create view "+res.FullName(), err)
	}

	return nil
}

func (v ViewHandle) Update(res *resource.Resource) error {
	_, viewName, err := getCompleteComponentName(res)
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
	view.Name = viewName

	viewSchema := buildViewSchema(view)
	if err := v.viewTable.CreateView(viewSchema, true, false, false); err != nil {
		return errors.InternalError(EntityView, "failed to update view "+res.FullName(), err)
	}

	return nil
}

func (v ViewHandle) Exists(tableName string) bool {
	_, err := v.viewTable.BatchLoadTables([]string{tableName})
	return err == nil
}

func buildViewSchema(v *View) tableschema.TableSchema {
	builder := tableschema.NewSchemaBuilder()
	builder.Name(v.Name.String()).
		Comment(v.Description).
		Lifecycle(v.Lifecycle).
		IsVirtualView(true).
		ViewText(v.ViewQuery)

	return builder.Build()
}

func NewViewHandle(viewSQLExecutor ViewSQLExecutor, viewSchema ViewSchema, viewTable ViewTable) *ViewHandle {
	return &ViewHandle{viewSQLExecutor: viewSQLExecutor, viewSchema: viewSchema, viewTable: viewTable}
}
