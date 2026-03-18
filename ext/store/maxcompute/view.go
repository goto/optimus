package maxcompute

import (
	"strings"

	"github.com/aliyun/aliyun-odps-go-sdk/odps"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/tableschema"

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
	CreateViewWithHints(schema tableschema.TableSchema, orReplace, createIfNotExists, buildDeferred bool, hints map[string]string) error
	BatchLoadTables(tableNames []string) ([]*odps.Table, error)
}

type ViewHandle struct {
	viewSQLExecutor          ViewSQLExecutor
	viewSchema               ViewSchema
	viewTable                ViewTable
	tableCommentWithMetadata bool
}

func (v ViewHandle) Create(res *resource.Resource) error {
	view, err := ConvertSpecTo[View](res)
	if err != nil {
		return err
	}

	if err := v.viewSchema.Create(v.viewSQLExecutor.CurrentSchemaName(), true, ""); err != nil {
		return errors.InternalError(EntitySchema, "error while creating schema on maxcompute", err)
	}
	comment := getResourceComment(view.Description, res, v.tableCommentWithMetadata)
	viewSchema := buildViewSchema(view, comment)
	if err := v.viewTable.CreateViewWithHints(viewSchema, false, false, false, view.Hints); err != nil {
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
	comment := getResourceComment(view.Description, res, v.tableCommentWithMetadata)
	viewSchema := buildViewSchema(view, comment)
	if err := v.viewTable.CreateViewWithHints(viewSchema, true, false, false, view.Hints); err != nil {
		return errors.InternalError(EntityView, "failed to update view "+res.FullName(), err)
	}

	return nil
}

func (v ViewHandle) Exists(tableName string) bool {
	_, err := v.viewTable.BatchLoadTables([]string{tableName})
	return err == nil
}

func buildViewSchema(v *View, comment string) tableschema.TableSchema {
	builder := tableschema.NewSchemaBuilder()
	builder.Name(v.Name).
		Comment(comment).
		Lifecycle(v.Lifecycle).
		IsVirtualView(true).
		ViewText(v.ViewQuery)

	return builder.Build()
}

func NewViewHandle(viewSQLExecutor ViewSQLExecutor, viewSchema ViewSchema, viewTable ViewTable, tableCommentWithMetadata bool) *ViewHandle {
	return &ViewHandle{viewSQLExecutor: viewSQLExecutor, viewSchema: viewSchema, viewTable: viewTable, tableCommentWithMetadata: tableCommentWithMetadata}
}
