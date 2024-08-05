package bigquery

import (
	"context"

	"cloud.google.com/go/bigquery"

	"github.com/goto/optimus/core/resource"
	"github.com/goto/optimus/internal/errors"
)

const EntityModel = "resource_model"

// BqModel is BigQuery Model
type BqModel interface {
	Metadata(ctx context.Context) (mm *bigquery.ModelMetadata, err error)
}

type ModelHandle struct {
	bqModel BqModel
}

func (ModelHandle) Create(_ context.Context, _ *resource.Resource) error {
	return errors.FailedPrecondition(EntityModel, "create is not supported")
}

func (ModelHandle) Update(_ context.Context, _ *resource.Resource) error {
	return errors.FailedPrecondition(EntityModel, "update is not supported")
}

func (r ModelHandle) Exists(ctx context.Context) bool {
	if r.bqModel == nil {
		return false
	}
	_, err := r.bqModel.Metadata(ctx)
	return err == nil
}

func NewModelHandle(bq BqModel) *ModelHandle {
	return &ModelHandle{bqModel: bq}
}
