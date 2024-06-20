package bigquery

import (
	"context"

	"cloud.google.com/go/bigquery"

	"github.com/goto/optimus/core/resource"
	"github.com/goto/optimus/internal/errors"
)

const EntityRoutine = "routines"

// BqRoutine is including UDF, Store Procedure, Table Function
type BqRoutine interface {
	Create(ctx context.Context, rm *bigquery.RoutineMetadata) (err error)
	Update(ctx context.Context, upd *bigquery.RoutineMetadataToUpdate, etag string) (rm *bigquery.RoutineMetadata, err error)
	Metadata(ctx context.Context) (*bigquery.RoutineMetadata, error)
}

type RoutineHandle struct {
	bqRoutine BqRoutine
}

func (r RoutineHandle) Create(_ context.Context, _ *resource.Resource) error {
	return errors.FailedPrecondition(EntityRoutine, "create is not supported")
}

func (r RoutineHandle) Update(_ context.Context, _ *resource.Resource) error {
	return errors.FailedPrecondition(EntityRoutine, "update is not supported")
}

func (r RoutineHandle) Exists(ctx context.Context) bool {
	_, err := r.bqRoutine.Metadata(ctx)
	// There can be connection issue, we return false for now
	return err == nil
}

func NewRoutineHandle(bq BqRoutine) *RoutineHandle {
	return &RoutineHandle{bqRoutine: bq}
}
