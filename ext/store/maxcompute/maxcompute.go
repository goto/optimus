package maxcompute

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"

	"github.com/goto/optimus/core/resource"
	"github.com/goto/optimus/core/tenant"
	"github.com/goto/optimus/internal/errors"
)

const accountKey = "DATASTORE_MAXCOMPUTE"

type SecretProvider interface {
	GetSecret(ctx context.Context, tnnt tenant.Tenant, key string) (*tenant.PlainTextSecret, error)
}

type MaxCompute struct {
	secretProvider SecretProvider
}

func (m MaxCompute) Create(ctx context.Context, res *resource.Resource) error {
	//spanCtx, span := startChildSpan(ctx, "bigquery/CreateResource")
	//defer span.End()

	//account, err := m.secretProvider.GetSecret(spanCtx, res.Tenant(), accountKey)
	//if err != nil {
	//	return err
	//}

	// Create a Handler for the maxcompute client
	// Invoke create for the table on the handler
	// Return the result
	return nil
}

func (MaxCompute) Update(ctx context.Context, resource *resource.Resource) error {
	return errors.InternalError(resourceSchema, "support for Update is not present", nil)
}

func (MaxCompute) BatchUpdate(ctx context.Context, resources []*resource.Resource) error {
	return errors.InternalError(resourceSchema, "support for BatchUpdate is not present", nil)
}

func (MaxCompute) Validate(r *resource.Resource) error {
	if r.Kind() == "table" {
		table, err := ConvertSpecTo[Table](r)
		if err != nil {
			return err
		}
		table.Name = r.Name()
		return table.Validate()
	}
	return nil
}

func (MaxCompute) GetURN(res *resource.Resource) (resource.URN, error) {
	return resource.NewURN(resource.Bigquery.String(), res.FullName())
}

func (MaxCompute) Backup(ctx context.Context, backup *resource.Backup, resources []*resource.Resource) (*resource.BackupResult, error) {
	return nil, errors.InternalError(resourceSchema, "support for Backup is not present", nil)
}

func (MaxCompute) Exist(ctx context.Context, tnnt tenant.Tenant, urn resource.URN) (bool, error) {
	return false, errors.InternalError(resourceSchema, "support for Exists is not present", nil)
}

func startChildSpan(ctx context.Context, name string) (context.Context, trace.Span) {
	tracer := otel.Tracer("datastore/maxcompute")

	return tracer.Start(ctx, name)
}
