package maxcompute

import (
	"context"
	"github.com/goto/optimus/core/resource"
	"github.com/goto/optimus/core/tenant"
	"github.com/goto/optimus/internal/errors"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

const (
	accountKey = "DATASTORE_MAXCOMPUTE"
	store      = "MaxComputeStore"
)

type ResourceHandle interface {
	Create(res *resource.Resource) error
}

type TableResourceHandle interface {
	ResourceHandle
}

type Client interface {
	TableHandleFrom() TableResourceHandle
}

type ClientProvider interface {
	Get(account string) (Client, error)
}

type SecretProvider interface {
	GetSecret(ctx context.Context, tnnt tenant.Tenant, key string) (*tenant.PlainTextSecret, error)
}

type MaxCompute struct {
	secretProvider SecretProvider
	clientProvider ClientProvider
}

func (m MaxCompute) Create(ctx context.Context, res *resource.Resource) error {
	spanCtx, span := startChildSpan(ctx, "maxcompute/CreateResource")
	defer span.End()

	account, err := m.secretProvider.GetSecret(spanCtx, res.Tenant(), accountKey)
	if err != nil {
		return err
	}

	odpsClient, err := m.clientProvider.Get(account.Value())
	if err != nil {
		return err
	}

	switch res.Kind() {
	case KindTable:
		handle := odpsClient.TableHandleFrom()
		return handle.Create(res)

	default:
		return errors.InvalidArgument(store, "invalid kind for maxcompute resource "+res.Kind())
	}
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
	return resource.NewURN(resource.MaxCompute.String(), res.FullName())
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

func NewMaxComputeDataStore(secretProvider SecretProvider, clientProvider ClientProvider) *MaxCompute {
	return &MaxCompute{
		secretProvider: secretProvider,
		clientProvider: clientProvider,
	}
}
