package maxcompute

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"

	"github.com/goto/optimus/core/resource"
	"github.com/goto/optimus/core/tenant"
	"github.com/goto/optimus/internal/errors"
)

const (
	accountKey = "DATASTORE_MAXCOMPUTE"
	store      = "MaxComputeStore"

	maxcomputeID = "maxcompute"

	TableNameSections = 2
)

type ResourceHandle interface {
	Create(res *resource.Resource) error
	Update(res *resource.Resource) error
	Exists(tableName string) bool
}

type TableResourceHandle interface {
	ResourceHandle
}

type Client interface {
	TableHandleFrom() TableResourceHandle
	ViewHandleFrom() TableResourceHandle
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

	case KindView:
		handle := odpsClient.ViewHandleFrom()
		return handle.Create(res)

	default:
		return errors.InvalidArgument(store, "invalid kind for maxcompute resource "+res.Kind())
	}
}

func (m MaxCompute) Update(ctx context.Context, resource *resource.Resource) error {
	spanCtx, span := startChildSpan(ctx, "maxcompute/UpdateteResource")
	defer span.End()

	account, err := m.secretProvider.GetSecret(spanCtx, resource.Tenant(), accountKey)
	if err != nil {
		return err
	}

	odpsClient, err := m.clientProvider.Get(account.Value())
	if err != nil {
		return err
	}

	switch resource.Kind() {
	case KindTable:
		handle := odpsClient.TableHandleFrom()
		return handle.Update(resource)

	default:
		return errors.InvalidArgument(store, "invalid kind for maxcompute resource "+resource.Kind())
	}
}

func (MaxCompute) BatchUpdate(ctx context.Context, resources []*resource.Resource) error {
	return errors.InternalError(resourceSchema, "support for BatchUpdate is not present", nil)
}

func (MaxCompute) Validate(r *resource.Resource) error {
	switch r.Kind() {
	case KindTable:
		table, err := ConvertSpecTo[Table](r)
		if err != nil {
			return err
		}
		table.Name = r.Name()
		return table.Validate()

	case KindView:
		view, err := ConvertSpecTo[View](r)
		if err != nil {
			return err
		}
		view.Name = r.Name()
		return view.Validate()

	default:
		return errors.InvalidArgument(resource.EntityResource, "unknown kind")
	}
}

func (MaxCompute) GetURN(res *resource.Resource) (resource.URN, error) {
	return resource.NewURN(resource.MaxCompute.String(), res.FullName())
}

func (MaxCompute) Backup(ctx context.Context, backup *resource.Backup, resources []*resource.Resource) (*resource.BackupResult, error) {
	return nil, errors.InternalError(resourceSchema, "support for Backup is not present", nil)
}

func (m MaxCompute) Exist(ctx context.Context, tnnt tenant.Tenant, urn resource.URN) (bool, error) {
	spanCtx, span := startChildSpan(ctx, "maxcompute/Exist")
	defer span.End()

	if urn.GetStore() != maxcomputeID {
		msg := fmt.Sprintf("expected store [%s] but received [%s]", maxcomputeID, urn.GetStore())
		return false, errors.InvalidArgument(store, msg)
	}

	account, err := m.secretProvider.GetSecret(spanCtx, tnnt, accountKey)
	if err != nil {
		return false, err
	}

	client, err := m.clientProvider.Get(account.Value())
	if err != nil {
		return false, err
	}

	name, err := resource.NameFrom(urn.GetName())
	if err != nil {
		return false, err
	}

	kindToHandleFn := map[string]func() ResourceHandle{
		KindTable: func() ResourceHandle {
			return client.TableHandleFrom()
		},
	}

	for _, resourceHandleFn := range kindToHandleFn {
		resourceName, err := resourceNameFor(name)
		if err != nil {
			return true, err
		}

		if resourceHandleFn().Exists(resourceName) {
			return true, nil
		}
	}

	return false, nil
}

func startChildSpan(ctx context.Context, name string) (context.Context, trace.Span) {
	tracer := otel.Tracer("datastore/maxcompute")

	return tracer.Start(ctx, name)
}

func resourceNameFor(name resource.Name) (string, error) {
	sections := name.Sections()
	var strName string
	if len(sections) < TableNameSections {
		return "", errors.InvalidArgument(resource.EntityResource, "invalid resource name: "+name.String())
	}
	strName = sections[1]

	if strName == "" {
		return "", errors.InvalidArgument(resource.EntityResource, "invalid resource name: "+name.String())
	}
	return strName, nil
}

func NewMaxComputeDataStore(secretProvider SecretProvider, clientProvider ClientProvider) *MaxCompute {
	return &MaxCompute{
		secretProvider: secretProvider,
		clientProvider: clientProvider,
	}
}
