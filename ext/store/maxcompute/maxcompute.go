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
	TableHandleFrom(projectSchema ProjectSchema) TableResourceHandle
	ViewHandleFrom(projectSchema ProjectSchema) TableResourceHandle
	ExternalTableHandleFrom(schema ProjectSchema, getter TenantDetailsGetter) TableResourceHandle
}

type ClientProvider interface {
	Get(account string) (Client, error)
}

type SecretProvider interface {
	GetSecret(ctx context.Context, tnnt tenant.Tenant, key string) (*tenant.PlainTextSecret, error)
}

type TenantDetailsGetter interface {
	GetDetails(ctx context.Context, tnnt tenant.Tenant) (*tenant.WithDetails, error)
}

type SyncRepo interface {
	Upsert(ctx context.Context, projectName tenant.ProjectName, entityType, identifier string, remarks map[string]string, success bool) error
}

type MaxCompute struct {
	secretProvider SecretProvider
	clientProvider ClientProvider
	tenantGetter   TenantDetailsGetter
	SyncRepo       SyncRepo
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

	projectSchema, _, err := getCompleteComponentName(res)
	if err != nil {
		return err
	}

	switch res.Kind() {
	case KindTable:
		handle := odpsClient.TableHandleFrom(projectSchema)
		return handle.Create(res)

	case KindView:
		handle := odpsClient.ViewHandleFrom(projectSchema)
		return handle.Create(res)

	case KindExternalTable:
		syncer := NewSyncer(m.secretProvider, m.tenantGetter, m.SyncRepo)
		err = syncer.Sync(ctx, res)
		if err != nil {
			return errors.Wrap(EntityExternalTable, "unable to sync", err)
		}

		handle := odpsClient.ExternalTableHandleFrom(projectSchema, m.tenantGetter)
		return handle.Create(res)
	default:
		return errors.InvalidArgument(store, "invalid kind for maxcompute resource "+res.Kind())
	}
}

func (m MaxCompute) Update(ctx context.Context, res *resource.Resource) error {
	spanCtx, span := startChildSpan(ctx, "maxcompute/UpdateResource")
	defer span.End()

	account, err := m.secretProvider.GetSecret(spanCtx, res.Tenant(), accountKey)
	if err != nil {
		return err
	}

	odpsClient, err := m.clientProvider.Get(account.Value())
	if err != nil {
		return err
	}

	projectSchema, _, err := getCompleteComponentName(res)
	if err != nil {
		return err
	}

	switch res.Kind() {
	case KindTable:
		handle := odpsClient.TableHandleFrom(projectSchema)
		return handle.Update(res)

	case KindView:
		handle := odpsClient.ViewHandleFrom(projectSchema)
		return handle.Update(res)

	case KindExternalTable:
		handle := odpsClient.ExternalTableHandleFrom(projectSchema, m.tenantGetter)
		return handle.Update(res)

	default:
		return errors.InvalidArgument(store, "invalid kind for maxcompute resource "+res.Kind())
	}
}

func (MaxCompute) BatchUpdate(_ context.Context, _ []*resource.Resource) error {
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

	case KindExternalTable:
		extTable, err := ConvertSpecTo[ExternalTable](r)
		if err != nil {
			return err
		}
		extTable.Name = r.Name()
		return extTable.Validate()

	default:
		return errors.InvalidArgument(resource.EntityResource, "unknown kind")
	}
}

func (MaxCompute) GetURN(res *resource.Resource) (resource.URN, error) {
	return URNFor(res)
}

func (MaxCompute) Backup(_ context.Context, _ *resource.Backup, _ []*resource.Resource) (*resource.BackupResult, error) {
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

	projectSchema, err := ProjectSchemaFor(name)
	if err != nil {
		return false, err
	}

	kindToHandleFn := map[string]func(projectSchema ProjectSchema) TableResourceHandle{
		KindTable: client.TableHandleFrom,
		KindView:  client.ViewHandleFrom,
		KindExternalTable: func(projectSchema ProjectSchema) TableResourceHandle {
			return client.ExternalTableHandleFrom(projectSchema, m.tenantGetter)
		},
	}

	for _, resourceHandleFn := range kindToHandleFn {
		resourceName, err := resourceNameFor(name)
		if err != nil {
			return true, err
		}

		if resourceHandleFn(projectSchema).Exists(resourceName) {
			return true, nil
		}
	}

	return false, nil
}

func startChildSpan(ctx context.Context, name string) (context.Context, trace.Span) {
	tracer := otel.Tracer("datastore/maxcompute")

	return tracer.Start(ctx, name)
}

func NewMaxComputeDataStore(secretProvider SecretProvider, clientProvider ClientProvider, tenantProvider TenantDetailsGetter, syncRepo SyncRepo) *MaxCompute {
	return &MaxCompute{
		secretProvider: secretProvider,
		clientProvider: clientProvider,
		tenantGetter:   tenantProvider,
		SyncRepo:       syncRepo,
	}
}
