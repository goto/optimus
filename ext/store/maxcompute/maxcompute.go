package maxcompute

import (
	"context"
	"fmt"
	"time"

	"github.com/goto/salt/log"
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

	accountMaskPolicyKey = "DATASTORE_MAXCOMPUTE_MASK_POLICY"
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
	TableHandleFrom(projectSchema ProjectSchema, maskingPolicyHandle TableMaskingPolicyHandle) TableResourceHandle
	ViewHandleFrom(projectSchema ProjectSchema) TableResourceHandle
	ExternalTableHandleFrom(schema ProjectSchema, getter TenantDetailsGetter, maskingPolicyHandle TableMaskingPolicyHandle) TableResourceHandle
	TableMaskingPolicyHandleFrom(projectSchema ProjectSchema) TableMaskingPolicyHandle
	SchemaHandleFrom(projectSchema ProjectSchema) TableResourceHandle
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
	Touch(ctx context.Context, projectName tenant.ProjectName, entityType string, identifiers []string) error
	UpsertRevision(ctx context.Context, projectName tenant.ProjectName, entityType, identifier string, remarks map[string]string, revision int, success bool) error
}

type MaxCompute struct {
	logger                    log.Logger
	secretProvider            SecretProvider
	clientProvider            ClientProvider
	tenantGetter              TenantDetailsGetter
	SyncRepo                  SyncRepo
	maxFileSizeSupported      int
	driveFileCleanupSizeLimit int
	maxSyncDelayTolerance     time.Duration
}

func (m MaxCompute) Create(ctx context.Context, res *resource.Resource) error {
	spanCtx, span := startChildSpan(ctx, "maxcompute/CreateResource")
	defer span.End()

	odpsClient, err := m.initializeClient(spanCtx, res.Tenant(), accountKey)
	if err != nil {
		return err
	}

	projectSchema, _, err := getCompleteComponentName(res)
	if err != nil {
		return err
	}

	switch res.Kind() {
	case KindTable:
		maskingPolicyClient, err := m.initializeClient(spanCtx, res.Tenant(), accountMaskPolicyKey)
		if err != nil {
			maskingPolicyClient = odpsClient
		}

		handle := odpsClient.TableHandleFrom(projectSchema, maskingPolicyClient.TableMaskingPolicyHandleFrom(projectSchema))
		return handle.Create(res)

	case KindView:
		handle := odpsClient.ViewHandleFrom(projectSchema)
		return handle.Create(res)

	case KindExternalTable:
		syncer := NewSyncer(m.logger, m.secretProvider, m.tenantGetter, m.SyncRepo, m.maxFileSizeSupported, m.driveFileCleanupSizeLimit, m.maxSyncDelayTolerance)
		err = syncer.Sync(ctx, res)
		if err != nil {
			return errors.Wrap(EntityExternalTable, "unable to sync", err)
		}

		maskingPolicyClient, err := m.initializeClient(spanCtx, res.Tenant(), accountMaskPolicyKey)
		if err != nil {
			maskingPolicyClient = odpsClient
		}
		maskingPolicyHandle := maskingPolicyClient.TableMaskingPolicyHandleFrom(projectSchema)

		handle := odpsClient.ExternalTableHandleFrom(projectSchema, m.tenantGetter, maskingPolicyHandle)
		return handle.Create(res)
	case KindSchema:
		handle := odpsClient.SchemaHandleFrom(projectSchema)
		return handle.Create(res)
	default:
		return errors.InvalidArgument(store, "invalid kind for maxcompute resource "+res.Kind())
	}
}

func (m MaxCompute) initializeClient(ctx context.Context, tnnt tenant.Tenant, accountKey string) (Client, error) {
	account, err := m.secretProvider.GetSecret(ctx, tnnt, accountKey)
	if err != nil {
		return nil, err
	}

	return m.clientProvider.Get(account.Value())
}

func (m MaxCompute) Update(ctx context.Context, res *resource.Resource) error {
	spanCtx, span := startChildSpan(ctx, "maxcompute/UpdateResource")
	defer span.End()

	odpsClient, err := m.initializeClient(spanCtx, res.Tenant(), accountKey)
	if err != nil {
		return err
	}

	projectSchema, _, err := getCompleteComponentName(res)
	if err != nil {
		return err
	}

	switch res.Kind() {
	case KindTable:
		maskingPolicyClient, err := m.initializeClient(spanCtx, res.Tenant(), accountMaskPolicyKey)
		if err != nil {
			maskingPolicyClient = odpsClient
		}

		handle := odpsClient.TableHandleFrom(projectSchema, maskingPolicyClient.TableMaskingPolicyHandleFrom(projectSchema))
		return handle.Update(res)

	case KindView:
		handle := odpsClient.ViewHandleFrom(projectSchema)
		return handle.Update(res)

	case KindExternalTable:
		maskingPolicyClient, err := m.initializeClient(spanCtx, res.Tenant(), accountMaskPolicyKey)
		if err != nil {
			maskingPolicyClient = odpsClient
		}
		maskingPolicyHandle := maskingPolicyClient.TableMaskingPolicyHandleFrom(projectSchema)

		handle := odpsClient.ExternalTableHandleFrom(projectSchema, m.tenantGetter, maskingPolicyHandle)
		return handle.Update(res)
	case KindSchema:
		handle := odpsClient.SchemaHandleFrom(projectSchema)
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
		return table.Validate()

	case KindView:
		view, err := ConvertSpecTo[View](r)
		if err != nil {
			return err
		}
		return view.Validate()

	case KindExternalTable:
		extTable, err := ConvertSpecTo[ExternalTable](r)
		if err != nil {
			return err
		}
		return extTable.Validate()
	case KindSchema:
		schema, err := ConvertSpecToSchemaDetails(r)
		if err != nil {
			return err
		}
		return schema.Validate()
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

	client, err := m.initializeClient(spanCtx, tnnt, accountKey)
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

	if !client.SchemaHandleFrom(projectSchema).Exists(name.String()) {
		return false, nil
	}

	kindToHandleFn := map[string]func(projectSchema ProjectSchema) TableResourceHandle{
		KindTable: func(projectSchema ProjectSchema) TableResourceHandle {
			maskingPolicyClient, err := m.initializeClient(spanCtx, tnnt, accountMaskPolicyKey)
			if err != nil {
				maskingPolicyClient = client
			}
			maskingPolicyHandle := maskingPolicyClient.TableMaskingPolicyHandleFrom(projectSchema)

			return client.TableHandleFrom(projectSchema, maskingPolicyHandle)
		},
		KindView: client.ViewHandleFrom,
		KindExternalTable: func(projectSchema ProjectSchema) TableResourceHandle {
			maskingPolicyClient, err := m.initializeClient(spanCtx, tnnt, accountMaskPolicyKey)
			if err != nil {
				maskingPolicyClient = client
			}
			maskingPolicyHandle := maskingPolicyClient.TableMaskingPolicyHandleFrom(projectSchema)

			return client.ExternalTableHandleFrom(projectSchema, m.tenantGetter, maskingPolicyHandle)
		},
	}

	for kind, resourceHandleFn := range kindToHandleFn {
		resourceName, err := resourceNameFor(name, kind)
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

func NewMaxComputeDataStore(logger log.Logger, secretProvider SecretProvider, clientProvider ClientProvider, tenantProvider TenantDetailsGetter, syncRepo SyncRepo, maxFileSizeSupported, driveFileCleanupSizeLimit int, maxSyncDelayTolerance time.Duration) *MaxCompute {
	return &MaxCompute{
		logger:                    logger,
		secretProvider:            secretProvider,
		clientProvider:            clientProvider,
		tenantGetter:              tenantProvider,
		SyncRepo:                  syncRepo,
		maxFileSizeSupported:      maxFileSizeSupported,
		driveFileCleanupSizeLimit: driveFileCleanupSizeLimit,
		maxSyncDelayTolerance:     maxSyncDelayTolerance,
	}
}
