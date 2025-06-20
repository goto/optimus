package service

import (
	"context"
	"fmt"

	"github.com/goto/salt/log"

	"github.com/goto/optimus/core/resource"
	"github.com/goto/optimus/core/tenant"
	"github.com/goto/optimus/internal/errors"
)

const (
	KindExternalTable       string = "external_table"
	KindExternalTableGoogle string = "external_table_google"
	KindExternalTableLark   string = "external_table_lark"
)

type DataStore interface {
	Create(context.Context, *resource.Resource) error
	Update(context.Context, *resource.Resource) error
	BatchUpdate(context.Context, []*resource.Resource) error
	Validate(*resource.Resource) error
	GetURN(res *resource.Resource) (resource.URN, error)
	Backup(context.Context, *resource.Backup, []*resource.Resource) (*resource.BackupResult, error)
	Exist(ctx context.Context, tnnt tenant.Tenant, urn resource.URN) (bool, error)
}

type ResourceStatusRepo interface {
	UpdateStatus(ctx context.Context, res ...*resource.Resource) error
}

type StatusRepo interface {
	GetLastUpdate(ctx context.Context, projectName tenant.ProjectName, resources []*resource.Resource) (map[string]*resource.SourceVersioningInfo, error)
}

type ResourceMgr struct {
	datastoreMap map[resource.Store]DataStore

	repo ResourceStatusRepo

	logger log.Logger
}

func (m *ResourceMgr) CreateResource(ctx context.Context, res *resource.Resource) error {
	store := res.Store()
	datastore, ok := m.datastoreMap[store]
	if !ok {
		msg := fmt.Sprintf("datastore [%s] for resource [%s] is not found", store.String(), res.FullName())
		m.logger.Error(msg)
		return errors.InternalError(resource.EntityResource, msg, nil)
	}

	me := errors.NewMultiError("error in create resource")
	err := datastore.Create(ctx, res)
	if err != nil {
		m.logger.Error("error creating resource [%s] to datastore [%s]: %s", res.FullName(), store.String(), err)
		if errors.IsErrorType(err, errors.ErrAlreadyExists) {
			me.Append(res.MarkExistInStore())
		} else {
			me.Append(err)
			me.Append(res.MarkFailure())
		}
	} else {
		me.Append(res.MarkSuccess())
	}

	me.Append(m.repo.UpdateStatus(ctx, res))
	return me.ToErr()
}

func (m *ResourceMgr) UpdateResource(ctx context.Context, res *resource.Resource) error {
	store := res.Store()
	datastore, ok := m.datastoreMap[store]
	if !ok {
		msg := fmt.Sprintf("datastore [%s] for resource [%s] is not found", store.String(), res.FullName())
		m.logger.Error(msg)
		return errors.InternalError(resource.EntityResource, msg, nil)
	}

	me := errors.NewMultiError("error in update resource")
	if err := datastore.Update(ctx, res); err != nil {
		me.Append(err)
		me.Append(res.MarkFailure())
		m.logger.Error("error updating resource [%s] to datastore [%s]: %s", res.FullName(), store.String(), err)
	} else {
		me.Append(res.MarkSuccess())
	}

	me.Append(m.repo.UpdateStatus(ctx, res))
	return me.ToErr()
}

func (m *ResourceMgr) SyncResource(ctx context.Context, res *resource.Resource) error {
	store := res.Store()
	datastore, ok := m.datastoreMap[store]
	if !ok {
		msg := fmt.Sprintf("datastore [%s] for resource [%s] is not found", store.String(), res.FullName())
		m.logger.Error(msg)
		return errors.InternalError(resource.EntityResource, msg, nil)
	}

	err := datastore.Create(ctx, res)
	if err != nil {
		if !errors.IsErrorType(err, errors.ErrAlreadyExists) {
			return errors.AddErrContext(err, resource.EntityResource, "unable to create on datastore")
		} else if errUpdate := datastore.Update(ctx, res); errUpdate != nil {
			return errors.AddErrContext(errUpdate, resource.EntityResource, "unable to update on datastore")
		}
	}

	resNew := resource.FromExisting(res, resource.ReplaceStatus(resource.StatusSuccess))
	if err := m.repo.UpdateStatus(ctx, resNew); err != nil {
		return errors.AddErrContext(err, resource.EntityResource, "unable to update status in database")
	}

	return nil
}

func (m *ResourceMgr) Validate(res *resource.Resource) error {
	store := res.Store()
	datastore, ok := m.datastoreMap[store]
	if !ok {
		msg := fmt.Sprintf("datastore [%s] for resource [%s] is not found", store.String(), res.FullName())
		m.logger.Error(msg)
		return errors.InternalError(resource.EntityResource, msg, nil)
	}

	return datastore.Validate(res)
}

func (m *ResourceMgr) GetURN(res *resource.Resource) (resource.URN, error) {
	store := res.Store()
	datastore, ok := m.datastoreMap[store]
	if !ok {
		msg := fmt.Sprintf("datastore [%s] for resource [%s] is not found", store.String(), res.FullName())
		m.logger.Error(msg)
		return resource.ZeroURN(), errors.InternalError(resource.EntityResource, msg, nil)
	}

	return datastore.GetURN(res)
}

func (m *ResourceMgr) BatchUpdate(ctx context.Context, store resource.Store, resources []*resource.Resource) error {
	datastore, ok := m.datastoreMap[store]
	if !ok {
		m.logger.Error("datastore [%s]  is not found", store.String())
		return errors.InvalidArgument(resource.EntityResource, "data store service not found for "+store.String())
	}

	me := errors.NewMultiError("error in batch update")
	me.Append(datastore.BatchUpdate(ctx, resources))
	me.Append(m.repo.UpdateStatus(ctx, resources...))

	return me.ToErr()
}

func (m *ResourceMgr) Backup(ctx context.Context, details *resource.Backup, resources []*resource.Resource) (*resource.BackupResult, error) {
	datastore, ok := m.datastoreMap[details.Store()]
	if !ok {
		m.logger.Error("datastore [%s] is not found", details.Store())
		return nil, errors.InvalidArgument(resource.EntityResource, "data store service not found for "+details.Store().String())
	}

	return datastore.Backup(ctx, details, resources)
}

func (m *ResourceMgr) RegisterDatastore(store resource.Store, dataStore DataStore) {
	m.datastoreMap[store] = dataStore
}

func (m *ResourceMgr) Exist(ctx context.Context, tnnt tenant.Tenant, urn resource.URN) (bool, error) {
	if urn.IsZero() {
		return false, errors.InvalidArgument(resource.EntityResource, "urn is zero-valued")
	}

	store, err := resource.FromStringToStore(urn.GetStore())
	if err != nil {
		return false, err
	}

	datastore, ok := m.datastoreMap[store]
	if !ok {
		msg := fmt.Sprintf("datastore [%s] is not found", store)
		m.logger.Error(msg)
		return false, errors.InternalError(resource.EntityResource, msg, nil)
	}

	return datastore.Exist(ctx, tnnt, urn)
}

func NewResourceManager(repo ResourceStatusRepo, logger log.Logger) *ResourceMgr {
	return &ResourceMgr{
		repo:         repo,
		datastoreMap: map[resource.Store]DataStore{},
		logger:       logger,
	}
}
