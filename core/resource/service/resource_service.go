package service

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"github.com/goto/salt/log"

	"github.com/goto/optimus/core/event"
	"github.com/goto/optimus/core/event/moderator"
	"github.com/goto/optimus/core/job"
	"github.com/goto/optimus/core/resource"
	"github.com/goto/optimus/core/tenant"
	"github.com/goto/optimus/internal/errors"
	"github.com/goto/optimus/internal/writer"
)

type ResourceRepository interface {
	Create(ctx context.Context, res *resource.Resource) error
	Update(ctx context.Context, res *resource.Resource) error
	Delete(ctx context.Context, resourceModel *resource.Resource) error
	ChangeNamespace(ctx context.Context, res *resource.Resource, newTenant tenant.Tenant) error
	ReadByFullName(ctx context.Context, tnnt tenant.Tenant, store resource.Store, fullName string) (*resource.Resource, error)
	ReadAll(ctx context.Context, tnnt tenant.Tenant, store resource.Store) ([]*resource.Resource, error)
	GetResources(ctx context.Context, tnnt tenant.Tenant, store resource.Store, names []string) ([]*resource.Resource, error)
}

type ResourceManager interface {
	CreateResource(ctx context.Context, res *resource.Resource) error
	UpdateResource(ctx context.Context, res *resource.Resource) error
	SyncResource(ctx context.Context, res *resource.Resource) error
	BatchUpdate(ctx context.Context, store resource.Store, resources []*resource.Resource) error
	Validate(res *resource.Resource) error
	GetURN(res *resource.Resource) (string, error)
}

type DownstreamRefresher interface {
	RefreshResourceDownstream(ctx context.Context, resourceURNs []job.ResourceURN, logWriter writer.LogWriter) error
}

type DependencyResolver interface {
	GetDownstreamByDestination(ctx context.Context, tnnt tenant.Tenant, urn job.ResourceURN) (job.DownstreamList, error)
}

type EventHandler interface {
	HandleEvent(moderator.Event)
}

type ResourceService struct {
	repo               ResourceRepository
	mgr                ResourceManager
	refresher          DownstreamRefresher
	dependencyResolver DependencyResolver

	logger       log.Logger
	eventHandler EventHandler
}

func NewResourceService(
	logger log.Logger,
	repo ResourceRepository, downstreamRefresher DownstreamRefresher, mgr ResourceManager,
	eventHandler EventHandler, dependencyResolver DependencyResolver,
) *ResourceService {
	return &ResourceService{
		repo:               repo,
		mgr:                mgr,
		refresher:          downstreamRefresher,
		dependencyResolver: dependencyResolver,
		logger:             logger,
		eventHandler:       eventHandler,
	}
}

func (rs ResourceService) Create(ctx context.Context, incoming *resource.Resource) error { // nolint:gocritic
	if err := rs.mgr.Validate(incoming); err != nil {
		rs.logger.Error("error validating resource [%s]: %s", incoming.FullName(), err)
		return err
	}

	incoming.MarkValidationSuccess()
	urn, err := rs.mgr.GetURN(incoming)
	if err != nil {
		rs.logger.Error("error validating resource [%s]: %s", incoming.FullName(), err)
		return err
	}
	err = incoming.UpdateURN(urn)
	if err != nil {
		rs.logger.Error("error updating urn of resource [%s]: %s", incoming.FullName(), err)
		return err
	}

	if existing, err := rs.repo.ReadByFullName(ctx, incoming.Tenant(), incoming.Store(), incoming.FullName()); err != nil {
		if !errors.IsErrorType(err, errors.ErrNotFound) {
			rs.logger.Error("error getting resource [%s]: %s", incoming.FullName(), err)
			return err
		}
		incoming.MarkToCreate()

		if err := rs.repo.Create(ctx, incoming); err != nil {
			rs.logger.Error("error creating resource [%s] to db: %s", incoming.FullName(), err)
			return err
		}
	} else {
		if existing.Status() == resource.StatusSuccess || existing.Status() == resource.StatusExistInStore {
			return nil // Note: return in case resource already exists
		}
		if !resource.StatusForToCreate(existing.Status()) {
			msg := fmt.Sprintf("cannot create resource [%s] since it already exists with status [%s]", incoming.FullName(), existing.Status())
			rs.logger.Error(msg)
			return errors.InvalidArgument(resource.EntityResource, msg)
		}
		incoming.MarkToCreate()

		if err := rs.repo.Update(ctx, incoming); err != nil {
			rs.logger.Error("error updating resource [%s] to db: %s", incoming.FullName(), err)
			return err
		}
	}

	if err := rs.mgr.CreateResource(ctx, incoming); err != nil {
		rs.logger.Error("error creating resource [%s] to manager: %s", incoming.FullName(), err)
		return err
	}

	rs.raiseCreateEvent(incoming)
	return nil
}

func (rs ResourceService) Update(ctx context.Context, incoming *resource.Resource, logWriter writer.LogWriter) error { // nolint:gocritic
	if err := rs.mgr.Validate(incoming); err != nil {
		rs.logger.Error("error validating resource [%s]: %s", incoming.FullName(), err)
		return err
	}

	incoming.MarkValidationSuccess()
	urn, err := rs.mgr.GetURN(incoming)
	if err != nil {
		rs.logger.Error("error validating resource [%s]: %s", incoming.FullName(), err)
		return err
	}
	err = incoming.UpdateURN(urn)
	if err != nil {
		rs.logger.Error("error updating urn of resource [%s]: %s", incoming.FullName(), err)
		return err
	}

	existing, err := rs.repo.ReadByFullName(ctx, incoming.Tenant(), incoming.Store(), incoming.FullName())
	if err != nil {
		rs.logger.Error("error getting stored resource [%s]: %s", incoming.FullName(), err)
		return err
	}

	if !(resource.StatusForToUpdate(existing.Status())) {
		msg := fmt.Sprintf("cannot update resource [%s] with existing status [%s]", incoming.FullName(), existing.Status())
		rs.logger.Error(msg)
		return errors.InvalidArgument(resource.EntityResource, msg)
	}
	incoming.MarkToUpdate()

	if err := rs.repo.Update(ctx, incoming); err != nil {
		rs.logger.Error("error updating stored resource [%s]: %s", incoming.FullName(), err)
		return err
	}

	if err := rs.mgr.UpdateResource(ctx, incoming); err != nil {
		rs.logger.Error("error updating resource [%s] to manager: %s", incoming.FullName(), err)
		return err
	}

	rs.raiseUpdateEvent(incoming)

	err = rs.handleRefreshDownstream(ctx,
		[]*resource.Resource{incoming},
		map[string]*resource.Resource{existing.FullName(): existing},
		logWriter,
	)
	if err != nil {
		rs.logger.Error("error refreshing downstream for resource [%s]: %s", incoming.FullName(), err)
		return err
	}
	return nil
}

func (rs ResourceService) Delete(ctx context.Context, req *resource.DeleteRequest) (*resource.DeleteResponse, error) {
	existing, err := rs.Get(ctx, req.Tenant, req.Datastore, req.FullName)
	if err != nil {
		return nil, err
	}

	downstreamList, err := rs.dependencyResolver.GetDownstreamByDestination(ctx, existing.Tenant(), job.ResourceURN(existing.URN()))
	if err != nil {
		return nil, err
	}

	var jobNames string
	if len(downstreamList) > 0 {
		jobNames = downstreamList.GetDownstreamFullNames().String()
		if !req.Force {
			msg := fmt.Sprintf("there are still resource downstream using %s, jobs: [%s]", existing.FullName(), jobNames)
			return nil, errors.NewError(errors.ErrFailedPrecond, resource.EntityResource, msg)
		}
		rs.logger.Info(fmt.Sprintf("attempt to delete resource %s with downstreamJobs: [%s]", existing.FullName(), jobNames))
	}
	_ = existing.MarkToDelete()

	err = rs.repo.Delete(ctx, existing)
	if err != nil {
		rs.logger.Error("error deleting resource [%s] to db: %s", existing.FullName(), err)
		return nil, err
	}

	res := &resource.DeleteResponse{Resource: existing}
	if strings.TrimSpace(jobNames) != "" {
		res.DownstreamJobs = strings.Split(jobNames, ", ")
	}

	return res, nil
}

func (rs ResourceService) ChangeNamespace(ctx context.Context, datastore resource.Store, resourceFullName string, oldTenant, newTenant tenant.Tenant) error { // nolint:gocritic
	resourceSpec, err := rs.Get(ctx, oldTenant, datastore, resourceFullName)
	if err != nil {
		rs.logger.Error("failed to read existing resource [%s]: %s", resourceFullName, err)
		return err
	}
	if err := rs.repo.ChangeNamespace(ctx, resourceSpec, newTenant); err != nil {
		rs.logger.Error("error changing namespace of stored resource [%s]: %s", resourceSpec.FullName(), err)
		return err
	}
	resourceSpec.UpdateTenant(newTenant)
	rs.raiseUpdateEvent(resourceSpec)
	return nil
}

func (rs ResourceService) Get(ctx context.Context, tnnt tenant.Tenant, store resource.Store, resourceFullName string) (*resource.Resource, error) { // nolint:gocritic
	if resourceFullName == "" {
		rs.logger.Error("resource full name is empty")
		return nil, errors.InvalidArgument(resource.EntityResource, "empty resource full name")
	}
	res, err := rs.repo.ReadByFullName(ctx, tnnt, store, resourceFullName)
	if err != nil {
		return nil, err
	}
	if res.IsDeleted() {
		return nil, errors.NotFound(resource.EntityResource, "resource are not found or has been deleted")
	}
	return res, nil
}

func (rs ResourceService) GetAll(ctx context.Context, tnnt tenant.Tenant, store resource.Store) ([]*resource.Resource, error) { // nolint:gocritic
	return rs.repo.ReadAll(ctx, tnnt, store)
}

func (rs ResourceService) SyncResources(ctx context.Context, tnnt tenant.Tenant, store resource.Store, names []string) (*resource.SyncResponse, error) { // nolint:gocritic
	resources, err := rs.repo.GetResources(ctx, tnnt, store, names)
	if err != nil {
		rs.logger.Error("error getting resources [%s] from db: %s", strings.Join(names, ", "), err)
		return nil, err
	}

	synced := &resource.SyncResponse{
		IgnoredResources: findMissingResources(names, resources),
	}

	if len(resources) == 0 {
		return synced, nil
	}

	for _, r := range resources {
		err := rs.mgr.SyncResource(ctx, r)
		if err != nil {
			synced.IgnoredResources = append(synced.IgnoredResources, resource.IgnoredResource{
				Name:   r.Name().String(),
				Reason: err.Error(),
			})
			continue
		}
		synced.ResourceNames = append(synced.ResourceNames, r.FullName())
	}

	return synced, nil
}

func (rs ResourceService) Deploy(ctx context.Context, tnnt tenant.Tenant, store resource.Store, incomings []*resource.Resource, logWriter writer.LogWriter) error { // nolint:gocritic
	multiError := errors.NewMultiError("error batch updating resources")
	for _, r := range incomings {
		if err := rs.mgr.Validate(r); err != nil {
			msg := fmt.Sprintf("error validating [%s]: %s", r.FullName(), err)
			multiError.Append(errors.Wrap(resource.EntityResource, msg, err))

			rs.logger.Error(msg)
			r.MarkValidationFailure()
			continue
		}

		urn, err := rs.mgr.GetURN(r)
		if err != nil {
			multiError.Append(err)
			rs.logger.Error("error getting resource urn [%s]: %s", r.FullName(), err)
			continue
		}
		err = r.UpdateURN(urn)
		if err != nil {
			multiError.Append(err)
			rs.logger.Error("error updating urn of resource [%s]: %s", r.FullName(), err)
			continue
		}
		r.MarkValidationSuccess()
	}

	existingResources, err := rs.repo.ReadAll(ctx, tnnt, store)
	if err != nil {
		rs.logger.Error("error reading all existing resources: %s", err)
		multiError.Append(err)
		return multiError.ToErr()
	}
	existingMappedByFullName := createFullNameToResourceMap(existingResources)

	toUpdateOnStore, err := rs.getResourcesToBatchUpdate(ctx, incomings, existingMappedByFullName)
	multiError.Append(err)

	if len(toUpdateOnStore) == 0 {
		rs.logger.Warn("no resources to be batch updated")
		return multiError.ToErr()
	}

	var toCreate []*resource.Resource
	var toUpdate []*resource.Resource
	var toDelete []*resource.Resource
	for _, r := range toUpdateOnStore {
		switch r.Status() {
		case resource.StatusToCreate:
			toCreate = append(toCreate, r)
		case resource.StatusToUpdate:
			toUpdate = append(toUpdate, r)
		case resource.StatusToDelete:
			toDelete = append(toDelete, r)
		}
	}

	multiError.Append(rs.mgr.BatchUpdate(ctx, store, toUpdateOnStore))

	for _, r := range toCreate {
		rs.raiseCreateEvent(r)
	}

	for _, r := range toUpdate {
		rs.raiseUpdateEvent(r)
	}

	for _, r := range toDelete {
		rs.raiseDeleteEvent(r)
	}

	if err = rs.handleRefreshDownstream(ctx, toUpdate, existingMappedByFullName, logWriter); err != nil {
		multiError.Append(err)
	}

	return multiError.ToErr()
}

func (rs ResourceService) getResourcesToBatchUpdate(ctx context.Context, incomings []*resource.Resource, existingMappedByFullName map[string]*resource.Resource) ([]*resource.Resource, error) { // nolint:gocritic
	var toUpdateOnStore []*resource.Resource
	me := errors.NewMultiError("error in resources to batch update")
	incomingByFullName := make(map[string]*resource.Resource)

	for _, incoming := range incomings {
		incomingByFullName[incoming.FullName()] = incoming
		if incoming.Status() != resource.StatusValidationSuccess {
			continue
		}

		existing, ok := existingMappedByFullName[incoming.FullName()]
		if !ok {
			_ = incoming.MarkToCreate()
			err := rs.repo.Create(ctx, incoming)
			if err == nil {
				toUpdateOnStore = append(toUpdateOnStore, incoming)
			}
			me.Append(err)
			continue
		}

		if resource.StatusIsSuccess(existing.Status()) && incoming.Equal(existing) {
			_ = incoming.MarkSkipped()
			rs.logger.Warn("resource [%s] is skipped because it has no changes", existing.FullName())
			continue
		}

		if resource.StatusForToCreate(existing.Status()) {
			_ = incoming.MarkToCreate()
		} else if resource.StatusForToUpdate(existing.Status()) {
			_ = incoming.MarkToUpdate()
		}

		err := rs.repo.Update(ctx, incoming)
		if err == nil {
			toUpdateOnStore = append(toUpdateOnStore, incoming)
		}
		me.Append(err)
	}

	for _, existing := range existingMappedByFullName {
		_, found := incomingByFullName[existing.FullName()]
		if found || existing.IsDeleted() {
			continue
		}

		_ = existing.MarkToDelete()
		err := rs.repo.Update(ctx, existing)
		if err == nil {
			toUpdateOnStore = append(toUpdateOnStore, existing)
		}
		me.Append(err)
	}

	return toUpdateOnStore, me.ToErr()
}

func (rs ResourceService) raiseCreateEvent(res *resource.Resource) { // nolint:gocritic
	if res.Status() != resource.StatusSuccess {
		return
	}

	ev, err := event.NewResourceCreatedEvent(res)
	if err != nil {
		rs.logger.Error("error creating event for resource create: %s", err)
		return
	}
	rs.eventHandler.HandleEvent(ev)
}

func (rs ResourceService) raiseUpdateEvent(res *resource.Resource) { // nolint:gocritic
	if res.Status() != resource.StatusSuccess {
		return
	}

	ev, err := event.NewResourceUpdatedEvent(res)
	if err != nil {
		rs.logger.Error("error creating event for resource update: %s", err)
		return
	}
	rs.eventHandler.HandleEvent(ev)
}

func (rs ResourceService) raiseDeleteEvent(res *resource.Resource) { // nolint:gocritic
	if res.Status() != resource.StatusDeleted {
		return
	}

	ev, err := event.NewResourceDeleteEvent(res)
	if err != nil {
		rs.logger.Error("error creating event for resource delete: %s", err)
		return
	}
	rs.eventHandler.HandleEvent(ev)
}

func (rs ResourceService) handleRefreshDownstream( // nolint:gocritic
	ctx context.Context,
	incomings []*resource.Resource,
	existingMappedByFullName map[string]*resource.Resource,
	logWriter writer.LogWriter,
) error {
	var resourceURNsToRefresh []job.ResourceURN
	for _, incoming := range incomings {
		if incoming.Status() != resource.StatusSuccess {
			continue
		}

		existing, ok := existingMappedByFullName[incoming.FullName()]

		skipMessage := fmt.Sprintf("downstream refresh for resource [%s] is skipped", existing.FullName())
		if !ok {
			rs.logger.Warn(skipMessage)
			logWriter.Write(writer.LogLevelWarning, skipMessage)
			continue
		}

		if rs.isToRefreshDownstream(incoming, existing) {
			resourceURNsToRefresh = append(resourceURNsToRefresh, job.ResourceURN(incoming.URN()))
		} else {
			rs.logger.Warn(skipMessage)
			logWriter.Write(writer.LogLevelWarning, skipMessage)
		}
	}

	if len(resourceURNsToRefresh) == 0 {
		rs.logger.Info("no resource urns to which the refresh will be done upon")
		return nil
	}

	return rs.refresher.RefreshResourceDownstream(ctx, resourceURNsToRefresh, logWriter)
}

func (ResourceService) isToRefreshDownstream(incoming, existing *resource.Resource) bool {
	var key []string
	for k := range incoming.Spec() {
		key = append(key, k)
	}
	for k := range existing.Spec() {
		key = append(key, k)
	}

	// TODO: this is not ideal solution, we need to see how to get these 'special' fields
	for _, k := range key {
		switch strings.ToLower(k) {
		case "view_query", "schema", "source":
			return !reflect.DeepEqual(incoming.Spec()[k], existing.Spec()[k])
		}
	}

	return false
}

func createFullNameToResourceMap(resources []*resource.Resource) map[string]*resource.Resource {
	output := make(map[string]*resource.Resource, len(resources))
	for _, r := range resources {
		output[r.FullName()] = r
	}
	return output
}
