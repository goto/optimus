package service

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/goto/salt/log"

	"github.com/goto/optimus/core/event"
	"github.com/goto/optimus/core/event/moderator"
	"github.com/goto/optimus/core/job"
	"github.com/goto/optimus/core/resource"
	"github.com/goto/optimus/core/tenant"
	"github.com/goto/optimus/internal/compiler"
	"github.com/goto/optimus/internal/errors"
	"github.com/goto/optimus/internal/utils/filter"
	"github.com/goto/optimus/internal/writer"
)

const (
	projectConfigPrefix = "GLOBAL__"
)

type ResourceRepository interface {
	Create(ctx context.Context, res *resource.Resource) error
	Update(ctx context.Context, res *resource.Resource) error
	Delete(ctx context.Context, res *resource.Resource) error
	ChangeNamespace(ctx context.Context, res *resource.Resource, newTenant tenant.Tenant) error
	ReadByFullName(ctx context.Context, tnnt tenant.Tenant, store resource.Store, fullName string, onlyActive bool) (*resource.Resource, error)
	ReadAll(ctx context.Context, tnnt tenant.Tenant, store resource.Store, onlyActive bool) ([]*resource.Resource, error)
	GetExternal(ctx context.Context, projName tenant.ProjectName, store resource.Store, filters []filter.FilterOpt) ([]*resource.Resource, error)
	GetAllExternal(ctx context.Context, store resource.Store) ([]*resource.Resource, error)
	GetResources(ctx context.Context, tnnt tenant.Tenant, store resource.Store, names []string) ([]*resource.Resource, error)
	ReadByURN(ctx context.Context, tnnt tenant.Tenant, urn resource.URN) (*resource.Resource, error)
	GetExternalCreatFailures(ctx context.Context) ([]*resource.Resource, error)
}

type Syncer interface {
	SyncBatch(ctx context.Context, resources []*resource.Resource) ([]resource.SyncStatus, error)
	TouchUnModified(ctx context.Context, project tenant.ProjectName, resources []*resource.Resource) error
	GetSyncInterval(res *resource.Resource) (int64, error)
	GetETSourceLastModified(ctx context.Context, tnnt tenant.Tenant, resources []*resource.Resource) ([]resource.SourceModifiedTimeStatus, error)
}

type ResourceManager interface {
	CreateResource(ctx context.Context, res *resource.Resource) error
	UpdateResource(ctx context.Context, res *resource.Resource) error
	SyncResource(ctx context.Context, res *resource.Resource) error
	BatchUpdate(ctx context.Context, store resource.Store, resources []*resource.Resource) error
	Validate(res *resource.Resource) error
	GetURN(res *resource.Resource) (resource.URN, error)
	Exist(ctx context.Context, tnnt tenant.Tenant, urn resource.URN) (bool, error)
}

type DownstreamRefresher interface {
	RefreshResourceDownstream(ctx context.Context, resourceURNs []resource.URN, logWriter writer.LogWriter) error
}

type DownstreamResolver interface {
	GetDownstreamByResourceURN(ctx context.Context, tnnt tenant.Tenant, urn resource.URN) (job.DownstreamList, error)
}

type EventHandler interface {
	HandleEvent(moderator.Event)
}

type TenantDetailsGetter interface {
	GetDetails(ctx context.Context, tnnt tenant.Tenant) (*tenant.WithDetails, error)
}

type TemplateCompiler interface {
	Compile(templateMap map[string]string, context map[string]any) (map[string]string, error)
	CompileString(input string, context map[string]any) (string, error)
}

type ResourceService struct {
	repo               ResourceRepository
	statusRepo         StatusRepo
	mgr                ResourceManager
	refresher          DownstreamRefresher
	downstreamResolver DownstreamResolver

	logger       log.Logger
	eventHandler EventHandler
	alertHandler AlertManager

	tenantDetailsGetter TenantDetailsGetter
	compileEngine       TemplateCompiler
	syncer              Syncer
}

type AlertManager interface {
	SendResourceEvent(attr *resource.AlertAttrs)
	SendExternalTableEvent(attr *resource.ETAlertAttrs)
}

func NewResourceService(
	logger log.Logger,
	repo ResourceRepository, downstreamRefresher DownstreamRefresher, mgr ResourceManager,
	eventHandler EventHandler, downstreamResolver DownstreamResolver, alertManager AlertManager,
	tenantDetailsGetter TenantDetailsGetter, compileEngine TemplateCompiler, syncer Syncer, statusRepo StatusRepo,
) *ResourceService {
	return &ResourceService{
		repo:                repo,
		mgr:                 mgr,
		refresher:           downstreamRefresher,
		downstreamResolver:  downstreamResolver,
		logger:              logger,
		eventHandler:        eventHandler,
		alertHandler:        alertManager,
		statusRepo:          statusRepo,
		tenantDetailsGetter: tenantDetailsGetter,
		compileEngine:       compileEngine,
		syncer:              syncer,
	}
}

func (rs ResourceService) Create(ctx context.Context, incoming *resource.Resource) error { // nolint:gocritic
	compiledSpec, err := rs.compileSpec(ctx, incoming)
	if err != nil {
		rs.logger.Warn("suppressed error compiling spec for resource [%s]: %s", incoming.FullName(), err)
		compiledSpec = incoming.Spec()
	}
	incoming.UpdateSpec(compiledSpec)

	err = rs.validateAndGenerateURN(incoming)
	if err != nil {
		return err
	}

	if existing, err := rs.repo.ReadByFullName(ctx, incoming.Tenant(), incoming.Store(), incoming.FullName(), false); err != nil {
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
		if incoming.Kind() == KindExternalTable {
			rs.alertHandler.SendExternalTableEvent(&resource.ETAlertAttrs{
				URN:       incoming.FullName(),
				Tenant:    incoming.Tenant(),
				EventType: "Create Failure",
				Message:   err.Error(),
			})
		}
		rs.logger.Error("error creating resource [%s] to manager: %s", incoming.FullName(), err)
		return err
	}

	rs.raiseCreateEvent(incoming)
	return nil
}

func (rs ResourceService) Update(ctx context.Context, incoming *resource.Resource, logWriter writer.LogWriter) error { // nolint:gocritic
	compiledSpec, err := rs.compileSpec(ctx, incoming)
	if err != nil {
		rs.logger.Warn("suppressed error compiling spec for resource [%s]: %s", incoming.FullName(), err)
		compiledSpec = incoming.Spec()
	}
	incoming.UpdateSpec(compiledSpec)

	err = rs.validateAndGenerateURN(incoming)
	if err != nil {
		return err
	}

	existing, err := rs.repo.ReadByFullName(ctx, incoming.Tenant(), incoming.Store(), incoming.FullName(), true)
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

	rs.raiseUpdateEvent(incoming, existing.GetUpdateImpact(incoming))

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

func (rs ResourceService) Upsert(ctx context.Context, incoming *resource.Resource, logWriter writer.LogWriter) error { // nolint:gocritic
	compiledSpec, err := rs.compileSpec(ctx, incoming)
	if err != nil {
		rs.logger.Warn("suppressed error compiling spec for resource [%s]: %s", incoming.FullName(), err)
		compiledSpec = incoming.Spec()
	}
	incoming.UpdateSpec(compiledSpec)

	err = rs.validateAndGenerateURN(incoming)
	if err != nil {
		return err
	}

	existing, err := rs.repo.ReadByFullName(ctx, incoming.Tenant(), incoming.Store(), incoming.FullName(), false)
	if err != nil {
		if !errors.IsErrorType(err, errors.ErrNotFound) {
			rs.logger.Error("error getting resource [%s]: %s", incoming.FullName(), err)
			return err
		}
	}

	if err := rs.identifyResourceToUpdateOnStore(ctx, incoming, existing); err != nil {
		rs.logger.Error("error identifying resource [%s] before updating on store : %s", incoming.FullName(), err)
		return err
	}

	switch incoming.Status() {
	case resource.StatusToCreate:
		if err := rs.mgr.CreateResource(ctx, incoming); err != nil {
			rs.logger.Error("error creating resource [%s] to manager: %s", incoming.FullName(), err)
			return err
		}
		rs.raiseCreateEvent(incoming)
	case resource.StatusToUpdate:
		if err := rs.mgr.UpdateResource(ctx, incoming); err != nil {
			rs.logger.Error("error updating resource [%s] to manager: %s", incoming.FullName(), err)
			return err
		}
		rs.raiseUpdateEvent(incoming, existing.GetUpdateImpact(incoming))
		if err = rs.handleRefreshDownstream(ctx,
			[]*resource.Resource{incoming},
			map[string]*resource.Resource{existing.FullName(): existing},
			logWriter,
		); err != nil {
			rs.logger.Error("error refreshing downstream for resource [%s]: %s", existing.FullName(), err)
			return err
		}
	}
	return nil
}

func (rs ResourceService) validateAndGenerateURN(incoming *resource.Resource) error {
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

	return nil
}

// helper method to compile resource specs containing template variables
// which is only supported on resource specs with version: 2 and above
func (rs ResourceService) compileSpec(ctx context.Context, res *resource.Resource) (map[string]any, error) {
	if res.Version() < resource.ResourceSpecV2 {
		return res.Spec(), nil
	}

	sourceSpec := res.Spec()
	tnnt, err := rs.tenantDetailsGetter.GetDetails(ctx, res.Tenant())
	if err != nil {
		return nil, err
	}

	tmplCtx := compiler.PrepareContext(
		compiler.From(tnnt.GetVariables()).WithName("proj").WithKeyPrefix(projectConfigPrefix),
	)

	// instead of having to traverse through each field in spec, and compile them individually,
	// we should be able to compile the entire spec at once by treating the spec as a string
	specBytes, err := json.Marshal(sourceSpec)
	if err != nil {
		return nil, err
	}
	specStr := string(specBytes)
	compiledSpecStr, err := rs.compileEngine.CompileString(specStr, tmplCtx)
	if err != nil {
		return nil, err
	}
	var compiledSpec map[string]any
	err = json.Unmarshal([]byte(compiledSpecStr), &compiledSpec)
	if err != nil {
		return nil, err
	}

	return compiledSpec, nil
}

func (rs ResourceService) Delete(ctx context.Context, req *resource.DeleteRequest) (*resource.DeleteResponse, error) {
	existing, err := rs.Get(ctx, req.Tenant, req.Datastore, req.FullName)
	if err != nil {
		return nil, err
	}

	downstreamList, err := rs.downstreamResolver.GetDownstreamByResourceURN(ctx, existing.Tenant(), existing.URN())
	if err != nil {
		return nil, err
	}

	var jobNames string
	if len(downstreamList) > 0 {
		jobNames = downstreamList.GetDownstreamFullNames().String()
		if !req.Force {
			msg := fmt.Sprintf("there are still resource using %s, jobs: [%s]", existing.FullName(), jobNames)
			return nil, errors.NewError(errors.ErrFailedPrecond, resource.EntityResource, msg)
		}
		rs.logger.Info(fmt.Sprintf("attempt to delete resource %s with downstreamJobs: [%s]", existing.FullName(), jobNames))
	}
	_ = existing.MarkToDelete()

	if err = rs.repo.Delete(ctx, existing); err != nil {
		rs.logger.Error("error soft-delete resource [%s] to db: %s", existing.FullName(), err)
		return nil, err
	}

	res := &resource.DeleteResponse{Resource: existing}
	if strings.TrimSpace(jobNames) != "" {
		res.DownstreamJobs = strings.Split(jobNames, ", ")
	}

	rs.raiseDeleteEvent(existing)

	return res, nil
}

func (rs ResourceService) ChangeNamespace(ctx context.Context, datastore resource.Store, resourceFullName string, oldTenant, newTenant tenant.Tenant) error { // nolint:gocritic
	resourceSpec, err := rs.repo.ReadByFullName(ctx, oldTenant, datastore, resourceFullName, true)
	if err != nil {
		rs.logger.Error("failed to read existing resource [%s]: %s", resourceFullName, err)
		return err
	}
	if err := rs.repo.ChangeNamespace(ctx, resourceSpec, newTenant); err != nil {
		rs.logger.Error("error changing namespace of stored resource [%s]: %s", resourceSpec.FullName(), err)
		return err
	}
	resourceSpec.UpdateTenant(newTenant)
	rs.raiseUpdateEvent(resourceSpec, resource.UnspecifiedImpactChange)
	return nil
}

func (rs ResourceService) Get(ctx context.Context, tnnt tenant.Tenant, store resource.Store, resourceFullName string) (*resource.Resource, error) { // nolint:gocritic
	if resourceFullName == "" {
		rs.logger.Error("resource full name is empty")
		return nil, errors.InvalidArgument(resource.EntityResource, "empty resource full name")
	}
	return rs.repo.ReadByFullName(ctx, tnnt, store, resourceFullName, true)
}

func (rs ResourceService) GetAll(ctx context.Context, tnnt tenant.Tenant, store resource.Store) ([]*resource.Resource, error) { // nolint:gocritic
	return rs.repo.ReadAll(ctx, tnnt, store, true)
}

func lastModifiedListToMap(input []resource.SourceModifiedTimeStatus) map[string]resource.SourceModifiedTimeStatus {
	output := make(map[string]resource.SourceModifiedTimeStatus)
	for _, status := range input {
		output[status.FullName] = status
	}
	return output
}

func (rs ResourceService) getExternalTablesDueForSync(ctx context.Context, resources []*resource.Resource) ([]*resource.Resource, []*resource.Resource, error) {
	var toUpdateResources, unModifiedSinceUpdate []*resource.Resource
	mapTenantResources := groupByTenant(resources)
	for tnnt, res := range mapTenantResources {
		lastUpdateMap, err := rs.statusRepo.GetLastUpdateTime(ctx, tnnt.ProjectName(), KindExternalTable, res)
		if err != nil {
			return nil, nil, err
		}
		rs.logger.Info("[ON DB] Fetched last Resource Sync time list ")
		for resName, updateTime := range lastUpdateMap {
			rs.logger.Info(fmt.Sprintf("[ON DB] resource: %s, lastUpdateTime in DB: %s ", resName, updateTime.String()))
		}
		lastModifiedList, err := rs.syncer.GetETSourceLastModified(ctx, tnnt, res)
		rs.logger.Info("[On Drive] Fetched last resource update time list ")
		for _, modifiedTimeStatus := range lastModifiedList {
			if modifiedTimeStatus.Err == nil {
				rs.logger.Info(fmt.Sprintf("[On Drive] resource: %s, lastUpdateTime: %s ", modifiedTimeStatus.FullName, modifiedTimeStatus.LastModifiedTime))
			} else {
				rs.logger.Error(fmt.Sprintf("[On Drive] resource: %s, error: %s ", modifiedTimeStatus.FullName, modifiedTimeStatus.Err.Error()))
			}
		}
		if err != nil {
			return nil, nil, err
		}
		lastModifiedMap := lastModifiedListToMap(lastModifiedList)
		for _, r := range res {
			lastSyncedAt, ok := lastUpdateMap[r.FullName()]
			if !ok {
				toUpdateResources = append(toUpdateResources, r)
				continue
			}
			if r.GetUpdateAt().After(lastSyncedAt) {
				toUpdateResources = append(toUpdateResources, r)
				continue
			}
			if lastModifiedMap[r.FullName()].Err != nil {
				rs.logger.Error(fmt.Sprintf("unable to get last modified time, err:%s", lastModifiedMap[r.FullName()].Err.Error()))
				toUpdateResources = append(toUpdateResources, r)
				continue
			}
			if lastModifiedMap[r.FullName()].LastModifiedTime.After(lastSyncedAt) {
				toUpdateResources = append(toUpdateResources, r)
			} else {
				unModifiedSinceUpdate = append(unModifiedSinceUpdate, r)
			}
		}
	}

	return toUpdateResources, unModifiedSinceUpdate, nil
}

func groupByTenant(resources []*resource.Resource) map[tenant.Tenant][]*resource.Resource {
	grouped := make(map[tenant.Tenant][]*resource.Resource)
	for _, res := range resources {
		tnnt := res.Tenant()
		grouped[tnnt] = append(grouped[tnnt], res)
	}
	return grouped
}

func (rs ResourceService) getExternalTablesWithFilters(ctx context.Context, projectName tenant.ProjectName, store resource.Store, filters ...filter.FilterOpt) ([]*resource.Resource, error) {
	resources, err := rs.repo.GetExternal(ctx, projectName, store, filters)
	if err != nil {
		return nil, err
	}
	if len(resources) == 0 {
		var filterString string
		f := filter.NewFilter(filters...)
		if f.Contains(filter.NamespaceName) {
			filterString += "Namespace Name = " + f.GetStringValue(filter.NamespaceName)
		}
		if f.Contains(filter.TableName) {
			filterString += " Table Name = " + f.GetStringValue(filter.TableName)
		}
		return nil, errors.InvalidArgument(resource.EntityResource, "no resources found for filter: "+filterString)
	}
	return resources, nil
}

func (rs ResourceService) updateLastCheckedUnSyncedETs(ctx context.Context, projectName tenant.ProjectName, tablesWithUnmodifiedSource []*resource.Resource) {
	rs.logger.Info(fmt.Sprintf("Found %d unmodified sources", len(tablesWithUnmodifiedSource)))
	err := rs.syncer.TouchUnModified(ctx, projectName, tablesWithUnmodifiedSource)
	if err != nil {
		rs.logger.Error("unable to update Unmodified Tables last sync attempt, err:", err.Error())
	}
}

func (rs ResourceService) SyncExternalTables(ctx context.Context, projectName tenant.ProjectName, store resource.Store, skipIntervalCheck bool, filters ...filter.FilterOpt) ([]string, error) {
	resources, err := rs.getExternalTablesWithFilters(ctx, projectName, store, filters...)
	if err != nil {
		return nil, err
	}

	var toUpdateResource, tablesWithUnmodifiedSource []*resource.Resource
	if skipIntervalCheck {
		toUpdateResource = resources
	} else {
		toUpdateResource, tablesWithUnmodifiedSource, err = rs.getExternalTablesDueForSync(ctx, resources)
		if err != nil {
			rs.logger.Error("unable to get tables due for syncing, err: ", err.Error())
			return []string{}, err
		}
		rs.updateLastCheckedUnSyncedETs(ctx, projectName, tablesWithUnmodifiedSource)
	}
	if len(toUpdateResource) < 1 {
		return []string{}, nil
	}

	syncStatus, err := rs.syncer.SyncBatch(ctx, toUpdateResource)
	var successTables []string
	for _, i := range syncStatus {
		if i.Success {
			successTables = append(successTables, i.Resource.FullName())
		}
	}
	return successTables, err
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

func (rs ResourceService) GetByURN(ctx context.Context, tnnt tenant.Tenant, urn resource.URN) (*resource.Resource, error) {
	if urn.IsZero() {
		rs.logger.Error("urn is zero value")
		return nil, errors.InvalidArgument(resource.EntityResource, "urn is zero value")
	}

	return rs.repo.ReadByURN(ctx, tnnt, urn)
}

func (rs ResourceService) ExistInStore(ctx context.Context, tnnt tenant.Tenant, urn resource.URN) (bool, error) {
	if urn.IsZero() {
		return false, errors.NewError(errors.ErrInvalidArgument, resource.EntityResource, "urn is zero-valued")
	}

	return rs.mgr.Exist(ctx, tnnt, urn)
}

func (rs ResourceService) Deploy(ctx context.Context, tnnt tenant.Tenant, store resource.Store, incomings []*resource.Resource, logWriter writer.LogWriter) error { // nolint:gocritic
	multiError := errors.NewMultiError("error batch updating resources")
	for _, r := range incomings {
		compiledSpec, err := rs.compileSpec(ctx, r)
		if err != nil {
			rs.logger.Warn("suppressed error compiling spec for resource [%s]: %s", r.FullName(), err)
			compiledSpec = r.Spec()
		}
		r.UpdateSpec(compiledSpec)

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

	existingResources, err := rs.repo.ReadAll(ctx, tnnt, store, false)
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
	for _, r := range toUpdateOnStore {
		switch r.Status() {
		case resource.StatusToCreate:
			toCreate = append(toCreate, r)
		case resource.StatusToUpdate:
			toUpdate = append(toUpdate, r)
		}
	}

	multiError.Append(rs.mgr.BatchUpdate(ctx, store, toUpdateOnStore))

	for _, r := range toCreate {
		rs.raiseCreateEvent(r)
	}

	for _, r := range toUpdate {
		rs.raiseUpdateEvent(r, existingMappedByFullName[r.FullName()].GetUpdateImpact(r))
	}

	if err = rs.handleRefreshDownstream(ctx, toUpdate, existingMappedByFullName, logWriter); err != nil {
		multiError.Append(err)
	}

	return multiError.ToErr()
}

func (rs ResourceService) getResourcesToBatchUpdate(ctx context.Context, incomings []*resource.Resource, existingMappedByFullName map[string]*resource.Resource) ([]*resource.Resource, error) { // nolint:gocritic
	var toUpdateOnStore []*resource.Resource
	me := errors.NewMultiError("error in resources to batch update")

	for _, incoming := range incomings {
		err := rs.identifyResourceToUpdateOnStore(ctx, incoming, existingMappedByFullName[incoming.FullName()])
		if err != nil {
			me.Append(err)
			continue
		}
		if incoming.Status() == resource.StatusToCreate || incoming.Status() == resource.StatusToUpdate {
			toUpdateOnStore = append(toUpdateOnStore, incoming)
		}
	}

	return toUpdateOnStore, me.ToErr()
}

func (rs ResourceService) identifyResourceToUpdateOnStore(ctx context.Context, incoming, existing *resource.Resource) error {
	if incoming.Status() != resource.StatusValidationSuccess {
		return nil
	}

	if existing == nil {
		_ = incoming.MarkToCreate()
		return rs.repo.Create(ctx, incoming)
	}

	if resource.StatusIsSuccess(existing.Status()) && incoming.Equal(existing) {
		_ = incoming.MarkSkipped()
		rs.logger.Warn("resource [%s] is skipped because it has no changes", existing.FullName())
		return nil
	}

	if resource.StatusForToCreate(existing.Status()) {
		_ = incoming.MarkToCreate()
	} else {
		_ = incoming.MarkToUpdate()
	}

	return rs.repo.Update(ctx, incoming)
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

func (rs ResourceService) raiseDeleteEvent(res *resource.Resource) { // nolint:gocritic
	if res.Status() != resource.StatusDeleted {
		return
	}
	rs.alertHandler.SendResourceEvent(&resource.AlertAttrs{
		Name:      res.Name(),
		URN:       res.ConsoleURN(),
		Tenant:    res.Tenant(),
		EventTime: time.Now(),
		EventType: resource.ChangeTypeDelete,
	})
}

func (rs ResourceService) raiseUpdateEvent(res *resource.Resource, impact resource.UpdateImpact) { // nolint:gocritic
	if res.Status() != resource.StatusSuccess {
		return
	}
	rs.alertHandler.SendResourceEvent(&resource.AlertAttrs{
		Name:      res.Name(),
		URN:       res.ConsoleURN(),
		Tenant:    res.Tenant(),
		EventTime: time.Now(),
		EventType: resource.ChangeTypeUpdate,
	})
	ev, err := event.NewResourceUpdatedEvent(res, impact)
	if err != nil {
		rs.logger.Error("error creating event for resource update: %s", err)
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
	var resourceURNsToRefresh []resource.URN
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
			resourceURNsToRefresh = append(resourceURNsToRefresh, incoming.URN())
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
