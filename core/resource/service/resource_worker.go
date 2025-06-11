package service

import (
	"context"
	"fmt"
	"time"

	"github.com/goto/salt/log"

	"github.com/goto/optimus/core/resource"
)

type ResourceWorker struct {
	logger                    log.Logger
	repo                      ResourceRepository
	syncer                    Syncer
	mgr                       ResourceManager
	resService                *ResourceService
	AccessIssuesRetryInterval int64
	SourceSyncInterval        int64
}

func NewResourceWorker(logger log.Logger, repo ResourceRepository, syncer Syncer, mgr ResourceManager, resService *ResourceService) *ResourceWorker {
	return &ResourceWorker{
		logger:     logger,
		repo:       repo,
		syncer:     syncer,
		mgr:        mgr,
		resService: resService,
	}
}

func (w *ResourceWorker) UpdateResources(ctx context.Context, resources []*resource.Resource) {
	mapTenantResources := groupByTenant(resources)

	for t, toUpdateResources := range mapTenantResources {
		start := time.Now()
		var sheetsSyncedCount int
		if len(toUpdateResources) < 1 {
			w.logger.Info(fmt.Sprintf("[SyncExternalSheets] [%s] Founds no Sheets to be updated", t))
			continue
		}

		syncStatus, err := w.resService.syncer.SyncBatch(ctx, t, toUpdateResources)
		if err != nil {
			w.logger.Error(fmt.Sprintf("[SyncExternalSheets] [%s] unable to sync external sheets, err:%s", t, err.Error()))
		}
		for _, i := range syncStatus {
			resourceName := i.Identifier
			if i.Success {
				sheetsSyncedCount++
				w.logger.Info(fmt.Sprintf("[SyncExternalSheets] [%s] successfully synced source for Res: %s", t, resourceName))
			} else {
				w.resService.alertHandler.SendExternalTableEvent(&resource.ETAlertAttrs{
					URN:       i.Identifier,
					Tenant:    t,
					EventType: "Sync Failure",
					Message:   i.ErrorMsg,
				})
				w.logger.Error(fmt.Sprintf("[SyncExternalSheets] [%s] failed to sync resource for Res: %s, err: %s", t, resourceName, i.ErrorMsg))
			}
		}

		w.logger.Info(fmt.Sprintf("[SyncExternalSheets] [%s] finished syncing external sheets, sheets synced: %d/%d, Total Time: %s", t, sheetsSyncedCount, len(toUpdateResources), time.Since(start).String()))
	}
}

func (w *ResourceWorker) SyncExternalSheets(ctx context.Context, sourceSyncInterval int64) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			time.Sleep(time.Duration(sourceSyncInterval) * time.Minute)
		}

		w.logger.Info("[SyncExternalSheets] starting to sync external sheets")

		allResources, err := w.repo.GetAllExternal(ctx, resource.MaxCompute)
		if err != nil {
			w.logger.Error(fmt.Sprintf("[SyncExternalSheets] failed to get all external resources, err:%s", err.Error()))
		}

		toSyncResources, tablesWithUnmodifiedSource, err := w.resService.getExternalTablesDueForSync(ctx, allResources)
		if err != nil {
			w.logger.Error(fmt.Sprintf("[SyncExternalSheets] unable to get tables due for syncing, err:%s", err.Error()))
			continue
		}
		mapTenantUnmodifiedResources := groupByTenant(tablesWithUnmodifiedSource)
		for tnnt, resources := range mapTenantUnmodifiedResources {
			w.resService.updateLastCheckedUnSyncedETs(ctx, tnnt.ProjectName(), resources)
		}

		w.UpdateResources(ctx, toSyncResources)
	}
}
func lastModifiedListToMap(input []resource.SourceModifiedTimeStatus) map[string]resource.SourceModifiedTimeStatus {
	output := make(map[string]resource.SourceModifiedTimeStatus)
	for _, status := range input {
		output[status.FullName] = status
	}
	return output
}

func (w *ResourceWorker) RetrySheetsAccessIssues(ctx context.Context, accessIssuesRetryInterval int64) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			time.Sleep(time.Duration(accessIssuesRetryInterval) * time.Minute)
		}

		// get replay requests from DB
		failedResources, err := w.repo.GetExternalCreateFailures(ctx, KindExternalTableGoogle)
		if err != nil {
			w.logger.Error(fmt.Sprintf("[RetrySheetsAccessIssues] unable to scan for resources with status create failure due to auth, err:%s", err.Error()))
			continue
		}
		if len(failedResources) == 0 {
			continue
		}
		mapTenantResources := groupByTenant(failedResources)
		for tnnt, resources := range mapTenantResources {
			lastModifiedList, err := w.syncer.GetGoogleSourceLastModified(ctx, tnnt, resources)
			if err != nil {
				w.logger.Error(fmt.Sprintf("[RetrySheetsAccessIssues] unable to get last modified time for resource, err:%s", err.Error()))
				continue
			}
			lastUpdateTimeMap := lastModifiedListToMap(lastModifiedList)
			for _, res := range resources {
				w.logger.Info("[RetrySheetsAccessIssues] Retrying Create for resource [%s]", res.FullName())
				if lastUpdateTimeMap[res.FullName()].Err != nil {
					w.logger.Warn(fmt.Sprintf("[RetrySheetsAccessIssues] auth issue not yet resolved for resource [%s], got error: [%s]", res.FullName(), lastUpdateTimeMap[res.FullName()].Err))
					continue
				}
				res.MarkStatusUnknown()
				if err := w.resService.mgr.Validate(res); err != nil {
					w.logger.Warn(fmt.Sprintf("[RetrySheetsAccessIssues] resource [%s], found validation issues, got error: [%s]", res.FullName(), err.Error()))
					continue
				}
				if err := res.MarkValidationSuccess(); err != nil {
					w.logger.Warn(fmt.Sprintf("[RetrySheetsAccessIssues] resource [%s], err: [%s]", res.FullName(), err.Error()))
					continue
				}
				if err := res.MarkToCreate(); err != nil {
					w.logger.Warn(fmt.Sprintf("[RetrySheetsAccessIssues] resource [%s], err: [%s]", res.FullName(), err.Error()))
					continue
				}
				err := w.mgr.CreateResource(ctx, res)
				if err != nil {
					w.logger.Error(fmt.Sprintf("[RetrySheetsAccessIssues] unable to recreate resource [%s], err:[%s]", res.FullName(), err.Error()))
					w.resService.alertHandler.SendExternalTableEvent(&resource.ETAlertAttrs{
						URN:       res.FullName(),
						Tenant:    res.Tenant(),
						EventType: "ReCreate Failure",
						Message:   err.Error(),
					})
					continue
				}
				w.logger.Info(fmt.Sprintf("[RetrySheetsAccessIssues] resource:[%s] Successfully recreated", res.FullName()))
			}
		}
	}
}
