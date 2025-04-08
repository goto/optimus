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

func (w *ResourceWorker) SyncExternalSheets(ctx context.Context, sourceSyncInterval int64) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			time.Sleep(time.Duration(sourceSyncInterval) * time.Minute)
		}

		w.logger.Info("[SyncExternalSheets] starting to sync external sheets")
		start := time.Now()
		allResources, err := w.repo.GetAllExternal(ctx, resource.MaxCompute)
		if err != nil {
			w.logger.Error(fmt.Sprintf("[SyncExternalSheets] failed to get all external resources, err:%s", err.Error()))
		}

		toUpdateResources, tablesWithUnmodifiedSource, err := w.resService.getExternalTablesDueForSync(ctx, allResources)
		if err != nil {
			w.logger.Error(fmt.Sprintf("[SyncExternalSheets] unable to get tables due for syncing, err:%s", err.Error()))
			continue
		}
		mapTenantResources := groupByTenant(tablesWithUnmodifiedSource)
		for tnnt, resources := range mapTenantResources {
			w.resService.updateLastCheckedUnSyncedETs(ctx, tnnt.ProjectName(), resources)
		}
		var sheetsSyncedCount int
		syncStatus, err := w.resService.syncer.SyncBatch(ctx, toUpdateResources)
		if err != nil {
			w.logger.Error(fmt.Sprintf("[SyncExternalSheets] unable to sync external sheets, err:%s", err.Error()))
		}
		for _, i := range syncStatus {
			if i.Success {
				sheetsSyncedCount++
				w.logger.Info(fmt.Sprintf("[SyncExternalSheets] successfully synced source for Res: %s", i.Resource.FullName()))
			} else {
				w.resService.alertHandler.SendExternalTableEvent(&resource.ETAlertAttrs{
					URN:       i.Resource.FullName(),
					Tenant:    i.Resource.Tenant(),
					EventType: "Sync Failure",
					Message:   err.Error(),
				})
				w.logger.Error(fmt.Sprintf("[SyncExternalSheets] failed to sync resource for Res: %s, err: %s", i.Resource.FullName(), i.ErrorMsg))
			}
		}

		w.logger.Info(fmt.Sprintf("[SyncExternalSheets] finished syncing external sheets, sheets synced: %d/%d, Total Time: %s", sheetsSyncedCount, len(toUpdateResources), time.Since(start).String()))
	}
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
		failedResources, err := w.repo.GetExternalCreatAuthFailures(ctx)
		if err != nil {
			w.logger.Error(fmt.Sprintf("[RetrySheetsAccessIssues] unable to scan for resources with status create failure due to auth, err:%s", err.Error()))
			continue
		}
		if len(failedResources) == 0 {
			continue
		}
		mapTenantResources := groupByTenant(failedResources)
		for tnnt, resources := range mapTenantResources {
			lastModifiedList, err := w.syncer.GetETSourceLastModified(ctx, tnnt, resources)
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
