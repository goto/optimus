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
		allResources, err := w.repo.GetAllExternal(ctx, resource.MaxCompute)
		if err != nil {
			w.logger.Error(fmt.Sprintf("[SyncExternalSheets] failed to get all external resources, err:%s \n", err.Error()))
		}

		toUpdateResource, tablesWithUnmodifiedSource, err := w.resService.getExternalTablesDueForSync(ctx, allResources)
		if err != nil {
			w.logger.Error(fmt.Sprintf("[SyncExternalSheets] unable to get tables due for syncing, err:%s \n", err.Error()))
			continue
		}
		mapTenantResources := groupByTenant(tablesWithUnmodifiedSource)
		for tnnt, resources := range mapTenantResources {
			w.resService.updateLastCheckedUnSyncedETs(ctx, tnnt.ProjectName(), resources)
		}
		var sheetsSyncedCount int
		syncStatus, err := w.resService.syncer.SyncBatch(ctx, toUpdateResource)
		if err != nil {
			w.logger.Error(fmt.Sprintf("[SyncExternalSheets] unable to sync external sheets, err:%s \n", err.Error()))
			continue
		}
		for _, i := range syncStatus {
			if i.Success {
				sheetsSyncedCount++
				w.logger.Info(fmt.Sprintf("[SyncExternalSheets] successfully synced source for Res: %s", i.ResourceName))
			} else {
				// also raise Lark Alert here
				w.logger.Error(fmt.Sprintf("[SyncExternalSheets] failed to sync resource for Res: %s", i.ResourceName))
			}
		}
		w.logger.Info(fmt.Sprintf("[SyncExternalSheets] finished syncing external sheets, sheets synced: %d/%d\n", sheetsSyncedCount, len(toUpdateResource)))
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
			w.logger.Error("unable to scan for resources with status create failure due to auth")
			continue
		}
		if len(failedResources) == 0 {
			continue
		}
		mapTenantResources := groupByTenant(failedResources)
		for tnnt, resources := range mapTenantResources {
			lastModifiedList, err := w.syncer.GetETSourceLastModified(ctx, tnnt, resources)
			lastUpdateTimeMap := lastModifiedListToMap(lastModifiedList)
			if err != nil {
				w.logger.Error("unable to get last modified time for resource")
				continue
			}
			for _, res := range resources {
				w.logger.Info("scan for resource [%s]", res.FullName())
				if lastUpdateTimeMap[res.FullName()].Err != nil {
					w.logger.Warn(fmt.Sprintf("[RetrySheetsAccessIssues] auth issue not yet resolved for resource [%s]", res.FullName()))
					continue
				}
				err := w.mgr.CreateResource(ctx, res)
				if err != nil {
					w.logger.Error(fmt.Sprintf("[RetrySheetsAccessIssues] unable to recreate resource [%s]", res.FullName()))
				}
			}
		}
	}
}
