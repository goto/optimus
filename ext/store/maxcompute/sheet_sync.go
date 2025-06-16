package maxcompute

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss"
	"github.com/goto/salt/log"

	"github.com/goto/optimus/core/resource"
	"github.com/goto/optimus/core/tenant"
	bucket "github.com/goto/optimus/ext/bucket/oss"
	"github.com/goto/optimus/ext/sheets/csv"
	"github.com/goto/optimus/ext/sheets/gdrive"
	"github.com/goto/optimus/ext/sheets/gsheet"
	"github.com/goto/optimus/ext/sheets/lark"
	"github.com/goto/optimus/internal/errors"
	"github.com/goto/optimus/internal/lib/pool"
)

const (
	GsheetCredsKey          = "GOOGLE_SHEETS_ACCOUNT"
	LarkCredentialsKey      = "LARK_SHEETS_ACCOUNT"
	OSSCredsKey             = "OSS_CREDS"
	ExtLocation             = "EXT_LOCATION"
	MaxSyncInterval         = 24
	headersCountSerde       = "odps.text.option.header.lines.count"
	UseQuoteSerde           = "odps.text.option.use.quote"
	AssumeRoleSerde         = "odps.properties.rolearn"
	AssumeRoleProjectConfig = "EXTERNAL_TABLE_ASSUME_RAM_USER"
)

var validInfinityValues = map[string]struct{}{
	"Infinity":  {},
	"+Infinity": {},
	"-Infinity": {},
	"+Inf":      {},
	"Inf":       {},
	"-Inf":      {},
}

type SyncerService struct {
	logger                    log.Logger
	secretProvider            SecretProvider
	tenantDetailsGetter       TenantDetailsGetter
	SyncRepo                  SyncRepo
	MaxSyncDelayTolerance     time.Duration
	maxFileSizeSupported      int
	driveFileCleanupSizeLimit int
}

func NewSyncer(log log.Logger, secretProvider SecretProvider, tenantDetailsGetter TenantDetailsGetter,
	syncRepo SyncRepo, maxFileSizeSupported, driveFileCleanupSizeLimit int, maxSyncDelayTolerance time.Duration,
) *SyncerService {
	return &SyncerService{
		logger:                    log,
		secretProvider:            secretProvider,
		tenantDetailsGetter:       tenantDetailsGetter,
		SyncRepo:                  syncRepo,
		maxFileSizeSupported:      maxFileSizeSupported,
		driveFileCleanupSizeLimit: driveFileCleanupSizeLimit,
		MaxSyncDelayTolerance:     maxSyncDelayTolerance,
	}
}

func (s *SyncerService) TouchUnModified(ctx context.Context, projectName tenant.ProjectName, resources []*resource.Resource) error {
	ets, err := ConvertSpecsTo[ExternalTable](resources)
	if err != nil {
		return err
	}
	etSourceMap := groupBySourceType(ets)
	me := errors.NewMultiError("error while update last sync attempt time")
	for sourceType, externalTables := range etSourceMap {
		switch sourceType {
		case GoogleSheet, GoogleDrive:
			tableIdentifiers := make([]string, len(externalTables))
			for i, table := range externalTables {
				tableIdentifiers[i] = table.FullName()
			}
			me.Append(s.SyncRepo.Touch(ctx, projectName, KindExternalTableGoogle, tableIdentifiers))
		case LarkSheet:
			tableIdentifiers := make([]string, len(externalTables))
			for i, table := range externalTables {
				tableIdentifiers[i] = table.FullName()
			}
			me.Append(s.SyncRepo.Touch(ctx, projectName, KindExternalTableLark, tableIdentifiers))
		}
	}
	return me.ToErr()
}

func getAllSourceTypes(et []*ExternalTable) ExternalTableSources {
	sourceTypes := make(ExternalTableSources, 0)
	for _, spec := range et {
		sourceTypeString := spec.GetSourceType()
		sourceTypes.Append(sourceTypeString)
	}
	return sourceTypes
}

func groupBySourceType(ets []*ExternalTable) map[ExternalTableSourceType][]*ExternalTable {
	grouped := make(map[ExternalTableSourceType][]*ExternalTable)
	for _, et := range ets {
		if grouped[et.GetSourceType()] == nil {
			grouped[et.GetSourceType()] = make([]*ExternalTable, 0)
		}
		grouped[et.GetSourceType()] = append(grouped[et.GetSourceType()], et)
	}
	return grouped
}

func revisionListToMap(input []resource.SourceModifiedRevisionStatus) map[string]resource.SourceModifiedRevisionStatus {
	output := make(map[string]resource.SourceModifiedRevisionStatus)
	for _, status := range input {
		output[status.FullName] = status
	}
	return output
}

func lastModifiedListToMap(input []resource.SourceModifiedTimeStatus) map[string]resource.SourceModifiedTimeStatus {
	output := make(map[string]resource.SourceModifiedTimeStatus)
	for _, status := range input {
		output[status.FullName] = status
	}
	return output
}

func (s *SyncerService) getGoogleExternalTablesDueForSync(ctx context.Context, tnnt tenant.Tenant, ets []*ExternalTable, lastUpdateMap map[string]*resource.SourceVersioningInfo) ([]*ExternalTable, []*ExternalTable, error) {
	var toUpdateExternalTables, unModifiedSinceUpdate []*ExternalTable

	for resName, versionInfo := range lastUpdateMap {
		s.logger.Info(fmt.Sprintf("[ON DB] [Google] resource: %s, lastUpdateTime in DB: %s ", resName, versionInfo.ModifiedTime))
	}
	lastSourceModifiedList, err := s.getGoogleSourceLastModified(ctx, tnnt, ets)
	s.logger.Info("[On Drive] [Google] Fetched last resource update time list ")
	for _, modifiedTimeStatus := range lastSourceModifiedList {
		if modifiedTimeStatus.Err == nil {
			s.logger.Info(fmt.Sprintf("[On Drive] [Google] resource: %s, lastUpdateTime: %s ", modifiedTimeStatus.FullName, modifiedTimeStatus.LastModifiedTime))
		} else {
			s.logger.Error(fmt.Sprintf("[On Drive] [Google] resource: %s, error: %s ", modifiedTimeStatus.FullName, modifiedTimeStatus.Err.Error()))
		}
	}
	if err != nil {
		return nil, nil, err
	}
	lastSourceModifiedMap := lastModifiedListToMap(lastSourceModifiedList)
	for _, et := range ets {
		lastSyncedAt, ok := lastUpdateMap[et.FullName()]
		if !ok {
			toUpdateExternalTables = append(toUpdateExternalTables, et)
			continue
		}
		if lastSourceModifiedMap[et.FullName()].Err != nil {
			s.logger.Error(fmt.Sprintf("unable to get last modified time, err:%s", lastSourceModifiedMap[et.FullName()].Err.Error()))
			toUpdateExternalTables = append(toUpdateExternalTables, et)
			continue
		}
		if lastSourceModifiedMap[et.FullName()].LastModifiedTime.After(lastSyncedAt.ModifiedTime) {
			toUpdateExternalTables = append(toUpdateExternalTables, et)
		} else {
			unModifiedSinceUpdate = append(unModifiedSinceUpdate, et)
		}
	}
	return toUpdateExternalTables, unModifiedSinceUpdate, nil
}

func (s *SyncerService) getLarkExternalTablesDueForSync(ctx context.Context, tnnt tenant.Tenant, ets []*ExternalTable, lastUpdateMap map[string]*resource.SourceVersioningInfo) ([]*ExternalTable, []*ExternalTable, error) {
	var toUpdateExternalTables, unModifiedSinceUpdate []*ExternalTable

	s.logger.Info("[ON DB] [Lark] Fetched last Resource Sync time list ")
	for resName, versionInfo := range lastUpdateMap {
		s.logger.Info(fmt.Sprintf("[ON DB] [Lark] resource: %s, lastUpdateTime in DB: %s, Last Synced Revision: %d", resName, versionInfo.ModifiedTime.String(), versionInfo.Revision))
	}
	latestRevisionList, err := s.getLarkRevisionIDs(ctx, tnnt, ets)
	s.logger.Info("[On Lark] Fetched last resource update time list ")
	for _, latestRevision := range latestRevisionList {
		if latestRevision.Err == nil {
			s.logger.Info(fmt.Sprintf("[On Lark] resource: %s, latest Revision: %d ", latestRevision.FullName, latestRevision.Revision))
		} else {
			s.logger.Error(fmt.Sprintf("[On Lark] resource: %s, error: %s ", latestRevision.FullName, latestRevision.Err.Error()))
		}
	}
	if err != nil {
		return nil, nil, err
	}
	sourceRevisionMap := revisionListToMap(latestRevisionList)
	for _, r := range ets {
		lastSyncedAt, ok := lastUpdateMap[r.FullName()]
		if !ok {
			toUpdateExternalTables = append(toUpdateExternalTables, r)
			continue
		}
		if sourceRevisionMap[r.FullName()].Err != nil {
			s.logger.Error(fmt.Sprintf("[On Lark] unable to get current revision, err:%s", sourceRevisionMap[r.FullName()].Err.Error()))
			toUpdateExternalTables = append(toUpdateExternalTables, r)
			continue
		}
		if sourceRevisionMap[r.FullName()].Revision > lastSyncedAt.Revision {
			toUpdateExternalTables = append(toUpdateExternalTables, r)
		} else {
			unModifiedSinceUpdate = append(unModifiedSinceUpdate, r)
		}
	}
	return toUpdateExternalTables, unModifiedSinceUpdate, nil
}

func getETNameToResourceMap(resources []*resource.Resource) (map[string]*resource.Resource, error) {
	output := make(map[string]*resource.Resource)
	for _, r := range resources {
		et, err := ConvertSpecTo[ExternalTable](r)
		if err != nil {
			return nil, err
		}
		output[et.FullName()] = r
	}
	return output, nil
}

func maxDuration(d1, d2 time.Duration) time.Duration {
	if d1 > d2 {
		return d1
	}
	return d2
}

func (s *SyncerService) GetExternalTablesDueForSync(ctx context.Context, tnnt tenant.Tenant, resources []*resource.Resource, lastUpdateMap map[string]*resource.SourceVersioningInfo) ([]*resource.Resource, []*resource.Resource, error) {
	var toUpdateResources, unModifiedSinceUpdate []*resource.Resource
	etNameResourcesMap, err := getETNameToResourceMap(resources)
	if err != nil {
		return nil, nil, err
	}

	ets, err := ConvertSpecsTo[ExternalTable](resources)
	if err != nil {
		return nil, nil, err
	}

	externalTablesBySourceTypes := groupBySourceType(ets)
	for sourceType, externalTables := range externalTablesBySourceTypes {
		var toUpdateET, unModifiedET []*ExternalTable
		switch sourceType {
		case GoogleSheet:
			var externalTablesToCheck []*ExternalTable
			for _, externalTable := range externalTables {
				configuredSyncDelayTolerance := externalTable.GetSyncDelayTolerance()
				syncDelayTolerance := maxDuration(s.MaxSyncDelayTolerance, configuredSyncDelayTolerance)
				if syncDelayTolerance == 0 {
					externalTablesToCheck = append(externalTablesToCheck, externalTable)
					continue
				}
				resourceName := etNameResourcesMap[externalTable.FullName()].FullName()

				if lastUpdateMap[resourceName].ModifiedTime.Add(syncDelayTolerance).Before(time.Now()) {
					toUpdateResources = append(toUpdateResources, etNameResourcesMap[externalTable.FullName()])
					continue
				}
				externalTablesToCheck = append(externalTablesToCheck, externalTable)
			}
			toUpdateET, unModifiedET, err = s.getGoogleExternalTablesDueForSync(ctx, tnnt, externalTablesToCheck, lastUpdateMap)
			if err != nil {
				return nil, nil, err
			}
		case GoogleDrive:
			toUpdateET, unModifiedET, err = s.getGoogleExternalTablesDueForSync(ctx, tnnt, externalTables, lastUpdateMap)
			if err != nil {
				return nil, nil, err
			}
		case LarkSheet:
			toUpdateET, unModifiedET, err = s.getLarkExternalTablesDueForSync(ctx, tnnt, externalTables, lastUpdateMap)
			if err != nil {
				return nil, nil, err
			}
		}
		for _, et := range toUpdateET {
			toUpdateResources = append(toUpdateResources, etNameResourcesMap[et.FullName()])
		}
		for _, et := range unModifiedET {
			unModifiedSinceUpdate = append(unModifiedSinceUpdate, etNameResourcesMap[et.FullName()])
		}
	}
	return toUpdateResources, unModifiedSinceUpdate, nil
}

func (s *SyncerService) SyncBatch(ctx context.Context, tnnt tenant.Tenant, resources []*resource.Resource) ([]resource.SyncStatus, error) {
	if len(resources) == 0 {
		return []resource.SyncStatus{}, nil
	}

	tenantWithDetails, err := s.tenantDetailsGetter.GetDetails(ctx, tnnt)
	if err != nil {
		return nil, err
	}

	commonLocation, err := tenantWithDetails.GetConfig(ExtLocation)
	if err != nil {
		if !errors.IsErrorType(err, errors.ErrNotFound) {
			return nil, err
		}
	}

	externalTables, err := ConvertSpecsTo[ExternalTable](resources)
	if err != nil {
		return nil, err
	}
	sourceTypes := getAllSourceTypes(externalTables)
	externalTableClients, err := s.getExtTableClients(ctx, tnnt, sourceTypes)
	if err != nil {
		return nil, err
	}

	var jobs []func() pool.JobResult[*resource.Resource]
	for _, r := range resources {
		r := r
		f1 := func() pool.JobResult[*resource.Resource] {
			err = processResource(ctx, s.SyncRepo, externalTableClients, r, commonLocation)
			if err != nil {
				return pool.JobResult[*resource.Resource]{Output: r, Err: err}
			}
			return pool.JobResult[*resource.Resource]{Output: r}
		}
		jobs = append(jobs, f1)
	}

	resultsChan := pool.RunWithWorkers(0, jobs)

	var syncStatus []resource.SyncStatus
	mu := errors.NewMultiError("error in batch sync")
	for result := range resultsChan {
		var errMsg string
		success := true
		if result.Err != nil {
			mu.Append(result.Err)
			errMsg = result.Err.Error()
			success = false
		}
		syncStatus = append(syncStatus, resource.SyncStatus{
			Identifier: result.Output.FullName(),
			Success:    success,
			ErrorMsg:   errMsg,
		})
	}
	return syncStatus, mu.ToErr()
}

func (*SyncerService) GetSyncInterval(res *resource.Resource) (int64, error) {
	et, err := ConvertSpecTo[ExternalTable](res)
	if err != nil {
		return 0, err
	}
	if et.Source == nil {
		return 0, errors.NotFound(EntityExternalTable, "source is empty for "+et.FullName())
	}
	if et.Source.SyncInterval < 1 || et.Source.SyncInterval > MaxSyncInterval {
		return MaxSyncInterval, nil
	}
	return et.Source.SyncInterval, nil
}

func (s *SyncerService) Sync(ctx context.Context, res *resource.Resource) error {
	et, err := ConvertSpecTo[ExternalTable](res)
	if err != nil {
		return err
	}

	sources := []ExternalTableSourceType{et.GetSourceType()}

	// also get lark client if resource type is lark
	externalTableClients, err := s.getExtTableClients(ctx, res.Tenant(), sources)
	if err != nil {
		return err
	}
	tenantWithDetails, err := s.tenantDetailsGetter.GetDetails(ctx, res.Tenant())
	if err != nil {
		return err
	}
	commonLocation, err := tenantWithDetails.GetConfig(ExtLocation)
	if err != nil {
		if !errors.IsErrorType(err, errors.ErrNotFound) {
			return err
		}
	}
	return processResource(ctx, s.SyncRepo, externalTableClients, res, commonLocation)
}

type ExtTableClients struct {
	GSheet *gsheet.GSheets
	OSS    *oss.Client
	GDrive *gdrive.GDrive
	Lark   *lark.Client
}

func (s *SyncerService) getExtTableClients(ctx context.Context, tnnt tenant.Tenant, sourceTypes ExternalTableSources) (ExtTableClients, error) {
	var clients ExtTableClients
	if sourceTypes.Has(GoogleSheet) {
		secret, err := s.secretProvider.GetSecret(ctx, tnnt, GsheetCredsKey)
		if err != nil {
			return clients, err
		}
		sheetClient, err := gsheet.NewGSheets(ctx, secret.Value())
		if err != nil {
			return clients, err
		}
		clients.GSheet = sheetClient

		driveSrv, err := gdrive.NewGDrives(ctx, secret.Value(), s.maxFileSizeSupported, s.driveFileCleanupSizeLimit)
		if err != nil {
			return clients, fmt.Errorf("not able to create drive service err: %w", err)
		}
		clients.GDrive = driveSrv
	}
	if sourceTypes.Has(GoogleDrive) && clients.GDrive == nil {
		driveSrv, err := s.getDriveClient(ctx, tnnt)
		if err != nil {
			return clients, err
		}
		clients.GDrive = driveSrv
	}
	if sourceTypes.Has(LarkSheet) {
		larkClient, err := s.getLarkClient(ctx, tnnt)
		if err != nil {
			return clients, err
		}
		clients.Lark = larkClient
	}

	creds, err := s.secretProvider.GetSecret(ctx, tnnt, OSSCredsKey)
	if err != nil {
		return clients, err
	}
	ossClient, err := bucket.NewOssClient(creds.Value())
	if err != nil {
		return clients, err
	}
	clients.OSS = ossClient

	return clients, nil
}

func cleanCsvContent(data string, et *ExternalTable) (string, error) {
	records, err := csv.FromString(data)
	if err != nil {
		return "", err
	}
	headers, err := et.Source.GetHeaderCount()
	if err != nil {
		return "", err
	}
	columnCount := len(et.Schema)
	var output string
	output, err = csv.FromRecords[string](records, columnCount, func(rowIndex, colIndex int, data any) (string, error) {
		if rowIndex < headers {
			s, _ := ParseString(data) // ignore header parsing error, as headers will be ignored in data
			return s, nil
		}
		value, err := formatSheetData(colIndex, data, et.Schema)
		if err != nil {
			if d, ok := data.(string); ok {
				if strings.HasPrefix(d, "#REF!") || strings.HasPrefix(d, "#N/A") {
					err = nil
					value = ""
				} else {
					if _, ok := validInfinityValues[d]; ok { // check infinity
						err = nil
						value = d
					}
				}
			}
		}
		err = errors.WrapIfErr(EntityFormatter, fmt.Sprintf("for column Index:%d", colIndex), err)
		return value, err
	})

	return output, err
}

func processResource(ctx context.Context, syncRepo SyncRepo, externalTableClients ExtTableClients, resource *resource.Resource, commonLocation string) error {
	et, err := ConvertSpecTo[ExternalTable](resource)
	if err != nil {
		return err
	}
	switch et.Source.SourceType {
	case GoogleSheet, GoogleDrive:
		err := processGoogleTypeSources(ctx, externalTableClients, externalTableClients.OSS, et, commonLocation)
		syncStatusRemarks := map[string]string{}
		if err != nil {
			syncStatusRemarks["error"] = err.Error()
			syncStatusRemarks["sheet_url"] = et.Source.SourceURIs[0]
			syncRepo.Upsert(ctx, resource.Tenant().ProjectName(), KindExternalTableGoogle, resource.FullName(), syncStatusRemarks, false)
		} else {
			syncRepo.Upsert(ctx, resource.Tenant().ProjectName(), KindExternalTableGoogle, resource.FullName(), syncStatusRemarks, true)
		}
		return err
	case LarkSheet:
		revisionNumber, err := processLarkSheet(ctx, externalTableClients.Lark, externalTableClients.OSS, et, commonLocation)
		syncStatusRemarks := map[string]string{}
		if err != nil {
			syncStatusRemarks["error"] = err.Error()
			syncStatusRemarks["sheet_url"] = et.Source.SourceURIs[0]
			syncRepo.UpsertRevision(ctx, resource.Tenant().ProjectName(), KindExternalTableLark, resource.FullName(), syncStatusRemarks, revisionNumber, false)
		} else {
			syncRepo.UpsertRevision(ctx, resource.Tenant().ProjectName(), KindExternalTableLark, resource.FullName(), syncStatusRemarks, revisionNumber, true)
		}
		return err
	default:
		return nil
	}
}

func getBucketNameAndPath(commonLocation, loc, fullName string) (bucketName, path string, err error) { // nolint
	if loc == "" {
		if commonLocation == "" {
			err = errors.NotFound(EntityExternalTable, "location for the external table is empty")
			return
		}
		loc = commonLocation
	}

	parts := strings.Split(loc, "/")
	if len(parts) < 4 { // nolint:mnd
		err = errors.InvalidArgument(EntityExternalTable, "unable to parse url "+loc)
		return
	}

	bucketName = parts[3]
	components := strings.Join(parts[4:], "/")
	path = fmt.Sprintf("%s/%s/", strings.TrimSuffix(components, "/"), strings.ReplaceAll(fullName, ".", "/"))
	return
}

func deleteFolderFromBucket(ctx context.Context, client *oss.Client, bucketName, folderPrefix string) error {
	result, err := client.ListObjectsV2(ctx, &oss.ListObjectsV2Request{
		Bucket: &bucketName,
		Prefix: &folderPrefix,
	})
	if err != nil {
		return err
	}

	// Delete objects if any
	if len(result.Contents) > 0 {
		for _, obj := range result.Contents {
			keyToDelete := *obj.Key
			_, err := client.DeleteObject(ctx, &oss.DeleteObjectRequest{
				Bucket: &bucketName,
				Key:    &keyToDelete,
			})
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func writeToBucket(ctx context.Context, client *oss.Client, bucketName, objectKey, content string) error {
	_, err := client.PutObject(ctx, &oss.PutObjectRequest{
		Bucket:      &bucketName,
		Key:         &objectKey,
		ContentType: oss.Ptr("text/csv"),
		Body:        strings.NewReader(content),
	})
	return err
}
