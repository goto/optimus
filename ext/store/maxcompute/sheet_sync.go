package maxcompute

import (
	"context"
	"fmt"
	"strings"

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
	maxFileSizeSupported      int
	driveFileCleanupSizeLimit int
}

func NewSyncer(log log.Logger, secretProvider SecretProvider, tenantDetailsGetter TenantDetailsGetter,
	syncRepo SyncRepo, maxFileSizeSupported, driveFileCleanupSizeLimit int,
) *SyncerService {
	return &SyncerService{
		logger:                    log,
		secretProvider:            secretProvider,
		tenantDetailsGetter:       tenantDetailsGetter,
		SyncRepo:                  syncRepo,
		maxFileSizeSupported:      maxFileSizeSupported,
		driveFileCleanupSizeLimit: driveFileCleanupSizeLimit,
	}
}

func (s *SyncerService) TouchUnModified(ctx context.Context, projectName tenant.ProjectName, resources []*resource.Resource) error {
	return s.SyncRepo.Touch(ctx, projectName, KindExternalTable, resources)
}

func getAllSourceTypes(resources []*resource.Resource) (ExternalTableSources, error) {
	sourceTypes := make(ExternalTableSources, 0)
	for _, spec := range resources {
		sourceTypeString := spec.Metadata().EtSourceType
		source, err := NewExternalTableSource(sourceTypeString)
		if err != nil {
			return nil, err
		}
		sourceTypes.Append(source)
	}
	return sourceTypes, nil
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
	sourceTypes, err := getAllSourceTypes(resources)
	if err != nil {
		return nil, err
	}
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
			Resource: result.Output,
			Success:  success,
			ErrorMsg: errMsg,
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
