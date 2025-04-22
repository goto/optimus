package maxcompute

import (
	"context"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss"
	"github.com/goto/salt/log"
	"google.golang.org/api/drive/v3"

	"github.com/goto/optimus/core/resource"
	"github.com/goto/optimus/core/tenant"
	bucket "github.com/goto/optimus/ext/bucket/oss"
	"github.com/goto/optimus/ext/sheets/gdrive"
	"github.com/goto/optimus/ext/sheets/gsheet"
	"github.com/goto/optimus/internal/errors"
	"github.com/goto/optimus/internal/lib/pool"
)

const (
	GsheetCredsKey    = "GOOGLE_SHEETS_ACCOUNT"
	OSSCredsKey       = "OSS_CREDS"
	putTimeOut        = time.Second * 10
	ExtLocation       = "EXT_LOCATION"
	MaxSyncInterval   = 24
	headersCountSerde = "odps.text.option.header.lines.count"
	useQuoteSerde     = "odps.text.option.use.quote"
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
	logger               log.Logger
	secretProvider       SecretProvider
	tenantDetailsGetter  TenantDetailsGetter
	SyncRepo             SyncRepo
	maxFileSizeSupported int
}

func NewSyncer(log log.Logger, secretProvider SecretProvider, tenantDetailsGetter TenantDetailsGetter, syncRepo SyncRepo, maxFileSizeSupported int) *SyncerService {
	return &SyncerService{
		logger:               log,
		secretProvider:       secretProvider,
		tenantDetailsGetter:  tenantDetailsGetter,
		SyncRepo:             syncRepo,
		maxFileSizeSupported: maxFileSizeSupported,
	}
}

func (s *SyncerService) TouchUnModified(ctx context.Context, projectName tenant.ProjectName, resources []*resource.Resource) error {
	return s.SyncRepo.Touch(ctx, projectName, KindExternalTable, resources)
}

func (s *SyncerService) SyncBatch(ctx context.Context, resources []*resource.Resource) ([]resource.SyncStatus, error) {
	if len(resources) == 0 {
		return []resource.SyncStatus{}, nil
	}
	sheets, ossClient, drive, err := s.getClients(ctx, resources[0].Tenant())
	if err != nil {
		return nil, err
	}

	tenantWithDetails, err := s.tenantDetailsGetter.GetDetails(ctx, resources[0].Tenant())
	if err != nil {
		return nil, err
	}

	commonLocation, err := tenantWithDetails.GetConfig(ExtLocation)
	if err != nil {
		if !errors.IsErrorType(err, errors.ErrNotFound) {
			return nil, err
		}
	}

	var jobs []func() pool.JobResult[*resource.Resource]
	for _, r := range resources {
		r := r
		f1 := func() pool.JobResult[*resource.Resource] {
			et, err := ConvertSpecTo[ExternalTable](r)
			if err != nil {
				return pool.JobResult[*resource.Resource]{Output: r, Err: err}
			}
			quoteSerdeMissing, err := processResource(ctx, sheets, ossClient, drive, et, commonLocation)
			syncStatusRemarks := map[string]string{}
			if quoteSerdeMissing {
				syncStatusRemarks["quoteSerdeMissing"] = "True"
			}
			if err != nil {
				syncStatusRemarks["error"] = err.Error()
				syncStatusRemarks["sheet_url"] = et.Source.SourceURIs[0]
				s.SyncRepo.Upsert(ctx, r.Tenant().ProjectName(), KindExternalTable, r.FullName(), syncStatusRemarks, false)
			} else {
				s.SyncRepo.Upsert(ctx, r.Tenant().ProjectName(), KindExternalTable, r.FullName(), syncStatusRemarks, true)
			}
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

func (s *SyncerService) GetETSourceLastModified(ctx context.Context, tnnt tenant.Tenant, resources []*resource.Resource) ([]resource.SourceModifiedTimeStatus, error) {
	var response []resource.SourceModifiedTimeStatus
	driveClient, clientErr := s.getDriveClient(ctx, tnnt)
	if clientErr != nil {
		return nil, errors.InternalError(EntityExternalTable, "unable to get google drive Client", clientErr)
	}
	var jobs []func() pool.JobResult[resource.SourceModifiedTimeStatus]
	for _, res := range resources {
		r := res
		et, err := ConvertSpecTo[ExternalTable](r)
		if err != nil {
			response = append(response, resource.SourceModifiedTimeStatus{
				FullName: r.FullName(),
				Err:      err,
			})
			continue
		}
		switch strings.ToUpper(et.Source.SourceType) {
		case GoogleSheet, GoogleDrive:
			jobs = append(jobs, func() pool.JobResult[resource.SourceModifiedTimeStatus] {
				lastModified, err := driveClient.GetLastModified(et.Source.SourceURIs[0])
				if err != nil {
					return pool.JobResult[resource.SourceModifiedTimeStatus]{
						Output: resource.SourceModifiedTimeStatus{
							FullName: r.FullName(),
							Err:      errors.InvalidArgument(EntityExternalTable, err.Error()),
						},
						Err: errors.InvalidArgument(EntityExternalTable, err.Error()),
					}
				}
				return pool.JobResult[resource.SourceModifiedTimeStatus]{
					Output: resource.SourceModifiedTimeStatus{
						FullName:         r.FullName(),
						LastModifiedTime: *lastModified,
					},
				}
			})

		default:
			response = append(response, resource.SourceModifiedTimeStatus{
				FullName: r.FullName(),
				Err:      errors.InvalidArgument(EntityExternalTable, "source is not GoogleSheet or GoogleDrive"),
			})
		}
	}
	resultsChan := pool.RunWithWorkers(10, jobs)
	for result := range resultsChan {
		response = append(response, result.Output)
	}

	return response, nil
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

func getGSheetContent(et *ExternalTable, sheets *gsheet.GSheets) (string, bool, error) {
	headers := 0
	if val, ok := et.Source.SerdeProperties[headersCountSerde]; ok && val != "" {
		num, err := strconv.Atoi(val)
		if err != nil {
			return "", false, errors.InvalidArgument(EntityExternalTable, "unable to parse "+headersCountSerde)
		}
		headers = num
	}

	et.Source.GetFormattedDate = et.Source.GetFormattedDate || !et.Schema.ContainsDateTimeColumns()

	uri := et.Source.SourceURIs[0]
	return sheets.GetAsCSV(uri, et.Source.Range, et.Source.GetFormattedDate, et.Source.GetFormattedData, func(rowIndex, colIndex int, data any) (string, error) {
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
}

func (s *SyncerService) Sync(ctx context.Context, res *resource.Resource) error {
	sheets, ossClient, drive, err := s.getClients(ctx, res.Tenant())
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
	et, err := ConvertSpecTo[ExternalTable](res)
	if err != nil {
		return err
	}
	quoteSerdeMissing, err := processResource(ctx, sheets, ossClient, drive, et, commonLocation)
	syncStatusRemarks := map[string]string{}
	if quoteSerdeMissing {
		syncStatusRemarks["quoteSerdeMissing"] = "True"
	}
	if err != nil {
		syncStatusRemarks["error"] = err.Error()
		syncStatusRemarks["sheet_url"] = et.Source.SourceURIs[0]
		s.SyncRepo.Upsert(ctx, res.Tenant().ProjectName(), KindExternalTable, et.FullName(), syncStatusRemarks, false)
	} else {
		s.SyncRepo.Upsert(ctx, res.Tenant().ProjectName(), KindExternalTable, et.FullName(), syncStatusRemarks, true)
	}
	return err
}

func (s *SyncerService) getDriveClient(ctx context.Context, tnnt tenant.Tenant) (*gdrive.GDrive, error) {
	secret, err := s.secretProvider.GetSecret(ctx, tnnt, GsheetCredsKey)
	if err != nil {
		return nil, err
	}

	driveSrv, err := gdrive.NewGDrives(ctx, secret.Value(), s.maxFileSizeSupported)
	if err != nil {
		return nil, fmt.Errorf("not able to create drive service err: %w", err)
	}

	return driveSrv, nil
}

func (s *SyncerService) getClients(ctx context.Context, tnnt tenant.Tenant) (*gsheet.GSheets, *oss.Client, *gdrive.GDrive, error) {
	secret, err := s.secretProvider.GetSecret(ctx, tnnt, GsheetCredsKey)
	if err != nil {
		return nil, nil, nil, err
	}

	sheetClient, err := gsheet.NewGSheets(ctx, secret.Value())
	if err != nil {
		return nil, nil, nil, err
	}

	creds, err := s.secretProvider.GetSecret(ctx, tnnt, OSSCredsKey)
	if err != nil {
		return nil, nil, nil, err
	}

	ossClient, err := bucket.NewOssClient(creds.Value())
	if err != nil {
		return nil, nil, nil, err
	}

	driveSrv, err := gdrive.NewGDrives(ctx, secret.Value(), s.maxFileSizeSupported)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("not able to create drive service err: %w", err)
	}

	return sheetClient, ossClient, driveSrv, nil
}

func processGoogleSheet(ctx context.Context, sheetSrv *gsheet.GSheets, ossClient *oss.Client, et *ExternalTable, commonLocation string) (bool, error) {
	if len(et.Source.SourceURIs) == 0 {
		return false, errors.InvalidArgument(EntityExternalTable, "source URI is empty for Google Sheet")
	}

	content, fileNeedQuoteSerde, err := getGSheetContent(et, sheetSrv)
	if err != nil {
		return fileNeedQuoteSerde, err
	}
	var quoteSerdeMissing bool
	if fileNeedQuoteSerde {
		if val, ok := et.Source.SerdeProperties[useQuoteSerde]; ok {
			boolVal, _ := strconv.ParseBool(val)
			quoteSerdeMissing = !boolVal
		} else {
			quoteSerdeMissing = true
		}
	}

	bucketName, objectPath, err := getBucketNameAndPath(commonLocation, et.Source.Location, et.FullName())
	if err != nil {
		return quoteSerdeMissing, err
	}
	objectKey := objectPath + "file.csv"
	err = deleteFolderFromBucket(ctx, ossClient, bucketName, objectPath)
	if err != nil {
		return fileNeedQuoteSerde, err
	}
	return quoteSerdeMissing, writeToBucket(ctx, ossClient, bucketName, objectKey, content)
}

func SyncDriveFileToOSS(ctx context.Context, driveClient *gdrive.GDrive, driveFile *drive.File, ossClient *oss.Client, bucketName, objectName, contentType string) error {
	if !strings.EqualFold(driveFile.FileExtension, contentType) {
		return nil
	}
	if !driveClient.IsWithinDownloadLimit(driveFile) {
		return errors.InvalidArgument(EntityExternalTable, fmt.Sprintf("file size:[%d] mb, greated than configured limit", driveFile.Size/1000000))
	}
	content, err := driveClient.DownloadFile(driveFile.Id)
	if err != nil {
		return err
	}
	return writeToBucket(ctx, ossClient, bucketName, objectName, string(content))
}

func SyncDriveFolderToOSS(ctx context.Context, driveClient *gdrive.GDrive, ossClient *oss.Client, folderID, bucketName, destination, contentType string) error {
	driveFiles, err := driveClient.FolderListShow(folderID)
	if err != nil {
		return err
	}
	for _, driveFile := range driveFiles.Files {
		objectName := filepath.Join(destination, driveFile.Name)
		if strings.EqualFold(driveFile.MimeType, gdrive.TypeFolder) {
			err = SyncDriveFolderToOSS(ctx, driveClient, ossClient, driveFile.Id, bucketName, objectName, contentType)
			if err != nil {
				return err
			}
		}
		err = SyncDriveFileToOSS(ctx, driveClient, driveFile, ossClient, bucketName, objectName, contentType)
		if err != nil {
			return err
		}
	}
	return nil
}

func syncFromDrive(ctx context.Context, driveSrv *gdrive.GDrive, ossClient *oss.Client, et *ExternalTable, bucketName, objectPath string) error {
	fileType, driveFile, err := driveSrv.ListDriveEntity(et.Source.SourceURIs[0])
	if err != nil {
		return err
	}
	if fileType == "folder" {
		return SyncDriveFolderToOSS(ctx, driveSrv, ossClient, driveFile.Id, bucketName, objectPath, et.Source.ContentType)
	}
	objectName := filepath.Join(objectPath, driveFile.Name)
	return SyncDriveFileToOSS(ctx, driveSrv, driveFile, ossClient, bucketName, objectName, et.Source.ContentType)
}

func processGoogleDrive(ctx context.Context, driveSrv *gdrive.GDrive, ossClient *oss.Client, et *ExternalTable, commonLocation string) error {
	if len(et.Source.SourceURIs) == 0 {
		return errors.InvalidArgument(EntityExternalTable, "source URI is empty for Google Drive")
	}

	bucketName, objectPath, err := getBucketNameAndPath(commonLocation, et.Source.Location, et.FullName())
	if err != nil {
		return err
	}

	err = deleteFolderFromBucket(ctx, ossClient, bucketName, objectPath)
	if err != nil {
		return err
	}

	return syncFromDrive(ctx, driveSrv, ossClient, et, bucketName, objectPath)
}

func processResource(ctx context.Context, sheetSrv *gsheet.GSheets, ossClient *oss.Client, drive *gdrive.GDrive, et *ExternalTable, commonLocation string) (bool, error) {
	switch strings.ToUpper(et.Source.SourceType) {
	case GoogleSheet:
		return processGoogleSheet(ctx, sheetSrv, ossClient, et, commonLocation)
	case GoogleDrive:
		err := processGoogleDrive(ctx, drive, ossClient, et, commonLocation)
		return false, err
	default:
		return false, nil
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
