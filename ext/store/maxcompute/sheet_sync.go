package maxcompute

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss"

	"github.com/goto/optimus/core/resource"
	"github.com/goto/optimus/core/tenant"
	bucket "github.com/goto/optimus/ext/bucket/oss"
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

type SyncerService struct {
	secretProvider      SecretProvider
	tenantDetailsGetter TenantDetailsGetter
	SyncRepo            SyncRepo
}

func NewSyncer(secretProvider SecretProvider, tenantDetailsGetter TenantDetailsGetter, syncRepo SyncRepo) *SyncerService {
	return &SyncerService{
		secretProvider:      secretProvider,
		tenantDetailsGetter: tenantDetailsGetter,
		SyncRepo:            syncRepo,
	}
}

func (s *SyncerService) SyncBatch(ctx context.Context, resources []*resource.Resource) ([]resource.SyncStatus, error) {
	sheets, ossClient, err := s.getClients(ctx, resources[0].Tenant())
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

	var jobs []func() pool.JobResult[string]
	for _, r := range resources {
		r := r
		f1 := func() pool.JobResult[string] {
			et, err := ConvertSpecTo[ExternalTable](r)
			if err != nil {
				return pool.JobResult[string]{Output: r.FullName(), Err: err}
			}
			quoteSerdeMissing, err := processResource(ctx, sheets, ossClient, et, commonLocation)
			syncStatusRemarks := map[string]string{}
			if quoteSerdeMissing {
				syncStatusRemarks["quoteSerdeMissing"] = "True"
			}
			if err != nil {
				err = errors.Wrap(EntityExternalTable, fmt.Sprintf("Resource: %s", r.FullName()), err)
				syncStatusRemarks["error"] = err.Error()
				syncStatusRemarks["sheet_url"] = et.Source.SourceURIs[0]
				s.SyncRepo.Upsert(ctx, r.Tenant().ProjectName(), KindExternalTable, r.FullName(), syncStatusRemarks, false)
			} else {
				s.SyncRepo.Upsert(ctx, r.Tenant().ProjectName(), KindExternalTable, r.FullName(), syncStatusRemarks, true)
			}
			if err != nil {
				return pool.JobResult[string]{Output: r.FullName(), Err: err}
			}
			return pool.JobResult[string]{Output: r.FullName()}
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
			ResourceName: result.Output,
			Success:      success,
			ErrorMsg:     errMsg,
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

func getGSheetContent(et *ExternalTable, sheets *gsheet.GSheets) (string, bool, error) {
	headers := 0
	if val, ok := et.Source.SerdeProperties[headersCountSerde]; ok && val != "" {
		num, err := strconv.Atoi(val)
		if err != nil {
			return "", false, errors.InvalidArgument(EntityExternalTable, "")
		}
		headers = num
	}

	uri := et.Source.SourceURIs[0]
	return sheets.GetAsCSV(uri, et.Source.Range, et.Source.GetFormattedDate, func(rowIndex, colIndex int, data any) (string, error) {
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
				}
			}
		}
		err = errors.WrapIfErr(EntityFormatter, fmt.Sprintf("for column Index:%d", colIndex), err)
		return value, err
	})
}

func (s *SyncerService) Sync(ctx context.Context, res *resource.Resource) error {
	sheets, ossClient, err := s.getClients(ctx, res.Tenant())
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
	quoteSerdeMissing, err := processResource(ctx, sheets, ossClient, et, commonLocation)
	syncStatusRemarks := map[string]string{}
	if quoteSerdeMissing {
		syncStatusRemarks["quoteSerdeMissing"] = "True"
	}
	if err != nil {
		err = errors.Wrap(EntityExternalTable, fmt.Sprintf("Resource: %s", et.FullName()), err)
		syncStatusRemarks["error"] = err.Error()
		syncStatusRemarks["sheet_url"] = et.Source.SourceURIs[0]
		s.SyncRepo.Upsert(ctx, res.Tenant().ProjectName(), KindExternalTable, et.FullName(), syncStatusRemarks, false)
	} else {
		s.SyncRepo.Upsert(ctx, res.Tenant().ProjectName(), KindExternalTable, et.FullName(), syncStatusRemarks, true)
	}
	return err
}

func (s *SyncerService) getClients(ctx context.Context, tnnt tenant.Tenant) (*gsheet.GSheets, *oss.Client, error) {
	secret, err := s.secretProvider.GetSecret(ctx, tnnt, GsheetCredsKey)
	if err != nil {
		return nil, nil, err
	}

	sheetClient, err := gsheet.NewGSheets(ctx, secret.Value())
	if err != nil {
		return nil, nil, err
	}

	creds, err := s.secretProvider.GetSecret(ctx, tnnt, OSSCredsKey)
	if err != nil {
		return nil, nil, err
	}

	ossClient, err := bucket.NewOssClient(creds.Value())
	if err != nil {
		return nil, nil, err
	}

	return sheetClient, ossClient, nil
}

func processResource(ctx context.Context, sheetSrv *gsheet.GSheets, ossClient *oss.Client, et *ExternalTable, commonLocation string) (bool, error) {
	if !strings.EqualFold(et.Source.SourceType, GoogleSheet) {
		return false, nil
	}

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

	bucketName, objectKey, err := getBucketNameAndPath(commonLocation, et.Source.Location, et.FullName())
	if err != nil {
		return quoteSerdeMissing, err
	}

	return quoteSerdeMissing, writeToBucket(ctx, ossClient, bucketName, objectKey, content)
}

func getBucketNameAndPath(commonLocation, loc string, fullName string) (bucketName string, path string, err error) { // nolint
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
	path = fmt.Sprintf("%s/%s/file.csv", strings.TrimSuffix(components, "/"), strings.ReplaceAll(fullName, ".", "/"))
	return
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
