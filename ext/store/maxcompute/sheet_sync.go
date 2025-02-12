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
	"github.com/goto/optimus/internal/utils"
)

const (
	GsheetCredsKey  = "GOOGLE_SHEETS_ACCOUNT"
	OSSCredsKey     = "OSS_CREDS"
	putTimeOut      = time.Second * 10
	ExtLocation     = "EXT_LOCATION"
	MaxSyncInterval = 24
)

type SyncerService struct {
	secretProvider      SecretProvider
	tenantDetailsGetter TenantDetailsGetter
}

func NewSyncer(secretProvider SecretProvider, tenantDetailsGetter TenantDetailsGetter) *SyncerService {
	return &SyncerService{
		secretProvider:      secretProvider,
		tenantDetailsGetter: tenantDetailsGetter,
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

	commonLocaton, err := tenantWithDetails.GetConfig(ExtLocation)
	if err != nil {
		if !errors.IsErrorType(err, errors.ErrNotFound) {
			return nil, err
		}
	}

	var jobs []func() pool.JobResult[string]
	for _, r := range resources {
		r := r
		f1 := func() pool.JobResult[string] {
			err := processResource(ctx, sheets, ossClient, r, commonLocaton)
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
		return 0, errors.NotFound(EntityExternalTable, "source is empty for "+res.FullName())
	}
	if et.Source.SyncInterval < 1 || et.Source.SyncInterval > MaxSyncInterval {
		return MaxSyncInterval, nil
	}
	return et.Source.SyncInterval, nil
}

func formatSheetData(colIndex int, data any, schema Schema) string {
	colSchema := schema[colIndex]
	switch colSchema.Type {
	case "BIGINT", "TINYINT", "SMALLINT", "INT":
		s, ok := data.(float64)
		if !ok {
			return ""
		}
		return strconv.FormatInt(int64(s), 10)
	case "DOUBLE", "DECIMAL", "FLOAT":
		s, ok := data.(float64)
		if !ok {
			return ""
		}
		precision := 4
		if colSchema.Decimal != nil {
			precision = int(colSchema.Decimal.Scale)
		}
		return strconv.FormatFloat(s, 'f', precision, 64)
	case "BOOLEAN":
		s, ok := data.(string)
		if !ok {
			return "False"
		}
		if utils.ConvertToBoolean(s) {
			return "True"
		}
		return "False"
	case "DATETIME", "DATE", "TIMESTAMP", "TIMESTAMP_NTZ":
		s, ok := data.(string)
		if !ok {
			return ""
		}
		if colSchema.SourceTimeFormat != "" {
			goTimeLayout := utils.ConvertTimeToGoLayout(colSchema.SourceTimeFormat)
			parsedTime, err := time.Parse(goTimeLayout, s)
			if err != nil {
				return s
			}
			var outPutFormat string
			if colSchema.Type == "DATE" {
				outPutFormat = time.DateOnly
			} else if colSchema.Type == "DATETIME" {
				outPutFormat = time.DateTime
			} else if colSchema.Type == "TIMESTAMP" || colSchema.Type == "TIMESTAMP_NTZ" {
				outPutFormat = "2006-01-02 15:04:05.000000000"
			}
			return parsedTime.Format(outPutFormat)
		}
		return s

	default:
		s, ok := data.(string)
		if !ok {
			return ""
		}
		return s
	}
}

func (s *SyncerService) Sync(ctx context.Context, res *resource.Resource) error {
	// Check if external table is for sheets
	et, err := ConvertSpecTo[ExternalTable](res)
	if err != nil {
		return err
	}

	if !strings.EqualFold(et.Source.SourceType, GoogleSheet) {
		return nil
	}

	if len(et.Source.SourceURIs) == 0 {
		return errors.InvalidArgument(EntityExternalTable, "source URI is empty for Google Sheet")
	}
	uri := et.Source.SourceURIs[0]

	sheets, ossClient, err := s.getClients(ctx, res.Tenant())
	if err != nil {
		return err
	}

	var getUnformattedValues bool
	var formatFn func(int, any) string
	if et.Source.ProcessData {
		getUnformattedValues = true
		formatFn = func(index int, a any) string {
			return formatSheetData(index, a, et.Schema)
		}
	}
	content, err := sheets.GetAsCSV(uri, et.Source.Range, getUnformattedValues, formatFn)
	if err != nil {
		return err
	}
	tenantWithDetails, err := s.tenantDetailsGetter.GetDetails(ctx, res.Tenant())
	if err != nil {
		return err
	}
	commonLocaton, err := tenantWithDetails.GetConfig(ExtLocation)
	if err != nil {
		if !errors.IsErrorType(err, errors.ErrNotFound) {
			return err
		}
	}

	bucketName, objectKey, err := getBucketNameAndPath(commonLocaton, et.Source.Location, res.FullName())
	if err != nil {
		return err
	}

	return writeToBucket(ctx, ossClient, bucketName, objectKey, content)
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

func processResource(ctx context.Context, sheetSrv *gsheet.GSheets, ossClient *oss.Client, res *resource.Resource, commonLocation string) error {
	et, err := ConvertSpecTo[ExternalTable](res)
	if err != nil {
		return err
	}

	if !strings.EqualFold(et.Source.SourceType, GoogleSheet) {
		return nil
	}

	if len(et.Source.SourceURIs) == 0 {
		return errors.InvalidArgument(EntityExternalTable, "source URI is empty for Google Sheet")
	}
	uri := et.Source.SourceURIs[0]

	var getUnformattedValues bool
	var formatFn func(int, any) string
	if et.Source.ProcessData {
		getUnformattedValues = true
		formatFn = func(index int, a any) string {
			return formatSheetData(index, a, et.Schema)
		}
	}

	content, err := sheetSrv.GetAsCSV(uri, et.Source.Range, getUnformattedValues, formatFn)
	if err != nil {
		return err
	}

	bucketName, objectKey, err := getBucketNameAndPath(commonLocation, et.Source.Location, res.FullName())
	if err != nil {
		return err
	}

	return writeToBucket(ctx, ossClient, bucketName, objectKey, content)
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
