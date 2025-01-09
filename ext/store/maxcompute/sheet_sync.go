package maxcompute

import (
	"context"
	"fmt"
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
	GsheetCredsKey = "GOOGLE_SHEETS_ACCOUNT"
	OSSCredsKey    = "OSS_CREDS"
	putTimeOut     = time.Second * 10
)

type SyncerService struct {
	secretProvider SecretProvider
}

func NewSyncer(secretProvider SecretProvider) *SyncerService {
	return &SyncerService{secretProvider: secretProvider}
}

func (s *SyncerService) SyncBatch(ctx context.Context, resources []*resource.Resource) ([]string, error) {
	sheets, ossClient, err := s.getClients(ctx, resources[0].Tenant())
	if err != nil {
		return nil, err
	}

	var jobs []func() pool.JobResult[string]
	for _, r := range resources {
		r := r
		f1 := func() pool.JobResult[string] {
			err := processResource(ctx, sheets, ossClient, r)
			if err != nil {
				return pool.JobResult[string]{Err: err}
			} else {
				return pool.JobResult[string]{Output: r.FullName()}
			}
		}
		jobs = append(jobs, f1)
	}

	resultsChan := pool.RunWithWorkers(0, jobs)

	var successNames []string
	mu := errors.NewMultiError("error in batch sync")
	for result := range resultsChan {
		if result.Err != nil {
			mu.Append(err)
		} else {
			successNames = append(successNames, result.Output)
		}
	}

	return successNames, mu.ToErr()
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

	content, err := sheets.GetAsCSV(uri, et.Source.Range)
	if err != nil {
		return err
	}

	bucketName, objectKey, err := getBucketNameAndPath(et.Source.Location, res.FullName())
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

func processResource(ctx context.Context, sheetSrv *gsheet.GSheets, ossClient *oss.Client, res *resource.Resource) error {
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

	content, err := sheetSrv.GetAsCSV(uri, et.Source.Range)
	if err != nil {
		return err
	}

	bucketName, objectKey, err := getBucketNameAndPath(et.Source.Location, res.FullName())
	if err != nil {
		return err
	}

	return writeToBucket(ctx, ossClient, bucketName, objectKey, content)
}

func getBucketNameAndPath(loc string, fullName string) (string, string, error) {
	if loc == "" {
		return "", "", errors.InvalidArgument(EntityExternalTable, "location for the external table is empty")
	}

	parts := strings.Split(loc, "/")
	if len(parts) < 4 { // nolint:mnd
		return "", "", errors.InvalidArgument(EntityExternalTable, "unable to parse url "+loc)
	}

	bucketName := parts[3]

	path := strings.Join(parts[4:], "/")
	return bucketName, fmt.Sprintf("%s%s/file.csv", path, fullName), nil
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
