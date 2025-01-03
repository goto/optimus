package maxcompute

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss"

	"github.com/goto/optimus/core/resource"
	"github.com/goto/optimus/core/tenant"
	bucket "github.com/goto/optimus/ext/bucket/oss"
	"github.com/goto/optimus/ext/sheets/gsheet"
)

const (
	GsheetCredsKey = "GOOGLE_SHEETS_ACCOUNT"
	OSSCredsKey    = "OSS_CREDS"
	ExtLocation    = ""
	putTimeOut     = time.Second * 10
)

type SyncerService struct {
	secretProvider SecretProvider
	tenantGetter   TenantDetailsGetter
}

func NewSyncer(secretProvider SecretProvider, tenantProvider TenantDetailsGetter) *SyncerService {
	return &SyncerService{secretProvider: secretProvider, tenantGetter: tenantProvider}
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
		return errors.New("source URI is empty for Google Sheet")
	}
	uri := et.Source.SourceURIs[0]

	// Get sheet content
	content, err := s.getGsheet(ctx, res.Tenant(), uri, et.Source.Range)
	if err != nil {
		return err
	}

	bucketName, err := s.getBucketName(ctx, res, et)
	if err != nil {
		return err
	}
	objectKey, err := s.getObjectKey(ctx, res, et)
	if err != nil {
		return err
	}

	return s.writeContentToLocation(ctx, res.Tenant(), bucketName, objectKey, content)
}

func (s *SyncerService) getGsheet(ctx context.Context, tnnt tenant.Tenant, sheetURI, sheetRange string) (string, error) {
	secret, err := s.secretProvider.GetSecret(ctx, tnnt, GsheetCredsKey)
	if err != nil {
		return "", err
	}

	sheets, err := gsheet.NewGSheets(ctx, secret.Value())
	if err != nil {
		return "", err
	}
	return sheets.GetAsCSV(sheetURI, sheetRange)
}

func (s *SyncerService) getBucketName(ctx context.Context, res *resource.Resource, et *ExternalTable) (string, error) {
	location, err := s.getLocation(ctx, res, et)
	if err != nil {
		return "", err
	}
	parts := strings.Split(location, "/")
	if len(parts) > 3 { // nolint:mnd
		bucketName := parts[3]
		return bucketName, nil
	}
	return "", errors.New("unable to get bucketName from Location")
}

func (s *SyncerService) getObjectKey(ctx context.Context, res *resource.Resource, et *ExternalTable) (string, error) {
	location, err := s.getLocation(ctx, res, et)
	if err != nil {
		return "", err
	}
	parts := strings.Split(location, "/")
	if len(parts) > 4 { // nolint:mnd
		path := strings.Join(parts[4:], "/")
		return fmt.Sprintf("%s%s/file.csv", path, res.FullName()), nil
	}
	return "", errors.New("unable to get object path from location")
}

func (s *SyncerService) getLocation(ctx context.Context, res *resource.Resource, et *ExternalTable) (string, error) {
	location := et.Source.Location
	if location == "" {
		details, err := s.tenantGetter.GetDetails(ctx, res.Tenant())
		if err != nil {
			return "", err
		}
		loc, err := details.GetConfig(ExtLocation)
		if err != nil {
			return "", err
		}
		location = loc
	}
	return location, nil
}

func (s *SyncerService) writeContentToLocation(ctx context.Context, tnnt tenant.Tenant, bucketName, objectKey, content string) error {
	// Setup oss bucket writer
	creds, err := s.secretProvider.GetSecret(ctx, tnnt, OSSCredsKey)
	if err != nil {
		return err
	}
	ossClient, err := bucket.NewOssClient(creds.Value())
	if err != nil {
		return err
	}

	_, err = ossClient.PutObject(ctx, &oss.PutObjectRequest{
		Bucket:      &bucketName,
		Key:         &objectKey,
		ContentType: oss.Ptr("text/csv"),
		Body:        strings.NewReader(content),
	})
	return err
}
