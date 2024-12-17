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
	"github.com/goto/optimus/ext/bucket/oss"
	"github.com/goto/optimus/ext/sheets/gsheet"
)

const (
	GsheetCreds = ""
	OSSCreds    = ""
	ExtLocation = ""
	putTimeOut  = time.Duration(time.Second * 10)
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

	if et.Source.SourceType != GoogleSheet {
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

	// Setup oss bucket writer
	creds, err := s.secretProvider.GetSecret(ctx, res.Tenant(), OSSCreds)
	if err != nil {
		return err
	}
	ossClient, err := bucket.NewOssClient(creds.Value())
	if err != nil {
		return err
	}

	// oss put request
	var putStatus chan int64
	bucketName := res.Name().String()
	objectKey, err := s.getObjectKey(ctx, res, et)
	if err != nil {
		return err
	}
	contentType := "text/csv"
	resp, err := ossClient.PutObject(ctx, &oss.PutObjectRequest{
		Bucket:      &bucketName,
		Key:         &objectKey,
		ContentType: &contentType,
		Body:        strings.NewReader(content),
		ProgressFn: func(increment, transferred, total int64) {
			putStatus <- total
		},
	}, nil)
	if err != nil {
		return err
	}

	for {
		select {
		case <-putStatus:
			if resp.StatusCode != 200 {
				return errors.New(fmt.Sprintf("error putting OSS object, status:%s", resp.Status))
			}

			return nil
		case <-time.After(putTimeOut):
			return errors.New("put timeout")
		}

	}

	// log resp status
}

func (s *SyncerService) getGsheet(ctx context.Context, tnnt tenant.Tenant, sheetURI string, range_ string) (string, error) {
	secret, err := s.secretProvider.GetSecret(ctx, tnnt, GsheetCreds)
	if err != nil {
		return "", err
	}
	sheets, err := gsheet.NewGSheets(ctx, secret.Value())

	return sheets.GetAsCSV(sheetURI, range_)
}

func (s *SyncerService) getObjectKey(ctx context.Context, res *resource.Resource, et *ExternalTable) (string, error) {
	location, err := s.getLocation(ctx, res, et)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s/%s.csv", location, res.Name().String()), nil
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

func WriteFileToLocation(context.Context, string) {

}
