package maxcompute

import (
	"context"
	"errors"

	"github.com/goto/optimus/core/resource"
	"github.com/goto/optimus/core/tenant"
	"github.com/goto/optimus/ext/sheets/gsheet"
)

const (
	GsheetCreds = ""
	ExtLocation = ""
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
	_, err = s.getGsheet(ctx, res.Tenant(), uri)
	if err != nil {
		return err
	}

	// Setup oss bucket writer

	// Get location to write, Write to oss bucket
	//path := s.getLocation(ctx, res, et)

	//WriteFileToLocation(ctx, path)
	return nil
}

func (s *SyncerService) getGsheet(ctx context.Context, tnnt tenant.Tenant, sheetURI string) (string, error) {
	secret, err := s.secretProvider.GetSecret(ctx, tnnt, GsheetCreds)
	if err != nil {
		return "", err
	}
	sheets, err := gsheet.NewGSheets(ctx, secret.Value())

	return sheets.GetAsCSV(sheetURI)
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
