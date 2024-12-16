package gsheet

import (
	"context"
	"errors"

	"google.golang.org/api/option"
	"google.golang.org/api/sheets/v4"

	"github.com/goto/optimus/ext/format/csv"
)

const (
	readRange = "Sheet1"
)

type GSheets struct {
	srv *sheets.Service
}

func NewGSheets(ctx context.Context, creds string) (*GSheets, error) {
	srv, err := sheets.NewService(ctx, option.WithCredentialsJSON([]byte(creds)))
	if err != nil {
		return nil, errors.New("not able to create sheets service")
	}

	return &GSheets{srv: srv}, nil
}

func (gs *GSheets) GetAsCSV(url string) (string, error) {
	info, err := FromURL(url)
	if err != nil {
		return "", err
	}

	content, err := gs.getSheetContent(info.SheetID)
	if err != nil {
		return "", err
	}

	return csv.FromRecords(content)
}

func (gs *GSheets) getSheetContent(sheetID string) ([][]interface{}, error) {
	resp, err := gs.srv.Spreadsheets.Values.Get(sheetID, readRange).Do()
	if err != nil {
		return nil, err
	}

	if len(resp.Values) == 0 {
		return nil, errors.New("no data found in the sheet")
	}

	return resp.Values, nil
}
