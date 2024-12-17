package gsheet

import (
	"context"
	"errors"
	"fmt"

	"google.golang.org/api/option"
	"google.golang.org/api/sheets/v4"

	"github.com/goto/optimus/ext/sheets/csv"
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
		return nil, fmt.Errorf("not able to create sheets service err: %w", err)
	}

	return &GSheets{srv: srv}, nil
}

func (gs *GSheets) GetAsCSV(url string, range_ string) (string, error) {
	info, err := FromURL(url)
	if err != nil {
		return "", err
	}

	if range_ == "" {
		range_ = readRange
	}
	content, err := gs.getSheetContent(info.SheetID, range_)
	if err != nil {
		return "", err
	}

	return csv.FromRecords(content)
}

func (gs *GSheets) getSheetContent(sheetID string, range_ string) ([][]interface{}, error) {
	resp, err := gs.srv.Spreadsheets.Values.Get(sheetID, range_).Do()
	if err != nil {
		return nil, err
	}

	if len(resp.Values) == 0 {
		return nil, errors.New("no data found in the sheet")
	}

	return resp.Values, nil
}
