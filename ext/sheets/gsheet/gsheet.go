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

func (gs *GSheets) GetAsCSV(url, sheetRange string) (string, error) {
	info, err := FromURL(url)
	if err != nil {
		return "", err
	}

	content, err := gs.getSheetContent(info.SheetID, sheetRange)
	if err != nil {
		return "", err
	}

	return csv.FromRecords(content)
}

func (gs *GSheets) getSheetContent(sheetID, sheetRange string) ([][]interface{}, error) {
	batchGetCall := gs.srv.Spreadsheets.Values.BatchGet(sheetID)
	if sheetRange != "" {
		batchGetCall = batchGetCall.Ranges(sheetRange)
	}
	resp, err := batchGetCall.Do()

	if err != nil {
		return nil, err
	}

	if len(resp.ValueRanges) == 0 {
		return nil, errors.New("no sheets found in the spreadsheet ")
	}

	if len(resp.ValueRanges[0].Values) == 0 {
		return nil, errors.New("no data found in the sheet[0]")
	}
	return resp.ValueRanges[0].Values, nil
}

func (gs *GSheets) GetSheetName(sheetID string) (string, error) {
	spreadsheet, err := gs.srv.Spreadsheets.Get(sheetID).Do()
	if err != nil {
		return "", err
	}

	if len(spreadsheet.Sheets) == 0 {
		return "", errors.New("no sub sheet found")
	}
	sid := spreadsheet.Sheets[0].Properties.Title
	return sid, err
}
