package gsheet

import (
	"context"
	"errors"
<<<<<<< HEAD
=======
	"fmt"
>>>>>>> bcbff2271f0d5e0687938c53057b54b69a2193ac

	"google.golang.org/api/option"
	"google.golang.org/api/sheets/v4"

<<<<<<< HEAD
	"github.com/goto/optimus/ext/format/csv"
)

const (
	readRange = "Sheet1"
=======
	"github.com/goto/optimus/ext/sheets/csv"
>>>>>>> bcbff2271f0d5e0687938c53057b54b69a2193ac
)

type GSheets struct {
	srv *sheets.Service
}

func NewGSheets(ctx context.Context, creds string) (*GSheets, error) {
	srv, err := sheets.NewService(ctx, option.WithCredentialsJSON([]byte(creds)))
	if err != nil {
<<<<<<< HEAD
		return nil, errors.New("not able to create sheets service")
=======
		return nil, fmt.Errorf("not able to create sheets service err: %w", err)
>>>>>>> bcbff2271f0d5e0687938c53057b54b69a2193ac
	}

	return &GSheets{srv: srv}, nil
}

<<<<<<< HEAD
func (gs *GSheets) GetAsCSV(url string) (string, error) {
=======
func (gs *GSheets) GetAsCSV(url, sheetRange string) (string, error) {
>>>>>>> bcbff2271f0d5e0687938c53057b54b69a2193ac
	info, err := FromURL(url)
	if err != nil {
		return "", err
	}

<<<<<<< HEAD
	content, err := gs.getSheetContent(info.SheetID)
=======
	content, err := gs.getSheetContent(info.SheetID, sheetRange)
>>>>>>> bcbff2271f0d5e0687938c53057b54b69a2193ac
	if err != nil {
		return "", err
	}

	return csv.FromRecords(content)
}

<<<<<<< HEAD
func (gs *GSheets) getSheetContent(sheetID string) ([][]interface{}, error) {
	resp, err := gs.srv.Spreadsheets.Values.Get(sheetID, readRange).Do()
=======
func (gs *GSheets) getSheetContent(sheetID, sheetRange string) ([][]interface{}, error) {
	batchGetCall := gs.srv.Spreadsheets.Values.BatchGet(sheetID)
	if sheetRange != "" {
		batchGetCall = batchGetCall.Ranges(sheetRange)
	}
	resp, err := batchGetCall.Do()
>>>>>>> bcbff2271f0d5e0687938c53057b54b69a2193ac
	if err != nil {
		return nil, err
	}

<<<<<<< HEAD
	if len(resp.Values) == 0 {
		return nil, errors.New("no data found in the sheet")
	}

	return resp.Values, nil
=======
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
>>>>>>> bcbff2271f0d5e0687938c53057b54b69a2193ac
}
