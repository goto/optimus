package gsheet

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"
	"google.golang.org/api/sheets/v4"

	"github.com/goto/optimus/ext/sheets/csv"
	"github.com/goto/optimus/internal/errors"
)

var delays = []int{10, 30, 90}

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

func (gs *GSheets) GetAsCSV(url, sheetRange string, withFormatOptions bool, formatFn func(int, any) string) (string, error) {
	info, err := FromURL(url)
	if err != nil {
		return "", err
	}

	content, err := gs.getSheetContent(info.SheetID, sheetRange, withFormatOptions)
	if err != nil {
		return "", err
	}

	return csv.FromRecords(content, formatFn)
}

func (gs *GSheets) getSheetContent(sheetID, sheetRange string, withFormatOptions bool) ([][]interface{}, error) {
	batchGetCall := gs.srv.Spreadsheets.Values.BatchGet(sheetID)
	if withFormatOptions {
		batchGetCall = batchGetCall.
			DateTimeRenderOption("FORMATTED_STRING").
			ValueRenderOption("UNFORMATTED_VALUE")
	}

	if sheetRange != "" {
		batchGetCall = batchGetCall.Ranges(sheetRange)
	}

	for _, d := range delays {
		resp, err := batchGetCall.Do()
		if err != nil {
			var batchErr *googleapi.Error
			if errors.As(err, &batchErr) && batchErr.Code == http.StatusTooManyRequests {
				// When too many request, sleep delay sec and try again once
				time.Sleep(time.Second * time.Duration(d))
				continue
			}

			return nil, err
		}

		if len(resp.ValueRanges) == 0 {
			return nil, errors.InvalidArgument("sheets", "no sheets found in the spreadsheet ")
		}

		return resp.ValueRanges[0].Values, nil
	}

	return nil, errors.InternalError("sheets", "failed all the retry attempts", nil)
}

func (gs *GSheets) GetSheetName(sheetURL string) (string, error) {
	sheetInfo, err := FromURL(sheetURL)
	if err != nil {
		return "", err
	}
	spreadsheet, err := gs.srv.Spreadsheets.Get(sheetInfo.SheetID).Do()
	if err != nil {
		return "", err
	}

	if len(spreadsheet.Sheets) == 0 {
		return "", errors.InvalidArgument("sheets", "no sub sheet found")
	}

	for _, s := range spreadsheet.Sheets {
		if s.Properties.SheetId == sheetInfo.GID {
			return s.Properties.Title, nil
		}
	}
	sid := spreadsheet.Sheets[0].Properties.Title
	return sid, err
}
