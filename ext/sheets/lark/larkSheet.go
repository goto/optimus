package lark

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"strings"

	"github.com/goto/optimus/ext/sheets/csv"
)

var (
	sheetTokenRegex = regexp.MustCompile(`larksuite.com/sheets/([^/]*)`)
)

func (lc *Client) enrichTenantAccessToken(authSecret string) error {
	lr := larkRequest{
		Host:   Host,
		Path:   GetAccessTokenURL,
		Method: http.MethodPost,
		Body:   []byte(authSecret),
	}
	resBody, err := lc.Invoke(context.TODO(), lr)
	if err != nil {
		return err
	}
	var resp TokenResponse
	err = json.Unmarshal(resBody, &resp)
	if err != nil {
		return err
	}
	lc.TenantAccessToken = resp.TenantAccessToken
	return nil
}

func NewLarkClient(authSecret string) (*Client, error) {
	lc := Client{client: &http.Client{}}

	err := lc.enrichTenantAccessToken(authSecret)
	if err != nil {
		return nil, err
	}
	return &lc, nil
}

func FromURL(u1 string) (string, error) {
	res := sheetTokenRegex.FindStringSubmatch(u1)
	if len(res) < 2 || res[1] == "" {
		return "", errors.New("not able to get spreadsheetToken")
	}
	return res[1], nil
}

func FromRange(sheetRange string) (string, string) {
	res := strings.Split(sheetRange, "!")
	if len(res) == 2 {
		return res[0], res[1]
	}
	return res[0], ""
}

func (lc *Client) GetSheetMetadata(ctx context.Context, sheetToken string) (*SheetMetadata, error) {
	resp, err := lc.Invoke(ctx, larkRequest{
		Host:   Host,
		Method: http.MethodGet,
		Path:   fmt.Sprintf(GetLSheetMeta, sheetToken),
	})
	if err != nil {
		return nil, err
	}
	var sheetMetadata SheetMetadata
	err = json.Unmarshal(resp, &sheetMetadata)
	if err != nil {
		return nil, err
	}
	return &sheetMetadata, nil
}

func parseSheetRange(metadata *SheetMetadata, sheetRange string) (string, error) {
	var sheetId string
	sheetName, rangeQuery := FromRange(sheetRange)
	for _, sheet := range metadata.Data.Sheets {
		if sheet.Title == sheetName {
			sheetId = sheet.SheetID
			break
		}
	}
	if sheetId == "" {
		return "", fmt.Errorf("sheet title: %s not found in Lark Spreadsheet", sheetRange)
	}
	if rangeQuery != "" {
		return sheetId + "!" + rangeQuery, nil
	}
	return sheetId, nil
}

func (lc *Client) GetRevisionID(url string) (int, error) {
	sheetToken, err := FromURL(url)
	if err != nil {
		return 0, err
	}
	sheetMetadata, err := lc.GetSheetMetadata(context.Background(), sheetToken)
	if err != nil {
		return 0, err
	}
	revisionNumber := sheetMetadata.GetRevisionId()
	return revisionNumber, nil
}

func (lc *Client) GetAsCSV(url, sheetRange string, getFormattedDateTime, getFormattedData bool, columnCount int, formatFn func(int, int, any) (string, error)) (int, string, error) {
	sheetToken, err := FromURL(url)
	if err != nil {
		return 0, "", err
	}
	sheetMetadata, err := lc.GetSheetMetadata(context.Background(), sheetToken)
	if err != nil {
		return 0, "", err
	}
	revisionNumber := sheetMetadata.GetRevisionId()
	parsedRange, err := parseSheetRange(sheetMetadata, sheetRange)
	if err != nil {
		return revisionNumber, "", err
	}

	content, err := lc.GetSheetContent(context.Background(), sheetToken, parsedRange, getFormattedDateTime, getFormattedData)
	if err != nil {
		return revisionNumber, "", err
	}
	contentString, err := csv.FromRecords[any](content, columnCount, formatFn)
	return revisionNumber, contentString, err
}

func (lc *Client) GetSheetContent(ctx context.Context, sheetToken, sheetRange string, getFormattedDateTime, getFormattedData bool) ([][]interface{}, error) {
	query := make(map[string]string)
	if getFormattedData {
		query["valueRenderOption"] = "ToString"
	}
	if getFormattedDateTime {
		query["dateTimeRenderOption"] = "FormattedString"
	}

	resp, err := lc.Invoke(ctx, larkRequest{
		Host:   Host,
		Method: http.MethodGet,
		Path:   fmt.Sprintf(GetLSheetContent, sheetToken, sheetRange),
		Query:  query,
	})

	spreadSheetResponse := GetSpreadSheetResponse{}

	err = json.Unmarshal(resp, &spreadSheetResponse)
	if err != nil {
		return nil, err
	}

	return spreadSheetResponse.SpreadSheetData.ValueRange.Values, nil
}
