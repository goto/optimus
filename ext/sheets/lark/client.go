package lark

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/goto/optimus/internal/errors"
)

const (
	EntityLark        = "lark"
	Host              = "open.larksuite.com"
	GetAccessTokenURL = "/open-apis/auth/v3/tenant_access_token/internal"
	GetLSheetContent  = "/open-apis/sheets/v2/spreadsheets/%s/values/%s"
	GetLSheetMeta     = "/open-apis/sheets/v2/spreadsheets/%s/metainfo"
)

var delays = []int{10, 30, 90}

type Client struct {
	client            *http.Client
	TenantAccessToken string
}

type SheetMetadata struct {
	Code int    `json:"code"`
	Data Data   `json:"data"`
	Msg  string `json:"msg"`
}

func (s SheetMetadata) GetRevisionId() int {
	return s.Data.Properties.Revision
}

type Data struct {
	Properties       Properties `json:"properties"`
	Sheets           []Sheet    `json:"sheets"`
	SpreadsheetToken string     `json:"spreadsheetToken"`
}

type Properties struct {
	OwnerUser  int64  `json:"ownerUser"`
	Revision   int    `json:"revision"`
	SheetCount int    `json:"sheetCount"`
	Title      string `json:"title"`
}

type Sheet struct {
	ColumnCount    int    `json:"columnCount"`
	FrozenColCount int    `json:"frozenColCount"`
	FrozenRowCount int    `json:"frozenRowCount"`
	Index          int    `json:"index"`
	RowCount       int    `json:"rowCount"`
	SheetID        string `json:"sheetId"`
	Title          string `json:"title"`
}

type GetSpreadSheetResponse struct {
	Code            int             `json:"code"`
	SpreadSheetData SpreadSheetData `json:"data"`
	Msg             string          `json:"msg"`
}

type SpreadSheetData struct {
	Revision         int        `json:"revision"`
	SpreadsheetToken string     `json:"spreadsheetToken"`
	ValueRange       ValueRange `json:"valueRange"`
}

type ValueRange struct {
	MajorDimension string          `json:"majorDimension"`
	Range          string          `json:"range"`
	Revision       int             `json:"revision"`
	Values         [][]interface{} `json:"values"`
}

type TokenResponse struct {
	Code              int    `json:"code"`
	Expire            int    `json:"expire"`
	Msg               string `json:"msg"`
	TenantAccessToken string `json:"tenant_access_token"`
}

type larkRequest struct {
	Host    string
	Method  string
	Path    string
	Query   map[string]string
	Body    []byte
	Headers map[string]string
}

func buildEndPoint(req larkRequest) string {
	params := url.Values{}
	for key, val := range req.Query {
		params.Add(key, val)
	}
	u := &url.URL{
		Scheme:   "https",
		Host:     strings.Trim(req.Host, "/"),
		Path:     req.Path,
		RawQuery: params.Encode(),
	}
	return u.String()
}

func (lc *Client) Invoke(ctx context.Context, r larkRequest) ([]byte, error) {
	var resp []byte

	endpoint := buildEndPoint(r)
	request, err := http.NewRequestWithContext(ctx, r.Method, endpoint, bytes.NewBuffer(r.Body))
	if err != nil {
		return resp, fmt.Errorf("failed to build http request for %s due to %w", endpoint, err)
	}

	if len(r.Headers) > 0 {
		for k, v := range r.Headers {
			request.Header.Set(k, v)
		}
	}

	request.Header.Set("Content-Type", "application/json")
	if lc.TenantAccessToken != "" {
		request.Header.Set("Authorization", fmt.Sprintf("Bearer %s", lc.TenantAccessToken))
	}

	httpResp, respErr := lc.client.Do(request)
	if respErr != nil {
		return resp, fmt.Errorf("failed to call Lark %s due to %w", endpoint, respErr)
	}
	if httpResp.StatusCode != http.StatusOK {
		httpResp.Body.Close()
		return resp, fmt.Errorf("status code received %d on calling %s", httpResp.StatusCode, endpoint)
	}
	return parseResponse(httpResp)
}

func parseResponse(resp *http.Response) ([]byte, error) {
	var body []byte
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		return body, errors.Wrap(EntityLark, "failed to read airflow response", err)
	}
	return body, nil
}
