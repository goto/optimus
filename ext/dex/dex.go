package dex

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/goto/optimus/config"
	"github.com/goto/optimus/core/scheduler"
)

const tableStatsEndpoint = "/dex/tables/%s/%s/stats"

type Client struct {
	config     *config.DexClientConfig
	httpClient *http.Client
}

// NewDexClient initializes client for communication with Dex
func NewDexClient(dexClientConfig *config.DexClientConfig) (*Client, error) {
	if dexClientConfig.Host == "" {
		return nil, errors.New("dex client host is empty")
	}

	httpClient, err := newHTTPClient(dexClientConfig.Host)
	if err != nil {
		return nil, fmt.Errorf("error initializing http client: %w", err)
	}

	return &Client{
		config:     dexClientConfig,
		httpClient: httpClient,
	}, nil
}

func newHTTPClient(host string) (*http.Client, error) {
	httpClient := new(http.Client)

	if strings.HasPrefix(host, "https") {
		certPool, err := x509.SystemCertPool()
		if err != nil {
			return nil, fmt.Errorf("error reading system certificate: %w", err)
		}

		httpClient.Transport = &http.Transport{
			TLSClientConfig: &tls.Config{
				RootCAs:    certPool,
				MinVersion: tls.VersionTLS12,
			},
		}
	}

	return httpClient, nil
}

func (d *Client) constructGetTableStatsRequest(ctx context.Context, store, tableName string, startTime, endTime time.Time) (*http.Request, error) {
	var filters []string
	filters = append(filters, "with_date_breakdown=true")
	filters = append(filters, fmt.Sprintf("from=%s", startTime.Format("2006-01-02")))
	filters = append(filters, fmt.Sprintf("to=%s", endTime.Format("2006-01-02")))

	endpoint := fmt.Sprintf(tableStatsEndpoint, store, tableName)
	url := d.config.Host + endpoint + "?" + strings.Join(filters, "&")

	request, err := http.NewRequestWithContext(ctx, http.MethodGet, url, http.NoBody)
	if err != nil {
		return nil, err
	}
	request.Header.Set("Accept", "application/json")

	return request, nil
}

func (d *Client) IsResourceManagedUntil(ctx context.Context, store, resourceURN string, dataAvailabilityTime time.Time) (bool, error) {
	request, err := d.constructGetTableStatsRequest(ctx, store, resourceURN, dataAvailabilityTime.Add(time.Hour*-24), dataAvailabilityTime)
	if err != nil {
		return false, fmt.Errorf("error encountered when constructing request: %w", err)
	}

	response, err := d.httpClient.Do(request)
	if err != nil {
		return false, fmt.Errorf("error encountered when sending request: %w", err)
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		return false, fmt.Errorf("unexpected status response: %s", response.Status)
	}

	var statsResp dexTableStatsAPIResponse
	decoder := json.NewDecoder(response.Body)
	if err := decoder.Decode(&statsResp); err != nil {
		return false, fmt.Errorf("error decoding response: %w", err)
	}

	return statsResp.Stats.IsManagedUntil(dataAvailabilityTime), nil
}

func (d *Client) GetCompletenessStats(ctx context.Context, store, tableName string, startTime, endTime time.Time) (*scheduler.DataCompletenessStatus, error) {
	request, err := d.constructGetTableStatsRequest(ctx, store, tableName, startTime, endTime)
	if err != nil {
		return nil, fmt.Errorf("error encountered when constructing request: %w", err)
	}

	response, err := d.httpClient.Do(request)
	if err != nil {
		return nil, fmt.Errorf("error encountered when sending request: %w", err)
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status response: %s", response.Status)
	}

	var statsResp dexTableStatsAPIResponse
	decoder := json.NewDecoder(response.Body)
	if err := decoder.Decode(&statsResp); err != nil {
		return nil, fmt.Errorf("error decoding response: %w", err)
	}

	return statsResp.Stats.toSchedulerDataCompletenessStatus(), nil
}
