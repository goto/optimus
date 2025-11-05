package dex

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/goto/salt/log"

	"github.com/goto/optimus/config"
	"github.com/goto/optimus/core/resource"
	"github.com/goto/optimus/core/scheduler"
)

const tableStatsEndpoint = "/dex/tables/%s/%s/stats"

type Client struct {
	l          log.Logger
	config     *config.DexClientConfig
	httpClient *http.Client
}

// NewDexClient initializes client for communication with Dex
func NewDexClient(l log.Logger, dexClientConfig *config.DexClientConfig) (*Client, error) {
	if dexClientConfig.Host == "" {
		return nil, errors.New("dex client host is empty")
	}

	httpClient, err := newHTTPClient(dexClientConfig.Host)
	if err != nil {
		return nil, fmt.Errorf("error initializing http client: %w", err)
	}

	return &Client{
		l:          l,
		config:     dexClientConfig,
		httpClient: httpClient,
	}, nil
}

func (d *Client) IsManaged(ctx context.Context, resourceURN resource.URN) (bool, error) {
	return d.isResourceManagedUntil(ctx, resourceURN.GetStore(), resourceURN.GetName(), time.Now())
}

func (d *Client) IsComplete(ctx context.Context, resourceURN resource.URN, dateFrom, dateTo time.Time) (bool, interface{}, error) {
	stats, err := d.getCompletenessStats(ctx, resourceURN.GetStore(), resourceURN.GetName(), dateFrom, dateTo)
	if err != nil {
		return false, nil, err
	}
	for _, dateStat := range stats.DataCompletenessByDate {
		d.l.Info("dex completeness status", "date", dateStat.Date, "is_complete", dateStat.IsComplete)
	}
	return stats.IsComplete, stats, nil
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
	path := fmt.Sprintf(tableStatsEndpoint, store, tableName)

	values := url.Values{}
	values.Add("with_date_breakdown", "true")
	values.Add("from", startTime.Format(time.RFC3339)) // ISO8601
	values.Add("to", endTime.Format(time.RFC3339))     // ISO8601

	u, err := url.Parse(d.config.Host)
	if err != nil {
		return nil, fmt.Errorf("error parsing dex host url: %w", err)
	}
	u.Path = path
	u.RawQuery = values.Encode()

	request, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), http.NoBody)
	if err != nil {
		return nil, err
	}
	request.Header.Set("Accept", "application/json")

	return request, nil
}

func (d *Client) isResourceManagedUntil(ctx context.Context, store, resourceURN string, dataAvailabilityTime time.Time) (bool, error) {
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
		d.l.Error("unexpected status response", "status", response.Status, "body", response.Body)
		return false, fmt.Errorf("unexpected status response: %s", response.Status)
	}

	var statsResp dexTableStatsAPIResponse
	decoder := json.NewDecoder(response.Body)
	if err := decoder.Decode(&statsResp); err != nil {
		return false, fmt.Errorf("error decoding response: %w", err)
	}

	return statsResp.Stats.ProducerType == d.config.ProducerType && statsResp.Stats.IsManagedUntil(dataAvailabilityTime), nil
}

func (d *Client) getCompletenessStats(ctx context.Context, store, tableName string, startTime, endTime time.Time) (*scheduler.DataCompletenessStatus, error) {
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
		d.l.Error("unexpected status response", "status", response.Status, "body", response.Body)
		return nil, fmt.Errorf("unexpected status response: %s", response.Status)
	}

	var statsResp dexTableStatsAPIResponse
	decoder := json.NewDecoder(response.Body)
	if err := decoder.Decode(&statsResp); err != nil {
		return nil, fmt.Errorf("error decoding response: %w", err)
	}

	return statsResp.Stats.toSchedulerDataCompletenessStatus(), nil
}
