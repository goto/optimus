package resolver

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/goto/optimus/ext/store/bigquery"
	"github.com/goto/optimus/ext/store/bigquery/upstream"
	"github.com/goto/salt/log"
)

var (
	MaxBQApiRetries = 3
)

type UpstreamExtractor interface {
	ExtractUpstreams(ctx context.Context, query string, destinationResource *upstream.Resource) ([]*upstream.Resource, error)
}

// GenerateDependencies uses assets to find out the source tables of this
// transformation.
// Try using BQ APIs to search for referenced tables. This work for Select stmts
// but not for Merge/Scripts, for them use regex based search and then create
// fake select stmts. Fake statements help finding actual referenced tables in
// case regex based table is a view & not actually a source table. Because this
// fn should generate the actual source as dependency
// BQ2BQ dependencies are BQ tables in format "project:dataset.table"
// Note: only for bq2bq job, previously named as GenerateDependencies
func GenerateDependencies(ctx context.Context, l log.Logger, queryParserFunc upstream.QueryParser, svcAcc, query, destinationURN string) ([]string, error) {
	destinationResource, err := upstream.FromDestinationURN(destinationURN)
	if err != nil {
		return nil, fmt.Errorf("error getting destination resource: %w", err)
	}

	bqClient, err := bigquery.NewClient(ctx, svcAcc)
	if err != nil {
		return nil, fmt.Errorf("error creating bigquery client: %w", err)
	}
	upstreamExtractor, err := upstream.NewExtractor(bqClient, queryParserFunc)
	if err != nil {
		return nil, fmt.Errorf("error initializing upstream extractor: %w", err)
	}
	upstreams, err := extractUpstreams(ctx, l, upstreamExtractor, query, destinationResource)
	if err != nil {
		return nil, fmt.Errorf("error extracting upstreams: %w", err)
	}

	flattenedUpstreams := upstream.Resources(upstreams).GetFlattened()
	uniqueUpstreams := upstream.Resources(flattenedUpstreams).GetUnique()

	upstreamResources := []string{}
	for _, u := range uniqueUpstreams {
		upstreamResources = append(upstreamResources, u.URN())
	}

	return upstreamResources, nil
}

func extractUpstreams(ctx context.Context, l log.Logger, extractor UpstreamExtractor, query string, destinationResource *upstream.Resource) ([]*upstream.Resource, error) {
	for try := 1; try <= MaxBQApiRetries; try++ {
		upstreams, err := extractor.ExtractUpstreams(ctx, query, destinationResource)
		if err != nil {
			if strings.Contains(err.Error(), "net/http: TLS handshake timeout") ||
				strings.Contains(err.Error(), "unexpected EOF") ||
				strings.Contains(err.Error(), "i/o timeout") ||
				strings.Contains(err.Error(), "connection reset by peer") {
				// retry
				continue
			}

			l.Error("error extracting upstreams", err)
		}

		return upstreams, nil
	}
	return nil, errors.New("bigquery api retries exhausted")
}
