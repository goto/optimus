package resolver

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"cloud.google.com/go/bigquery"
	"github.com/googleapis/google-cloud-go-testing/bigquery/bqiface"
	"github.com/goto/optimus/core/job/resolver/upstream"
	"github.com/goto/salt/log"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/drive/v2"
	"google.golang.org/api/option"
	storageV1 "google.golang.org/api/storage/v1"
)

var (
	QueryFileName    = "query.sql"
	BqServiceAccount = "BQ_SERVICE_ACCOUNT"
	MaxBQApiRetries  = 3
)

type UpstreamExtractor interface {
	ExtractUpstreams(ctx context.Context, query string, resourcesToIgnore []upstream.Resource) ([]*upstream.Upstream, error)
}

type UpstreamExtractorFactory interface {
	New(ctx context.Context, bqSvcAccount string) (UpstreamExtractor, error)
}

type DefaultUpstreamExtractorFactory struct{}

func (d *DefaultUpstreamExtractorFactory) New(ctx context.Context, bqSvcAccount string) (UpstreamExtractor, error) {
	client, err := newBQClient(ctx, bqSvcAccount)
	if err != nil {
		return nil, fmt.Errorf("error creating bigquery client: %w", err)
	}
	return upstream.NewExtractor(client)
}

func newBQClient(ctx context.Context, svcAccount string) (bqiface.Client, error) {
	cred, err := google.CredentialsFromJSON(ctx, []byte(svcAccount),
		bigquery.Scope, storageV1.CloudPlatformScope, drive.DriveScope)
	if err != nil {
		return nil, fmt.Errorf("failed to read secret: %w", err)
	}

	client, err := bigquery.NewClient(ctx, cred.ProjectID, option.WithCredentials(cred))
	if err != nil {
		return nil, fmt.Errorf("failed to create BQ client: %w", err)
	}

	return bqiface.AdaptClient(client), nil
}

// GenerateDependencies uses assets to find out the source tables of this
// transformation.
// Try using BQ APIs to search for referenced tables. This work for Select stmts
// but not for Merge/Scripts, for them use regex based search and then create
// fake select stmts. Fake statements help finding actual referenced tables in
// case regex based table is a view & not actually a source table. Because this
// fn should generate the actual source as dependency
// BQ2BQ dependencies are BQ tables in format "project:dataset.table"
// Note: only for bq2bq jobs
func GenerateDependencies(ctx context.Context, l log.Logger, extractorFactory UpstreamExtractorFactory, configs, assets map[string]string, destinationURN string) ([]string, error) {
	svcAcc, ok := configs[BqServiceAccount]
	if !ok || len(svcAcc) == 0 {
		l.Error("Required secret BQ_SERVICE_ACCOUNT not found in config")
		return nil, fmt.Errorf("secret BQ_SERVICE_ACCOUNT required to generate dependencies not found")
	}

	query, ok := assets[QueryFileName]
	if !ok {
		return nil, errors.New("empty sql file")
	}

	destinationResource, err := destinationToResource(destinationURN)
	if err != nil {
		return nil, fmt.Errorf("error getting destination resource: %w", err)
	}

	upstreamExtractor, err := extractorFactory.New(ctx, svcAcc)
	if err != nil {
		return nil, fmt.Errorf("error initializing upstream extractor: %w", err)
	}
	upstreams, err := extractUpstreams(ctx, l, upstreamExtractor, query, svcAcc, []upstream.Resource{destinationResource})
	if err != nil {
		return nil, fmt.Errorf("error extracting upstreams: %w", err)
	}

	flattenedUpstreams := upstream.FlattenUpstreams(upstreams)
	uniqueUpstreams := upstream.UniqueFilterResources(flattenedUpstreams)

	upstreamResources := []string{}
	for _, u := range uniqueUpstreams {
		name := fmt.Sprintf("%s:%s.%s", u.Project, u.Dataset, u.Name)
		upstreamResources = append(upstreamResources, name)
	}

	return upstreamResources, nil
}

func extractUpstreams(ctx context.Context, l log.Logger, extractor UpstreamExtractor, query, svcAccSecret string, resourcesToIgnore []upstream.Resource) ([]*upstream.Upstream, error) {
	for try := 1; try <= MaxBQApiRetries; try++ {
		upstreams, err := extractor.ExtractUpstreams(ctx, query, resourcesToIgnore)
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

func destinationToResource(destination string) (upstream.Resource, error) {
	splitDestination := strings.Split(destination, ":")
	if len(splitDestination) != 2 {
		return upstream.Resource{}, fmt.Errorf("cannot get project from destination [%s]", destination)
	}

	project, datasetTable := splitDestination[0], splitDestination[1]

	splitDataset := strings.Split(datasetTable, ".")
	if len(splitDataset) != 2 {
		return upstream.Resource{}, fmt.Errorf("cannot get dataset and table from [%s]", datasetTable)
	}

	return upstream.Resource{
		Project: project,
		Dataset: splitDataset[0],
		Name:    splitDataset[1],
	}, nil
}
