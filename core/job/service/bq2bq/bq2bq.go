package bq2bq

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/goto/salt/log"

	"github.com/goto/optimus/core/job"
	"github.com/goto/optimus/core/job/service/bq2bq/upstream"
)

const (
	destinationTypeBigquery = "bigquery"
	scheduledAtTimeLayout   = time.RFC3339

	DestinationURNFormat = "%s://%s"
)

var (
	Name    = "bq2bq"
	Version = "dev"

	QueryFileName    = "query.sql"
	BqServiceAccount = "BQ_SERVICE_ACCOUNT"
	MaxBQApiRetries  = 3

	LoadMethod        = "LOAD_METHOD"
	LoadMethodReplace = "REPLACE"

	QueryFileReplaceBreakMarker = "\n--*--optimus-break-marker--*--\n"
)

type FilesCompiler interface {
	Compile(fileMap map[string]string, context map[string]any) (map[string]string, error)
}

type UpstreamExtractorFactory interface {
	New(ctx context.Context, bqSvcAccount string) (UpstreamExtractor, error)
}

// GenerateDestination uses config details to build target table
// this format should match with GenerateDependencies output
func GenerateDestination(ctx context.Context, configs map[string]string) (job.ResourceURN, error) {
	proj, ok1 := configs["PROJECT"]
	dataset, ok2 := configs["DATASET"]
	tab, ok3 := configs["TABLE"]
	if ok1 && ok2 && ok3 {
		return job.ResourceURN(fmt.Sprintf("%s:%s.%s", proj, dataset, tab)), nil
	}
	return "", errors.New("missing config key required to generate destination")
}

// GenerateDependencies uses assets to find out the source tables of this
// transformation.
// Try using BQ APIs to search for referenced tables. This work for Select stmts
// but not for Merge/Scripts, for them use regex based search and then create
// fake select stmts. Fake statements help finding actual referenced tables in
// case regex based table is a view & not actually a source table. Because this
// fn should generate the actual source as dependency
// BQ2BQ dependencies are BQ tables in format "project:dataset.table"
func GenerateDependencies(ctx context.Context, l log.Logger, extractorFactory UpstreamExtractorFactory, configs, assets map[string]string, destinationURN job.ResourceURN) ([]job.ResourceURN, error) {
	svcAcc, ok := configs[BqServiceAccount]
	if !ok || len(svcAcc) == 0 {
		l.Error("Required secret BQ_SERVICE_ACCOUNT not found in config")
		return nil, fmt.Errorf("secret BQ_SERVICE_ACCOUNT required to generate dependencies not found for %s", Name)
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

	formattedUpstreams := []job.ResourceURN{}
	for _, u := range uniqueUpstreams {
		name := fmt.Sprintf("%s:%s.%s", u.Project, u.Dataset, u.Name)
		formattedUpstream := fmt.Sprintf(DestinationURNFormat, destinationTypeBigquery, name)
		formattedUpstreams = append(formattedUpstreams, job.ResourceURN(formattedUpstream))
	}

	return formattedUpstreams, nil
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

func destinationToResource(destination job.ResourceURN) (upstream.Resource, error) {
	splitDestination := strings.Split(destination.String(), ":")
	if len(splitDestination) != 2 {
		return upstream.Resource{}, fmt.Errorf("cannot get project from destination [%s]", destination.String())
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
