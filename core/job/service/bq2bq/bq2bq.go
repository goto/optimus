package bq2bq

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/goto/optimus/sdk/plugin"
	"github.com/goto/salt/log"

	"github.com/goto/optimus/core/job/service/bq2bq/upstream"
)

const (
	ConfigKeyDstart = "DSTART"
	ConfigKeyDend   = "DEND"

	dataTypeEnv             = "env"
	dataTypeFile            = "file"
	destinationTypeBigquery = "bigquery"
	scheduledAtTimeLayout   = time.RFC3339
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

type BQ2BQ struct {
	Compiler *Compiler

	logger log.Logger
}

type FilesCompiler interface {
	Compile(fileMap map[string]string, context map[string]any) (map[string]string, error)
}

func CompileAssets(ctx context.Context, compiler FilesCompiler, startTime, endTime time.Time, configs, systemEnvVars, assets map[string]string) (map[string]string, error) {
	method, ok := configs["LOAD_METHOD"]
	if !ok || method != "REPLACE" {
		return assets, nil
	}

	// partition window in range
	instanceEnvMap := map[string]interface{}{}
	for name, value := range systemEnvVars {
		instanceEnvMap[name] = value
	}

	// TODO: making few assumptions here, should be documented
	// assume destination table is time partitioned
	// assume table is partitioned as DAY
	const dayHours = time.Duration(24)
	partitionDelta := time.Hour * dayHours

	// find destination partitions
	var destinationsPartitions []struct {
		start time.Time
		end   time.Time
	}
	for currentPart := startTime; currentPart.Before(endTime); currentPart = currentPart.Add(partitionDelta) {
		destinationsPartitions = append(destinationsPartitions, struct {
			start time.Time
			end   time.Time
		}{
			start: currentPart,
			end:   currentPart.Add(partitionDelta),
		})
	}

	// check if window size is greater than partition delta(a DAY), if not do nothing
	if endTime.Sub(startTime) <= partitionDelta {
		return assets, nil
	}

	var parsedQueries []string
	var err error

	compiledAssetMap := assets
	// append job spec assets to list of files need to write
	fileMap := compiledAssetMap
	queryFileReplaceBreakMarker := "\n--*--optimus-break-marker--*--\n"
	for _, part := range destinationsPartitions {
		instanceEnvMap["DSTART"] = part.start.Format(time.RFC3339)
		instanceEnvMap["DEND"] = part.end.Format(time.RFC3339)
		if compiledAssetMap, err = compiler.Compile(fileMap, instanceEnvMap); err != nil {
			return nil, err
		}
		parsedQueries = append(parsedQueries, compiledAssetMap["query.sql"])
	}
	compiledAssetMap["query.sql"] = strings.Join(parsedQueries, queryFileReplaceBreakMarker)

	return compiledAssetMap, nil
}

// GenerateDestination uses config details to build target table
// this format should match with GenerateDependencies output
func (b *BQ2BQ) GenerateDestination(ctx context.Context, request plugin.GenerateDestinationRequest) (*plugin.GenerateDestinationResponse, error) {
	proj, ok1 := request.Config.Get("PROJECT")
	dataset, ok2 := request.Config.Get("DATASET")
	tab, ok3 := request.Config.Get("TABLE")
	if ok1 && ok2 && ok3 {
		return &plugin.GenerateDestinationResponse{
			Destination: fmt.Sprintf("%s:%s.%s", proj.Value, dataset.Value, tab.Value),
			Type:        destinationTypeBigquery,
		}, nil
	}
	return nil, errors.New("missing config key required to generate destination")
}

// GenerateDependencies uses assets to find out the source tables of this
// transformation.
// Try using BQ APIs to search for referenced tables. This work for Select stmts
// but not for Merge/Scripts, for them use regex based search and then create
// fake select stmts. Fake statements help finding actual referenced tables in
// case regex based table is a view & not actually a source table. Because this
// fn should generate the actual source as dependency
// BQ2BQ dependencies are BQ tables in format "project:dataset.table"
func (b *BQ2BQ) GenerateDependencies(ctx context.Context, request plugin.GenerateDependenciesRequest) (response *plugin.GenerateDependenciesResponse, err error) {
	response = &plugin.GenerateDependenciesResponse{}
	response.Dependencies = []string{}

	var svcAcc string
	accConfig, ok := request.Config.Get(BqServiceAccount)
	if !ok || len(accConfig.Value) == 0 {
		b.logger.Error("Required secret BQ_SERVICE_ACCOUNT not found in config")
		return response, fmt.Errorf("secret BQ_SERVICE_ACCOUNT required to generate dependencies not found for %s", Name)
	} else {
		svcAcc = accConfig.Value
	}

	queryData, ok := request.Assets.Get(QueryFileName)
	if !ok {
		return nil, errors.New("empty sql file")
	}

	selfTable, err := b.GenerateDestination(ctx, plugin.GenerateDestinationRequest{
		Config: request.Config,
		Assets: request.Assets,
	})
	if err != nil {
		return response, err
	}

	destinationResource, err := b.destinationToResource(selfTable)
	if err != nil {
		return response, fmt.Errorf("error getting destination resource: %w", err)
	}

	upstreams, err := b.extractUpstreams(ctx, queryData.Value, svcAcc, []upstream.Resource{destinationResource})
	if err != nil {
		return response, fmt.Errorf("error extracting upstreams: %w", err)
	}

	flattenedUpstreams := upstream.FlattenUpstreams(upstreams)
	uniqueUpstreams := upstream.UniqueFilterResources(flattenedUpstreams)

	formattedUpstreams := b.formatUpstreams(uniqueUpstreams, func(r upstream.Resource) string {
		name := fmt.Sprintf("%s:%s.%s", r.Project, r.Dataset, r.Name)
		return fmt.Sprintf(plugin.DestinationURNFormat, selfTable.Type, name)
	})

	response.Dependencies = formattedUpstreams

	return response, nil
}

func (b *BQ2BQ) extractUpstreams(ctx context.Context, query, svcAccSecret string, resourcesToIgnore []upstream.Resource) ([]*upstream.Upstream, error) {
	for try := 1; try <= MaxBQApiRetries; try++ {
		client, err := newBQClient(ctx, svcAccSecret)
		if err != nil {
			return nil, fmt.Errorf("error creating bigquery client: %w", err)
		}

		extractor, err := newUpstreamExtractor(client)
		if err != nil {
			return nil, fmt.Errorf("error initializing upstream extractor: %w", err)
		}

		upstreams, err := extractor.ExtractUpstreams(ctx, query, resourcesToIgnore)
		if err != nil {
			if strings.Contains(err.Error(), "net/http: TLS handshake timeout") ||
				strings.Contains(err.Error(), "unexpected EOF") ||
				strings.Contains(err.Error(), "i/o timeout") ||
				strings.Contains(err.Error(), "connection reset by peer") {
				// retry
				continue
			}

			b.logger.Error("error extracting upstreams", err)
		}

		return upstreams, nil
	}
	return nil, errors.New("bigquery api retries exhausted")
}

func (b *BQ2BQ) destinationToResource(destination *plugin.GenerateDestinationResponse) (upstream.Resource, error) {
	splitDestination := strings.Split(destination.Destination, ":")
	if len(splitDestination) != 2 {
		return upstream.Resource{}, fmt.Errorf("cannot get project from destination [%s]", destination.Destination)
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

func (b *BQ2BQ) formatUpstreams(upstreams []upstream.Resource, fn func(r upstream.Resource) string) []string {
	var output []string
	for _, u := range upstreams {
		output = append(output, fn(u))
	}

	return output
}
