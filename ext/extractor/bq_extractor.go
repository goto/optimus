package extractor

import (
	"context"
	"fmt"
	"strings"

	"github.com/goto/optimus/core/job"
	"github.com/goto/optimus/ext/store/bigquery"
	"github.com/goto/salt/log"
)

type (
	// ExtractorFunc extracts the rawSources from given list of resource
	ExtractorFunc             func(context.Context, log.Logger, []job.ResourceURN) (map[job.ResourceURN]string, error)
	DefaultBQExtractorFactory struct{}
)

type DDLViewGetter interface {
	BulkGetDDLView(ctx context.Context, dataset bigquery.Dataset, names []string) (map[job.ResourceURN]string, error)
}

func (DefaultBQExtractorFactory) New(ctx context.Context, svcAcc string) (ExtractorFunc, error) {
	client, err := bigquery.NewClient(ctx, svcAcc)
	if err != nil {
		return nil, err
	}

	return DefaultExtractorFunc(client), nil
}

func DefaultExtractorFunc(client DDLViewGetter) ExtractorFunc {
	return func(ctx context.Context, l log.Logger, resourceURNs []job.ResourceURN) (urnToDDL map[job.ResourceURN]string, err error) {
		if client == nil {
			return nil, fmt.Errorf("client is nil")
		}

		// grouping
		dsToNames := datasetToNames(resourceURNs)

		// fetch ddl for each resourceURN
		urnToDDL = make(map[job.ResourceURN]string, len(resourceURNs))
		const maxRetry = 3
		for ds, names := range dsToNames {
			urnToDDLView, err := bulkGetDDLViewWithRetry(client, l, maxRetry)(ctx, ds, names)
			if err != nil {
				return nil, err
			}
			for urn, ddl := range urnToDDLView {
				urnToDDL[urn] = ddl
			}
		}

		return urnToDDL, nil
	}
}

// TODO: find a better way for grouping
func datasetToNames(resourceURNs []job.ResourceURN) map[bigquery.Dataset][]string {
	output := make(map[bigquery.Dataset][]string)

	for _, resourceURN := range resourceURNs {
		resourceFullName := strings.TrimPrefix(resourceURN.String(), "bigquery://")
		project, datasetName := strings.Split(resourceFullName, ":")[0], strings.Split(resourceFullName, ":")[1]
		dataset, name := strings.Split(datasetName, ".")[0], strings.Split(datasetName, ".")[1]
		ds := bigquery.Dataset{Project: project, DatasetName: dataset}
		if _, ok := output[ds]; !ok {
			output[ds] = []string{}
		}
		output[ds] = append(output[ds], name)
	}

	return output
}

func bulkGetDDLViewWithRetry(c DDLViewGetter, l log.Logger, retry int) func(context.Context, bigquery.Dataset, []string) (map[job.ResourceURN]string, error) {
	return func(ctx context.Context, dataset bigquery.Dataset, names []string) (map[job.ResourceURN]string, error) {
		for try := 1; try <= retry; try++ {
			urnToDDL, err := c.BulkGetDDLView(ctx, dataset, names)
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

			return urnToDDL, nil
		}
		return nil, fmt.Errorf("bigquery api retries exhausted")
	}
}
