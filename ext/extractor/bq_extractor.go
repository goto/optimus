package extractor

import (
	"context"
	"fmt"
	"strings"

	"github.com/goto/salt/log"

	"github.com/goto/optimus/ext/store/bigquery"
	"github.com/goto/optimus/internal/errors"
)

type DDLViewGetter interface {
	BulkGetDDLView(ctx context.Context, dataset bigquery.ProjectDataset, names []string) (map[bigquery.ResourceURN]string, error)
}

type BQExtractor struct {
	client DDLViewGetter
	l      log.Logger
}

func NewBQExtractor(client DDLViewGetter, l log.Logger) (*BQExtractor, error) {
	me := errors.NewMultiError("construct bq extractor errors")
	if client == nil {
		me.Append(fmt.Errorf("client is nil"))
	}
	if l == nil {
		me.Append(fmt.Errorf("logger is nil"))
	}
	if len(me.Errors) > 0 {
		return nil, me.ToErr()
	}

	return &BQExtractor{
		client: client,
		l:      l,
	}, nil
}

// Extract returns map of urns and its query string given list of urns
// It extract the corresponding query only if the urn is considered as a view
func (e BQExtractor) Extract(ctx context.Context, resourceURNs []bigquery.ResourceURN) (urnToDDL map[bigquery.ResourceURN]string, err error) {
	// grouping
	dsToNames := bigquery.ResourceURNs(resourceURNs).GroupByProjectDataset()

	// fetch ddl for each resourceURN
	urnToDDL = make(map[bigquery.ResourceURN]string, len(resourceURNs))
	const maxRetry = 3
	for ds, names := range dsToNames {
		urnToDDLView, err := bulkGetDDLViewWithRetry(e.client, e.l, maxRetry)(ctx, ds, names)
		if err != nil {
			return nil, err
		}
		for urn, ddl := range urnToDDLView {
			urnToDDL[urn] = ddl
		}
	}

	return urnToDDL, nil
}

func bulkGetDDLViewWithRetry(c DDLViewGetter, l log.Logger, retry int) func(context.Context, bigquery.ProjectDataset, []string) (map[bigquery.ResourceURN]string, error) {
	return func(ctx context.Context, dataset bigquery.ProjectDataset, names []string) (map[bigquery.ResourceURN]string, error) {
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
