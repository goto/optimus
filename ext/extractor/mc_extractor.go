package extractor

import (
	"context"
	"fmt"

	"github.com/goto/optimus/internal/errors"
	"github.com/goto/salt/log"
)

type ViewGetter interface {
	GetDDLView(ctx context.Context, table string) (string, error)
}

type MCExtractor struct {
	client ViewGetter
	l      log.Logger
}

func NewMCExtractor(client ViewGetter, l log.Logger) (*MCExtractor, error) {
	me := errors.NewMultiError("construct mc extractor errors")
	if client == nil {
		me.Append(fmt.Errorf("client is nil"))
	}
	if l == nil {
		me.Append(fmt.Errorf("logger is nil"))
	}
	if len(me.Errors) > 0 {
		return nil, me.ToErr()
	}

	return &MCExtractor{
		client: client,
		l:      l,
	}, nil
}

func (e MCExtractor) Extract(ctx context.Context, resourceURNs []string) (map[string]string, error) {
	me := errors.NewMultiError("extract resourceURN to ddl errors")
	urnToDDLAll := make(map[string]string)

	for _, resourceURN := range resourceURNs {
		ddl, err := e.client.GetDDLView(ctx, resourceURN)
		if err != nil {
			me.Append(err)
		}
		urnToDDLAll[resourceURN] = ddl
	}
	return urnToDDLAll, me.ToErr()
}
