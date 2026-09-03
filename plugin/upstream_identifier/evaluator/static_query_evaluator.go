package evaluator

import (
	"fmt"

	"github.com/goto/salt/log"

	"github.com/goto/optimus/internal/errors"
)

// staticQueryEvaluator feeds a fixed raw query string to an upstream identifier,
// regardless of the assets/config passed to Evaluate. Used when the query text is
// already known up front (no job asset file to read it from).
type staticQueryEvaluator struct {
	logger log.Logger
	query  string
}

func (e staticQueryEvaluator) Evaluate(_, _ map[string]string) string {
	return e.query
}

func newStaticQueryEvaluator(logger log.Logger, query string) (*staticQueryEvaluator, error) {
	me := errors.NewMultiError("create static query evaluator errors")
	if logger == nil {
		me.Append(fmt.Errorf("logger is nil"))
	}
	if query == "" {
		me.Append(fmt.Errorf("query is empty"))
	}
	if me.ToErr() != nil {
		return nil, me.ToErr()
	}

	return &staticQueryEvaluator{
		logger: logger,
		query:  query,
	}, nil
}
