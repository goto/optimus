package evaluator

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/goto/salt/log"

	"github.com/goto/optimus/internal/errors"
)

var likelyPath = regexp.MustCompile(`(?i)^([a-zA-Z]:\\|\.{0,2}[\\/]|\/).+`)

type envEvaluator struct {
	logger log.Logger
	env    string
}

func newEnvEvaluator(logger log.Logger, env string) (*envEvaluator, error) {
	me := errors.NewMultiError("create env evaluator errors")
	if logger == nil {
		me.Append(fmt.Errorf("logger is nil"))
	}
	if env == "" {
		me.Append(fmt.Errorf("env is empty"))
	}
	if me.ToErr() != nil {
		return nil, me.ToErr()
	}

	return &envEvaluator{
		logger: logger,
		env:    env,
	}, nil
}

func (e *envEvaluator) Evaluate(assets, config map[string]string) string {
	// env evaluator returns the env as the rawResource
	// this is used to identify the upstream by env
	query := config[e.env]
	if likelyPath.MatchString(config[e.env]) {
		path := config[e.env]
		if q, ok := assets[strings.TrimPrefix(path, "/data/in/")]; !ok {
			e.logger.Error("filepath %s not exist on asset, use direct value", path)
		} else {
			query = q
		}
	}
	return query
}
