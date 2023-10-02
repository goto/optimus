package evaluator

import (
	"fmt"
	"path/filepath"

	"github.com/goto/optimus/internal/errors"
	"github.com/goto/salt/log"
)

type fileEvaluator struct {
	logger   log.Logger
	filepath string
}

// Evaluator returns the rawResource eg. query string given assets
// it returns whatever inside the defined filepath
func (e fileEvaluator) Evaluate(assets map[string]string) string {
	cleanedFilePath := filepath.Base(e.filepath)
	rawResource, ok := assets[cleanedFilePath]
	if !ok {
		e.logger.Error("filepath %s not exist on asset", cleanedFilePath)
		return ""
	}
	return rawResource
}

func newFileEvaluator(logger log.Logger, filepath string) (*fileEvaluator, error) {
	me := errors.NewMultiError("create file evaluator errors")
	if logger == nil {
		me.Append(fmt.Errorf("logger is nil"))
	}
	if filepath == "" {
		me.Append(fmt.Errorf("filepath is empty"))
	}
	if me.ToErr() != nil {
		return nil, me.ToErr()
	}

	return &fileEvaluator{
		logger:   logger,
		filepath: filepath,
	}, nil
}
