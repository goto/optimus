package evaluator

import (
	"fmt"
	"path/filepath"

	"github.com/vmware-labs/yaml-jsonpath/pkg/yamlpath"
	"gopkg.in/yaml.v3"

	"github.com/goto/optimus/internal/errors"
	"github.com/goto/salt/log"
)

type yamlpathEvaluator struct {
	logger       log.Logger
	filepath     string
	pathSelector *yamlpath.Path
}

// Evaluator returns the rawResource eg. query string given assets
// it returns whatever inside the defined filepath and its selector
// valid selector is defined from this BNF https://github.com/vmware-labs/yaml-jsonpath#syntax
func (e yamlpathEvaluator) Evaluate(assets map[string]string) string {
	cleanedFilePath := filepath.Base(e.filepath)
	raw, ok := assets[cleanedFilePath]
	if !ok {
		e.logger.Error("filepath %s not exist on asset", cleanedFilePath)
		return ""
	}
	var content yaml.Node
	if err := yaml.Unmarshal([]byte(raw), &content); err != nil {
		e.logger.Error("error to parse yaml content: %s", err.Error())
		return ""
	}

	results, _ := e.pathSelector.Find(&content)
	if len(results) < 1 {
		e.logger.Error("couln't find evaluator yaml result")
		return ""
	}

	return results[0].Value
}

func newYamlpathEvaluator(logger log.Logger, filepath, selector string) (*yamlpathEvaluator, error) {
	me := errors.NewMultiError("create jsonpath evaluator errors")
	if logger == nil {
		me.Append(fmt.Errorf("logger is nil"))
	}
	if filepath == "" {
		me.Append(fmt.Errorf("filepath is empty"))
	}
	if selector == "" {
		me.Append(fmt.Errorf("selector is empty"))
	}
	pathSelector, err := yamlpath.NewPath(selector)
	if err != nil {
		me.Append(fmt.Errorf("instantiate yamlpath error: %s", err.Error()))
	}
	if me.ToErr() != nil {
		return nil, me.ToErr()
	}

	return &yamlpathEvaluator{
		logger:       logger,
		filepath:     filepath,
		pathSelector: pathSelector,
	}, nil
}
