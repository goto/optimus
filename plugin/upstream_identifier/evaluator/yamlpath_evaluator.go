package evaluator

import (
	"fmt"
	"path/filepath"
	"strings"

	"github.com/goto/salt/log"
	"github.com/vmware-labs/yaml-jsonpath/pkg/yamlpath"
	"gopkg.in/yaml.v3"

	"github.com/goto/optimus/internal/errors"
)

type yamlPathEvaluator struct {
	logger       log.Logger
	filepath     string
	pathSelector *yamlpath.Path
}

// Evaluator returns the rawResource eg. query string given assets
// it returns whatever inside the defined filepath and its selector
// valid selector is defined from this BNF https://github.com/vmware-labs/yaml-jsonpath#syntax
func (e yamlPathEvaluator) Evaluate(assets map[string]string) string {
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
		e.logger.Error("couldn't find the result of the yaml evaluator")
		return ""
	}
	values := make([]string, len(results))
	for i, result := range results {
		values[i] = e.extractValue(result.Value, assets)
	}

	return strings.Join(values, "\n")
}

func (e yamlPathEvaluator) extractValue(value string, assets map[string]string) string {
	// heuristically value can potentially be a filepath if there's no spaces
	if !strings.Contains(value, " ") {
		cleanedFilePath := filepath.Base(value)
		if raw, ok := assets[cleanedFilePath]; ok {
			return raw
		}
	}
	return value
}

func newYamlPathEvaluator(logger log.Logger, filepath, selector string) (*yamlPathEvaluator, error) {
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
		me.Append(fmt.Errorf("instantiate yamlpath error: %w", err))
	}
	if me.ToErr() != nil {
		return nil, me.ToErr()
	}

	return &yamlPathEvaluator{
		logger:       logger,
		filepath:     filepath,
		pathSelector: pathSelector,
	}, nil
}
