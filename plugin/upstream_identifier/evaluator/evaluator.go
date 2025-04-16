package evaluator

import (
	"fmt"

	"github.com/goto/salt/log"
)

type Evaluator interface {
	Evaluate(assets map[string]string, config map[string]string) (rawResource string)
}

type EvaluatorFactory struct {
	logger log.Logger
}

func (e EvaluatorFactory) GetFileEvaluator(filepath string) (Evaluator, error) {
	return newFileEvaluator(e.logger, filepath)
}

func (e EvaluatorFactory) GetYamlPathEvaluator(filepath, selector string) (Evaluator, error) {
	return newYamlPathEvaluator(e.logger, filepath, selector)
}

func (e EvaluatorFactory) GetEnvEvaluator(env string) (Evaluator, error) {
	return newEnvEvaluator(e.logger, env)
}

func NewEvaluatorFactory(logger log.Logger) (*EvaluatorFactory, error) {
	if logger == nil {
		return nil, fmt.Errorf("logger is nil")
	}
	return &EvaluatorFactory{logger: logger}, nil
}
