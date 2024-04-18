package plugin_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/goto/salt/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/goto/optimus/internal/lib"
	"github.com/goto/optimus/plugin"
	upstreamidentifier "github.com/goto/optimus/plugin/upstream_identifier"
	"github.com/goto/optimus/plugin/upstream_identifier/evaluator"
	"github.com/goto/optimus/plugin/yaml"
	p "github.com/goto/optimus/sdk/plugin"
)

func TestNewPluginService(t *testing.T) {
	t.Run("should return error when logger is nil", func(t *testing.T) {
		var logger log.Logger = nil
		pluginGetter := new(PluginGetter)
		defer pluginGetter.AssertExpectations(t)
		upstreamIdentifierFactory := NewUpstreamIdentifierFactory(t)
		evaluatorFactory := new(EvaluatorFactory)
		defer evaluatorFactory.AssertExpectations(t)

		_, err := plugin.NewPluginService(logger, pluginGetter, upstreamIdentifierFactory, evaluatorFactory)
		assert.ErrorContains(t, err, "logger is nil")
	})
	t.Run("should return error when pluginGetter is nil", func(t *testing.T) {
		logger := log.NewNoop()
		var pluginGetter plugin.PluginGetter = nil
		upstreamIdentifierFactory := NewUpstreamIdentifierFactory(t)
		evaluatorFactory := new(EvaluatorFactory)
		defer evaluatorFactory.AssertExpectations(t)

		_, err := plugin.NewPluginService(logger, pluginGetter, upstreamIdentifierFactory, evaluatorFactory)
		assert.ErrorContains(t, err, "pluginGetter is nil")
	})
	t.Run("should return error when upstreamIdentifierFactory is nil", func(t *testing.T) {
		logger := log.NewNoop()
		pluginGetter := new(PluginGetter)
		defer pluginGetter.AssertExpectations(t)
		var upstreamIdentifierFactory plugin.UpstreamIdentifierFactory = nil
		evaluatorFactory := new(EvaluatorFactory)
		defer evaluatorFactory.AssertExpectations(t)

		_, err := plugin.NewPluginService(logger, pluginGetter, upstreamIdentifierFactory, evaluatorFactory)
		assert.ErrorContains(t, err, "upstreamIdentifierFactory is nil")
	})
	t.Run("should return error when evaluatorFactory is nil", func(t *testing.T) {
		logger := log.NewNoop()
		pluginGetter := new(PluginGetter)
		defer pluginGetter.AssertExpectations(t)
		upstreamIdentifierFactory := NewUpstreamIdentifierFactory(t)
		var evaluatorFactory plugin.EvaluatorFactory = nil

		_, err := plugin.NewPluginService(logger, pluginGetter, upstreamIdentifierFactory, evaluatorFactory)
		assert.ErrorContains(t, err, "evaluatorFactory is nil")
	})
	t.Run("should return plugin service", func(t *testing.T) {
		logger := log.NewNoop()
		pluginGetter := new(PluginGetter)
		defer pluginGetter.AssertExpectations(t)
		upstreamIdentifierFactory := NewUpstreamIdentifierFactory(t)
		evaluatorFactory := new(EvaluatorFactory)
		defer evaluatorFactory.AssertExpectations(t)

		pluginService, err := plugin.NewPluginService(logger, pluginGetter, upstreamIdentifierFactory, evaluatorFactory)
		assert.NoError(t, err)
		assert.NotNil(t, pluginService)
	})
}

func TestInfo(t *testing.T) {
	logger := log.NewNoop()
	ctx := context.Background()
	taskName := "bq2bqtest"
	pluginYamlTest, err := yaml.NewPluginSpec("./yaml/tests/sample_plugin_with_parser.yaml")
	assert.NoError(t, err)
	pluginTest := &p.Plugin{
		YamlMod: pluginYamlTest,
	}
	t.Run("returns error when no plugin", func(t *testing.T) {
		pluginGetter := new(PluginGetter)
		defer pluginGetter.AssertExpectations(t)
		upstreamIdentifierFactory := NewUpstreamIdentifierFactory(t)
		evaluatorFactory := new(EvaluatorFactory)
		defer evaluatorFactory.AssertExpectations(t)

		pluginGetter.On("GetByName", mock.Anything).Return(nil, fmt.Errorf("some error"))
		pluginService, err := plugin.NewPluginService(logger, pluginGetter, upstreamIdentifierFactory, evaluatorFactory)
		assert.NoError(t, err)
		assert.NotNil(t, pluginService)

		result, err := pluginService.Info(ctx, taskName)
		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Equal(t, "some error", err.Error())
	})
	t.Run("returns error when yaml mod not supported", func(t *testing.T) {
		pluginGetter := new(PluginGetter)
		defer pluginGetter.AssertExpectations(t)
		upstreamIdentifierFactory := NewUpstreamIdentifierFactory(t)
		evaluatorFactory := new(EvaluatorFactory)
		defer evaluatorFactory.AssertExpectations(t)

		pluginWithoutYaml := &p.Plugin{}
		pluginGetter.On("GetByName", mock.Anything).Return(pluginWithoutYaml, nil)
		pluginService, err := plugin.NewPluginService(logger, pluginGetter, upstreamIdentifierFactory, evaluatorFactory)
		assert.NoError(t, err)
		assert.NotNil(t, pluginService)

		result, err := pluginService.Info(ctx, taskName)
		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Equal(t, "yaml mod not exist", err.Error())
	})
	t.Run("returns plugin info", func(t *testing.T) {
		pluginGetter := new(PluginGetter)
		defer pluginGetter.AssertExpectations(t)
		upstreamIdentifierFactory := NewUpstreamIdentifierFactory(t)
		evaluatorFactory := new(EvaluatorFactory)
		defer evaluatorFactory.AssertExpectations(t)

		pluginGetter.On("GetByName", mock.Anything).Return(pluginTest, nil)
		pluginService, err := plugin.NewPluginService(logger, pluginGetter, upstreamIdentifierFactory, evaluatorFactory)
		assert.NoError(t, err)
		assert.NotNil(t, pluginService)

		result, err := pluginService.Info(ctx, taskName)
		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, "docker.io/goto/optimus-task-bq2bq-executor:latest", result.Image)
	})
}

func TestIdentifyUpstreams(t *testing.T) {
	logger := log.NewNoop()
	ctx := context.Background()
	taskName := "bq2bqtest"
	config := map[string]string{
		"BQ_SERVICE_ACCOUNT": "service_account_value",
	}
	assets := map[string]string{
		"query.sql": "select 1;",
	}
	pluginYamlTest, err := yaml.NewPluginSpec("./yaml/tests/sample_plugin_with_parser.yaml")
	assert.NoError(t, err)
	pluginTest := &p.Plugin{
		YamlMod: pluginYamlTest,
	}
	pluginYamlTestWithSelector, err := yaml.NewPluginSpec("./yaml/tests/sample_plugin_with_parser_and_yamlpath_selector.yaml")
	assert.NoError(t, err)
	pluginTestWithSelector := &p.Plugin{
		YamlMod: pluginYamlTestWithSelector,
	}

	urn1, err := lib.ParseURN("bigquery://proj:datas.table1")
	assert.NoError(t, err)
	urn2, err := lib.ParseURN("bigquery://proj:datas.table2")
	assert.NoError(t, err)

	t.Run("return error when plugin is not exist on pluginGetter", func(t *testing.T) {
		pluginGetter := new(PluginGetter)
		defer pluginGetter.AssertExpectations(t)
		upstreamIdentifierFactory := NewUpstreamIdentifierFactory(t)
		evaluatorFactory := new(EvaluatorFactory)
		defer evaluatorFactory.AssertExpectations(t)

		pluginGetter.On("GetByName", mock.Anything).Return(nil, fmt.Errorf("some error"))
		pluginService, err := plugin.NewPluginService(logger, pluginGetter, upstreamIdentifierFactory, evaluatorFactory)
		assert.NoError(t, err)
		assert.NotNil(t, pluginService)

		resourceURNs, err := pluginService.IdentifyUpstreams(ctx, taskName, config, assets)
		assert.Error(t, err)
		assert.Nil(t, resourceURNs)
	})
	t.Run("return empty resource urn if plugin doesn't have parser", func(t *testing.T) {
		pluginGetter := new(PluginGetter)
		defer pluginGetter.AssertExpectations(t)
		upstreamIdentifierFactory := NewUpstreamIdentifierFactory(t)
		evaluatorFactory := new(EvaluatorFactory)
		defer evaluatorFactory.AssertExpectations(t)
		evaluator := new(Evaluator)
		defer evaluator.AssertExpectations(t)

		pluginYamlWithoutParserTest, err := yaml.NewPluginSpec("./yaml/tests/sample_plugin.yaml")
		assert.NoError(t, err)
		pluginWithoutParserTest := &p.Plugin{
			YamlMod: pluginYamlWithoutParserTest,
		}

		pluginGetter.On("GetByName", mock.Anything).Return(pluginWithoutParserTest, nil)
		pluginService, err := plugin.NewPluginService(logger, pluginGetter, upstreamIdentifierFactory, evaluatorFactory)
		assert.NoError(t, err)
		assert.NotNil(t, pluginService)

		configEmpty := map[string]string{}
		resourceURNs, err := pluginService.IdentifyUpstreams(ctx, taskName, configEmpty, assets)
		assert.NoError(t, err)
		assert.Len(t, resourceURNs, 0)
	})
	t.Run("return error when evaluator factory couldn't return file evaluator", func(t *testing.T) {
		pluginGetter := new(PluginGetter)
		defer pluginGetter.AssertExpectations(t)
		upstreamIdentifierFactory := NewUpstreamIdentifierFactory(t)
		evaluatorFactory := new(EvaluatorFactory)
		defer evaluatorFactory.AssertExpectations(t)

		pluginGetter.On("GetByName", mock.Anything).Return(pluginTest, nil)
		evaluatorFactory.On("GetFileEvaluator", mock.Anything).Return(nil, errors.New("some error"))
		pluginService, err := plugin.NewPluginService(logger, pluginGetter, upstreamIdentifierFactory, evaluatorFactory)
		assert.NoError(t, err)
		assert.NotNil(t, pluginService)

		resourceURNs, err := pluginService.IdentifyUpstreams(ctx, taskName, config, assets)
		assert.Error(t, err)
		assert.Nil(t, resourceURNs)
	})
	t.Run("return error when evaluator factory couldn't return specilized evaluator", func(t *testing.T) {
		pluginGetter := new(PluginGetter)
		defer pluginGetter.AssertExpectations(t)
		upstreamIdentifierFactory := NewUpstreamIdentifierFactory(t)
		evaluatorFactory := new(EvaluatorFactory)
		defer evaluatorFactory.AssertExpectations(t)

		pluginGetter.On("GetByName", mock.Anything).Return(pluginTestWithSelector, nil)
		evaluatorFactory.On("GetYamlPathEvaluator", mock.Anything, "$.query").Return(nil, errors.New("some error"))
		pluginService, err := plugin.NewPluginService(logger, pluginGetter, upstreamIdentifierFactory, evaluatorFactory)
		assert.NoError(t, err)
		assert.NotNil(t, pluginService)

		resourceURNs, err := pluginService.IdentifyUpstreams(ctx, taskName, config, assets)
		assert.Error(t, err)
		assert.Nil(t, resourceURNs)
	})
	t.Run("return error when evaluator factory couldn't return evaluator due to invalid filepath type", func(t *testing.T) {
		pluginGetter := new(PluginGetter)
		defer pluginGetter.AssertExpectations(t)
		upstreamIdentifierFactory := NewUpstreamIdentifierFactory(t)
		evaluatorFactory := new(EvaluatorFactory)
		defer evaluatorFactory.AssertExpectations(t)

		pluginTestWithWrongFilePath := *pluginTestWithSelector // copy by value
		pluginTestWithWrongFilePath.Info().AssetParsers[p.BQParser][0].FilePath = "wrong_extension.yyx"
		pluginGetter.On("GetByName", mock.Anything).Return(&pluginTestWithWrongFilePath, nil)
		pluginService, err := plugin.NewPluginService(logger, pluginGetter, upstreamIdentifierFactory, evaluatorFactory)
		assert.NoError(t, err)
		assert.NotNil(t, pluginService)

		resourceURNs, err := pluginService.IdentifyUpstreams(ctx, taskName, config, assets)
		assert.Error(t, err)
		assert.ErrorContains(t, err, "evaluator for filepath wrong_extension.yyx is not supported")
		assert.Nil(t, resourceURNs)
	})
	t.Run("return error when bq2bq service account config is not provided", func(t *testing.T) { // will remove once all plugin is supported
		pluginGetter := new(PluginGetter)
		defer pluginGetter.AssertExpectations(t)
		upstreamIdentifierFactory := NewUpstreamIdentifierFactory(t)
		evaluatorFactory := new(EvaluatorFactory)
		defer evaluatorFactory.AssertExpectations(t)
		evaluator := new(Evaluator)
		defer evaluator.AssertExpectations(t)

		pluginGetter.On("GetByName", mock.Anything).Return(pluginTest, nil)
		evaluatorFactory.On("GetFileEvaluator", mock.Anything).Return(evaluator, nil)
		pluginService, err := plugin.NewPluginService(logger, pluginGetter, upstreamIdentifierFactory, evaluatorFactory)
		assert.NoError(t, err)
		assert.NotNil(t, pluginService)

		configEmpty := map[string]string{}
		resourceURNs, err := pluginService.IdentifyUpstreams(ctx, taskName, configEmpty, assets)
		assert.ErrorContains(t, err, "secret BQ_SERVICE_ACCOUNT required to generate upstream is not found")
		assert.Nil(t, resourceURNs)
	})
	t.Run("return error when upstream generator can't be created", func(t *testing.T) {
		pluginGetter := new(PluginGetter)
		defer pluginGetter.AssertExpectations(t)
		upstreamIdentifierFactory := NewUpstreamIdentifierFactory(t)
		evaluatorFactory := new(EvaluatorFactory)
		defer evaluatorFactory.AssertExpectations(t)
		evaluator := new(Evaluator)
		defer evaluator.AssertExpectations(t)

		pluginGetter.On("GetByName", mock.Anything).Return(pluginTest, nil)
		evaluatorFactory.On("GetFileEvaluator", mock.Anything).Return(evaluator, nil)
		upstreamIdentifierFactory.On("GetBQUpstreamIdentifier", ctx, mock.Anything, evaluator).Return(nil, errors.New("some error"))
		pluginService, err := plugin.NewPluginService(logger, pluginGetter, upstreamIdentifierFactory, evaluatorFactory)
		assert.NoError(t, err)
		assert.NotNil(t, pluginService)

		resourceURNs, err := pluginService.IdentifyUpstreams(ctx, taskName, config, assets)
		assert.Error(t, err)
		assert.Nil(t, resourceURNs)
	})
	t.Run("should success when no error encountered", func(t *testing.T) {
		pluginGetter := new(PluginGetter)
		defer pluginGetter.AssertExpectations(t)
		upstreamIdentifierFactory := NewUpstreamIdentifierFactory(t)
		evaluatorFactory := new(EvaluatorFactory)
		defer evaluatorFactory.AssertExpectations(t)
		evaluator := new(Evaluator)
		defer evaluator.AssertExpectations(t)
		upstreamIdentifier := NewUpstreamIdentifier(t)

		pluginGetter.On("GetByName", mock.Anything).Return(pluginTest, nil)
		evaluatorFactory.On("GetFileEvaluator", mock.Anything).Return(evaluator, nil)
		upstreamIdentifierFactory.On("GetBQUpstreamIdentifier", ctx, mock.Anything, evaluator).Return(upstreamIdentifier, nil)
		upstreamIdentifier.On("IdentifyResources", ctx, assets).Return([]lib.URN{urn1}, nil)
		pluginService, err := plugin.NewPluginService(logger, pluginGetter, upstreamIdentifierFactory, evaluatorFactory)
		assert.NoError(t, err)
		assert.NotNil(t, pluginService)

		resourceURNs, err := pluginService.IdentifyUpstreams(ctx, taskName, config, assets)
		assert.NoError(t, err)
		assert.NotEmpty(t, resourceURNs)
		assert.Len(t, resourceURNs, 1)
	})
	t.Run("should generate clean dependencies without destination in it", func(t *testing.T) {
		pluginGetter := new(PluginGetter)
		defer pluginGetter.AssertExpectations(t)
		upstreamIdentifierFactory := NewUpstreamIdentifierFactory(t)
		evaluatorFactory := new(EvaluatorFactory)
		defer evaluatorFactory.AssertExpectations(t)
		evaluator := new(Evaluator)
		defer evaluator.AssertExpectations(t)
		upstreamIdentifier := NewUpstreamIdentifier(t)

		pluginYamlTestWithDestinationTemplate, err := yaml.NewPluginSpec("./yaml/tests/sample_plugin_with_parser_and_destination_template.yaml")
		assert.NoError(t, err)
		pluginTestWithDestinationTemplate := &p.Plugin{
			YamlMod: pluginYamlTestWithDestinationTemplate,
		}
		pluginGetter.On("GetByName", mock.Anything).Return(pluginTestWithDestinationTemplate, nil)
		evaluatorFactory.On("GetFileEvaluator", mock.Anything).Return(evaluator, nil)
		upstreamIdentifierFactory.On("GetBQUpstreamIdentifier", ctx, mock.Anything, evaluator).Return(upstreamIdentifier, nil)
		upstreamIdentifier.On("IdentifyResources", ctx, assets).Return([]lib.URN{urn1, urn2}, nil)
		pluginService, err := plugin.NewPluginService(logger, pluginGetter, upstreamIdentifierFactory, evaluatorFactory)
		assert.NoError(t, err)
		assert.NotNil(t, pluginService)

		configTask := map[string]string{}
		configTask["BQ_SERVICE_ACCOUNT"] = "service_account_value"
		configTask["PROJECT"] = "proj"
		configTask["DATASET"] = "datas"
		configTask["TABLE"] = "table2"
		resourceURNs, err := pluginService.IdentifyUpstreams(ctx, taskName, configTask, assets)
		assert.NoError(t, err)
		assert.NotEmpty(t, resourceURNs)
		assert.Len(t, resourceURNs, 1)
	})
}

func TestConstructDestinationURN(t *testing.T) {
	logger := log.NewNoop()
	ctx := context.Background()
	taskName := "bq2bqtest"
	config := map[string]string{
		"BQ_SERVICE_ACCOUNT": "service_account_value",
		"PROJECT":            "project1",
		"DATASET":            "dataset1",
		"TABLE":              "table1",
	}
	pluginYamlTest, err := yaml.NewPluginSpec("./yaml/tests/sample_plugin_with_destination_template.yaml")
	assert.NoError(t, err)
	pluginTest := &p.Plugin{
		YamlMod: pluginYamlTest,
	}
	t.Run("returns error if unable to find the plugin", func(t *testing.T) {
		pluginGetter := new(PluginGetter)
		defer pluginGetter.AssertExpectations(t)
		upstreamIdentifierFactory := NewUpstreamIdentifierFactory(t)
		evaluatorFactory := new(EvaluatorFactory)
		defer evaluatorFactory.AssertExpectations(t)

		pluginGetter.On("GetByName", mock.Anything).Return(nil, fmt.Errorf("some error"))
		pluginService, err := plugin.NewPluginService(logger, pluginGetter, upstreamIdentifierFactory, evaluatorFactory)
		assert.NoError(t, err)
		assert.NotNil(t, pluginService)

		result, err := pluginService.ConstructDestinationURN(ctx, taskName, config)
		assert.Error(t, err)
		assert.Empty(t, result)
	})
	t.Run("should return empty destination if the plugin doesn't contain destination template", func(t *testing.T) {
		pluginGetter := new(PluginGetter)
		defer pluginGetter.AssertExpectations(t)
		upstreamIdentifierFactory := NewUpstreamIdentifierFactory(t)
		evaluatorFactory := new(EvaluatorFactory)
		defer evaluatorFactory.AssertExpectations(t)

		pluginYamlTest, err := yaml.NewPluginSpec("./yaml/tests/sample_plugin_with_parser.yaml")
		assert.NoError(t, err)
		pluginTestWithoutDestinationTemplate := &p.Plugin{
			YamlMod: pluginYamlTest,
		}
		pluginGetter.On("GetByName", mock.Anything).Return(pluginTestWithoutDestinationTemplate, nil)
		pluginService, err := plugin.NewPluginService(logger, pluginGetter, upstreamIdentifierFactory, evaluatorFactory)
		assert.NoError(t, err)
		assert.NotNil(t, pluginService)

		result, err := pluginService.ConstructDestinationURN(ctx, taskName, config)
		assert.NoError(t, err)
		assert.Empty(t, result)
	})
	t.Run("returns error if template is not proper", func(t *testing.T) {
		pluginGetter := new(PluginGetter)
		defer pluginGetter.AssertExpectations(t)
		upstreamIdentifierFactory := NewUpstreamIdentifierFactory(t)
		evaluatorFactory := new(EvaluatorFactory)
		defer evaluatorFactory.AssertExpectations(t)
		pluginYamlTest, err := yaml.NewPluginSpec("./yaml/tests/sample_plugin_with_unproper_destination_template.yaml")
		assert.NoError(t, err)
		pluginTestUnproperTemplate := &p.Plugin{
			YamlMod: pluginYamlTest,
		}
		pluginGetter.On("GetByName", mock.Anything).Return(pluginTestUnproperTemplate, nil)
		pluginService, err := plugin.NewPluginService(logger, pluginGetter, upstreamIdentifierFactory, evaluatorFactory)
		assert.NoError(t, err)
		assert.NotNil(t, pluginService)

		result, err := pluginService.ConstructDestinationURN(ctx, taskName, config)
		assert.Error(t, err)
		assert.Empty(t, result)
	})
	t.Run("should properly generate a destination provided correct config inputs", func(t *testing.T) {
		pluginGetter := new(PluginGetter)
		defer pluginGetter.AssertExpectations(t)
		upstreamIdentifierFactory := NewUpstreamIdentifierFactory(t)
		evaluatorFactory := new(EvaluatorFactory)
		defer evaluatorFactory.AssertExpectations(t)

		pluginGetter.On("GetByName", mock.Anything).Return(pluginTest, nil)
		pluginService, err := plugin.NewPluginService(logger, pluginGetter, upstreamIdentifierFactory, evaluatorFactory)
		assert.NoError(t, err)
		assert.NotNil(t, pluginService)

		expectedURN, err := lib.ParseURN("bigquery://project1:dataset1.table1")
		assert.NoError(t, err)

		result, err := pluginService.ConstructDestinationURN(ctx, taskName, config)
		assert.NoError(t, err)
		assert.NotEmpty(t, result)
		assert.Equal(t, expectedURN, result)
	})
}

type PluginGetter struct {
	mock.Mock
}

// GetByName provides a mock function with given fields: name
func (_m *PluginGetter) GetByName(name string) (*p.Plugin, error) {
	ret := _m.Called(name)

	var r0 *p.Plugin
	var r1 error
	if rf, ok := ret.Get(0).(func(string) (*p.Plugin, error)); ok {
		return rf(name)
	}
	if rf, ok := ret.Get(0).(func(string) *p.Plugin); ok {
		r0 = rf(name)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*p.Plugin)
		}
	}

	if rf, ok := ret.Get(1).(func(string) error); ok {
		r1 = rf(name)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

type YamlMod struct {
	mock.Mock
}

// PluginInfo provides a mock function with given fields:
func (_m *YamlMod) PluginInfo() *p.Info {
	ret := _m.Called()

	var r0 *p.Info
	if rf, ok := ret.Get(0).(func() *p.Info); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*p.Info)
		}
	}

	return r0
}

// UpstreamIdentifierFactory is an autogenerated mock type for the UpstreamIdentifierFactory type
type UpstreamIdentifierFactory struct {
	mock.Mock
}

// GetBQUpstreamIdentifier provides a mock function with given fields: ctx, svcAcc, evaluators
func (_m *UpstreamIdentifierFactory) GetBQUpstreamIdentifier(ctx context.Context, svcAcc string, evaluators ...evaluator.Evaluator) (upstreamidentifier.UpstreamIdentifier, error) {
	_va := make([]interface{}, len(evaluators))
	for _i := range evaluators {
		_va[_i] = evaluators[_i]
	}
	var _ca []interface{}
	_ca = append(_ca, ctx, svcAcc)
	_ca = append(_ca, _va...)
	ret := _m.Called(_ca...)

	if len(ret) == 0 {
		panic("no return value specified for GetBQUpstreamIdentifier")
	}

	var r0 upstreamidentifier.UpstreamIdentifier
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context, string, ...evaluator.Evaluator) (upstreamidentifier.UpstreamIdentifier, error)); ok {
		return rf(ctx, svcAcc, evaluators...)
	}
	if rf, ok := ret.Get(0).(func(context.Context, string, ...evaluator.Evaluator) upstreamidentifier.UpstreamIdentifier); ok {
		r0 = rf(ctx, svcAcc, evaluators...)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(upstreamidentifier.UpstreamIdentifier)
		}
	}

	if rf, ok := ret.Get(1).(func(context.Context, string, ...evaluator.Evaluator) error); ok {
		r1 = rf(ctx, svcAcc, evaluators...)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// NewUpstreamIdentifierFactory creates a new instance of UpstreamIdentifierFactory. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewUpstreamIdentifierFactory(t interface {
	mock.TestingT
	Cleanup(func())
},
) *UpstreamIdentifierFactory {
	mock := &UpstreamIdentifierFactory{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}

// EvaluatorFactory is an autogenerated mock type for the EvaluatorFactory type
type EvaluatorFactory struct {
	mock.Mock
}

// GetFileEvaluator provides a mock function with given fields: filepath
func (_m *EvaluatorFactory) GetFileEvaluator(filepath string) (evaluator.Evaluator, error) {
	ret := _m.Called(filepath)

	var r0 evaluator.Evaluator
	var r1 error
	if rf, ok := ret.Get(0).(func(string) (evaluator.Evaluator, error)); ok {
		return rf(filepath)
	}
	if rf, ok := ret.Get(0).(func(string) evaluator.Evaluator); ok {
		r0 = rf(filepath)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(evaluator.Evaluator)
		}
	}

	if rf, ok := ret.Get(1).(func(string) error); ok {
		r1 = rf(filepath)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// GetYamlPathEvaluator provides a mock function with given fields: filepath, selector
func (_m *EvaluatorFactory) GetYamlPathEvaluator(filepath, selector string) (evaluator.Evaluator, error) {
	ret := _m.Called(filepath, selector)

	var r0 evaluator.Evaluator
	var r1 error
	if rf, ok := ret.Get(0).(func(string, string) (evaluator.Evaluator, error)); ok {
		return rf(filepath, selector)
	}
	if rf, ok := ret.Get(0).(func(string, string) evaluator.Evaluator); ok {
		r0 = rf(filepath, selector)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(evaluator.Evaluator)
		}
	}

	if rf, ok := ret.Get(1).(func(string, string) error); ok {
		r1 = rf(filepath, selector)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// Evaluator is an autogenerated mock type for the Evaluator type
type Evaluator struct {
	mock.Mock
}

// Evaluate provides a mock function with given fields: assets
func (_m *Evaluator) Evaluate(assets map[string]string) string {
	ret := _m.Called(assets)

	var r0 string
	if rf, ok := ret.Get(0).(func(map[string]string) string); ok {
		r0 = rf(assets)
	} else {
		r0 = ret.Get(0).(string)
	}

	return r0
}

// UpstreamIdentifier is an autogenerated mock type for the UpstreamIdentifier type
type UpstreamIdentifier struct {
	mock.Mock
}

// IdentifyResources provides a mock function with given fields: ctx, assets
func (_m *UpstreamIdentifier) IdentifyResources(ctx context.Context, assets map[string]string) ([]lib.URN, error) {
	ret := _m.Called(ctx, assets)

	if len(ret) == 0 {
		panic("no return value specified for IdentifyResources")
	}

	var r0 []lib.URN
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context, map[string]string) ([]lib.URN, error)); ok {
		return rf(ctx, assets)
	}
	if rf, ok := ret.Get(0).(func(context.Context, map[string]string) []lib.URN); ok {
		r0 = rf(ctx, assets)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]lib.URN)
		}
	}

	if rf, ok := ret.Get(1).(func(context.Context, map[string]string) error); ok {
		r1 = rf(ctx, assets)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// NewUpstreamIdentifier creates a new instance of UpstreamIdentifier. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewUpstreamIdentifier(t interface {
	mock.TestingT
	Cleanup(func())
},
) *UpstreamIdentifier {
	mock := &UpstreamIdentifier{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
