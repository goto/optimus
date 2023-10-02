package plugin_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/goto/salt/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/goto/optimus/plugin"
	"github.com/goto/optimus/plugin/upstream_generator"
	"github.com/goto/optimus/plugin/yaml"
	p "github.com/goto/optimus/sdk/plugin"
)

func TestNewPluginService(t *testing.T) {
	t.Run("should return error when logger is nil", func(t *testing.T) {
		var logger log.Logger = nil
		pluginGetter := new(PluginGetter)
		defer pluginGetter.AssertExpectations(t)
		upstreamGeneratorFactory := new(UpstreamGeneratorFactory)
		defer upstreamGeneratorFactory.AssertExpectations(t)
		evaluatorFactory := new(EvaluatorFactory)
		defer evaluatorFactory.AssertExpectations(t)

		_, err := plugin.NewPluginService(logger, pluginGetter, upstreamGeneratorFactory, evaluatorFactory)
		assert.ErrorContains(t, err, "logger is nil")
	})
	t.Run("should return error when pluginGetter is nil", func(t *testing.T) {
		logger := log.NewNoop()
		var pluginGetter plugin.PluginGetter = nil
		upstreamGeneratorFactory := new(UpstreamGeneratorFactory)
		defer upstreamGeneratorFactory.AssertExpectations(t)
		evaluatorFactory := new(EvaluatorFactory)
		defer evaluatorFactory.AssertExpectations(t)

		_, err := plugin.NewPluginService(logger, pluginGetter, upstreamGeneratorFactory, evaluatorFactory)
		assert.ErrorContains(t, err, "pluginGetter is nil")
	})
	t.Run("should return error when upstreamGeneratorFactory is nil", func(t *testing.T) {
		logger := log.NewNoop()
		pluginGetter := new(PluginGetter)
		defer pluginGetter.AssertExpectations(t)
		var upstreamGeneratorFactory plugin.UpstreamGeneratorFactory = nil
		evaluatorFactory := new(EvaluatorFactory)
		defer evaluatorFactory.AssertExpectations(t)

		_, err := plugin.NewPluginService(logger, pluginGetter, upstreamGeneratorFactory, evaluatorFactory)
		assert.ErrorContains(t, err, "upstreamGeneratorFactory is nil")
	})
	t.Run("should return error when evaluatorFactory is nil", func(t *testing.T) {
		logger := log.NewNoop()
		pluginGetter := new(PluginGetter)
		defer pluginGetter.AssertExpectations(t)
		upstreamGeneratorFactory := new(UpstreamGeneratorFactory)
		defer upstreamGeneratorFactory.AssertExpectations(t)
		var evaluatorFactory plugin.EvaluatorFactory = nil

		_, err := plugin.NewPluginService(logger, pluginGetter, upstreamGeneratorFactory, evaluatorFactory)
		assert.ErrorContains(t, err, "evaluatorFactory is nil")
	})
	t.Run("should return plugin service", func(t *testing.T) {
		logger := log.NewNoop()
		pluginGetter := new(PluginGetter)
		defer pluginGetter.AssertExpectations(t)
		upstreamGeneratorFactory := new(UpstreamGeneratorFactory)
		defer upstreamGeneratorFactory.AssertExpectations(t)
		evaluatorFactory := new(EvaluatorFactory)
		defer evaluatorFactory.AssertExpectations(t)

		pluginService, err := plugin.NewPluginService(logger, pluginGetter, upstreamGeneratorFactory, evaluatorFactory)
		assert.NoError(t, err)
		assert.NotNil(t, pluginService)
	})
}

func TestGenerateUpstreams(t *testing.T) {
	logger := log.NewNoop()
	ctx := context.Background()
	taskName := "bq2bq"
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

	t.Run("return error when plugin is not exist on pluginGetter", func(t *testing.T) {
		pluginGetter := new(PluginGetter)
		defer pluginGetter.AssertExpectations(t)
		upstreamGeneratorFactory := new(UpstreamGeneratorFactory)
		defer upstreamGeneratorFactory.AssertExpectations(t)
		evaluatorFactory := new(EvaluatorFactory)
		defer evaluatorFactory.AssertExpectations(t)

		pluginGetter.On("GetByName", mock.Anything).Return(nil, fmt.Errorf("some error"))
		pluginService, err := plugin.NewPluginService(logger, pluginGetter, upstreamGeneratorFactory, evaluatorFactory)
		assert.NoError(t, err)
		assert.NotNil(t, pluginService)

		resourceURNs, err := pluginService.GenerateUpstreams(ctx, taskName, config, assets)
		assert.Error(t, err)
		assert.Nil(t, resourceURNs)
	})
	t.Run("return empty resource urn if plugin doesn't have parser", func(t *testing.T) {
		pluginGetter := new(PluginGetter)
		defer pluginGetter.AssertExpectations(t)
		upstreamGeneratorFactory := new(UpstreamGeneratorFactory)
		defer upstreamGeneratorFactory.AssertExpectations(t)
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
		pluginService, err := plugin.NewPluginService(logger, pluginGetter, upstreamGeneratorFactory, evaluatorFactory)
		assert.NoError(t, err)
		assert.NotNil(t, pluginService)

		configEmpty := map[string]string{}
		resourceURNs, err := pluginService.GenerateUpstreams(ctx, taskName, configEmpty, assets)
		assert.NoError(t, err)
		assert.Len(t, resourceURNs, 0)
	})
	t.Run("return error when evaluator couldn't return file evaluator", func(t *testing.T) {
		pluginGetter := new(PluginGetter)
		defer pluginGetter.AssertExpectations(t)
		upstreamGeneratorFactory := new(UpstreamGeneratorFactory)
		defer upstreamGeneratorFactory.AssertExpectations(t)
		evaluatorFactory := new(EvaluatorFactory)
		defer evaluatorFactory.AssertExpectations(t)

		pluginGetter.On("GetByName", mock.Anything).Return(pluginTest, nil)
		evaluatorFactory.On("GetFileEvaluator", mock.Anything).Return(nil, errors.New("some error"))
		pluginService, err := plugin.NewPluginService(logger, pluginGetter, upstreamGeneratorFactory, evaluatorFactory)
		assert.NoError(t, err)
		assert.NotNil(t, pluginService)

		resourceURNs, err := pluginService.GenerateUpstreams(ctx, taskName, config, assets)
		assert.Error(t, err)
		assert.Nil(t, resourceURNs)
	})
	t.Run("return error when bq2bq service account config is not provided", func(t *testing.T) { // will remove once all plugin is supported
		pluginGetter := new(PluginGetter)
		defer pluginGetter.AssertExpectations(t)
		upstreamGeneratorFactory := new(UpstreamGeneratorFactory)
		defer upstreamGeneratorFactory.AssertExpectations(t)
		evaluatorFactory := new(EvaluatorFactory)
		defer evaluatorFactory.AssertExpectations(t)
		evaluator := new(Evaluator)
		defer evaluator.AssertExpectations(t)

		pluginGetter.On("GetByName", mock.Anything).Return(pluginTest, nil)
		evaluatorFactory.On("GetFileEvaluator", mock.Anything).Return(evaluator, nil)
		pluginService, err := plugin.NewPluginService(logger, pluginGetter, upstreamGeneratorFactory, evaluatorFactory)
		assert.NoError(t, err)
		assert.NotNil(t, pluginService)

		configEmpty := map[string]string{}
		resourceURNs, err := pluginService.GenerateUpstreams(ctx, taskName, configEmpty, assets)
		assert.ErrorContains(t, err, "secret BQ_SERVICE_ACCOUNT required to generate upstream is not found")
		assert.Nil(t, resourceURNs)
	})
	t.Run("return error when upstream generator can't be created", func(t *testing.T) {
		pluginGetter := new(PluginGetter)
		defer pluginGetter.AssertExpectations(t)
		upstreamGeneratorFactory := new(UpstreamGeneratorFactory)
		defer upstreamGeneratorFactory.AssertExpectations(t)
		evaluatorFactory := new(EvaluatorFactory)
		defer evaluatorFactory.AssertExpectations(t)
		evaluator := new(Evaluator)
		defer evaluator.AssertExpectations(t)

		pluginGetter.On("GetByName", mock.Anything).Return(pluginTest, nil)
		evaluatorFactory.On("GetFileEvaluator", mock.Anything).Return(evaluator, nil)
		upstreamGeneratorFactory.On("GetBQUpstreamGenerator", ctx, evaluator, mock.Anything).Return(nil, errors.New("some error"))
		pluginService, err := plugin.NewPluginService(logger, pluginGetter, upstreamGeneratorFactory, evaluatorFactory)
		assert.NoError(t, err)
		assert.NotNil(t, pluginService)

		resourceURNs, err := pluginService.GenerateUpstreams(ctx, taskName, config, assets)
		assert.Error(t, err)
		assert.Nil(t, resourceURNs)
	})
	t.Run("should success when no error encountered", func(t *testing.T) {
		pluginGetter := new(PluginGetter)
		defer pluginGetter.AssertExpectations(t)
		upstreamGeneratorFactory := new(UpstreamGeneratorFactory)
		defer upstreamGeneratorFactory.AssertExpectations(t)
		evaluatorFactory := new(EvaluatorFactory)
		defer evaluatorFactory.AssertExpectations(t)
		evaluator := new(Evaluator)
		defer evaluator.AssertExpectations(t)
		upstreamGenerator := new(UpstreamGenerator)
		defer upstreamGenerator.AssertExpectations(t)

		pluginGetter.On("GetByName", mock.Anything).Return(pluginTest, nil)
		evaluatorFactory.On("GetFileEvaluator", mock.Anything).Return(evaluator, nil)
		upstreamGeneratorFactory.On("GetBQUpstreamGenerator", ctx, evaluator, mock.Anything).Return(upstreamGenerator, nil)
		upstreamGenerator.On("GenerateResources", ctx, assets).Return([]string{"bigquery://proj:datas:tabl"}, nil)
		pluginService, err := plugin.NewPluginService(logger, pluginGetter, upstreamGeneratorFactory, evaluatorFactory)
		assert.NoError(t, err)
		assert.NotNil(t, pluginService)

		resourceURNs, err := pluginService.GenerateUpstreams(ctx, taskName, config, assets)
		assert.NoError(t, err)
		assert.NotEmpty(t, resourceURNs)
		assert.Len(t, resourceURNs, 1)
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

// UpstreamGeneratorFactory is an autogenerated mock type for the UpstreamGeneratorFactory type
type UpstreamGeneratorFactory struct {
	mock.Mock
}

// GetBQUpstreamGenerator provides a mock function with given fields: ctx, evaluator, svcAcc
func (_m *UpstreamGeneratorFactory) GetBQUpstreamGenerator(ctx context.Context, evaluator plugin.Evaluator, svcAcc string) (upstream_generator.UpstreamGenerator, error) {
	ret := _m.Called(ctx, evaluator, svcAcc)

	var r0 upstream_generator.UpstreamGenerator
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context, plugin.Evaluator, string) (upstream_generator.UpstreamGenerator, error)); ok {
		return rf(ctx, evaluator, svcAcc)
	}
	if rf, ok := ret.Get(0).(func(context.Context, plugin.Evaluator, string) upstream_generator.UpstreamGenerator); ok {
		r0 = rf(ctx, evaluator, svcAcc)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(upstream_generator.UpstreamGenerator)
		}
	}

	if rf, ok := ret.Get(1).(func(context.Context, plugin.Evaluator, string) error); ok {
		r1 = rf(ctx, evaluator, svcAcc)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// EvaluatorFactory is an autogenerated mock type for the EvaluatorFactory type
type EvaluatorFactory struct {
	mock.Mock
}

// GetFileEvaluator provides a mock function with given fields: filepath
func (_m *EvaluatorFactory) GetFileEvaluator(filepath string) (plugin.Evaluator, error) {
	ret := _m.Called(filepath)

	var r0 plugin.Evaluator
	var r1 error
	if rf, ok := ret.Get(0).(func(string) (plugin.Evaluator, error)); ok {
		return rf(filepath)
	}
	if rf, ok := ret.Get(0).(func(string) plugin.Evaluator); ok {
		r0 = rf(filepath)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(plugin.Evaluator)
		}
	}

	if rf, ok := ret.Get(1).(func(string) error); ok {
		r1 = rf(filepath)
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

// UpstreamGenerator is an autogenerated mock type for the UpstreamGenerator type
type UpstreamGenerator struct {
	mock.Mock
}

// GenerateResources provides a mock function with given fields: ctx, assets
func (_m *UpstreamGenerator) GenerateResources(ctx context.Context, assets map[string]string) ([]string, error) {
	ret := _m.Called(ctx, assets)

	var r0 []string
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context, map[string]string) ([]string, error)); ok {
		return rf(ctx, assets)
	}
	if rf, ok := ret.Get(0).(func(context.Context, map[string]string) []string); ok {
		r0 = rf(ctx, assets)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]string)
		}
	}

	if rf, ok := ret.Get(1).(func(context.Context, map[string]string) error); ok {
		r1 = rf(ctx, assets)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}
