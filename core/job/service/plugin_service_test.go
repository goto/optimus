package service_test

import (
	"context"
	"errors"
	"testing"

	"github.com/goto/salt/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/goto/optimus/core/job"
	"github.com/goto/optimus/core/job/service"
	"github.com/goto/optimus/ext/extractor"
	"github.com/goto/optimus/ext/store/bigquery"
	"github.com/goto/optimus/sdk/plugin"
	mockOpt "github.com/goto/optimus/sdk/plugin/mock"
)

func TestPluginService(t *testing.T) {
	ctx := context.Background()
	jobTaskConfig, err := job.ConfigFrom(map[string]string{
		"SECRET_TABLE_NAME": "{{.secret.table_name}}",
	})
	assert.NoError(t, err)
	jobTask := job.NewTask("bq2bq", jobTaskConfig)

	logger := log.NewLogrus()

	t.Run("Info", func(t *testing.T) {
		t.Run("returns error when no plugin", func(t *testing.T) {
			pluginRepo := new(mockPluginRepo)
			pluginRepo.On("GetByName", jobTask.Name().String()).Return(nil, errors.New("some error when fetch plugin"))
			defer pluginRepo.AssertExpectations(t)

			pluginService := service.NewJobPluginService(pluginRepo, nil, logger)
			result, err := pluginService.Info(ctx, jobTask.Name())
			assert.Error(t, err)
			assert.Nil(t, result)
			assert.Equal(t, "some error when fetch plugin", err.Error())
		})
		t.Run("returns error when yaml mod not supported", func(t *testing.T) {
			pluginRepo := new(mockPluginRepo)
			defer pluginRepo.AssertExpectations(t)

			yamlMod := new(mockOpt.YamlMod)
			defer yamlMod.AssertExpectations(t)

			newPlugin := &plugin.Plugin{}
			pluginRepo.On("GetByName", jobTask.Name().String()).Return(newPlugin, nil)

			pluginService := service.NewJobPluginService(pluginRepo, nil, logger)
			result, err := pluginService.Info(ctx, jobTask.Name())
			assert.Error(t, err)
			assert.Nil(t, result)
			assert.Equal(t, "yaml mod not found for plugin", err.Error())
		})
		t.Run("returns plugin info", func(t *testing.T) {
			pluginRepo := new(mockPluginRepo)
			defer pluginRepo.AssertExpectations(t)

			yamlMod := new(mockOpt.YamlMod)
			defer yamlMod.AssertExpectations(t)

			taskPlugin := &plugin.Plugin{YamlMod: yamlMod}

			pluginRepo.On("GetByName", jobTask.Name().String()).Return(taskPlugin, nil)
			yamlMod.On("PluginInfo").Return(&plugin.Info{
				Name:        jobTask.Name().String(),
				Description: "example",
				Image:       "http://to.repo",
			}, nil)
			defer yamlMod.AssertExpectations(t)

			pluginService := service.NewJobPluginService(pluginRepo, nil, logger)
			result, err := pluginService.Info(ctx, jobTask.Name())
			assert.NoError(t, err)
			assert.NotNil(t, result)
			assert.Equal(t, jobTask.Name().String(), result.Name)
			assert.Equal(t, "example", result.Description)
			assert.Equal(t, "http://to.repo", result.Image)
		})
	})

	t.Run("GenerateDestination", func(t *testing.T) {
		logger := log.NewNoop()
		t.Run("should properly generate a destination provided correct config inputs", func(t *testing.T) {
			pluginRepo := new(mockPluginRepo)
			defer pluginRepo.AssertExpectations(t)

			yamlMod := new(mockOpt.YamlMod)
			yamlMod.On("PluginInfo").Return(&plugin.Info{
				Name:        jobTask.Name().String(),
				Description: "example",
				Image:       "http://to.repo",
			}, nil)
			defer yamlMod.AssertExpectations(t)

			taskPlugin := &plugin.Plugin{YamlMod: yamlMod}
			pluginRepo.On("GetByName", jobTask.Name().String()).Return(taskPlugin, nil)

			configs := map[string]string{
				"PROJECT": "proj",
				"DATASET": "datas",
				"TABLE":   "tab",
			}
			destinationURN := job.ResourceURN("bigquery://proj:datas.tab")

			pluginService := service.NewJobPluginService(pluginRepo, nil, logger)
			result, err := pluginService.GenerateDestination(ctx, jobTask.Name(), configs)
			assert.Nil(t, err)
			assert.Equal(t, destinationURN, result)
		})
		t.Run("returns error if unable to find the plugin", func(t *testing.T) {
			pluginRepo := new(mockPluginRepo)
			defer pluginRepo.AssertExpectations(t)

			pluginRepo.On("GetByName", jobTask.Name().String()).Return(nil, errors.New("not found"))

			pluginService := service.NewJobPluginService(pluginRepo, nil, logger)
			result, err := pluginService.GenerateDestination(ctx, jobTask.Name(), nil)
			assert.ErrorContains(t, err, "not found")
			assert.Equal(t, "", result.String())
		})
		t.Run("returns error if generate destination failed", func(t *testing.T) {
			pluginRepo := new(mockPluginRepo)
			defer pluginRepo.AssertExpectations(t)

			yamlMod := new(mockOpt.YamlMod)
			yamlMod.On("PluginInfo").Return(&plugin.Info{
				Name:        jobTask.Name().String(),
				Description: "example",
				Image:       "http://to.repo",
			}, nil)
			defer yamlMod.AssertExpectations(t)

			configs := map[string]string{
				"PROJECT": "proj",
				"DATASET": "datas",
			}

			taskPlugin := &plugin.Plugin{YamlMod: yamlMod}
			pluginRepo.On("GetByName", jobTask.Name().String()).Return(taskPlugin, nil)

			pluginService := service.NewJobPluginService(pluginRepo, nil, logger)
			result, err := pluginService.GenerateDestination(ctx, jobTask.Name(), configs)
			assert.ErrorContains(t, err, "missing config key")
			assert.Equal(t, "", result.String())
		})
	})

	t.Run("GenerateDependencies", func(t *testing.T) {
		logger := log.NewNoop()
		t.Run("should return error when specific plugin is fail to fetch", func(t *testing.T) {
			svcAcc := "service_account"
			query := "Select * from proj.dataset.table1"
			destinationURN := job.ResourceURN("bigquery://proj:datas.tab")

			pluginRepo := new(mockPluginRepo)
			defer pluginRepo.AssertExpectations(t)

			pluginRepo.On("GetByName", jobTask.Name().String()).Return(nil, errors.New("fail"))

			pluginService := service.NewJobPluginService(pluginRepo, nil, logger)
			result, err := pluginService.GenerateDependencies(ctx, jobTask.Name(), svcAcc, query, destinationURN)
			assert.Error(t, err)
			assert.Nil(t, result)
		})
		t.Run("should return error when fail to create extractor", func(t *testing.T) {
			svcAcc := "service_account"
			query := "Select * from proj.dataset.table1"
			destinationURN := job.ResourceURN("bigquery://proj:datas.tab")

			pluginRepo := new(mockPluginRepo)
			defer pluginRepo.AssertExpectations(t)

			yamlMod := new(mockOpt.YamlMod)
			defer yamlMod.AssertExpectations(t)

			extractorFac := new(ExtractorFactory)
			defer extractorFac.AssertExpectations(t)

			extractorFac.On("New", ctx, svcAcc, mock.Anything).Return(nil, errors.New("error creating extractor"))

			yamlMod.On("PluginInfo").Return(&plugin.Info{
				Name:        jobTask.Name().String(),
				Description: "example",
				Image:       "http://to.repo",
			}, nil)
			taskPlugin := &plugin.Plugin{YamlMod: yamlMod}
			pluginRepo.On("GetByName", jobTask.Name().String()).Return(taskPlugin, nil)

			pluginService := service.NewJobPluginService(pluginRepo, extractorFac, logger)
			result, err := pluginService.GenerateDependencies(ctx, jobTask.Name(), svcAcc, query, destinationURN)
			assert.ErrorContains(t, err, "error creating extractor")
			assert.Nil(t, result)
		})
		t.Run("should return empty resources when extractor error", func(t *testing.T) {
			svcAcc := "service_account"
			query := "Select * from proj.dataset.table1"
			destinationURN := job.ResourceURN("bigquery://proj:datas.tab")

			pluginRepo := new(mockPluginRepo)
			defer pluginRepo.AssertExpectations(t)

			yamlMod := new(mockOpt.YamlMod)
			defer yamlMod.AssertExpectations(t)

			extractorFac := new(ExtractorFactory)
			defer extractorFac.AssertExpectations(t)

			var extractorFunc extractor.BQExtractorFunc = func(context.Context, []bigquery.ResourceURN) (map[bigquery.ResourceURN]string, error) {
				return nil, errors.New("error extract resource")
			}
			extractorFac.On("New", ctx, svcAcc, mock.Anything).Return(extractorFunc, nil)

			yamlMod.On("PluginInfo").Return(&plugin.Info{
				Name:        jobTask.Name().String(),
				Description: "example",
				Image:       "http://to.repo",
			}, nil)
			taskPlugin := &plugin.Plugin{YamlMod: yamlMod}
			pluginRepo.On("GetByName", jobTask.Name().String()).Return(taskPlugin, nil)

			pluginService := service.NewJobPluginService(pluginRepo, extractorFac, logger)
			result, err := pluginService.GenerateDependencies(ctx, jobTask.Name(), svcAcc, query, destinationURN)
			assert.NoError(t, err)
			assert.Empty(t, result)
		})
		t.Run("should generate dependencies for select statements", func(t *testing.T) {
			svcAcc := "service_account"
			query := "Select * from proj.dataset.table1"
			destinationURN := job.ResourceURN("bigquery://proj:datas.tab")
			expectedDeps := []job.ResourceURN{"bigquery://proj:dataset.table1"}

			pluginRepo := new(mockPluginRepo)
			defer pluginRepo.AssertExpectations(t)

			yamlMod := new(mockOpt.YamlMod)
			defer yamlMod.AssertExpectations(t)

			extractorFac := new(ExtractorFactory)
			defer extractorFac.AssertExpectations(t)

			table1BqResourceURN, _ := bigquery.NewResourceURN("proj", "dataset", "table1")
			var extractorFunc extractor.BQExtractorFunc = func(context.Context, []bigquery.ResourceURN) (map[bigquery.ResourceURN]string, error) {
				return map[bigquery.ResourceURN]string{
					table1BqResourceURN: "",
				}, nil
			}
			extractorFac.On("New", ctx, svcAcc, mock.Anything).Return(extractorFunc, nil)

			yamlMod.On("PluginInfo").Return(&plugin.Info{
				Name:        jobTask.Name().String(),
				Description: "example",
				Image:       "http://to.repo",
			}, nil)
			taskPlugin := &plugin.Plugin{YamlMod: yamlMod}
			pluginRepo.On("GetByName", jobTask.Name().String()).Return(taskPlugin, nil)

			pluginService := service.NewJobPluginService(pluginRepo, extractorFac, logger)
			result, err := pluginService.GenerateDependencies(ctx, jobTask.Name(), svcAcc, query, destinationURN)
			assert.NoError(t, err)
			assert.ElementsMatch(t, expectedDeps, result)
		})
		t.Run("should generate unique dependencies for select statements", func(t *testing.T) {
			svcAcc := "service_account"
			query := "Select * from proj.dataset.table1 t1 join proj.dataset.table1 t2 on t1.col1 = t2.col1"
			destinationURN := job.ResourceURN("bigquery://proj:datas.tab")
			expectedDeps := []job.ResourceURN{"bigquery://proj:dataset.table1", "bigquery://proj:dataset.table2"}

			pluginRepo := new(mockPluginRepo)
			defer pluginRepo.AssertExpectations(t)

			yamlMod := new(mockOpt.YamlMod)
			defer yamlMod.AssertExpectations(t)

			extractorFac := new(ExtractorFactory)
			defer extractorFac.AssertExpectations(t)

			table1BqResourceURN, _ := bigquery.NewResourceURN("proj", "dataset", "table1")
			table2BqResourceURN, _ := bigquery.NewResourceURN("proj", "dataset", "table2")
			var extractorFunc extractor.BQExtractorFunc = func(context.Context, []bigquery.ResourceURN) (map[bigquery.ResourceURN]string, error) {
				return map[bigquery.ResourceURN]string{
					table1BqResourceURN: "CREATE VIEW `proj.dataset.table1` AS select * from `proj.dataset.table2`;;",
					table2BqResourceURN: "",
				}, nil
			}
			extractorFac.On("New", ctx, svcAcc, mock.Anything).Return(extractorFunc, nil)

			yamlMod.On("PluginInfo").Return(&plugin.Info{
				Name:        jobTask.Name().String(),
				Description: "example",
				Image:       "http://to.repo",
			}, nil)
			taskPlugin := &plugin.Plugin{YamlMod: yamlMod}
			pluginRepo.On("GetByName", jobTask.Name().String()).Return(taskPlugin, nil)

			pluginService := service.NewJobPluginService(pluginRepo, extractorFac, logger)
			result, err := pluginService.GenerateDependencies(ctx, jobTask.Name(), svcAcc, query, destinationURN)
			assert.NoError(t, err)
			assert.ElementsMatch(t, expectedDeps, result)
		})
		t.Run("should generate dependencies for select statements but ignore if asked explicitly", func(t *testing.T) {
			svcAcc := "service_account"
			query := "Select * from /* @ignoreupstream */ proj.dataset.table1"
			destinationURN := job.ResourceURN("bigquery://proj:datas.tab")
			expectedDeps := []job.ResourceURN{}

			pluginRepo := new(mockPluginRepo)
			defer pluginRepo.AssertExpectations(t)

			yamlMod := new(mockOpt.YamlMod)
			defer yamlMod.AssertExpectations(t)

			extractorFac := new(ExtractorFactory)
			defer extractorFac.AssertExpectations(t)

			var extractorFunc extractor.BQExtractorFunc = func(context.Context, []bigquery.ResourceURN) (map[bigquery.ResourceURN]string, error) {
				return map[bigquery.ResourceURN]string{}, nil
			}
			extractorFac.On("New", ctx, svcAcc, mock.Anything).Return(extractorFunc, nil)

			yamlMod.On("PluginInfo").Return(&plugin.Info{
				Name:        jobTask.Name().String(),
				Description: "example",
				Image:       "http://to.repo",
			}, nil)
			taskPlugin := &plugin.Plugin{YamlMod: yamlMod}
			pluginRepo.On("GetByName", jobTask.Name().String()).Return(taskPlugin, nil)

			pluginService := service.NewJobPluginService(pluginRepo, extractorFac, logger)
			result, err := pluginService.GenerateDependencies(ctx, jobTask.Name(), svcAcc, query, destinationURN)
			assert.NoError(t, err)
			assert.ElementsMatch(t, expectedDeps, result)
		})
		t.Run("should generate dependencies for select statements but ignore if asked explicitly for view", func(t *testing.T) {
			svcAcc := "service_account"
			query := "Select * from proj.dataset.table1 t1 left join /* @ignoreupstream */ proj.dataset.view1 v1 on t1.date=v1.date"
			destinationURN := job.ResourceURN("bigquery://proj:datas.tab")
			expectedDeps := []job.ResourceURN{"bigquery://proj:dataset.table1"}

			pluginRepo := new(mockPluginRepo)
			defer pluginRepo.AssertExpectations(t)

			yamlMod := new(mockOpt.YamlMod)
			defer yamlMod.AssertExpectations(t)

			extractorFac := new(ExtractorFactory)
			defer extractorFac.AssertExpectations(t)

			table1BqResourceURN, _ := bigquery.NewResourceURN("proj", "dataset", "table1")
			var extractorFunc extractor.BQExtractorFunc = func(context.Context, []bigquery.ResourceURN) (map[bigquery.ResourceURN]string, error) {
				return map[bigquery.ResourceURN]string{
					table1BqResourceURN: "",
				}, nil
			}
			extractorFac.On("New", ctx, svcAcc, mock.Anything).Return(extractorFunc, nil)

			yamlMod.On("PluginInfo").Return(&plugin.Info{
				Name:        jobTask.Name().String(),
				Description: "example",
				Image:       "http://to.repo",
			}, nil)
			taskPlugin := &plugin.Plugin{YamlMod: yamlMod}
			pluginRepo.On("GetByName", jobTask.Name().String()).Return(taskPlugin, nil)

			pluginService := service.NewJobPluginService(pluginRepo, extractorFac, logger)
			result, err := pluginService.GenerateDependencies(ctx, jobTask.Name(), svcAcc, query, destinationURN)
			assert.NoError(t, err)
			assert.ElementsMatch(t, expectedDeps, result)
		})
		t.Run("should generate clean dependencies without destination in it", func(t *testing.T) {
			svcAcc := "service_account"
			query := "Select * from proj.dataset.table1 join proj.datas.tab"
			destinationURN := job.ResourceURN("bigquery://proj:datas.tab")
			expectedDeps := []job.ResourceURN{"bigquery://proj:dataset.table1"}

			pluginRepo := new(mockPluginRepo)
			defer pluginRepo.AssertExpectations(t)

			yamlMod := new(mockOpt.YamlMod)
			defer yamlMod.AssertExpectations(t)

			extractorFac := new(ExtractorFactory)
			defer extractorFac.AssertExpectations(t)

			table1BqResourceURN, _ := bigquery.NewResourceURN("proj", "dataset", "table1")
			var extractorFunc extractor.BQExtractorFunc = func(context.Context, []bigquery.ResourceURN) (map[bigquery.ResourceURN]string, error) {
				return map[bigquery.ResourceURN]string{
					table1BqResourceURN: "",
				}, nil
			}
			extractorFac.On("New", ctx, svcAcc, mock.Anything).Return(extractorFunc, nil)

			yamlMod.On("PluginInfo").Return(&plugin.Info{
				Name:        jobTask.Name().String(),
				Description: "example",
				Image:       "http://to.repo",
			}, nil)
			taskPlugin := &plugin.Plugin{YamlMod: yamlMod}
			pluginRepo.On("GetByName", jobTask.Name().String()).Return(taskPlugin, nil)

			pluginService := service.NewJobPluginService(pluginRepo, extractorFac, logger)
			result, err := pluginService.GenerateDependencies(ctx, jobTask.Name(), svcAcc, query, destinationURN)
			assert.NoError(t, err)
			assert.ElementsMatch(t, expectedDeps, result)
		})
	})
}

type mockPluginRepo struct {
	mock.Mock
}

func (m *mockPluginRepo) GetByName(name string) (*plugin.Plugin, error) {
	args := m.Called(name)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*plugin.Plugin), args.Error(1)
}

// ExtractorFactory is an autogenerated mock type for the ExtractorFactory type
type ExtractorFactory struct {
	mock.Mock
}

// New provides a mock function with given fields: ctx, svcAcc, l
func (_m *ExtractorFactory) New(ctx context.Context, svcAcc string, l log.Logger) (extractor.BQExtractorFunc, error) {
	ret := _m.Called(ctx, svcAcc, l)

	var r0 extractor.BQExtractorFunc
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context, string, log.Logger) (extractor.BQExtractorFunc, error)); ok {
		return rf(ctx, svcAcc, l)
	}
	if rf, ok := ret.Get(0).(func(context.Context, string, log.Logger) extractor.BQExtractorFunc); ok {
		r0 = rf(ctx, svcAcc, l)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(extractor.BQExtractorFunc)
		}
	}

	if rf, ok := ret.Get(1).(func(context.Context, string, log.Logger) error); ok {
		r1 = rf(ctx, svcAcc, l)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}
