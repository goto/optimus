package service_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/goto/salt/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/goto/optimus/core/scheduler"
	"github.com/goto/optimus/core/scheduler/service"
	"github.com/goto/optimus/core/tenant"
	"github.com/goto/optimus/internal/compiler"
	"github.com/goto/optimus/internal/lib/window"
	"github.com/goto/optimus/internal/models"
	"github.com/goto/optimus/sdk/plugin"
)

func TestJobAssetsCompiler(t *testing.T) {
	ctx := context.Background()
	project, _ := tenant.NewProject("proj1", map[string]string{
		"STORAGE_PATH":   "somePath",
		"SCHEDULER_HOST": "localhost",
	})
	namespace, _ := tenant.NewNamespace("ns1", project.Name(), map[string]string{})
	tnnt, _ := tenant.NewTenant(project.Name().String(), namespace.Name().String())
	currentTime := time.Now()
	scheduleTime := currentTime.Add(-time.Hour)
	w1, _ := models.NewWindow(2, "d", "1h", "24h")
	windowConfig1 := window.NewCustomConfig(w1)

	job := &scheduler.Job{
		Name:   "jobName",
		Tenant: tnnt,
		Task: &scheduler.Task{
			Name: "bq2bq",
			Config: map[string]string{
				"configName":  "configValue",
				"LOAD_METHOD": "REPLACE",
			},
		},
		Hooks:        nil,
		WindowConfig: windowConfig1,
		Assets: map[string]string{
			"assetName": "assetValue",
			"query.sql": "select 1;",
		},
	}
	w2 := window.FromBaseWindow(w1)
	interval, err := w2.GetInterval(scheduleTime)
	assert.NoError(t, err)

	executedAt := currentTime.Add(time.Hour)
	systemEnvVars := map[string]string{
		"DSTART":          interval.Start.Format(time.RFC3339),
		"DEND":            interval.End.Format(time.RFC3339),
		"EXECUTION_TIME":  executedAt.Format(time.RFC3339),
		"JOB_DESTINATION": job.Destination,
	}

	logger := log.NewNoop()

	t.Run("CompileJobRunAssets", func(t *testing.T) {
		t.Run("compile should return no error when task is not bq2bq", func(t *testing.T) {
			jobRunAssetsCompiler := service.NewJobAssetsCompiler(compiler.NewEngine(), logger)

			contextForTask := map[string]any{}
			jobNonBq2bq := job
			jobNonBq2bq.Task = &scheduler.Task{
				Name:   "python",
				Config: map[string]string{},
			}
			assets, err := jobRunAssetsCompiler.CompileJobRunAssets(ctx, jobNonBq2bq, systemEnvVars, interval, contextForTask)

			assert.NoError(t, err)
			assert.NotNil(t, assets)
		})
		t.Run("compile", func(t *testing.T) {
			contextForTask := map[string]any{}

			t.Run("return error if compiler.compile fails", func(t *testing.T) {
				filesCompiler := new(mockFilesCompiler)
				filesCompiler.On("Compile", map[string]string{"assetName": "assetValue", "query.sql": "select 1;"}, contextForTask).
					Return(nil, fmt.Errorf("error in compiling"))
				defer filesCompiler.AssertExpectations(t)

				jobRunAssetsCompiler := service.NewJobAssetsCompiler(filesCompiler, logger)
				assets, err := jobRunAssetsCompiler.CompileJobRunAssets(ctx, job, systemEnvVars, interval, contextForTask)

				assert.NotNil(t, err)
				assert.EqualError(t, err, "error in compiling")
				assert.Nil(t, assets)
			})
			t.Run("return compiled assets", func(t *testing.T) {
				expectedFileMap := map[string]string{
					"assetName": "assetValue",
					"query.sql": "select 1;",
				}

				filesCompiler := new(mockFilesCompiler)
				filesCompiler.On("Compile", map[string]string{"assetName": "assetValue", "query.sql": "select 1;"}, contextForTask).
					Return(expectedFileMap, nil)
				defer filesCompiler.AssertExpectations(t)

				jobRunAssetsCompiler := service.NewJobAssetsCompiler(filesCompiler, logger)
				assets, err := jobRunAssetsCompiler.CompileJobRunAssets(ctx, job, systemEnvVars, interval, contextForTask)

				assert.Nil(t, err)
				assert.Equal(t, expectedFileMap, assets)
			})
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

type mockFilesCompiler struct {
	mock.Mock
}

func (m *mockFilesCompiler) Compile(fileMap map[string]string, context map[string]any) (map[string]string, error) {
	args := m.Called(fileMap, context)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(map[string]string), args.Error(1)
}
