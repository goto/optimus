package service

import (
	"errors"
	"fmt"
	"time"

	"github.com/goto/salt/log"
	"golang.org/x/net/context"

	"github.com/goto/optimus/core/job"
	"github.com/goto/optimus/sdk/plugin"
)

const (
	projectConfigPrefix = "GLOBAL__"

	configKeyDstart        = "DSTART"
	configKeyDend          = "DEND"
	configKeyExecutionTime = "EXECUTION_TIME"
	configKeyDestination   = "JOB_DESTINATION"

	TimeISOFormat = time.RFC3339
)

var (
	ErrYamlModNotExist = errors.New("yaml mod not found for plugin")
)

type PluginRepo interface {
	GetByName(string) (*plugin.Plugin, error)
}

type JobPluginService struct {
	pluginRepo PluginRepo

	logger log.Logger
}

func NewJobPluginService(pluginRepo PluginRepo, logger log.Logger) *JobPluginService {
	return &JobPluginService{pluginRepo: pluginRepo, logger: logger}
}

func (p JobPluginService) Info(_ context.Context, taskName job.TaskName) (*plugin.Info, error) {
	taskPlugin, err := p.pluginRepo.GetByName(taskName.String())
	if err != nil {
		p.logger.Error("error getting plugin [%s]: %s", taskName.String(), err)
		return nil, err
	}

	if taskPlugin.YamlMod == nil {
		p.logger.Error("task plugin yaml mod is not found")
		return nil, ErrYamlModNotExist
	}

	return taskPlugin.Info(), nil
}

func (p JobPluginService) GenerateDestination(ctx context.Context, taskName job.TaskName, configs map[string]string) (job.ResourceURN, error) {
	taskPlugin, err := p.pluginRepo.GetByName(taskName.String())
	if err != nil {
		p.logger.Error("error getting plugin [%s]: %s", taskName.String(), err)
		return "", err
	}
	if taskPlugin.Info().Name != "bq2bq" {
		return "", nil
	}

	proj, ok1 := configs["PROJECT"]
	dataset, ok2 := configs["DATASET"]
	tab, ok3 := configs["TABLE"]
	if ok1 && ok2 && ok3 {
		return job.ResourceURN("bigquery://" + fmt.Sprintf("%s:%s.%s", proj, dataset, tab)), nil
	}
	return "", errors.New("missing config key required to generate destination")
}
