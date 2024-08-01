package resolver

import (
	"context"
	"fmt"

	"github.com/goto/optimus/config"
	"github.com/goto/optimus/core/scheduler"
	"github.com/goto/optimus/core/tenant"
	"github.com/goto/optimus/ext/resourcemanager"
	"github.com/goto/optimus/internal/errors"
)

type ExternalOptimusConnectors struct {
	optimusResourceManagers []OptimusResourceManager
}

type OptimusResourceManager interface {
	GetHostURL() string
	GetJobScheduleInterval(ctx context.Context, tnnt tenant.Tenant, jobName scheduler.JobName) (string, error)
	GetJobRuns(ctx context.Context, sensorParameters scheduler.JobSensorParameters, criteria *scheduler.JobRunsCriteria) ([]*scheduler.JobRunStatus, error)
}

const ExternalUpstreamEntity = "ExternalResourceInterface"

// NewExternalOptimusManager creates a new instance of externalResourceResolver
func NewExternalOptimusManager(resourceManagerConfigs []config.ResourceManager) (*ExternalOptimusConnectors, error) {
	var optimusResourceManagers []OptimusResourceManager
	for _, conf := range resourceManagerConfigs {
		switch conf.Type {
		case "optimus":
			optimusResourceManager, err := resourcemanager.NewOptimusResourceManager(conf)
			if err != nil {
				return nil, err
			}
			optimusResourceManagers = append(optimusResourceManagers, optimusResourceManager)
		default:
			return nil, fmt.Errorf("resource manager %s is not recognized", conf.Type)
		}
	}
	return &ExternalOptimusConnectors{
		optimusResourceManagers: optimusResourceManagers,
	}, nil
}

func (e *ExternalOptimusConnectors) getResourceManager(hostURL string) (OptimusResourceManager, error) {
	for _, manager := range e.optimusResourceManagers {
		if manager.GetHostURL() == hostURL {
			return manager, nil
		}
	}
	return nil, errors.NewError(errors.ErrNotFound, ExternalUpstreamEntity, "could not find external resource manager by host: "+hostURL)
}

func (e *ExternalOptimusConnectors) GetJobScheduleInterval(ctx context.Context, upstreamHost string, tnnt tenant.Tenant, jobName scheduler.JobName) (string, error) {
	rm, err := e.getResourceManager(upstreamHost)
	if err != nil {
		return "", err
	}
	return rm.GetJobScheduleInterval(ctx, tnnt, jobName)
}

func (e *ExternalOptimusConnectors) GetJobRuns(ctx context.Context, upstreamHost string, sensorParameters scheduler.JobSensorParameters, criteria *scheduler.JobRunsCriteria) ([]*scheduler.JobRunStatus, error) {
	rm, err := e.getResourceManager(upstreamHost)
	if err != nil {
		return nil, err
	}
	return rm.GetJobRuns(ctx, sensorParameters, criteria)
}
