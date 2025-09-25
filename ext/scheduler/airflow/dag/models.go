package dag

import (
	"github.com/goto/optimus/core/scheduler"
	"github.com/goto/optimus/core/tenant"
	"github.com/goto/optimus/internal/errors"
	"github.com/goto/optimus/plugin"
)

const (
	EntitySchedulerAirflow = "schedulerAirflow"
)

type TemplateContext struct {
	JobDetails *scheduler.JobWithDetails

	Tenant           tenant.Tenant
	Version          string
	SchedulerVersion string
	SLAMissDuration  int64
	Hostname         string
	GRPCHostName     string
	ExecutorTask     string
	ExecutorHook     string

	RuntimeConfig RuntimeConfig
	Task          Task
	Hooks         Hooks
	Priority      int
	Upstreams     Upstreams

	DisableJobScheduling bool
	EnableRBAC           bool
}

type Task struct {
	Name       string
	Image      string
	Entrypoint plugin.Entrypoint
}

func PrepareTask(job *scheduler.Job, pluginRepo PluginRepo) (Task, error) {
	spec, err := pluginRepo.GetByName(job.Task.Name)
	if err != nil {
		return Task{}, errors.NotFound(EntitySchedulerAirflow, "plugin not found for "+job.Task.Name)
	}

	img, err := spec.GetImage(job.Task.Version)
	if err != nil {
		return Task{}, errors.NotFound("schedulerAirflow", "error in getting image "+job.Task.Name)
	}

	ep, err := spec.GetEntrypoint(job.Task.Version)
	if err != nil {
		return Task{}, errors.NotFound("schedulerAirflow", "error in getting entrypoint "+job.Task.Name)
	}

	return Task{
		Name:       spec.Name,
		Image:      img,
		Entrypoint: ep,
	}, nil
}

type Hook struct {
	Name       string
	Image      string
	Entrypoint plugin.Entrypoint
	IsFailHook bool // Set to false
}

type Hooks []Hook

func PrepareHooksForJob(job *scheduler.Job, pluginRepo PluginRepo) (Hooks, error) {
	var hooks Hooks

	for _, h := range job.Hooks {
		spec, err := pluginRepo.GetByName(h.Name)
		if err != nil {
			return Hooks{}, errors.NotFound("schedulerAirflow", "hook not found for name "+h.Name)
		}

		img, err := spec.GetImage(h.Version)
		if err != nil {
			return Hooks{}, errors.NotFound("schedulerAirflow", "error in getting image "+h.Name)
		}

		ep, err := spec.GetEntrypoint(h.Version)
		if err != nil {
			return Hooks{}, errors.NotFound("schedulerAirflow", "error in getting entrypoint "+h.Name)
		}

		hk := Hook{
			Name:       spec.Name,
			Image:      img,
			Entrypoint: ep,
		}

		hooks = append(hooks, hk)
	}

	return hooks, nil
}

type RuntimeConfig struct {
	Resource *Resource
	Airflow  AirflowConfig
}

func SetupRuntimeConfig(jobDetails *scheduler.JobWithDetails) RuntimeConfig {
	runtimeConf := RuntimeConfig{
		Airflow: ToAirflowConfig(jobDetails.RuntimeConfig.Scheduler),
	}
	if resource := ToResource(jobDetails.RuntimeConfig.Resource); resource != nil {
		runtimeConf.Resource = resource
	}
	return runtimeConf
}

type Resource struct {
	Request *ResourceConfig
	Limit   *ResourceConfig
}

func ToResource(resource *scheduler.Resource) *Resource {
	if resource == nil {
		return nil
	}
	req := ToResourceConfig(resource.Request)
	limit := ToResourceConfig(resource.Limit)
	if req == nil && limit == nil {
		return nil
	}
	res := &Resource{}
	if req != nil {
		res.Request = req
	}
	if limit != nil {
		res.Limit = limit
	}
	return res
}

type ResourceConfig struct {
	CPU    string
	Memory string
}

func ToResourceConfig(config *scheduler.ResourceConfig) *ResourceConfig {
	if config == nil {
		return nil
	}
	if config.CPU == "" && config.Memory == "" {
		return nil
	}
	return &ResourceConfig{
		CPU:    config.CPU,
		Memory: config.Memory,
	}
}

type AirflowConfig struct {
	Pool  string
	Queue string
}

func ToAirflowConfig(schedulerConf map[string]string) AirflowConfig {
	conf := AirflowConfig{}
	if pool, ok := schedulerConf["pool"]; ok {
		conf.Pool = pool
	}
	if queue, ok := schedulerConf["queue"]; ok {
		conf.Queue = queue
	}
	return conf
}
