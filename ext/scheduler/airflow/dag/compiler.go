package dag

import (
	"bytes"
	"fmt"

	"github.com/goto/optimus/config"
	"github.com/goto/optimus/core/scheduler"
	"github.com/goto/optimus/core/tenant"
	"github.com/goto/optimus/internal/errors"
	"github.com/goto/optimus/sdk/plugin"
)

type PluginRepo interface {
	GetByName(name string) (*plugin.Plugin, error)
}

type Compiler struct {
	hostname string

	templateFactory TemplateFactory
	pluginRepo      PluginRepo
}

func (c *Compiler) Compile(project *tenant.Project, jobDetails *scheduler.JobWithDetails) ([]byte, error) {
	task, err := PrepareTask(jobDetails.Job, c.pluginRepo)
	if err != nil {
		return nil, err
	}

	hooks, err := PrepareHooksForJob(jobDetails.Job, c.pluginRepo)
	if err != nil {
		return nil, err
	}

	slaDuration, err := SLAMissDuration(jobDetails)
	if err != nil {
		return nil, err
	}

	runtimeConfig := SetupRuntimeConfig(jobDetails)

	upstreams := SetupUpstreams(jobDetails.Upstreams, c.hostname)

	templateContext := TemplateContext{
		JobDetails:      jobDetails,
		Tenant:          jobDetails.Job.Tenant,
		Version:         config.BuildVersion,
		SLAMissDuration: slaDuration,
		Hostname:        c.hostname,
		ExecutorTask:    scheduler.ExecutorTask.String(),
		ExecutorHook:    scheduler.ExecutorHook.String(),
		Task:            task,
		Hooks:           hooks,
		RuntimeConfig:   runtimeConfig,
		Priority:        jobDetails.Priority,
		Upstreams:       upstreams,
	}

	airflowVersion, err := project.GetConfig(tenant.ProjectAirflowVersion)
	if err != nil {
		msg := fmt.Sprintf("%s is not provided in project %s, %s", tenant.ProjectAirflowVersion, project.Name(), err.Error())
		return nil, errors.InvalidArgument(EntitySchedulerAirflow, msg)
	}
	tmpl := c.templateFactory.New(airflowVersion)

	var buf bytes.Buffer
	if err = tmpl.Execute(&buf, templateContext); err != nil {
		msg := fmt.Sprintf("unable to compile template for job %s with airflow version %s, %s", jobDetails.Name.String(), airflowVersion, err.Error())
		return nil, errors.InvalidArgument(EntitySchedulerAirflow, msg)
	}

	return buf.Bytes(), nil
}

func NewDagCompiler(hostname string, repo PluginRepo) (*Compiler, error) {
	templateFactory, err := NewTemplateFactory()
	if err != nil {
		return nil, errors.InternalError(EntitySchedulerAirflow, "unable to instantiate template factory", err)
	}

	return &Compiler{
		hostname:        hostname,
		templateFactory: templateFactory,
		pluginRepo:      repo,
	}, nil
}
