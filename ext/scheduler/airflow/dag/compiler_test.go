package dag_test

import (
	_ "embed"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/goto/optimus/core/resource"
	"github.com/goto/optimus/core/scheduler"
	"github.com/goto/optimus/core/tenant"
	"github.com/goto/optimus/ext/scheduler/airflow/dag"
	"github.com/goto/optimus/internal/errors"
	"github.com/goto/optimus/internal/lib/window"
	"github.com/goto/optimus/internal/models"
	"github.com/goto/optimus/sdk/plugin"
	"github.com/goto/optimus/sdk/plugin/mock"
)

//go:embed expected_dag.2.1.py
var compiledTemplate21 []byte

//go:embed expected_dag.2.4.py
var compiledTemplate24 []byte

//go:embed expected_dag.2.6.py
var compiledTemplate26 []byte

//go:embed expected_dag.2.9.py
var compiledTemplate29 []byte

func TestDagCompiler(t *testing.T) {
	t.Run("Compile", func(t *testing.T) {
		grpcHost := "http://grpc.optimus.example.com:8081"
		repo := setupPluginRepo()
		tnnt, err := tenant.NewTenant("example-proj", "billing")
		assert.NoError(t, err)

		t.Run("returns error when cannot find task", func(t *testing.T) {
			emptyRepo := mockPluginRepo{plugins: []*plugin.Plugin{}}
			com, err := dag.NewDagCompiler(nil, "http://optimus.example.com", grpcHost, emptyRepo)
			assert.NoError(t, err)

			job := setupJobDetails(tnnt)
			project := setProject(tnnt, "2.1.4")
			_, err = com.Compile(project, job)
			assert.True(t, errors.IsErrorType(err, errors.ErrNotFound))
			assert.ErrorContains(t, err, "plugin not found for bq-bq")
		})
		t.Run("returns error when cannot find hook", func(t *testing.T) {
			com, err := dag.NewDagCompiler(nil, "http://optimus.example.com", grpcHost, repo)
			assert.NoError(t, err)

			job := setupJobDetails(tnnt)
			job.Job.Hooks = append(job.Job.Hooks, &scheduler.Hook{Name: "invalid"})
			project := setProject(tnnt, "2.1.4")
			_, err = com.Compile(project, job)
			assert.True(t, errors.IsErrorType(err, errors.ErrNotFound))
			assert.ErrorContains(t, err, "hook not found for name invalid")
		})

		t.Run("returns error when sla duration is invalid", func(t *testing.T) {
			com, err := dag.NewDagCompiler(nil, "http://optimus.example.com", grpcHost, repo)
			assert.NoError(t, err)

			job := setupJobDetails(tnnt)
			job.Alerts = append(job.Alerts, scheduler.Alert{
				On: scheduler.EventCategorySLAMiss,
			},
				scheduler.Alert{
					On:     scheduler.EventCategorySLAMiss,
					Config: map[string]string{"duration": "2"},
				})
			project := setProject(tnnt, "2.1.4")
			_, err = com.Compile(project, job)
			assert.ErrorContains(t, err, "failed to parse sla_miss duration 2")
		})

		t.Run("compiles basic template without any error", func(t *testing.T) {
			t.Run("with airflow version 2.1.4", func(t *testing.T) {
				com, err := dag.NewDagCompiler(nil, "http://optimus.example.com", grpcHost, repo)
				assert.NoError(t, err)

				job := setupJobDetails(tnnt)
				project := setProject(tnnt, "2.1.4")
				compiledDag, err := com.Compile(project, job)
				assert.NoError(t, err)
				assert.Equal(t, string(compiledTemplate21), string(compiledDag))
			})
			t.Run("with airflow version 2.4.3", func(t *testing.T) {
				com, err := dag.NewDagCompiler(nil, "http://optimus.example.com", grpcHost, repo)
				assert.NoError(t, err)

				job := setupJobDetails(tnnt)
				project := setProject(tnnt, "2.4.3")
				compiledDag, err := com.Compile(project, job)
				assert.NoError(t, err)
				assert.Equal(t, string(compiledTemplate24), string(compiledDag))
			})
			t.Run("with airflow version 2.6.3", func(t *testing.T) {
				com, err := dag.NewDagCompiler(nil, "http://optimus.example.com", grpcHost, repo)
				assert.NoError(t, err)

				job := setupJobDetails(tnnt)
				project := setProject(tnnt, "2.6.3")
				compiledDag, err := com.Compile(project, job)
				assert.NoError(t, err)
				assert.Equal(t, string(compiledTemplate26), string(compiledDag))
			})

			t.Run("with airflow version 2.9.3", func(t *testing.T) {
				com, err := dag.NewDagCompiler(nil, "http://optimus.example.com", grpcHost, repo)
				assert.NoError(t, err)

				job := setupJobDetails(tnnt)
				project := setProject(tnnt, "2.9.3")
				compiledDag, err := com.Compile(project, job)
				assert.NoError(t, err)
				assert.Equal(t, string(compiledTemplate29), string(compiledDag))
			})
		})
	})
}

func setProject(tnnt tenant.Tenant, airflowVersion string) *tenant.Project {
	p, _ := tenant.NewProject(tnnt.ProjectName().String(), map[string]string{
		tenant.ProjectSchedulerVersion: airflowVersion,
		tenant.ProjectStoragePathKey:   "./path/to/storage",
		tenant.ProjectSchedulerHost:    "http://airflow.com",
	})
	return p
}

func setupJobDetails(tnnt tenant.Tenant) *scheduler.JobWithDetails {
	w1, err := models.NewWindow(1, "d", "0", "1h")
	window1 := window.NewCustomConfig(w1)
	if err != nil {
		panic(err)
	}
	end := time.Date(2022, 11, 10, 10, 2, 0, 0, time.UTC)
	schedule := &scheduler.Schedule{
		StartDate:     time.Date(2022, 11, 10, 5, 2, 0, 0, time.UTC),
		EndDate:       &end,
		Interval:      "0 2 * * 0",
		DependsOnPast: true,
		CatchUp:       true,
	}

	retry := scheduler.Retry{
		Count:              2,
		Delay:              100,
		ExponentialBackoff: true,
	}

	alert := scheduler.Alert{
		On:       scheduler.EventCategorySLAMiss,
		Channels: []string{"#alerts"},
		Config:   map[string]string{"duration": "2h"},
	}

	hooks := []*scheduler.Hook{
		{Name: "transporter"},
		{Name: "predator"},
		{Name: "failureHook"},
	}

	jobMeta := &scheduler.JobMetadata{
		Version:     1,
		Owner:       "infra-team@example.com",
		Description: "This job collects the billing information related to infrastructure.\nThis job will run in a weekly basis.",
		Labels:      map[string]string{"orchestrator": "optimus"},
	}

	urn, err := resource.ParseURN("bigquery://billing:reports.weekly-status")
	if err != nil {
		panic(err)
	}

	jobName := scheduler.JobName("infra.billing.weekly-status-reports")
	job := &scheduler.Job{
		Name:         jobName,
		Tenant:       tnnt,
		Destination:  urn,
		Task:         &scheduler.Task{Name: "bq-bq"},
		Hooks:        hooks,
		WindowConfig: window1,
		Assets:       nil,
	}

	runtimeConfig := scheduler.RuntimeConfig{
		Resource: &scheduler.Resource{
			Limit: &scheduler.ResourceConfig{
				CPU:    "200m",
				Memory: "2G",
			},
		},
	}

	tnnt1, _ := tenant.NewTenant("project", "namespace")
	tnnt2, _ := tenant.NewTenant("external-project", "external-namespace")
	upstreams := scheduler.Upstreams{
		HTTP: nil,
		UpstreamJobs: []*scheduler.JobUpstream{
			{
				Host:     "http://optimus.example.com",
				Tenant:   tnnt,
				JobName:  "foo-intra-dep-job",
				TaskName: "bq",
				State:    "resolved",
			},
			{
				Host:     "http://optimus.example.com",
				Tenant:   tnnt1,
				JobName:  "foo-inter-dep-job",
				TaskName: "bq-bq",
				State:    "resolved",
			},
			{
				JobName:  "foo-external-optimus-dep-job",
				Host:     "http://optimus.external.io",
				TaskName: "bq-bq",
				Tenant:   tnnt2,
				External: true,
				State:    "resolved",
			},
		},
	}

	return &scheduler.JobWithDetails{
		Name:        jobName,
		Job:         job,
		JobMetadata: jobMeta,
		Schedule:    schedule,
		Retry:       retry,
		Alerts:      []scheduler.Alert{alert},

		RuntimeConfig: runtimeConfig,
		Upstreams:     upstreams,
		Priority:      2000,
	}
}

type mockPluginRepo struct {
	plugins []*plugin.Plugin
}

func (m mockPluginRepo) GetByName(name string) (*plugin.Plugin, error) {
	for _, plugin := range m.plugins {
		if plugin.Info().Name == name {
			return plugin, nil
		}
	}
	return nil, fmt.Errorf("error finding %s", name)
}

func setupPluginRepo() mockPluginRepo {
	execUnit := new(mock.YamlMod)
	execUnit.On("PluginInfo").Return(&plugin.Info{
		Name:  "bq-bq",
		Image: "example.io/namespace/bq2bq-executor:latest",
		Entrypoint: plugin.Entrypoint{
			Shell:  "/bin/bash",
			Script: "python3 /opt/bumblebee/main.py",
		},
	}, nil)

	transporterHook := "transporter"
	hookUnit := new(mock.YamlMod)
	hookUnit.On("PluginInfo").Return(&plugin.Info{
		Name:     transporterHook,
		HookType: plugin.HookTypePre,
		Image:    "example.io/namespace/transporter-executor:latest",
		Entrypoint: plugin.Entrypoint{
			Shell:  "/bin/sh",
			Script: "java -cp /opt/transporter/transporter.jar:/opt/transporter/jolokia-jvm-agent.jar -javaagent:jolokia-jvm-agent.jar=port=7777,host=0.0.0.0 com.gojek.transporter.Main",
		},
		DependsOn: []string{"predator"},
	}, nil)

	predatorHook := "predator"
	hookUnit2 := new(mock.YamlMod)
	hookUnit2.On("PluginInfo").Return(&plugin.Info{
		Name:     predatorHook,
		HookType: plugin.HookTypePost,
		Image:    "example.io/namespace/predator-image:latest",
		Entrypoint: plugin.Entrypoint{
			Shell:  "/bin/sh",
			Script: "predator ${SUB_COMMAND} -s ${PREDATOR_URL} -u \"${BQ_PROJECT}.${BQ_DATASET}.${BQ_TABLE}\"",
		},
	}, nil)

	hookUnit3 := new(mock.YamlMod)
	hookUnit3.On("PluginInfo").Return(&plugin.Info{
		Name:     "failureHook",
		HookType: plugin.HookTypeFail,
		Image:    "example.io/namespace/failure-hook-image:latest",
		Entrypoint: plugin.Entrypoint{
			Shell:  "/bin/sh",
			Script: "sleep 5",
		},
	}, nil)

	repo := mockPluginRepo{plugins: []*plugin.Plugin{
		{YamlMod: execUnit}, {YamlMod: hookUnit}, {YamlMod: hookUnit2}, {YamlMod: hookUnit3},
	}}
	return repo
}
