package server

import (
	"fmt"

	"github.com/goto/salt/log"

	"github.com/goto/optimus/config"
	"github.com/goto/optimus/ext/scheduler/airflow"
	"github.com/goto/optimus/ext/scheduler/airflow/bucket"
	"github.com/goto/optimus/ext/scheduler/airflow/dag"
)

func NewScheduler(l log.Logger, conf *config.ServerConfig, pluginRepo dag.PluginRepo, projecGetter airflow.ProjectGetter,
	secretGetter airflow.SecretGetter,
) (*airflow.Scheduler, error) {
	bucketFactory := bucket.NewFactory(projecGetter, secretGetter)

	grpcHost := fmt.Sprintf("%s:%d", conf.Serve.IngressHostGRPC, conf.Serve.PortGRPC)

	dagCompiler, err := dag.NewDagCompiler(l, conf.Serve.IngressHost, grpcHost, pluginRepo)
	if err != nil {
		return nil, err
	}

	client := airflow.NewAirflowClient()
	scheduler := airflow.NewScheduler(l, bucketFactory, client, dagCompiler, projecGetter, secretGetter)
	return scheduler, nil
}
