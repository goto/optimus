package job

import (
	"context"
	"fmt"
	"time"

	"github.com/goto/salt/log"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"

	"github.com/goto/optimus/client/cmd/internal/connection"
	"github.com/goto/optimus/client/cmd/internal/logger"
	"github.com/goto/optimus/client/local/model"
	"github.com/goto/optimus/client/local/specio"
	"github.com/goto/optimus/config"
	pb "github.com/goto/optimus/protos/gotocompany/optimus/core/v1beta1"
)

const (
	deployTimeout    = time.Minute * 30
	defaultBatchSize = 10
)

type deployCommand struct {
	logger     log.Logger
	connection connection.Connection

	configFilePath string

	projectName   string
	namespaceName string
	host          string

	clientConfig *config.ClientConfig

	jobNames  []string
	batchSize int
}

// NewDeployCommand initializes command for deploying job specifications
func NewDeployCommand() *cobra.Command {
	deploy := &deployCommand{
		clientConfig: &config.ClientConfig{},
	}
	cmd := &cobra.Command{
		Use:     "deploy",
		Short:   "Deploy optimus job specifications",
		Long:    "Deploy (create or modify) specific optimus job specifications",
		Example: "optimus job deploy --jobs [<job_name>,<job_name>] --namespace [namespace_name]",
		RunE:    deploy.RunE,
		PreRunE: deploy.PreRunE,
	}
	cmd.Flags().StringVarP(&deploy.configFilePath, "config", "c", config.EmptyPath, "File path for client configuration")
	cmd.Flags().StringVar(&deploy.host, "host", "", "Optimus service endpoint url")

	cmd.Flags().StringVarP(&deploy.namespaceName, "namespace", "n", deploy.namespaceName, "Namespace name in which the job resides")
	cmd.Flags().StringSliceVarP(&deploy.jobNames, "jobs", "J", nil, "Job names")
	cmd.Flags().IntVarP(&deploy.batchSize, "batch-size", "b", defaultBatchSize, "Number of jobs to be deployed in a batch")

	cmd.MarkFlagRequired("namespace")
	return cmd
}

func (e *deployCommand) PreRunE(_ *cobra.Command, _ []string) error {
	if err := e.loadConfig(); err != nil {
		return err
	}
	e.logger = logger.NewClientLogger()

	e.connection = connection.New(e.logger, e.clientConfig)
	return nil
}

func (e *deployCommand) RunE(_ *cobra.Command, _ []string) error {
	e.projectName = e.clientConfig.Project.Name

	namespace, err := e.clientConfig.GetNamespaceByName(e.namespaceName)
	if err != nil {
		return err
	}

	start := time.Now()
	if err := e.deployJobSpecifications(namespace.Job.Path); err != nil {
		return err
	}
	e.logger.Info("Jobs deployed successfully, took %s", time.Since(start).Round(time.Second))
	return nil
}

func (e *deployCommand) loadConfig() error {
	conf, err := config.LoadClientConfig(e.configFilePath)
	if err != nil {
		return err
	}
	*e.clientConfig = *conf
	return nil
}

func (e *deployCommand) deployJobSpecifications(namespacePath string) error {
	conn, err := e.connection.Create(e.clientConfig.Host)
	if err != nil {
		return err
	}
	defer conn.Close()

	jobSpecificationServiceClient := pb.NewJobSpecificationServiceClient(conn)
	ctx, dialCancel := context.WithTimeout(context.Background(), deployTimeout)
	defer dialCancel()

	jobSpecs, err := e.getJobSpecs(namespacePath)
	if err != nil {
		return err
	}

	return e.executeJobUpsert(ctx, jobSpecificationServiceClient, jobSpecs)
}

func (e *deployCommand) executeJobUpsert(ctx context.Context, jobSpecificationServiceClient pb.JobSpecificationServiceClient, jobSpecsToUpsert []*model.JobSpec) error {
	if len(jobSpecsToUpsert) == 0 {
		return nil
	}
	specsToUpsertProto := make([]*pb.JobSpecification, len(jobSpecsToUpsert))
	for i, jobSpec := range jobSpecsToUpsert {
		specsToUpsertProto[i] = jobSpec.ToProto()
	}

	countJobsToProcess := len(specsToUpsertProto)
	countJobsFailed := 0
	for i := 0; i < countJobsToProcess; i += e.batchSize {
		endIndex := i + e.batchSize
		if countJobsToProcess < endIndex {
			endIndex = countJobsToProcess
		}
		resp, err := jobSpecificationServiceClient.UpsertJobSpecifications(ctx, &pb.UpsertJobSpecificationsRequest{
			ProjectName:   e.projectName,
			NamespaceName: e.namespaceName,
			Specs:         specsToUpsertProto[i:endIndex],
		})
		if err != nil {
			e.logger.Error("failure in job deploy", err)
			continue
		}
		for _, name := range resp.SuccessfulJobNames {
			e.logger.Info("[success] %s", name)
		}
		for _, name := range resp.SkippedJobNames {
			e.logger.Info("[skipped] %s", name)
		}
		for _, name := range resp.FailedJobNames {
			e.logger.Error("[failed] %s", name)
		}
		if resp.GetLog() != "" {
			e.logger.Warn(resp.GetLog())
		}
	}
	if countJobsFailed > 0 {
		return fmt.Errorf("failed to deploy %d jobs", countJobsFailed)
	}
	return nil
}

func (e *deployCommand) getJobSpecs(namespaceJobPath string) ([]*model.JobSpec, error) {
	jobSpecReadWriter, err := specio.NewJobSpecReadWriter(afero.NewOsFs(), specio.WithJobSpecParentReading())
	if err != nil {
		return nil, err
	}
	allJobSpecsInNamespace, err := jobSpecReadWriter.ReadAll(namespaceJobPath)
	if err != nil {
		return nil, err
	}
	jobNameToSpecMap := make(map[string]*model.JobSpec, len(allJobSpecsInNamespace))
	for _, spec := range allJobSpecsInNamespace {
		jobNameToSpecMap[spec.Name] = spec
	}

	jobSpecs := make([]*model.JobSpec, 0)
	if len(e.jobNames) == 0 {
		for _, spec := range jobNameToSpecMap {
			jobSpecs = append(jobSpecs, spec)
		}
		return jobSpecs, nil
	}

	for _, jobName := range e.jobNames {
		jobSpec, ok := jobNameToSpecMap[jobName]
		if !ok {
			return nil, fmt.Errorf("job %s not found in namespace %s", jobName, e.namespaceName)
		}
		jobSpecs = append(jobSpecs, jobSpec)
	}
	return jobSpecs, nil
}
