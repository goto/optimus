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
	deployTimeout = time.Minute * 30
)

type deployCommand struct {
	logger     log.Logger
	connection connection.Connection

	configFilePath string

	projectName   string
	namespaceName string
	host          string

	clientConfig *config.ClientConfig

	jobNames []string
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

	cmd.MarkFlagRequired("jobs")
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

	jobSpecs, err := e.getJobSpecByNames(namespacePath)
	if err != nil {
		return err
	}

	jobSpecsToAdd, jobSpecsToUpdate, err := e.differentiateJobSpecsAction(ctx, jobSpecificationServiceClient, jobSpecs)
	if err != nil {
		return err
	}

	err = e.executeJobAdd(ctx, jobSpecificationServiceClient, jobSpecsToAdd)
	if err != nil {
		return err
	}

	return e.executeJobUpdate(ctx, jobSpecificationServiceClient, jobSpecsToUpdate)
}

func (e *deployCommand) differentiateJobSpecsAction(ctx context.Context, jobSpecificationServiceClient pb.JobSpecificationServiceClient, jobSpecs []*model.JobSpec) ([]*model.JobSpec, []*model.JobSpec, error) {
	jobSpecsToAdd := make([]*model.JobSpec, 0)
	jobSpecsToUpdate := make([]*model.JobSpec, 0)
	for _, jobSpec := range jobSpecs {
		response, err := jobSpecificationServiceClient.GetJobSpecifications(ctx, &pb.GetJobSpecificationsRequest{
			ProjectName:   e.projectName,
			NamespaceName: e.namespaceName,
			JobName:       jobSpec.Name,
		})
		if err != nil {
			return nil, nil, err
		}
		if len(response.GetJobSpecificationResponses()) == 0 {
			jobSpecsToAdd = append(jobSpecsToAdd, jobSpec)
		} else {
			jobSpecsToUpdate = append(jobSpecsToUpdate, jobSpec)
		}
	}
	return jobSpecsToAdd, jobSpecsToUpdate, nil
}

func (e *deployCommand) executeJobAdd(ctx context.Context, jobSpecificationServiceClient pb.JobSpecificationServiceClient, jobSpecsToAdd []*model.JobSpec) error {
	if len(jobSpecsToAdd) == 0 {
		return nil
	}
	specsToAddProto := make([]*pb.JobSpecification, len(jobSpecsToAdd))
	for i, jobSpec := range jobSpecsToAdd {
		specsToAddProto[i] = jobSpec.ToProto()
	}
	_, err := jobSpecificationServiceClient.AddJobSpecifications(ctx, &pb.AddJobSpecificationsRequest{
		ProjectName:   e.projectName,
		NamespaceName: e.namespaceName,
		Specs:         specsToAddProto,
	})
	if err != nil {
		return err
	}
	for _, spec := range jobSpecsToAdd {
		e.logger.Info("Added %s job", spec.Name)
	}
	return nil
}

func (e *deployCommand) executeJobUpdate(ctx context.Context, jobSpecificationServiceClient pb.JobSpecificationServiceClient, jobSpecsToUpdate []*model.JobSpec) error {
	if len(jobSpecsToUpdate) == 0 {
		return nil
	}
	specsToUpdateProto := make([]*pb.JobSpecification, len(jobSpecsToUpdate))
	for i, jobSpec := range jobSpecsToUpdate {
		specsToUpdateProto[i] = jobSpec.ToProto()
	}
	_, err := jobSpecificationServiceClient.UpdateJobSpecifications(ctx, &pb.UpdateJobSpecificationsRequest{
		ProjectName:   e.projectName,
		NamespaceName: e.namespaceName,
		Specs:         specsToUpdateProto,
	})
	if err != nil {
		return err
	}
	for _, spec := range jobSpecsToUpdate {
		e.logger.Info("Updated %s job", spec.Name)
	}
	return nil
}

func (e *deployCommand) getJobSpecByNames(namespaceJobPath string) ([]*model.JobSpec, error) {
	jobSpecs := make([]*model.JobSpec, 0)
	for _, jobName := range e.jobNames {
		jobSpecReadWriter, err := specio.NewJobSpecReadWriter(afero.NewOsFs(), specio.WithJobSpecParentReading())
		if err != nil {
			return nil, err
		}
		jobSpec, err := jobSpecReadWriter.ReadByName(namespaceJobPath, jobName)
		if err != nil {
			return nil, err
		}
		if jobSpec == nil {
			return nil, fmt.Errorf("job %s not found in namespace %s", jobName, e.namespaceName)
		}
		jobSpecs = append(jobSpecs, jobSpec)
	}

	return jobSpecs, nil
}
