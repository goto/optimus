package apply

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/goto/salt/log"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"

	"github.com/goto/optimus/client/cmd/internal"
	"github.com/goto/optimus/client/cmd/internal/connection"
	"github.com/goto/optimus/client/cmd/internal/logger"
	"github.com/goto/optimus/client/cmd/internal/plan"
	"github.com/goto/optimus/client/local"
	"github.com/goto/optimus/client/local/model"
	"github.com/goto/optimus/client/local/specio"
	"github.com/goto/optimus/config"
	pb "github.com/goto/optimus/protos/gotocompany/optimus/core/v1beta1"
)

const (
	applyTimeout     = time.Minute * 5
	executedPlanFile = "plan_apply.csv"
)

type applyCommand struct {
	logger                 log.Logger
	connection             connection.Connection
	config                 *config.ClientConfig
	jobSpecReadWriter      local.SpecReadWriter[*model.JobSpec]
	resourceSpecReadWriter local.SpecReadWriter[*model.ResourceSpec]

	configFilePath string
	sources        []string
	output         string
}

// NewApplyCommand apply the job / resource changes
// based on the result of optimus job/resource plan
// contract: job,p-godata-id,batching,sample_select,create,true
func NewApplyCommand() *cobra.Command {
	apply := &applyCommand{}
	cmd := &cobra.Command{
		Use:     "apply",
		Short:   "Apply job/resource changes",
		Example: "optimus apply --sources <plan_sources_path>...",
		RunE:    apply.RunE,
		PreRunE: apply.PreRunE,
	}
	apply.injectFlags(cmd)
	return cmd
}

func (c *applyCommand) injectFlags(cmd *cobra.Command) {
	// Config filepath flag
	cmd.PersistentFlags().StringVarP(&c.configFilePath, "config", "c", config.EmptyPath, "File path for client configuration")

	cmd.Flags().StringSliceVarP(&c.sources, "sources", "s", c.sources, "Sources of plan result to be executed")
	cmd.Flags().StringVarP(&c.output, "output", "o", executedPlanFile, "Output of plan result after executed")
}

func (c *applyCommand) PreRunE(_ *cobra.Command, _ []string) error {
	// Load config
	conf, err := internal.LoadOptionalConfig(c.configFilePath)
	if err != nil {
		return err
	}
	if conf == nil {
		return fmt.Errorf("config should be specified")
	}
	jobSpecReadWriter, err := specio.NewJobSpecReadWriter(afero.NewOsFs(), specio.WithJobSpecParentReading())
	if err != nil {
		return fmt.Errorf("couldn't instantiate job spec reader")
	}
	resourceSpecReadWriter, err := specio.NewResourceSpecReadWriter(afero.NewOsFs())
	if err != nil {
		return fmt.Errorf("couldn't instantiate resource spec reader")
	}
	c.logger = logger.NewClientLogger()
	c.connection = connection.New(c.logger, conf)
	c.jobSpecReadWriter = jobSpecReadWriter
	c.resourceSpecReadWriter = resourceSpecReadWriter
	c.config = conf
	return nil
}

func (c *applyCommand) RunE(cmd *cobra.Command, _ []string) error {
	conn, err := c.connection.Create(c.config.Host)
	if err != nil {
		return err
	}
	defer conn.Close()

	jobClient := pb.NewJobSpecificationServiceClient(conn)
	resourceClient := pb.NewResourceServiceClient(conn)
	ctx, dialCancel := context.WithTimeout(cmd.Context(), applyTimeout)
	defer dialCancel()

	// read from plan file
	basePlans, err := c.getPlans()
	if err != nil {
		return err
	}
	plans := basePlans.GetByProjectName(c.config.Project.Name)

	// prepare for proto request
	var (
		addJobRequest         = []*pb.AddJobSpecificationsRequest{}
		updateJobRequest      = []*pb.UpdateJobSpecificationsRequest{}
		deleteJobRequest      = []*pb.DeleteJobSpecificationRequest{}
		addResourceRequest    = []*pb.CreateResourceRequest{}
		updateResourceRequest = []*pb.UpdateResourceRequest{}
		deleteResourceRequest = []*pb.DeleteResourceRequest{}
	)

	for _, namespace := range c.config.Namespaces {
		addJobRequest = append(addJobRequest, c.getAddJobRequest(namespace, plans)...)
		updateJobRequest = append(updateJobRequest, c.getUpdateJobRequest(namespace, plans)...)
		deleteJobRequest = append(deleteJobRequest, c.getDeleteJobRequest(c.config.Project.Name, namespace, plans)...)
		addResourceRequest = append(addResourceRequest, c.getAddResourceRequest(c.config.Project.Name, namespace, plans)...)
		updateResourceRequest = append(updateResourceRequest, c.getUpdateResourceRequest(c.config.Project.Name, namespace, plans)...)
		deleteResourceRequest = append(deleteResourceRequest, c.getDeleteResourceRequest(c.config.Project.Name, namespace, plans)...)
	}

	// send to server based on operation
	deletedJobs := c.executeJobDelete(ctx, jobClient, deleteJobRequest)
	addedJobs := c.executeJobAdd(ctx, jobClient, addJobRequest)
	updatedJobs := c.executeJobUpdate(ctx, jobClient, updateJobRequest)
	deletedResources := c.executeResourceDelete(ctx, resourceClient, deleteResourceRequest)
	addedResources := c.executeResourceAdd(ctx, resourceClient, addResourceRequest)
	updatedResources := c.executeResourceUpdate(ctx, resourceClient, updateResourceRequest)

	// update plan file
	isExecuted := true
	basePlans.UpdateExecutedByNames(isExecuted, plan.KindJob, deletedJobs...)
	basePlans.UpdateExecutedByNames(isExecuted, plan.KindJob, addedJobs...)
	basePlans.UpdateExecutedByNames(isExecuted, plan.KindJob, updatedJobs...)
	basePlans.UpdateExecutedByNames(isExecuted, plan.KindResource, deletedResources...)
	basePlans.UpdateExecutedByNames(isExecuted, plan.KindResource, addedResources...)
	basePlans.UpdateExecutedByNames(isExecuted, plan.KindResource, updatedResources...)

	return c.savePlans(basePlans)
}

func (c *applyCommand) executeJobDelete(ctx context.Context, client pb.JobSpecificationServiceClient, requests []*pb.DeleteJobSpecificationRequest) []string {
	deletedJobs := []string{}
	for _, request := range requests {
		response, err := client.DeleteJobSpecification(ctx, request)
		if err != nil {
			c.logger.Error(err.Error())
			continue
		}
		if response.Success {
			deletedJobs = append(deletedJobs, request.JobName)
		}
	}
	return deletedJobs
}

func (c *applyCommand) executeJobAdd(ctx context.Context, client pb.JobSpecificationServiceClient, requests []*pb.AddJobSpecificationsRequest) []string {
	addedJobs := []string{}
	for _, request := range requests {
		response, err := client.AddJobSpecifications(ctx, request)
		if err != nil {
			c.logger.Error(err.Error())
			continue
		}
		addedJobs = append(addedJobs, response.JobNameSuccesses...)
	}
	return addedJobs
}

func (c *applyCommand) executeJobUpdate(ctx context.Context, client pb.JobSpecificationServiceClient, requests []*pb.UpdateJobSpecificationsRequest) []string {
	updatedJobs := []string{}
	for _, request := range requests {
		response, err := client.UpdateJobSpecifications(ctx, request)
		if err != nil {
			c.logger.Error(err.Error())
			continue
		}
		updatedJobs = append(updatedJobs, response.JobNameSuccesses...)
	}
	return updatedJobs
}

func (c *applyCommand) executeResourceDelete(ctx context.Context, client pb.ResourceServiceClient, requests []*pb.DeleteResourceRequest) []string {
	deletedResources := []string{}
	for _, request := range requests {
		_, err := client.DeleteResource(ctx, request)
		if err != nil {
			c.logger.Error(err.Error())
			continue
		}
		deletedResources = append(deletedResources, request.ResourceName)
	}
	return deletedResources
}

func (c *applyCommand) executeResourceAdd(ctx context.Context, client pb.ResourceServiceClient, requests []*pb.CreateResourceRequest) []string {
	addedResources := []string{}
	for _, request := range requests {
		response, err := client.CreateResource(ctx, request)
		if err != nil {
			c.logger.Error(err.Error())
			continue
		}
		if response.Success {
			addedResources = append(addedResources, request.Resource.Name)
		}
	}
	return addedResources
}

func (c *applyCommand) executeResourceUpdate(ctx context.Context, client pb.ResourceServiceClient, requests []*pb.UpdateResourceRequest) []string {
	updatedResources := []string{}
	for _, request := range requests {
		response, err := client.UpdateResource(ctx, request)
		if err != nil {
			c.logger.Error(err.Error())
			continue
		}
		if response.Success {
			updatedResources = append(updatedResources, request.Resource.Name)
		}
	}
	return updatedResources
}

func (c *applyCommand) getAddJobRequest(namespace *config.Namespace, plans plan.Plans) []*pb.AddJobSpecificationsRequest {
	jobsToBeSend := []*pb.JobSpecification{}
	for _, plan := range plans.GetByNamespaceName(namespace.Name).GetByKind(plan.KindJob).GetByOperation(plan.OperationCreate) {
		jobSpec, err := c.jobSpecReadWriter.ReadByName(".", plan.KindName)
		if err != nil {
			c.logger.Error(err.Error())
			continue
		}
		jobsToBeSend = append(jobsToBeSend, jobSpec.ToProto())
	}

	return []*pb.AddJobSpecificationsRequest{
		{
			ProjectName:   c.config.Project.Name,
			NamespaceName: namespace.Name,
			Specs:         jobsToBeSend,
		},
	}
}

func (c *applyCommand) getUpdateJobRequest(namespace *config.Namespace, plans plan.Plans) []*pb.UpdateJobSpecificationsRequest {
	jobsToBeSend := []*pb.JobSpecification{}
	for _, plan := range plans.GetByNamespaceName(namespace.Name).GetByKind(plan.KindJob).GetByOperation(plan.OperationUpdate) {
		jobSpec, err := c.jobSpecReadWriter.ReadByName(".", plan.KindName)
		if err != nil {
			c.logger.Error(err.Error())
			continue
		}
		jobsToBeSend = append(jobsToBeSend, jobSpec.ToProto())
	}

	return []*pb.UpdateJobSpecificationsRequest{
		{
			ProjectName:   c.config.Project.Name,
			NamespaceName: namespace.Name,
			Specs:         jobsToBeSend,
		},
	}
}

func (c *applyCommand) getDeleteJobRequest(projectName string, namespace *config.Namespace, plans plan.Plans) []*pb.DeleteJobSpecificationRequest {
	jobsToBeDeleted := []*pb.DeleteJobSpecificationRequest{}
	for _, plan := range plans.GetByNamespaceName(namespace.Name).GetByKind(plan.KindJob).GetByOperation(plan.OperationDelete) {
		jobSpec, err := c.jobSpecReadWriter.ReadByName(".", plan.KindName)
		if err != nil {
			c.logger.Error(err.Error())
			continue
		}
		jobsToBeDeleted = append(jobsToBeDeleted, &pb.DeleteJobSpecificationRequest{
			ProjectName:   projectName,
			NamespaceName: namespace.Name,
			JobName:       jobSpec.Name,
			CleanHistory:  false,
			Force:         false,
		})
	}
	return jobsToBeDeleted
}

func (c *applyCommand) getAddResourceRequest(projectName string, namespace *config.Namespace, plans plan.Plans) []*pb.CreateResourceRequest {
	resourcesToBeCreate := []*pb.CreateResourceRequest{}
	for _, plan := range plans.GetByNamespaceName(namespace.Name).GetByKind(plan.KindResource).GetByOperation(plan.OperationCreate) {
		for _, ds := range namespace.Datastore {
			resourceSpec, err := c.resourceSpecReadWriter.ReadByName(".", plan.KindName)
			if err != nil {
				c.logger.Error(err.Error())
				continue
			}
			resourceSpecProto, err := resourceSpec.ToProto()
			if err != nil {
				c.logger.Error(err.Error())
				continue
			}
			resourcesToBeCreate = append(resourcesToBeCreate, &pb.CreateResourceRequest{
				ProjectName:   projectName,
				NamespaceName: namespace.Name,
				DatastoreName: ds.Type,
				Resource:      resourceSpecProto,
			})
		}
	}
	return resourcesToBeCreate
}

func (c *applyCommand) getUpdateResourceRequest(projectName string, namespace *config.Namespace, plans plan.Plans) []*pb.UpdateResourceRequest {
	resourcesToBeUpdate := []*pb.UpdateResourceRequest{}
	for _, plan := range plans.GetByNamespaceName(namespace.Name).GetByKind(plan.KindResource).GetByOperation(plan.OperationUpdate) {
		for _, ds := range namespace.Datastore {
			resourceSpec, err := c.resourceSpecReadWriter.ReadByName(".", plan.KindName)
			if err != nil {
				c.logger.Error(err.Error())
				continue
			}
			resourceSpecProto, err := resourceSpec.ToProto()
			if err != nil {
				c.logger.Error(err.Error())
				continue
			}
			resourcesToBeUpdate = append(resourcesToBeUpdate, &pb.UpdateResourceRequest{
				ProjectName:   projectName,
				NamespaceName: namespace.Name,
				DatastoreName: ds.Type,
				Resource:      resourceSpecProto,
			})
		}
	}
	return resourcesToBeUpdate
}

func (c *applyCommand) getDeleteResourceRequest(projectName string, namespace *config.Namespace, plans plan.Plans) []*pb.DeleteResourceRequest {
	resourcesToBeDelete := []*pb.DeleteResourceRequest{}
	for _, plan := range plans.GetByNamespaceName(namespace.Name).GetByKind(plan.KindResource).GetByOperation(plan.OperationDelete) {
		for _, ds := range namespace.Datastore {
			resourceSpec, err := c.resourceSpecReadWriter.ReadByName(".", plan.KindName)
			if err != nil {
				c.logger.Error(err.Error())
				continue
			}
			resourcesToBeDelete = append(resourcesToBeDelete, &pb.DeleteResourceRequest{
				ProjectName:   projectName,
				NamespaceName: namespace.Name,
				DatastoreName: ds.Type,
				ResourceName:  resourceSpec.Name,
				Force:         false,
			})
		}
	}
	return resourcesToBeDelete
}

func (c *applyCommand) getPlans() (plan.Plans, error) {
	var plans plan.Plans = []*plan.Plan{}
	for _, source := range c.sources {
		f, err := os.OpenFile(source, os.O_RDONLY, os.ModePerm)
		if err != nil {
			f.Close()
			return nil, err
		}

		content, err := io.ReadAll(f)
		if err != nil {
			f.Close()
			return nil, err
		}
		f.Close()

		currentPlans := []*plan.Plan{}
		if err := json.Unmarshal(content, &currentPlans); err != nil {
			return nil, err
		}
		plans = append(plans, currentPlans...)
	}
	return plans, nil
}

func (c *applyCommand) savePlans(plans plan.Plans) error {
	f, err := os.OpenFile(c.output, os.O_RDWR|os.O_CREATE|os.O_TRUNC, os.ModePerm)
	if err != nil {
		return err
	}
	defer f.Close()
	raw, err := json.Marshal(plans)
	if err != nil {
		return err
	}
	_, err = f.Write(raw)
	return err
}
