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
	verbose                bool

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
	cmd.Flags().BoolVarP(&c.verbose, "verbose", "v", false, "Determines whether to show the complete message or just the summary")
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
	plans = plans.GetByExecuted(false)

	// prepare for proto request
	var (
		addJobRequest          = []*pb.AddJobSpecificationsRequest{}
		updateJobRequest       = []*pb.UpdateJobSpecificationsRequest{}
		deleteJobRequest       = []*pb.DeleteJobSpecificationRequest{}
		migrateJobRequest      = []*pb.ChangeJobNamespaceRequest{}
		addResourceRequest     = []*pb.CreateResourceRequest{}
		updateResourceRequest  = []*pb.UpdateResourceRequest{}
		deleteResourceRequest  = []*pb.DeleteResourceRequest{}
		migrateResourceRequest = []*pb.ChangeResourceNamespaceRequest{}
	)

	for _, namespace := range c.config.Namespaces {
		// job request preparation
		migrateJobs, updateFromMigrateJobs := c.getMigrateJobRequest(namespace, plans)
		addJobRequest = append(addJobRequest, c.getAddJobRequest(namespace, plans)...)
		updateJobRequest = append(updateJobRequest, c.getUpdateJobRequest(namespace, plans)...)
		updateJobRequest = append(updateJobRequest, updateFromMigrateJobs...)
		deleteJobRequest = append(deleteJobRequest, c.getDeleteJobRequest(namespace, plans)...)
		migrateJobRequest = append(migrateJobRequest, migrateJobs...)
		// resource request preparation
		migrateResources, updateFromMigrateResources := c.getMigrateResourceRequest(namespace, plans)
		addResourceRequest = append(addResourceRequest, c.getAddResourceRequest(namespace, plans)...)
		updateResourceRequest = append(updateResourceRequest, c.getUpdateResourceRequest(namespace, plans)...)
		updateResourceRequest = append(updateResourceRequest, updateFromMigrateResources...)
		deleteResourceRequest = append(deleteResourceRequest, c.getDeleteResourceRequest(namespace, plans)...)
		migrateResourceRequest = append(migrateResourceRequest, migrateResources...)
	}

	// send to server based on operation
	deletedJobs := c.executeJobDelete(ctx, jobClient, deleteJobRequest)
	addedJobs := c.executeJobAdd(ctx, jobClient, addJobRequest)
	migratedJobs := c.executeJobMigrate(ctx, jobClient, migrateJobRequest)
	updatedJobs := c.executeJobUpdate(ctx, jobClient, updateJobRequest)
	deletedResources := c.executeResourceDelete(ctx, resourceClient, deleteResourceRequest)
	addedResources := c.executeResourceAdd(ctx, resourceClient, addResourceRequest)
	migratedResources := c.executeResourceMigrate(ctx, resourceClient, migrateResourceRequest)
	updatedResources := c.executeResourceUpdate(ctx, resourceClient, updateResourceRequest)

	// update plan file
	isExecuted := true
	plans = plans.UpdateExecutedByNames(isExecuted, plan.KindJob, deletedJobs...)
	plans = plans.UpdateExecutedByNames(isExecuted, plan.KindJob, addedJobs...)
	plans = plans.UpdateExecutedByNames(isExecuted, plan.KindJob, migratedJobs...)
	plans = plans.UpdateExecutedByNames(isExecuted, plan.KindJob, updatedJobs...)
	plans = plans.UpdateExecutedByNames(isExecuted, plan.KindResource, deletedResources...)
	plans = plans.UpdateExecutedByNames(isExecuted, plan.KindResource, addedResources...)
	plans = plans.UpdateExecutedByNames(isExecuted, plan.KindResource, migratedResources...)
	plans = plans.UpdateExecutedByNames(isExecuted, plan.KindResource, updatedResources...)

	if err := c.savePlans(plans); err != nil {
		return err
	}

	isNotExecuted := false
	for _, plan := range plans {
		if plan.Executed {
			c.logger.Info("✅ [%s] %s %s from namespace %s", plan.Operation, plan.Kind, plan.KindName, plan.NamespaceName)
		} else {
			isNotExecuted = true
			c.logger.Error("❌ [%s] %s %s from namespace %s", plan.Operation, plan.Kind, plan.KindName, plan.NamespaceName)
		}
	}

	if isNotExecuted {
		return fmt.Errorf("some operations couldn't be proceed")
	}

	return nil
}

func (c *applyCommand) executeJobDelete(ctx context.Context, client pb.JobSpecificationServiceClient, requests []*pb.DeleteJobSpecificationRequest) []string {
	deletedJobs := []string{}
	for _, request := range requests {
		response, err := client.DeleteJobSpecification(ctx, request)
		if c.verbose {
			c.logger.Info(response.GetMessage())
		}
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
		if c.verbose {
			c.logger.Info(response.GetLog())
		}
		if err != nil {
			c.logger.Error(err.Error())
			continue
		}
		addedJobs = append(addedJobs, response.JobNameSuccesses...)
	}
	return addedJobs
}

func (c *applyCommand) executeJobMigrate(ctx context.Context, client pb.JobSpecificationServiceClient, requests []*pb.ChangeJobNamespaceRequest) []string {
	migratedJobs := []string{}
	for _, request := range requests {
		response, err := client.ChangeJobNamespace(ctx, request)
		if err != nil {
			c.logger.Error(err.Error())
			continue
		}
		if c.verbose {
			c.logger.Info("job %s successfully migrated", request.GetJobName())
		}
		if response.Success {
			migratedJobs = append(migratedJobs, request.JobName)
		}
	}
	return migratedJobs
}

func (c *applyCommand) executeJobUpdate(ctx context.Context, client pb.JobSpecificationServiceClient, requests []*pb.UpdateJobSpecificationsRequest) []string {
	updatedJobs := []string{}
	for _, request := range requests {
		response, err := client.UpdateJobSpecifications(ctx, request)
		if c.verbose {
			c.logger.Info(response.GetLog())
		}
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
		if c.verbose {
			c.logger.Info("resource %s successfully deleted", request.ResourceName)
		}
		deletedResources = append(deletedResources, request.ResourceName)
	}
	return deletedResources
}

func (c *applyCommand) executeResourceAdd(ctx context.Context, client pb.ResourceServiceClient, requests []*pb.CreateResourceRequest) []string {
	addedResources := []string{}
	for _, request := range requests {
		response, err := client.CreateResource(ctx, request)
		if c.verbose {
			c.logger.Info(response.GetMessage())
		}
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

func (c *applyCommand) executeResourceMigrate(ctx context.Context, client pb.ResourceServiceClient, requests []*pb.ChangeResourceNamespaceRequest) []string {
	migratedResources := []string{}
	for _, request := range requests {
		response, err := client.ChangeResourceNamespace(ctx, request)
		if err != nil {
			c.logger.Error(err.Error())
			continue
		}
		if c.verbose {
			c.logger.Info("resource %s successfully migrated", request.GetResourceName())
		}
		if response.Success {
			migratedResources = append(migratedResources, request.ResourceName)
		}
	}
	return migratedResources
}

func (c *applyCommand) executeResourceUpdate(ctx context.Context, client pb.ResourceServiceClient, requests []*pb.UpdateResourceRequest) []string {
	updatedResources := []string{}
	for _, request := range requests {
		response, err := client.UpdateResource(ctx, request)
		if c.verbose {
			c.logger.Info(response.GetMessage())
		}
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

	if len(jobsToBeSend) == 0 {
		return []*pb.AddJobSpecificationsRequest{}
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

	if len(jobsToBeSend) == 0 {
		return []*pb.UpdateJobSpecificationsRequest{}
	}

	return []*pb.UpdateJobSpecificationsRequest{
		{
			ProjectName:   c.config.Project.Name,
			NamespaceName: namespace.Name,
			Specs:         jobsToBeSend,
		},
	}
}

func (c *applyCommand) getDeleteJobRequest(namespace *config.Namespace, plans plan.Plans) []*pb.DeleteJobSpecificationRequest {
	jobsToBeDeleted := []*pb.DeleteJobSpecificationRequest{}
	for _, plan := range plans.GetByNamespaceName(namespace.Name).GetByKind(plan.KindJob).GetByOperation(plan.OperationDelete) {
		jobSpec, err := c.jobSpecReadWriter.ReadByName(".", plan.KindName)
		if err != nil {
			c.logger.Error(err.Error())
			continue
		}
		jobsToBeDeleted = append(jobsToBeDeleted, &pb.DeleteJobSpecificationRequest{
			ProjectName:   c.config.Project.Name,
			NamespaceName: namespace.Name,
			JobName:       jobSpec.Name,
			CleanHistory:  false,
			Force:         false,
		})
	}
	return jobsToBeDeleted
}

func (c *applyCommand) getMigrateJobRequest(namespace *config.Namespace, plans plan.Plans) ([]*pb.ChangeJobNamespaceRequest, []*pb.UpdateJobSpecificationsRequest) {
	// after migration is done, update should be performed on new namespace
	jobsToBeMigrated := []*pb.ChangeJobNamespaceRequest{}
	jobsToBeUpdated := []*pb.JobSpecification{}

	for _, plan := range plans.GetByNamespaceName(namespace.Name).GetByKind(plan.KindJob).GetByOperation(plan.OperationMigrate) {
		jobSpec, err := c.jobSpecReadWriter.ReadByName(".", plan.KindName)
		if err != nil {
			c.logger.Error(err.Error())
			continue
		}
		if plan.OldNamespaceName == nil {
			c.logger.Error(fmt.Sprintf("old namespace on job %s could not be nil", jobSpec.Name))
			continue
		}
		jobsToBeMigrated = append(jobsToBeMigrated, &pb.ChangeJobNamespaceRequest{
			ProjectName:      c.config.Project.Name,
			JobName:          jobSpec.Name,
			NamespaceName:    *plan.OldNamespaceName,
			NewNamespaceName: plan.NamespaceName,
		})
		jobsToBeUpdated = append(jobsToBeUpdated, jobSpec.ToProto())
	}

	jobsToBeUpdatedRequest := []*pb.UpdateJobSpecificationsRequest{}
	if len(jobsToBeUpdated) > 0 {
		jobsToBeUpdatedRequest = append(jobsToBeUpdatedRequest, &pb.UpdateJobSpecificationsRequest{
			ProjectName:   c.config.Project.Name,
			NamespaceName: namespace.Name,
			Specs:         jobsToBeUpdated,
		})
	}
	return jobsToBeMigrated, jobsToBeUpdatedRequest
}

func (c *applyCommand) getAddResourceRequest(namespace *config.Namespace, plans plan.Plans) []*pb.CreateResourceRequest {
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
				ProjectName:   c.config.Project.Name,
				NamespaceName: namespace.Name,
				DatastoreName: ds.Type,
				Resource:      resourceSpecProto,
			})
		}
	}
	return resourcesToBeCreate
}

func (c *applyCommand) getUpdateResourceRequest(namespace *config.Namespace, plans plan.Plans) []*pb.UpdateResourceRequest {
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
				ProjectName:   c.config.Project.Name,
				NamespaceName: namespace.Name,
				DatastoreName: ds.Type,
				Resource:      resourceSpecProto,
			})
		}
	}
	return resourcesToBeUpdate
}

func (c *applyCommand) getDeleteResourceRequest(namespace *config.Namespace, plans plan.Plans) []*pb.DeleteResourceRequest {
	resourcesToBeDelete := []*pb.DeleteResourceRequest{}
	for _, plan := range plans.GetByNamespaceName(namespace.Name).GetByKind(plan.KindResource).GetByOperation(plan.OperationDelete) {
		for _, ds := range namespace.Datastore {
			resourceSpec, err := c.resourceSpecReadWriter.ReadByName(".", plan.KindName)
			if err != nil {
				c.logger.Error(err.Error())
				continue
			}
			resourcesToBeDelete = append(resourcesToBeDelete, &pb.DeleteResourceRequest{
				ProjectName:   c.config.Project.Name,
				NamespaceName: namespace.Name,
				DatastoreName: ds.Type,
				ResourceName:  resourceSpec.Name,
				Force:         false,
			})
		}
	}
	return resourcesToBeDelete
}

func (c *applyCommand) getMigrateResourceRequest(namespace *config.Namespace, plans plan.Plans) ([]*pb.ChangeResourceNamespaceRequest, []*pb.UpdateResourceRequest) {
	// after migration is done, update should be performed on new namespace
	resourcesToBeMigrated := []*pb.ChangeResourceNamespaceRequest{}
	resourcesToBeUpdated := []*pb.UpdateResourceRequest{}

	for _, plan := range plans.GetByNamespaceName(namespace.Name).GetByKind(plan.KindResource).GetByOperation(plan.OperationMigrate) {
		resourceSpec, err := c.resourceSpecReadWriter.ReadByName(".", plan.KindName)
		if err != nil {
			c.logger.Error(err.Error())
			continue
		}
		if plan.OldNamespaceName == nil {
			c.logger.Error(fmt.Sprintf("old namespace on resource %s could not be nil", resourceSpec.Name))
			continue
		}
		resourceSpecProto, err := resourceSpec.ToProto()
		if err != nil {
			c.logger.Error(fmt.Sprintf("resource %s could not be converted to proto", resourceSpec.Name))
			continue
		}
		resourcesToBeMigrated = append(resourcesToBeMigrated, &pb.ChangeResourceNamespaceRequest{
			ProjectName:      c.config.Project.Name,
			NamespaceName:    *plan.OldNamespaceName,
			NewNamespaceName: plan.NamespaceName,
			ResourceName:     resourceSpec.Name,
			DatastoreName:    resourceSpec.Type,
		})
		resourcesToBeUpdated = append(resourcesToBeUpdated, &pb.UpdateResourceRequest{
			ProjectName:   c.config.Project.Name,
			NamespaceName: plan.NamespaceName,
			Resource:      resourceSpecProto,
			DatastoreName: resourceSpec.Type,
		})
	}

	return resourcesToBeMigrated, resourcesToBeUpdated
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
