package apply

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/goto/salt/log"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"

	"github.com/goto/optimus/client/cmd/internal"
	"github.com/goto/optimus/client/cmd/internal/connection"
	"github.com/goto/optimus/client/cmd/internal/logger"
	"github.com/goto/optimus/client/cmd/internal/plan"
	providermodel "github.com/goto/optimus/client/extension/model"
	"github.com/goto/optimus/client/extension/provider/github"
	"github.com/goto/optimus/client/extension/provider/gitlab"
	"github.com/goto/optimus/client/local"
	"github.com/goto/optimus/client/local/model"
	"github.com/goto/optimus/client/local/specio"
	"github.com/goto/optimus/config"
	"github.com/goto/optimus/internal/errors"
	pb "github.com/goto/optimus/protos/gotocompany/optimus/core/v1beta1"
)

const (
	applyTimeout     = time.Minute * 60
	executedPlanFile = "plan_apply.json"
)

type applyCommand struct {
	logger                 log.Logger
	connection             connection.Connection
	errors                 *errors.MultiError
	config                 *config.ClientConfig
	jobSpecReadWriter      local.SpecReadWriter[*model.JobSpec]
	resourceSpecReadWriter local.SpecReadWriter[*model.ResourceSpec]
	verbose                bool
	isOperationFail        bool
	withValidation         bool
	validation             applyCommandValidation

	configFilePath string
	sources        []string
	output         string
}

type applyCommandValidation struct {
	gitURL       string
	gitToken     string
	gitProvider  string
	gitProjectID string
	commitSHA    string
	commitAPI    providermodel.CommitAPI
}

func (c *applyCommand) initValidation() error {
	if !c.withValidation {
		return nil
	}

	var err error
	switch c.validation.gitProvider {
	case providermodel.ProviderGitHub:
		c.validation.commitAPI, err = github.NewAPI(c.validation.gitURL, c.validation.gitToken)
	case providermodel.ProviderGitLab:
		c.validation.commitAPI, err = gitlab.NewAPI(c.validation.gitURL, c.validation.gitToken)
	default:
		return fmt.Errorf("unsupported git provider: %s", c.validation.gitProvider)
	}

	return err
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
	cmd.Flags().BoolVar(&c.withValidation, "with-validation", false, "Determine whether to validate the plan before applying it")

	c.validation = applyCommandValidation{}
	cmd.Flags().StringVar(&c.validation.gitProvider, "git-provider", os.Getenv("GIT_PROVIDER"), "selected git provider used in the repository")
	cmd.Flags().StringVar(&c.validation.gitURL, "git-host", os.Getenv("GIT_HOST"), "Git host based on git provider used in the repository")
	cmd.Flags().StringVar(&c.validation.gitToken, "git-token", os.Getenv("GIT_TOKEN"), "Git token based on git provider used in the repository")
	cmd.Flags().StringVar(&c.validation.gitProjectID, "git-project-id", os.Getenv("GIT_PROJECT_ID"), "Determine which git project will be checked")
	cmd.Flags().StringVar(&c.validation.commitSHA, "commit-sha", os.Getenv("COMMIT_SHA"), "Current commit SHA to compare against latest commit")
}

func (c *applyCommand) PreRunE(_ *cobra.Command, _ []string) error {
	// Load config
	conf, err := internal.LoadOptionalConfig(c.configFilePath)
	if err != nil {
		return err
	}
	c.errors = errors.NewMultiError("ErrorsInApplyCommand")
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
	c.isOperationFail = false
	c.connection = connection.New(c.logger, conf)
	c.jobSpecReadWriter = jobSpecReadWriter
	c.resourceSpecReadWriter = resourceSpecReadWriter
	c.config = conf

	if c.withValidation {
		if err := c.initValidation(); err != nil {
			return fmt.Errorf("couldn't instantiate apply validation: %w", err)
		}
	}

	return nil
}

func (c *applyCommand) validateLatestCommit(ctx context.Context, path string) error {
	if !c.withValidation {
		return nil
	}

	latestCommit, err := c.validation.commitAPI.GetLatestCommitByPath(ctx, c.validation.gitProjectID, path)
	if err != nil {
		return err
	}

	isLatestCommit := latestCommit.SHA == c.validation.commitSHA
	if !isLatestCommit {
		return fmt.Errorf("commit validation failed for path %s: current commit [%s] is not the latest [%s]. Please use the latest commit to apply changes: %s",
			path, c.validation.commitSHA, latestCommit.SHA, latestCommit.URL)
	}
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
	plans := basePlans

	// prepare for proto request
	var (
		addJobRequest          = []*pb.AddJobSpecificationsRequest{}
		updateJobRequest       = []*pb.UpdateJobSpecificationsRequest{}
		deleteJobRequest       = []*pb.BulkDeleteJobsRequest_JobToDelete{}
		migrateJobRequest      = []*pb.ChangeJobNamespaceRequest{}
		addResourceRequest     = []*pb.CreateResourceRequest{}
		updateResourceRequest  = []*pb.UpdateResourceRequest{}
		deleteResourceRequest  = []*pb.DeleteResourceRequest{}
		migrateResourceRequest = []*pb.ChangeResourceNamespaceRequest{}
	)

	if len(plans.Job.GetAllNamespaces()) > 0 {
		c.logger.Info("🔄 Processing Jobs from the plan")
		for _, namespaceName := range plans.Job.GetAllNamespaces() {
			c.logger.Info("[%s]", namespaceName)
			namespace, err := c.config.GetNamespaceByName(namespaceName)
			if err != nil {
				c.logger.Error("\t├─ ❌ Failed to get namespace config for [%s]\n\t└─ err: %v", namespaceName, err)
				return fmt.Errorf("failed to get namespace config %s: %w", namespaceName, err)
			}

			migrateJobs, updateFromMigrateJobs := c.getMigrateJobRequest(namespace, plans)
			addJobRequest = append(addJobRequest, c.getAddJobRequest(namespace, plans)...)
			updateJobRequest = append(updateJobRequest, c.getUpdateJobRequest(ctx, namespace, plans)...)
			updateJobRequest = append(updateJobRequest, updateFromMigrateJobs...)
			deleteJobRequest = append(deleteJobRequest, c.getBulkDeleteJobsRequest(namespace, plans)...)
			migrateJobRequest = append(migrateJobRequest, migrateJobs...)
		}
		c.logger.Info("Finished processing jobs for apply")
		c.logger.Info(strings.Repeat("-", 60))
	}

	if len(plans.Resource.GetAllNamespaces()) > 0 {
		c.logger.Info("\n🔄 Processing Resources from the plan")
		for _, namespaceName := range plans.Resource.GetAllNamespaces() {
			c.logger.Info("[%s]...", namespaceName)
			namespace, err := c.config.GetNamespaceByName(namespaceName)
			if err != nil {
				c.logger.Error("\t├─ ❌ Failed to get namespace config for [%s]\n\t└─ err: %v", namespaceName, err)
				return fmt.Errorf("failed to get namespace config %s: %w", namespaceName, err)
			}

			migrateResources, updateFromMigrateResources := c.getMigrateResourceRequest(namespace, plans)
			addResourceRequest = append(addResourceRequest, c.getAddResourceRequest(namespace, plans)...)
			updateResourceRequest = append(updateResourceRequest, c.getUpdateResourceRequest(ctx, namespace, plans)...)
			updateResourceRequest = append(updateResourceRequest, updateFromMigrateResources...)
			deleteResourceRequest = append(deleteResourceRequest, c.getDeleteResourceRequest(namespace, plans)...)
			migrateResourceRequest = append(migrateResourceRequest, migrateResources...)
		}
		c.logger.Info("Finished processing resources for apply")
		c.logger.Info(strings.Repeat("-", 60))
	}

	c.logger.Info("")
	c.logger.Info(strings.Repeat("-", 60))
	c.logger.Info("🚀 Initiating the apply process! Buckle up, executing your plan now...")
	c.logger.Info(strings.Repeat("-", 60))
	c.logger.Info("")
	// send to server based on operation
	addedResources := c.executeResourceAdd(ctx, resourceClient, addResourceRequest)
	migratedResources := c.executeResourceMigrate(ctx, resourceClient, migrateResourceRequest)
	updatedResources := c.executeResourceUpdate(ctx, resourceClient, updateResourceRequest)

	addedJobs := c.executeJobAdd(ctx, jobClient, addJobRequest)
	migratedJobs := c.executeJobMigrate(ctx, jobClient, migrateJobRequest)
	updatedJobs := c.executeJobUpdate(ctx, jobClient, updateJobRequest)

	// job deletion < resource deletion
	deletedJobs := c.executeJobBulkDelete(ctx, jobClient, &pb.BulkDeleteJobsRequest{ProjectName: plans.ProjectName, Jobs: deleteJobRequest})
	deletedResources := c.executeResourceDelete(ctx, resourceClient, deleteResourceRequest)

	// update plan file, delete successful operations
	plans.Job.Delete = plans.Job.Delete.DeletePlansByNames(deletedJobs...)
	plans.Job.Create = plans.Job.Create.DeletePlansByNames(addedJobs...)
	plans.Job.Migrate = plans.Job.Migrate.DeletePlansByNames(migratedJobs...)
	plans.Job.Update = plans.Job.Update.DeletePlansByNames(updatedJobs...)
	plans.Resource.Delete = plans.Resource.Delete.DeletePlansByNames(deletedResources...)
	plans.Resource.Create = plans.Resource.Create.DeletePlansByNames(addedResources...)
	plans.Resource.Migrate = plans.Resource.Migrate.DeletePlansByNames(migratedResources...)
	plans.Resource.Update = plans.Resource.Update.DeletePlansByNames(updatedResources...)

	c.logger.Info(strings.Repeat("-", 60))
	if c.errors.ToErr() != nil {
		c.logger.Warn("⚠️ Apply phase finished! Some operations failed during apply phase.")
		c.logger.Info(strings.Repeat("-", 60))
		return errors.InternalError("ErrorsInApplyCommand", "encountered errors in apply flow", nil)
	}
	c.logger.Info("✅ Apply phase finished!")
	c.logger.Info(strings.Repeat("-", 60))

	if err := c.savePlans(plans); err != nil {
		return err
	}

	if c.isOperationFail {
		return fmt.Errorf("some operations couldn't be proceed")
	}

	return nil
}

func (c *applyCommand) printSuccess(namespaceName, operation, kind, name string) {
	c.logger.Info("[%s] %s: %s %s ✅\n", namespaceName, operation, kind, name)
}

func (c *applyCommand) printSuccessBulk(namespaceName, operation, kind string, names []string) {
	c.logger.Info("[%s] %s: %s ✅\n", namespaceName, operation, kind)
	for i, n := range names {
		if i == len(names)-1 {
			c.logger.Info("\t└─ %s\n", n)
		} else {
			c.logger.Info("\t├─ %s", n)
		}
	}
}

func (c *applyCommand) printFailedBulk(namespaceName, operation, kind string, names []string, cause string) { //nolint:unparam
	c.logger.Error("[%s] %s: %s ❌", namespaceName, operation, kind)
	for _, v := range names {
		c.logger.Error("\t├─ %s", v)
	}
	if c.verbose && cause != "" {
		c.logger.Error("\t└─ cause: %s\n", cause)
	}
	c.isOperationFail = true
}

func (c *applyCommand) printFailed(namespaceName, operation, kind, name, cause string) {
	c.logger.Error("[%s] %s: %s %s ❌", namespaceName, operation, kind, name)
	if c.verbose && cause != "" {
		c.logger.Error("\t└─ cause: %s\n", cause)
	}
	c.isOperationFail = true
}

func (c *applyCommand) printFailedAll(operation, kind string, jobNames []string, cause string) {
	c.logger.Error("[all] %s: %s %s ❌", operation, kind)
	for _, v := range jobNames {
		c.logger.Error("\t├─ %s", v)
	}
	if c.verbose && cause != "" {
		c.logger.Error("\t└─ cause: %s\n", cause)
	}
	c.isOperationFail = true
}

func (c *applyCommand) executeJobBulkDelete(ctx context.Context, client pb.JobSpecificationServiceClient, request *pb.BulkDeleteJobsRequest) []string {
	if len(request.Jobs) == 0 {
		return []string{}
	}

	response, err := client.BulkDeleteJobs(ctx, request)
	if err != nil {
		c.errors.Append(err)
		jobNameList := []string{}
		for _, spec := range request.Jobs {
			jobNameList = append(jobNameList, fmt.Sprintf("%s/%s", spec.GetNamespaceName(), spec.JobName))
		}
		c.printFailedAll("bulk-delete", "job", jobNameList, err.Error())
		return nil
	}

	// if no failure, check the status of each bulk deletion
	deletedJobs := []string{}
	for _, jobToDelete := range request.Jobs {
		result, found := response.ResultsByJobName[jobToDelete.JobName]
		if !found {
			continue
		}

		if result.GetSuccess() {
			c.printSuccess(jobToDelete.NamespaceName, "bulk-delete", "job", jobToDelete.JobName)
			deletedJobs = append(deletedJobs, jobToDelete.JobName)
		} else {
			c.errors.Append(errors.InternalError("ApplyCommand", "finished bulk without success", nil))
			c.printFailed(jobToDelete.NamespaceName, "bulk-delete", "job", jobToDelete.JobName, result.GetMessage())
		}
	}

	return deletedJobs
}

func (c *applyCommand) executeJobAdd(ctx context.Context, client pb.JobSpecificationServiceClient, requests []*pb.AddJobSpecificationsRequest) []string {
	addedJobs := []string{}
	for _, request := range requests {
		response, err := client.AddJobSpecifications(ctx, request)
		if err != nil {
			c.errors.Append(err)
			jobNameList := []string{}
			for _, spec := range request.GetSpecs() {
				jobNameList = append(jobNameList, spec.GetName())
			}
			if c.verbose {
				c.printFailedBulk(request.NamespaceName, "create", "job", jobNameList, err.Error())
			} else {
				c.printFailedBulk(request.NamespaceName, "create", "job", jobNameList, "No jobs added, add verbose for more details")
			}
			continue
		}
		isJobSuccess := map[string]bool{}
		for _, jobName := range response.GetSuccessfulJobNames() {
			isJobSuccess[jobName] = true
		}
		if len(response.GetSuccessfulJobNames()) == 0 {
			c.printSuccessBulk(request.NamespaceName, "create", "job", response.GetSuccessfulJobNames())
		}

		failedJobNameList := []string{}
		for _, spec := range request.GetSpecs() {
			if _, ok := isJobSuccess[spec.GetName()]; !ok {
				c.errors.Append(errors.InternalError("ApplyCommand", "", nil))
				failedJobNameList = append(failedJobNameList, spec.GetName())
			}
		}
		if c.verbose {
			c.printFailedBulk(request.NamespaceName, "create", "job", failedJobNameList, response.GetLog())
		} else {
			c.printFailedBulk(request.NamespaceName, "create", "job", failedJobNameList, "add verbose for more details")
		}
		addedJobs = append(addedJobs, response.GetSuccessfulJobNames()...)
	}
	return addedJobs
}

func (c *applyCommand) executeJobMigrate(ctx context.Context, client pb.JobSpecificationServiceClient, requests []*pb.ChangeJobNamespaceRequest) []string {
	migratedJobs := []string{}
	for _, request := range requests {
		_, err := client.ChangeJobNamespace(ctx, request)
		if err != nil {
			c.errors.Append(err)
			c.printFailed(request.NamespaceName, "migrate", "job", request.GetJobName(), err.Error())
			continue
		}
		c.printSuccess(request.NamespaceName, "migrate", "job", request.GetJobName())
		migratedJobs = append(migratedJobs, request.GetJobName())
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
			c.errors.Append(err)
			jobNameList := []string{}
			for _, spec := range request.GetSpecs() {
				jobNameList = append(jobNameList, spec.GetName())
			}
			if c.verbose {
				c.printFailedBulk(request.NamespaceName, "update", "job", jobNameList, err.Error())
			} else {
				c.printFailedBulk(request.NamespaceName, "update", "job", jobNameList, "No jobs updated, add verbose for more details")
			}
			continue
		}
		isJobSuccess := map[string]bool{}
		for _, jobName := range response.GetSuccessfulJobNames() {
			isJobSuccess[jobName] = true
		}
		if len(response.GetSuccessfulJobNames()) == 0 {
			c.printSuccessBulk(request.NamespaceName, "update", "job", response.GetSuccessfulJobNames())
		}

		failedJobNameList := []string{}
		for _, spec := range request.GetSpecs() {
			if _, ok := isJobSuccess[spec.GetName()]; !ok {
				c.errors.Append(errors.InternalError("ApplyCommand", "", nil))
				failedJobNameList = append(failedJobNameList, spec.GetName())
			}
		}
		if c.verbose {
			c.printFailedBulk(request.NamespaceName, "update", "job", failedJobNameList, response.GetLog())
		} else {
			c.printFailedBulk(request.NamespaceName, "update", "job", failedJobNameList, "add verbose for more details")
		}
		updatedJobs = append(updatedJobs, response.GetSuccessfulJobNames()...)
	}
	return updatedJobs
}

func (c *applyCommand) executeResourceDelete(ctx context.Context, client pb.ResourceServiceClient, requests []*pb.DeleteResourceRequest) []string {
	deletedResources := []string{}
	for _, request := range requests {
		_, err := client.DeleteResource(ctx, request)
		resourceName := plan.ConstructResourceName(request.DatastoreName, request.GetResourceName())
		if err != nil {
			c.errors.Append(err)
			c.printFailed(request.NamespaceName, "delete", "resource", resourceName, err.Error())
			continue
		}
		c.printSuccess(request.NamespaceName, "delete", "resource", resourceName)
		deletedResources = append(deletedResources, resourceName)
	}
	return deletedResources
}

func (c *applyCommand) executeResourceAdd(ctx context.Context, client pb.ResourceServiceClient, requests []*pb.CreateResourceRequest) []string {
	addedResources := []string{}
	for _, request := range requests {
		_, err := client.CreateResource(ctx, request)
		resourceName := plan.ConstructResourceName(request.DatastoreName, request.GetResource().GetName())
		if err != nil {
			c.errors.Append(err)
			c.printFailed(request.NamespaceName, "add", "resource", resourceName, err.Error())
			continue
		}
		c.printSuccess(request.NamespaceName, "add", "resource", resourceName)
		addedResources = append(addedResources, resourceName)
	}
	return addedResources
}

func (c *applyCommand) executeResourceMigrate(ctx context.Context, client pb.ResourceServiceClient, requests []*pb.ChangeResourceNamespaceRequest) []string {
	migratedResources := []string{}
	for _, request := range requests {
		_, err := client.ChangeResourceNamespace(ctx, request)
		resourceName := plan.ConstructResourceName(request.DatastoreName, request.GetResourceName())
		if err != nil {
			c.errors.Append(err)
			c.printFailed(request.NamespaceName, "migrate", "resource", resourceName, err.Error())
			continue
		}
		c.printSuccess(request.NamespaceName, "migrate", "resource", resourceName)
		migratedResources = append(migratedResources, resourceName)
	}
	return migratedResources
}

func (c *applyCommand) executeResourceUpdate(ctx context.Context, client pb.ResourceServiceClient, requests []*pb.UpdateResourceRequest) []string {
	updatedResources := []string{}
	for _, request := range requests {
		_, err := client.UpdateResource(ctx, request)
		resourceName := plan.ConstructResourceName(request.DatastoreName, request.GetResource().GetName())
		if err != nil {
			if strings.Contains(err.Error(), "[create_failure]") {
				c.logger.Warn("[%s] %s: update %s ⚠️, \n\tReceived an update request for resource %s.\n\tThis resource is in 'create_failure' state on the server.\n\tAttempting to re-create the resource instead",
					request.NamespaceName, "resource", resourceName, resourceName)
				addResourceRequest := convertUpdateResourceRequestToAdd(request)
				c.executeResourceAdd(ctx, client, []*pb.CreateResourceRequest{addResourceRequest})
				continue
			}
			if strings.Contains(err.Error(), "Not Found") {
				c.logger.Warn("[%s] %s: update %s ⚠️, \n\tReceived an update request for resource %s.\n\tThis resource does not exist on the server.\n\tAttempting to Create the resource instead",
					request.NamespaceName, "resource", resourceName, resourceName)
				addResourceRequest := convertUpdateResourceRequestToAdd(request)
				c.executeResourceAdd(ctx, client, []*pb.CreateResourceRequest{addResourceRequest})
				continue
			}

			c.errors.Append(err)
			c.printFailed(request.NamespaceName, "update", "resource", resourceName, err.Error())
			continue
		}
		c.printSuccess(request.NamespaceName, "update", "resource", resourceName)
		updatedResources = append(updatedResources, resourceName)
	}
	return updatedResources
}

func (c *applyCommand) getAddJobRequest(namespace *config.Namespace, plans plan.Plan) []*pb.AddJobSpecificationsRequest {
	jobsToBeSend := []*pb.JobSpecification{}
	for _, currentPlan := range plans.Job.Create.GetByNamespace(namespace.Name) {
		jobSpec, err := c.jobSpecReadWriter.ReadByDirPath(currentPlan.Path)
		if err != nil {
			c.logger.Error("\t├─ ❌ Read Job Spec Failed for Path:[%s]\n\t└─ err: %v", currentPlan.Path, err)
			c.errors.Append(err)
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

func convertUpdateResourceRequestToAdd(req *pb.UpdateResourceRequest) *pb.CreateResourceRequest {
	return &pb.CreateResourceRequest{
		ProjectName:   req.ProjectName,
		DatastoreName: req.DatastoreName,
		Resource:      req.Resource,
		NamespaceName: req.NamespaceName,
	}
}

func (c *applyCommand) getUpdateJobRequest(ctx context.Context, namespace *config.Namespace, plans plan.Plan) []*pb.UpdateJobSpecificationsRequest {
	jobsToBeSend := []*pb.JobSpecification{}

	for _, currentPlan := range plans.Job.Update.GetByNamespace(namespace.Name) {
		c.logger.Info("\t├─ ⏳ Validate Job for Update: %s", currentPlan.Name)
		jobSpec, err := c.jobSpecReadWriter.ReadByDirPath(currentPlan.Path)
		if err != nil {
			c.logger.Error("\t├─ ❌ Read Job Spec Failed for Path:[%s]\n\t└─ err: %v", currentPlan.Path, err)
			c.errors.Append(err)
			continue
		}

		if err := c.validateLatestCommit(ctx, currentPlan.Path); err != nil {
			c.logger.Error("\t└─ ❌ Commit freshness Validation Failed: %v", err)
			c.errors.Append(err)
			continue
		}

		c.logger.Info("\t└─ ✅ Valid spec and latest commit")

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

func (*applyCommand) getBulkDeleteJobsRequest(namespace *config.Namespace, plans plan.Plan) []*pb.BulkDeleteJobsRequest_JobToDelete {
	jobsToDelete := []*pb.BulkDeleteJobsRequest_JobToDelete{}
	for _, currentPlan := range plans.Job.Delete.GetByNamespace(namespace.Name) {
		jobsToDelete = append(jobsToDelete, &pb.BulkDeleteJobsRequest_JobToDelete{
			NamespaceName: namespace.Name,
			JobName:       currentPlan.Name,
		})
	}
	return jobsToDelete
}

func (c *applyCommand) getMigrateJobRequest(namespace *config.Namespace, plans plan.Plan) ([]*pb.ChangeJobNamespaceRequest, []*pb.UpdateJobSpecificationsRequest) {
	// after migration is done, update should be performed on new namespace
	jobsToBeMigrated := []*pb.ChangeJobNamespaceRequest{}
	jobsToBeUpdated := []*pb.JobSpecification{}

	for _, currentPlan := range plans.Job.Migrate.GetByNamespace(namespace.Name) {
		jobSpec, err := c.jobSpecReadWriter.ReadByDirPath(currentPlan.Path)
		if err != nil {
			c.logger.Error("\t├─ ❌ Read Job Spec Failed for Path:[%s]\n\t└─ err: %v", currentPlan.Path, err)
			c.errors.Append(err)
			continue
		}
		if currentPlan.OldNamespace == nil {
			c.logger.Error("\t├─ ❌ Old Namespace is nil for Job [%s]\n\t└─ err: %v", jobSpec.Name, err)
			c.errors.Append(err)
			continue
		}
		jobsToBeMigrated = append(jobsToBeMigrated, &pb.ChangeJobNamespaceRequest{
			ProjectName:      c.config.Project.Name,
			JobName:          jobSpec.Name,
			NamespaceName:    *currentPlan.OldNamespace,
			NewNamespaceName: namespace.Name,
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

func (c *applyCommand) getAddResourceRequest(namespace *config.Namespace, plans plan.Plan) []*pb.CreateResourceRequest {
	resourcesToBeCreate := []*pb.CreateResourceRequest{}
	for _, currentPlan := range plans.Resource.Create.GetByNamespace(namespace.Name) {
		resourceSpec, err := c.resourceSpecReadWriter.ReadByDirPath(currentPlan.Path)
		if err != nil {
			c.logger.Error("\t├─ ❌ Read Resource Spec Failed for Path:[%s]\n\t└─ err: %v", currentPlan.Path, err)
			c.errors.Append(err)
			continue
		}
		resourceSpecProto, err := resourceSpec.ToProto()
		if err != nil {
			c.logger.Error("\t└─ ❌ Convert Resource Spec to Proto Failed: %v", err)
			c.errors.Append(err)
			continue
		}
		resourcesToBeCreate = append(resourcesToBeCreate, &pb.CreateResourceRequest{
			ProjectName:   c.config.Project.Name,
			NamespaceName: namespace.Name,
			DatastoreName: currentPlan.Datastore,
			Resource:      resourceSpecProto,
		})
	}
	return resourcesToBeCreate
}

func (c *applyCommand) getUpdateResourceRequest(ctx context.Context, namespace *config.Namespace, plans plan.Plan) []*pb.UpdateResourceRequest {
	resourcesToBeUpdate := []*pb.UpdateResourceRequest{}
	for _, currentPlan := range plans.Resource.Update.GetByNamespace(namespace.Name) {
		c.logger.Info("\t└─ ⏳ Validate Resource for Update: %s", currentPlan.Name)

		resourceSpec, err := c.resourceSpecReadWriter.ReadByDirPath(currentPlan.Path)
		if err != nil {
			c.logger.Error("\t├─ ❌ Read Resource Spec Failed for Path:[%s]\n\t└─ err: %v", currentPlan.Path, err)
			c.errors.Append(err)
			continue
		}
		resourceSpecProto, err := resourceSpec.ToProto()
		if err != nil {
			c.logger.Error("\t└─ ❌ Convert Resource Spec to Proto Failed: %v", err)
			c.errors.Append(err)
			continue
		}
		if err := c.validateLatestCommit(ctx, currentPlan.Path); err != nil {
			c.logger.Error("\t└─ ❌ Commit freshness Validation Failed: %v", err)
			c.errors.Append(err)
			continue
		}
		c.logger.Info("\t└─ ✅ Valid spec and latest commit")

		resourcesToBeUpdate = append(resourcesToBeUpdate, &pb.UpdateResourceRequest{
			ProjectName:   c.config.Project.Name,
			NamespaceName: namespace.Name,
			DatastoreName: currentPlan.Datastore,
			Resource:      resourceSpecProto,
		})
	}

	return resourcesToBeUpdate
}

func (c *applyCommand) getDeleteResourceRequest(namespace *config.Namespace, plans plan.Plan) []*pb.DeleteResourceRequest {
	resourcesToBeDelete := []*pb.DeleteResourceRequest{}
	for _, currentPlan := range plans.Resource.Delete.GetByNamespace(namespace.Name) {
		resourcesToBeDelete = append(resourcesToBeDelete, &pb.DeleteResourceRequest{
			ProjectName:   c.config.Project.Name,
			NamespaceName: namespace.Name,
			DatastoreName: currentPlan.Datastore,
			ResourceName:  currentPlan.Name,
			Force:         false,
		})
	}
	return resourcesToBeDelete
}

func (c *applyCommand) getMigrateResourceRequest(namespace *config.Namespace, plans plan.Plan) ([]*pb.ChangeResourceNamespaceRequest, []*pb.UpdateResourceRequest) {
	// after migration is done, update should be performed on new namespace
	resourcesToBeMigrated := []*pb.ChangeResourceNamespaceRequest{}
	resourcesToBeUpdated := []*pb.UpdateResourceRequest{}

	for _, currentPlan := range plans.Resource.Migrate.GetByNamespace(namespace.Name) {
		resourceSpec, err := c.resourceSpecReadWriter.ReadByDirPath(currentPlan.Path)
		if err != nil {
			c.logger.Error("\t├─ ❌ Read Resource Spec Failed for Path:[%s]\n\t└─ err: %v", currentPlan.Path, err)
			c.errors.Append(err)
			continue
		}
		if currentPlan.OldNamespace == nil {
			c.logger.Error("\t├─ ❌ Old Namespace is nil for Resource [%s]\n\t└─ err: %v", resourceSpec.Name, err)
			c.errors.Append(err)
			continue
		}
		resourceSpecProto, err := resourceSpec.ToProto()
		if err != nil {
			c.logger.Error("\t├─ ❌ Convert Resource Spec to Proto Failed for Resource [%s]\n\t└─ err: %v", resourceSpec.Name, err)
			c.errors.Append(err)
			continue
		}
		resourcesToBeMigrated = append(resourcesToBeMigrated, &pb.ChangeResourceNamespaceRequest{
			ProjectName:      c.config.Project.Name,
			NamespaceName:    *currentPlan.OldNamespace,
			NewNamespaceName: namespace.Name,
			ResourceName:     resourceSpec.Name,
			DatastoreName:    currentPlan.Datastore,
		})
		resourcesToBeUpdated = append(resourcesToBeUpdated, &pb.UpdateResourceRequest{
			ProjectName:   c.config.Project.Name,
			NamespaceName: namespace.Name,
			Resource:      resourceSpecProto,
			DatastoreName: currentPlan.Datastore,
		})
	}

	return resourcesToBeMigrated, resourcesToBeUpdated
}

func (c *applyCommand) getPlans() (plan.Plan, error) {
	var plans plan.Plan
	for _, source := range c.sources {
		f, err := os.OpenFile(source, os.O_RDONLY, os.ModePerm)
		if err != nil {
			f.Close()
			return plans, err
		}

		content, err := io.ReadAll(f)
		if err != nil {
			f.Close()
			return plans, err
		}
		f.Close()

		currentPlans := plan.Plan{}
		if err := json.Unmarshal(content, &currentPlans); err != nil {
			return plans, err
		}
		if plans.IsEmpty() {
			plans = plan.NewPlan(currentPlans.ProjectName)
		}
		plans = plans.Merge(currentPlans)
	}
	if c.verbose {
		c.logger.Info("Plans loaded from sources: %s", strings.Join(c.sources, ", "))
		c.logger.Info("Loaded plans: \n%s", plans.String())
	}
	return plans, nil
}

func (c *applyCommand) savePlans(plans plan.Plan) error {
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
