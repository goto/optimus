package resource

import (
	"context"
	"fmt"
	"time"

	"github.com/MakeNowJust/heredoc"
	"github.com/goto/salt/log"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"

	"github.com/goto/optimus/client/cmd/internal/connection"
	"github.com/goto/optimus/client/cmd/internal/logger"
	"github.com/goto/optimus/client/local"
	"github.com/goto/optimus/client/local/model"
	"github.com/goto/optimus/client/local/specio"
	"github.com/goto/optimus/config"
	"github.com/goto/optimus/core/resource"
	pb "github.com/goto/optimus/protos/gotocompany/optimus/core/v1beta1"
)

const (
	uploadTimeout    = time.Minute * 30
	defaultBatchSize = 10
)

type uploadCommand struct {
	logger     log.Logger
	connection connection.Connection

	clientConfig   *config.ClientConfig
	configFilePath string

	namespaceName string
	resourceNames []string
	batchSize     int

	resourceSpecReadWriter local.SpecReadWriter[*model.ResourceSpec]
}

// NewUploadCommand initializes command for uploading a single resource
func NewUploadCommand() *cobra.Command {
	uploadCmd := &uploadCommand{
		logger: logger.NewClientLogger(),
	}

	cmd := &cobra.Command{
		Use:     "upload",
		Short:   "Upload a resource to server",
		Long:    heredoc.Doc(`Apply local changes to destination server which includes creating/updating resources`),
		Example: "optimus resource upload -R [resource1,resource2] -n []",
		Annotations: map[string]string{
			"group:core": "true",
		},
		RunE:    uploadCmd.RunE,
		PreRunE: uploadCmd.PreRunE,
	}
	cmd.Flags().StringVarP(&uploadCmd.configFilePath, "config", "c", uploadCmd.configFilePath, "File path for client configuration")
	cmd.Flags().StringVarP(&uploadCmd.namespaceName, "namespace", "n", "", "Namespace name in which the resource resides")
	cmd.Flags().StringSliceVarP(&uploadCmd.resourceNames, "resources", "R", nil, "Resource names")
	cmd.Flags().IntVarP(&uploadCmd.batchSize, "batch-size", "b", defaultBatchSize, "Number of resources to upload in a batch")

	cmd.MarkFlagRequired("namespace")
	return cmd
}

func (u *uploadCommand) PreRunE(_ *cobra.Command, _ []string) error {
	var err error
	u.clientConfig, err = config.LoadClientConfig(u.configFilePath)
	if err != nil {
		return err
	}
	u.connection = connection.New(u.logger, u.clientConfig)

	resourceSpecReadWriter, err := specio.NewResourceSpecReadWriter(afero.NewOsFs())
	if err != nil {
		return fmt.Errorf("couldn't instantiate resource spec reader")
	}
	u.resourceSpecReadWriter = resourceSpecReadWriter

	return nil
}

func (u *uploadCommand) RunE(_ *cobra.Command, _ []string) error {
	namespace, err := u.clientConfig.GetNamespaceByName(u.namespaceName)
	if err != nil {
		return err
	}

	return u.upload(namespace)
}

func (u *uploadCommand) upload(namespace *config.Namespace) error {
	conn, err := u.connection.Create(u.clientConfig.Host)
	if err != nil {
		return err
	}
	defer conn.Close()

	resourceClient := pb.NewResourceServiceClient(conn)

	ctx, cancelFunc := context.WithTimeout(context.Background(), uploadTimeout)
	defer cancelFunc()

	isFailed := false
	for _, ds := range namespace.Datastore {
		resourceSpecs, err := u.getResourceSpecs(ds.Path)
		if err != nil {
			u.logger.Error(err.Error())
			isFailed = true
			continue
		}

		resourceProtos := make([]*pb.ResourceSpecification, 0)
		for _, resourceSpec := range resourceSpecs {
			resourceProto, err := resourceSpec.ToProto()
			if err != nil {
				u.logger.Error(err.Error())
				isFailed = true
				continue
			}
			resourceProtos = append(resourceProtos, resourceProto)
		}

		countResources := len(resourceProtos)
		for i := 0; i < countResources; i += u.batchSize {
			endIndex := i + u.batchSize
			if countResources < endIndex {
				endIndex = countResources
			}

			upsertRequest := &pb.UpsertResourceRequest{
				ProjectName:   u.clientConfig.Project.Name,
				NamespaceName: namespace.Name,
				DatastoreName: ds.Type,
				Resources:     resourceProtos[i:endIndex],
			}

			resp, err := resourceClient.UpsertResource(ctx, upsertRequest)
			if err != nil {
				u.logger.Error("Unable to upload resource of namespace %s, err: %s", u.namespaceName, err)
				isFailed = true
			}
			for _, result := range resp.Results {
				message := result.Message
				if message != "" {
					message = fmt.Sprintf("(%s)", message)
				}
				if result.Status == resource.StatusFailure.String() {
					u.logger.Error("[%s] %s %s", result.Status, result.ResourceName, message)
					isFailed = true
					continue
				}
				u.logger.Info("[%s] %s %s", result.Status, result.ResourceName, message)
			}
		}
	}
	if isFailed {
		return fmt.Errorf("upload resource specifications to namespace %s failed", u.namespaceName)
	}
	u.logger.Info("finished uploading resource specifications to server\n")
	return nil
}

func (u *uploadCommand) getResourceSpecs(namespaceResourcePath string) ([]*model.ResourceSpec, error) {
	allResourcesInNamespace, err := u.resourceSpecReadWriter.ReadAll(namespaceResourcePath)
	if err != nil {
		return nil, err
	}
	resourceNameToSpecMap := make(map[string]*model.ResourceSpec, len(allResourcesInNamespace))
	for _, spec := range allResourcesInNamespace {
		resourceNameToSpecMap[spec.Name] = spec
	}

	resourceSpecs := make([]*model.ResourceSpec, 0)

	if len(u.resourceNames) == 0 {
		for _, spec := range resourceNameToSpecMap {
			resourceSpecs = append(resourceSpecs, spec)
		}
		return resourceSpecs, nil
	}

	for _, resourceName := range u.resourceNames {
		resourceSpec, ok := resourceNameToSpecMap[resourceName]
		if !ok {
			return nil, fmt.Errorf("resource %s not found in namespace %s", resourceName, u.namespaceName)
		}
		resourceSpecs = append(resourceSpecs, resourceSpec)
	}
	return resourceSpecs, nil
}
