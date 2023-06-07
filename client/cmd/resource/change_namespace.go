package resource

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/goto/salt/log"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"

	"github.com/goto/optimus/client/cmd/internal"
	"github.com/goto/optimus/client/cmd/internal/connectivity"
	"github.com/goto/optimus/client/cmd/internal/logger"
	"github.com/goto/optimus/client/local/specio"
	"github.com/goto/optimus/config"
	"github.com/goto/optimus/core/resource"
	"github.com/goto/optimus/core/tenant"
	"github.com/goto/optimus/internal/errors"
	pb "github.com/goto/optimus/protos/gotocompany/optimus/core/v1beta1"
)

const (
	changeNamespaceTimeout = time.Minute * 1
)

type changeNamespaceCommand struct {
	logger         log.Logger
	configFilePath string
	clientConfig   *config.ClientConfig

	project      string
	oldNamespace string
	newNamespace string
	dataStore    string
	host         string
}

// NewChangeNamespaceCommand initializes resource namespace change command
func NewChangeNamespaceCommand() *cobra.Command {
	l := logger.NewClientLogger()
	changeNamespace := &changeNamespaceCommand{
		logger: l,
	}
	cmd := &cobra.Command{
		Use:      "change-namespace",
		Short:    "Create namespace of a resource",
		Example:  "optimus resource change-namespace <resource-name> <datastore-name> --old-namespace <old-namespace> --new-namespace <new-namespace>",
		Args:     cobra.MinimumNArgs(2), //nolint
		PreRunE:  changeNamespace.PreRunE,
		RunE:     changeNamespace.RunE,
		PostRunE: changeNamespace.PostRunE,
	}
	// Config filepath flag
	cmd.Flags().StringVarP(&changeNamespace.configFilePath, "config", "c", config.EmptyPath, "File path for client configuration")
	internal.MarkFlagsRequired(cmd, []string{"old-namespace", "new-namespace"})
	changeNamespace.injectFlags(cmd)

	return cmd
}

func (c *changeNamespaceCommand) injectFlags(cmd *cobra.Command) {
	// Mandatory flags
	cmd.Flags().StringVarP(&c.oldNamespace, "old-namespace", "o", "", "current namespace of the resource")
	cmd.Flags().StringVarP(&c.newNamespace, "new-namespace", "n", "", "namespace to which the resource needs to be moved to")

	// Mandatory flags if config is not set
	cmd.Flags().StringVarP(&c.project, "project-name", "p", "", "Name of the optimus project")
	cmd.Flags().StringVar(&c.host, "host", "", "Optimus service endpoint url")
}

func (c *changeNamespaceCommand) PreRunE(_ *cobra.Command, _ []string) error {
	// Load mandatory config
	conf, err := config.LoadClientConfig(c.configFilePath)
	if err != nil {
		return err
	}

	c.clientConfig = conf
	return err
}

func (c *changeNamespaceCommand) RunE(_ *cobra.Command, args []string) error {
	resourceFullName := args[0]
	c.dataStore = args[1]
	err := c.sendChangeNamespaceRequest(resourceFullName)
	if err != nil {
		return fmt.Errorf("namespace change request failed for resource %s: %w", resourceFullName, err)
	}
	c.logger.Info("successfully changed namespace and deployed new DAG on Scheduler")
	return nil
}

func (c *changeNamespaceCommand) sendChangeNamespaceRequest(resourceName string) error {
	conn, err := connectivity.NewConnectivity(c.host, changeNamespaceTimeout)
	if err != nil {
		return err
	}
	defer conn.Close()

	// fetch Instance by calling the optimus API
	resourceRunServiceClient := pb.NewResourceServiceClient(conn.GetConnection())
	request := &pb.ChangeResourceNamespaceRequest{
		ProjectName:      c.project,
		NamespaceName:    c.oldNamespace,
		DatastoreName:    c.dataStore,
		ResourceName:     resourceName,
		NewNamespaceName: c.newNamespace,
	}

	_, err = resourceRunServiceClient.ChangeResourceNamespace(conn.GetContext(), request)
	return err
}

func (c *changeNamespaceCommand) PostRunE(_ *cobra.Command, args []string) error {
	c.logger.Info("\n[INFO] Moving resource in filesystem")
	resourceName := args[0]
	c.dataStore = args[1]

	oldNamespaceConfig, err := c.getResourceDatastoreConfig(c.oldNamespace, c.dataStore)
	if err != nil {
		return errors.Wrap(tenant.EntityNamespace, "unregistered old namespace", err)
	}

	resourceSpecReadWriter, err := specio.NewResourceSpecReadWriter(afero.NewOsFs())
	if err != nil {
		return err
	}

	resourceSpec, err := resourceSpecReadWriter.ReadByName(oldNamespaceConfig.Path, resourceName)
	if err != nil {
		return errors.Wrap(tenant.EntityNamespace, "unable to find resource in old namespace", err)
	}

	fs := afero.NewOsFs()
	newNamespaceConfig, err := c.getResourceDatastoreConfig(c.newNamespace, c.dataStore)
	if err != nil || newNamespaceConfig.Path == "" {
		c.logger.Warn("[warn] new namespace not recognised for Resources")
		c.logger.Warn("[info] removing resource from old namespace")
		err = fs.RemoveAll(resourceSpec.Path)
		if err != nil {
			c.logger.Warn("unable to remove resource from old namespace")
			return errors.NewError(errors.ErrInternalError, "change-namespace", "unable to remove resource from old namespace")
		}
		c.logger.Warn("[info] removed resource spec from current namespace directory")
		c.logger.Warn("[info] run `optimus resource export` on the new namespace repo, to fetch the newly moved resource.")
		c.logger.Info("[OK] Resource moved successfully")
		return nil
	}

	var relativeResourcePath string
	splitComp := strings.Split(resourceSpec.Path, oldNamespaceConfig.Path)
	if !(len(splitComp) > 1) {
		return errors.NewError(errors.ErrInternalError, "change-namespace", "unable to parse resource spec path")
	}
	relativeResourcePath = splitComp[1]

	c.logger.Info(fmt.Sprintf("\t* Old Path : '%s' \n\t* New Path : '%s' \n", resourceSpec.Path, newNamespaceConfig.Path+relativeResourcePath))

	c.logger.Info(fmt.Sprintf("[info] creating Resource directry: %s", newNamespaceConfig.Path+relativeResourcePath))

	err = fs.MkdirAll(filepath.Dir(newNamespaceConfig.Path+relativeResourcePath), os.FileMode(0o755))
	if err != nil {
		return err
	}

	err = fs.Rename(resourceSpec.Path, newNamespaceConfig.Path+relativeResourcePath)
	if err != nil {
		return err
	}
	c.logger.Info("[OK] Resource moved successfully")
	return nil
}

func (c *changeNamespaceCommand) getResourceDatastoreConfig(namespaceName, datastoreName string) (*config.Datastore, error) {
	for _, namespace := range c.clientConfig.Namespaces {
		if namespace.Name == namespaceName {
			for _, datastore := range namespace.Datastore {
				if datastore.Type == datastoreName {
					return &datastore, nil
				}
			}
		}
	}
	return nil, errors.NotFound(resource.EntityResource, "not recognised in config")
}
