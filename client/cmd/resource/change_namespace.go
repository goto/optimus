package resource

import (
	"fmt"
	"time"

	"github.com/goto/salt/log"
	"github.com/spf13/cobra"

	"github.com/goto/optimus/client/cmd/internal"
	"github.com/goto/optimus/client/cmd/internal/connectivity"
	"github.com/goto/optimus/client/cmd/internal/logger"
	"github.com/goto/optimus/config"
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
		Use:     "change-namespace",
		Short:   "Create namespace of a resource",
		Example: "optimus resource change-namespace <resource-name> --old-namespace <old-namespace> --new-namespace <new-namespace>",
		RunE:    changeNamespace.RunE,
		Args:    cobra.MinimumNArgs(1),
		PreRunE: changeNamespace.PreRunE,
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
	resourceName := args[0]
	err := c.sendChangeNamespaceRequest(resourceName)
	if err != nil {
		return fmt.Errorf("namespace change request failed for resource %s: %w", resourceName, err)
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
