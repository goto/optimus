package job

import (
	"fmt"
	"time"

	"github.com/goto/salt/log"
	"github.com/spf13/afero"
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
	host         string
}

// NewChangeNamespaceCommand initializes job namespace change command
func NewChangeNamespaceCommand() *cobra.Command {
	l := logger.NewClientLogger()
	changeNamespace := &changeNamespaceCommand{
		logger: l,
	}
	cmd := &cobra.Command{
		Use:      "change-namespace",
		Short:    "Change namespace of a Job",
		Example:  "optimus job change-namespace <job-name> --old-namespace <old-namespace> --new-namespace <new-namespace>",
		Args:     cobra.MinimumNArgs(1),
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
	cmd.Flags().StringVarP(&c.oldNamespace, "old-namespace", "o", "", "current namespace of the job")
	cmd.Flags().StringVarP(&c.newNamespace, "new-namespace", "n", "", "namespace to which the job needs to be moved to")

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
	jobName := args[0]
	err := c.sendChangeNamespaceRequest(jobName)
	if err != nil {
		return fmt.Errorf("namespace change request failed for job %s: %w", jobName, err)
	}
	c.logger.Info("Successfully changed namespace and deployed new DAG on Scheduler")
	return nil
}

func (c *changeNamespaceCommand) sendChangeNamespaceRequest(jobName string) error {
	conn, err := connectivity.NewConnectivity(c.host, changeNamespaceTimeout)
	if err != nil {
		return err
	}
	defer conn.Close()

	// fetch Instance by calling the optimus API
	jobRunServiceClient := pb.NewJobSpecificationServiceClient(conn.GetConnection())
	request := &pb.ChangeJobNamespaceRequest{
		ProjectName:      c.project,
		NamespaceName:    c.oldNamespace,
		NewNamespaceName: c.newNamespace,
		JobName:          jobName,
	}

	_, err = jobRunServiceClient.ChangeJobNamespace(conn.GetContext(), request)
	return err
}

func (c *changeNamespaceCommand) PostRunE(_ *cobra.Command, args []string) error {
	c.logger.Info("\n[INFO] Moving job in filesystem")
	jobName := args[0]
	fs := afero.NewOsFs()
	var source, destination string
	for _, namespace := range c.clientConfig.Namespaces {
		if namespace.Name == c.oldNamespace {
			source = "./" + namespace.Job.Path + "/" + jobName
		} else if namespace.Name == c.newNamespace {
			destination = "./" + namespace.Job.Path + "/" + jobName
		}
	}

	c.logger.Info(fmt.Sprintf("\t* Old Path : '%s' \n\t* New Path : '%s' \n", source, destination))

	err := fs.Rename(source, destination)
	if err != nil {
		return err
	}
	c.logger.Info("[OK] Job moved successfully")
	return nil
}
