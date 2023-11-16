package job

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/goto/salt/log"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"

	"github.com/goto/optimus/client/cmd/internal"
	"github.com/goto/optimus/client/cmd/internal/connection"
	"github.com/goto/optimus/client/cmd/internal/logger"
	"github.com/goto/optimus/client/local"
	"github.com/goto/optimus/client/local/model"
	"github.com/goto/optimus/client/local/specio"
	"github.com/goto/optimus/config"
	pb "github.com/goto/optimus/protos/gotocompany/optimus/core/v1beta1"
)

const (
	changeNamespaceTimeout = time.Minute * 1
)

type changeNamespaceCommand struct {
	logger     log.Logger
	connection connection.Connection

	writer local.SpecReadWriter[*model.JobSpec]

	configFilePath string
	clientConfig   *config.ClientConfig

	projectName      string
	oldNamespaceName string
	newNamespaceName string
	host             string
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
	changeNamespace.injectFlags(cmd)
	internal.MarkFlagsRequired(cmd, []string{"old-namespace", "new-namespace"})

	return cmd
}

func (c *changeNamespaceCommand) injectFlags(cmd *cobra.Command) {
	// Config filepath flag
	cmd.Flags().StringVarP(&c.configFilePath, "config", "c", config.EmptyPath, "File path for client configuration")

	// Mandatory flags
	cmd.Flags().StringVarP(&c.oldNamespaceName, "old-namespace", "o", "", "current namespace of the job")
	cmd.Flags().StringVarP(&c.newNamespaceName, "new-namespace", "n", "", "namespace to which the job needs to be moved to")

	// Mandatory flags if config is not set
	cmd.Flags().StringVarP(&c.projectName, "project-name", "p", "", "Name of the optimus project")
	cmd.Flags().StringVar(&c.host, "host", "", "Optimus service endpoint url")
}

func (c *changeNamespaceCommand) PreRunE(cmd *cobra.Command, _ []string) error {
	readWriter, err := specio.NewJobSpecReadWriter(afero.NewOsFs())
	if err != nil {
		c.logger.Error(err.Error())
	}
	c.writer = readWriter

	// Load mandatory config
	conf, err := internal.LoadOptionalConfig(c.configFilePath)
	if err != nil {
		return err
	}

	if conf == nil {
		internal.MarkFlagsRequired(cmd, []string{"project-name", "host"})
		return nil
	}

	if c.projectName == "" {
		c.projectName = conf.Project.Name
	}
	if c.host == "" {
		c.host = conf.Host
	}

	c.clientConfig = conf
	c.connection = connection.New(c.logger, conf)

	return nil
}

func (c *changeNamespaceCommand) RunE(_ *cobra.Command, args []string) error {
	jobName := args[0]
	err := c.sendChangeNamespaceRequest(jobName)
	if err != nil {
		return fmt.Errorf("namespace change request failed for job %s: %w", jobName, err)
	}
	c.logger.Info("[OK] Successfully changed namespace and deployed new DAG on Scheduler")
	return nil
}

func (c *changeNamespaceCommand) sendChangeNamespaceRequest(jobName string) error {
	conn, err := c.connection.Create(c.host)
	if err != nil {
		return err
	}
	defer conn.Close()

	// fetch Instance by calling the optimus API
	jobRunServiceClient := pb.NewJobSpecificationServiceClient(conn)
	request := &pb.ChangeJobNamespaceRequest{
		ProjectName:      c.projectName,
		NamespaceName:    c.oldNamespaceName,
		NewNamespaceName: c.newNamespaceName,
		JobName:          jobName,
	}

	ctx, dialCancel := context.WithTimeout(context.Background(), changeNamespaceTimeout)
	defer dialCancel()

	_, err = jobRunServiceClient.ChangeJobNamespace(ctx, request)
	return err
}

func (c *changeNamespaceCommand) downloadJobSpecFile(fs afero.Fs, jobName, newJobPath string) error {
	conn, err := c.connection.Create(c.host)
	if err != nil {
		return err
	}
	defer conn.Close()

	jobSpecificationServiceClient := pb.NewJobSpecificationServiceClient(conn)

	ctx, cancelFunc := context.WithTimeout(context.Background(), fetchJobTimeout)
	defer cancelFunc()

	response, err := jobSpecificationServiceClient.GetJobSpecifications(ctx, &pb.GetJobSpecificationsRequest{
		ProjectName:   c.projectName,
		NamespaceName: c.newNamespaceName,
		JobName:       jobName,
	})
	if err != nil {
		return err
	}

	if len(response.JobSpecificationResponses) == 0 {
		return errors.New("job is not found")
	}
	jobSpec := model.ToJobSpec(response.JobSpecificationResponses[0].Job)

	c.logger.Info(fmt.Sprintf("[info] creating job directry: %s", newJobPath))
	err = fs.MkdirAll(filepath.Dir(newJobPath), os.ModePerm)
	if err != nil {
		return err
	}
	return c.writer.Write(newJobPath, jobSpec)
}

func (c *changeNamespaceCommand) getOldJobPath(jobName string) (string, error) {
	oldNamespaceConfig, err := c.getNamespaceConfig(c.oldNamespaceName)
	if err != nil {
		return "", fmt.Errorf("old namespace does not exist in filesystem, err: %w", err)
	}
	jobSpec, err := c.writer.ReadByName(oldNamespaceConfig.Job.Path, jobName)
	if err != nil {
		return "", fmt.Errorf("unable to find job in old namespace directory, err: %w", err)
	}
	return jobSpec.Path, nil
}

func (c *changeNamespaceCommand) deleteJobFile(fs afero.Fs, jobName string) (string, error) {
	oldJobPath, err := c.getOldJobPath(jobName)
	if err != nil {
		return oldJobPath, err
	}
	c.logger.Info("[info] removing job from old namespace")
	err = fs.RemoveAll(oldJobPath)
	if err != nil {
		return oldJobPath, err
	}
	c.logger.Info("[OK] removed job spec from old namespace directory")
	return oldJobPath, nil
}

func (c *changeNamespaceCommand) PostRunE(_ *cobra.Command, args []string) error {
	c.logger.Info("\n[info] Moving job in filesystem")
	jobName := args[0]
	fs := afero.NewOsFs()

	oldJobPath, err := c.deleteJobFile(fs, jobName)
	if err != nil {
		c.logger.Error(fmt.Sprintf("[error] unable to remove job from old namespace , err: %s", err.Error()))
		c.logger.Warn("[info] consider deleting source files from old namespace manually, if they exist")
	}

	var newJobPath string
	newNamespaceConfig, err := c.getNamespaceConfig(c.newNamespaceName)
	if err != nil {
		c.logger.Error("[error] new namespace not recognised for jobs: err: %s", err.Error())
	}
	if newNamespaceConfig.Job.Path == "" {
		c.logger.Error("[error] namespace config does not have a defined jobs path")
	}
	if err != nil || newNamespaceConfig.Job.Path == "" {
		c.logger.Warn("[info] register the new namespace and run \n\t`optimus job export -p %s -n %s -r %s `, to fetch the newly moved job.",
			c.projectName, c.newNamespaceName, jobName)
		return err
	}

	oldNamespaceConfig, nsConfigErr := c.getNamespaceConfig(c.oldNamespaceName)
	if oldJobPath == "" || nsConfigErr != nil {
		newJobPath = fmt.Sprintf("%s/%s", newNamespaceConfig.Job.Path, jobName)
	} else {
		newJobPath = strings.Replace(oldJobPath, oldNamespaceConfig.Job.Path, newNamespaceConfig.Job.Path, 1)
	}

	err = c.downloadJobSpecFile(fs, jobName, newJobPath)
	if err != nil {
		c.logger.Error(fmt.Sprintf("[error] unable to download job spec to new namespace directory, err: %s", err.Error()))
		c.logger.Warn("[info] manually run \n\t`optimus job export -p %s -n %s -r %s `, to fetch the newly moved job.",
			c.projectName, c.newNamespaceName, jobName)
	}
	c.logger.Info("[OK] Job moved successfully")
	return nil
}

func (c *changeNamespaceCommand) getNamespaceConfig(namespaceName string) (*config.Namespace, error) {
	for _, namespace := range c.clientConfig.Namespaces {
		if namespace.Name == namespaceName {
			return namespace, nil
		}
	}
	return nil, errors.New("not recognised in config")
}
