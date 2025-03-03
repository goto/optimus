package project

import (
	"context"
	"path"
	"time"

	"github.com/goto/salt/log"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v2"

	"github.com/goto/optimus/client/cmd/internal"
	"github.com/goto/optimus/client/cmd/internal/connection"
	"github.com/goto/optimus/client/cmd/internal/logger"
	"github.com/goto/optimus/config"
	pb "github.com/goto/optimus/protos/gotocompany/optimus/core/v1beta1"
)

const describeTimeout = time.Minute * 15

type describeCommand struct {
	logger     log.Logger
	connection connection.Connection

	configFilePath string

	dirPath     string
	host        string
	projectName string
}

// NewDescribeCommand initializes command to describe a project
func NewDescribeCommand() *cobra.Command {
	describe := &describeCommand{
		logger: logger.NewClientLogger(),
	}

	cmd := &cobra.Command{
		Use:     "describe",
		Short:   "Describes project configuration in the selected server",
		Example: "optimus project describe [--flag]",
		RunE:    describe.RunE,
		PreRunE: describe.PreRunE,
	}

	describe.injectFlags(cmd)
	return cmd
}

func (d *describeCommand) injectFlags(cmd *cobra.Command) {
	// Config filepath flag
	cmd.Flags().StringVarP(&d.configFilePath, "config", "c", config.EmptyPath, "File path for client configuration")
	cmd.Flags().StringVar(&d.dirPath, "dir", d.dirPath, "Directory where the Optimus client config resides")

	// Mandatory flags if config is not set
	cmd.Flags().StringVar(&d.host, "host", d.host, "Targeted server host, by default taking from client config")
	cmd.Flags().StringVar(&d.projectName, "project-name", d.projectName, "Targeted project name, by default taking from client config")
}

func (d *describeCommand) PreRunE(cmd *cobra.Command, _ []string) error {
	if d.dirPath != "" {
		d.configFilePath = path.Join(d.dirPath, config.DefaultFilename)
	}
	// Load config
	conf, err := internal.LoadOptionalConfig(d.configFilePath)
	if err != nil {
		return err
	}

	if conf == nil {
		internal.MarkFlagsRequired(cmd, []string{"project-name", "host"})
		return nil
	}

	if d.projectName == "" {
		d.projectName = conf.Project.Name
	}
	if d.host == "" {
		d.host = conf.Host
	}
	d.connection = connection.New(d.logger, conf)

	return nil
}

func (d *describeCommand) RunE(_ *cobra.Command, _ []string) error {
	d.logger.Info("Getting project [%s] from host [%s]", d.projectName, d.host)
	project, err := d.getProject()
	if err != nil {
		return err
	}
	marshalledProject, err := yaml.Marshal(project)
	if err != nil {
		return err
	}
	d.logger.Info("Successfully getting project!")
	d.logger.Info("============================\n%s", string(marshalledProject))
	return nil
}

func (d *describeCommand) getProject() (config.Project, error) {
	var project config.Project
	conn, err := d.connection.Create(d.host)
	if err != nil {
		return project, err
	}
	defer conn.Close()

	request := &pb.GetProjectRequest{
		ProjectName: d.projectName,
	}

	projectServiceClient := pb.NewProjectServiceClient(conn)

	ctx, cancelFunc := context.WithTimeout(context.Background(), describeTimeout)
	defer cancelFunc()

	response, err := projectServiceClient.GetProject(ctx, request)
	if err != nil {
		return project, err
	}
	return config.Project{
		Name:      response.GetProject().Name,
		Config:    response.GetProject().Config,
		Variables: response.GetProject().Variables,
	}, nil
}
