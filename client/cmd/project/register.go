package project

import (
	"context"
	"fmt"
	"time"

	"github.com/goto/salt/log"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/goto/optimus/client/cmd/internal/connection"
	"github.com/goto/optimus/client/cmd/internal/logger"
	"github.com/goto/optimus/client/cmd/namespace"
	"github.com/goto/optimus/config"
	pb "github.com/goto/optimus/protos/gotocompany/optimus/core/v1beta1"
)

const registerTimeout = time.Minute * 15

type registerCommand struct {
	logger         log.Logger
	configFilePath string
	clientConfig   *config.ClientConfig

	dirPath        string
	withNamespaces bool
}

// NewRegisterCommand initializes command to create a project
func NewRegisterCommand() *cobra.Command {
	register := &registerCommand{
		logger: logger.NewClientLogger(),
	}

	cmd := &cobra.Command{
		Use:     "register",
		Short:   "Register project if it does not exist and update if it does",
		Example: "optimus project register [--flag]",
		PreRunE: register.PreRunE,
		RunE:    register.RunE,
	}

	cmd.Flags().StringVarP(&register.configFilePath, "config", "c", config.EmptyPath, "File path for client configuration")
	cmd.Flags().StringVar(&register.dirPath, "dir", register.dirPath, "Directory where the Optimus client config resides")
	cmd.Flags().BoolVar(&register.withNamespaces, "with-namespaces", register.withNamespaces, "If yes, then namespace will be registered or updated as well")
	return cmd
}

func (r *registerCommand) PreRunE(_ *cobra.Command, _ []string) error {
	conf, err := config.LoadClientConfig(r.configFilePath)
	if err != nil {
		return err
	}

	r.clientConfig = conf
	return nil
}

func (r *registerCommand) RunE(_ *cobra.Command, _ []string) error {
	conn := connection.New(r.logger, r.clientConfig)
	c, err := conn.Create(r.clientConfig.Host)
	if err != nil {
		return err
	}
	defer c.Close()

	r.logger.Info("Registering project [%s] to server [%s]", r.clientConfig.Project.Name, r.clientConfig.Host)
	if err := RegisterProject(r.logger, c, r.clientConfig.Project); err != nil {
		return err
	}
	if r.withNamespaces {
		r.logger.Info("Registering all namespaces from: %s", r.configFilePath)
		if err := namespace.RegisterSelectedNamespaces(r.logger, c, r.clientConfig.Project.Name, r.clientConfig.Namespaces...); err != nil {
			return err
		}
	}
	return nil
}

// RegisterProject registers a project to the targeted server host
func RegisterProject(logger log.Logger, conn *grpc.ClientConn, project config.Project) error {
	projectServiceClient := pb.NewProjectServiceClient(conn)
	projectSpec := &pb.ProjectSpecification{
		Name:   project.Name,
		Config: project.Config,
	}

	ctx, cancelFunc := context.WithTimeout(context.Background(), registerTimeout)
	defer cancelFunc()

	_, err := projectServiceClient.RegisterProject(ctx, &pb.RegisterProjectRequest{
		Project: projectSpec,
	})
	if err != nil {
		if status.Code(err) == codes.FailedPrecondition {
			logger.Warn(fmt.Sprintf("Ignoring project config changes: %v", err))
			return nil
		}
		return fmt.Errorf("failed to register or update project: %w", err)
	}
	logger.Info("Project registration finished successfully")
	return nil
}

func getProjectPresets(presetsPath string) (*model.PresetsMap, error) {
	specFS := afero.NewOsFs()
	fileSpec, err := specFS.Open(presetsPath)
	if err != nil {
		return nil, fmt.Errorf("error opening presets file[%s]: %w", presetsPath, err)
	}
	defer fileSpec.Close()

	var spec model.PresetsMap
	if err := yaml.NewDecoder(fileSpec).Decode(&spec); err != nil {
		return nil, fmt.Errorf("error decoding spec under [%s]: %w", presetsPath, err)
	}
	return &spec, nil
}
