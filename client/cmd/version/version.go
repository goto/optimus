package version

import (
	"context"
	"fmt"
	"time"

	"github.com/goto/salt/log"
	"github.com/goto/salt/version"
	"github.com/spf13/cobra"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/goto/optimus/client/cmd/internal"
	"github.com/goto/optimus/client/cmd/internal/connection"
	"github.com/goto/optimus/client/cmd/internal/logger"
	"github.com/goto/optimus/client/cmd/internal/progressbar"
	"github.com/goto/optimus/config"
	"github.com/goto/optimus/plugin"
	pb "github.com/goto/optimus/protos/gotocompany/optimus/core/v1beta1"
)

const versionTimeout = time.Second * 2

type versionCommand struct {
	logger     log.Logger
	connection connection.Connection

	configFilePath string

	isWithServer bool
	host         string

	pluginRepo *plugin.Store
}

// NewVersionCommand initializes command to get version
func NewVersionCommand() *cobra.Command {
	v := &versionCommand{
		logger: logger.NewClientLogger(),
	}

	cmd := &cobra.Command{
		Use:     "version",
		Short:   "Print the client version information",
		Example: "optimus version [--with-server]",
		RunE:    v.RunE,
		PreRunE: v.PreRunE,
	}

	v.injectFlags(cmd)

	return cmd
}

func (v *versionCommand) injectFlags(cmd *cobra.Command) {
	cmd.Flags().BoolVar(&v.isWithServer, "with-server", v.isWithServer, "Check for server version")

	// Config filepath flag
	cmd.Flags().StringVarP(&v.configFilePath, "config", "c", config.EmptyPath, "File path for client configuration")

	// Mandatory flags if with-server is set but config is not set
	cmd.Flags().StringVar(&v.host, "host", "", "Optimus service endpoint url")
}

func (v *versionCommand) PreRunE(cmd *cobra.Command, _ []string) error {
	if v.isWithServer {
		conf, err := internal.LoadOptionalConfig(v.configFilePath)
		if err != nil {
			return err
		}

		if conf == nil {
			cmd.MarkFlagRequired("host")
		} else if v.host == "" {
			v.host = conf.Host
		}

		v.connection = connection.New(v.logger, conf)
	}

	var err error

	v.pluginRepo, err = plugin.LoadPluginToStore(v.logger)
	return err
}

func (v *versionCommand) RunE(_ *cobra.Command, _ []string) error {
	// Print client version
	v.logger.Info("Client: %s-%s", config.BuildVersion, config.BuildCommit)

	// Print server version
	if v.isWithServer {
		srvVer, err := v.getVersionRequest(config.BuildVersion, v.host)
		if err != nil {
			return err
		}
		v.logger.Info("Server: %s", srvVer)
	}

	// Print version update if new version is exist
	githubRepo := "goto/optimus"
	if updateNotice := version.UpdateNotice(config.BuildVersion, githubRepo); updateNotice != "" {
		v.logger.Info(updateNotice)
	}
	v.printAllPluginInfos()
	return nil
}

func (v *versionCommand) printAllPluginInfos() {
	v.logger.Info("\nDiscovered plugins:")
	i := 0
	for task := range v.pluginRepo.All {
		i++
		v.logger.Info("\n%d. %s", i, task.Name)
		v.logger.Info("Description: %s", task.Description)
		v.logger.Info("Version: %d", task.SpecVersion)
		for ver, details := range task.PluginVersion {
			v.logger.Info("Version: %s", ver)
			v.logger.Info("  Image: %s:%s", details.Image, details.Tag)
			v.logger.Info("  Entrypoint: {Shell: %s, Script: %s}", details.Entrypoint.Shell, details.Entrypoint.Script)
		}
	}
}

// getVersionRequest send a version request to service
func (v *versionCommand) getVersionRequest(clientVer, host string) (ver string, err error) {
	conn, err := v.connection.Create(host)
	if err != nil {
		return "", err
	}
	defer conn.Close()

	runtime := pb.NewRuntimeServiceClient(conn)
	spinner := progressbar.NewProgressBar()
	spinner.Start("please wait...")

	ctx, cancelFunc := context.WithTimeout(context.Background(), versionTimeout)
	defer cancelFunc()

	versionResponse, err := runtime.Version(ctx, &pb.VersionRequest{
		Client: clientVer,
	})
	if err != nil {
		if status.Code(err) == codes.Unauthenticated {
			return "", fmt.Errorf("please check if client_id belongs to this application")
		}
		return "", fmt.Errorf("request failed for version: %w", err)
	}
	spinner.Stop()
	return versionResponse.Server, nil
}
