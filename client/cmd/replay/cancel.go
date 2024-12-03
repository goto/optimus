package replay

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"github.com/goto/salt/log"
	"github.com/spf13/cobra"

	"github.com/goto/optimus/client/cmd/internal"
	"github.com/goto/optimus/client/cmd/internal/connection"
	"github.com/goto/optimus/client/cmd/internal/logger"
	"github.com/goto/optimus/config"
	pb "github.com/goto/optimus/protos/gotocompany/optimus/core/v1beta1"
)

type cancelCommand struct {
	logger     log.Logger
	connection connection.Connection

	configFilePath string

	projectName string
	host        string
}

// CancelCommand cancel the corresponding replay
func CancelCommand() *cobra.Command {
	cancelCmd := &cancelCommand{
		logger: logger.NewClientLogger(),
	}

	cmd := &cobra.Command{
		Use:     "cancel",
		Short:   "Cancel replay using replay ID",
		Long:    "This operation takes 1 argument, replayID [required] \nwhich UUID format ",
		Example: "optimus replay cancel <replay_id>",
		Args: func(_ *cobra.Command, args []string) error {
			if len(args) < 1 {
				return errors.New("replayID is required")
			}
			return nil
		},
		RunE:    cancelCmd.RunE,
		PreRunE: cancelCmd.PreRunE,
	}

	cancelCmd.injectFlags(cmd)
	return cmd
}

func (c *cancelCommand) injectFlags(cmd *cobra.Command) {
	// Config filepath flag
	cmd.Flags().StringVarP(&c.configFilePath, "config", "c", config.EmptyPath, "File path for client configuration")

	// Mandatory flags if config is not set
	cmd.Flags().StringVarP(&c.projectName, "project-name", "p", "", "Name of the optimus project")
	cmd.Flags().StringVar(&c.host, "host", "", "Optimus service endpoint url")
}

func (c *cancelCommand) PreRunE(cmd *cobra.Command, _ []string) error {
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
	c.connection = connection.New(c.logger, conf)
	return nil
}

func (c *cancelCommand) RunE(_ *cobra.Command, args []string) error {
	replayID := args[0]
	resp, err := c.cancelReplay(replayID)
	if err != nil {
		return err
	}
	result := c.stringifyReplayCancelResponse(replayID, resp)
	c.logger.Info(result)
	return nil
}

func (c *cancelCommand) cancelReplay(replayID string) (*pb.CancelReplayResponse, error) {
	conn, err := c.connection.Create(c.host)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	req := &pb.CancelReplayRequest{ReplayId: replayID}

	replayService := pb.NewReplayServiceClient(conn)

	ctx, cancelFunc := context.WithTimeout(context.Background(), replayTimeout)
	defer cancelFunc()

	return replayService.CancelReplay(ctx, req)
}

func (*cancelCommand) stringifyReplayCancelResponse(replayID string, resp *pb.CancelReplayResponse) string {
	buff := &bytes.Buffer{}
	buff.WriteString(fmt.Sprintf("Job Name      : %s\n", resp.GetJobName()))
	buff.WriteString(fmt.Sprintf("Total Runs    : %d\n\n", len(resp.GetReplayRuns())))
	if len(resp.GetReplayRuns()) > 0 {
		header := []string{"scheduled at", "latest status"}
		stringifyReplayRuns(buff, header, resp.GetReplayRuns())
	}
	buff.WriteString(fmt.Sprintf("\nReplay with ID %s has been cancelled.", replayID))
	return buff.String()
}
