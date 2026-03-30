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

	projectName   string
	host          string
	useApproverID bool
}

// CancelCommand cancel the corresponding replay
func CancelCommand() *cobra.Command {
	cancelCmd := &cancelCommand{
		logger: logger.NewClientLogger(),
	}

	cmd := &cobra.Command{
		Use:     "cancel",
		Short:   "Cancel replay using replay ID or approver ID",
		Long:    "This operation takes 1 argument, ID [required]\nBy default the ID is used as replay_id. Use --approver-id flag to treat the ID as approver_id.",
		Example: "optimus replay cancel <replay_id>\noptimus replay cancel <approver_id> --approver-id",
		Args: func(_ *cobra.Command, args []string) error {
			if len(args) < 1 {
				return errors.New("ID is required")
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
	cmd.Flags().BoolVar(&c.useApproverID, "approver-id", false, "Treat the provided ID as approver_id instead of replay_id")
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
	id := args[0]
	resp, err := c.cancelReplay(id)
	if err != nil {
		return err
	}
	result := c.stringifyReplayCancelResponse(id, resp)
	c.logger.Info(result)
	return nil
}

func (c *cancelCommand) cancelReplay(id string) (*pb.CancelReplayResponse, error) {
	conn, err := c.connection.Create(c.host)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	var req *pb.CancelReplayRequest
	if c.useApproverID {
		req = &pb.CancelReplayRequest{ApproverId: id}
	} else {
		req = &pb.CancelReplayRequest{ReplayId: id}
	}

	replayService := pb.NewReplayServiceClient(conn)

	ctx, cancelFunc := context.WithTimeout(context.Background(), replayTimeout)
	defer cancelFunc()

	return replayService.CancelReplay(ctx, req)
}

func (*cancelCommand) stringifyReplayCancelResponse(replayID string, resp *pb.CancelReplayResponse) string {
	buff := &bytes.Buffer{}
	fmt.Fprintf(buff, "Job Name      : %s\n", resp.GetJobName())
	fmt.Fprintf(buff, "Total Runs    : %d\n\n", len(resp.GetReplayRuns()))
	if len(resp.GetReplayRuns()) > 0 {
		header := []string{"scheduled at", "latest status"}
		stringifyReplayRuns(buff, header, resp.GetReplayRuns())
	}
	fmt.Fprintf(buff, "\nReplay with ID %s has been cancelled.", replayID)
	return buff.String()
}
