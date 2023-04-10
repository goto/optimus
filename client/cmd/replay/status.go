package replay

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/goto/optimus/client/cmd/internal"
	"github.com/goto/optimus/client/cmd/internal/connectivity"
	"github.com/goto/optimus/client/cmd/internal/logger"
	"github.com/goto/optimus/config"
	pb "github.com/goto/optimus/protos/gotocompany/optimus/core/v1beta1"
	"github.com/goto/salt/log"
	"github.com/olekukonko/tablewriter"
	"github.com/spf13/cobra"
)

type statusCommand struct {
	logger         log.Logger
	configFilePath string

	projectName string
	host        string
}

// StatusCommand get status for corresponding replay
func StatusCommand() *cobra.Command {
	status := &statusCommand{
		logger: logger.NewClientLogger(),
	}

	cmd := &cobra.Command{
		Use:     "status",
		Short:   "Get replay detailed status by replay ID",
		Long:    "This operation takes 1 argument, replayID [required] \nwhich UUID format ",
		Example: "optimus replay status <replay_id>",
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) < 1 {
				return errors.New("replayID is required")
			}
			return nil
		},
		RunE:    status.RunE,
		PreRunE: status.PreRunE,
	}

	status.injectFlags(cmd)
	return cmd
}

func (r *statusCommand) injectFlags(cmd *cobra.Command) {
	// Config filepath flag
	cmd.Flags().StringVarP(&r.configFilePath, "config", "c", config.EmptyPath, "File path for client configuration")

	// Mandatory flags if config is not set
	cmd.Flags().StringVarP(&r.projectName, "project-name", "p", "", "Name of the optimus project")
	cmd.Flags().StringVar(&r.host, "host", "", "Optimus service endpoint url")
}

func (r *statusCommand) PreRunE(cmd *cobra.Command, _ []string) error {
	conf, err := internal.LoadOptionalConfig(r.configFilePath)
	if err != nil {
		return err
	}

	if conf == nil {
		internal.MarkFlagsRequired(cmd, []string{"project-name", "host"})
		return nil
	}

	if r.projectName == "" {
		r.projectName = conf.Project.Name
	}
	if r.host == "" {
		r.host = conf.Host
	}
	return nil
}

func (r *statusCommand) RunE(_ *cobra.Command, args []string) error {
	replayID := args[0]
	resp, err := r.getReplay(replayID)
	if err != nil {
		return err
	}
	result := r.stringifyReplayStatus(resp)
	r.logger.Info("Replay status for replay ID: %s", replayID)
	r.logger.Info(result)
	return nil
}

func (r *statusCommand) getReplay(replayID string) (*pb.GetReplayResponse, error) {
	conn, err := connectivity.NewConnectivity(r.host, replayTimeout)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	req := &pb.GetReplayRequest{ReplayId: replayID}

	replayService := pb.NewReplayServiceClient(conn.GetConnection())
	return replayService.GetReplay(conn.GetContext(), req)
}

func (r *statusCommand) stringifyReplayStatus(resp *pb.GetReplayResponse) string {
	buff := &bytes.Buffer{}
	buff.WriteString(fmt.Sprintf("ID       : %s", resp.GetId()))
	buff.WriteString(fmt.Sprintf("Job Name : %s", resp.GetJobName()))
	buff.WriteString(fmt.Sprintf("Runs     : %d", len(resp.GetReplayRuns())))

	table := tablewriter.NewWriter(buff)
	table.SetBorder(false)
	table.SetHeader([]string{
		"Config Key",
		"Config Value",
	})
	table.SetAlignment(tablewriter.ALIGN_LEFT)
	for k, v := range resp.ReplayConfig.GetJobConfig() {
		table.Append([]string{k, v})
	}
	table.Render()

	table = tablewriter.NewWriter(buff)
	table.SetBorder(false)
	table.SetHeader([]string{
		"ScheduledAt",
		"Status",
	})
	table.SetAlignment(tablewriter.ALIGN_LEFT)
	for _, run := range resp.GetReplayRuns() {
		table.Append([]string{
			run.GetScheduledAt().AsTime().String(),
			run.GetStatus(),
		})
	}
	table.Render()
	return buff.String()
}
