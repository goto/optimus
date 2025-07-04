package replay

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/goto/salt/log"
	"github.com/olekukonko/tablewriter"
	"github.com/spf13/cobra"

	"github.com/goto/optimus/client/cmd/internal"
	"github.com/goto/optimus/client/cmd/internal/connection"
	"github.com/goto/optimus/client/cmd/internal/logger"
	"github.com/goto/optimus/config"
	pb "github.com/goto/optimus/protos/gotocompany/optimus/core/v1beta1"
)

type statusCommand struct {
	logger     log.Logger
	connection connection.Connection

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
		Args: func(_ *cobra.Command, args []string) error {
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
	r.connection = connection.New(r.logger, conf)
	return nil
}

func (r *statusCommand) RunE(_ *cobra.Command, args []string) error {
	replayID := args[0]
	resp, err := r.getReplay(replayID)
	if err != nil {
		return err
	}
	result := stringifyReplayStatus(resp)
	r.logger.Info("Replay status for replay ID: %s", replayID)
	r.logger.Info(result)
	return nil
}

func (r *statusCommand) getReplay(replayID string) (*pb.GetReplayResponse, error) {
	return getReplay(r.host, replayID, r.connection)
}

func getReplay(host, replayID string, connection connection.Connection) (*pb.GetReplayResponse, error) {
	conn, err := connection.Create(host)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	req := &pb.GetReplayRequest{ReplayId: replayID}

	replayService := pb.NewReplayServiceClient(conn)

	ctx, cancelFunc := context.WithTimeout(context.Background(), replayTimeout)
	defer cancelFunc()

	return replayService.GetReplay(ctx, req)
}

func stringifyReplayStatus(resp *pb.GetReplayResponse) string {
	buff := &bytes.Buffer{}
	mode := "sequential"
	if resp.GetReplayConfig().GetParallel() {
		mode = "parallel"
	}
	var message string
	if resp.Message != "" {
		message = fmt.Sprintf(" (%s)", resp.Message)
	}
	fmt.Fprintf(buff, "Job Name      : %s\n", resp.GetJobName())
	fmt.Fprintf(buff, "Replay Mode   : %s\n", mode)
	fmt.Fprintf(buff, "Description   : %s\n", resp.ReplayConfig.GetDescription())
	fmt.Fprintf(buff, "Start Date    : %s\n", resp.ReplayConfig.GetStartTime().AsTime().Format(time.RFC3339))
	fmt.Fprintf(buff, "End Date      : %s\n", resp.ReplayConfig.GetEndTime().AsTime().Format(time.RFC3339))
	fmt.Fprintf(buff, "Replay Status : %s%s\n", resp.GetStatus(), message)
	fmt.Fprintf(buff, "Total Runs    : %d\n\n", len(resp.GetReplayRuns()))

	if len(resp.ReplayConfig.GetJobConfig()) > 0 {
		stringifyReplayConfig(buff, resp.ReplayConfig.GetJobConfig())
		buff.WriteString("\n")
	}

	if len(resp.GetReplayRuns()) > 0 {
		header := []string{"scheduled at", "current run status"}
		stringifyReplayRuns(buff, header, resp.GetReplayRuns())
	}

	return buff.String()
}

func stringifyReplayConfig(buff *bytes.Buffer, jobConfig map[string]string) {
	table := tablewriter.NewWriter(buff)
	table.SetBorder(false)
	table.SetHeader([]string{"config key", "config value"})
	table.SetAlignment(tablewriter.ALIGN_LEFT)
	for k, v := range jobConfig {
		table.Append([]string{k, v})
	}
	table.Render()
}

func stringifyReplayRuns(buff *bytes.Buffer, header []string, runs []*pb.ReplayRun) {
	table := tablewriter.NewWriter(buff)
	table.SetBorder(false)
	table.SetHeader(header)
	table.SetAlignment(tablewriter.ALIGN_LEFT)
	for _, run := range runs {
		table.Append([]string{
			run.GetScheduledAt().AsTime().String(),
			run.GetStatus(),
		})
	}
	table.Render()
}
