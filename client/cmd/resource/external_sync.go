package resource

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/MakeNowJust/heredoc"
	"github.com/goto/salt/log"
	"github.com/spf13/cobra"

	"github.com/goto/optimus/client/cmd/internal/connection"
	"github.com/goto/optimus/client/cmd/internal/logger"
	"github.com/goto/optimus/client/cmd/internal/progressbar"
	"github.com/goto/optimus/client/cmd/internal/survey"
	"github.com/goto/optimus/config"
	pb "github.com/goto/optimus/protos/gotocompany/optimus/core/v1beta1"
)

const (
	syncTimeout = time.Minute * 15
)

type syncExternalCommand struct {
	logger     log.Logger
	connection connection.Connection

	configFilePath string
	clientConfig   *config.ClientConfig

	namespaceSurvey *survey.NamespaceSurvey
	namespaceName   string
	projectName     string
	resourceName    string
}

// NewSyncExternalCommand is to initiate sync of data for external table
func NewSyncExternalCommand() *cobra.Command {
	l := logger.NewClientLogger()
	sync := &syncExternalCommand{
		logger:          l,
		namespaceSurvey: survey.NewNamespaceSurvey(l),
	}

	cmd := &cobra.Command{
		Use:     "sync_external",
		Short:   "Sync External table resources from optimus to datastore",
		Long:    heredoc.Doc(`Sync sheet based external tables to datastore`),
		Example: "optimus resource sync_external -R resource-name1",
		Annotations: map[string]string{
			"group:core": "true",
		},
		RunE:    sync.RunE,
		PreRunE: sync.PreRunE,
	}
	cmd.Flags().StringVarP(&sync.configFilePath, "config", "c", sync.configFilePath, "File path for client configuration")
	cmd.Flags().StringVarP(&sync.resourceName, "resource-name", "R", "", "Selected resource of optimus project")
	cmd.Flags().StringVarP(&sync.namespaceName, "namespace", "n", "", "Namespace name within project")
	return cmd
}

func (se *syncExternalCommand) PreRunE(_ *cobra.Command, _ []string) error {
	var err error
	se.clientConfig, err = config.LoadClientConfig(se.configFilePath)
	if err != nil {
		return err
	}

	se.connection = connection.New(se.logger, se.clientConfig)

	return nil
}

func (se *syncExternalCommand) RunE(_ *cobra.Command, _ []string) error {
	se.logger.Info("> Triggering external table sync")

	if se.projectName == "" {
		se.projectName = se.clientConfig.Project.Name
	}

	var namespace *config.Namespace
	// use flag or ask namespace name
	if se.namespaceName == "" {
		var err error
		namespace, err = se.namespaceSurvey.AskToSelectNamespace(se.clientConfig)
		if err != nil {
			return err
		}
		se.namespaceName = namespace.Name
	}

	return se.triggerSync()
}

func (se *syncExternalCommand) triggerSync() error {
	conn, err := se.connection.Create(se.clientConfig.Host)
	if err != nil {
		return err
	}
	defer conn.Close()

	apply := pb.NewResourceServiceClient(conn)

	spinner := progressbar.NewProgressBar()
	spinner.Start("please wait...")

	syncExternalTablesRequest := pb.SyncExternalTablesRequest{
		ProjectName:   se.projectName,
		NamespaceName: se.namespaceName,
		TableName:     se.resourceName,
	}

	ctx, cancelFunc := context.WithTimeout(context.Background(), syncTimeout)
	defer cancelFunc()

	response, err := apply.SyncExternalTables(ctx, &syncExternalTablesRequest)
	spinner.Stop()
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			se.logger.Error("Sync took too long, timing out")
		}
		return fmt.Errorf("failed to sync resourcse: %w", err)
	}

	se.printStatus(response)
	return nil
}

func (se *syncExternalCommand) printStatus(res *pb.SyncExternalTablesResponse) {
	se.logger.Info("Sync finished")
	if len(res.SuccessfullySynced) > 0 {
		se.logger.Info("Resources with success")
		for i, name := range res.SuccessfullySynced {
			se.logger.Info("%d. %s", i+1, name)
		}
	}

	if res.Error != "" {
		se.logger.Error("Sync encountered following error: %s", res.Error)
	}
}
