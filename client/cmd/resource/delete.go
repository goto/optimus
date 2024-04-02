package resource

import (
	"context"
	"errors"
	"fmt"

	"github.com/MakeNowJust/heredoc"
	"github.com/goto/optimus/client/cmd/internal/connection"
	"github.com/goto/optimus/client/cmd/internal/logger"
	"github.com/goto/optimus/client/cmd/internal/progressbar"
	"github.com/goto/optimus/client/cmd/internal/survey"
	"github.com/goto/optimus/config"
	pb "github.com/goto/optimus/protos/gotocompany/optimus/core/v1beta1"
	"github.com/goto/salt/log"
	"github.com/spf13/cobra"
)

type deleteCommand struct {
	logger     log.Logger
	connection connection.Connection

	configFilePath string
	clientConfig   *config.ClientConfig

	namespaceSurvey *survey.NamespaceSurvey
	namespaceName   string
	projectName     string
	storeName       string
	resourceName    string
	verbose, force  bool
}

// NewDeleteCommand initializes command for delete resource from optimus
func NewDeleteCommand() *cobra.Command {
	l := logger.NewClientLogger()
	apply := &deleteCommand{
		logger:          l,
		namespaceSurvey: survey.NewNamespaceSurvey(l),
	}

	cmd := &cobra.Command{
		Use:     "delete",
		Short:   "Delete resource from optimus",
		Long:    heredoc.Doc(`Delete resource from Optimus`),
		Example: "optimus resource delete <resource-name>",
		Annotations: map[string]string{
			"group:core": "true",
		},
		RunE:    apply.RunE,
		PreRunE: apply.PreRunE,
	}
	cmd.Flags().StringVarP(&apply.configFilePath, "config", "c", apply.configFilePath, "File path for client configuration")
	cmd.Flags().StringVarP(&apply.resourceName, "resource-name", "R", "", "Selected resource of optimus project")
	cmd.Flags().BoolVarP(&apply.verbose, "verbose", "v", false, "Print details related to delete stages")
	cmd.Flags().StringVarP(&apply.namespaceName, "namespace", "n", "", "Namespace name within project")
	cmd.Flags().StringVarP(&apply.storeName, "datastore", "s", "bigquery", "Datastore type where the resource belongs")
	return cmd
}

func (a *deleteCommand) PreRunE(_ *cobra.Command, _ []string) error {
	var err error
	a.clientConfig, err = config.LoadClientConfig(a.configFilePath)
	if err != nil {
		return err
	}

	a.connection = connection.New(a.logger, a.clientConfig)

	return nil
}

func (a *deleteCommand) RunE(_ *cobra.Command, _ []string) error {
	a.logger.Info("> Validating resource name")
	if len(a.resourceName) == 0 {
		return errors.New("empty resource name")
	}

	if a.projectName == "" {
		a.projectName = a.clientConfig.Project.Name
	}

	var namespace *config.Namespace
	// use flag or ask namespace name
	if a.namespaceName == "" {
		var err error
		namespace, err = a.namespaceSurvey.AskToSelectNamespace(a.clientConfig)
		if err != nil {
			return err
		}
		a.namespaceName = namespace.Name
	}

	return a.apply()
}

func (a *deleteCommand) apply() error {
	conn, err := a.connection.Create(a.clientConfig.Host)
	if err != nil {
		return err
	}
	defer conn.Close()

	apply := pb.NewResourceServiceClient(conn)

	spinner := progressbar.NewProgressBar()
	spinner.Start("please wait...")

	deleteResourceRequest := pb.DeleteResourceRequest{
		ProjectName:   a.projectName,
		NamespaceName: a.namespaceName,
		DatastoreName: a.storeName,
		ResourceName:  a.resourceName,
	}

	ctx, cancelFunc := context.WithTimeout(context.Background(), applyTimeout)
	defer cancelFunc()

	responses, err := apply.DeleteResource(ctx, &deleteResourceRequest)
	spinner.Stop()
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			a.logger.Error("Apply took too long, timing out")
		}
		return fmt.Errorf("failed to apply resourcse: %w", err)
	}

	if !responses.IsDeleted {
		a.logger.Error(fmt.Sprintf("failed to delete resource %s with message: %s", deleteResourceRequest.ResourceName, responses.FailedReason))
		return fmt.Errorf("failed to delete resource: %s", responses.FailedReason)
	} else {
		a.logger.Info("success to delete resource %s", deleteResourceRequest.ResourceName)
	}

	return nil
}
