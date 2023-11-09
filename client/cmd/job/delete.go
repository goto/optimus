package job

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/goto/salt/log"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"

	"github.com/goto/optimus/client/cmd/internal/connection"
	"github.com/goto/optimus/client/cmd/internal/logger"
	"github.com/goto/optimus/client/cmd/internal/survey"
	"github.com/goto/optimus/client/local"
	"github.com/goto/optimus/client/local/model"
	"github.com/goto/optimus/client/local/specio"
	"github.com/goto/optimus/config"
	pb "github.com/goto/optimus/protos/gotocompany/optimus/core/v1beta1"
)

const (
	deleteTimeout = time.Minute * 5
)

type deleteCommand struct {
	deleteSurvey    *survey.JobDeleteSurvey
	namespaceSurvey *survey.NamespaceSurvey
	logger          log.Logger

	connection connection.Connection

	configFilePath string
	force          bool
	cleanHistory   bool

	clientConfig *config.ClientConfig
	fs           afero.Fs
	reader       local.SpecReader[*model.JobSpec]
}

// NewDeleteCommand initializes job delete command
func NewDeleteCommand() *cobra.Command {
	l := logger.NewClientLogger()
	deleteCmd := &deleteCommand{
		deleteSurvey:    survey.NewJobDeleteSurvey(),
		namespaceSurvey: survey.NewNamespaceSurvey(l),
		logger:          l,
	}

	cmd := &cobra.Command{
		Use:     "delete <job_name>",
		Short:   "Delete an existing job in the SERVER",
		Example: "optimus job delete daily_scheduled_job",
		RunE:    deleteCmd.RunE,
		PreRunE: deleteCmd.PreRunE,
	}

	cmd.Flags().StringVarP(&deleteCmd.configFilePath, "config", "c", config.EmptyPath, "File path for client configuration")
	cmd.Flags().BoolVar(&deleteCmd.force, "force", false, "Whether to force delete regardless of downstream or not")
	cmd.Flags().BoolVar(&deleteCmd.cleanHistory, "clean-history", false, "Whether to clean history or not")

	return cmd
}

func (d *deleteCommand) PreRunE(_ *cobra.Command, _ []string) error {
	conf, err := config.LoadClientConfig(d.configFilePath)
	if err != nil {
		return err
	}

	fs := afero.NewOsFs()
	readWriter, err := specio.NewJobSpecReadWriter(fs)
	if err != nil {
		return err
	}

	d.clientConfig = conf
	d.connection = connection.New(d.logger, d.clientConfig)
	d.fs = fs
	d.reader = readWriter

	return nil
}

func (d *deleteCommand) RunE(_ *cobra.Command, args []string) error {
	if len(args) != 1 {
		return errors.New("one argument for job name is required")
	}

	namespace, err := d.namespaceSurvey.AskToSelectNamespace(d.clientConfig)
	if err != nil {
		return err
	}
	jobName := args[0]

	confirm, err := d.confirm(jobName)
	if err != nil {
		return err
	}

	if !confirm {
		d.logger.Info("deletion is cancelled")
		return nil
	}

	return d.delete(namespace.Name, jobName)
}

func (d *deleteCommand) confirm(jobName string) (bool, error) {
	confirmed, err := d.deleteSurvey.AskToConfirm(jobName)
	if err != nil {
		return false, err
	}

	if confirmed && d.cleanHistory {
		confirmed, err = d.deleteSurvey.AskToConfirmCleanHistory(jobName)
		if err != nil {
			return false, err
		}
	}

	if confirmed && d.force {
		confirmed, err = d.deleteSurvey.AskToConfirmForce(jobName)
		if err != nil {
			return false, err
		}
	}

	return confirmed, nil
}

func (d *deleteCommand) delete(namespaceName, jobName string) error {
	if err := d.deleteFromServer(namespaceName, jobName); err != nil {
		return err
	}

	if err := d.deleteFromLocal(namespaceName, jobName); err != nil {
		d.logger.Warn("local deletion attempt met error: %v", err)
	}

	return nil
}

func (d *deleteCommand) deleteFromLocal(namespaceName, jobName string) error {
	namespace, err := d.clientConfig.GetNamespaceByName(namespaceName)
	if err != nil {
		return err
	}

	jobSpecs, err := d.reader.ReadAll(namespace.Job.Path)
	if err != nil {
		return err
	}

	for _, j := range jobSpecs {
		if j.Name == jobName {
			return d.fs.RemoveAll(j.Path)
		}
	}

	return fmt.Errorf("job [%s] with namespace [%s] is not found in local", jobName, namespaceName)
}

func (d *deleteCommand) deleteFromServer(namespaceName, jobName string) error {
	conn, err := d.connection.Create(d.clientConfig.Host)
	if err != nil {
		return err
	}
	defer conn.Close()

	client := pb.NewJobSpecificationServiceClient(conn)

	ctx, dialCancel := context.WithTimeout(context.Background(), deleteTimeout)
	defer dialCancel()

	response, err := client.DeleteJobSpecification(ctx, &pb.DeleteJobSpecificationRequest{
		ProjectName:   d.clientConfig.Project.Name,
		NamespaceName: namespaceName,
		JobName:       jobName,
		CleanHistory:  d.cleanHistory,
		Force:         d.force,
	})
	if err != nil {
		d.logger.Error("error deleting job [%s]", jobName)
		return err
	}

	if response.GetSuccess() {
		d.logger.Info("successfully deleted job [%s]", jobName)
		if d.force {
			d.logger.Warn(response.GetMessage())
		}

		return nil
	}

	d.logger.Error(response.GetMessage())
	return fmt.Errorf("deletion for job [%s] failed", jobName)
}
