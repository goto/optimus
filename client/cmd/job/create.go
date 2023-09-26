package job

import (
	"fmt"
	"path/filepath"

	"github.com/goto/salt/log"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"

	"github.com/goto/optimus/client/cmd/internal"
	"github.com/goto/optimus/client/cmd/internal/logger"
	"github.com/goto/optimus/client/cmd/internal/survey"
	"github.com/goto/optimus/client/local/specio"
	"github.com/goto/optimus/config"
	"github.com/goto/optimus/internal/models"
)

type createCommand struct {
	logger          log.Logger
	configFilePath  string
	clientConfig    *config.ClientConfig
	namespaceSurvey *survey.NamespaceSurvey
	jobCreateSurvey *survey.JobCreateSurvey
	pluginRepo      *models.PluginRepository
}

// NewCreateCommand initializes job create command
func NewCreateCommand() *cobra.Command {
	l := logger.NewClientLogger()
	create := &createCommand{
		logger:          l,
		namespaceSurvey: survey.NewNamespaceSurvey(l),
		jobCreateSurvey: survey.NewJobCreateSurvey(l),
	}
	cmd := &cobra.Command{
		Use:     "create",
		Short:   "Create a new Job",
		Example: "optimus job create",
		RunE:    create.RunE,
		PreRunE: create.PreRunE,
	}
	// Config filepath flag
	cmd.Flags().StringVarP(&create.configFilePath, "config", "c", config.EmptyPath, "File path for client configuration")

	return cmd
}

func (c *createCommand) PreRunE(_ *cobra.Command, _ []string) error {
	// Load mandatory config
	conf, err := config.LoadClientConfig(c.configFilePath)
	if err != nil {
		return err
	}

	c.clientConfig = conf

	c.pluginRepo, err = internal.InitPlugins(config.LogLevel(c.logger.Level()))
	return err
}

func (c *createCommand) RunE(_ *cobra.Command, _ []string) error {
	namespace, err := c.namespaceSurvey.AskToSelectNamespace(c.clientConfig)
	if err != nil {
		return err
	}

	jobSpecFs := afero.NewBasePathFs(afero.NewOsFs(), namespace.Job.Path)
	jwd, err := survey.AskWorkingDirectory(jobSpecFs, "")
	if err != nil {
		return err
	}

	newDirName, err := survey.AskDirectoryName(jwd)
	if err != nil {
		return err
	}

	jobDirectory := filepath.Join(jwd, newDirName)

	jobSpecReadWriter, err := specio.NewJobSpecReadWriter(jobSpecFs)
	if err != nil {
		return err
	}

	presets, err := internal.GetProjectPresets(c.clientConfig.Project.PresetsPath)
	if err != nil {
		return fmt.Errorf("error reading presets: %w", err)
	}

	jobSpec, err := c.jobCreateSurvey.AskToCreateJob(c.pluginRepo, jobSpecReadWriter, jobDirectory, presets)
	if err != nil {
		return err
	}

	if err := jobSpecReadWriter.Write(jobDirectory, &jobSpec); err != nil {
		return fmt.Errorf("error di folder ini %s %w", jobDirectory, err)
	}

	c.logger.Info("Job successfully created at %s", jobDirectory)
	return nil
}
