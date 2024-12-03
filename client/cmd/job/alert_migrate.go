package job

import (
	"strings"

	"github.com/goto/salt/log"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"

	"github.com/goto/optimus/client/cmd/internal/logger"
	"github.com/goto/optimus/client/local/specio"
	"github.com/goto/optimus/config"
)

type migrateCommand struct {
	logger log.Logger

	configFilePath string
	clientConfig   *config.ClientConfig
}

func NewMigrateCommand() *cobra.Command {
	migrate := &migrateCommand{
		logger: logger.NewClientLogger(),
	}

	cmd := &cobra.Command{
		Use:     "migrate",
		RunE:    migrate.RunE,
		PreRunE: migrate.PreRunE,
	}

	cmd.Flags().StringVarP(&migrate.configFilePath, "config", "c", config.EmptyPath, "File path for client configuration")
	return cmd
}

func (v *migrateCommand) PreRunE(_ *cobra.Command, _ []string) error {
	conf, err := config.LoadClientConfig(v.configFilePath)
	if err != nil {
		return err
	}
	v.clientConfig = conf
	return nil
}

func (v *migrateCommand) RunE(_ *cobra.Command, _ []string) error {
	return v.migrate()

}

func (v *migrateCommand) migrate() error {
	jobSpecReadWriter, err := specio.NewJobSpecReadWriter(afero.NewOsFs())
	if err != nil {
		return err
	}

	for _, namespace := range v.clientConfig.Namespaces {
		jobs, err := jobSpecReadWriter.ReadAll(namespace.Job.Path)
		if err != nil {
			v.logger.Error(err.Error())
			continue
		}
		for _, job := range jobs {
			isModified := false
			for i, on := range job.Behavior.Notify {
				pagerduty := false
				for _, ch := range on.Channels {
					if strings.Contains(ch, "pagerduty://") {
						pagerduty = true
						break
					}
				}
				if pagerduty {
					job.Behavior.Notify[i].Severity = "CRITICAL"
					v.logger.Info("Modified job %s", job.Path)
					isModified = true
				}
			}
			if isModified {
				jobSpecReadWriter.Write(job.Path, job)
			}
		}
	}

	return nil
}
