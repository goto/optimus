package apply

import (
	"github.com/goto/optimus/client/cmd/internal"
	"github.com/goto/optimus/client/cmd/internal/connection"
	"github.com/goto/optimus/client/cmd/internal/logger"
	"github.com/goto/optimus/config"
	"github.com/goto/salt/log"
	"github.com/spf13/cobra"
)

type applyCommand struct {
	logger     log.Logger
	connection connection.Connection

	configFilePath string

	sources []string

	projectName string
	host        string
	namespace   string
}

// NewApplyCommand apply the job / resource changes
// based on the result of optimus job/resource plan
// contract: job,p-godata-id,batching,sample_select,create,true
func NewApplyCommand() *cobra.Command {
	apply := &applyCommand{}
	cmd := &cobra.Command{
		Use:     "apply",
		Short:   "Apply job/resource changes",
		Example: "optimus apply --sources <plan_sources_path>...",
		RunE:    nil,
		PreRunE: apply.PreRunE,
	}
	return cmd
}

func (c *applyCommand) injectFlags(cmd *cobra.Command) {
	// Config filepath flag
	cmd.PersistentFlags().StringVarP(&c.configFilePath, "config", "c", config.EmptyPath, "File path for client configuration")

	cmd.Flags().StringSliceVarP(&c.sources, "sources", "s", c.sources, "Sources of plan result to be executed")

	// Mandatory flags if config is not set
	cmd.Flags().StringVar(&c.host, "host", "", "Optimus service endpoint url")
}

func (c *applyCommand) PreRunE(cmd *cobra.Command, _ []string) error {
	// Load config
	conf, err := internal.LoadOptionalConfig(c.configFilePath)
	if err != nil {
		return err
	}
	defer func() {
		c.logger = logger.NewClientLogger()
		c.connection = connection.New(c.logger, conf)
	}()

	if conf == nil {
		internal.MarkFlagsRequired(cmd, []string{"host"})
		return nil
	}

	c.host = conf.Host
	return nil
}

func (c *applyCommand) RunE(_ *cobra.Command, _ []string) error {
	return nil
}
