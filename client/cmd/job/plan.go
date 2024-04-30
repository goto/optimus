package job

import (
	"os"

	"github.com/goto/optimus/client/cmd/internal/logger"
	"github.com/goto/salt/log"
	"github.com/spf13/cobra"
)

type planCommand struct {
	logger log.Logger

	gitUrl, gitToken, gitProvider string
	verbose                       bool

	resultFilePath string
}

func NewPlanCommand() *cobra.Command {
	planCmd := &planCommand{logger: logger.NewClientLogger()}

	cmd := &cobra.Command{
		Use:     "plan",
		Short:   "Plan Job Deployment",
		Long:    "PLan job deployment based on git diff state",
		Example: "optimus job plan",
		PreRunE: planCmd.PreRunE,
		RunE:    planCmd.RunE,
	}

	planCmd.inject(cmd)
	return cmd
}

func (p *planCommand) inject(cmd *cobra.Command) {
	cmd.Flags().StringVarP(&p.gitProvider, "git-provider", "gp", os.Getenv("GIT_PROVIDER"), "selected git provider used in the repository")
	cmd.Flags().StringVarP(&p.gitUrl, "git-url", "gu", os.Getenv("GIT_URL"), "git url based on git provider used in the repository")
	cmd.Flags().StringVarP(&p.gitToken, "git-token", "gt", os.Getenv("GIT_TOKEN"), "git token based on git provider used in the repository")

	cmd.Flags().StringVarP(&p.resultFilePath, "resultPath", "rp", "./plan", "File path for plan result")

	cmd.Flags().BoolVarP(&p.verbose, "verbose", "v", false, "Print details related to operation")
}

func (p *planCommand) PreRunE(_ *cobra.Command, _ []string) error {
	return createDirectoryIfNotExist(p.resultFilePath)
}

func createDirectoryIfNotExist(directory string) error {
	_, err := os.Stat(directory)
	if err != nil {
		if !os.IsNotExist(err) {
			return err
		}

		if err = os.MkdirAll(directory, os.ModeDir); err != nil {
			return err
		}
	}

	return nil
}

func (p *planCommand) RunE(_ *cobra.Command, _ []string) error {

	return nil
}
