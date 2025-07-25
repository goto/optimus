package cmd

import (
	"github.com/MakeNowJust/heredoc"
	"github.com/goto/salt/cmdx"
	cli "github.com/spf13/cobra"

	"github.com/goto/optimus/client/cmd/apply"
	"github.com/goto/optimus/client/cmd/backup"
	"github.com/goto/optimus/client/cmd/extension"
	"github.com/goto/optimus/client/cmd/initialize"
	"github.com/goto/optimus/client/cmd/job"
	"github.com/goto/optimus/client/cmd/namespace"
	"github.com/goto/optimus/client/cmd/playground"
	"github.com/goto/optimus/client/cmd/project"
	"github.com/goto/optimus/client/cmd/replay"
	"github.com/goto/optimus/client/cmd/resource"
	"github.com/goto/optimus/client/cmd/scheduler"
	"github.com/goto/optimus/client/cmd/secret"
	"github.com/goto/optimus/client/cmd/verify"
	"github.com/goto/optimus/client/cmd/version"
)

// New constructs the 'root' command. It houses all other sub commands
// default output of logging should go to stdout
// interactive output like progress bars should go to stderr
// unless the stdout/err is a tty, colors/progressbar should be disabled
func New() *cli.Command {
	cmd := &cli.Command{
		Use: "optimus <command> <subcommand> [flags]",
		Long: heredoc.Doc(`
			Optimus is an easy-to-use, reliable, and performant workflow orchestrator for 
			data transformation, data modeling, pipelines, and data quality management.
		
			For passing authentication header, set one of the following environment
			variables:
			1. OPTIMUS_AUTH_BASIC_TOKEN
			2. OPTIMUS_AUTH_BEARER_TOKEN`),
		SilenceUsage: true,
		Example: heredoc.Doc(`
				$ optimus job create
				$ optimus backup create
				$ optimus backup list
			`),
		Annotations: map[string]string{
			"group:core": "true",
			"help:learn": heredoc.Doc(`
				Use 'optimus <command> <subcommand> --help' for more information about a command.
				Read the manual at https://goto.github.io/optimus/
			`),
			"help:feedback": heredoc.Doc(`
				Open an issue here https://github.com/goto/optimus/issues
			`),
		},
	}

	cmdx.SetHelp(cmd)

	// Client related commands
	cmd.AddCommand(
		backup.NewBackupCommand(),
		initialize.NewInitializeCommand(),
		job.NewJobCommand(),
		namespace.NewNamespaceCommand(),
		project.NewProjectCommand(),
		resource.NewResourceCommand(),
		secret.NewSecretCommand(),
		version.NewVersionCommand(),
		playground.NewPlaygroundCommand(),
		scheduler.NewSchedulerCommand(),
		replay.NewReplayCommand(),
		apply.NewApplyCommand(),
		verify.NewVerifyCommand(),
	)

	extension.UpdateWithExtension(cmd)
	return cmd
}
