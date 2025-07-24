package verify

import (
	"github.com/spf13/cobra"
)

// NewVerifyCommand initializes command for replay
func NewVerifyCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "verify",
		Short: "optimus verify",
		Annotations: map[string]string{
			"group:core": "true",
		},
	}

	cmd.AddCommand(
		NewValidateSpecCommand(),
	)
	return cmd
}
