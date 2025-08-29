package window

import (
	tea "github.com/charmbracelet/bubbletea"
	"github.com/goto/salt/log"
	"github.com/spf13/cobra"

	"github.com/goto/optimus/client/cmd/internal/logger"
)

type command struct {
	log log.Logger
}

// NewCommand initializes command for window playground
func NewCommand() *cobra.Command {
	window := command{log: logger.NewClientLogger()}
	cmd := &cobra.Command{
		Use:   "window",
		Short: "Play around with window configuration",
		RunE:  window.RunE,
	}

	return cmd
}

func (j *command) RunE(_ *cobra.Command, _ []string) error {
	return j.runV3()
}

func (j *command) runV3() error {
	welcome := `
 __          __    _                              
 \ \        / /   | |                             
  \ \  /\  / /___ | |  ___  ___   _ __ ___    ___ 
   \ \/  \/ // _ \| | / __|/ _ \ | '_ ' _ \  / _ \
    \  /\  /|  __/| || (__| (_) || | | | | ||  __/
     \/  \/  \___||_| \___|\___/ |_| |_| |_| \___|
`

	instruction := `
                                       _________________________
Hi, this is an interactive CLI to     |  up   | : arrow up    ↑ |
play around with window v3.           | down  | : arrow down  ↓ |
You can navigate around the           | right | : arrow right → |
available configurations with         | left  | : arrow left  ← |
the following keys                    | quit  | : q or ctrl+c   |
                                       -------------------------
`
	j.log.Info(welcome)
	j.log.Info(instruction)
	p := tea.NewProgram(newModel())
	return p.Start()
}
