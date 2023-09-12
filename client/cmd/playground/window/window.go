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
	message := `
 __          __    _                              
 \ \        / /   | |                             
  \ \  /\  / /___ | |  ___  ___   _ __ ___    ___ 
   \ \/  \/ // _ \| | / __|/ _ \ | '_ ' _ \  / _ \
    \  /\  /|  __/| || (__| (_) || | | | | ||  __/
     \/  \/  \___||_| \___|\___/ |_| |_| |_| \___|
                                                  
                                                  
Hi, this is an interactive CLI to play around with window configuration.
You can navigate around the available configurations with the following keys:
   ______________________________
  |  up   | : arrow up    ↑ or w |
  | down  | : arrow down  ↓ or s |
  | right | : arrow right → or d |
  | left  | : arrow left  ← or a |
  | quit  | : q or ctrl+c        |
   ------------------------------
`
	j.log.Info(message)
	p := tea.NewProgram(newModel())
	return p.Start()
}
