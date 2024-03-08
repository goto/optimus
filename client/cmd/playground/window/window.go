package window

import (
	tea "github.com/charmbracelet/bubbletea"
	"github.com/goto/salt/log"
	"github.com/spf13/cobra"

	"github.com/goto/optimus/client/cmd/internal/logger"
	"github.com/goto/optimus/client/cmd/playground/window/v1v2"
)

type command struct {
	log log.Logger

	isV1V2 bool
}

// NewCommand initializes command for window playground
func NewCommand() *cobra.Command {
	window := command{log: logger.NewClientLogger()}
	cmd := &cobra.Command{
		Use:   "window",
		Short: "Play around with window configuration",
		RunE:  window.RunE,
	}

	cmd.Flags().BoolVar(&window.isV1V2, "v1-v2", false, "if set, then playground will be for v1 and v2")
	return cmd
}

func (j *command) RunE(_ *cobra.Command, _ []string) error {
	if j.isV1V2 {
		return j.runV1V2()
	}

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

func (j *command) runV1V2() error {
	message := `
 __          __    _                              
 \ \        / /   | |                             
  \ \  /\  / /___ | |  ___  ___   _ __ ___    ___ 
   \ \/  \/ // _ \| | / __|/ _ \ | '_ ' _ \  / _ \
    \  /\  /|  __/| || (__| (_) || | | | | ||  __/
     \/  \/  \___||_| \___|\___/ |_| |_| |_| \___|
                                                  
    🚨THIS VERSION IS SOON TO BE DEPRECATED🚨

Hi, this is an interactive CLI to play around with window configuration for v1 and v2.
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
	p := tea.NewProgram(v1v2.NewModel())
	return p.Start()
}
