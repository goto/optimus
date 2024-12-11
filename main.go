package main

import (
	"errors"
	"fmt"
	"math/rand"
	"os"
	"time"

	_ "go.uber.org/automaxprocs"

	clientCmd "github.com/goto/optimus/client/cmd"
	_ "github.com/goto/optimus/client/extension/provider"
	lerrors "github.com/goto/optimus/client/local/errors"
	server "github.com/goto/optimus/server/cmd"
	"github.com/goto/optimus/server/cmd/migration"
)

const DefaultExitCode = 1

var errRequestFail = errors.New("🔥 unable to complete request successfully")

//nolint:forbidigo
func main() {
	rand.Seed(time.Now().UTC().UnixNano())

	command := clientCmd.New()

	// Add Server related commands
	command.AddCommand(
		server.NewServeCommand(),
		migration.NewMigrationCommand(),
	)

	if err := command.Execute(); err != nil {
		fmt.Println(errRequestFail)
		Exit(err)
	}
}

func Exit(err error) {
	var cmdErr *lerrors.CmdError
	if errors.As(err, &cmdErr) {
		os.Exit(cmdErr.Code)
		return
	}
	os.Exit(DefaultExitCode)
}
