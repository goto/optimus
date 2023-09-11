package internal

import (
	"os"

	hPlugin "github.com/hashicorp/go-plugin"

	"github.com/goto/optimus/config"
	"github.com/goto/optimus/internal/models"
	oPlugin "github.com/goto/optimus/plugin"
	"github.com/goto/salt/log"
)

// InitPlugins triggers initialization of all available plugins
func InitPlugins(logLevel config.LogLevel) (*models.PluginRepository, error) {
	logger := log.NewLogrus(
		log.LogrusWithLevel(logLevel.String()),
		log.LogrusWithWriter(os.Stdout),
	)
	// discover and load plugins.
	pluginRepo, err := oPlugin.Initialize(logger)
	return pluginRepo, err
}

func CleanupPlugins() {
	hPlugin.CleanupClients()
}
