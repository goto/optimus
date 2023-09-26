package internal

import (
	"os"

	"github.com/goto/salt/log"

	"github.com/goto/optimus/config"
	"github.com/goto/optimus/internal/models"
	oPlugin "github.com/goto/optimus/plugin"
)

// InitPlugins triggers initialization of all available plugins
func InitPlugins(logLevel config.LogLevel) (*models.PluginRepository, error) {
	if logLevel != config.LogLevelDebug {
		logLevel = config.LogLevelInfo
	}
	logger := log.NewLogrus(
		log.LogrusWithLevel(logLevel.String()),
		log.LogrusWithWriter(os.Stdout),
	)
	// discover and load plugins.
	pluginRepo, err := oPlugin.Initialize(logger)
	return pluginRepo, err
}
