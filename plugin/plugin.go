package plugin

import (
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/goto/optimus/internal/models"
	"github.com/goto/optimus/plugin/yaml"
	"github.com/goto/salt/log"
)

func Initialize(l log.Logger) (*models.PluginRepository, error) {
	pluginRepository := models.NewPluginRepository()
	// fetch yaml plugins first, it holds detailed information about the plugin
	discoveredYamlPlugins := discoverPluginsGivenFilePattern(l, yaml.Prefix, yaml.Suffix)
	l.Debug(fmt.Sprintf("discovering yaml   plugins(%d)...", len(discoveredYamlPlugins)))
	err := yaml.Init(pluginRepository, discoveredYamlPlugins, l)

	return pluginRepository, err
}

// discoverPluginsGivenFilePattern look for plugin with the specific pattern in following folders
// order to search is top to down
// ./
// <exec>/
// <exec>/.optimus/plugins
// $HOME/.optimus/plugins
// /usr/bin
// /usr/local/bin
//
// for duplicate plugins(even with different versions for now), only the first found will be used
// sample plugin name:
// - optimus-myplugin_linux_amd64 | with suffix: optimus- and prefix: _linux_amd64
// - optimus-plugin-myplugin.yaml | with suffix: optimus-plugin and prefix: .yaml
func discoverPluginsGivenFilePattern(l log.Logger, prefix, suffix string) []string {
	var discoveredPlugins, dirs []string

	if p, err := os.Getwd(); err == nil {
		dirs = append(dirs, path.Join(p, PluginsDir), p)
	} else {
		l.Debug(fmt.Sprintf("Error discovering working dir: %s", err))
	}

	// look in the same directory as the executable
	if exePath, err := os.Executable(); err != nil {
		l.Debug(fmt.Sprintf("Error discovering exe directory: %s", err))
	} else {
		dirs = append(dirs, filepath.Dir(exePath))
	}

	// add user home directory
	if currentHomeDir, err := os.UserHomeDir(); err == nil {
		dirs = append(dirs, filepath.Join(currentHomeDir, ".optimus", "plugins"))
	}
	dirs = append(dirs, []string{"/usr/bin", "/usr/local/bin"}...)

	for _, dirPath := range dirs {
		fileInfos, err := os.ReadDir(dirPath)
		if err != nil {
			continue
		}

		for _, item := range fileInfos {
			fullName := item.Name()

			if !strings.HasPrefix(fullName, prefix) {
				continue
			}
			if !strings.HasSuffix(fullName, suffix) {
				continue
			}

			absPath, err := filepath.Abs(filepath.Join(dirPath, fullName))
			if err != nil {
				continue
			}

			info, err := os.Stat(absPath)
			if err != nil {
				continue
			}
			if info.IsDir() {
				continue
			}

			if len(strings.Split(fullName, "-")) < 2 { //nolint: gomnd
				continue
			}

			// get plugin name
			pluginName := strings.Split(fullName, "_")[0]
			absPath = filepath.Clean(absPath)

			// check for duplicate binaries, could be different versions
			// if we have already discovered one, ignore rest
			isAlreadyFound := false
			for _, storedName := range discoveredPlugins {
				if strings.Contains(storedName, pluginName) {
					isAlreadyFound = true
				}
			}

			if !isAlreadyFound {
				discoveredPlugins = append(discoveredPlugins, absPath)
			}
		}
	}
	return discoveredPlugins
}
