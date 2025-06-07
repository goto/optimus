package plugin

import (
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/goto/salt/log"
)

const (
	Prefix     = "optimus-plugin-"
	Suffix     = ".yaml"
	PluginsDir = ".plugins"
)

func LoadPluginToStore(l log.Logger) (*Store, error) {
	discoveredYamlPlugins := discoverPluginsGivenFilePattern(l, Prefix, Suffix)
	l.Debug(fmt.Sprintf("discovering yaml   plugins(%d)...", len(discoveredYamlPlugins)))
	return InitStore(discoveredYamlPlugins, l)
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

			if len(strings.Split(fullName, "-")) < 2 { //nolint: mnd
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
