package models

import (
	"errors"
	"fmt"
	"sort"

	"github.com/goto/optimus/sdk/plugin"
)

var ErrUnsupportedPlugin = errors.New("unsupported plugin requested, make sure its correctly installed")

type PluginRepository struct {
	data       map[string]*plugin.Plugin
	sortedKeys []string
}

func (s *PluginRepository) lazySortPluginKeys() {
	// already sorted
	if len(s.data) == 0 || len(s.sortedKeys) > 0 {
		return
	}

	for k := range s.data {
		s.sortedKeys = append(s.sortedKeys, k)
	}
	sort.Strings(s.sortedKeys)
}

func (s *PluginRepository) GetByName(name string) (*plugin.Plugin, error) {
	if unit, ok := s.data[name]; ok {
		return unit, nil
	}
	return nil, fmt.Errorf("%s: %w", name, ErrUnsupportedPlugin)
}

func (s *PluginRepository) GetAll() []*plugin.Plugin {
	var list []*plugin.Plugin
	s.lazySortPluginKeys() // sorts keys if not sorted
	for _, pluginName := range s.sortedKeys {
		list = append(list, s.data[pluginName])
	}
	return list
}

func (s *PluginRepository) GetTasks() []*plugin.Plugin {
	var list []*plugin.Plugin
	s.lazySortPluginKeys() // sorts keys if not sorted
	for _, pluginName := range s.sortedKeys {
		unit := s.data[pluginName]
		if unit.Info().PluginType == plugin.TypeTask {
			list = append(list, unit)
		}
	}
	return list
}

func (s *PluginRepository) GetHooks() []*plugin.Plugin {
	var list []*plugin.Plugin
	s.lazySortPluginKeys()
	for _, pluginName := range s.sortedKeys {
		unit := s.data[pluginName]
		if unit.Info().PluginType == plugin.TypeHook {
			list = append(list, unit)
		}
	}
	return list
}

func (s *PluginRepository) AddYaml(yamlMod plugin.YamlMod) error {
	info := yamlMod.PluginInfo()
	if err := info.Validate(); err != nil {
		return err
	}

	if _, ok := s.data[info.Name]; ok {
		// duplicated yaml plugin
		return fmt.Errorf("plugin name already in use %s", info.Name)
	}

	s.data[info.Name] = &plugin.Plugin{YamlMod: yamlMod}
	return nil
}

func NewPluginRepository() *PluginRepository {
	return &PluginRepository{data: map[string]*plugin.Plugin{}}
}
