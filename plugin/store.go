package plugin

import (
	"errors"
	"fmt"

	"github.com/goto/salt/log"
)

var ErrUnsupportedPlugin = errors.New("unsupported plugin requested, make sure its correctly installed")

type Store struct {
	data map[string]*Spec
}

func (s *Store) GetByName(name string) (*Spec, error) {
	if unit, ok := s.data[name]; ok {
		return unit, nil
	}
	return nil, fmt.Errorf("%s: %w", name, ErrUnsupportedPlugin)
}

func (s *Store) All(yield func(spec *Spec) bool) {
	for _, unit := range s.data {
		if !yield(unit) {
			return
		}
	}
}

func (s *Store) Add(pluginSpec *Spec) error {
	if err := pluginSpec.Validate(); err != nil {
		return err
	}

	if _, ok := s.data[pluginSpec.Name]; ok {
		// duplicated yaml plugin
		return fmt.Errorf("plugin name already in use %s", pluginSpec.Name)
	}

	s.data[pluginSpec.Name] = pluginSpec
	return nil
}

func NewPluginStore() *Store {
	return &Store{data: map[string]*Spec{}}
}

func InitStore(pluginPaths []string, l log.Logger) (*Store, error) {
	store := NewPluginStore()

	for _, pluginPath := range pluginPaths {
		pluginSpec, err := Load(pluginPath)
		if err != nil {
			l.Error(fmt.Sprintf("plugin store Init: %s", pluginPath), err)
			return nil, err
		}

		if err = store.Add(pluginSpec); err != nil {
			l.Error(fmt.Sprintf("Store.Add: %s", pluginPath), err)
			return nil, err
		}
		l.Debug("plugin ready: ", pluginSpec.Name)
	}

	return store, nil
}
