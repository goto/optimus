package plugin

import (
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"gopkg.in/yaml.v3"

	"github.com/goto/optimus/internal/utils"
)

var ErrNoSuchSpec = errors.New("spec not found")

type ParserType string

const (
	BQParser         ParserType = "bq"
	MaxcomputeParser ParserType = "maxcompute"

	DefaultVersion = "default"
)

type Evaluator struct {
	Env      string `yaml:"env,omitempty"`
	FilePath string `yaml:"filepath,omitempty"`
	Selector string `yaml:"selector,omitempty"`
}

type Spec struct {
	SpecVersion int    `yaml:"version"`
	Name        string `yaml:"name"`
	Description string `yaml:"description"`

	DestinationURNTemplate string `yaml:"destination_urn_template,omitempty"`

	AssetParsers  map[ParserType][]Evaluator `yaml:"asset_parsers,omitempty"`
	PluginVersion map[string]VersionDetails  `yaml:"plugin_versions,omitempty"`
	DefaultConfig map[string]string          `yaml:"default_config,omitempty"`
}

type Entrypoint struct {
	Shell string `yaml:"shell,omitempty"`
	// Setup  string `yaml:"setup,omitempty"`
	Script string `yaml:"script,omitempty"`
}

type VersionDetails struct {
	Image      string     `yaml:"image,omitempty"`
	Tag        string     `yaml:"tag,omitempty"`
	Entrypoint Entrypoint `yaml:"entrypoint,omitempty"`
}

func (s *Spec) Validate() error {
	if s.Name == "" {
		return errors.New("plugin name is required")
	}
	if s.SpecVersion == 0 {
		return errors.New("plugin spec version is required")
	}
	if len(s.PluginVersion) == 0 {
		return errors.New("plugin versions are required")
	}

	def, ok := s.PluginVersion[DefaultVersion]
	if !ok {
		return errors.New("default version is required")
	}

	if def.Image == "" {
		return errors.New("default image is required")
	}

	if def.Tag == "" {
		return errors.New("default tag is required")
	}

	if def.Entrypoint.Script == "" {
		return errors.New("default entrypoint is required")
	}

	return nil
}

func (s *Spec) GetImage(version string) (string, error) {
	def, ok := s.PluginVersion[DefaultVersion]
	if !ok {
		return "", errors.New("default version not found")
	}

	ver := VersionDetails{}
	if version != "" {
		details, ok := s.PluginVersion[version]
		if ok {
			ver = details
		}
	}

	img := utils.GetFirstNonEmpty(ver.Image, def.Image)
	tag := utils.GetFirstNonEmpty(ver.Tag, def.Tag)

	return fmt.Sprintf("%s:%s", img, tag), nil
}

func (s *Spec) GetEntrypoint(version string) (Entrypoint, error) {
	def, ok := s.PluginVersion[DefaultVersion]
	if !ok {
		return Entrypoint{}, errors.New("default version not found")
	}

	ver := Entrypoint{}
	v1, okVer := s.PluginVersion[version]
	if okVer {
		ver = v1.Entrypoint
	}

	shell := utils.GetFirstNonEmpty(ver.Shell, def.Entrypoint.Shell, "/bin/sh")
	script := utils.GetFirstNonEmpty(ver.Script, def.Entrypoint.Script)

	return Entrypoint{
		Shell:  shell,
		Script: script,
	}, nil
}

func Load(pluginPath string) (*Spec, error) {
	fd, err := os.Open(pluginPath)
	if err != nil {
		if os.IsNotExist(err) {
			err = ErrNoSuchSpec
		}
		return nil, err
	}
	defer fd.Close()

	pluginBytes, err := io.ReadAll(fd)
	if err != nil {
		return nil, err
	}
	var plugin Spec
	if err := yaml.Unmarshal(pluginBytes, &plugin); err != nil {
		return &plugin, err
	}

	for _, ver := range plugin.PluginVersion {
		ver.Entrypoint.Shell = strings.ReplaceAll(ver.Entrypoint.Script, "\n", "; ")
	}

	return &plugin, nil
}
