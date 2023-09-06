package internal

import (
	"fmt"

	"github.com/spf13/afero"
	"gopkg.in/yaml.v3"

	"github.com/goto/optimus/client/local/model"
)

func GetProjectPresets(presetsPath string) (model.PresetsMap, error) {
	if presetsPath == "" {
		return model.PresetsMap{}, nil
	}

	specFS := afero.NewOsFs()
	fileSpec, err := specFS.Open(presetsPath)
	if err != nil {
		return model.PresetsMap{}, fmt.Errorf("error opening presets file[%s]: %w", presetsPath, err)
	}
	defer fileSpec.Close()

	var spec model.PresetsMap
	if err := yaml.NewDecoder(fileSpec).Decode(&spec); err != nil {
		return model.PresetsMap{}, fmt.Errorf("error decoding spec under [%s]: %w", presetsPath, err)
	}
	return spec, nil
}
