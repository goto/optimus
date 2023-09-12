package model

type Preset struct {
	Description string            `yaml:"description"`
	Window      JobSpecTaskWindow `yaml:"window"`
}

type PresetsMap struct {
	Presets map[string]Preset `yaml:"presets"`
}
