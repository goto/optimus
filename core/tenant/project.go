package tenant

import (
	"strings"

	"github.com/goto/optimus/internal/errors"
	"github.com/goto/optimus/internal/utils"
)

const (
	EntityProject = "project"

	ProjectStoragePathKey   = "STORAGE_PATH"
	ProjectSchedulerHost    = "SCHEDULER_HOST"
	ProjectSchedulerVersion = "SCHEDULER_VERSION"
)

type ProjectName string

func ProjectNameFrom(name string) (ProjectName, error) {
	if name == "" {
		return "", errors.InvalidArgument(EntityProject, "project name is empty")
	}
	// TODO: add condition, that project name should not have "." as this will break the standard URN design
	return ProjectName(name), nil
}

func (pn ProjectName) String() string {
	return string(pn)
}

type Project struct {
	name   ProjectName
	config map[string]string

	presets   map[string]Preset
	locations map[string]Location
}

func (p *Project) Name() ProjectName {
	return p.name
}

func (p *Project) GetConfig(key string) (string, error) {
	for k, v := range p.config {
		if key == k {
			return v, nil
		}
	}
	return "", errors.NotFound(EntityProject, "config not found: "+key)
}

// GetConfigs returns a clone of project configurations
func (p *Project) GetConfigs() map[string]string {
	confs := make(map[string]string, len(p.config))
	for k, v := range p.config {
		confs[k] = v
	}
	return confs
}

func (p *Project) SetPresets(presets map[string]Preset) {
	if presets == nil {
		p.presets = make(map[string]Preset)
		return
	}

	p.presets = presets
}

func (p *Project) GetPresets() map[string]Preset {
	return p.presets
}

func (p *Project) GetPreset(name string) (Preset, error) {
	preset, ok := p.presets[strings.ToLower(name)]
	if !ok {
		return Preset{}, errors.NotFound(EntityProject, "preset not found "+name)
	}

	return preset, nil
}

func (p *Project) SetLocations(locations []Location) {
	if p.locations == nil {
		p.locations = make(map[string]Location)
	}

	for _, loc := range locations {
		p.locations[loc.Name()] = loc
	}
}

func (p *Project) GetLocations() map[string]Location {
	return p.locations
}

func (p *Project) GetLocationsList() []Location {
	locationsList := make([]Location, 0, len(p.locations))
	for _, loc := range p.locations {
		locationsList = append(locationsList, loc)
	}

	return locationsList
}

func (p *Project) GetLocation(name string) (Location, error) {
	location, ok := p.locations[name]
	if !ok {
		return Location{}, errors.NotFound(EntityProject, "location not found: "+name)
	}

	return location, nil
}

func NewProject(name string, config map[string]string) (*Project, error) {
	prjName, err := ProjectNameFrom(name)
	if err != nil {
		return nil, err
	}

	if !utils.Contains(config, ProjectStoragePathKey, ProjectSchedulerHost) {
		return nil, errors.InvalidArgument(EntityProject, "missing mandatory configuration")
	}

	return &Project{
		name:      prjName,
		config:    config,
		presets:   make(map[string]Preset),
		locations: make(map[string]Location),
	}, nil
}
