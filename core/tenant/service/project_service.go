package service

import (
	"context"

	"github.com/goto/optimus/core/tenant"
	"github.com/goto/optimus/internal/errors"
)

type ProjectService struct {
	projectRepo ProjectRepository
	presetRepo  PresetRepository
}

func NewProjectService(projectRepo ProjectRepository, presetRepo PresetRepository) *ProjectService {
	return &ProjectService{
		projectRepo: projectRepo,
		presetRepo:  presetRepo,
	}
}

type ProjectRepository interface {
	Save(context.Context, *tenant.Project) error
	GetByName(context.Context, tenant.ProjectName) (*tenant.Project, error)
	GetAll(context.Context) ([]*tenant.Project, error)
}

type PresetRepository interface {
	Create(ctx context.Context, projectName tenant.ProjectName, preset tenant.Preset) error
	Read(ctx context.Context, projectName tenant.ProjectName) ([]tenant.Preset, error)
	Update(ctx context.Context, projectName tenant.ProjectName, preset tenant.Preset) error
	Delete(ctx context.Context, projectName tenant.ProjectName, presetName string) error
}

func (s ProjectService) Save(ctx context.Context, project *tenant.Project) error {
	if err := s.projectRepo.Save(ctx, project); err != nil {
		return err
	}

	return s.replacePresets(ctx, project.Name(), project.GetPresets())
}

func (s ProjectService) Get(ctx context.Context, name tenant.ProjectName) (*tenant.Project, error) {
	project, err := s.projectRepo.GetByName(ctx, name)
	if err != nil {
		return nil, err
	}

	presets, err := s.getPresets(ctx, name)
	if err != nil {
		return nil, err
	}

	project.SetPresets(presets)
	return project, nil
}

func (s ProjectService) GetAll(ctx context.Context) ([]*tenant.Project, error) {
	projects, err := s.projectRepo.GetAll(ctx)
	if err != nil {
		return nil, err
	}

	me := errors.NewMultiError("getting relevant presets")
	for _, p := range projects {
		presets, err := s.getPresets(ctx, p.Name())
		if err != nil {
			me.Append(err)
			continue
		}

		p.SetPresets(presets)
	}

	return projects, me.ToErr()
}

func (p ProjectService) getPresets(ctx context.Context, projectName tenant.ProjectName) (map[string]tenant.Preset, error) {
	existings, err := p.presetRepo.Read(ctx, projectName)
	if err != nil {
		return nil, err
	}

	output := make(map[string]tenant.Preset)
	for _, e := range existings {
		output[e.Name()] = e
	}

	return output, nil
}

func (p ProjectService) replacePresets(ctx context.Context, projectName tenant.ProjectName, incomings map[string]tenant.Preset) error {
	existings, err := p.presetRepo.Read(ctx, projectName)
	if err != nil {
		return err
	}

	me := errors.NewMultiError("replace presets within project")

	toCreate, toUpdate, toDelete := p.getPresetsDiff(incomings, existings)
	for _, preset := range toCreate {
		me.Append(p.presetRepo.Create(ctx, projectName, preset))
	}

	for _, preset := range toUpdate {
		me.Append(p.presetRepo.Update(ctx, projectName, preset))
	}

	for _, preset := range toDelete {
		me.Append(p.presetRepo.Delete(ctx, projectName, preset.Name()))
	}

	return me.ToErr()
}

func (p ProjectService) getPresetsDiff(incomings map[string]tenant.Preset, existings []tenant.Preset) (toCreate, toUpdate, toDelete []tenant.Preset) {
	existingsMap := make(map[string]tenant.Preset)
	for _, existing := range existings {
		if _, ok := incomings[existing.Name()]; !ok {
			toDelete = append(toDelete, existing)
			continue
		}

		existingsMap[existing.Name()] = existing
	}

	for _, incoming := range incomings {
		if existing, ok := existingsMap[incoming.Name()]; ok {
			if !existing.Equal(incoming) {
				toUpdate = append(toUpdate, incoming)
			}
			continue
		}

		toCreate = append(toCreate, incoming)
	}

	return toCreate, toUpdate, toDelete
}
