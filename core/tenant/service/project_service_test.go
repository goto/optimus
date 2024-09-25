package service_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/goto/optimus/core/tenant"
	"github.com/goto/optimus/core/tenant/service"
)

func TestProjectService(t *testing.T) {
	ctx := context.Background()
	conf := map[string]string{
		tenant.ProjectSchedulerHost:  "host",
		tenant.ProjectStoragePathKey: "gs://location",
		"BUCKET":                     "gs://some_folder",
	}
	savedProject, _ := tenant.NewProject("savedProj", conf)

	preset, err := tenant.NewPreset("test_preset", "preset for testing", "1d", "1h", "", "")
	assert.NoError(t, err)

	presetsMap := map[string]tenant.Preset{
		"test_preset": preset,
	}
	location := tenant.NewLocation("location-name", "data-mart", "dataset")
	locations := []tenant.Location{location}

	savedProject.SetPresets(presetsMap)
	savedProject.SetLocations(locations)

	t.Run("Save", func(t *testing.T) {
		t.Run("returns error when fails in saving project", func(t *testing.T) {
			projectRepo := new(projectRepo)
			projectRepo.On("Save", ctx, mock.Anything).Return(errors.New("error in saving"))
			defer projectRepo.AssertExpectations(t)

			presetRepo := new(presetRepo)
			locationRepo := new(locationRepo)

			toSaveProj, _ := tenant.NewProject("proj", conf)

			projService := service.NewProjectService(projectRepo, presetRepo, locationRepo)
			err := projService.Save(ctx, toSaveProj)

			assert.NotNil(t, err)
			assert.EqualError(t, err, "error in saving")
		})
		t.Run("returns error when fails in saving presets", func(t *testing.T) {
			projectRepo := new(projectRepo)
			projectRepo.On("Save", ctx, mock.Anything).Return(nil)
			defer projectRepo.AssertExpectations(t)

			presetRepo := new(presetRepo)
			defer presetRepo.AssertExpectations(t)
			locationRepo := new(locationRepo)
			defer locationRepo.AssertExpectations(t)

			toSaveProj, _ := tenant.NewProject("proj", conf)
			toSaveProj.SetPresets(presetsMap)

			presetRepo.On("Read", ctx, toSaveProj.Name()).Return([]tenant.Preset{}, nil)
			presetRepo.On("Create", ctx, toSaveProj.Name(), preset).Return(errors.New("error in creating preset"))

			projService := service.NewProjectService(projectRepo, presetRepo, locationRepo)
			err = projService.Save(ctx, toSaveProj)

			assert.NotNil(t, err)
			assert.ErrorContains(t, err, "error in creating preset")
		})
		t.Run("saves the project successfully", func(t *testing.T) {
			projectRepo := new(projectRepo)
			projectRepo.On("Save", ctx, mock.Anything).Return(nil)
			defer projectRepo.AssertExpectations(t)

			presetRepo := new(presetRepo)
			defer presetRepo.AssertExpectations(t)
			locationRepo := new(locationRepo)
			defer locationRepo.AssertExpectations(t)

			toSaveProj, _ := tenant.NewProject("proj", conf)
			toSaveProj.SetPresets(presetsMap)
			toSaveProj.SetLocations(locations)

			presetRepo.On("Read", ctx, toSaveProj.Name()).Return([]tenant.Preset{}, nil)
			presetRepo.On("Create", ctx, toSaveProj.Name(), preset).Return(nil)

			locationRepo.On("Get", ctx, toSaveProj.Name()).Return([]tenant.Location{}, nil)
			locationRepo.On("Create", ctx, toSaveProj.Name(), location).Return(nil)

			projService := service.NewProjectService(projectRepo, presetRepo, locationRepo)
			err := projService.Save(ctx, toSaveProj)

			assert.Nil(t, err)
		})
	})
	t.Run("GetAll", func(t *testing.T) {
		t.Run("returns error when service returns error", func(t *testing.T) {
			projectRepo := new(projectRepo)
			projectRepo.On("GetAll", ctx).
				Return(nil, errors.New("error in getting all"))
			defer projectRepo.AssertExpectations(t)

			presetRepo := new(presetRepo)
			defer presetRepo.AssertExpectations(t)
			locationRepo := new(locationRepo)
			defer locationRepo.AssertExpectations(t)

			projService := service.NewProjectService(projectRepo, presetRepo, locationRepo)
			_, err := projService.GetAll(ctx)

			assert.NotNil(t, err)
			assert.EqualError(t, err, "error in getting all")
		})
		t.Run("returns error when getting presets returns error", func(t *testing.T) {
			projectRepo := new(projectRepo)
			projectRepo.On("GetAll", ctx).
				Return([]*tenant.Project{savedProject}, nil)
			defer projectRepo.AssertExpectations(t)

			presetRepo := new(presetRepo)
			defer presetRepo.AssertExpectations(t)
			locationRepo := new(locationRepo)
			defer locationRepo.AssertExpectations(t)

			presetRepo.On("Read", ctx, savedProject.Name()).Return(nil, errors.New("error getting presets"))

			projService := service.NewProjectService(projectRepo, presetRepo, locationRepo)
			_, err := projService.GetAll(ctx)

			assert.NotNil(t, err)
			assert.ErrorContains(t, err, "error getting presets")
		})
		t.Run("returns the list of saved projects", func(t *testing.T) {
			projectRepo := new(projectRepo)
			projectRepo.On("GetAll", ctx).
				Return([]*tenant.Project{savedProject}, nil)
			defer projectRepo.AssertExpectations(t)

			presetRepo := new(presetRepo)
			defer presetRepo.AssertExpectations(t)
			locationRepo := new(locationRepo)
			defer locationRepo.AssertExpectations(t)

			presetRepo.On("Read", ctx, savedProject.Name()).Return([]tenant.Preset{preset}, nil)

			projService := service.NewProjectService(projectRepo, presetRepo, locationRepo)
			projs, err := projService.GetAll(ctx)

			assert.Nil(t, err)
			assert.Equal(t, 1, len(projs))
		})
	})
	t.Run("Get", func(t *testing.T) {
		t.Run("returns error when service returns error", func(t *testing.T) {
			projectRepo := new(projectRepo)
			projectRepo.On("GetByName", ctx, tenant.ProjectName("savedProj")).
				Return(nil, errors.New("error in getting"))
			defer projectRepo.AssertExpectations(t)

			presetRepo := new(presetRepo)
			defer presetRepo.AssertExpectations(t)
			locationRepo := new(locationRepo)
			defer locationRepo.AssertExpectations(t)

			projService := service.NewProjectService(projectRepo, presetRepo, locationRepo)
			_, err := projService.Get(ctx, savedProject.Name())

			assert.NotNil(t, err)
			assert.EqualError(t, err, "error in getting")
		})
		t.Run("returns error when getting presets returns error", func(t *testing.T) {
			projectRepo := new(projectRepo)
			projectRepo.On("GetByName", ctx, tenant.ProjectName("savedProj")).Return(savedProject, nil)
			defer projectRepo.AssertExpectations(t)

			presetRepo := new(presetRepo)
			defer presetRepo.AssertExpectations(t)
			locationRepo := new(locationRepo)
			defer locationRepo.AssertExpectations(t)

			presetRepo.On("Read", ctx, savedProject.Name()).Return(nil, errors.New("error getting presets"))

			projService := service.NewProjectService(projectRepo, presetRepo, locationRepo)
			_, err := projService.Get(ctx, savedProject.Name())

			assert.NotNil(t, err)
			assert.EqualError(t, err, "error getting presets")
		})
		t.Run("returns the project successfully", func(t *testing.T) {
			projectRepo := new(projectRepo)
			projectRepo.On("GetByName", ctx, tenant.ProjectName("savedProj")).Return(savedProject, nil)
			defer projectRepo.AssertExpectations(t)

			presetRepo := new(presetRepo)
			defer presetRepo.AssertExpectations(t)
			locationRepo := new(locationRepo)
			defer locationRepo.AssertExpectations(t)

			presetRepo.On("Read", ctx, savedProject.Name()).Return([]tenant.Preset{preset}, nil)
			locationRepo.On("Get", ctx, savedProject.Name()).Return(locations, nil)

			projService := service.NewProjectService(projectRepo, presetRepo, locationRepo)
			proj, err := projService.Get(ctx, savedProject.Name())

			assert.Nil(t, err)
			assert.Equal(t, savedProject.Name(), proj.Name())
		})
	})
}

type projectRepo struct {
	mock.Mock
}

func (p *projectRepo) Save(ctx context.Context, project *tenant.Project) error {
	args := p.Called(ctx, project)
	return args.Error(0)
}

func (p *projectRepo) GetByName(ctx context.Context, name tenant.ProjectName) (*tenant.Project, error) {
	args := p.Called(ctx, name)
	var prj *tenant.Project
	if args.Get(0) != nil {
		prj = args.Get(0).(*tenant.Project)
	}
	return prj, args.Error(1)
}

func (p *projectRepo) GetAll(ctx context.Context) ([]*tenant.Project, error) {
	args := p.Called(ctx)
	var prjs []*tenant.Project
	if args.Get(0) != nil {
		prjs = args.Get(0).([]*tenant.Project)
	}
	return prjs, args.Error(1)
}

type presetRepo struct {
	mock.Mock
}

func (p *presetRepo) Create(ctx context.Context, projectName tenant.ProjectName, preset tenant.Preset) error {
	args := p.Called(ctx, projectName, preset)
	return args.Error(0)
}

func (p *presetRepo) Read(ctx context.Context, projectName tenant.ProjectName) ([]tenant.Preset, error) {
	args := p.Called(ctx, projectName)
	var presets []tenant.Preset
	if args.Get(0) != nil {
		presets = args.Get(0).([]tenant.Preset)
	}
	return presets, args.Error(1)
}

func (p *presetRepo) Update(ctx context.Context, projectName tenant.ProjectName, preset tenant.Preset) error {
	args := p.Called(ctx, projectName, preset)
	return args.Error(0)
}

func (p *presetRepo) Delete(ctx context.Context, projectName tenant.ProjectName, presetName string) error {
	args := p.Called(ctx, projectName, presetName)
	return args.Error(0)
}

// locationRepo is an autogenerated mock type for the LocationRepository type
type locationRepo struct {
	mock.Mock
}

// Create provides a mock function with given fields: ctx, projectName, location
func (_m *locationRepo) Create(ctx context.Context, projectName tenant.ProjectName, location tenant.Location) error {
	ret := _m.Called(ctx, projectName, location)

	if len(ret) == 0 {
		panic("no return value specified for Create")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, tenant.ProjectName, tenant.Location) error); ok {
		r0 = rf(ctx, projectName, location)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// Delete provides a mock function with given fields: ctx, projectName, locationName
func (_m *locationRepo) Delete(ctx context.Context, projectName tenant.ProjectName, locationName string) error {
	ret := _m.Called(ctx, projectName, locationName)

	if len(ret) == 0 {
		panic("no return value specified for Delete")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, tenant.ProjectName, string) error); ok {
		r0 = rf(ctx, projectName, locationName)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// Get provides a mock function with given fields: ctx, projectName
func (_m *locationRepo) Get(ctx context.Context, projectName tenant.ProjectName) ([]tenant.Location, error) {
	ret := _m.Called(ctx, projectName)

	if len(ret) == 0 {
		panic("no return value specified for Get")
	}

	var r0 []tenant.Location
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context, tenant.ProjectName) ([]tenant.Location, error)); ok {
		return rf(ctx, projectName)
	}
	if rf, ok := ret.Get(0).(func(context.Context, tenant.ProjectName) []tenant.Location); ok {
		r0 = rf(ctx, projectName)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]tenant.Location)
		}
	}

	if rf, ok := ret.Get(1).(func(context.Context, tenant.ProjectName) error); ok {
		r1 = rf(ctx, projectName)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// Update provides a mock function with given fields: ctx, projectName, location
func (_m *locationRepo) Update(ctx context.Context, projectName tenant.ProjectName, location tenant.Location) error {
	ret := _m.Called(ctx, projectName, location)

	if len(ret) == 0 {
		panic("no return value specified for Update")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, tenant.ProjectName, tenant.Location) error); ok {
		r0 = rf(ctx, projectName, location)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}
