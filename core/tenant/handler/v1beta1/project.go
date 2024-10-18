package v1beta1

import (
	"context"
	"fmt"
	"strings"

	"github.com/goto/salt/log"

	"github.com/goto/optimus/core/tenant"
	"github.com/goto/optimus/internal/errors"
	pb "github.com/goto/optimus/protos/gotocompany/optimus/core/v1beta1"
)

type ProjectHandler struct {
	l              log.Logger
	projectService ProjectService

	pb.UnimplementedProjectServiceServer
}

type ProjectService interface {
	Save(context.Context, *tenant.Project) error
	Get(context.Context, tenant.ProjectName) (*tenant.Project, error)
	GetAll(context.Context) ([]*tenant.Project, error)
}

type TenantService interface {
	GetDetails(ctx context.Context, tnnt tenant.Tenant) (*tenant.WithDetails, error)
	GetSecrets(ctx context.Context, tnnt tenant.Tenant) ([]*tenant.PlainTextSecret, error)
	GetSecret(ctx context.Context, tnnt tenant.Tenant, name string) (*tenant.PlainTextSecret, error)
}

func (ph *ProjectHandler) RegisterProject(ctx context.Context, req *pb.RegisterProjectRequest) (*pb.RegisterProjectResponse, error) {
	project, err := fromProjectProto(req.GetProject())
	if err != nil {
		ph.l.Error("error adapting project: %s", err)
		return nil, errors.GRPCErr(err, fmt.Sprintf("not able to register project %s", req.GetProject().Name))
	}
	if err := ph.projectService.Save(ctx, project); err != nil {
		ph.l.Error("error saving project: %s", err)
		return nil, errors.GRPCErr(err, fmt.Sprintf("not able to register project %s", req.GetProject().Name))
	}

	return &pb.RegisterProjectResponse{}, nil
}

func (ph *ProjectHandler) ListProjects(ctx context.Context, _ *pb.ListProjectsRequest) (*pb.ListProjectsResponse, error) {
	projects, err := ph.projectService.GetAll(ctx)
	if err != nil {
		ph.l.Error("error getting all projects: %s", err)
		return nil, errors.GRPCErr(err, "failed to retrieve saved projects")
	}

	var projSpecsProto []*pb.ProjectSpecification
	for _, project := range projects {
		projSpecsProto = append(projSpecsProto, toProjectProto(project))
	}

	return &pb.ListProjectsResponse{
		Projects: projSpecsProto,
	}, nil
}

func (ph *ProjectHandler) GetProject(ctx context.Context, req *pb.GetProjectRequest) (*pb.GetProjectResponse, error) {
	projName, err := tenant.ProjectNameFrom(req.GetProjectName())
	if err != nil {
		ph.l.Error("error adapting project name [%s]: %s", req.GetProjectName(), err)
		return nil, errors.GRPCErr(err, fmt.Sprintf("failed to retrieve project [%s]", req.GetProjectName()))
	}
	project, err := ph.projectService.Get(ctx, projName)
	if err != nil {
		ph.l.Error("error getting project [%s]: %s", projName, err)
		return nil, errors.GRPCErr(err, fmt.Sprintf("failed to retrieve project [%s]", req.GetProjectName()))
	}
	return &pb.GetProjectResponse{
		Project: toProjectProto(project),
	}, nil
}

func NewProjectHandler(l log.Logger, projectService ProjectService) *ProjectHandler {
	return &ProjectHandler{
		l:              l,
		projectService: projectService,
	}
}

func fromProjectProto(conf *pb.ProjectSpecification) (*tenant.Project, error) {
	pConf := map[string]string{}
	for key, val := range conf.GetConfig() {
		pConf[strings.ToUpper(key)] = val
	}

	variablesMap := map[string]string{}
	for key, val := range conf.GetVariables() {
		variablesMap[strings.ToUpper(key)] = val
	}

	presets := make(map[string]tenant.Preset, len(conf.GetPresets()))
	for name, preset := range conf.GetPresets() {
		lowerName := strings.ToLower(name)
		newPreset, err := tenant.NewPreset(lowerName, preset.Description, preset.GetSize(), preset.GetShiftBy(), preset.GetLocation(), preset.GetTruncateTo())
		if err != nil {
			return nil, err
		}
		presets[lowerName] = newPreset
	}

	project, err := tenant.NewProject(conf.GetName(), pConf, variablesMap)
	if err != nil {
		return nil, err
	}

	project.SetPresets(presets)
	return project, nil
}

func toProjectProto(project *tenant.Project) *pb.ProjectSpecification {
	return &pb.ProjectSpecification{
		Name:      project.Name().String(),
		Config:    project.GetConfigs(),
		Variables: project.GetVariables(),
		Presets:   toProjectPresets(project.GetPresets()),
	}
}

func toProjectPresets(presets map[string]tenant.Preset) map[string]*pb.ProjectSpecification_ProjectPreset {
	presetPb := make(map[string]*pb.ProjectSpecification_ProjectPreset, len(presets))
	for name, preset := range presets {
		presetPb[name] = &pb.ProjectSpecification_ProjectPreset{
			Name:        preset.Name(),
			Description: preset.Description(),
			TruncateTo:  preset.Config().TruncateTo,
			ShiftBy:     preset.Config().ShiftBy,
			Size:        preset.Config().Size,
			Location:    preset.Config().Location,
		}
	}
	return presetPb
}
