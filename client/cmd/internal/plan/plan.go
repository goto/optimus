package plan

type Plan struct {
	ProjectName string                               `json:"project_name"`
	Job         OperationByNamespaces[*JobPlan]      `json:"job"`
	Resource    OperationByNamespaces[*ResourcePlan] `json:"resource"`
}

func (p Plan) GetResult() Plan {
	return Plan{
		ProjectName: p.ProjectName,
		Job:         p.Job.getResult(),
		Resource:    p.Resource.getResult(),
	}
	return p
}

func (p Plan) SameProjectName(compare Plan) bool { return p.ProjectName == compare.ProjectName }

func NewPlan(projectName string) Plan {
	return Plan{
		ProjectName: projectName,
		Job:         NewOperationByNamespace[*JobPlan](),
		Resource:    NewOperationByNamespace[*ResourcePlan](),
	}
}
