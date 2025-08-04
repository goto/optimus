package plan

import (
	"fmt"
)

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
}

func getIcon(operation string) string {
	switch operation {
	case "Create":
		return "➕"
	case "Delete":
		return "➖"
	case "Update":
		return "✏️"
	case "Migrate":
		return "🔄"
	default:
		return operation
	}
}

func PrintJobPlan(operation, resourceType string, p ListByNamespace[*JobPlan]) string {
	var result string
	if len(p.GetAll()) > 0 {
		result += fmt.Sprintf("[ %s %s ]\n", operation, resourceType)
		for _, namespaceName := range p.GetAllNamespaces() {
			result += fmt.Sprintf("[%s]\n", namespaceName)
			planList := p.GetByNamespace(namespaceName)
			for i, item := range planList {
				if i == len(planList)-1 {
					result += fmt.Sprintf("\t└─ %s %s\n", getIcon(operation), item.Name)
				} else {
					result += fmt.Sprintf("\t├─ %s %s\n", getIcon(operation), item.Name)
				}
			}
		}
		result += "\n"
	}
	return result
}

func PrintResourcePlan(operation, resourceType string, p ListByNamespace[*JobPlan]) string {
	var result string
	if len(p.GetAll()) > 0 {
		result += fmt.Sprintf("[ %s %s ]\n", operation, resourceType)
		for _, namespaceName := range p.GetAllNamespaces() {
			result += fmt.Sprintf("[%s]\n", namespaceName)
			planList := p.GetByNamespace(namespaceName)
			for i, item := range planList {
				if i == len(planList)-1 {
					result += fmt.Sprintf("\t└─ %s %s\n", getIcon(operation), item.Name)
				} else {
					result += fmt.Sprintf("\t├─ %s %s\n", getIcon(operation), item.Name)
				}
			}
		}
		result += "\n"
	}
	return result
}

func (p Plan) PrintComplete() string {
	var result string
	result += PrintJobPlan("Create", "Job", p.Job.Create)
	result += PrintJobPlan("Delete", "Job", p.Job.Delete)
	result += PrintJobPlan("Update", "Job", p.Job.Update)
	result += PrintJobPlan("Migrate", "Job", p.Job.Migrate)
	result += PrintJobPlan("Create", "Job", p.Job.Create)
	result += PrintJobPlan("Delete", "Job", p.Job.Delete)
	result += PrintJobPlan("Update", "Job", p.Job.Update)
	result += PrintJobPlan("Migrate", "Job", p.Job.Migrate)
	return result
}

func (p Plan) Merge(otherPlan Plan) Plan {
	newPlan := NewPlan(p.ProjectName)
	newPlan.Job = p.Job.merge(otherPlan.Job)
	newPlan.Resource = p.Resource.merge(otherPlan.Resource)
	return newPlan
}

func (p Plan) IsEmpty() bool {
	return p.ProjectName == "" && p.Job.IsZero() && p.Resource.IsZero()
}

func (p Plan) SameProjectName(compare Plan) bool { return p.ProjectName == compare.ProjectName }

func NewPlan(projectName string) Plan {
	return Plan{
		ProjectName: projectName,
		Job:         NewOperationByNamespace[*JobPlan](),
		Resource:    NewOperationByNamespace[*ResourcePlan](),
	}
}
