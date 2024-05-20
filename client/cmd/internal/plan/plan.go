package plan

type Plan struct {
	Kind          Kind      `json:"kind"`
	ProjectName   string    `json:"project_name"`
	NamespaceName string    `json:"namespace_name"`
	KindName      string    `json:"kind_name"`
	Operation     Operation `json:"operation"`
	Executed      bool      `json:"executed"`

	// OldNamespaceName used when Operation is OperationMigrate where it used on migrate namespace command / API
	OldNamespaceName *string `json:"old_namespace_name"`
}

type Plans []*Plan

func (p Plans) GetByKind(kind Kind) Plans {
	return getBy(p, func(p *Plan) bool {
		return p.Kind == kind
	})
}

func (p Plans) GetByProjectName(projectName string) Plans {
	return getBy(p, func(plan *Plan) bool {
		return plan.ProjectName == projectName
	})
}

func (p Plans) GetByNamespaceName(namespaceName string) Plans {
	return getBy(p, func(plan *Plan) bool {
		return plan.NamespaceName == namespaceName
	})
}

func (p Plans) GetByOperation(operation Operation) Plans {
	return getBy(p, func(plan *Plan) bool {
		return plan.Operation == operation
	})
}

func (p Plans) UpdateExecutedByNames(executed bool, kind Kind, kindNames ...string) Plans {
	mp := map[string]bool{}
	for _, name := range kindNames {
		mp[name] = true
	}
	for i, plan := range p {
		if _, ok := mp[plan.KindName]; ok && plan.Kind == kind {
			p[i].Executed = executed
		}
	}
	return p
}

func getBy(plans Plans, filter func(*Plan) bool) Plans {
	result := []*Plan{}
	for _, plan := range plans {
		if filter(plan) {
			result = append(result, plan)
		}
	}
	return result
}
