package plan

type Plan struct {
	Kind          Kind      `csv:"kind"`
	ProjectName   string    `csv:"project_name"`
	NamespaceName string    `csv:"namespace_name"`
	KindName      string    `csv:"kind_name"`
	Operation     Operation `csv:"operation"`
	Executed      bool      `csv:"executed"`
}

type Plans []*Plan

func (p Plans) SortByOperationPriority(i, j int) bool { return p[i].Operation < p[j].Operation }

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

func (p Plans) UpdateExecutedByNames(executed bool, kind Kind, kindNames ...string) {
	mp := map[string]bool{}
	for _, name := range kindNames {
		mp[name] = true
	}
	for i, plan := range p {
		if _, ok := mp[plan.KindName]; ok && plan.Kind == kind {
			p[i].Executed = executed
		}
	}
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
