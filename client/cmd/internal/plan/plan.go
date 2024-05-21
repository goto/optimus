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

// MergeMigrateOperation will merge OperationCreate and OperationDelete into one plan as OperationMigrate and leave other operation as it is
func (p Plans) MergeMigrateOperation() []*Plan {
	res := make([]*Plan, 0, len(p))
	createPlanByName := make(map[string][]*Plan)
	deletePlanByName := make(map[string][]*Plan)
	for _, plan := range p {
		switch plan.Operation {
		case OperationCreate:
			createPlanByName[plan.KindName] = append(createPlanByName[plan.KindName], plan)
		case OperationDelete:
			deletePlanByName[plan.KindName] = append(deletePlanByName[plan.KindName], plan)
		default:
			res = append(res, plan)
		}
	}

	if len(createPlanByName)+len(deletePlanByName) == 0 {
		return res
	}

	for kindName, createPlans := range createPlanByName {
		for _, createPlan := range createPlans {
			deletePlans, ok := deletePlanByName[kindName]
			if !ok {
				res = append(res, createPlan)
				continue
			}

			migratePlan := createPlan
			migratePlan.Operation = OperationMigrate
			migratePlan.OldNamespaceName = &deletePlans[0].NamespaceName
			res = append(res, migratePlan)
			deletePlans = deletePlans[1:]
			deletePlanByName[kindName] = deletePlans
			if len(deletePlans) == 0 {
				delete(deletePlanByName, kindName)
			}
		}
		delete(createPlanByName, kindName)
	}

	for _, deletePlans := range deletePlanByName {
		res = append(res, deletePlans...)
	}

	return res
}
