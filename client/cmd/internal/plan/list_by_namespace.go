package plan

type ListByNamespace[plan Kind] map[string][]plan

func (p *ListByNamespace[Kind]) GetAll() []Kind {
	res := make([]Kind, 0)
	for _, plans := range *p {
		res = append(res, plans...)
	}
	return res
}

func (p *ListByNamespace[Kind]) GetByNamespace(namespace string) []Kind {
	for planNamespace, plans := range *p {
		if namespace == planNamespace {
			return plans
		}
	}
	return nil
}

func (p *ListByNamespace[Kind]) GetPlansByNamespaceName() map[string][]string {
	planByNamespaceName := make(map[string][]string)
	for namespace, plans := range *p {
		var names []string
		for _, plan := range plans {
			names = append(names, plan.GetName())
		}
		planByNamespaceName[namespace] = names
	}
	return planByNamespaceName
}

func (p *ListByNamespace[Kind]) GetAllNamespaces() []string {
	res := make([]string, 0)
	exists := make(map[string]bool)
	for namespace := range *p {
		if exists[namespace] {
			continue
		}
		res = append(res, namespace)
		exists[namespace] = true
	}
	return res
}

func (p ListByNamespace[Kind]) DeletePlansByNames(names ...string) ListByNamespace[Kind] {
	newListByNamespace := ListByNamespace[Kind]{}
	isDelete := map[string]bool{}
	for _, name := range names {
		isDelete[name] = true
	}

	for namespace, kinds := range p {
		for _, kind := range kinds {
			if _, ok := isDelete[kind.GetName()]; !ok {
				newListByNamespace.Append(namespace, kind)
			}
		}
	}
	return newListByNamespace
}

func (p *ListByNamespace[Kind]) getMapByNameAndNamespace() map[string]map[string]Kind {
	planByNameAndNamespace := make(map[string]map[string]Kind)
	for namespace, plans := range *p {
		for _, plan := range plans {
			planByNamespace, exist := planByNameAndNamespace[plan.GetName()]
			if !exist {
				planByNamespace = make(map[string]Kind)
			}
			planByNamespace[namespace] = plan
			planByNameAndNamespace[plan.GetName()] = planByNamespace
		}
	}
	return planByNameAndNamespace
}

func (p *ListByNamespace[Kind]) Append(namespace string, newPlan Kind) {
	plansByNamespace := *p
	plans := plansByNamespace[namespace]
	plansByNamespace[namespace] = append(plans, newPlan)
	*p = plansByNamespace
}

func (p *ListByNamespace[Kind]) IsZero() bool {
	if p == nil {
		return true
	}

	value := *p
	return len(value) == 0
}

func NewListByNamespace[kind Kind]() ListByNamespace[kind] { return make(ListByNamespace[kind]) }
