package plan

type OperationByNamespaces[kind Kind] struct {
	Create  ListByNamespace[kind] `json:"create"`
	Delete  ListByNamespace[kind] `json:"delete"`
	Update  ListByNamespace[kind] `json:"update"`
	Migrate ListByNamespace[kind] `json:"migrate"`
}

// Add will decide where to add the plan, sourceName: latest state, targetName: current state
func (o *OperationByNamespaces[Kind]) Add(namespace, sourceName, targetName string, plan Kind) {
	// do not append to the plan if both sourceName and targetName are empty,
	// which means there's nothing to track on the changes
	if len(sourceName) == 0 && len(targetName) == 0 {
		return
	}

	plan.SetName(targetName)

	if len(sourceName) > 0 && len(targetName) == 0 {
		plan.SetName(sourceName)
		o.Create.Append(namespace, plan)
		return
	}

	if len(sourceName) == 0 && len(targetName) > 0 {
		o.Delete.Append(namespace, plan)
		return
	}

	o.Update.Append(namespace, plan)
}

func (o *OperationByNamespaces[Kind]) getResult() OperationByNamespaces[Kind] {
	var (
		createOperation  = o.Create.getMapByNameAndNamespace()
		deleteOperation  = o.Delete.getMapByNameAndNamespace()
		migrateOperation = NewListByNamespace[Kind]()
		result           = NewOperationByNamespace[Kind]()
	)

	result.Update = o.Update
	if len(createOperation)+len(deleteOperation) == 0 {
		return result
	}

	for kindName, createPlans := range createOperation {
		for namespace, createPlan := range createPlans {
			deletePlans, ok := deleteOperation[kindName]
			if !ok {
				result.Create.Append(namespace, createPlan)
				continue
			}

			// Handle move directory with same namespace, marking as UPDATE (there are possibilities spec changes)
			if _, foundSameNamespace := deletePlans[namespace]; foundSameNamespace {
				result.Update.Append(namespace, createPlan)
				delete(deletePlans, namespace)
				continue
			}

			for oldNamespace := range deletePlans {
				migratePlan := createPlan
				migratePlan.SetOldNamespace(oldNamespace)
				migrateOperation.Append(namespace, migratePlan)
				delete(deletePlans, oldNamespace)
				deleteOperation[kindName] = deletePlans
				if len(deletePlans) == 0 {
					delete(deleteOperation, kindName)
				}
				break
			}
		}

		delete(createOperation, kindName)
	}

	for _, deletePlans := range deleteOperation {
		for namespace, deletePlan := range deletePlans {
			result.Delete.Append(namespace, deletePlan)
		}
	}

	result.Migrate = migrateOperation
	return result
}

func (o OperationByNamespaces[Kind]) IsZero() bool {
	return o.Create.IsZero() && o.Update.IsZero() && o.Delete.IsZero() && o.Migrate.IsZero()
}

func (o OperationByNamespaces[kind]) GetAllNamespaces() []string {
	namespaces := make([]string, 0)
	namespaceExists := make(map[string]bool)

	appendDistinct := func(res []string, elements ...string) []string {
		for _, namespace := range elements {
			if namespaceExists[namespace] {
				continue
			}
			res = append(res, namespace)
			namespaceExists[namespace] = true
		}
		return res
	}

	namespaces = appendDistinct(namespaces, o.Create.GetAllNamespaces()...)
	namespaces = appendDistinct(namespaces, o.Update.GetAllNamespaces()...)
	namespaces = appendDistinct(namespaces, o.Delete.GetAllNamespaces()...)
	namespaces = appendDistinct(namespaces, o.Migrate.GetAllNamespaces()...)

	return namespaces
}

func (o OperationByNamespaces[Kind]) merge(other OperationByNamespaces[Kind]) OperationByNamespaces[Kind] {
	for _, namespaceName := range other.GetAllNamespaces() {
		for _, k := range other.Create.GetByNamespace(namespaceName) {
			o.Create.Append(namespaceName, k)
		}
		for _, k := range other.Update.GetByNamespace(namespaceName) {
			o.Update.Append(namespaceName, k)
		}
		for _, k := range other.Delete.GetByNamespace(namespaceName) {
			o.Delete.Append(namespaceName, k)
		}
		for _, k := range other.Migrate.GetByNamespace(namespaceName) {
			o.Migrate.Append(namespaceName, k)
		}
	}
	return o
}

func NewOperationByNamespace[T Kind]() OperationByNamespaces[T] {
	return OperationByNamespaces[T]{
		Create:  NewListByNamespace[T](),
		Delete:  NewListByNamespace[T](),
		Update:  NewListByNamespace[T](),
		Migrate: NewListByNamespace[T](),
	}
}
