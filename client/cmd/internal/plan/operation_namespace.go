package plan

type OperationByNamespaces[kind Kind] struct {
	Create  MapByNamespace[kind] `json:"create"`
	Delete  MapByNamespace[kind] `json:"delete"`
	Update  MapByNamespace[kind] `json:"update"`
	Migrate MapByNamespace[kind] `json:"migrate"`
}

// Add will decide where to add the plan, sourceName: latest state, targetName: current state
func (o *OperationByNamespaces[Kind]) Add(namespace string, sourceName, targetName string, plan Kind) {
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
		migrateOperation = make(MapByNamespace[Kind])
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

			for oldNamespace := range deletePlans {
				migratePlan := createPlan
				migratePlan.SetOldNamespace(oldNamespace)
				migrateOperation.Append(namespace, migratePlan)
				delete(deletePlans, namespace)
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

func NewOperationByNamespace[T Kind]() OperationByNamespaces[T] {
	return OperationByNamespaces[T]{
		Create:  NewMapByNamespace[T](),
		Delete:  NewMapByNamespace[T](),
		Update:  NewMapByNamespace[T](),
		Migrate: NewMapByNamespace[T](),
	}
}
