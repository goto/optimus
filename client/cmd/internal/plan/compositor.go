package plan

type Compositor struct {
	deleteOperation map[string]Queue[*Plan]
	createOperation map[string]Queue[*Plan]
	result          []*Plan
}

func NewCompositor() Compositor {
	return Compositor{
		deleteOperation: make(map[string]Queue[*Plan]),
		createOperation: make(map[string]Queue[*Plan]),
		result:          make([]*Plan, 0),
	}
}

// Add will append Plan and enqueue create and delete plan Operation
func (compositor *Compositor) Add(addPlan *Plan) {
	switch addPlan.Operation {
	case OperationCreate:
		createPlansByName, exist := compositor.createOperation[addPlan.KindName]
		if !exist {
			createPlansByName = NewQueue[*Plan]()
		}
		createPlansByName.Push(addPlan)
		compositor.createOperation[addPlan.KindName] = createPlansByName
	case OperationDelete:
		deletePlanByName, exist := compositor.deleteOperation[addPlan.KindName]
		if !exist {
			deletePlanByName = NewQueue[*Plan]()
		}
		deletePlanByName.Push(addPlan)
		compositor.deleteOperation[addPlan.KindName] = deletePlanByName
	default:
		compositor.result = append(compositor.result, addPlan)
	}
}

func (compositor Compositor) GetAll() []*Plan {
	if len(compositor.createOperation)+len(compositor.deleteOperation) == 0 {
		return compositor.result
	}

	res := compositor.result
	res = append(res, compositor.getPlan(compositor.createOperation, compositor.deleteOperation)...)
	res = append(res, compositor.getPlan(compositor.deleteOperation, compositor.createOperation)...)
	return res
}

// getPlan will dequeue create and delete plan Operation
// if there are same Plan.KindName on different Plan.NamespaceName, it will be merged as one plan with OperationMigrate
// else it will append as result directly
func (Compositor) getPlan(createOperation, deleteOperation map[string]Queue[*Plan]) []*Plan {
	var res []*Plan
	for kindName, createPlanQueue := range createOperation {
		if createPlanQueue == nil {
			continue
		}

		deletePlanQueue := deleteOperation[kindName]
		for createPlanQueue.Next() {
			if !deletePlanQueue.Next() {
				res = append(res, createPlanQueue.Pop())
				continue
			}

			deletePlan := deletePlanQueue.Pop()
			migratePlan := createPlanQueue.Pop()
			migratePlan.Operation = OperationMigrate
			migratePlan.OldNamespaceName = &deletePlan.NamespaceName
			res = append(res, migratePlan)
		}
		createOperation[kindName] = createPlanQueue
		deleteOperation[kindName] = deletePlanQueue
	}
	return res
}
