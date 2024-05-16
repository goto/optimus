package plan_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/goto/optimus/client/cmd/internal/plan"
)

func TestPlanCompositor(t *testing.T) {
	var (
		projectName = "p-optimus-1"
		job1        = "j-job-1"
		namespace1  = "n-optimus-1"
		namespace2  = "n-optimus-2"
	)

	t.Run("case migration only", func(t *testing.T) {
		inputs := []*plan.Plan{
			{Kind: plan.KindJob, ProjectName: projectName, NamespaceName: namespace1, Operation: plan.OperationDelete, KindName: job1},
			{Kind: plan.KindJob, ProjectName: projectName, NamespaceName: namespace2, Operation: plan.OperationCreate, KindName: job1},
		}
		expected := []*plan.Plan{
			{Kind: plan.KindJob, ProjectName: projectName, NamespaceName: namespace2, Operation: plan.OperationMigrate, KindName: job1, OldNamespaceName: &namespace1},
		}

		compositor := plan.NewCompositor()
		for i := range inputs {
			compositor.Add(inputs[i])
		}

		actual := compositor.GetAll()
		assert.ElementsMatch(t, actual, expected)
	})

	t.Run("case create update delete on different kind name and namespace", func(t *testing.T) {
		inputs := []*plan.Plan{
			{Kind: plan.KindJob, ProjectName: projectName, NamespaceName: namespace1, Operation: plan.OperationCreate, KindName: "job-1"},
			{Kind: plan.KindJob, ProjectName: projectName, NamespaceName: namespace1, Operation: plan.OperationUpdate, KindName: "job-2"},
			{Kind: plan.KindJob, ProjectName: projectName, NamespaceName: namespace1, Operation: plan.OperationDelete, KindName: "job-3"},
			{Kind: plan.KindJob, ProjectName: projectName, NamespaceName: namespace2, Operation: plan.OperationCreate, KindName: "job-1"},
			{Kind: plan.KindJob, ProjectName: projectName, NamespaceName: namespace2, Operation: plan.OperationUpdate, KindName: "job-2"},
			{Kind: plan.KindJob, ProjectName: projectName, NamespaceName: namespace2, Operation: plan.OperationDelete, KindName: "job-3"},
		}
		expected := []*plan.Plan{
			{Kind: plan.KindJob, ProjectName: projectName, NamespaceName: namespace1, Operation: plan.OperationUpdate, KindName: "job-2"},
			{Kind: plan.KindJob, ProjectName: projectName, NamespaceName: namespace2, Operation: plan.OperationUpdate, KindName: "job-2"},
			{Kind: plan.KindJob, ProjectName: projectName, NamespaceName: namespace1, Operation: plan.OperationCreate, KindName: "job-1"},
			{Kind: plan.KindJob, ProjectName: projectName, NamespaceName: namespace2, Operation: plan.OperationCreate, KindName: "job-1"},
			{Kind: plan.KindJob, ProjectName: projectName, NamespaceName: namespace1, Operation: plan.OperationDelete, KindName: "job-3"},
			{Kind: plan.KindJob, ProjectName: projectName, NamespaceName: namespace2, Operation: plan.OperationDelete, KindName: "job-3"},
		}

		compositor := plan.NewCompositor()
		for i := range inputs {
			compositor.Add(inputs[i])
		}

		actual := compositor.GetAll()
		assert.Len(t, actual, len(expected), "actual has %d length, but expect has %d length", len(actual), len(expected))
		assert.ElementsMatch(t, actual, expected)
	})

	t.Run("case migration from create to delete", func(t *testing.T) {
		inputs := []*plan.Plan{
			{Kind: plan.KindJob, ProjectName: projectName, NamespaceName: namespace1, Operation: plan.OperationCreate, KindName: "job-1"},
			{Kind: plan.KindJob, ProjectName: projectName, NamespaceName: namespace1, Operation: plan.OperationUpdate, KindName: "job-2"},
			{Kind: plan.KindJob, ProjectName: projectName, NamespaceName: namespace2, Operation: plan.OperationDelete, KindName: "job-1"},
			{Kind: plan.KindJob, ProjectName: projectName, NamespaceName: namespace2, Operation: plan.OperationUpdate, KindName: "job-2"},
			{Kind: plan.KindJob, ProjectName: projectName, NamespaceName: namespace2, Operation: plan.OperationCreate, KindName: "job-3"},
		}
		expected := []*plan.Plan{
			{Kind: plan.KindJob, ProjectName: projectName, NamespaceName: namespace1, Operation: plan.OperationMigrate, KindName: "job-1", OldNamespaceName: &namespace2},
			{Kind: plan.KindJob, ProjectName: projectName, NamespaceName: namespace1, Operation: plan.OperationUpdate, KindName: "job-2"},
			{Kind: plan.KindJob, ProjectName: projectName, NamespaceName: namespace2, Operation: plan.OperationUpdate, KindName: "job-2"},
			{Kind: plan.KindJob, ProjectName: projectName, NamespaceName: namespace2, Operation: plan.OperationCreate, KindName: "job-3"},
		}

		compositor := plan.NewCompositor()
		for i := range inputs {
			compositor.Add(inputs[i])
		}

		actual := compositor.GetAll()
		assert.ElementsMatch(t, actual, expected)
	})

	t.Run("case migration from delete to create", func(t *testing.T) {
		inputs := []*plan.Plan{
			{Kind: plan.KindJob, ProjectName: projectName, NamespaceName: namespace1, Operation: plan.OperationDelete, KindName: "job-1"},
			{Kind: plan.KindJob, ProjectName: projectName, NamespaceName: namespace1, Operation: plan.OperationUpdate, KindName: "job-2"},
			{Kind: plan.KindJob, ProjectName: projectName, NamespaceName: namespace2, Operation: plan.OperationCreate, KindName: "job-1"},
			{Kind: plan.KindJob, ProjectName: projectName, NamespaceName: namespace2, Operation: plan.OperationUpdate, KindName: "job-2"},
			{Kind: plan.KindJob, ProjectName: projectName, NamespaceName: namespace2, Operation: plan.OperationCreate, KindName: "job-3"},
		}
		expected := []*plan.Plan{
			{Kind: plan.KindJob, ProjectName: projectName, NamespaceName: namespace2, Operation: plan.OperationMigrate, KindName: "job-1", OldNamespaceName: &namespace1},
			{Kind: plan.KindJob, ProjectName: projectName, NamespaceName: namespace1, Operation: plan.OperationUpdate, KindName: "job-2"},
			{Kind: plan.KindJob, ProjectName: projectName, NamespaceName: namespace2, Operation: plan.OperationUpdate, KindName: "job-2"},
			{Kind: plan.KindJob, ProjectName: projectName, NamespaceName: namespace2, Operation: plan.OperationCreate, KindName: "job-3"},
		}

		compositor := plan.NewCompositor()
		for i := range inputs {
			compositor.Add(inputs[i])
		}

		actual := compositor.GetAll()
		assert.ElementsMatch(t, actual, expected)
	})

	t.Run("case multiple migration on multiple namespace", func(t *testing.T) {
		namespace3, namespace4, namespace5 := "n-optimus-3", "n-optimus-4", "n-optimus-5"
		inputs := []*plan.Plan{
			{Kind: plan.KindJob, ProjectName: projectName, NamespaceName: namespace1, Operation: plan.OperationDelete, KindName: job1},
			{Kind: plan.KindJob, ProjectName: projectName, NamespaceName: namespace2, Operation: plan.OperationCreate, KindName: job1},
			{Kind: plan.KindJob, ProjectName: projectName, NamespaceName: namespace3, Operation: plan.OperationCreate, KindName: job1},
			{Kind: plan.KindJob, ProjectName: projectName, NamespaceName: namespace4, Operation: plan.OperationDelete, KindName: job1},
			{Kind: plan.KindJob, ProjectName: projectName, NamespaceName: namespace5, Operation: plan.OperationCreate, KindName: job1},
		}
		expected := []*plan.Plan{
			{Kind: plan.KindJob, ProjectName: projectName, NamespaceName: namespace2, Operation: plan.OperationMigrate, KindName: job1, OldNamespaceName: &namespace1},
			{Kind: plan.KindJob, ProjectName: projectName, NamespaceName: namespace3, Operation: plan.OperationMigrate, KindName: job1, OldNamespaceName: &namespace4},
			{Kind: plan.KindJob, ProjectName: projectName, NamespaceName: namespace5, Operation: plan.OperationCreate, KindName: job1},
		}

		compositor := plan.NewCompositor()
		for i := range inputs {
			compositor.Add(inputs[i])
		}

		actual := compositor.GetAll()
		assert.ElementsMatch(t, actual, expected)
	})
}
