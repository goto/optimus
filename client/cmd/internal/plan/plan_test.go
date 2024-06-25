package plan_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/goto/optimus/client/cmd/internal/plan"
)

func assertMapPlanMatch[kind plan.Kind](t *testing.T, m1, m2 plan.ListByNamespace[kind]) {
	t.Helper()
	assert.Equal(t, len(m1), len(m2))
	for namespace, plans := range m1 {
		expected := m2[namespace]
		assert.ElementsMatch(t, plans, expected, "element not match on map plan, actual: %+v, expected: %+v", plans, expected)
	}
}

func TestPlanGetResult(t *testing.T) {
	var (
		projectName = "p-optimus-1"
		job1        = "j-job-1"
		namespace1  = "n-optimus-1"
		namespace2  = "n-optimus-2"
	)

	t.Run("case migration only", func(t *testing.T) {
		plans := plan.NewPlan(projectName)
		plans.Job.Delete.Append(namespace1, &plan.JobPlan{Name: job1})
		plans.Job.Create.Append(namespace2, &plan.JobPlan{Name: job1})

		expected := plan.Plan{
			ProjectName: projectName,
			Job: plan.OperationByNamespaces[*plan.JobPlan]{
				Migrate: plan.ListByNamespace[*plan.JobPlan]{
					namespace2: {{Name: job1, OldNamespace: &namespace1}},
				},
			},
			Resource: plan.OperationByNamespaces[*plan.ResourcePlan]{},
		}

		actual := plans.GetResult()
		assert.True(t, actual.Resource.IsZero())
		assert.Empty(t, actual.Job.Create)
		assert.Empty(t, actual.Job.Update)
		assert.Empty(t, actual.Job.Delete)
		assertMapPlanMatch(t, actual.Job.Migrate, expected.Job.Migrate)
	})

	t.Run("case create update delete on different kind name and namespace", func(t *testing.T) {
		plans := plan.NewPlan(projectName)
		plans.Job.Create.Append(namespace1, &plan.JobPlan{Name: "job-1"})
		plans.Job.Update.Append(namespace1, &plan.JobPlan{Name: "job-2"})
		plans.Job.Delete.Append(namespace1, &plan.JobPlan{Name: "job-3"})
		plans.Job.Create.Append(namespace2, &plan.JobPlan{Name: "job-1"})
		plans.Job.Update.Append(namespace2, &plan.JobPlan{Name: "job-2"})
		plans.Job.Delete.Append(namespace2, &plan.JobPlan{Name: "job-3"})

		expected := plan.Plan{
			ProjectName: projectName,
			Job: plan.OperationByNamespaces[*plan.JobPlan]{
				Create: plan.ListByNamespace[*plan.JobPlan]{
					namespace1: {{Name: "job-1", OldNamespace: nil}},
					namespace2: {{Name: "job-1", OldNamespace: nil}},
				},
				Update: plan.ListByNamespace[*plan.JobPlan]{
					namespace1: {{Name: "job-2", OldNamespace: nil}},
					namespace2: {{Name: "job-2", OldNamespace: nil}},
				},
				Delete: plan.ListByNamespace[*plan.JobPlan]{
					namespace1: {{Name: "job-3", OldNamespace: nil}},
					namespace2: {{Name: "job-3", OldNamespace: nil}},
				},
			},
			Resource: plan.OperationByNamespaces[*plan.ResourcePlan]{},
		}

		actual := plans.GetResult()
		assert.True(t, actual.Job.Migrate.IsZero())
		assert.True(t, actual.Resource.IsZero())
		assertMapPlanMatch(t, actual.Job.Create, expected.Job.Create)
		assertMapPlanMatch(t, actual.Job.Update, expected.Job.Update)
		assertMapPlanMatch(t, actual.Job.Delete, expected.Job.Delete)
	})

	t.Run("case migration from create to delete", func(t *testing.T) {
		plans := plan.NewPlan(projectName)
		plans.Job.Create.Append(namespace1, &plan.JobPlan{Name: "job-1"})
		plans.Job.Update.Append(namespace1, &plan.JobPlan{Name: "job-2"})
		plans.Job.Delete.Append(namespace2, &plan.JobPlan{Name: "job-1"})
		plans.Job.Update.Append(namespace2, &plan.JobPlan{Name: "job-2"})
		plans.Job.Create.Append(namespace2, &plan.JobPlan{Name: "job-3"})

		expected := plan.Plan{
			ProjectName: projectName,
			Job: plan.OperationByNamespaces[*plan.JobPlan]{
				Create: plan.ListByNamespace[*plan.JobPlan]{
					namespace2: {{Name: "job-3", OldNamespace: nil}},
				},
				Update: plan.ListByNamespace[*plan.JobPlan]{
					namespace1: {{Name: "job-2", OldNamespace: nil}},
					namespace2: {{Name: "job-2", OldNamespace: nil}},
				},
				Delete: plan.ListByNamespace[*plan.JobPlan]{},
				Migrate: plan.ListByNamespace[*plan.JobPlan]{
					namespace1: {{Name: "job-1", OldNamespace: &namespace2}},
				},
			},
			Resource: plan.OperationByNamespaces[*plan.ResourcePlan]{},
		}

		actual := plans.GetResult()
		assert.True(t, actual.Job.Delete.IsZero())
		assert.True(t, actual.Resource.IsZero())
		assertMapPlanMatch(t, actual.Job.Create, expected.Job.Create)
		assertMapPlanMatch(t, actual.Job.Update, expected.Job.Update)
		assertMapPlanMatch(t, actual.Job.Migrate, expected.Job.Migrate)
	})

	t.Run("case migration from delete to create", func(t *testing.T) {
		plans := plan.NewPlan(projectName)
		plans.Job.Delete.Append(namespace1, &plan.JobPlan{Name: "job-1"})
		plans.Job.Update.Append(namespace1, &plan.JobPlan{Name: "job-2"})
		plans.Job.Create.Append(namespace2, &plan.JobPlan{Name: "job-1"})
		plans.Job.Update.Append(namespace2, &plan.JobPlan{Name: "job-2"})
		plans.Job.Create.Append(namespace2, &plan.JobPlan{Name: "job-3"})

		expected := plan.Plan{
			ProjectName: projectName,
			Job: plan.OperationByNamespaces[*plan.JobPlan]{
				Create: plan.ListByNamespace[*plan.JobPlan]{
					namespace2: {{Name: "job-3", OldNamespace: nil}},
				},
				Update: plan.ListByNamespace[*plan.JobPlan]{
					namespace1: {{Name: "job-2", OldNamespace: nil}},
					namespace2: {{Name: "job-2", OldNamespace: nil}},
				},
				Delete: plan.ListByNamespace[*plan.JobPlan]{},
				Migrate: plan.ListByNamespace[*plan.JobPlan]{
					namespace2: {{Name: "job-1", OldNamespace: &namespace1}},
				},
			},
			Resource: plan.OperationByNamespaces[*plan.ResourcePlan]{},
		}

		actual := plans.GetResult()
		assert.True(t, actual.Job.Delete.IsZero())
		assert.True(t, actual.Resource.IsZero())
		assertMapPlanMatch(t, actual.Job.Create, expected.Job.Create)
		assertMapPlanMatch(t, actual.Job.Update, expected.Job.Update)
		assertMapPlanMatch(t, actual.Job.Migrate, expected.Job.Migrate)
	})

	t.Run("return empty plan when create and delete job on same namespace", func(t *testing.T) {
		plans := plan.NewPlan(projectName)
		plans.Job.Delete.Append(namespace1, &plan.JobPlan{Name: "job-1"})
		plans.Job.Create.Append(namespace1, &plan.JobPlan{Name: "job-1"})

		actual := plans.GetResult()
		assert.True(t, actual.Job.IsZero())
		assert.True(t, actual.Resource.IsZero())
	})

	t.Run("case multiple migration on multiple namespace", func(t *testing.T) {
		namespace3, namespace4, namespace5 := "n-optimus-3", "n-optimus-4", "n-optimus-5"
		plans := plan.NewPlan(projectName)
		plans.Job.Delete.Append(namespace1, &plan.JobPlan{Name: job1})
		plans.Job.Create.Append(namespace2, &plan.JobPlan{Name: job1})
		plans.Job.Create.Append(namespace3, &plan.JobPlan{Name: job1})
		plans.Job.Delete.Append(namespace4, &plan.JobPlan{Name: job1})
		plans.Job.Create.Append(namespace5, &plan.JobPlan{Name: job1})

		expected := plan.Plan{
			ProjectName: projectName,
			Job: plan.OperationByNamespaces[*plan.JobPlan]{
				Create: plan.ListByNamespace[*plan.JobPlan]{
					namespace2: {{Name: job1, OldNamespace: nil}},
				},
				Update: plan.ListByNamespace[*plan.JobPlan]{},
				Delete: plan.ListByNamespace[*plan.JobPlan]{},
				Migrate: plan.ListByNamespace[*plan.JobPlan]{
					namespace3: {{Name: job1, OldNamespace: &namespace1}},
					namespace5: {{Name: job1, OldNamespace: &namespace4}},
				},
			},
			Resource: plan.OperationByNamespaces[*plan.ResourcePlan]{},
		}

		actual := plans.GetResult()
		assert.True(t, actual.Job.Delete.IsZero())
		assert.True(t, actual.Job.Update.IsZero())
		assert.True(t, actual.Resource.IsZero())
		// this is tricky to get order of the content (we use map on list of plan in namespace), so will check length instead
		assert.Equal(t, len(actual.Job.Create.GetAll()), len(expected.Job.Create.GetAll()))
		assert.Equal(t, len(actual.Job.Migrate.GetAll()), len(expected.Job.Migrate.GetAll()))
	})
}
