package service // nolint: testpackage

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/goto/optimus/core/scheduler"
)

// This is a package-internal test (not service_test) because overallStatus is
// unexported. It's the one piece of CheckQueryCompleteness's logic that's still a pure
// function after Service's dependencies became concrete repository/plugin types
// (jobRepo.JobRepository, *schedulerRepo.JobRunRepository, *plugin.PluginService) --
// those can no longer be swapped for mocks, so exercising the rest of
// CheckQueryCompleteness now needs a real Postgres-backed integration test, the same
// way internal/store/postgres/job's own tests do.
func TestOverallStatus(t *testing.T) {
	t.Run("complete when every managed table succeeded", func(t *testing.T) {
		tables := []ManagedTable{
			{JobName: "job-a", Run: &RunStatus{State: scheduler.StateSuccess}},
			{JobName: "job-b", Run: &RunStatus{State: scheduler.StateSuccess}},
		}
		assert.Equal(t, OverallStatusComplete, overallStatus(tables))
	})

	t.Run("not complete if any managed table failed", func(t *testing.T) {
		tables := []ManagedTable{
			{JobName: "job-a", Run: &RunStatus{State: scheduler.StateSuccess}},
			{JobName: "job-b", Run: &RunStatus{State: scheduler.StateFailed}},
		}
		assert.Equal(t, OverallStatusNotComplete, overallStatus(tables))
	})

	t.Run("not complete if any managed table has no run yet", func(t *testing.T) {
		tables := []ManagedTable{
			{JobName: "job-a", Run: nil},
		}
		assert.Equal(t, OverallStatusNotComplete, overallStatus(tables))
	})

	t.Run("vacuously complete with no managed tables at all", func(t *testing.T) {
		assert.Equal(t, OverallStatusComplete, overallStatus(nil))
	})
}
