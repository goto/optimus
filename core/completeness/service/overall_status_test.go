package service // nolint: testpackage

import (
	"testing"

	"github.com/goto/optimus/core/completeness"
	"github.com/stretchr/testify/assert"

	"github.com/goto/optimus/core/scheduler"
)

// Package-internal (not service_test) because overallStatus is unexported.
func TestOverallStatus(t *testing.T) {
	t.Run("complete when every active managed table succeeded", func(t *testing.T) {
		tables := []completeness.ManagedTable{
			{JobName: "job-a", IsActive: true, Run: &completeness.RunStatus{State: scheduler.StateSuccess}},
			{JobName: "job-b", IsActive: true, Run: &completeness.RunStatus{State: scheduler.StateSuccess}},
		}
		assert.Equal(t, completeness.OverallStatusComplete, overallStatus(tables))
	})

	t.Run("not complete if any active managed table failed", func(t *testing.T) {
		tables := []completeness.ManagedTable{
			{JobName: "job-a", IsActive: true, Run: &completeness.RunStatus{State: scheduler.StateSuccess}},
			{JobName: "job-b", IsActive: true, Run: &completeness.RunStatus{State: scheduler.StateFailed}},
		}
		assert.Equal(t, completeness.OverallStatusNotComplete, overallStatus(tables))
	})

	t.Run("not complete if any active managed table has no run yet", func(t *testing.T) {
		tables := []completeness.ManagedTable{
			{JobName: "job-a", IsActive: true, Run: nil},
		}
		assert.Equal(t, completeness.OverallStatusNotComplete, overallStatus(tables))
	})

	t.Run("vacuously complete with no managed tables at all", func(t *testing.T) {
		assert.Equal(t, completeness.OverallStatusComplete, overallStatus(nil))
	})

	t.Run("inactive job's failed/missing run is excluded from the check", func(t *testing.T) {
		tables := []completeness.ManagedTable{
			{JobName: "job-a", IsActive: true, Run: &completeness.RunStatus{State: scheduler.StateSuccess}},
			{JobName: "job-b", IsActive: false, Run: &completeness.RunStatus{State: scheduler.StateFailed}},
			{JobName: "job-c", IsActive: false, Run: nil},
		}
		assert.Equal(t, completeness.OverallStatusComplete, overallStatus(tables))
	})

	t.Run("complete when every managed table is inactive", func(t *testing.T) {
		tables := []completeness.ManagedTable{
			{JobName: "job-a", IsActive: false, Run: &completeness.RunStatus{State: scheduler.StateFailed}},
		}
		assert.Equal(t, completeness.OverallStatusComplete, overallStatus(tables))
	})
}
