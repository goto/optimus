package job_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/goto/optimus/core/job"
)

func TestFindCyclicPath(t *testing.T) {
	t.Run("returns nil when the graph has no cycle", func(t *testing.T) {
		graph := job.UpstreamGraph{
			"proj/a": {"proj/b"},
			"proj/b": {"proj/c"},
			"proj/c": {},
		}

		assert.Nil(t, job.FindCyclicPath(graph, "proj/a", map[job.FullName]bool{}))
	})

	t.Run("returns nil when root has no path back to itself, even if a cycle exists elsewhere", func(t *testing.T) {
		graph := job.UpstreamGraph{
			"proj/a": {"proj/b"},
			"proj/b": {},
			"proj/x": {"proj/y"},
			"proj/y": {"proj/x"},
		}

		assert.Nil(t, job.FindCyclicPath(graph, "proj/a", map[job.FullName]bool{}))
	})

	t.Run("detects a direct self-cycle", func(t *testing.T) {
		graph := job.UpstreamGraph{
			"proj/a": {"proj/a"},
		}

		cycle := job.FindCyclicPath(graph, "proj/a", map[job.FullName]bool{})
		assert.Equal(t, []job.FullName{"proj/a", "proj/a"}, cycle)
	})

	t.Run("detects a multi-hop cycle back to root", func(t *testing.T) {
		graph := job.UpstreamGraph{
			"proj/a": {"proj/b"},
			"proj/b": {"proj/c"},
			"proj/c": {"proj/a"},
		}

		cycle := job.FindCyclicPath(graph, "proj/a", map[job.FullName]bool{})
		assert.Equal(t, []job.FullName{"proj/a", "proj/b", "proj/c", "proj/a"}, cycle)
	})

	t.Run("detects a cycle spanning multiple projects", func(t *testing.T) {
		graph := job.UpstreamGraph{
			"project-1/a": {"project-2/b"},
			"project-2/b": {"project-3/c"},
			"project-3/c": {"project-1/a"},
		}

		cycle := job.FindCyclicPath(graph, "project-1/a", map[job.FullName]bool{})
		assert.Equal(t, []job.FullName{"project-1/a", "project-2/b", "project-3/c", "project-1/a"}, cycle)
	})

	t.Run("detects a cycle that root reaches but does not start", func(t *testing.T) {
		// root -> b -> c -> b (root itself isn't part of the cycle, but it reaches one)
		graph := job.UpstreamGraph{
			"proj/root": {"proj/b"},
			"proj/b":    {"proj/c"},
			"proj/c":    {"proj/b"},
		}

		cycle := job.FindCyclicPath(graph, "proj/root", map[job.FullName]bool{})
		assert.Equal(t, []job.FullName{"proj/b", "proj/c", "proj/b"}, cycle)
	})

	t.Run("does not loop forever on a diamond dependency (b and c both depend on d)", func(t *testing.T) {
		graph := job.UpstreamGraph{
			"proj/a": {"proj/b", "proj/c"},
			"proj/b": {"proj/d"},
			"proj/c": {"proj/d"},
			"proj/d": {},
		}

		assert.Nil(t, job.FindCyclicPath(graph, "proj/a", map[job.FullName]bool{}))
	})

	t.Run("root not present in graph as a key is treated as having no upstreams", func(t *testing.T) {
		graph := job.UpstreamGraph{
			"proj/b": {"proj/c"},
		}

		assert.Nil(t, job.FindCyclicPath(graph, "proj/a", map[job.FullName]bool{}))
	})
}

func TestFindCyclicPaths(t *testing.T) {
	t.Run("reports each root independently, including disjoint cycles", func(t *testing.T) {
		graph := job.UpstreamGraph{
			"proj/a": {"proj/b"},
			"proj/b": {"proj/a"},
			"proj/x": {"proj/y"},
			"proj/y": {"proj/x"},
			"proj/m": {"proj/n"},
			"proj/n": {},
		}

		result := job.FindCyclicPaths(graph, []job.FullName{"proj/a", "proj/x", "proj/m"})

		assert.Equal(t, []job.FullName{"proj/a", "proj/b", "proj/a"}, result["proj/a"])
		assert.Equal(t, []job.FullName{"proj/x", "proj/y", "proj/x"}, result["proj/x"])
		_, hasM := result["proj/m"]
		assert.False(t, hasM, "job-m is cycle-free and should not be present in the result")
	})
}
