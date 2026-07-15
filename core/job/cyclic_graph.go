package job

// UpstreamGraph is a lightweight, install-wide job dependency graph keyed by FullName
// ("project/job_name") on both sides. It intentionally carries no other job data, so it can be
// built cheaply from persisted upstream edges and patched with in-memory edges from an incoming
// validate request, without needing full Job objects for every job in the graph.
type UpstreamGraph map[FullName][]FullName

// FindCyclicPath performs a DFS over the graph starting at root, following outgoing (upstream)
// edges. It returns the specific cycle path (root -> ... -> root) if root can reach itself
// through the graph, or nil if no cycle involving root exists.
//
// This is scoped per root rather than scanning the whole graph for "the first" cycle: each root
// gets its own accurate answer, so validating job B still detects a cycle through B even if job A
// sits on a separate, disjoint cycle elsewhere in the same graph.
//
// visited is a caller-provided cache of nodes already proven cycle-free by a previous call; it is
// safe to share across multiple FindCyclicPath calls against the same graph, since a node found to
// have no path back to any of its ancestors during one DFS will never have one in a later DFS
// either (the graph does not change between calls).
func FindCyclicPath(graph UpstreamGraph, root FullName, visited map[FullName]bool) []FullName {
	visiting := map[FullName]bool{}
	var path []FullName

	var dfs func(node FullName) []FullName
	dfs = func(node FullName) []FullName {
		if visiting[node] {
			for i, ancestor := range path {
				if ancestor == node {
					cycle := append([]FullName{}, path[i:]...)
					return append(cycle, node)
				}
			}
		}
		if visited[node] {
			return nil
		}

		visiting[node] = true
		path = append(path, node)

		for _, upstream := range graph[node] {
			if cycle := dfs(upstream); cycle != nil {
				return cycle
			}
		}

		path = path[:len(path)-1]
		visiting[node] = false
		visited[node] = true
		return nil
	}

	return dfs(root)
}

// FindCyclicPaths runs FindCyclicPath for each of the given roots against the same graph, sharing
// the cycle-free cache across calls, and returns only the roots that participate in a cycle,
// mapped to that cycle's path.
func FindCyclicPaths(graph UpstreamGraph, roots []FullName) map[FullName][]FullName {
	visited := map[FullName]bool{}
	result := make(map[FullName][]FullName)
	for _, root := range roots {
		if cycle := FindCyclicPath(graph, root, visited); cycle != nil {
			result[root] = cycle
		}
	}
	return result
}
