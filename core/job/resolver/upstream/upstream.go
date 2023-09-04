package upstream

type Upstream struct {
	Resource  Resource
	Upstreams []*Upstream
}

func FlattenUpstreams(upstreams []*Upstream) []Resource {
	var output []Resource
	for _, u := range upstreams {
		if u == nil {
			continue
		}

		output = append(output, u.Resource)

		nested := FlattenUpstreams(u.Upstreams)
		output = append(output, nested...)
	}

	return output
}
