package tenant

type Location struct {
	name    string
	project string
	dataset string
}

func (l *Location) Name() string {
	return l.name
}

func (l *Location) Project() string {
	return l.project
}

func (l *Location) Dataset() string {
	return l.dataset
}

func (l *Location) Equal(incoming Location) bool {
	return l.Name() == incoming.Name() &&
		l.Project() == incoming.Project() &&
		l.Dataset() == incoming.Dataset()
}

func NewLocation(name, project, dataset string) Location {
	return Location{
		name:    name,
		project: project,
		dataset: dataset,
	}
}
