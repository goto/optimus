package plan

type Kind interface {
	Getter
	Setter
}

type Setter interface {
	SetName(string)
	SetOldNamespace(oldNamespace string)
	SetPath(string)
}

type Getter interface {
	GetName() string
	GetPath() string
}

type KindList[kind Kind] []kind

func (kinds KindList[Kind]) GetNames() []string {
	names := make([]string, 0, len(kinds))
	for i := range kinds {
		names = append(names, kinds[i].GetName())
	}
	return names
}
