package plan

type Kind interface {
	Getter
	Setter
}

type Setter interface {
	SetName(string)
	SetOldNamespace(oldNamespace string)
}

type Getter interface {
	GetName() string
}
