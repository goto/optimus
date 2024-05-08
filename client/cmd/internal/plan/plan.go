package plan

type Plan struct {
	Kind          Kind      `csv:"kind"`
	ProjectName   string    `csv:"project_name"`
	NamespaceName string    `csv:"namespace_name"`
	KindName      string    `csv:"kind_name"`
	Operation     Operation `csv:"operation"`
	Executed      bool      `csv:"executed"`
}

type Plans []*Plan

func (p Plans) SortByOperationPriority(i, j int) bool { return p[i].Operation < p[j].Operation }
