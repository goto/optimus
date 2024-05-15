package plan

type Plan struct {
	Kind          Kind      `json:"kind"`
	ProjectName   string    `json:"project_name"`
	NamespaceName string    `json:"namespace_name"`
	KindName      string    `json:"kind_name"`
	Operation     Operation `json:"operation"`
	Executed      bool      `json:"executed"`

	// OldNamespaceName used when Operation is OperationMigrate where it used on migrate namespace command / API
	OldNamespaceName *string `json:"old_namespace_name"`
}

type Plans []*Plan

// SortByOperationPriority will sort Plans based on Operation integer value priority ASC
func (p Plans) SortByOperationPriority(i, j int) bool { return p[i].Operation < p[j].Operation }
