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
