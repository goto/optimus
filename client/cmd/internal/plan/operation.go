package plan

type Operation int

const (
	OperationDelete Operation = iota + 1
	OperationCreate
	OperationUpdate
)

func (o Operation) String() string {
	switch o {
	case OperationDelete:
		return "delete"
	case OperationCreate:
		return "create"
	case OperationUpdate:
		return "update"
	default:
		return ""
	}
}

func NewOperationByString(operation string) Operation {
	switch operation {
	case "delete":
		return OperationDelete
	case "create":
		return OperationCreate
	case "update":
		return OperationUpdate
	default:
		return 0
	}
}

func (o *Operation) UnmarshalCSV(csv string) error { //nolint:unparam
	*o = NewOperationByString(csv)
	return nil
}
