package plan

import "encoding/json"

type Operation int

const (
	OperationDelete Operation = iota + 1
	OperationCreate
	OperationMigrate
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
	case OperationMigrate:
		return "migrate"
	default:
		return ""
	}
}

func (o Operation) MarshalJSON() ([]byte, error) { return json.Marshal(o.String()) }

func NewOperationByString(operation string) Operation {
	switch operation {
	case "delete":
		return OperationDelete
	case "create":
		return OperationCreate
	case "update":
		return OperationUpdate
	case "migrate":
		return OperationMigrate
	default:
		return 0
	}
}

func (o *Operation) UnmarshalJSON(value []byte) error {
	var operationValue string
	if err := json.Unmarshal(value, &operationValue); err != nil {
		return err
	}
	*o = NewOperationByString(operationValue)
	return nil
}
