package plan

import "encoding/json"

// Operation determine Plan Operation and ordered by priority
type Operation int

const (
	OperationDelete Operation = iota + 1
	OperationCreate
	OperationMigrate
	OperationUpdate
)

// String is fmt.Stringer implementation
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

// MarshalJSON implement json.Marshaler with returning its string value
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

// UnmarshalJSON implement json.Unmarshaler and initialized based on string value of Operation
func (o *Operation) UnmarshalJSON(value []byte) error {
	var operationValue string
	if err := json.Unmarshal(value, &operationValue); err != nil {
		return err
	}
	*o = NewOperationByString(operationValue)
	return nil
}
