package plan

import "encoding/json"

// Operation determine Plan Operation
type Operation string

const (
	OperationDelete  Operation = "delete"
	OperationCreate  Operation = "create"
	OperationMigrate Operation = "migrate"
	OperationUpdate  Operation = "update"
)

// String is fmt.Stringer implementation
func (o Operation) String() string { return string(o) }

// MarshalJSON implement json.Marshaler with returning its string value
func (o Operation) MarshalJSON() ([]byte, error) { return json.Marshal(o.String()) }

// UnmarshalJSON implement json.Unmarshaler and initialized based on string value of Operation
func (o *Operation) UnmarshalJSON(value []byte) error {
	var operationValue string
	if err := json.Unmarshal(value, &operationValue); err != nil {
		return err
	}
	*o = Operation(operationValue)
	return nil
}
