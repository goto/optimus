package resource

import "github.com/goto/optimus/internal/errors"

const (
	Bigquery   Store = "bigquery"
	MaxCompute Store = "maxcompute"
)

// Store represents the type of datasource, resource corresponds to
type Store string

func (s Store) String() string {
	return string(s)
}

func FromStringToStore(name string) (Store, error) {
	switch name {
	case string(Bigquery):
		return Bigquery, nil
	case string(MaxCompute):
		return MaxCompute, nil
	default:
		return "", errors.InvalidArgument(EntityResource, "unknown store "+name)
	}
}
