package label

import (
	"errors"
)

type Labels map[string]string

func FromMap(incoming map[string]string) (Labels, error) {
	if incoming == nil {
		return nil, errors.New("labels map is nil")
	}

	return Labels(incoming), nil
}
