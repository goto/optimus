package labels

import (
	"errors"
	"fmt"
	"regexp"
)

const (
	maxLabelsLength   = 32
	maxLabelsKVLength = 63
)

const (
	keyType   = "key"
	valueType = "value"
)

var errNilLabels = errors.New("labels is nil")

var (
	kvInvalidChar       = regexp.MustCompile(`[^a-z0-9\-_]+`)
	kvValidStartEndChar = regexp.MustCompile(`^[a-z0-9].*[a-z0-9]$`)
)

type Labels map[string]string

func (l Labels) Validate() error {
	if l == nil {
		return errNilLabels
	}

	if len(l) > maxLabelsLength {
		return fmt.Errorf("labels length is more than [%d]", maxLabelsLength)
	}

	for key, value := range l {
		if err := l.validateKV(key, keyType); err != nil {
			return fmt.Errorf("error validating key [%s]: %w", key, err)
		}

		if err := l.validateKV(value, valueType); err != nil {
			return fmt.Errorf("error validating value [%s]: %w", value, err)
		}
	}

	return nil
}

func (l Labels) validateKV(s, _type string) error {
	if s == "" {
		return fmt.Errorf("%s is empty", _type)
	}

	if len(s) > maxLabelsKVLength {
		return fmt.Errorf("%s length is more than [%d]", _type, maxLabelsKVLength)
	}

	if l.kvContainsInvalidChar(s) {
		return fmt.Errorf("%s should only be combination of lower case letters, numerics, underscores, and/or dashes", _type)
	}

	if !l.kvContainsValidStartAndEndChar(s) {
		return fmt.Errorf("%s should start and end with alphanumerics only", _type)
	}

	return nil
}

func (Labels) kvContainsValidStartAndEndChar(s string) bool {
	return kvValidStartEndChar.MatchString(s)
}

func (Labels) kvContainsInvalidChar(s string) bool {
	return kvInvalidChar.MatchString(s)
}

func FromMap(incoming map[string]string) (Labels, error) {
	if incoming == nil {
		return nil, errNilLabels
	}

	return Labels(incoming), nil
}
