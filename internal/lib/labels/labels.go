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
		if err := l.validateKey(key); err != nil {
			return err
		}

		if err := l.validateValue(value); err != nil {
			return fmt.Errorf("error validating value for key [%s]: %w", key, err)
		}
	}

	return nil
}

func (l Labels) validateValue(value string) error {
	if value == "" {
		return errors.New("value is empty")
	}

	if len(value) > maxLabelsKVLength {
		return fmt.Errorf("value length is more than [%d]", maxLabelsKVLength)
	}

	if l.kvContainsInvalidChar(value) {
		return errors.New("value should only be combination of lower case letters, numerics, underscores, and/or dashes")
	}

	if !l.kvContainsValidStartAndEndChar(value) {
		return errors.New("value should start and end with alphanumerics only")
	}

	return nil
}

func (l Labels) validateKey(key string) error {
	if key == "" {
		return errors.New("key is empty")
	}

	if len(key) > maxLabelsKVLength {
		return fmt.Errorf("key length is more than [%d]", maxLabelsKVLength)
	}

	if l.kvContainsInvalidChar(key) {
		return errors.New("key should only be combination of lower case letters, numerics, underscores, and/or dashes")
	}

	if !l.kvContainsValidStartAndEndChar(key) {
		return errors.New("key should start and end with alphanumerics only")
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
