package lib

import (
	"errors"
	"fmt"
	"strings"
)

const urnSeparator = "://"

var (
	errURNWrongPattern            = fmt.Errorf("urn does not follow pattern <store>%s<name>", urnSeparator)
	errURNStoreNotSpecified       = errors.New("urn store is not specified")
	errURNNameNotSpecified        = errors.New("urn name is not specified")
	errURNStoreContainsWhitespace = errors.New("urn store contains whitespace")
	errURNNameContainsWhitespace  = errors.New("urn name contains whitespace")
)

type URN struct {
	store string
	name  string

	raw string
}

func (u *URN) GetStore() string {
	if u == nil {
		return ""
	}
	return u.store
}

func (u *URN) GetName() string {
	if u == nil {
		return ""
	}
	return u.name
}

func (u *URN) String() string {
	if u == nil {
		return ""
	}
	return u.raw
}

func ParseURN(urn string) (URN, error) {
	splitURN := strings.Split(urn, urnSeparator)
	if len(splitURN) != 2 {
		return URN{}, errURNWrongPattern
	}

	store := splitURN[0]
	name := splitURN[1]

	if store == "" {
		return URN{}, errURNStoreNotSpecified
	}

	if name == "" {
		return URN{}, errURNNameNotSpecified
	}

	if trimmedStore := strings.TrimSpace(store); len(trimmedStore) != len(store) {
		return URN{}, errURNStoreContainsWhitespace
	}

	if trimmedName := strings.TrimSpace(name); len(trimmedName) != len(name) {
		return URN{}, errURNNameContainsWhitespace
	}

	return URN{
		store: store,
		name:  name,
		raw:   urn,
	}, nil
}
