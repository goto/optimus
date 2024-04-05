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

var zeroURN URN

type URN struct {
	store string
	name  string

	raw string
}

func (u URN) GetStore() string {
	return u.store
}

func (u URN) GetName() string {
	return u.name
}

func (u URN) String() string {
	return u.raw
}

func (u URN) IsZero() bool {
	return u == zeroURN
}

func ParseURN(urn string) (URN, error) {
	if urn == "" {
		return zeroURN, nil
	}

	splitURN := strings.Split(urn, urnSeparator)
	if len(splitURN) != 2 {
		return zeroURN, errURNWrongPattern
	}

	store := splitURN[0]
	name := splitURN[1]

	if store == "" {
		return zeroURN, errURNStoreNotSpecified
	}

	if name == "" {
		return zeroURN, errURNNameNotSpecified
	}

	if trimmedStore := strings.TrimSpace(store); len(trimmedStore) != len(store) {
		return zeroURN, errURNStoreContainsWhitespace
	}

	if trimmedName := strings.TrimSpace(name); len(trimmedName) != len(name) {
		return zeroURN, errURNNameContainsWhitespace
	}

	return URN{
		store: store,
		name:  name,
		raw:   urn,
	}, nil
}

func ZeroURN() URN {
	return zeroURN
}
