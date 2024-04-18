package lib

import (
	"errors"
	"fmt"
	"strings"
)

const (
	urnSeparator       = "://"
	urnComponentLength = 2
)

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

func NewURN(store, name string) (URN, error) {
	rawURN := store + urnSeparator + name

	return ParseURN(rawURN)
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
	splitURN := strings.Split(urn, urnSeparator)
	if len(splitURN) != urnComponentLength {
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

func ZeroURN() URN {
	return zeroURN
}
