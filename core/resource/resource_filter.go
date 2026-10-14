package resource

import (
	"strings"

	"github.com/goto/optimus/internal/errors"
	"github.com/goto/optimus/internal/utils/filter"
)

// Wildcard marks the prefix position in a resource filter expression.
const Wildcard = "*"

// ResourceFilter is the parsed form of the `resource_filter` request field of
// ListResourceSpecification. It selects resources by their datastore full name
// (e.g. `projectA.schemaA.tableA`):
//
//   - without a wildcard: exact match (`projectA.schemaA` matches only
//     resources whose full_name is exactly `projectA.schemaA`)
//   - with a wildcard: prefix match (`projectA.schemaA.*` matches every
//     resource whose full_name starts with `projectA.schemaA.`)
//
// Only the text before the first wildcard is used as the prefix.
type ResourceFilter struct {
	fullName       string
	fullNamePrefix string
}

// ResourceFilterFrom parses a raw resource filter expression.
func ResourceFilterFrom(filterStr string) (ResourceFilter, error) {
	trimmed := strings.TrimSpace(filterStr)
	if trimmed == "" {
		return ResourceFilter{}, errors.InvalidArgument(EntityResource, "resource filter is empty")
	}

	if idx := strings.Index(trimmed, Wildcard); idx >= 0 {
		prefix := strings.TrimSpace(trimmed[:idx])
		if prefix == "" {
			return ResourceFilter{}, errors.InvalidArgument(EntityResource, "invalid resource filter ["+trimmed+"]: wildcard must be preceded by a full name prefix")
		}
		return ResourceFilter{fullNamePrefix: prefix}, nil
	}

	return ResourceFilter{fullName: trimmed}, nil
}

func (rf ResourceFilter) IsExactMatch() bool {
	return rf.fullNamePrefix == ""
}

func (rf ResourceFilter) FullName() string {
	return rf.fullName
}

func (rf ResourceFilter) FullNamePrefix() string {
	return rf.fullNamePrefix
}

// FilterOpts converts the filter into repository filter options.
func (rf ResourceFilter) FilterOpts() []filter.FilterOpt {
	if !rf.IsExactMatch() {
		return []filter.FilterOpt{filter.WithString(filter.ResourceFullNamePrefix, rf.fullNamePrefix)}
	}
	return []filter.FilterOpt{filter.WithString(filter.ResourceFullName, rf.fullName)}
}
