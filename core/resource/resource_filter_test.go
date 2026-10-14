//go:build unit_test

package resource_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/goto/optimus/core/resource"
	"github.com/goto/optimus/internal/errors"
	"github.com/goto/optimus/internal/utils/filter"
)

func TestResourceFilterFrom(t *testing.T) {
	t.Run("exact match when no wildcard present", func(t *testing.T) {
		rf, err := resource.ResourceFilterFrom("projectA.schemaA")
		assert.NoError(t, err)
		assert.True(t, rf.IsExactMatch())
		assert.Equal(t, "projectA.schemaA", rf.FullName())
		assert.Equal(t, "", rf.FullNamePrefix())

		opts := rf.FilterOpts()
		f := filter.NewFilter(opts...)
		assert.True(t, f.Contains(filter.ResourceFullName))
		assert.Equal(t, "projectA.schemaA", f.GetStringValue(filter.ResourceFullName))
		assert.False(t, f.Contains(filter.ResourceFullNamePrefix))
	})

	t.Run("prefix match when wildcard present", func(t *testing.T) {
		rf, err := resource.ResourceFilterFrom("projectA.schemaA.*")
		assert.NoError(t, err)
		assert.False(t, rf.IsExactMatch())
		assert.Equal(t, "projectA.schemaA.", rf.FullNamePrefix())
		assert.Equal(t, "", rf.FullName())

		opts := rf.FilterOpts()
		f := filter.NewFilter(opts...)
		assert.True(t, f.Contains(filter.ResourceFullNamePrefix))
		assert.Equal(t, "projectA.schemaA.", f.GetStringValue(filter.ResourceFullNamePrefix))
		assert.False(t, f.Contains(filter.ResourceFullName))
	})

	t.Run("only text before the first wildcard is used as prefix", func(t *testing.T) {
		rf, err := resource.ResourceFilterFrom("projectA.sch*emaA.table*")
		assert.NoError(t, err)
		assert.False(t, rf.IsExactMatch())
		assert.Equal(t, "projectA.sch", rf.FullNamePrefix())
	})

	t.Run("trims surrounding whitespace", func(t *testing.T) {
		rf, err := resource.ResourceFilterFrom("  projectA.schemaA.*  ")
		assert.NoError(t, err)
		assert.Equal(t, "projectA.schemaA.", rf.FullNamePrefix())
	})

	t.Run("returns error for empty filter", func(t *testing.T) {
		rf, err := resource.ResourceFilterFrom("   ")
		assert.Error(t, err)
		assert.True(t, errors.IsErrorType(err, errors.ErrInvalidArgument))
		assert.Equal(t, resource.ResourceFilter{}, rf)
	})

	t.Run("returns error when wildcard has no prefix", func(t *testing.T) {
		rf, err := resource.ResourceFilterFrom("*")
		assert.Error(t, err)
		assert.True(t, errors.IsErrorType(err, errors.ErrInvalidArgument))
		assert.Equal(t, resource.ResourceFilter{}, rf)
	})
}
