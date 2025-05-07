package compiler_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/goto/optimus/internal/compiler"
)

func TestTemplateFunctions(t *testing.T) {
	d1 := time.Date(2023, 1, 15, 3, 12, 8, 0, time.UTC)
	t.Run("Date", func(t *testing.T) {
		t.Run("converts ISO time to date", func(t *testing.T) {
			d, err := compiler.Date(d1.Format(compiler.ISOTimeFormat))
			assert.NoError(t, err)
			assert.Equal(t, "2023-01-15", d)
		})
	})
	t.Run("Replace", func(t *testing.T) {
		t.Run("replaces string with another", func(t *testing.T) {
			d := "2023-01-15"
			replaced := compiler.Replace("-", ":", d)
			assert.Equal(t, "2023:01:15", replaced)
		})
	})
	t.Run("Trunc", func(t *testing.T) {
		t.Run("truncates a string at length", func(t *testing.T) {
			truncated := compiler.Trunc(10, "CompilerTest")
			assert.Equal(t, "CompilerTe", truncated)
		})
		t.Run("ignores negative values", func(t *testing.T) {
			truncated := compiler.Trunc(-5, "Compiler")
			assert.Equal(t, "Compiler", truncated)
		})
	})
	t.Run("DateModify", func(t *testing.T) {
		t.Run("adds a duration to time", func(t *testing.T) {
			newDate := compiler.DateModify("24h", d1)
			assert.Equal(t, "2023-01-16", newDate.Format(compiler.ISODateFormat))
		})
	})
	t.Run("UnixEpoch", func(t *testing.T) {
		t.Run("converts data to unix epoch", func(t *testing.T) {
			unixEpoch := compiler.UnixEpoch(d1)
			assert.Equal(t, "1673752328", unixEpoch)
		})
	})
	t.Run("List", func(t *testing.T) {
		t.Run("converts varargs to list", func(t *testing.T) {
			list := compiler.List("a", "b", "c")
			assert.Len(t, list, 3)
			assert.Equal(t, []string{"a", "b", "c"}, list)
		})
	})
	t.Run("Join", func(t *testing.T) {
		t.Run("returns joined strings", func(t *testing.T) {
			joined := compiler.Join("_", []string{"project", "dataset", "table"})
			assert.Equal(t, "project_dataset_table", joined)
		})
	})
	t.Run("DateFrom", func(t *testing.T) {
		t.Run("reduces days by 2", func(t *testing.T) {
			from, err := compiler.DateFrom(d1, "-2", "")
			assert.NoError(t, err)
			assert.Equal(t, "2023-01-13", from.Format(compiler.ISODateFormat))
		})
		t.Run("reduces month by 1", func(t *testing.T) {
			from, err := compiler.DateFrom(d1, "", "-1")
			assert.NoError(t, err)
			assert.Equal(t, "2022-12-15", from.Format(compiler.ISODateFormat))
		})
		t.Run("sets month as 5", func(t *testing.T) {
			from, err := compiler.DateFrom(d1, "", "5")
			assert.NoError(t, err)
			assert.Equal(t, "2023-05-15", from.Format(compiler.ISODateFormat))
		})
		t.Run("sets day as 20", func(t *testing.T) {
			from, err := compiler.DateFrom(d1, "20", "")
			assert.NoError(t, err)
			assert.Equal(t, "2023-01-20", from.Format(compiler.ISODateFormat))
		})
		t.Run("sets to first of month", func(t *testing.T) {
			from, err := compiler.DateFrom(d1, "1", "")
			assert.NoError(t, err)
			assert.Equal(t, "2023-01-01", from.Format(compiler.ISODateFormat))
		})
		t.Run("allows to set and change", func(t *testing.T) {
			from, err := compiler.DateFrom(d1, "1", "+3")
			assert.NoError(t, err)
			assert.Equal(t, "2023-04-01", from.Format(compiler.ISODateFormat))
		})
	})
}
