package gsheet_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/goto/optimus/ext/sheets/gsheet"
)

func TestSheetInfo(t *testing.T) {
	t.Run("return error when id missing", func(t *testing.T) {
		u1 := "https://docs.google.com/spreadsheets/d"

		_, err := gsheet.FromURL(u1)
		assert.Error(t, err)
		assert.ErrorContains(t, err, "not able to get spreadsheetID")
	})
	t.Run("return sheet info with id", func(t *testing.T) {
		u1 := "https://docs.google.com/spreadsheets/d/abcedefgh/edit?usp=sharing"

		info, err := gsheet.FromURL(u1)
		assert.Nil(t, err)
		assert.Equal(t, info.SheetID, "abcedefgh")
		assert.Equal(t, info.GID, "")
	})
	t.Run("return sheet info with sid and gid", func(t *testing.T) {
		u1 := "https://docs.google.com/spreadsheets/d/abcdeghi/edit#gid=3726"

		info, err := gsheet.FromURL(u1)
		assert.Nil(t, err)
		assert.Equal(t, info.SheetID, "abcdeghi")
		assert.Equal(t, info.GID, "3726")
	})
}
