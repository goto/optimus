package utils_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/goto/optimus/internal/utils"
)

func TestGetMyersDiff(t *testing.T) {
	text1 := `
this is line 1
this is line 2
this is line 3
this is line 4
this is line 5
this is line 6
this is line 7
this is line 8
this is line 9
this is line 10
`
	t.Run("get diff of two empty files", func(t *testing.T) {

		text11 := ``
		text22 := ``
		diff := utils.GetMyersDiff(strings.Split(text11, "\n"), strings.Split(text22, "\n"), 2)
		assert.Len(t, diff, 0)
	})

	t.Run("get diff of two identical files", func(t *testing.T) {

		text2 := text1
		diff := utils.GetMyersDiff(strings.Split(text1, "\n"), strings.Split(text2, "\n"), 1)
		assert.Len(t, diff, 0)
	})

	t.Run("get diff of two files with rows re arranged", func(t *testing.T) {

		text2 := `
this is line 1
this is line 2
this is line 3
this is line 4
this is line 6
this is line 5
this is line 7
this is line 8
this is line 9
this is line 10
`
		diff := utils.GetMyersDiff(strings.Split(text1, "\n"), strings.Split(text2, "\n"), 2)
		assert.Equal(t, diff, `......
  this is line 3
  this is line 4
- this is line 5
  this is line 6
+ this is line 5
  this is line 7
  this is line 8
......`)
	})

	t.Run("get diff rows re deleted", func(t *testing.T) {

		text2 := `
this is line 1
this is line 2
this is line 3
this is line 4
this is line 6
this is line 7
this is line 8
this is line 9
this is line 10
`
		diff := utils.GetMyersDiff(strings.Split(text1, "\n"), strings.Split(text2, "\n"), 2)
		assert.Equal(t, diff, `......
  this is line 3
  this is line 4
- this is line 5
  this is line 6
  this is line 7
......`)
	})

	t.Run("get diff rows re added", func(t *testing.T) {
		text2 := `
this is line 1
this is line 2
this is line 3
this is line 4
this is line 5
this is line 51
this is line 6
this is line 7
this is line 8
this is line 9
this is line 10
`
		diff := utils.GetMyersDiff(strings.Split(text1, "\n"), strings.Split(text2, "\n"), 2)
		assert.Equal(t, diff, `......
  this is line 4
  this is line 5
+ this is line 51
  this is line 6
  this is line 7
......`)
	})

	t.Run("get diff rows re modified", func(t *testing.T) {
		text2 := `
this is line 1
this is line 2
this is line 3
this is line 4
this is line 51
this is line 6
this is line 71
this is line 8
this is line 9
this is line 10
`
		diff := utils.GetMyersDiff(strings.Split(text1, "\n"), strings.Split(text2, "\n"), 2)
		assert.Equal(t, diff, `......
  this is line 3
  this is line 4
- this is line 5
+ this is line 51
  this is line 6
- this is line 7
+ this is line 71
  this is line 8
  this is line 9
......`)
	})

}
