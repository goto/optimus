package maxcompute_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/goto/optimus/ext/store/maxcompute"
)

func TestRelationalView(t *testing.T) {
	t.Run("return validation error when query is empty", func(t *testing.T) {
		view := maxcompute.View{
			Name:      "customer",
			Project:   "proj",
			Database:  "playground",
			ViewQuery: "",
		}

		err := view.Validate()
		assert.NotNil(t, err)
		assert.ErrorContains(t, err, "view query is empty for proj.playground.customer")
	})
	t.Run("has no validation error for correct view", func(t *testing.T) {
		view := maxcompute.View{
			Name:      "customer",
			Database:  "playground",
			ViewQuery: "select * from `playground.customer_table`",
			Columns:   []string{"id", "name"},
		}

		err := view.Validate()
		assert.Nil(t, err)

		assert.Equal(t, "customer", view.Name)
	})
}
