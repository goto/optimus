package maxcompute_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/goto/optimus/ext/store/maxcompute"
)

func TestDecimalProperties(t *testing.T) {
	t.Run("when invalid value", func(t *testing.T) {
		t.Run("returns error for invalid precision", func(t *testing.T) {
			d1 := maxcompute.Decimal{
				Precision: 0,
				Scale:     5,
			}
			err := d1.Validate()
			assert.Error(t, err)
			assert.ErrorContains(t, err, "decimal precision[0] is not valid")
		})
		t.Run("returns error for invalid scale", func(t *testing.T) {
			d1 := maxcompute.Decimal{
				Precision: 5,
				Scale:     20,
			}
			err := d1.Validate()
			assert.Error(t, err)
			assert.ErrorContains(t, err, "decimal scale[20] is not valid")
		})
	})
	t.Run("returns no error when valid", func(t *testing.T) {
		d1 := maxcompute.Decimal{
			Precision: 5,
			Scale:     10,
		}
		err := d1.Validate()
		assert.Nil(t, err)
	})
}
func TestCharProperties(t *testing.T) {
	t.Run("returns error when invalid length", func(t *testing.T) {
		v1 := maxcompute.Char{Length: 256}
		err := v1.Validate()
		assert.Error(t, err)
		assert.ErrorContains(t, err, "char length[256] is not valid")
	})
	t.Run("returns no error when valid", func(t *testing.T) {
		v1 := maxcompute.Char{Length: 100}
		err := v1.Validate()
		assert.Nil(t, err)
	})
}
func TestVarCharProperties(t *testing.T) {
	t.Run("returns error when invalid length", func(t *testing.T) {
		v1 := maxcompute.VarChar{Length: 0}
		err := v1.Validate()
		assert.Error(t, err)
		assert.ErrorContains(t, err, "varchar length[0] is not valid")
	})
	t.Run("returns no error when valid", func(t *testing.T) {
		v1 := maxcompute.VarChar{Length: 100}
		err := v1.Validate()
		assert.Nil(t, err)
	})
}
