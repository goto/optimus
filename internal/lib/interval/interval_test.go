package interval_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/goto/optimus/internal/lib/interval"
)

func TestInterval(t *testing.T) {
	t.Run("Properties", func(t *testing.T) {
		s1 := time.Date(2023, 9, 14, 5, 0, 0, 0, time.UTC)
		e1 := time.Date(2023, 9, 14, 6, 0, 0, 0, time.UTC)
		t.Run("returns start and end of interval", func(t *testing.T) {
			i1 := interval.NewInterval(s1, e1)
			assert.Equal(t, s1, i1.Start())
			assert.Equal(t, e1, i1.End())
		})
		t.Run("can compare 2 intervals", func(t *testing.T) {
			i1 := interval.NewInterval(s1, e1)
			i2 := interval.NewInterval(s1, e1)
			assert.Equal(t, i1.IsAfter(i2), false)
			assert.Equal(t, i1.Equal(i2), true)
		})
		t.Run("can compare 2 unequal intervals", func(t *testing.T) {
			i1 := interval.NewInterval(s1, e1)
			i2 := interval.NewInterval(s1.Add(time.Hour*10), e1.Add(time.Hour*10))
			assert.Equal(t, i2.IsAfter(i1), true)
			assert.Equal(t, i1.IsAfter(i2), false)
			assert.Equal(t, i1.Equal(i2), false)
		})
		t.Run("does not compare empty time", func(t *testing.T) {
			i1 := interval.NewInterval(s1, e1)
			i2 := interval.Interval{}
			assert.Equal(t, i1.IsAfter(i2), false)
			assert.Equal(t, i2.IsAfter(i1), false)
			assert.Equal(t, i1.Equal(i2), false)
		})
	})
}
