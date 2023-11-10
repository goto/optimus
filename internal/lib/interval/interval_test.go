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
	})
	t.Run("Contains", func(t *testing.T) {
		s1 := time.Date(2023, 9, 14, 5, 0, 0, 0, time.UTC)
		e1 := time.Date(2023, 9, 14, 6, 0, 0, 0, time.UTC)
		t.Run("returns false when intervals do not overlap", func(t *testing.T) {
			i1 := interval.NewInterval(s1, e1)
			i2 := interval.NewInterval(s1.Add(time.Hour*2), e1.Add(time.Hour*2))

			contains := i1.Contains(i2)
			assert.False(t, contains)
		})
		t.Run("returns false when intervals overlap, but not contains", func(t *testing.T) {
			i1 := interval.NewInterval(s1, e1)
			i2 := interval.NewInterval(s1.Add(time.Minute*30), e1.Add(time.Hour*1))

			contains := i1.Contains(i2)
			assert.False(t, contains)
		})
		t.Run("returns true when one contains other", func(t *testing.T) {
			i1 := interval.NewInterval(s1, e1)
			i2 := interval.NewInterval(s1.Add(time.Minute*10), e1.Add(-time.Minute*10))

			contains := i1.Contains(i2)
			assert.True(t, contains)
		})
		t.Run("returns true when same date in other timezone", func(t *testing.T) {
			tzJakarta, _ := time.LoadLocation("Asia/Jakarta")
			i1 := interval.NewInterval(s1, e1)
			i2 := interval.NewInterval(s1.In(tzJakarta), e1.In(tzJakarta))

			contains := i1.Contains(i2)
			assert.True(t, contains)
		})
		t.Run("returns true when both are equal", func(t *testing.T) {
			i1 := interval.NewInterval(s1, e1)
			i2 := interval.NewInterval(s1, e1)

			contains := i1.Contains(i2)
			assert.True(t, contains)
		})
	})
}
