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
}
