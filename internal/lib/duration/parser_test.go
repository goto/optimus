package duration_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/goto/optimus/internal/lib/duration"
)

func TestParser(t *testing.T) {
	t.Run("From", func(t *testing.T) {
		tests := []struct {
			name   string
			input  string
			errStr string
			value  duration.Duration
		}{
			{
				name:   "returns error when invalid",
				input:  "5s",
				errStr: "invalid value for unit s, accepted values are [h,d,w,M,y]",
				value:  duration.Duration{},
			},
			{
				name:   "returns error when invalid unit",
				input:  "5g",
				errStr: "invalid value for unit g, accepted values are [h,d,w,M,y]",
				value:  duration.Duration{},
			},
			{
				name:   "returns error when invalid count",
				input:  "gh",
				errStr: "parsing \"g\": invalid syntax",
				value:  duration.Duration{},
			},
			{
				name:  "returns None unit when None",
				input: "None",
				value: duration.NewDuration(0, duration.None),
			},
			{
				name:  "returns None unit when empty",
				input: "",
				value: duration.NewDuration(0, duration.None),
			},
			{
				name:  "returns hourly duration",
				input: "5h",
				value: duration.NewDuration(5, duration.Hour),
			},
			{
				name:  "returns negative hourly duration",
				input: "-6h",
				value: duration.NewDuration(-6, duration.Hour),
			},
			{
				name:  "returns daily duration",
				input: "3d",
				value: duration.NewDuration(3, duration.Day),
			},
			{
				name:  "returns weekly duration",
				input: "2w",
				value: duration.NewDuration(2, duration.Week),
			},
			{
				name:  "returns monthly duration",
				input: "3M",
				value: duration.NewDuration(3, duration.Month),
			},
			{
				name:  "returns yearly duration",
				input: "2y",
				value: duration.NewDuration(2, duration.Year),
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				final, err := duration.From(tt.input)
				if err != nil {
					assert.ErrorContains(t, err, tt.errStr)
				} else {
					assert.Equal(t, tt.value, final)
				}
			})
		}

		t.Run("returns components from duration", func(t *testing.T) {
			d, err := duration.From("5d")
			assert.NoError(t, err)
			assert.Equal(t, duration.Day, d.GetUnit())
			assert.Equal(t, 5, d.GetCount())
		})
	})
	t.Run("Validate", func(t *testing.T) {
		t.Run("returns error when invalid", func(t *testing.T) {
			err := duration.Validate("5s")
			assert.Error(t, err)
			assert.ErrorContains(t, err, "invalid string for duration 5s")
		})
		t.Run("returns Duration when valid", func(t *testing.T) {
			values := []string{"None", "5d", "3h", "6M", "2w"}
			for _, v := range values {
				err := duration.Validate(v)
				assert.NoError(t, err)
			}
		})
	})
	t.Run("UnitFrom", func(t *testing.T) {
		t.Run("returns error when invalid unit", func(t *testing.T) {
			_, err := duration.UnitFrom("invalid")
			assert.Error(t, err)
			assert.ErrorContains(t, err, "invalid value for unit invalid, accepted values are [h,d,w,M,y]")
		})
		t.Run("returns valid unit when correct input", func(t *testing.T) {
			values := []string{"None", "h", "d", "w", "M", "y"}
			for _, v := range values {
				u, err := duration.UnitFrom(v)
				assert.NoError(t, err)
				assert.Equal(t, string(u), v)
			}
		})
	})
	t.Run("CountFrom", func(t *testing.T) {
		t.Run("returns error when invalid", func(t *testing.T) {
			_, err := duration.CountFrom("invalid")
			assert.Error(t, err)
			assert.ErrorContains(t, err, "parsing \"invalid\": invalid syntax")
		})
		t.Run("returns count when valid", func(t *testing.T) {
			count, err := duration.CountFrom("5")
			assert.NoError(t, err)
			assert.Equal(t, 5, count)
		})
		t.Run("returns count when negative number", func(t *testing.T) {
			count, err := duration.CountFrom("-5")
			assert.NoError(t, err)
			assert.Equal(t, -5, count)
		})
	})
}
