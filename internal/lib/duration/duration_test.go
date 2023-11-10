package duration_test

import (
	"testing"
	"time"

	"github.com/goto/optimus/internal/lib/duration"
)

func TestDuration(t *testing.T) {
	t.Run("SubtractFrom", func(t *testing.T) {
		referenceTime := time.Date(2023, 11, 10, 10, 20, 50, 0, time.UTC)

		tests := []struct {
			name string
			dur  duration.Duration
			want time.Time
		}{
			{
				name: "returns same date when None unit",
				dur:  duration.NewDuration(0, duration.None),
				want: referenceTime,
			},
			{
				name: "returns same date when unknown unit",
				dur:  duration.NewDuration(0, "Unknown"),
				want: referenceTime,
			},
			{
				name: "returns hour before when positive",
				dur:  duration.NewDuration(2, duration.Hour),
				want: time.Date(2023, 11, 10, 8, 20, 50, 0, time.UTC),
			},
			{
				name: "returns hours after when negative duration",
				dur:  duration.NewDuration(-2, duration.Hour),
				want: time.Date(2023, 11, 10, 12, 20, 50, 0, time.UTC),
			},
			{
				name: "returns days before when positive",
				dur:  duration.NewDuration(1, duration.Day),
				want: time.Date(2023, 11, 9, 10, 20, 50, 0, time.UTC),
			},
			{
				name: "returns days after when negative duration",
				dur:  duration.NewDuration(-2, duration.Day),
				want: time.Date(2023, 11, 12, 10, 20, 50, 0, time.UTC),
			},
			{
				name: "returns week before when positive",
				dur:  duration.NewDuration(1, duration.Week),
				want: time.Date(2023, 11, 3, 10, 20, 50, 0, time.UTC),
			},
			{
				name: "returns weeks after when negative duration",
				dur:  duration.NewDuration(-1, duration.Week),
				want: time.Date(2023, 11, 17, 10, 20, 50, 0, time.UTC),
			}, {
				name: "returns months before when positive",
				dur:  duration.NewDuration(2, duration.Month),
				want: time.Date(2023, 9, 10, 10, 20, 50, 0, time.UTC),
			},
			{
				name: "returns month after when negative duration",
				dur:  duration.NewDuration(-2, duration.Month),
				want: time.Date(2024, 01, 10, 10, 20, 50, 0, time.UTC),
			}, {
				name: "returns year before when positive",
				dur:  duration.NewDuration(1, duration.Year),
				want: time.Date(2022, 11, 10, 10, 20, 50, 0, time.UTC),
			},
			{
				name: "returns year after when negative duration",
				dur:  duration.NewDuration(-1, duration.Year),
				want: time.Date(2024, 11, 10, 10, 20, 50, 0, time.UTC),
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				final := tt.dur.SubtractFrom(referenceTime)
				if final != tt.want {
					t.Errorf("Duration[%v] SubtractFrom returned %v, want %v", tt.dur,
						final.Format(time.RFC3339), tt.want.Format(time.RFC3339))
				}
			})
		}
	})
}
