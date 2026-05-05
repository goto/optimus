package window_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/goto/optimus/internal/lib/window"
)

func TestIncrementalWindow(t *testing.T) {
	t.Run("FromSchedule", func(t *testing.T) {
		t.Run("returns error when schedule not valid", func(t *testing.T) {
			schedule := "* * * *"
			_, err := window.FromSchedule(schedule)
			assert.Error(t, err)
		})
		t.Run("returns interval when schedule valid", func(t *testing.T) {
			schedule := "0 0 1 * *"
			w, err := window.FromSchedule(schedule)
			assert.NoError(t, err)

			sept1 := time.Date(2023, 9, 1, 1, 0, 0, 0, time.UTC)
			interval, err := w.GetInterval(sept1)
			assert.NoError(t, err)
			assert.Equal(t, "2023-09-01T00:00:00Z", interval.Start().Format(time.RFC3339))
			assert.Equal(t, "2023-10-01T00:00:00Z", interval.End().Format(time.RFC3339))
		})
	})

	t.Run("GetInterval", func(t *testing.T) {
		type testCase struct {
			name          string
			schedule      string
			referenceTime time.Time
			expectedStart string
			expectedEnd   string
		}

		// Flow 1: Sensor schedule window
		// referenceTime is the schedule time of an upstream job.
		// The IncrementalWindow is built from the subject job's own schedule.
		// Two sub-cases:
		//   A) upstream schedule time coincides with subject job schedule boundary
		//   B) upstream schedule time falls between two subject job schedule boundaries
		t.Run("sensor schedule window", func(t *testing.T) {
			t.Run("upstream schedule coincides with subject job schedule", func(t *testing.T) {
				tests := []testCase{
					{
						name:          "daily midnight subject job, upstream also triggers at midnight",
						schedule:      "0 0 * * *",
						referenceTime: time.Date(2023, time.November, 10, 0, 0, 0, 0, time.UTC),
						expectedStart: "2023-11-09T00:00:00Z",
						expectedEnd:   "2023-11-10T00:00:00Z",
					},
					{
						name:          "hourly subject job, upstream triggers exactly on the hour",
						schedule:      "0 * * * *",
						referenceTime: time.Date(2023, time.November, 10, 6, 0, 0, 0, time.UTC),
						expectedStart: "2023-11-10T05:00:00Z",
						expectedEnd:   "2023-11-10T06:00:00Z",
					},
					{
						name:          "monthly subject job, upstream triggers on 1st of month at midnight",
						schedule:      "0 0 1 * *",
						referenceTime: time.Date(2023, time.November, 1, 0, 0, 0, 0, time.UTC),
						expectedStart: "2023-10-01T00:00:00Z",
						expectedEnd:   "2023-11-01T00:00:00Z",
					},
					{
						name:          "weekly Monday midnight subject job, upstream also triggers on Monday midnight",
						schedule:      "0 0 * * 1",
						referenceTime: time.Date(2023, time.November, 6, 0, 0, 0, 0, time.UTC),
						expectedStart: "2023-10-30T00:00:00Z",
						expectedEnd:   "2023-11-06T00:00:00Z",
					},
					{
						name:          "every-6-hour subject job, upstream triggers at a 6-hour boundary",
						schedule:      "0 */6 * * *",
						referenceTime: time.Date(2023, time.November, 10, 12, 0, 0, 0, time.UTC),
						expectedStart: "2023-11-10T06:00:00Z",
						expectedEnd:   "2023-11-10T12:00:00Z",
					},
				}

				for _, tc := range tests {
					t.Run(tc.name, func(t *testing.T) {
						w, err := window.FromSchedule(tc.schedule)
						assert.NoError(t, err)

						iv, err := w.GetInterval(tc.referenceTime)
						assert.NoError(t, err)
						assert.Equal(t, tc.expectedStart, iv.Start().Format(time.RFC3339))
						assert.Equal(t, tc.expectedEnd, iv.End().Format(time.RFC3339))
					})
				}
			})

			t.Run("upstream schedule does not coincide with subject job schedule", func(t *testing.T) {
				tests := []testCase{
					{
						name:          "daily midnight subject job, upstream triggers at 6am on same day",
						schedule:      "0 0 * * *",
						referenceTime: time.Date(2023, time.November, 10, 6, 0, 0, 0, time.UTC),
						expectedStart: "2023-11-10T00:00:00Z",
						expectedEnd:   "2023-11-11T00:00:00Z",
					},
					{
						name:          "daily midnight subject job, upstream triggers at 11pm",
						schedule:      "0 0 * * *",
						referenceTime: time.Date(2023, time.November, 10, 23, 0, 0, 0, time.UTC),
						expectedStart: "2023-11-10T00:00:00Z",
						expectedEnd:   "2023-11-11T00:00:00Z",
					},
					{
						name:          "daily midnight subject job, upstream triggers at noon",
						schedule:      "0 0 * * *",
						referenceTime: time.Date(2023, time.November, 10, 12, 0, 0, 0, time.UTC),
						expectedStart: "2023-11-10T00:00:00Z",
						expectedEnd:   "2023-11-11T00:00:00Z",
					},
					{
						name:          "hourly subject job, upstream triggers at half past the hour",
						schedule:      "0 * * * *",
						referenceTime: time.Date(2023, time.November, 10, 6, 30, 0, 0, time.UTC),
						expectedStart: "2023-11-10T06:00:00Z",
						expectedEnd:   "2023-11-10T07:00:00Z",
					},
					{
						name:          "monthly subject job, upstream triggers mid-month",
						schedule:      "0 0 1 * *",
						referenceTime: time.Date(2023, time.November, 15, 0, 0, 0, 0, time.UTC),
						expectedStart: "2023-11-01T00:00:00Z",
						expectedEnd:   "2023-12-01T00:00:00Z",
					},
					{
						name:          "weekly Monday midnight subject job, upstream triggers on Wednesday",
						schedule:      "0 0 * * 1",
						referenceTime: time.Date(2023, time.November, 8, 0, 0, 0, 0, time.UTC), // Wednesday
						expectedStart: "2023-11-06T00:00:00Z",                                  // Monday
						expectedEnd:   "2023-11-13T00:00:00Z",                                  // Monday
					},
					{
						name:          "every-6-hour subject job, upstream triggers at a non-boundary time",
						schedule:      "0 */6 * * *",
						referenceTime: time.Date(2023, time.November, 10, 9, 0, 0, 0, time.UTC),
						expectedStart: "2023-11-10T06:00:00Z",
						expectedEnd:   "2023-11-10T12:00:00Z",
					},
					{
						name:          "monthly subject job, upstream triggers on last day of the month",
						schedule:      "0 0 1 * *",
						referenceTime: time.Date(2023, time.October, 31, 0, 0, 0, 0, time.UTC),
						expectedStart: "2023-10-01T00:00:00Z",
						expectedEnd:   "2023-11-01T00:00:00Z",
					},
				}

				for _, tc := range tests {
					t.Run(tc.name, func(t *testing.T) {
						w, err := window.FromSchedule(tc.schedule)
						assert.NoError(t, err)

						iv, err := w.GetInterval(tc.referenceTime)
						assert.NoError(t, err)
						assert.Equal(t, tc.expectedStart, iv.Start().Format(time.RFC3339))
						assert.Equal(t, tc.expectedEnd, iv.End().Format(time.RFC3339))
					})
				}
			})
		})

		// Flow 2: Data window for a job run
		// referenceTime is the subject job's own schedule time, so it always
		// falls exactly on a schedule boundary. The window covers the single
		// completed period immediately before that boundary.
		t.Run("data window for job run", func(t *testing.T) {
			tests := []testCase{
				{
					name:          "daily midnight job, window covers the previous day",
					schedule:      "0 0 * * *",
					referenceTime: time.Date(2023, time.November, 10, 0, 0, 0, 0, time.UTC),
					expectedStart: "2023-11-09T00:00:00Z",
					expectedEnd:   "2023-11-10T00:00:00Z",
				},
				{
					name:          "hourly job, window covers the previous hour",
					schedule:      "0 * * * *",
					referenceTime: time.Date(2023, time.November, 10, 14, 0, 0, 0, time.UTC),
					expectedStart: "2023-11-10T13:00:00Z",
					expectedEnd:   "2023-11-10T14:00:00Z",
				},
				{
					name:          "monthly job triggered on 1st, window covers the previous month",
					schedule:      "0 0 1 * *",
					referenceTime: time.Date(2023, time.December, 1, 0, 0, 0, 0, time.UTC),
					expectedStart: "2023-11-01T00:00:00Z",
					expectedEnd:   "2023-12-01T00:00:00Z",
				},
				{
					name:          "weekly Monday job, window covers the previous week",
					schedule:      "0 0 * * 1",
					referenceTime: time.Date(2023, time.November, 13, 0, 0, 0, 0, time.UTC),
					expectedStart: "2023-11-06T00:00:00Z",
					expectedEnd:   "2023-11-13T00:00:00Z",
				},
				{
					name:          "every-6-hour job, window covers the previous 6-hour slot",
					schedule:      "0 */6 * * *",
					referenceTime: time.Date(2023, time.November, 10, 18, 0, 0, 0, time.UTC),
					expectedStart: "2023-11-10T12:00:00Z",
					expectedEnd:   "2023-11-10T18:00:00Z",
				},
				{
					name:          "daily midnight job triggered on the 1st of the month, window ends at month boundary",
					schedule:      "0 0 * * *",
					referenceTime: time.Date(2023, time.November, 1, 0, 0, 0, 0, time.UTC),
					expectedStart: "2023-10-31T00:00:00Z",
					expectedEnd:   "2023-11-01T00:00:00Z",
				},
				{
					name:          "hourly job triggered at midnight, window covers the last hour of the previous day",
					schedule:      "0 * * * *",
					referenceTime: time.Date(2023, time.November, 10, 0, 0, 0, 0, time.UTC),
					expectedStart: "2023-11-09T23:00:00Z",
					expectedEnd:   "2023-11-10T00:00:00Z",
				},
			}

			for _, tc := range tests {
				t.Run(tc.name, func(t *testing.T) {
					w, err := window.FromSchedule(tc.schedule)
					assert.NoError(t, err)

					iv, err := w.GetInterval(tc.referenceTime)
					assert.NoError(t, err)
					assert.Equal(t, tc.expectedStart, iv.Start().Format(time.RFC3339))
					assert.Equal(t, tc.expectedEnd, iv.End().Format(time.RFC3339))
				})
			}
		})
	})
}
