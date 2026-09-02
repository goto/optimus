package service

import (
	"time"

	"github.com/goto/optimus/internal/lib/cron"
)

// JKT is the fixed reference timezone for "today" boundary decisions in run
// selection, matching the timezone used in the examples this algorithm was
// specified against.
var JKT = time.FixedZone("JKT", 7*60*60) //nolint:gochecknoglobals

// SelectScheduledAt determines which of a job's scheduled runs is the relevant one to
// check for completeness, given the job's cron schedule and the current time (already
// converted to the JKT reference timezone).
//
// Two cadence buckets:
//
//   - Sub-daily (runs more than once a day): always the occurrence *before* the most
//     recently fired one. A currently in-flight run never blocks completeness on its
//     own -- the last settled run does. E.g. an hourly job checked anytime in the
//     2:00-2:59 window reports the 1:00 run, not the (possibly still running) 2:00 run.
//
//   - Daily and slower (daily, weekly, monthly, irregular/weekday-only): if today has
//     a scheduled occurrence and its time hasn't arrived yet, there is nothing to
//     report (hasSchedule=false, caller should treat this as NOT_COMPLETE). Otherwise
//     the relevant occurrence is today's (if today is a scheduled day and its time has
//     passed) or the last occurrence before now (if today isn't a scheduled day at
//     all). Strict daily cadence needs no separate branch: since it fires every
//     calendar day, "today has a scheduled occurrence" is always true, so it falls out
//     of this same logic automatically.
func SelectScheduledAt(jobCron *cron.ScheduleSpec, nowJKT time.Time) (scheduledAt time.Time, hasSchedule bool) {
	if jobCron.IsSubDaily() {
		lastFired := jobCron.Prev(nowJKT)
		return jobCron.Prev(lastFired), true
	}

	todayStart := time.Date(nowJKT.Year(), nowJKT.Month(), nowJKT.Day(), 0, 0, 0, 0, nowJKT.Location())
	todayEnd := todayStart.Add(24 * time.Hour)

	// Next is strict (occurrence *after* the given time), so probe from one
	// nanosecond before the day boundary to also catch an occurrence exactly at
	// todayStart -- same trick already used in Schedule.GetLogicalStartTime.
	todayOccurrence := jobCron.Next(todayStart.Add(-time.Nanosecond))

	if todayOccurrence.Before(todayEnd) {
		if nowJKT.Before(todayOccurrence) {
			return time.Time{}, false // scheduled today, but hasn't fired yet
		}
		return todayOccurrence, true
	}

	// Today isn't a scheduled day at all -- fall back to the last time it did run.
	return jobCron.Prev(nowJKT), true
}
