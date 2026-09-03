package service

import (
	"time"

	"github.com/goto/optimus/internal/lib/cron"
)

// JKT is the fixed reference timezone for "today" boundaries in run selection.
var JKT = time.FixedZone("JKT", 7*60*60) //nolint:gochecknoglobals

// SelectScheduledAt picks which of a job's scheduled runs is relevant for a
// completeness check, given nowJKT (already in the JKT timezone).
//
//   - Sub-daily (more than once a day): the most recently fired occurrence, even if
//     still running. E.g. an hourly job checked at 2:00-2:59 reports the 2:00 run.
//   - Daily and slower: today's occurrence if today is a scheduled day and its time
//     has passed; hasSchedule=false (report NOT_COMPLETE) if it hasn't passed yet;
//     otherwise the last occurrence before now. Strict daily cadence falls out of this
//     automatically since every day has an occurrence.
func SelectScheduledAt(jobCron *cron.ScheduleSpec, nowJKT time.Time) (scheduledAt time.Time, hasSchedule bool) {
	if jobCron.IsSubDaily() {
		return jobCron.Prev(nowJKT), true
	}

	todayStart := time.Date(nowJKT.Year(), nowJKT.Month(), nowJKT.Day(), 0, 0, 0, 0, nowJKT.Location())
	todayEnd := todayStart.Add(24 * time.Hour)

	// Next is strict, so probe one nanosecond early to also catch an occurrence
	// exactly at todayStart (cf. Schedule.GetLogicalStartTime).
	todayOccurrence := jobCron.Next(todayStart.Add(-time.Nanosecond))

	if todayOccurrence.Before(todayEnd) {
		if nowJKT.Before(todayOccurrence) {
			return time.Time{}, false
		}
		return todayOccurrence, true
	}

	return jobCron.Prev(nowJKT), true // not a scheduled day at all
}
