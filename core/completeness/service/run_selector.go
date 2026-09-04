package service

import (
	"time"

	"github.com/goto/optimus/internal/lib/cron"
)

// JKT is the default "today" reference timezone for run selection, used when
// Config.Location isn't set.
var JKT = time.FixedZone("JKT", 7*60*60) //nolint:gochecknoglobals

// SelectScheduledAt picks which of a job's scheduled runs is relevant for a
// completeness check, given now (any timezone -- converted internally as needed) and
// loc, the reference timezone whose calendar day defines "today" for daily-or-slower
// schedules.
//
//   - Sub-daily (more than once a day): the most recently fired occurrence, even if
//     still running. E.g. an hourly job checked at 2:00-2:59 reports the 2:00 run.
//   - Daily and slower: today's occurrence if today is a scheduled day and its time
//     has passed; hasSchedule=false (report NOT_COMPLETE) if it hasn't passed yet;
//     otherwise the last occurrence before now. Strict daily cadence falls out of this
//     automatically since every day has an occurrence.
func SelectScheduledAt(jobCron *cron.ScheduleSpec, now time.Time, loc *time.Location) (scheduledAt time.Time, hasSchedule bool) {
	// "Today" is a calendar day in loc. Job cron intervals are defined in UTC (matching
	// Airflow's schedule_interval convention), so occurrences must be matched against
	// the UTC instant -- robfig/cron matches the hour/minute fields against whatever
	// Location() the given time.Time carries.
	nowLocal := now.In(loc)
	nowUTC := now.UTC()

	if jobCron.IsSubDaily() {
		return jobCron.Prev(nowUTC).In(loc), true
	}

	todayStart := time.Date(nowLocal.Year(), nowLocal.Month(), nowLocal.Day(), 0, 0, 0, 0, loc)
	todayEnd := todayStart.Add(24 * time.Hour)

	// Next is strict, so probe one nanosecond early to also catch an occurrence
	// exactly at todayStart (cf. Schedule.GetLogicalStartTime).
	todayOccurrence := jobCron.Next(todayStart.Add(-time.Nanosecond).UTC()).In(loc)

	if todayOccurrence.Before(todayEnd) {
		if nowLocal.Before(todayOccurrence) {
			return time.Time{}, false
		}
		return todayOccurrence, true
	}

	return jobCron.Prev(nowUTC).In(loc), true // not a scheduled day at all
}
