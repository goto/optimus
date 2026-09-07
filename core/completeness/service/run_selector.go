package service

import (
	"time"

	"github.com/goto/optimus/internal/lib/cron"
)

// UTC is the default "today" reference timezone for run selection, used when
// Config.Location isn't set.
var UTC = time.UTC //nolint:gochecknoglobals

// SelectScheduledAt picks which of a job's scheduled runs is relevant for a completeness check
//
//   - Sub-daily (more than once a day): the most recently fired occurrence, even if
//     still running. E.g. an hourly job checked at 2:00-2:59 reports the 2:00 run.
//   - Daily and slower: today's occurrence if today is a scheduled day and its time
//     has passed; hasSchedule=false (report NOT_COMPLETE) if it hasn't passed yet;
//     otherwise the last occurrence before now. Strict daily cadence falls out of this
//     automatically since every day has an occurrence.
func SelectScheduledAt(jobCron *cron.ScheduleSpec, now time.Time, loc *time.Location) (scheduledAt time.Time, hasSchedule bool) {
	nowLocal := now.In(loc)
	nowUTC := now.UTC()

	if jobCron.IsSubDaily() {
		return jobCron.Prev(nowUTC).In(loc), true
	}

	todayStart := time.Date(nowLocal.Year(), nowLocal.Month(), nowLocal.Day(), 0, 0, 0, 0, loc)
	todayEnd := todayStart.Add(24 * time.Hour)

	todayOccurrence := jobCron.Next(todayStart.Add(-time.Second).UTC()).In(loc)

	if todayOccurrence.Before(todayEnd) {
		if nowLocal.Before(todayOccurrence) {
			return time.Time{}, false
		}
		return todayOccurrence, true
	}

	return jobCron.Prev(nowUTC).In(loc), true // not a scheduled day at all
}
