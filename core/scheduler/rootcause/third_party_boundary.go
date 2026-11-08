package rootcause

import "time"

const defaultThirdPartyDelayStartHour = 0

// ThirdPartyDailyBoundary is the most recent daily occurrence of startHourUTC at or
// before the reference time, in UTC. Sensors lack a native scheduled_at, so the boundary
// doubles as the daily incident key's scheduled_at and as the floor of the measured wait:
// waits before it are not counted as third-party induced delay. A start hour of 18, for
// example, anchors the daily sensor window at 01:00 Jakarta.
func ThirdPartyDailyBoundary(reference time.Time, startHourUTC int) time.Time {
	if startHourUTC < 0 || startHourUTC > 23 {
		startHourUTC = defaultThirdPartyDelayStartHour
	}
	ref := reference.UTC()
	boundary := time.Date(ref.Year(), ref.Month(), ref.Day(), startHourUTC, 0, 0, 0, time.UTC)
	if boundary.After(ref) {
		boundary = boundary.AddDate(0, 0, -1)
	}
	return boundary
}
