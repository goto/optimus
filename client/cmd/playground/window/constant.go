package window

type cursorPointer string

const (
	pointToYear   cursorPointer = "year"
	pointToMonth  cursorPointer = "month"
	pointToDay    cursorPointer = "day"
	pointToHour   cursorPointer = "hour"
	pointToMinute cursorPointer = "minute"

	pointToSizeInput      cursorPointer = "size_input"
	pointToSizeUnit       cursorPointer = "size_unit"
	pointToShiftByInput   cursorPointer = "shift_by_input"
	pointToShiftByUnit    cursorPointer = "shift_by_unit"
	pointToTruncateToUnit cursorPointer = "truncate_to_unit"
	pointToLocationInput  cursorPointer = "location_input"
)
