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
	pointToDelayInput     cursorPointer = "delay_input"
	pointToDelayUnit      cursorPointer = "delay_unit"
	pointToTruncateToUnit cursorPointer = "truncate_to_unit"
	pointToLocationInput  cursorPointer = "location_input"
)
