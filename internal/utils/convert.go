package utils

import (
	"errors"
	"math"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/AlecAivazis/survey/v2"
)

var lotus123StartTimeReference = time.Date(1899, 12, 30, 0, 0, 0, 0, time.UTC) // Google Sheets API returns serialised days since 1899-12-30

func ConvertToStringMap(inputs map[string]interface{}) (map[string]string, error) {
	conv := map[string]string{}

	for key, val := range inputs {
		switch reflect.TypeOf(val).Name() {
		case "int":
			conv[key] = strconv.Itoa(val.(int))
		case "string":
			conv[key] = val.(string)
		case "OptionAnswer":
			conv[key] = val.(survey.OptionAnswer).Value
		case "bool":
			conv[key] = strconv.FormatBool(val.(bool))
		default:
			return conv, errors.New("unknown type found while parsing user inputs")
		}
	}
	return conv, nil
}

var (
	timePattern = regexp.MustCompile(`YYYY|YY|MMMM|MMM|MM|M|DDDD|DDD|DD|_D|D|ddd|__d|hh|h|am\/pm|AM\/PM|AM|PM|am|pm|mm|m|ss|s|u|n|TTT|±hhmmss|±hh\:mm\:ss|±hhmm|±hh\:mm|±hh|Zhhmmss|Zhh\:mm\:ss|Zhh:mm|Zhhmm|Zhh|`)
	truePattern = regexp.MustCompile(`^true$|^t$|^1$|^yes$|^y$`)
	numberRegex = regexp.MustCompile(`^[-+]?\d*(\.\d+)?$`)
)

var formatMap = map[string]string{
	"YYYY": "2006", "YY": "06",
	"MMMM": "January", "MMM": "Jan", "MM": "01", "M": "1",
	"DDDD": "Monday", "DDD": "Mon", "DD": "02", "_D": "_2", "D": "2", "ddd": "002", "__d": "__2",
	"hh": "15", "h": "3",
	"am": "pm", "pm": "pm", "AM": "PM", "PM": "PM", "am/pm": "pm", "AM/PM": "PM",
	"mm": "04", "m": "4",
	"ss": "05", "s": "5",
	"u":       "000000",
	"n":       "000000000",
	"TTT":     "MST",
	"±hhmmss": "-070000", "±hh:mm:ss": "-07:00:00", "±hhmm": "-0700", "±hh:mm": "-07:00", "±hh": "-07",
	"Zhhmmss": "Z070000", "Zhh:mm:ss": "Z07:00:00", "Zhhmm": "Z0700", "Zhh:mm": "Z07:00", "Zhh": "Z07",
}

func ConvertTimeToGoLayout(format string) string {
	return timePattern.ReplaceAllStringFunc(format, func(match string) string {
		if val, exists := formatMap[match]; exists {
			return val
		}
		return match
	})
}

func ConvertLotus123SerialToTime(lotus123Serial float64, precision time.Duration) time.Time {
	timeObj := lotus123StartTimeReference

	// Convert precision to a float64 factor (e.g., milliseconds = 86_400_000 units per day)
	unitsPerDay := float64(24 * time.Hour / precision)
	totalUnits := int64(math.Round(lotus123Serial * unitsPerDay))

	// Add time in batches to prevent overflow.
	//
	// Background:
	// The Lotus 1-2-3 serial date system uses a floating-point number where the integer
	// part represents days and the fractional part represents the time of day.
	// We convert this to milliseconds since a reference start date.
	//
	// Problem:
	// When the serial represents a far future date (e.g., year 3000), the total number
	// of milliseconds becomes too large to safely add using time.Duration due to overflow.
	//
	// Solution:
	// Split the total milliseconds into manageable batches and add them incrementally.
	// We use a batch size of 100 years (36500 days) worth of milliseconds.
	batchDays := int64(36500)
	batchSize := batchDays * int64(unitsPerDay)
	batchCount := totalUnits / batchSize
	remainingUnits := totalUnits % batchSize

	for i := int64(0); i < batchCount; i++ {
		timeObj = timeObj.Add(precision * time.Duration(batchSize))
	}

	return timeObj.Add(precision * time.Duration(remainingUnits))
}

func ConvertToBoolean(input string) bool {
	return truePattern.MatchString(strings.ToLower(input))
}

func IsNumber(input string) bool {
	return numberRegex.MatchString(strings.ToLower(input))
}
