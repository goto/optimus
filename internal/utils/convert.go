package utils

import (
	"errors"
	"reflect"
	"regexp"
	"strconv"
	"strings"

	"github.com/AlecAivazis/survey/v2"
)

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
	timePattern = regexp.MustCompile(`YYYY|YY|MMMM|MMM|MM|M|DDDD|DDD|DD|_D|D|ddd|__d|hh|h|am\/pm|AM\/PM|AM|PM|am|pm|mm|m|ss|s|\.s|TTT|±hhmmss|±hh\:mm\:ss|±hhmm|±hh\:mm|±hh|Zhhmmss|Zhh\:mm\:ss|Zhh:mm|Zhhmm|Zhh|`)
	truePattern = regexp.MustCompile(`true|t|1|yes|y`)
)

var formatMap = map[string]string{
	"YYYY": "2006", "YY": "06",
	"MMMM": "January", "MMM": "Jan", "MM": "01", "M": "1",
	"DDDD": "Monday", "DDD": "Mon", "DD": "02", "_D": "_2", "D": "2", "ddd": "002", "__d": "__2",
	"hh": "15", "h": "3",
	"am": "pm", "pm": "pm", "AM": "PM", "PM": "PM", "am/pm": "pm", "AM/PM": "PM",
	"mm": "04", "m": "4",
	"ss": "05", "s": "5", ".s": ".000000000",
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

func ConvertToBoolean(input string) bool {
	return truePattern.MatchString(strings.ToLower(input))
}
