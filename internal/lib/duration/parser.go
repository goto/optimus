package duration

import (
	"regexp"
	"strconv"
	"strings"

	"github.com/goto/optimus/internal/errors"
)

var validationRegex = regexp.MustCompile(`^$|^None$|^-?\d+[hdwMy]$`)

func From(str string) (Duration, error) {
	if str == "" || strings.EqualFold(str, "None") {
		return Duration{
			unit:  None,
			count: 0,
		}, nil
	}

	unitIndex := len(str) - 1
	unitStr := str[unitIndex:]
	unit, err := UnitFrom(unitStr)
	if err != nil {
		return Duration{}, err
	}

	count, err := CountFrom(str[0:unitIndex])
	if err != nil {
		return Duration{}, err
	}

	return NewDuration(count, unit), nil
}

func Validate(str string) error {
	if !validationRegex.MatchString(str) {
		return errors.InvalidArgument("window", "invalid string for duration "+str)
	}
	return nil
}

func UnitFrom(u string) (Unit, error) {
	switch u {
	case string(None):
		return None, nil
	case string(Hour):
		return Hour, nil
	case string(Day):
		return Day, nil
	case string(Week):
		return Week, nil
	case string(Month):
		return Month, nil
	case string(Year):
		return Year, nil
	default:
		return "", errors.InvalidArgument("window", "invalid value for unit "+u+", accepted values are [h,d,w,M,y]")
	}
}

func CountFrom(c string) (int, error) {
	v, err := strconv.Atoi(c)
	if err != nil {
		return 0, errors.InvalidArgument("window", "invalid value: "+err.Error())
	}

	return v, nil
}
