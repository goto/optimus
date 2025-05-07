package compiler

import (
	"strconv"
	"strings"
	"text/template"
	"time"
)

func OptimusFuncMap() template.FuncMap {
	return map[string]any{
		"Date":        Date,
		"replace":     Replace,
		"trunc":       Trunc,
		"date":        date,
		"date_modify": DateModify,
		"toDate":      toDate,
		"unixEpoch":   UnixEpoch,
		"list":        List,
		"join":        Join,
		"dateFrom":    DateFrom,
		"dateFromStr": DateFromStr,
	}
}

func Date(timeStr string) (string, error) {
	t, err := time.Parse(ISOTimeFormat, timeStr)
	if err != nil {
		return "", err
	}
	return t.Format(ISODateFormat), nil
}

func Replace(old, newStr, name string) string {
	return strings.ReplaceAll(name, old, newStr)
}

func Trunc(c int, s string) string {
	if c >= 0 && len(s) > c {
		return s[:c]
	}
	return s
}

func date(fmt string, date interface{}) string {
	// Cannot have a reliable test, depends on local machine time
	return dateInZone(fmt, date, "Local")
}

func dateInZone(fmt string, date interface{}, zone string) string {
	var t time.Time
	switch date := date.(type) {
	default:
		t = time.Now()
	case time.Time:
		t = date
	case *time.Time:
		t = *date
	case int64:
		t = time.Unix(date, 0)
	case int:
		t = time.Unix(int64(date), 0)
	case int32:
		t = time.Unix(int64(date), 0)
	}

	loc, err := time.LoadLocation(zone)
	if err != nil {
		loc, _ = time.LoadLocation("UTC")
	}

	return t.In(loc).Format(fmt)
}

func DateModify(fmt string, date time.Time) time.Time {
	d, err := time.ParseDuration(fmt)
	if err != nil {
		return date
	}
	return date.Add(d)
}

func toDate(fmt, str string) time.Time {
	// Cannot have a reliable test, depends on local machine time
	t, _ := time.ParseInLocation(fmt, str, time.Local)
	return t
}

func UnixEpoch(date time.Time) string {
	return strconv.FormatInt(date.Unix(), 10) //nolint
}

func List(v ...string) []string {
	return v
}

func Join(sep string, v []string) string {
	return strings.Join(v, sep)
}

func DateFromStr(day, month, timeStr string) (string, error) {
	t, err := time.Parse(ISOTimeFormat, timeStr)
	if err != nil {
		return "", err
	}

	from, err := DateFrom(day, month, t)
	if err != nil {
		return "", err
	}

	return from.Format(ISODateFormat), nil
}

func DateFrom(day, month string, date time.Time) (time.Time, error) {
	d1 := date
	dayNum, err := getValueOnOp(day, d1.Day())
	if err != nil {
		return date, err
	}

	monthNum, err := getValueOnOp(month, int(d1.Month()))
	if err != nil {
		return date, err
	}

	d2 := time.Date(d1.Year(), time.Month(monthNum), dayNum,
		d1.Hour(), d1.Minute(), d1.Second(), d1.Nanosecond(), d1.Location())
	return d2, nil
}

func getValueOnOp(str string, val int) (int, error) {
	if str == "" {
		return val, nil
	}

	numStr := str
	if strings.HasPrefix(str, "+") || strings.HasPrefix(str, "-") {
		numStr = str[1:]
	}

	num, err := strconv.Atoi(numStr)
	if err != nil {
		return 0, err
	}

	if strings.HasPrefix(str, "+") {
		num = val + num
		return num, nil
	}

	if strings.HasPrefix(str, "-") {
		num = val - num
		return num, nil
	}

	return num, nil
}
