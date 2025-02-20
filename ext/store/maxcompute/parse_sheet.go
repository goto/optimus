package maxcompute

import (
	"fmt"
	"reflect"
	"strconv"
	"time"

	"github.com/goto/optimus/internal/errors"
	"github.com/goto/optimus/internal/utils"
)

const (
	EntityFormatter = "CSVFormatter"
)

var googleSheetsStartTimeReference = time.Date(1899, 12, 30, 0, 0, 0, 0, time.UTC) // Google Sheets API returns serialised days since 1899-12-30

func parseBool(data any) (string, error) {
	var val bool
	switch data.(type) {
	case bool:
		val = data.(bool)
	case string:
		s := data.(string)
		if s == "" { // empty column
			return "", nil
		}
		val = utils.ConvertToBoolean(s)
	default:
		return "", errors.InvalidArgument(EntityFormatter, fmt.Sprintf("parseBool: invalid incoming data type for Parsing Bool, Got:%s, expected: %s", reflect.TypeOf(data), "Bool/String"))
	}
	if val {
		return "True", nil
	}
	return "False", nil
}

func parseInt(data any) (string, error) {
	switch data.(type) {
	case float64:
		return strconv.FormatInt(int64(data.(float64)), 10), nil
	case string:
		s := data.(string)
		if s == "" { // empty column
			return s, nil
		}
	}
	return "", errors.InvalidArgument(EntityFormatter, fmt.Sprintf("parseInt: invalid incoming data type for Parsing Int, Got:%s, expected: %s", reflect.TypeOf(data), "Float64"))
}

func parseFloat(data any, precision int) (string, error) {
	switch data.(type) {
	case float64:
		return strconv.FormatFloat(data.(float64), 'f', precision, 64), nil
	case string:
		s := data.(string)
		if s == "" { // empty column
			return s, nil
		}
	}
	return "", errors.InvalidArgument(EntityFormatter, fmt.Sprintf("parseFloat: invalid incoming data type for Parsing Float, Got:%s, expected: %s", reflect.TypeOf(data), "Float64/String"))
}

func parseDateTime(data any, sourceTimeFormat, outPutType string) (string, error) {
	var parsedTime time.Time
	switch data.(type) {
	case float64:
		milliSeconds := int(data.(float64) * 86400000)
		parsedTime = googleSheetsStartTimeReference.Add(time.Millisecond * time.Duration(milliSeconds))
	case string:
		s := data.(string)
		if s == "" { // empty column
			return s, nil
		}
		if sourceTimeFormat == "" {
			return s, nil
		}
		var err error
		goTimeLayout := utils.ConvertTimeToGoLayout(sourceTimeFormat)
		parsedTime, err = time.Parse(goTimeLayout, s)
		if err != nil {
			return "", errors.InvalidArgument(EntityFormatter, fmt.Sprintf("parseDateTime: invalid source_time_format, Got: '%s', Cooresponding goTimeLayout: '%s'", sourceTimeFormat, goTimeLayout))
		}
	default:
		return "", errors.InvalidArgument(EntityFormatter, fmt.Sprintf("parseDateTime: invalid incoming data type for Parsing DateTime/Date, Got:%s, expected: %s", reflect.TypeOf(data), "Float64/String"))
	}
	var outPutFormat string
	switch outPutType {
	case "DATE":
		outPutFormat = time.DateOnly
	case "DATETIME":
		outPutFormat = time.DateTime
	case "TIMESTAMP", "TIMESTAMP_NTZ":
		outPutFormat = "2006-01-02 15:04:05.000000000"
	default:
		return "", errors.InvalidArgument(EntityFormatter, fmt.Sprintf("parseDateTime: unrecognised output format Got: %s", outPutType))
	}

	return parsedTime.Format(outPutFormat), nil
}

func parseString(data any) (string, error) {
	s, ok := data.(string)
	if !ok {
		return "", errors.InvalidArgument(EntityFormatter, fmt.Sprintf("parseString: invalid incoming data type for Parsing Got:%s, expected: %s", reflect.TypeOf(data), "String"))
	}
	return s, nil
}

func formatSheetData(colIndex int, data any, schema Schema) (string, error) {
	if data == nil {
		return "", nil
	}
	colSchema := schema[colIndex]
	switch colSchema.Type {
	case "BIGINT", "TINYINT", "SMALLINT", "INT":
		return parseInt(data)
	case "DOUBLE", "DECIMAL", "FLOAT":
		precision := 14 // this is the default precision
		if colSchema.Decimal != nil {
			precision = int(colSchema.Decimal.Scale)
		}
		return parseFloat(data, precision)
	case "BOOLEAN":
		return parseBool(data)
	case "DATETIME", "DATE", "TIMESTAMP", "TIMESTAMP_NTZ":
		return parseDateTime(data, colSchema.SourceTimeFormat, colSchema.Type)
	default:
		val, err := parseString(data)
		err = errors.WrapIfErr(EntityFormatter, fmt.Sprintf("invalid data type for Parsing MaxCompute:'%s', Got:'%s', expected: String", colSchema.Type, reflect.TypeOf(data)), err)
		return val, err
	}
}
