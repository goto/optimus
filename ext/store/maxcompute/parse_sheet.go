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
	EntityFormatter   = "CSVFormatter"
	millisecondsInDay = 86400000
)

var googleSheetsStartTimeReference = time.Date(1899, 12, 30, 0, 0, 0, 0, time.UTC) // Google Sheets API returns serialised days since 1899-12-30

func ParseBool(data any) (string, error) {
	var val bool
	switch data := data.(type) {
	case bool:
		val = data
	case string:
		if data == "" { // empty column
			return data, nil
		}
		val = utils.ConvertToBoolean(data)
	default:
		return "", errors.InvalidArgument(EntityFormatter, fmt.Sprintf("parseBool: invalid incoming data: [%v] type for Parsing Bool, Got:%s", data, reflect.TypeOf(data)))
	}
	if val {
		return "True", nil
	}
	return "False", nil
}

func ParseNum(data any, precision int) (string, error) {
	switch data := data.(type) {
	case int:
		return strconv.FormatFloat(float64(data), 'f', precision, 64), nil
	case int8:
		return strconv.FormatFloat(float64(data), 'f', precision, 64), nil
	case int16:
		return strconv.FormatFloat(float64(data), 'f', precision, 64), nil
	case int32:
		return strconv.FormatFloat(float64(data), 'f', precision, 64), nil
	case int64:
		return strconv.FormatFloat(float64(data), 'f', precision, 64), nil
	case float32:
		return strconv.FormatFloat(float64(data), 'f', precision, 64), nil
	case float64:
		return strconv.FormatFloat(data, 'f', precision, 64), nil
	case string:
		if data == "" { // empty column
			return data, nil
		}
		if utils.IsNumber(data) { // to handle very large numbers
			return data, nil
		}
	}
	return "", errors.InvalidArgument(EntityFormatter, fmt.Sprintf("ParseFloat: invalid incoming data: [%v] type for Parsing Float, Got:%s", data, reflect.TypeOf(data)))
}

func ParseDateTime(data any, sourceTimeFormat, outPutType string) (string, error) {
	var parsedTime time.Time
	switch data := data.(type) {
	case float64:
		milliSeconds := int(data * millisecondsInDay)
		parsedTime = googleSheetsStartTimeReference.Add(time.Millisecond * time.Duration(milliSeconds))
	case string:
		if data == "" || sourceTimeFormat == "" {
			return data, nil
		}
		var err error
		goTimeLayout := utils.ConvertTimeToGoLayout(sourceTimeFormat)
		parsedTime, err = time.Parse(goTimeLayout, data)
		if err != nil {
			return "", errors.InvalidArgument(EntityFormatter, fmt.Sprintf("ParseDateTime: error parsing date time , source_time_format: '%s', Corresponding goTimeLayout: '%s', incomming Data: '%s'", sourceTimeFormat, goTimeLayout, data))
		}
	default:
		return "", errors.InvalidArgument(EntityFormatter, fmt.Sprintf("ParseDateTime: invalid incoming data: [%v] type for Parsing DateTime/Date, Got:%s", data, reflect.TypeOf(data)))
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
		return "", errors.InvalidArgument(EntityFormatter, fmt.Sprintf("ParseDateTime: unrecognised output format Got: %s", outPutType))
	}

	return parsedTime.Format(outPutFormat), nil
}

func ParseString(data any) (string, error) {
	switch v := data.(type) {
	case float32, float64, int8, int16, int32, int64:
		return ParseNum(v, -1)
	case bool:
		return ParseBool(v)
	case string:
		return v, nil
	default:
		return "", errors.InvalidArgument(EntityFormatter, fmt.Sprintf("ParseString: invalid incoming data: [%v] type for Parsing Got:%s", data, reflect.TypeOf(data)))
	}
}

func formatSheetData(colIndex int, data any, schema Schema) (string, error) {
	if data == nil {
		return "", nil
	}
	if colIndex >= len(schema) {
		return "", nil
	}
	colSchema := schema[colIndex]
	switch colSchema.Type {
	case "BIGINT", "TINYINT", "SMALLINT", "INT":
		return ParseNum(data, 0)
	case "DOUBLE", "DECIMAL", "FLOAT":
		precision := -1
		if colSchema.Decimal != nil {
			precision = int(colSchema.Decimal.Scale)
		}
		return ParseNum(data, precision)
	case "BOOLEAN":
		return ParseBool(data)
	case "DATETIME", "DATE", "TIMESTAMP", "TIMESTAMP_NTZ":
		return ParseDateTime(data, colSchema.SourceTimeFormat, colSchema.Type)
	default:
		val, err := ParseString(data)
		err = errors.WrapIfErr(EntityFormatter, fmt.Sprintf("Parsing MaxCompute:'%s'", colSchema.Type), err)
		return val, err
	}
}
