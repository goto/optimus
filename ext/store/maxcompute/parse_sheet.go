package maxcompute

import (
	"strconv"
	"time"

	"github.com/goto/optimus/internal/utils"
)

func parseBool(data any) string {
	var val bool
	switch data.(type) {
	case bool:
		val = data.(bool)
	case string:
		s := data.(string)
		if s == "" { // empty column
			return ""
		}
		val = utils.ConvertToBoolean(s)
	default:
		return "error ehrer"
	}
	if val {
		return "True"
	}
	return "False"
}

func parseInt(data any) string {
	switch data.(type) {
	case float64:
		return strconv.FormatInt(int64(data.(float64)), 10)
	case string:
		s := data.(string)
		if s == "" { // empty column
			return s
		}
	}
	return "error herer"
}

func parseFloat(data any, precision int) string {
	switch data.(type) {
	case float64:
		return strconv.FormatFloat(data.(float64), 'f', precision, 64)
	case string:
		s := data.(string)
		if s == "" { // empty column
			return s
		}
	}
	return "error herer"
}
func parseDate(data any, sourceTimeFormat, outPutType string) string {
	switch data.(type) {
	case string:
		s := data.(string)
		if s == "" { // empty column
			return s
		}

		if sourceTimeFormat != "" {
			goTimeLayout := utils.ConvertTimeToGoLayout(sourceTimeFormat)
			parsedTime, err := time.Parse(goTimeLayout, s)
			if err != nil {
				return s
			}
			var outPutFormat string
			switch outPutType {
			case "DATE":
				outPutFormat = time.DateOnly
			case "DATETIME":
				outPutFormat = time.DateTime
			case "TIMESTAMP", "TIMESTAMP_NTZ":
				outPutFormat = "2006-01-02 15:04:05.000000000"
			}

			return parsedTime.Format(outPutFormat)
		}
		return s

	default:
		return "error here"
	}

}

func formatSheetData(colIndex int, data any, schema Schema) string {
	if data == nil {
		return ""
	}
	colSchema := schema[colIndex]
	switch colSchema.Type {
	case "BIGINT", "TINYINT", "SMALLINT", "INT":
		return parseInt(data)
	case "DOUBLE", "DECIMAL", "FLOAT":
		precision := 4 // this is the default precision
		if colSchema.Decimal != nil {
			precision = int(colSchema.Decimal.Scale)
		}
		return parseFloat(data, precision)
	case "BOOLEAN":
		return parseBool(data)
	case "DATETIME", "DATE", "TIMESTAMP", "TIMESTAMP_NTZ":
		return parseDate(data, colSchema.SourceTimeFormat)
	default:
		s, ok := data.(string)
		if !ok {
			return ""
		}
		return s
	}
}
