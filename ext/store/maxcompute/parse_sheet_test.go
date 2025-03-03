package maxcompute_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/goto/optimus/ext/store/maxcompute"
)

func TestParseNum(t *testing.T) {
	t.Run("should return formatted float string with given precision, rounding off if needed", func(t *testing.T) {
		data := 123.456789
		precision := 3
		expected := "123.457"

		result, err := maxcompute.ParseNum(data, precision)

		assert.NoError(t, err)
		assert.Equal(t, expected, result)
	})

	t.Run("should return formatted float string with given precision when input is float64", func(t *testing.T) {
		data := 123.1
		precision := 0
		expected := "123"

		result, err := maxcompute.ParseNum(data, precision)

		assert.NoError(t, err)
		assert.Equal(t, expected, result)
	})

	t.Run("should return formatted float string with given precision when input is float64", func(t *testing.T) {
		data := 123.734000
		precision := 0
		expected := "124"

		result, err := maxcompute.ParseNum(data, precision)

		assert.NoError(t, err)
		assert.Equal(t, expected, result)
	})

	t.Run("should return empty string when input is empty string", func(t *testing.T) {
		data := ""
		expected := ""
		precision := 2

		result, err := maxcompute.ParseNum(data, precision)

		assert.NoError(t, err)
		assert.Equal(t, expected, result)
	})

	t.Run("should return error when input is invalid type", func(t *testing.T) {
		data := true
		precision := 2

		result, err := maxcompute.ParseNum(data, precision)
		assert.ErrorContains(t, err, "invalid argument for entity CSVFormatter: ParseFloat: invalid incoming data: [true] type for Parsing Float, Got:bool")
		assert.Equal(t, "", result)
	})

	t.Run("should return formatted int string when input is float64", func(t *testing.T) {
		data := 123.00
		expected := "123"

		result, err := maxcompute.ParseNum(data, -1)

		assert.NoError(t, err)
		assert.Equal(t, expected, result)
	})
	t.Run("should return formatted int string when input is float64", func(t *testing.T) {
		data := 123.30
		expected := "123.3"

		result, err := maxcompute.ParseNum(data, -1)

		assert.NoError(t, err)
		assert.Equal(t, expected, result)
	})
}

func TestParseString(t *testing.T) {
	t.Run("should return the same string when input is a valid string", func(t *testing.T) {
		data := "test string"
		expected := "test string"

		result, err := maxcompute.ParseString(data)

		assert.NoError(t, err)
		assert.Equal(t, expected, result)
	})

	t.Run("should return floating string upto the least significant digit", func(t *testing.T) {
		t.Run("123.10", func(t *testing.T) {
			data := 123.10
			result, err := maxcompute.ParseString(data)

			assert.NoError(t, err)
			assert.Equal(t, "123.1", result)
		})
		t.Run("123.00", func(t *testing.T) {
			data := 123.00
			result, err := maxcompute.ParseString(data)

			assert.NoError(t, err)
			assert.Equal(t, "123", result)
		})
	})

	t.Run("should return empty string when input is an empty string", func(t *testing.T) {
		data := ""
		expected := ""

		result, err := maxcompute.ParseString(data)

		assert.NoError(t, err)
		assert.Equal(t, expected, result)
	})
}

func TestParseDateTime(t *testing.T) {
	t.Run("should return formatted date string when input is float64", func(t *testing.T) {
		data := 44197.23415 // corresponds to 2021-01-01
		expected := "2021-01-01"
		sourceTimeFormat := []string{""}
		outPutType := "DATE"

		result, err := maxcompute.ParseDateTime(data, sourceTimeFormat, outPutType)

		assert.NoError(t, err)
		assert.Equal(t, expected, result)
	})

	t.Run("should return formatted datetime string when input is float64", func(t *testing.T) {
		data := 44197.521 // corresponds to 2021-01-01 12:00:00
		expected := "2021-01-01 12:30:14"
		sourceTimeFormat := []string{""}
		outPutType := "DATETIME"

		result, err := maxcompute.ParseDateTime(data, sourceTimeFormat, outPutType)

		assert.NoError(t, err)
		assert.Equal(t, expected, result)
	})

	t.Run("should return formatted timestamp string when input is float64", func(t *testing.T) {
		data := 44197.5 // corresponds to 2021-01-01 12:00:00
		expected := "2021-01-01 12:00:00.000000000"
		sourceTimeFormat := []string{""}
		outPutType := "TIMESTAMP"

		result, err := maxcompute.ParseDateTime(data, sourceTimeFormat, outPutType)

		assert.NoError(t, err)
		assert.Equal(t, expected, result)
	})

	t.Run("should return formatted date string when input is string", func(t *testing.T) {
		data := "2023/05-01"
		expected := "2023-05-01"
		sourceTimeFormat := []string{"YYYY/MM-DD"}
		outPutType := "DATE"

		result, err := maxcompute.ParseDateTime(data, sourceTimeFormat, outPutType)

		assert.NoError(t, err)
		assert.Equal(t, expected, result)
	})

	t.Run("should return error when input string has invalid format", func(t *testing.T) {
		data := "Jan/23/01"
		expected := "2001-01-23"
		sourceTimeFormat := []string{"MMM/DD/YY"}
		outPutType := "DATE"

		result, err := maxcompute.ParseDateTime(data, sourceTimeFormat, outPutType)

		assert.NoError(t, err)
		assert.Equal(t, expected, result)
	})

	t.Run("should return empty string when input is empty string", func(t *testing.T) {
		data := ""
		expected := ""
		sourceTimeFormat := []string{"yyyy-MM-dd"}
		outPutType := "DATE"

		result, err := maxcompute.ParseDateTime(data, sourceTimeFormat, outPutType)

		assert.NoError(t, err)
		assert.Equal(t, expected, result)
	})

	t.Run("should return error when input is invalid type", func(t *testing.T) {
		data := true
		sourceTimeFormat := []string{"yyyy-MM-dd"}
		outPutType := "DATE"

		result, err := maxcompute.ParseDateTime(data, sourceTimeFormat, outPutType)

		assert.ErrorContains(t, err, "invalid argument for entity CSVFormatter: ParseDateTime: invalid incoming data: [true] type for Parsing DateTime/Date, Got:bool")
		assert.Equal(t, "", result)
	})

	t.Run("should return error when output type is unrecognized", func(t *testing.T) {
		data := 44197.0 // corresponds to 2021-01-01
		sourceTimeFormat := []string{""}
		outPutType := "UNKNOWN"

		result, err := maxcompute.ParseDateTime(data, sourceTimeFormat, outPutType)

		assert.ErrorContains(t, err, "invalid argument for entity CSVFormatter: ParseDateTime: unrecognised output format Got: UNKNOWN")
		assert.Equal(t, "", result)
	})
}

func TestParseBool(t *testing.T) {
	t.Run("should return 'True' when input is boolean true", func(t *testing.T) {
		data := true
		expected := "True"

		result, err := maxcompute.ParseBool(data)

		assert.NoError(t, err)
		assert.Equal(t, expected, result)
	})

	t.Run("should return 'False' when input is boolean false", func(t *testing.T) {
		data := false
		expected := "False"

		result, err := maxcompute.ParseBool(data)

		assert.NoError(t, err)
		assert.Equal(t, expected, result)
	})

	t.Run("should return 'True' when input is string 'true'", func(t *testing.T) {
		data := "true"
		expected := "True"

		result, err := maxcompute.ParseBool(data)

		assert.NoError(t, err)
		assert.Equal(t, expected, result)
	})

	t.Run("should return 'True' when input is string 't'", func(t *testing.T) {
		data := "t"
		expected := "True"

		result, err := maxcompute.ParseBool(data)

		assert.NoError(t, err)
		assert.Equal(t, expected, result)
	})
	t.Run("should return 'True' when input is string '1'", func(t *testing.T) {
		data := "1"
		expected := "True"

		result, err := maxcompute.ParseBool(data)

		assert.NoError(t, err)
		assert.Equal(t, expected, result)
	})
	t.Run("should return 'True' when input is string 'y'", func(t *testing.T) {
		data := "y"
		expected := "True"

		result, err := maxcompute.ParseBool(data)

		assert.NoError(t, err)
		assert.Equal(t, expected, result)
	})
	t.Run("should return 'True' when input is string 'yes'", func(t *testing.T) {
		data := "yes"
		expected := "True"

		result, err := maxcompute.ParseBool(data)

		assert.NoError(t, err)
		assert.Equal(t, expected, result)
	})

	t.Run("should return 'False' when input is string 'false'", func(t *testing.T) {
		data := "false"
		expected := "False"

		result, err := maxcompute.ParseBool(data)

		assert.NoError(t, err)
		assert.Equal(t, expected, result)
	})

	t.Run("should return 'False' when input is string 'f'", func(t *testing.T) {
		data := "f"
		expected := "False"

		result, err := maxcompute.ParseBool(data)

		assert.NoError(t, err)
		assert.Equal(t, expected, result)
	})

	t.Run("should return 'False' when input is string 'anyRandomString'", func(t *testing.T) {
		data := "anyRandomString"
		expected := "False"

		result, err := maxcompute.ParseBool(data)

		assert.NoError(t, err)
		assert.Equal(t, expected, result)
	})
	t.Run("should return 'False' when input is string 'yes1'", func(t *testing.T) {
		data := "yes1"
		expected := "False"

		result, err := maxcompute.ParseBool(data)

		assert.NoError(t, err)
		assert.Equal(t, expected, result)
	})

	t.Run("should return empty string when input is empty string", func(t *testing.T) {
		data := ""
		expected := ""

		result, err := maxcompute.ParseBool(data)

		assert.NoError(t, err)
		assert.Equal(t, expected, result)
	})

	t.Run("should return error when input is invalid type", func(t *testing.T) {
		data := 123

		result, err := maxcompute.ParseBool(data)

		assert.ErrorContains(t, err, "invalid argument for entity CSVFormatter: parseBool: invalid incoming data: [123] type for Parsing Bool, Got:int")
		assert.Equal(t, "", result)
	})
}
