package csv

import (
	"encoding/csv"
	"fmt"
	"io"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/goto/optimus/internal/errors"
)

func FromRecords[T any](data [][]T, columnCount int, formatFn func(rowIndex, colIndex int, data any) (string, error)) (string, bool, error) {
	if len(data) == 0 {
		return "", false, nil
	}

	lenRecords := columnCount
	var allRecords [][]string
	for rowIndex, row := range data {
		var currRow []string
		i := 0
		for columnIndex, r1 := range row {
			i++
			s, err := formatFn(rowIndex, columnIndex, r1)
			err = errors.WrapIfErr("CSVFormatter", fmt.Sprintf(" at row : %d", rowIndex), err)
			if err != nil {
				return "", false, err
			}
			currRow = append(currRow, s)
			if i == lenRecords {
				break
			}
		}
		for i < lenRecords {
			currRow = append(currRow, "") // add empty column data
			i++
		}
		allRecords = append(allRecords, currRow)
	}

	fileNeedQuoteSerde := FileNeedQuoteSerde(allRecords)
	csvData, err := FromData(allRecords)
	return csvData, fileNeedQuoteSerde, err
}

func fieldNeedsQuotes(field string) bool {
	if field == "" {
		return false
	}

	if field == `\.` {
		return true
	}

	if ',' < utf8.RuneSelf {
		for i := 0; i < len(field); i++ {
			c := field[i]
			if c == '\n' || c == '\r' || c == '"' || c == ',' {
				return true
			}
		}
	} else if strings.ContainsRune(field, ',') || strings.ContainsAny(field, "\"\r\n") {
		return true
	}

	r1, _ := utf8.DecodeRuneInString(field)
	return unicode.IsSpace(r1)
}

func FileNeedQuoteSerde(content [][]string) bool {
	for _, row := range content {
		for _, val := range row {
			if fieldNeedsQuotes(val) {
				return true
			}
		}
	}
	return false
}

func FromString(data string) ([][]string, error) {
	reader := csv.NewReader(strings.NewReader(data))
	var records [][]string
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		records = append(records, record)
	}
	return records, nil
}

func FromData(records [][]string) (string, error) {
	out := new(strings.Builder)
	w := csv.NewWriter(out)

	err := w.WriteAll(records)
	if err != nil {
		return "", err
	}

	return out.String(), nil
}
