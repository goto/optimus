package csv

import (
	"encoding/csv"
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/goto/optimus/internal/errors"
)

func FromRecords(data [][]interface{}, formatFn func(rowIndex, colIndex int, data any) (string, error)) (string, bool, error) {
	if len(data) == 0 {
		return "", false, nil
	}

	lenRecords := len(data[0])
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

func FromData(records [][]string) (string, error) {
	out := new(strings.Builder)
	w := csv.NewWriter(out)

	err := w.WriteAll(records)
	if err != nil {
		return "", err
	}

	return out.String(), nil
}
