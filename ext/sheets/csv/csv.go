package csv

import (
	"encoding/csv"
	"strings"
)

func FromRecords(data [][]interface{}, formatFn func(colIndex int, data any) string) (string, error) {
	if len(data) == 0 {
		return "", nil
	}

	lenRecords := len(data[0])
	var allRecords [][]string
	for _, row := range data {
		var currRow []string
		i := 0
		for columnIndex, r1 := range row {
			i++
			var s string
			if formatFn != nil {
				s = formatFn(columnIndex, r1)
			} else {
				var ok bool
				s, ok = r1.(string)
				if !ok {
					s = ""
				}
			}
			currRow = append(currRow, s)
		}
		for i < lenRecords {
			currRow = append(currRow, "")
			i++
		}
		allRecords = append(allRecords, currRow)
	}

	return FromData(allRecords)
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
