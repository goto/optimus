package csv

import (
	"encoding/csv"
	"strings"
)

func From(data [][]interface{}) (string, error) {
	if len(data) == 0 {
		return "", nil
	}

	lenRecords := len(data[0])
	var allRecords [][]string
	for _, row := range data {
		var currRow []string
		i := 0
		for _, r1 := range row {
			i++
			s, ok := r1.(string)
			if !ok {
				s = ""
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
