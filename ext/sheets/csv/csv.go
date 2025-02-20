package csv

import (
	"encoding/csv"
	"fmt"
	"strings"

	"github.com/goto/optimus/internal/errors"
)

func FromRecords(data [][]interface{}, formatFn func(rowIndex, colIndex int, data any) (string, error)) (string, error) {
	if len(data) == 0 {
		return "", nil
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
				return "", err
			}
			currRow = append(currRow, s)
		}
		for i < lenRecords {
			currRow = append(currRow, "") // add empty column data
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
