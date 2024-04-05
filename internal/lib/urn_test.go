package lib_test

import (
	"testing"

	"github.com/goto/optimus/internal/lib"
	"github.com/stretchr/testify/assert"
)

func TestParseURN(t *testing.T) {
	t.Run("should return zero urn and error if parsing fails", func(t *testing.T) {
		testTable := []struct {
			conditionName string
			inputURN      string
			errorMessage  string
		}{
			{
				conditionName: "empty urn",
				inputURN:      "",
				errorMessage:  "urn does not follow pattern <store>://<name>",
			},
			{
				conditionName: "store is not specified",
				inputURN:      "://project.dataset.name",
				errorMessage:  "urn store is not specified",
			},
			{
				conditionName: "name is not specified",
				inputURN:      "store://",
				errorMessage:  "urn name is not specified",
			},
			{
				conditionName: "store contains whitespace",
				inputURN:      "store ://project.dataset.name",
				errorMessage:  "urn store contains whitespace",
			},
			{
				conditionName: "name contains whitespace",
				inputURN:      "store://project.dataset.name ",
				errorMessage:  "urn name contains whitespace",
			},
		}

		for _, testCase := range testTable {
			actualURN, actualError := lib.ParseURN(testCase.inputURN)

			assert.Zero(t, actualURN, testCase.conditionName)
			assert.EqualError(t, actualError, testCase.errorMessage, testCase.conditionName)
		}
	})

	t.Run("should return urn and nil if parsing succeeds", func(t *testing.T) {
		urn := "store://project.dataset.name"

		actualURN, actualError := lib.ParseURN(urn)

		assert.NotZero(t, actualURN)
		assert.NoError(t, actualError)
	})
}
