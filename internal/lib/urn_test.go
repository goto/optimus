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

	t.Run("should return zero urn and nil if parsing empty succeeds", func(t *testing.T) {
		urn := ""

		actualURN, actualError := lib.ParseURN(urn)

		assert.Zero(t, actualURN)
		assert.NoError(t, actualError)
	})

	t.Run("should return non-zero urn and nil if parsing non-empty succeeds", func(t *testing.T) {
		urn := "store://project.dataset.name"

		actualURN, actualError := lib.ParseURN(urn)

		assert.NotZero(t, actualURN)
		assert.NoError(t, actualError)
	})
}

func TestURN(t *testing.T) {
	t.Run("IsZero", func(t *testing.T) {
		t.Run("should return true if urn is zero valued", func(t *testing.T) {
			var urn lib.URN

			assert.True(t, urn.IsZero())
		})

		t.Run("should return false if urn is not zero valued", func(t *testing.T) {
			rawURN := "store://project.dataset.name"

			urn, err := lib.ParseURN(rawURN)
			assert.NoError(t, err)

			assert.False(t, urn.IsZero())
		})
	})

	t.Run("should return empty member if urn is not initialized", func(t *testing.T) {
		var urn lib.URN

		assert.Empty(t, urn.GetStore())
		assert.Empty(t, urn.GetName())
		assert.Empty(t, urn.String())
	})

	t.Run("should return proper member if the urn is parsed", func(t *testing.T) {
		rawURN := "store://project.dataset.name"

		expectedStore := "store"
		expectedName := "project.dataset.name"

		urn, err := lib.ParseURN(rawURN)
		assert.NoError(t, err)

		assert.EqualValues(t, expectedStore, urn.GetStore())
		assert.EqualValues(t, expectedName, urn.GetName())
		assert.EqualValues(t, rawURN, urn.String())
	})
}

func TestZeroURN(t *testing.T) {
	t.Run("should return zero urn", func(t *testing.T) {
		assert.Zero(t, lib.ZeroURN())
	})
}
