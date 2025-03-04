package utils_test

import (
	"testing"

	"github.com/AlecAivazis/survey/v2"
	"github.com/stretchr/testify/assert"

	"github.com/goto/optimus/internal/utils"
)

func TestConvert(t *testing.T) {
	t.Run("convert map containing int, string and optionAnswer", func(t *testing.T) {
		optionAnswer := survey.OptionAnswer{
			Value: "value",
		}
		inputs := map[string]interface{}{
			"key-1": 1,
			"key-2": "string",
			"key-3": optionAnswer,
		}
		answerMap, err := utils.ConvertToStringMap(inputs)
		assert.Nil(t, err)
		assert.Equal(t, "1", answerMap["key-1"])
		assert.Equal(t, "string", answerMap["key-2"])
		assert.Equal(t, optionAnswer.Value, answerMap["key-3"])
	})
	t.Run("convert fails while converting double vals	", func(t *testing.T) {
		inputs := map[string]interface{}{
			"key-1": 0.5,
		}
		_, err := utils.ConvertToStringMap(inputs)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "unknown type found while parsing user inputs")
	})
}

func TestConvertTimeToGoLayout(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"YYYY-MM-DD", "2006-01-02"},
		{"YY/MM/DD", "06/01/02"},
		{"MMMM DDDD", "January Monday"},
		{"MMM DDD", "Jan Mon"},
		{"MM/DD/YYYY", "01/02/2006"},
		{"M/D/YY", "1/2/06"},
		{"hh:mm:ss", "15:04:05"},
		{"h:m:s am/pm", "3:4:5 pm"},
		{"±hh:mm:ss", "-07:00:00"},
		{"Zhh:mm", "Z07:00"},
		{"TTT", "MST"},
		{"YYYY-MM-DD hh:mm:ss TTT ±hh:mm", "2006-01-02 15:04:05 MST -07:00"},
	}

	for _, test := range tests {
		t.Run(test.input, func(t *testing.T) {
			actual := utils.ConvertTimeToGoLayout(test.input)
			assert.Equal(t, test.expected, actual)
		})
	}
}
