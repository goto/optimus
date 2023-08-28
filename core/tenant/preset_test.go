package tenant_test

import (
	"testing"

	"github.com/goto/optimus/core/tenant"
	"github.com/goto/optimus/internal/models"
	"github.com/stretchr/testify/assert"
)

func TestPreset(t *testing.T) {
	t.Run("NewPreset", func(t *testing.T) {
		t.Run("should return zero preset and error if name is invalid", func(t *testing.T) {
			type presetInput struct {
				name        string
				description string
				truncateTo  string
				offset      string
				size        string
			}
			testCases := []struct {
				caseName             string
				presetInput          presetInput
				expectedErrorMessage string
			}{
				{
					caseName: "name is empty",
					presetInput: presetInput{
						name:        "",
						description: "preset for testing",
						truncateTo:  "d",
						offset:      "-1h",
						size:        "24h",
					},
					expectedErrorMessage: "invalid argument for entity project: cleaned preset name is empty",
				},
				{
					caseName: "cleaned name resulted in empty",
					presetInput: presetInput{
						name:        "	  ",
						description: "preset for testing",
						truncateTo:  "d",
						offset:      "-1h",
						size:        "24h",
					},
					expectedErrorMessage: "invalid argument for entity project: cleaned preset name is empty",
				},
				{
					caseName: "description is empty",
					presetInput: presetInput{
						name:        "yesterday",
						description: "",
						truncateTo:  "d",
						offset:      "-1h",
						size:        "24h",
					},
					expectedErrorMessage: "invalid argument for entity project: cleaned preset description is empty",
				},
				{
					caseName: "cleaned description resulted in empty",
					presetInput: presetInput{
						name:        "yesterday",
						description: "   	",
						truncateTo:  "d",
						offset:      "-1h",
						size:        "24h",
					},
					expectedErrorMessage: "invalid argument for entity project: cleaned preset description is empty",
				},
				{
					caseName: "window is invalid",
					presetInput: presetInput{
						name:        "yesterday",
						description: "preset for testing",
						truncateTo:  "Z",
						offset:      "-1h",
						size:        "24h",
					},
					expectedErrorMessage: "error validating truncate_to: invalid option provided, provide one of: [h d w M]",
				},
			}

			for _, test := range testCases {
				name := test.presetInput.name
				description := test.presetInput.description
				truncateTo := test.presetInput.truncateTo
				offset := test.presetInput.offset
				size := test.presetInput.size

				actualPreset, actualError := tenant.NewPreset(name, description, truncateTo, offset, size)

				assert.Zero(t, actualPreset, test.caseName)
				assert.EqualError(t, actualError, test.expectedErrorMessage, test.caseName)
			}
		})

		t.Run("should return non-zero preset and nil if no error is encountered", func(t *testing.T) {
			name := "yesterday"
			description := "preset for testing"
			truncateTo := "d"
			offset := "-1h"
			size := "24h"

			actualPreset, actualError := tenant.NewPreset(name, description, truncateTo, offset, size)

			assert.NotZero(t, actualPreset)
			assert.NoError(t, actualError)
		})
	})

	t.Run("Preset", func(t *testing.T) {
		t.Run("Name", func(t *testing.T) {
			t.Run("should return trimmed name", func(t *testing.T) {
				name := "yesterday  "
				description := "preset for testing"
				truncateTo := "d"
				offset := "-1h"
				size := "24h"

				preset, err := tenant.NewPreset(name, description, truncateTo, offset, size)
				assert.NotZero(t, preset)
				assert.NoError(t, err)

				expectedName := "yesterday"

				actualName := preset.Name()

				assert.Equal(t, expectedName, actualName)
			})

			t.Run("should return lowered case of name", func(t *testing.T) {
				name := "Yesterday"
				description := "preset for testing"
				truncateTo := "d"
				offset := "-1h"
				size := "24h"

				preset, err := tenant.NewPreset(name, description, truncateTo, offset, size)
				assert.NotZero(t, preset)
				assert.NoError(t, err)

				expectedName := "yesterday"

				actualName := preset.Name()

				assert.Equal(t, expectedName, actualName)
			})
		})

		t.Run("Description", func(t *testing.T) {
			t.Run("should return trimmed description", func(t *testing.T) {
				name := "yesterday"
				description := "preset for testing  "
				truncateTo := "d"
				offset := "-1h"
				size := "24h"

				preset, err := tenant.NewPreset(name, description, truncateTo, offset, size)
				assert.NotZero(t, preset)
				assert.NoError(t, err)

				expectedDescription := "preset for testing"

				actualDescription := preset.Description()

				assert.Equal(t, expectedDescription, actualDescription)
			})
		})

		t.Run("Window", func(t *testing.T) {
			t.Run("should return window", func(t *testing.T) {
				name := "yesterday"
				description := "preset for testing"
				truncateTo := "d"
				offset := "-1h"
				size := "24h"

				preset, err := tenant.NewPreset(name, description, truncateTo, offset, size)
				assert.NotZero(t, preset)
				assert.NoError(t, err)

				expectedWindow, err := models.NewWindow(2, truncateTo, offset, size)
				assert.NoError(t, err)

				actualWindow := preset.Window()

				assert.EqualValues(t, expectedWindow, actualWindow)
			})
		})

		t.Run("Equal", func(t *testing.T) {
			t.Run("should return false if not equal", func(t *testing.T) {
				name := "yesterday"
				description := "preset for testing"
				truncateTo := "d"
				offset := "-1h"
				size := "24h"

				presetReference, err := tenant.NewPreset(name, description, truncateTo, offset, size)
				assert.NotZero(t, presetReference)
				assert.NoError(t, err)

				type presetInput struct {
					name        string
					description string
					truncateTo  string
					offset      string
					size        string
				}
				testCases := []struct {
					caseName    string
					presetInput presetInput
				}{
					{
						caseName: "different name",
						presetInput: presetInput{
							name:        "different_name",
							description: description,
							truncateTo:  truncateTo,
							offset:      offset,
							size:        size,
						},
					},
					{
						caseName: "different description",
						presetInput: presetInput{
							name:        name,
							description: "different description for test",
							truncateTo:  truncateTo,
							offset:      offset,
							size:        size,
						},
					},
					{
						caseName: "different truncate_to",
						presetInput: presetInput{
							name:        name,
							description: description,
							truncateTo:  "M",
							offset:      offset,
							size:        size,
						},
					},
					{
						caseName: "different offset",
						presetInput: presetInput{
							name:        name,
							description: description,
							truncateTo:  truncateTo,
							offset:      "-2h",
							size:        size,
						},
					},
					{
						caseName: "different name",
						presetInput: presetInput{
							name:        name,
							description: description,
							truncateTo:  truncateTo,
							offset:      offset,
							size:        "23h",
						},
					},
				}

				for _, test := range testCases {
					name := test.presetInput.name
					description := test.presetInput.description
					truncateTo := test.presetInput.truncateTo
					offset := test.presetInput.offset
					size := test.presetInput.size

					actualPreset, actualError := tenant.NewPreset(name, description, truncateTo, offset, size)
					assert.NotZero(t, actualPreset, test.caseName)
					assert.NoError(t, actualError, test.caseName)
					assert.False(t, presetReference.Equal(actualPreset))
				}
			})

			t.Run("should return true if equal", func(t *testing.T) {
				name := "yesterday"
				description := "preset for testing"
				truncateTo := "d"
				offset := "-1h"
				size := "24h"

				presetReference, err := tenant.NewPreset(name, description, truncateTo, offset, size)
				assert.NotZero(t, presetReference)
				assert.NoError(t, err)

				actualPreset, err := tenant.NewPreset(name, description, truncateTo, offset, size)
				assert.NotZero(t, actualPreset)
				assert.NoError(t, err)

				assert.True(t, presetReference.Equal(actualPreset))
			})
		})
	})
}
