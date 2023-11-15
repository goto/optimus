package tenant_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/goto/optimus/core/tenant"
)

func TestPreset(t *testing.T) {
	t.Run("NewPreset", func(t *testing.T) {
		t.Run("should return zero preset and error if name is invalid", func(t *testing.T) {
			type presetInput struct {
				name        string
				description string
				truncateTo  string
				delay       string
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
						delay:       "1h",
						size:        "1d",
					},
					expectedErrorMessage: "invalid argument for entity project: name is empty",
				},
				{
					caseName: "description is empty",
					presetInput: presetInput{
						name:        "yesterday",
						description: "",
						delay:       "1h",
						size:        "1d",
					},
					expectedErrorMessage: "invalid argument for entity project: description is empty",
				},
				{
					caseName: "window is invalid",
					presetInput: presetInput{
						name:        "yesterday",
						description: "preset for testing",
						truncateTo:  "Z",
						delay:       "1h",
						size:        "24h",
					},
					expectedErrorMessage: "invalid value for unit Z, accepted values are [h,d,w,M,y]",
				},
			}

			for _, test := range testCases {
				name := test.presetInput.name
				description := test.presetInput.description
				truncateTo := test.presetInput.truncateTo
				delay := test.presetInput.delay
				size := test.presetInput.size

				actualPreset, actualError := tenant.NewPreset(name, description, size, delay, "", truncateTo)

				assert.Zero(t, actualPreset, test.caseName)
				assert.ErrorContains(t, actualError, test.expectedErrorMessage, test.caseName)
			}
		})

		t.Run("should return non-zero preset and nil if no error is encountered", func(t *testing.T) {
			name := "yesterday"
			description := "preset for testing"
			delay := "1h"
			size := "1d"

			actualPreset, actualError := tenant.NewPreset(name, description, size, delay, "", "")

			assert.NotZero(t, actualPreset)
			assert.NoError(t, actualError)
		})
	})

	t.Run("Preset", func(t *testing.T) {
		t.Run("Name", func(t *testing.T) {
			t.Run("should return name", func(t *testing.T) {
				name := "yesterday"
				description := "preset for testing"
				delay := "1h"
				size := "1d"

				preset, err := tenant.NewPreset(name, description, size, delay, "", "")
				assert.NotZero(t, preset)
				assert.NoError(t, err)

				expectedName := "yesterday"

				actualName := preset.Name()

				assert.Equal(t, expectedName, actualName)
			})
		})

		t.Run("Description", func(t *testing.T) {
			t.Run("should return description", func(t *testing.T) {
				name := "yesterday"
				description := "preset for testing"
				delay := "1h"
				size := "1d"

				preset, err := tenant.NewPreset(name, description, size, delay, "", "")
				assert.NotZero(t, preset)
				assert.NoError(t, err)

				expectedDescription := "preset for testing"

				actualDescription := preset.Description()

				assert.Equal(t, expectedDescription, actualDescription)
			})
		})

		t.Run("Equal", func(t *testing.T) {
			t.Run("should return false if not equal", func(t *testing.T) {
				name := "yesterday"
				description := "preset for testing"
				truncateTo := ""
				delay := "1h"
				size := "1d"

				presetReference, err := tenant.NewPreset(name, description, size, delay, "", truncateTo)
				assert.NotZero(t, presetReference)
				assert.NoError(t, err)

				type presetInput struct {
					name        string
					description string
					truncateTo  string
					delay       string
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
							delay:       delay,
							size:        size,
						},
					},
					{
						caseName: "different description",
						presetInput: presetInput{
							name:        name,
							description: "different description for test",
							truncateTo:  truncateTo,
							delay:       delay,
							size:        size,
						},
					},
					{
						caseName: "different truncate_to",
						presetInput: presetInput{
							name:        name,
							description: description,
							truncateTo:  "M",
							delay:       delay,
							size:        size,
						},
					},
					{
						caseName: "different delay",
						presetInput: presetInput{
							name:        name,
							description: description,
							truncateTo:  truncateTo,
							delay:       "2h",
							size:        size,
						},
					},
					{
						caseName: "different size",
						presetInput: presetInput{
							name:        name,
							description: description,
							truncateTo:  truncateTo,
							delay:       delay,
							size:        "23h",
						},
					},
				}

				for _, test := range testCases {
					name := test.presetInput.name
					description := test.presetInput.description
					truncateTo := test.presetInput.truncateTo
					delay := test.presetInput.delay
					size := test.presetInput.size

					actualPreset, actualError := tenant.NewPreset(name, description, size, delay, "", truncateTo)
					assert.NotZero(t, actualPreset, test.caseName)
					assert.NoError(t, actualError, test.caseName)
					assert.False(t, presetReference.Equal(actualPreset))
				}
			})

			t.Run("should return true if equal", func(t *testing.T) {
				name := "yesterday"
				description := "preset for testing"
				delay := "1h"
				size := "1d"

				presetReference, err := tenant.NewPreset(name, description, size, delay, "", "")
				assert.NotZero(t, presetReference)
				assert.NoError(t, err)

				actualPreset, err := tenant.NewPreset(name, description, size, delay, "", "")
				assert.NotZero(t, actualPreset)
				assert.NoError(t, err)

				assert.True(t, presetReference.Equal(actualPreset))
			})
		})
	})
}
