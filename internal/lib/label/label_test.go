package label_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/goto/optimus/internal/lib/label"
)

func TestFromMap(t *testing.T) {
	t.Run("should return nil and error if incoming labels are nil", func(t *testing.T) {
		var incoming map[string]string

		actualLabels, actualError := label.FromMap(incoming)

		assert.Nil(t, actualLabels)
		assert.ErrorContains(t, actualError, "labels is nil")
	})

	t.Run("should return labels and nil if no error is encountered", func(t *testing.T) {
		incoming := map[string]string{
			"key1": "value1",
		}

		actualLabels, actualError := label.FromMap(incoming)

		assert.NotEmpty(t, actualLabels)
		assert.NoError(t, actualError)
	})
}

func TestLabels(t *testing.T) {
	t.Run("should return error if labels is nil", func(t *testing.T) {
		var incoming label.Labels

		actualError := incoming.Validate()

		assert.ErrorContains(t, actualError, "labels is nil")
	})

	t.Run("should return error if labels is more than 32 in length", func(t *testing.T) {
		l := make(map[string]string)
		for i := 0; i < 33; i++ {
			key := fmt.Sprintf("key-%d", i)
			value := fmt.Sprintf("value-%d", i)
			l[key] = value
		}

		incoming, err := label.FromMap(l)
		assert.NoError(t, err)

		actualError := incoming.Validate()

		assert.ErrorContains(t, actualError, "labels length is more than [32]")
	})

	t.Run("should return error if labels key is empty", func(t *testing.T) {
		l := map[string]string{
			"": "test_value",
		}

		incoming, err := label.FromMap(l)
		assert.NoError(t, err)

		actualError := incoming.Validate()

		assert.ErrorContains(t, actualError, "key is empty")
	})

	t.Run("should return error if labels key length is more than 63", func(t *testing.T) {
		l := map[string]string{
			strings.Repeat("a", 64): "test_value",
		}

		incoming, err := label.FromMap(l)
		assert.NoError(t, err)

		actualError := incoming.Validate()

		assert.ErrorContains(t, actualError, "key length is more than [63]")
	})

	t.Run("should return error if labels key contains characters outside alphanumerics, underscores, and dashes", func(t *testing.T) {
		l := map[string]string{
			"invalid_key_with_!": "test_value",
		}

		incoming, err := label.FromMap(l)
		assert.NoError(t, err)

		actualError := incoming.Validate()

		assert.ErrorContains(t, actualError, "key should only be combination of lower case letters, numerics, underscores, and/or dashes")
	})

	t.Run("should return error if labels key contains upper case letters", func(t *testing.T) {
		l := map[string]string{
			"invalid_key_with_A": "test_value",
		}

		incoming, err := label.FromMap(l)
		assert.NoError(t, err)

		actualError := incoming.Validate()

		assert.ErrorContains(t, actualError, "key should only be combination of lower case letters, numerics, underscores, and/or dashes")
	})

	t.Run("should return error if labels key does not start with alphanumeric", func(t *testing.T) {
		l := map[string]string{
			"-invalid_key": "test_value",
		}

		incoming, err := label.FromMap(l)
		assert.NoError(t, err)

		actualError := incoming.Validate()

		assert.ErrorContains(t, actualError, "key should start and end with alphanumerics only")
	})

	t.Run("should return error if labels key does not end with alphanumeric", func(t *testing.T) {
		l := map[string]string{
			"invalid_key-": "test_value",
		}

		incoming, err := label.FromMap(l)
		assert.NoError(t, err)

		actualError := incoming.Validate()

		assert.ErrorContains(t, actualError, "key should start and end with alphanumerics only")
	})

	t.Run("should return error if labels value is empty", func(t *testing.T) {
		l := map[string]string{
			"test_key": "",
		}

		incoming, err := label.FromMap(l)
		assert.NoError(t, err)

		actualError := incoming.Validate()

		assert.ErrorContains(t, actualError, "value is empty")
	})

	t.Run("should return error if labels value length is more than 63", func(t *testing.T) {
		l := map[string]string{
			"test_key": strings.Repeat("a", 64),
		}

		incoming, err := label.FromMap(l)
		assert.NoError(t, err)

		actualError := incoming.Validate()

		assert.ErrorContains(t, actualError, "value length is more than [63]")
	})

	t.Run("should return error if labels value contains characters outside alphanumerics, underscores, and dashes", func(t *testing.T) {
		l := map[string]string{
			"test_key": "invalid_value_with_!",
		}

		incoming, err := label.FromMap(l)
		assert.NoError(t, err)

		actualError := incoming.Validate()

		assert.ErrorContains(t, actualError, "value should only be combination of lower case letters, numerics, underscores, and/or dashes")
	})

	t.Run("should return error if labels value contains upper case letters", func(t *testing.T) {
		l := map[string]string{
			"test_key": "invalid_value_with_A",
		}

		incoming, err := label.FromMap(l)
		assert.NoError(t, err)

		actualError := incoming.Validate()

		assert.ErrorContains(t, actualError, "value should only be combination of lower case letters, numerics, underscores, and/or dashes")
	})

	t.Run("should return error if labels value does not start with alphanumeric", func(t *testing.T) {
		l := map[string]string{
			"test_key": "-invalid_value",
		}

		incoming, err := label.FromMap(l)
		assert.NoError(t, err)

		actualError := incoming.Validate()

		assert.ErrorContains(t, actualError, "value should start and end with alphanumerics only")
	})

	t.Run("should return error if labels value does not end with alphanumeric", func(t *testing.T) {
		l := map[string]string{
			"test_key": "invalid_value-",
		}

		incoming, err := label.FromMap(l)
		assert.NoError(t, err)

		actualError := incoming.Validate()

		assert.ErrorContains(t, actualError, "value should start and end with alphanumerics only")
	})

	t.Run("should return nil if 'labels' follow valid spec", func(t *testing.T) {
		l := map[string]string{
			"test_key":      "test_value",
			"id":            "abcd-efgh-ijkl-mnop",
			"resource-name": "1_resource_test",
			"job-name":      "job_test_1",
		}

		incoming, err := label.FromMap(l)
		assert.NoError(t, err)

		actualError := incoming.Validate()

		assert.NoError(t, actualError)
	})
}
